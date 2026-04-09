"""AI-assisted migration execution supervisor with deterministic guardrails."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Protocol

from sdk.observability import EventCollector
from sdk.orchestration.execution_policy import (
    ApprovalState,
    ExecutionAction,
    ExecutionPolicyEngine,
)
from sdk.state.models import MigrationRun, OrchestrationPhase, ValidationCheckStatus
from sdk.state.store import MigrationRunStore


class SupervisorAction(str, Enum):
    """Allowed actions the AI supervisor can recommend."""

    NO_ACTION = "no_action"
    REDUCE_BACKFILL_CHUNK_SIZE = "reduce_backfill_chunk_size"
    PAUSE_MIGRATION = "pause_migration"
    RETRY_FAILED_TABLE = "retry_failed_table"
    HOLD_CUTOVER = "hold_cutover_due_to_lag_instability"
    RECOMMEND_ROLLBACK = "recommend_rollback"


class RecommendationDisposition(str, Enum):
    """Deterministic decision outcome for AI recommendations."""

    ADVISORY_ONLY = "advisory_only"
    AUTO_APPROVABLE = "auto_approvable"
    BLOCKED_PENDING_HUMAN_REVIEW = "blocked_pending_human_review"


@dataclass(frozen=True)
class ExecutionReviewInput:
    """Execution context provided to the AI operations supervisor."""

    run: MigrationRun
    recent_event_history: list[dict[str, Any]] = field(default_factory=list)
    validation_summaries: list[dict[str, Any]] = field(default_factory=list)
    lag_metrics: list[dict[str, Any]] = field(default_factory=list)
    adapter_error_summaries: list[dict[str, Any]] = field(default_factory=list)

    def as_prompt_payload(self) -> dict[str, Any]:
        return {
            "run": {
                "run_id": self.run.run_id,
                "status": self.run.status.value,
                "phase": self.run.phase.value,
                "orchestration_phase": self.run.orchestration_phase.value,
                "validation_status": self.run.validation_status.value,
                "replication_lag_seconds": self.run.replication_lag_seconds,
                "source_freshness_seconds": self.run.source_freshness_seconds,
            },
            "recent_event_history": self.recent_event_history,
            "validation_summaries": self.validation_summaries,
            "lag_metrics": self.lag_metrics,
            "adapter_error_summaries": self.adapter_error_summaries,
        }


@dataclass(frozen=True)
class SupervisorRecommendation:
    """Structured AI recommendation generated for migration operations."""

    recommended_action: SupervisorAction
    confidence: float
    rationale: str
    risk_flags: list[str] = field(default_factory=list)
    incident_note: str = ""

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["recommended_action"] = self.recommended_action.value
        return data


@dataclass(frozen=True)
class SupervisorDecision:
    """Final deterministic policy decision for a recommendation."""

    recommendation: SupervisorRecommendation
    disposition: RecommendationDisposition
    accepted: bool
    policy_reasons: list[str] = field(default_factory=list)
    fallback_used: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "recommendation": self.recommendation.as_dict(),
            "disposition": self.disposition.value,
            "accepted": self.accepted,
            "policy_reasons": list(self.policy_reasons),
            "fallback_used": self.fallback_used,
        }


class ExecutionReviewClient(Protocol):
    def complete_json(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> dict[str, Any]:
        """Return a structured recommendation payload."""


class HeuristicExecutionReviewClient:
    """Deterministic local recommendation generator for offline environments."""

    def complete_json(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> dict[str, Any]:
        del system_prompt, temperature
        payload = json.loads(user_prompt)
        lag = payload.get("run", {}).get("replication_lag_seconds")
        failed_checks = payload.get("run", {}).get("validation_status") == ValidationCheckStatus.FAILED.value
        if lag is not None and lag > 45:
            return {
                "recommended_action": SupervisorAction.HOLD_CUTOVER.value,
                "confidence": 0.78,
                "rationale": "Replication lag remains unstable beyond policy threshold.",
                "risk_flags": ["lag_instability"],
                "incident_note": "Hold cutover; monitor lag stability before resuming.",
            }
        if failed_checks:
            return {
                "recommended_action": SupervisorAction.RECOMMEND_ROLLBACK.value,
                "confidence": 0.82,
                "rationale": "Post-cutover validation checks are failing.",
                "risk_flags": ["post_cutover_validation_failure"],
                "incident_note": "Recommend controlled rollback with operator approval.",
            }
        return {
            "recommended_action": SupervisorAction.NO_ACTION.value,
            "confidence": 0.51,
            "rationale": "No significant anomalies detected.",
            "risk_flags": [],
            "incident_note": "System is healthy.",
        }


@dataclass(frozen=True)
class DeterministicOpsPolicy:
    """Deterministic policy gate for AI-generated operations recommendations."""

    max_auto_approve_confidence: float = 0.95

    def evaluate(self, recommendation: SupervisorRecommendation, run: MigrationRun) -> SupervisorDecision:
        reasons: list[str] = []

        if recommendation.recommended_action == SupervisorAction.NO_ACTION:
            return SupervisorDecision(
                recommendation=recommendation,
                disposition=RecommendationDisposition.ADVISORY_ONLY,
                accepted=True,
                policy_reasons=["No mutation requested; recommendation retained for audit only."],
            )

        if recommendation.confidence < 0.55:
            reasons.append("AI confidence below deterministic acceptance threshold.")

        if recommendation.recommended_action == SupervisorAction.RECOMMEND_ROLLBACK:
            allowed = (
                run.orchestration_phase == OrchestrationPhase.POST_CUTOVER_VALIDATION
                and run.validation_summary.status == ValidationCheckStatus.FAILED
            )
            if not allowed:
                return SupervisorDecision(
                    recommendation=recommendation,
                    disposition=RecommendationDisposition.BLOCKED_PENDING_HUMAN_REVIEW,
                    accepted=False,
                    policy_reasons=[
                        "Rollback recommendation blocked: only auto-approvable after post-cutover validation failure."
                    ],
                )
            return SupervisorDecision(
                recommendation=recommendation,
                disposition=RecommendationDisposition.AUTO_APPROVABLE,
                accepted=True,
                policy_reasons=["Rollback recommendation satisfies deterministic failure gate."],
            )

        if recommendation.recommended_action == SupervisorAction.HOLD_CUTOVER:
            in_cutover_window = run.orchestration_phase in {
                OrchestrationPhase.CUTOVER_PRECHECK,
                OrchestrationPhase.CUTOVER,
                OrchestrationPhase.CDC_CATCHUP,
            }
            lag_unstable = "lag_instability" in recommendation.risk_flags or (
                run.replication_lag_seconds is not None and run.replication_lag_seconds > 30
            )
            if in_cutover_window and lag_unstable and not reasons:
                return SupervisorDecision(
                    recommendation=recommendation,
                    disposition=RecommendationDisposition.AUTO_APPROVABLE,
                    accepted=True,
                    policy_reasons=["Lag instability gate met; holding cutover is auto-approvable."],
                )

        if recommendation.recommended_action in {
            SupervisorAction.REDUCE_BACKFILL_CHUNK_SIZE,
            SupervisorAction.PAUSE_MIGRATION,
            SupervisorAction.RETRY_FAILED_TABLE,
            SupervisorAction.HOLD_CUTOVER,
        }:
            disposition = RecommendationDisposition.ADVISORY_ONLY
            return SupervisorDecision(
                recommendation=recommendation,
                disposition=disposition,
                accepted=not reasons,
                policy_reasons=reasons or ["Recommendation is advisory; operator acknowledgment required."],
            )

        return SupervisorDecision(
            recommendation=recommendation,
            disposition=RecommendationDisposition.BLOCKED_PENDING_HUMAN_REVIEW,
            accepted=False,
            policy_reasons=["Unknown or disallowed action requested by AI supervisor."],
        )


@dataclass
class MigrationOpsSupervisor:
    """AI-assisted operations reviewer preserving deterministic control authority."""

    store: MigrationRunStore
    collector: EventCollector | None = None
    review_client: ExecutionReviewClient = field(default_factory=HeuristicExecutionReviewClient)
    policy: DeterministicOpsPolicy = field(default_factory=DeterministicOpsPolicy)
    execution_policy: ExecutionPolicyEngine = field(default_factory=ExecutionPolicyEngine)

    def review(self, review_input: ExecutionReviewInput) -> SupervisorDecision:
        payload = review_input.as_prompt_payload()
        run = review_input.run
        self._emit(
            event_type="ops_recommendation_generated",
            status="running",
            payload={"run_id": run.run_id, "orchestration_phase": run.orchestration_phase.value},
        )

        fallback_used = False
        try:
            response = self.review_client.complete_json(
                system_prompt="You are a migration operations supervisor. Recommend safe next actions.",
                user_prompt=json.dumps(payload),
            )
            recommendation = _parse_recommendation(response)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            fallback_used = True
            recommendation = SupervisorRecommendation(
                recommended_action=SupervisorAction.NO_ACTION,
                confidence=0.0,
                rationale="AI response malformed; deterministic fallback selected no_action.",
                risk_flags=["ai_output_malformed"],
                incident_note="Fallback engaged. Request operator review for context.",
            )
            self._emit(
                event_type="ops_recommendation_fallback",
                status="warning",
                payload={"run_id": run.run_id, "reason": "malformed_ai_output"},
            )

        decision = self.policy.evaluate(recommendation, run)
        execution_action = _ACTION_MAPPING.get(decision.recommendation.recommended_action)
        if execution_action and decision.accepted:
            approval_decision = self.execution_policy.decide(
                run=run,
                action=execution_action,
                phase=run.orchestration_phase,
                actor="migration_ops_supervisor",
            )
            run.approval_history.append(approval_decision.as_dict())
            self._emit(
                event_type="approval_requested",
                status="running",
                payload={
                    "run_id": run.run_id,
                    "action": execution_action.value,
                    "phase": run.orchestration_phase.value,
                },
            )
            if approval_decision.state == ApprovalState.OVERRIDDEN:
                self._emit(
                    event_type="approval_overridden",
                    status="warning",
                    payload={
                        "run_id": run.run_id,
                        "action": execution_action.value,
                        "phase": run.orchestration_phase.value,
                    },
                )
            elif approval_decision.approved:
                self._emit(
                    event_type="approval_granted",
                    status="completed",
                    payload={
                        "run_id": run.run_id,
                        "action": execution_action.value,
                        "phase": run.orchestration_phase.value,
                    },
                )
            else:
                self._emit(
                    event_type="approval_denied",
                    status="blocked",
                    payload={
                        "run_id": run.run_id,
                        "action": execution_action.value,
                        "phase": run.orchestration_phase.value,
                    },
                )
                decision = SupervisorDecision(
                    recommendation=decision.recommendation,
                    disposition=RecommendationDisposition.BLOCKED_PENDING_HUMAN_REVIEW,
                    accepted=False,
                    policy_reasons=decision.policy_reasons
                    + [f"Execution policy denied action {execution_action.value} pending human approval."],
                    fallback_used=decision.fallback_used,
                )
        if fallback_used:
            decision = SupervisorDecision(
                recommendation=decision.recommendation,
                disposition=decision.disposition,
                accepted=decision.accepted,
                policy_reasons=decision.policy_reasons,
                fallback_used=True,
            )

        run.ops_recommendation_history.append(decision.as_dict())
        self.store.save(run)

        event_type = "ops_recommendation_accepted" if decision.accepted else "ops_recommendation_rejected"
        self._emit(
            event_type=event_type,
            status="completed" if decision.accepted else "blocked",
            payload={
                "run_id": run.run_id,
                "disposition": decision.disposition.value,
                "action": decision.recommendation.recommended_action.value,
                "fallback_used": decision.fallback_used,
            },
        )
        return decision

    def _emit(self, *, event_type: str, status: str, payload: dict[str, object]) -> None:
        if not self.collector:
            return
        self.collector.emit(
            event_type=event_type,
            step="migration_ops_supervisor",
            status=status,
            payload=payload,
        )


def _parse_recommendation(data: dict[str, Any]) -> SupervisorRecommendation:
    for key in ("recommended_action", "confidence", "rationale"):
        if key not in data:
            raise ValueError(f"Missing required recommendation key: {key}")

    return SupervisorRecommendation(
        recommended_action=SupervisorAction(str(data["recommended_action"])),
        confidence=float(data["confidence"]),
        rationale=str(data["rationale"]),
        risk_flags=[str(item) for item in data.get("risk_flags", [])],
        incident_note=str(data.get("incident_note", "")),
    )

_ACTION_MAPPING: dict[SupervisorAction, ExecutionAction] = {
    SupervisorAction.REDUCE_BACKFILL_CHUNK_SIZE: ExecutionAction.TUNE_CHUNK_SIZE,
    SupervisorAction.RETRY_FAILED_TABLE: ExecutionAction.RETRY_FAILED_TABLE,
    SupervisorAction.HOLD_CUTOVER: ExecutionAction.BEGIN_CUTOVER,
    SupervisorAction.RECOMMEND_ROLLBACK: ExecutionAction.ROLLBACK,
}