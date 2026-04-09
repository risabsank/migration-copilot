"""Execution policy engine for autonomous vs human-gated migration actions."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from sdk.state.models import MigrationRun, OrchestrationPhase, utc_now_iso


class ExecutionPolicyProfile(str, Enum):
    """Governance profile controlling autonomous execution authority."""

    MANUAL = "manual"
    SUPERVISED = "supervised"
    SEMI_AUTONOMOUS = "semi_autonomous"
    GUARDED_AUTONOMOUS = "guarded_autonomous"


class ActionRiskTier(str, Enum):
    """Risk tier used to classify execution actions."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ExecutionAction(str, Enum):
    """Execution operations subject to policy review."""

    START_BACKFILL = "start_backfill"
    RETRY_FAILED_TABLE = "retry_failed_table"
    TUNE_CHUNK_SIZE = "tune_chunk_size"
    START_CDC = "start_cdc"
    BEGIN_CUTOVER = "begin_cutover"
    ROLLBACK = "rollback"


class ApprovalState(str, Enum):
    """Approval outcome captured for audit and persistence."""

    REQUESTED = "requested"
    GRANTED = "granted"
    DENIED = "denied"
    OVERRIDDEN = "overridden"


@dataclass(frozen=True)
class ApprovalRequirement:
    """Policy requirement for whether an action needs human approval."""

    action: ExecutionAction
    risk_tier: ActionRiskTier
    effective_profile: ExecutionPolicyProfile
    phase: OrchestrationPhase | None
    requires_human_approval: bool
    reason: str


@dataclass(frozen=True)
class ApprovalDecision:
    """Decision and metadata persisted to run state for audit trail."""

    action: ExecutionAction
    risk_tier: ActionRiskTier
    state: ApprovalState
    approved: bool
    phase: OrchestrationPhase | None
    reason: str
    decided_by: str
    decision_source: str
    requested_at: str = field(default_factory=utc_now_iso)
    decided_at: str = field(default_factory=utc_now_iso)
    override_by: str | None = None
    override_reason: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "action": self.action.value,
            "risk_tier": self.risk_tier.value,
            "state": self.state.value,
            "approved": self.approved,
            "phase": self.phase.value if self.phase else None,
            "reason": self.reason,
            "decided_by": self.decided_by,
            "decision_source": self.decision_source,
            "requested_at": self.requested_at,
            "decided_at": self.decided_at,
            "override_by": self.override_by,
            "override_reason": self.override_reason,
        }


@dataclass(frozen=True)
class ExecutionPolicyEngine:
    """Deterministic execution policy engine with phase-level overrides."""

    action_risk_tiers: dict[ExecutionAction, ActionRiskTier] = field(
        default_factory=lambda: {
            ExecutionAction.START_BACKFILL: ActionRiskTier.MEDIUM,
            ExecutionAction.RETRY_FAILED_TABLE: ActionRiskTier.LOW,
            ExecutionAction.TUNE_CHUNK_SIZE: ActionRiskTier.LOW,
            ExecutionAction.START_CDC: ActionRiskTier.MEDIUM,
            ExecutionAction.BEGIN_CUTOVER: ActionRiskTier.HIGH,
            ExecutionAction.ROLLBACK: ActionRiskTier.CRITICAL,
        }
    )

    def evaluate_requirement(
        self,
        *,
        run: MigrationRun,
        action: ExecutionAction,
        phase: OrchestrationPhase | None = None,
    ) -> ApprovalRequirement:
        effective_phase = phase or run.orchestration_phase
        profile = self._effective_profile(run, effective_phase)
        risk = self.action_risk_tiers[action]
        requires_human = self._requires_human(profile=profile, risk=risk)

        if requires_human:
            reason = f"{profile.value} policy requires human approval for {risk.value}-risk action {action.value}."
        else:
            reason = f"{profile.value} policy permits autonomous execution for {risk.value}-risk action {action.value}."

        return ApprovalRequirement(
            action=action,
            risk_tier=risk,
            effective_profile=profile,
            phase=effective_phase,
            requires_human_approval=requires_human,
            reason=reason,
        )

    def decide(
        self,
        *,
        run: MigrationRun,
        action: ExecutionAction,
        phase: OrchestrationPhase | None = None,
        actor: str = "system",
        human_approved: bool | None = None,
        override_by: str | None = None,
        override_reason: str | None = None,
    ) -> ApprovalDecision:
        requirement = self.evaluate_requirement(run=run, action=action, phase=phase)

        if override_by:
            return ApprovalDecision(
                action=action,
                risk_tier=requirement.risk_tier,
                state=ApprovalState.OVERRIDDEN,
                approved=True,
                phase=requirement.phase,
                reason=requirement.reason,
                decided_by=override_by,
                decision_source="human_override",
                override_by=override_by,
                override_reason=override_reason,
            )

        if requirement.requires_human_approval:
            approved = bool(human_approved)
            state = ApprovalState.GRANTED if approved else ApprovalState.DENIED
            source = "human" if human_approved is not None else "policy"
            return ApprovalDecision(
                action=action,
                risk_tier=requirement.risk_tier,
                state=state,
                approved=approved,
                phase=requirement.phase,
                reason=requirement.reason,
                decided_by=actor,
                decision_source=source,
            )

        return ApprovalDecision(
            action=action,
            risk_tier=requirement.risk_tier,
            state=ApprovalState.GRANTED,
            approved=True,
            phase=requirement.phase,
            reason=requirement.reason,
            decided_by=actor,
            decision_source="policy_auto",
        )

    def _effective_profile(self, run: MigrationRun, phase: OrchestrationPhase) -> ExecutionPolicyProfile:
        override = run.phase_execution_policy_overrides.get(phase.value)
        if override:
            return ExecutionPolicyProfile(override)
        return ExecutionPolicyProfile(run.execution_policy_profile)

    @staticmethod
    def _requires_human(*, profile: ExecutionPolicyProfile, risk: ActionRiskTier) -> bool:
        if profile == ExecutionPolicyProfile.MANUAL:
            return True
        if profile == ExecutionPolicyProfile.SUPERVISED:
            return risk in {ActionRiskTier.MEDIUM, ActionRiskTier.HIGH, ActionRiskTier.CRITICAL}
        if profile == ExecutionPolicyProfile.SEMI_AUTONOMOUS:
            return risk in {ActionRiskTier.HIGH, ActionRiskTier.CRITICAL}
        if profile == ExecutionPolicyProfile.GUARDED_AUTONOMOUS:
            return risk is not ActionRiskTier.LOW
        return True
