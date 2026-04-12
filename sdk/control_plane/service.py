"""Operator-facing control-plane service for migration run APIs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sdk.observability import EventCollector, PlanEvent
from sdk.operations.monitoring import RunHealthMonitoringService
from sdk.orchestration.execution_policy import (
    ApprovalDecision,
    ApprovalState,
    ExecutionAction,
    ExecutionPolicyEngine,
)
from sdk.orchestration.models import OrchestrationResult
from sdk.orchestration.service import MigrationOrchestrator
from sdk.engine.models import MigrationPattern
from sdk.state.models import (
    IncidentPack,
    MigrationRun,
    MigrationRunStatus,
    RollbackTriggerReason,
    RollbackStatus,
    TableExecutionStatus,
)
from sdk.state.store import MigrationRunStore


@dataclass
class ControlPlaneService:
    """Thin API-oriented façade over existing run-state and orchestration services."""

    store: MigrationRunStore
    collector: EventCollector
    execution_policy: ExecutionPolicyEngine

    def __post_init__(self) -> None:
        self._monitoring = RunHealthMonitoringService()
        self._orchestrator = MigrationOrchestrator(
            store=self.store,
            collector=self.collector,
            execution_policy=self.execution_policy,
        )

    def list_runs(self) -> list[dict[str, Any]]:
        return [self._run_summary(run) for run in self.store.list()]

    def create_run_from_plan(
        self,
        *,
        plan_id: str,
        schema: str,
        selected_variant: str,
        pattern: str,
        table_names: list[str],
        run_id: str | None = None,
    ) -> dict[str, Any]:
        run = MigrationRun.new(
            plan_id=plan_id,
            schema=schema,
            selected_variant=selected_variant,
            pattern=MigrationPattern(pattern),
            table_names=table_names,
            run_id=run_id,
        )
        saved = self.store.save(run)
        self._emit("run_created", "completed", saved, {"table_count": len(table_names)})
        return self._run_detail(saved)

    def get_run(self, run_id: str) -> dict[str, Any]:
        run = self._must_get(run_id)
        return self._run_detail(run)

    def start_orchestration(self, *, run_id: str, max_phases: int = 1) -> OrchestrationResult:
        return self._orchestrator.run(run_id=run_id, max_phases=max_phases)

    def pause_orchestration(self, *, run_id: str) -> MigrationRun:
        return self._orchestrator.request_pause(run_id=run_id)

    def resume_orchestration(self, *, run_id: str, max_phases: int = 1) -> OrchestrationResult:
        if max_phases == 1:
            # resume() executes until terminal state; keep UI-safe phase stepping.
            return self._orchestrator.run(run_id=run_id, max_phases=max_phases)
        return self._orchestrator.resume(run_id=run_id)

    def retry_failed_table(
        self,
        *,
        run_id: str,
        table_name: str,
        actor: str,
        human_approved: bool | None,
    ) -> ApprovalDecision:
        run = self._must_get(run_id)
        decision = self.execution_policy.decide(
            run=run,
            action=ExecutionAction.RETRY_FAILED_TABLE,
            actor=actor,
            human_approved=human_approved,
        )
        run.approval_history.append(decision.as_dict())
        if decision.approved:
            for table in run.table_progress:
                if table.table_name == table_name and table.status == TableExecutionStatus.FAILED:
                    table.status = TableExecutionStatus.PENDING
                    table.error_message = None
                    table.progress_percent = max(0.0, min(table.progress_percent, 99.0))
            self._emit(
                "table_retry_requested",
                "completed",
                run,
                {"table_name": table_name, "actor": actor},
            )
        self.store.save(run)
        return decision

    def request_approval(self, *, run_id: str, action: ExecutionAction, actor: str) -> dict[str, Any]:
        run = self._must_get(run_id)
        requirement = self.execution_policy.evaluate_requirement(run=run, action=action)
        request_record = {
            "action": action.value,
            "risk_tier": requirement.risk_tier.value,
            "state": ApprovalState.REQUESTED.value,
            "approved": False,
            "phase": requirement.phase.value if requirement.phase else None,
            "reason": requirement.reason,
            "decided_by": actor,
            "decision_source": "operator_request",
        }
        run.approval_history.append(request_record)
        self.store.save(run)
        self._emit("approval_requested", "running", run, {"action": action.value, "actor": actor})
        return request_record

    def approve_or_deny_action(
        self,
        *,
        run_id: str,
        action: ExecutionAction,
        actor: str,
        approved: bool,
    ) -> ApprovalDecision:
        run = self._must_get(run_id)
        decision = self.execution_policy.decide(
            run=run,
            action=action,
            actor=actor,
            human_approved=approved,
        )
        run.approval_history.append(decision.as_dict())
        self.store.save(run)
        self._emit(
            "approval_granted" if decision.approved else "approval_denied",
            "completed" if decision.approved else "blocked",
            run,
            {"action": action.value, "actor": actor},
        )
        return decision

    def trigger_rollback(
        self,
        *,
        run_id: str,
        actor: str,
        human_approved: bool | None,
        reason: RollbackTriggerReason = RollbackTriggerReason.OPERATOR_REQUESTED,
    ) -> dict[str, Any]:
        run = self._must_get(run_id)
        decision = self.execution_policy.decide(
            run=run,
            action=ExecutionAction.ROLLBACK,
            actor=actor,
            human_approved=human_approved,
        )
        run.approval_history.append(decision.as_dict())
        self.store.save(run)
        if not decision.approved:
            raise PermissionError(f"Rollback denied by policy: {decision.reason}")
        try:
            run.transition_to(MigrationRunStatus.ROLLBACK_IN_PROGRESS)
        except ValueError:
            if run.status != MigrationRunStatus.FAILED:
                run.transition_to(MigrationRunStatus.FAILED)
            run.transition_to(MigrationRunStatus.ROLLBACK_IN_PROGRESS)
        run.transition_to(MigrationRunStatus.ROLLED_BACK)
        run.rollback_ready = False
        run.cutover_ready = False
        self._emit(
            "rollback_triggered",
            "completed",
            run,
            {"actor": actor, "reason": reason.value},
        )
        return self._run_detail(self.store.save(run))

    def timeline(self, *, run_id: str) -> list[dict[str, Any]]:
        run = self._must_get(run_id)
        return self._build_timeline(run)

    def incident_pack(self, *, run_id: str) -> dict[str, Any]:
        run = self._must_get(run_id)
        events = self._events_for_run(run)
        pack: IncidentPack = self._monitoring.generate_incident_pack(run=run, events=events)
        return pack.as_dict()

    def dashboard(self, *, run_id: str) -> dict[str, Any]:
        run = self._must_get(run_id)
        snapshot = self._monitoring.compute_snapshot(run=run, events=self._events_for_run(run))
        table_completion = {
            "completed": len([t for t in run.table_progress if t.status == TableExecutionStatus.COMPLETED]),
            "failed": len([t for t in run.table_progress if t.status == TableExecutionStatus.FAILED]),
            "total": len(run.table_progress),
        }
        return {
            "health": snapshot.as_dict(),
            "table_completion": table_completion,
            "lag_seconds": run.replication_lag_seconds,
            "source_freshness_seconds": run.source_freshness_seconds,
            "cutover_ready": run.cutover_ready,
            "rollback_ready": run.rollback_ready,
            "warnings": run.cutover_evaluation.advisory_warnings,
            "blockers": run.cutover_evaluation.blocking_conditions,
        }

    def _run_summary(self, run: MigrationRun) -> dict[str, Any]:
        blocker_count = len(run.cutover_evaluation.blocking_conditions) + len(run.unresolved_risk_flags)
        return {
            "run_id": run.run_id,
            "plan_id": run.plan_id,
            "status": run.status.value,
            "phase": run.phase.value,
            "orchestration_phase": run.orchestration_phase.value,
            "validation_status": run.validation_status.value,
            "cutover_ready": run.cutover_ready,
            "rollback_ready": run.rollback_ready,
            "blocker_count": blocker_count,
            "created_at": run.created_at,
            "updated_at": run.updated_at,
        }

    def _run_detail(self, run: MigrationRun) -> dict[str, Any]:
        raw = run.as_dict()
        raw["lifecycle"] = {
            "is_terminal": run.status
            in {
                MigrationRunStatus.CUTOVER_COMPLETE,
                MigrationRunStatus.ROLLED_BACK,
                MigrationRunStatus.FAILED,
            },
            "has_blockers": bool(run.cutover_evaluation.blocking_conditions or run.unresolved_risk_flags),
            "warnings": list(run.cutover_evaluation.advisory_warnings),
            "blockers": list(run.cutover_evaluation.blocking_conditions) + list(run.unresolved_risk_flags),
        }
        raw["timeline"] = self._build_timeline(run)
        raw["dashboard"] = self.dashboard(run_id=run.run_id)
        raw["rollback_plan"] = {
            "status": RollbackStatus.COMPLETED.value if run.status == MigrationRunStatus.ROLLED_BACK else RollbackStatus.READY.value,
            "trigger_reason": RollbackTriggerReason.OPERATOR_REQUESTED.value if run.status == MigrationRunStatus.ROLLED_BACK else None,
            "summary": "Rollback status synthesized from persisted run lifecycle.",
            "steps": [],
            "checkpoints": [],
        }
        raw["rollback_readiness"] = {
            "ready": run.rollback_ready,
            "status": RollbackStatus.COMPLETED.value if run.status == MigrationRunStatus.ROLLED_BACK else RollbackStatus.READY.value,
            "notes": ["Use dedicated rollback executor integration for step-level detail."],
        }
        return raw

    def _build_timeline(self, run: MigrationRun) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = [
            {
                "timestamp": run.created_at,
                "event_type": "run_created",
                "description": f"Run {run.run_id} created from plan {run.plan_id}.",
            }
        ]
        for phase in run.completed_phases:
            events.append(
                {
                    "timestamp": run.updated_at,
                    "event_type": "phase_completed",
                    "description": f"Orchestration phase completed: {phase}.",
                    "phase": phase,
                }
            )
        for decision in run.approval_history:
            events.append(
                {
                    "timestamp": decision.get("decided_at") or decision.get("requested_at") or run.updated_at,
                    "event_type": f"approval_{decision.get('state', 'unknown')}",
                    "description": f"{decision.get('action', 'unknown')} -> {decision.get('state', 'unknown')}",
                    "action": decision.get("action"),
                }
            )
        if run.failed_phase:
            events.append(
                {
                    "timestamp": run.updated_at,
                    "event_type": "phase_failed",
                    "description": f"Phase failed: {run.failed_phase}",
                    "phase": run.failed_phase,
                }
            )
        return sorted(events, key=lambda item: str(item.get("timestamp", "")))

    def _events_for_run(self, run: MigrationRun) -> list[PlanEvent]:
        records = self._build_timeline(run)
        return [
            PlanEvent(
                event_type=item["event_type"],
                plan_id=run.plan_id,
                step="control_plane_api",
                status="completed" if "failed" not in item["event_type"] else "failed",
                payload={"run_id": run.run_id, "phase": item.get("phase") or "n/a", "description": item["description"]},
                ts_utc=str(item.get("timestamp", run.updated_at)),
            )
            for item in records
        ]

    def _must_get(self, run_id: str) -> MigrationRun:
        run = self.store.get(run_id)
        if run is None:
            raise ValueError(f"Migration run {run_id} was not found")
        return run

    def _emit(self, event_type: str, status: str, run: MigrationRun, payload: dict[str, Any]) -> None:
        self.collector.emit(
            event_type=event_type,
            step="control_plane_service",
            status=status,
            payload={"run_id": run.run_id, **payload},
        )
