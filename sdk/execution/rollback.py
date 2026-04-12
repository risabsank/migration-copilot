"""Rollback planning and execution primitives for failed migrations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sdk.observability import EventCollector
from sdk.state.models import (
    CDCJobStatus,
    CDCTableStatus,
    MigrationRun,
    MigrationRunStatus,
    RollbackPlan,
    RollbackStatus,
    RollbackStep,
    RollbackTriggerReason,
    utc_now_iso,
)
from sdk.state.store import MigrationRunStore


class RollbackOperationsAdapter(Protocol):
    """Operational hooks for deterministic rollback actions."""

    def mark_target_inactive(self, *, run: MigrationRun) -> str | None:
        """Mark target deployment inactive/untrusted and return optional checkpoint."""

    def restore_source_of_truth(self, *, run: MigrationRun) -> str | None:
        """Restore source system as source-of-truth and return optional checkpoint."""

    def stop_or_invalidate_replication(self, *, run: MigrationRun) -> str | None:
        """Stop CDC safely or mark replication validity invalid and return optional checkpoint."""


@dataclass(frozen=True)
class RollbackPolicy:
    """Guardrails for rollback execution."""

    allow_irreversible_after_cutover_complete: bool = False


class RollbackExecutor:
    """Resumable deterministic rollback executor with persisted step state."""

    _STEP_BLUEPRINT: tuple[tuple[str, str], ...] = (
        ("mark_target_inactive", "Mark target as inactive/untrusted"),
        ("restore_source_of_truth", "Restore source-of-truth designation"),
        ("stop_or_invalidate_replication", "Stop CDC or invalidate replication state"),
    )

    def __init__(
        self,
        *,
        store: MigrationRunStore,
        adapter: RollbackOperationsAdapter,
        collector: EventCollector | None = None,
        policy: RollbackPolicy | None = None,
    ):
        self._store = store
        self._adapter = adapter
        self._collector = collector
        self._policy = policy or RollbackPolicy()

    def execute(
        self,
        *,
        run_id: str,
        trigger_reason: RollbackTriggerReason,
        operator_summary: str | None = None,
    ) -> MigrationRun:
        run = self._load(run_id)
        self._assert_reversible(run)

        plan = self._ensure_plan(run=run, trigger_reason=trigger_reason, operator_summary=operator_summary)
        if run.status != MigrationRunStatus.ROLLBACK_IN_PROGRESS:
            run.transition_to(MigrationRunStatus.ROLLBACK_IN_PROGRESS)

        self._store.save(run)
        self._emit(
            "rollback_initiated",
            "started",
            run,
            {
                "run_id": run.run_id,
                "trigger_reason": trigger_reason.value,
                "resume_count": plan.resumed_count,
            },
        )

        try:
            for step in plan.steps:
                if step.status == RollbackStatus.COMPLETED:
                    continue
                self._run_step(run=run, plan=plan, step=step)

            plan.status = RollbackStatus.COMPLETED
            plan.completed_at = utc_now_iso()
            plan.failed_at = None
            plan.active_step_id = None
            plan.summary = (
                f"Rollback completed after trigger={trigger_reason.value}. "
                f"Executed {len(plan.steps)} deterministic steps."
            )
            run.rollback_ready = False
            run.rollback_readiness.ready = False
            run.rollback_readiness.status = RollbackStatus.COMPLETED
            run.rollback_readiness.notes.append("Rollback was executed; fresh readiness verification required before reuse.")
            run.cdc_status = CDCJobStatus.STOPPED
            for item in run.cdc_table_progress:
                if item.status != CDCTableStatus.FAILED:
                    item.status = CDCTableStatus.STOPPED
                    item.updated_at = utc_now_iso()
            run.replication_lag_seconds = None
            run.source_freshness_seconds = None
            run.cutover_ready = False
            run.transition_to(MigrationRunStatus.ROLLED_BACK)
            self._store.save(run)
            self._emit(
                "rollback_succeeded",
                "completed",
                run,
                {
                    "run_id": run.run_id,
                    "checkpoints": list(plan.checkpoints),
                    "summary": plan.summary,
                },
            )
            return run
        except Exception as exc:
            plan.status = RollbackStatus.FAILED
            plan.failed_at = utc_now_iso()
            plan.summary = f"Rollback failed at step={plan.active_step_id}: {exc}"
            run.failed_phase = "rollback"
            if run.status != MigrationRunStatus.FAILED:
                run.transition_to(MigrationRunStatus.FAILED)
            self._store.save(run)
            self._emit(
                "rollback_failed",
                "failed",
                run,
                {
                    "run_id": run.run_id,
                    "error": str(exc),
                    "active_step_id": plan.active_step_id,
                },
            )
            raise

    def _run_step(self, *, run: MigrationRun, plan: RollbackPlan, step: RollbackStep) -> None:
        step.status = RollbackStatus.IN_PROGRESS
        step.started_at = step.started_at or utc_now_iso()
        step.error_message = None
        plan.status = RollbackStatus.IN_PROGRESS
        plan.active_step_id = step.step_id
        self._store.save(run)
        self._emit(
            "rollback_step_started",
            "started",
            run,
            {"run_id": run.run_id, "step_id": step.step_id, "description": step.description},
        )

        if step.step_id == "mark_target_inactive":
            checkpoint = self._adapter.mark_target_inactive(run=run)
        elif step.step_id == "restore_source_of_truth":
            checkpoint = self._adapter.restore_source_of_truth(run=run)
        elif step.step_id == "stop_or_invalidate_replication":
            checkpoint = self._adapter.stop_or_invalidate_replication(run=run)
        else:
            raise RuntimeError(f"Unknown rollback step id {step.step_id}")

        step.status = RollbackStatus.COMPLETED
        step.completed_at = utc_now_iso()
        step.checkpoint = checkpoint
        step.details = f"Step completed successfully at {step.completed_at}"
        if checkpoint:
            plan.checkpoints.append(f"{step.step_id}:{checkpoint}")
        self._store.save(run)
        self._emit(
            "rollback_step_completed",
            "completed",
            run,
            {
                "run_id": run.run_id,
                "step_id": step.step_id,
                "checkpoint": checkpoint,
            },
        )

    def _ensure_plan(
        self,
        *,
        run: MigrationRun,
        trigger_reason: RollbackTriggerReason,
        operator_summary: str | None,
    ) -> RollbackPlan:
        plan = run.rollback_plan
        if not plan.steps:
            plan.steps = [RollbackStep(step_id=step_id, description=description) for step_id, description in self._STEP_BLUEPRINT]
            plan.status = RollbackStatus.READY
            plan.initiated_at = utc_now_iso()
            plan.trigger_reason = trigger_reason
            plan.checkpoints = []
            plan.summary = "Rollback plan prepared"
        else:
            plan.resumed_count += 1
            if plan.trigger_reason is None:
                plan.trigger_reason = trigger_reason

        if operator_summary:
            plan.operator_summary = operator_summary
        return plan

    def _assert_reversible(self, run: MigrationRun) -> None:
        if run.status == MigrationRunStatus.CUTOVER_COMPLETE and not self._policy.allow_irreversible_after_cutover_complete:
            raise RuntimeError(
                "Rollback blocked: run is in irreversible cutover_complete state. "
                "Override policy allow_irreversible_after_cutover_complete=True for emergency rollback."
            )

    def _load(self, run_id: str) -> MigrationRun:
        run = self._store.get(run_id)
        if run is None:
            raise ValueError(f"Migration run {run_id} was not found")
        return run

    def _emit(self, event_type: str, status: str, run: MigrationRun, payload: dict[str, object]) -> None:
        if not self._collector:
            return
        self._collector.emit(
            event_type=event_type,
            step="rollback_executor",
            status=status,
            payload=payload,
        )


class FakeRollbackOperationsAdapter:
    """In-memory adapter fake for rollback executor tests."""

    def __init__(self, *, fail_on: str | None = None):
        self.fail_on = fail_on
        self.calls: list[str] = []

    def mark_target_inactive(self, *, run: MigrationRun) -> str | None:
        self.calls.append("mark_target_inactive")
        if self.fail_on == "mark_target_inactive":
            raise RuntimeError("Injected rollback failure on mark_target_inactive")
        return "target-untrusted"

    def restore_source_of_truth(self, *, run: MigrationRun) -> str | None:
        self.calls.append("restore_source_of_truth")
        if self.fail_on == "restore_source_of_truth":
            raise RuntimeError("Injected rollback failure on restore_source_of_truth")
        return "source-primary"

    def stop_or_invalidate_replication(self, *, run: MigrationRun) -> str | None:
        self.calls.append("stop_or_invalidate_replication")
        if self.fail_on == "stop_or_invalidate_replication":
            raise RuntimeError("Injected rollback failure on stop_or_invalidate_replication")
        return "replication-invalid"
