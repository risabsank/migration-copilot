"""Migration orchestration service coordinating end-to-end phase execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from sdk.observability import EventCollector
from sdk.orchestration.models import OrchestrationFinalStatus, OrchestrationResult
from sdk.orchestration.policy import PhaseTransitionPolicy
from sdk.state.models import MigrationRun, MigrationRunStatus, OrchestrationPhase
from sdk.state.store import MigrationRunStore

PhaseHandler = Callable[[MigrationRun], None]


class RollbackRunner(Protocol):
    """Rollback executor interface used by orchestration failure policies."""

    def execute(
        self,
        *,
        run_id: str,
        trigger_reason: RollbackTriggerReason,
        operator_summary: str | None = None,
    ) -> MigrationRun:
        """Execute resumable rollback for a failed run."""

@dataclass
class MigrationOrchestrator:
    """Coordinates multi-phase migration execution using persisted run state."""

    store: MigrationRunStore
    collector: EventCollector | None = None
    phase_handlers: dict[OrchestrationPhase, PhaseHandler] | None = None
    transition_policy: PhaseTransitionPolicy = PhaseTransitionPolicy()
    rollback_runner: RollbackRunner | None = None

    def run(self, *, run_id: str) -> OrchestrationResult:
        """Start or continue orchestration from persisted migration run state."""
        run = self._load_run(run_id)
        is_resume = bool(run.completed_phases)
        run.paused = False
        run.pause_requested = False
        self._store_run(run)

        self._emit(
            event_type="orchestration_resumed" if is_resume else "orchestration_started",
            status="running",
            payload={"run_id": run.run_id, "phase": run.orchestration_phase.value},
        )

        while run.orchestration_phase not in {OrchestrationPhase.COMPLETED, OrchestrationPhase.ROLLBACK}:
            if run.pause_requested:
                run.paused = True
                self._store_run(run)
                return OrchestrationResult(
                    final_status=OrchestrationFinalStatus.PAUSED,
                    completed_phases=list(run.completed_phases),
                    failed_phase=run.failed_phase,
                    last_checkpoint=run.last_checkpoint,
                )

            current_phase = run.orchestration_phase
            self._emit(
                event_type="phase_started",
                status="running",
                payload={"run_id": run.run_id, "phase": current_phase.value},
            )

            try:
                self._execute_phase(run, current_phase)
                run.last_checkpoint = current_phase.value
                if current_phase.value not in run.completed_phases:
                    run.completed_phases.append(current_phase.value)
                run.failed_phase = None

                next_phase = _DEFAULT_PHASE_SEQUENCE[current_phase]
                self.transition_policy.assert_transition(current_phase, next_phase)
                run.orchestration_phase = next_phase
                self._store_run(run)

                self._emit(
                    event_type="phase_completed",
                    status="completed",
                    payload={"run_id": run.run_id, "phase": current_phase.value, "next_phase": next_phase.value},
                )
            except Exception as exc:
                run.failed_phase = current_phase.value
                run.last_checkpoint = current_phase.value
                run.orchestration_phase = OrchestrationPhase.ROLLBACK
                if run.status != MigrationRunStatus.FAILED:
                    run.transition_to(MigrationRunStatus.FAILED)
                self._store_run(run)
                if self.rollback_runner and current_phase in {
                    OrchestrationPhase.CUTOVER,
                    OrchestrationPhase.POST_CUTOVER_VALIDATION,
                }:
                    reason = (
                        RollbackTriggerReason.POST_CUTOVER_VALIDATION_FAILED
                        if current_phase == OrchestrationPhase.POST_CUTOVER_VALIDATION
                        else RollbackTriggerReason.CUTOVER_FAILED
                    )
                    self.rollback_runner.execute(
                        run_id=run.run_id,
                        trigger_reason=reason,
                        operator_summary=f"Orchestration-triggered rollback after phase failure: {current_phase.value}",
                    )
                    run = self._load_run(run.run_id)
                self._emit(
                    event_type="phase_failed",
                    status="failed",
                    payload={"run_id": run.run_id, "phase": current_phase.value, "error": str(exc)},
                )
                return OrchestrationResult(
                    final_status=OrchestrationFinalStatus.FAILED,
                    completed_phases=list(run.completed_phases),
                    failed_phase=run.failed_phase,
                    last_checkpoint=run.last_checkpoint,
                )

        if run.orchestration_phase == OrchestrationPhase.COMPLETED:
            self._emit(
                event_type="orchestration_completed",
                status="completed",
                payload={"run_id": run.run_id, "completed_phases": run.completed_phases},
            )
            return OrchestrationResult(
                final_status=OrchestrationFinalStatus.COMPLETED,
                completed_phases=list(run.completed_phases),
                failed_phase=None,
                last_checkpoint=run.last_checkpoint,
            )

        return OrchestrationResult(
            final_status=OrchestrationFinalStatus.FAILED,
            completed_phases=list(run.completed_phases),
            failed_phase=run.failed_phase,
            last_checkpoint=run.last_checkpoint,
        )

    def request_pause(self, *, run_id: str) -> MigrationRun:
        """Persist a pause request that is honored on the next phase boundary."""
        run = self._load_run(run_id)
        run.pause_requested = True
        return self._store_run(run)

    def resume(self, *, run_id: str) -> OrchestrationResult:
        """Resume orchestration for a paused or partially completed run."""
        run = self._load_run(run_id)
        run.pause_requested = False
        run.paused = False
        self._store_run(run)
        return self.run(run_id=run_id)

    def _execute_phase(self, run: MigrationRun, phase: OrchestrationPhase) -> None:
        handlers = self.phase_handlers or {}
        handler = handlers.get(phase)
        if handler:
            handler(run)

    def _load_run(self, run_id: str) -> MigrationRun:
        run = self.store.get(run_id)
        if run is None:
            raise ValueError(f"Migration run {run_id} was not found")
        return run

    def _store_run(self, run: MigrationRun) -> MigrationRun:
        return self.store.save(run)

    def _emit(self, *, event_type: str, status: str, payload: dict[str, object]) -> None:
        if not self.collector:
            return
        self.collector.emit(
            event_type=event_type,
            step="migration_orchestrator",
            status=status,
            payload=payload,
        )


_DEFAULT_PHASE_SEQUENCE: dict[OrchestrationPhase, OrchestrationPhase] = {
    OrchestrationPhase.PLAN_READY: OrchestrationPhase.PROVISIONING,
    OrchestrationPhase.PROVISIONING: OrchestrationPhase.PREFLIGHT_VALIDATION,
    OrchestrationPhase.PREFLIGHT_VALIDATION: OrchestrationPhase.BACKFILL,
    OrchestrationPhase.BACKFILL: OrchestrationPhase.POST_BACKFILL_VALIDATION,
    OrchestrationPhase.POST_BACKFILL_VALIDATION: OrchestrationPhase.CDC_START,
    OrchestrationPhase.CDC_START: OrchestrationPhase.CDC_CATCHUP,
    OrchestrationPhase.CDC_CATCHUP: OrchestrationPhase.CUTOVER_PRECHECK,
    OrchestrationPhase.CUTOVER_PRECHECK: OrchestrationPhase.CUTOVER,
    OrchestrationPhase.CUTOVER: OrchestrationPhase.POST_CUTOVER_VALIDATION,
    OrchestrationPhase.POST_CUTOVER_VALIDATION: OrchestrationPhase.COMPLETED,
}
