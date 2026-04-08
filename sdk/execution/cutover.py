"""Cutover readiness evaluation and controlled cutover execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sdk.observability import EventCollector
from sdk.state.models import (
    CutoverExecutionStatus,
    CutoverGateEvaluation,
    MigrationRun,
    MigrationRunStatus,
    TableExecutionStatus,
    ValidationStatus,
    utc_now_iso,
)
from sdk.state.store import MigrationRunStore


@dataclass(frozen=True)
class CutoverPolicy:
    """Deterministic gate thresholds and requirements for cutover readiness."""

    max_cdc_lag_seconds: float = 30.0
    require_validation_passed: bool = True
    require_backfill_complete: bool = True
    require_rollback_ready: bool = True


class CutoverOperationsAdapter(Protocol):
    """Adapter hooks used by cutover execution; no traffic switching is performed."""

    def freeze_source_writes(self, *, run: MigrationRun) -> None:
        """Freeze writes on the source or mark source write window closed."""

    def perform_final_sync(self, *, run: MigrationRun) -> str | None:
        """Run final sync/checkpoint operation and return checkpoint id."""

    def mark_target_as_source_of_truth(self, *, run: MigrationRun) -> None:
        """Finalize metadata state so target is the source of truth."""


class FinalValidationPackRunner(Protocol):
    """Final validation pack execution hook used during cutover."""

    def run_final_validation_pack(self, *, run: MigrationRun) -> bool:
        """Return True when final validation checks pass."""


class CutoverEvaluator:
    """Evaluates whether a run is ready for controlled cutover."""

    def __init__(
        self,
        *,
        store: MigrationRunStore,
        collector: EventCollector | None = None,
        policy: CutoverPolicy | None = None,
    ):
        self._store = store
        self._collector = collector
        self._policy = policy or CutoverPolicy()

    def evaluate(self, *, run: MigrationRun) -> CutoverGateEvaluation:
        self._emit(
            event_type="cutover_precheck_started",
            status="started",
            run=run,
            payload={"run_id": run.run_id},
        )

        blocking: list[str] = []
        warnings: list[str] = []

        if self._policy.require_validation_passed and run.validation_status != ValidationStatus.PASSED:
            blocking.append("Validation status is not passed")

        if self._policy.require_backfill_complete and not _backfill_complete(run):
            blocking.append("Backfill is not complete for all tables")

        lag_seconds = run.replication_lag_seconds
        if lag_seconds is None:
            blocking.append("CDC lag is unknown; cannot verify threshold")
        elif lag_seconds > self._policy.max_cdc_lag_seconds:
            blocking.append(
                f"CDC lag {lag_seconds:.3f}s exceeds threshold {self._policy.max_cdc_lag_seconds:.3f}s"
            )
        elif lag_seconds > (self._policy.max_cdc_lag_seconds * 0.8):
            warnings.append(
                f"CDC lag {lag_seconds:.3f}s is close to threshold {self._policy.max_cdc_lag_seconds:.3f}s"
            )

        if run.unresolved_risk_flags:
            blocking.append(f"Unresolved risk flags present: {', '.join(sorted(run.unresolved_risk_flags))}")

        if self._policy.require_rollback_ready and not run.rollback_ready:
            blocking.append("Rollback readiness has not been established")

        evaluation = CutoverGateEvaluation(
            ready=not blocking,
            blocking_conditions=blocking,
            advisory_warnings=warnings,
            evaluated_at=utc_now_iso(),
        )
        run.cutover_evaluation = evaluation
        run.cutover_ready = evaluation.ready

        self._store.save(run)

        for condition in blocking:
            self._emit(
                event_type="blocking_gate_detected",
                status="blocked",
                run=run,
                payload={"run_id": run.run_id, "condition": condition},
            )

        self._emit(
            event_type="cutover_precheck_completed",
            status="ready" if evaluation.ready else "not_ready",
            run=run,
            payload={
                "run_id": run.run_id,
                "ready": evaluation.ready,
                "blocking_conditions": list(evaluation.blocking_conditions),
                "warnings": list(evaluation.advisory_warnings),
            },
        )
        return evaluation

    def _emit(self, *, event_type: str, status: str, run: MigrationRun, payload: dict[str, object]) -> None:
        if not self._collector:
            return
        self._collector.emit(
            event_type=event_type,
            step="cutover_evaluator",
            status=status,
            payload=payload,
        )


class CutoverExecutor:
    """Executes a deterministic, persistently tracked cutover sequence."""

    def __init__(
        self,
        *,
        store: MigrationRunStore,
        evaluator: CutoverEvaluator,
        adapter: CutoverOperationsAdapter,
        validation_runner: FinalValidationPackRunner,
        collector: EventCollector | None = None,
    ):
        self._store = store
        self._evaluator = evaluator
        self._adapter = adapter
        self._validation_runner = validation_runner
        self._collector = collector

    def execute(self, *, run_id: str, operator_note: str | None = None) -> MigrationRun:
        run = self._load(run_id)
        evaluation = self._evaluator.evaluate(run=run)
        if not evaluation.ready:
            raise RuntimeError(
                "Cutover blocked by gating conditions: " + "; ".join(evaluation.blocking_conditions)
            )

        state = run.cutover_execution
        state.status = CutoverExecutionStatus.IN_PROGRESS
        state.started_at = state.started_at or utc_now_iso()
        state.failed_at = None
        state.error_message = None
        state.recovery_path = None
        if operator_note:
            state.operator_notes.append(operator_note)

        if run.status == MigrationRunStatus.SYNCING:
            run.transition_to(MigrationRunStatus.CUTOVER_READY)

        self._store.save(run)
        self._emit("cutover_started", "started", run, {"run_id": run.run_id})

        try:
            self._adapter.freeze_source_writes(run=run)
            state.freeze_writes_at = utc_now_iso()
            state.hook_trace.append("freeze_source_writes")
            self._store.save(run)

            reevaluated = self._evaluator.evaluate(run=run)
            if not reevaluated.ready:
                raise RuntimeError(
                    "Cutover gate failed after source freeze: " + "; ".join(reevaluated.blocking_conditions)
                )

            final_checkpoint = self._adapter.perform_final_sync(run=run)
            state.final_sync_completed_at = utc_now_iso()
            state.hook_trace.append("perform_final_sync")
            if final_checkpoint:
                run.replication_checkpoint = final_checkpoint
                state.final_checkpoint = final_checkpoint
            self._store.save(run)
            self._emit(
                "final_sync_completed",
                "completed",
                run,
                {
                    "run_id": run.run_id,
                    "checkpoint": state.final_checkpoint,
                },
            )

            validations_passed = self._validation_runner.run_final_validation_pack(run=run)
            state.final_validation_completed_at = utc_now_iso()
            state.hook_trace.append("run_final_validation_pack")
            self._store.save(run)
            if not validations_passed:
                raise RuntimeError("Final validation pack failed")

            self._adapter.mark_target_as_source_of_truth(run=run)
            state.hook_trace.append("mark_target_as_source_of_truth")
            state.status = CutoverExecutionStatus.COMPLETED
            state.completed_at = utc_now_iso()
            if run.status != MigrationRunStatus.CUTOVER_COMPLETE:
                run.transition_to(MigrationRunStatus.CUTOVER_COMPLETE)
            self._store.save(run)
            self._emit(
                "cutover_completed",
                "completed",
                run,
                {
                    "run_id": run.run_id,
                    "checkpoint": state.final_checkpoint,
                    "completed_at": state.completed_at,
                },
            )
            return run
        except Exception as exc:
            state.status = CutoverExecutionStatus.FAILED
            state.failed_at = utc_now_iso()
            state.error_message = str(exc)
            state.recovery_path = (
                "Recover by resuming from persisted cutover_execution state, verify checkpoints, "
                "and re-run cutover after remediation."
            )
            if run.status != MigrationRunStatus.FAILED:
                run.transition_to(MigrationRunStatus.FAILED)
            run.failed_phase = "cutover"
            self._store.save(run)
            self._emit(
                "cutover_failed",
                "failed",
                run,
                {
                    "run_id": run.run_id,
                    "error": str(exc),
                    "recovery_path": state.recovery_path,
                },
            )
            raise

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
            step="cutover_executor",
            status=status,
            payload=payload,
        )


class FakeCutoverOperationsAdapter:
    """In-memory adapter fake for controlled cutover tests."""

    def __init__(self, *, fail_on: str | None = None, final_checkpoint: str = "cutover:checkpoint"):
        self.fail_on = fail_on
        self.final_checkpoint = final_checkpoint
        self.calls: list[str] = []

    def freeze_source_writes(self, *, run: MigrationRun) -> None:
        self.calls.append("freeze_source_writes")
        if self.fail_on == "freeze_source_writes":
            raise RuntimeError("Injected freeze failure")

    def perform_final_sync(self, *, run: MigrationRun) -> str | None:
        self.calls.append("perform_final_sync")
        if self.fail_on == "perform_final_sync":
            raise RuntimeError("Injected final sync failure")
        return self.final_checkpoint

    def mark_target_as_source_of_truth(self, *, run: MigrationRun) -> None:
        self.calls.append("mark_target_as_source_of_truth")
        if self.fail_on == "mark_target_as_source_of_truth":
            raise RuntimeError("Injected source-of-truth failure")


class FakeFinalValidationPackRunner:
    """In-memory final validation pack runner fake for tests."""

    def __init__(self, *, should_pass: bool = True):
        self.should_pass = should_pass
        self.calls = 0

    def run_final_validation_pack(self, *, run: MigrationRun) -> bool:
        self.calls += 1
        return self.should_pass


def _backfill_complete(run: MigrationRun) -> bool:
    if not run.table_progress:
        return False
    return all(item.status == TableExecutionStatus.COMPLETED for item in run.table_progress)
