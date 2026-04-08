"""Deterministic backfill execution scaffolding."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sdk.engine.models import ResolvedTablePlan
from sdk.observability import EventCollector
from sdk.state.models import MigrationRun, MigrationRunStatus, TableExecutionProgress, TableExecutionStatus, utc_now_iso
from sdk.state.store import MigrationRunStore


@dataclass(frozen=True)
class BackfillChunkResult:
    """Result for one backfill chunk attempt."""

    rows_copied: int
    checkpoint: str | None
    watermark: str | None = None
    completed: bool = False


class BackfillExecutionAdapter(Protocol):
    """Adapter contract for table backfill execution against any warehouse."""

    def estimate_total_rows(self, *, table_name: str) -> int | None:
        """Return an estimated total row count for progress reporting."""

    def run_backfill_chunk(
        self,
        *,
        table_name: str,
        chunk_size_rows: int,
        checkpoint: str | None,
    ) -> BackfillChunkResult:
        """Execute one deterministic chunk for a table from a checkpoint."""


class BackfillExecutor:
    """Deterministic table backfill executor with resumable state transitions."""

    def __init__(
        self,
        *,
        adapter: BackfillExecutionAdapter,
        store: MigrationRunStore,
        collector: EventCollector | None = None,
    ):
        self._adapter = adapter
        self._store = store
        self._collector = collector

    def execute(
        self,
        *,
        run: MigrationRun,
        table_plans: list[ResolvedTablePlan],
        retry_failed: bool = False,
    ) -> MigrationRun:
        """Execute pending table backfills in deterministic dependency order."""
        if run.status == MigrationRunStatus.FAILED:
            raise RuntimeError("Cannot execute a failed migration run without explicit recovery")

        self._ensure_backfill_status(run)
        self._emit(
            event_type="execution_started",
            step="backfill_executor",
            status="started",
            payload={"run_id": run.run_id, "table_count": len(table_plans)},
        )

        progress_by_table = {item.table_name: item for item in run.table_progress}
        ordered_plans = sorted(table_plans, key=lambda item: (item.execution_order, item.table_name))

        try:
            for plan in ordered_plans:
                progress = progress_by_table.get(plan.table_name)
                if progress is None:
                    progress = TableExecutionProgress(table_name=plan.table_name)
                    run.table_progress.append(progress)
                    progress_by_table[plan.table_name] = progress

                if progress.status == TableExecutionStatus.COMPLETED:
                    continue
                if progress.status == TableExecutionStatus.FAILED and not retry_failed:
                    continue

                try:
                    self._run_table_backfill(run=run, plan=plan, progress=progress)
                except Exception as exc:
                    progress.status = TableExecutionStatus.FAILED
                    progress.error_message = str(exc)
                    progress.updated_at = utc_now_iso()
                    self._store.save(run)
                    raise

            if all(item.status == TableExecutionStatus.COMPLETED for item in run.table_progress):
                run.transition_to(MigrationRunStatus.VALIDATING)
            return self._store.save(run)

        except Exception as exc:
            run.transition_to(MigrationRunStatus.FAILED)
            self._store.save(run)
            self._emit(
                event_type="execution_failed",
                step="backfill_executor",
                status="failed",
                payload={"run_id": run.run_id, "error": str(exc)},
            )
            raise RuntimeError(f"Backfill execution failed for run {run.run_id}: {exc}") from exc

    def _ensure_backfill_status(self, run: MigrationRun) -> None:
        if run.status == MigrationRunStatus.PROVISIONING:
            run.transition_to(MigrationRunStatus.BACKFILLING)
        elif run.status == MigrationRunStatus.APPROVED:
            run.transition_to(MigrationRunStatus.PROVISIONING)
            run.transition_to(MigrationRunStatus.BACKFILLING)
        elif run.status == MigrationRunStatus.DRAFTED:
            run.transition_to(MigrationRunStatus.APPROVED)
            run.transition_to(MigrationRunStatus.PROVISIONING)
            run.transition_to(MigrationRunStatus.BACKFILLING)

        self._store.save(run)

    def _run_table_backfill(
        self,
        *,
        run: MigrationRun,
        plan: ResolvedTablePlan,
        progress: TableExecutionProgress,
    ) -> None:
        progress.status = TableExecutionStatus.RUNNING
        progress.error_message = None
        if progress.started_at is None:
            progress.started_at = utc_now_iso()
        progress.updated_at = utc_now_iso()
        self._store.save(run)

        self._emit(
            event_type="table_started",
            step="backfill_executor",
            status="running",
            payload={"run_id": run.run_id, "table": plan.table_name, "checkpoint": progress.checkpoint},
        )

        total_rows = self._adapter.estimate_total_rows(table_name=plan.table_name)
        while progress.status == TableExecutionStatus.RUNNING:
            chunk_result = self._adapter.run_backfill_chunk(
                table_name=plan.table_name,
                chunk_size_rows=plan.chunk_size_rows,
                checkpoint=progress.checkpoint,
            )
            progress.rows_copied += max(chunk_result.rows_copied, 0)
            progress.chunks_completed += 1
            progress.checkpoint = chunk_result.checkpoint
            progress.watermark = chunk_result.watermark
            progress.updated_at = utc_now_iso()
            if total_rows and total_rows > 0:
                progress.progress_percent = min(100.0, round((progress.rows_copied / total_rows) * 100, 2))

            run.last_checkpoint = progress.checkpoint
            run.last_watermark = progress.watermark
            self._store.save(run)

            self._emit(
                event_type="chunk_completed",
                step="backfill_executor",
                status="running",
                payload={
                    "run_id": run.run_id,
                    "table": plan.table_name,
                    "rows_copied": progress.rows_copied,
                    "checkpoint": progress.checkpoint,
                    "completed": chunk_result.completed,
                },
            )

            if chunk_result.completed:
                progress.status = TableExecutionStatus.COMPLETED
                progress.progress_percent = 100.0
                progress.completed_at = utc_now_iso()
                progress.updated_at = utc_now_iso()
                self._store.save(run)
                self._emit(
                    event_type="table_completed",
                    step="backfill_executor",
                    status="completed",
                    payload={"run_id": run.run_id, "table": plan.table_name, "rows_copied": progress.rows_copied},
                )

    def _emit(self, *, event_type: str, step: str, status: str, payload: dict[str, object]) -> None:
        if not self._collector:
            return
        self._collector.emit(event_type=event_type, step=step, status=status, payload=payload)


class SimulatedBackfillExecutionAdapter:
    """In-memory deterministic adapter for dry-run and unit testing."""

    def __init__(self, table_totals: dict[str, int]):
        self._table_totals = dict(table_totals)

    def estimate_total_rows(self, *, table_name: str) -> int | None:
        return self._table_totals.get(table_name)

    def run_backfill_chunk(
        self,
        *,
        table_name: str,
        chunk_size_rows: int,
        checkpoint: str | None,
    ) -> BackfillChunkResult:
        total = self._table_totals.get(table_name, 0)
        done_so_far = int(checkpoint or "0")
        if done_so_far >= total:
            return BackfillChunkResult(rows_copied=0, checkpoint=str(total), completed=True)

        next_done = min(total, done_so_far + chunk_size_rows)
        copied = max(0, next_done - done_so_far)
        return BackfillChunkResult(
            rows_copied=copied,
            checkpoint=str(next_done),
            completed=next_done >= total,
        )
