"""CDC execution scaffolding for migration catch-up orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sdk.connectors import ResolvedConnectionSettings, configure_adapter_connections
from sdk.observability import EventCollector
from sdk.state.models import (
    CDCCatchupReadiness,
    CDCJobStatus,
    CDCLagMetrics,
    CDCTableProgress,
    CDCTableStatus,
    MigrationRun,
    MigrationRunStatus,
    utc_now_iso,
)
from sdk.state.store import MigrationRunStore


@dataclass(frozen=True)
class CDCStartResult:
    """Adapter response for starting CDC on a single table."""

    table_name: str
    job_id: str
    checkpoint: str | None = None
    watermark: str | None = None


@dataclass(frozen=True)
class CDCLagSnapshot:
    """Adapter response for current table lag and checkpoint."""

    table_name: str
    job_id: str
    lag_seconds: float
    source_freshness_seconds: float | None = None
    checkpoint: str | None = None
    watermark: str | None = None
    healthy: bool = True


class CDCAdapter(Protocol):
    """Vendor-neutral CDC control-plane abstraction."""

    def start_replication(self, *, table_name: str, checkpoint: str | None = None) -> CDCStartResult:
        """Start CDC for one table and return job metadata."""

    def get_replication_lag(self, *, table_name: str, job_id: str) -> CDCLagSnapshot:
        """Get current lag and freshness metrics for one replication job."""

    def stop_replication(self, *, table_name: str, job_id: str) -> None:
        """Stop CDC for one table."""


@dataclass(frozen=True)
class CDCGate:
    """Lag-based gate for determining catch-up readiness."""

    lag_threshold_seconds: float = 30.0
    stabilization_samples: int = 2


class CDCSyncService:
    """Resumable CDC starter/monitor for cutover readiness gating."""

    def __init__(
        self,
        *,
        adapter: CDCAdapter,
        store: MigrationRunStore,
        collector: EventCollector | None = None,
        resolved_connections: ResolvedConnectionSettings | None = None,
        gate: CDCGate | None = None,
    ):
        self._adapter = adapter
        self._store = store
        self._collector = collector
        self._resolved_connections = resolved_connections
        self._gate = gate or CDCGate()

    def initialize(self, *, run: MigrationRun, table_names: list[str]) -> MigrationRun:
        """Start CDC jobs for selected tables and persist run state."""
        if self._resolved_connections is not None:
            configure_adapter_connections(adapter=self._adapter, settings=self._resolved_connections)
            
        if run.status == MigrationRunStatus.FAILED:
            raise RuntimeError("Cannot initialize CDC on a failed migration run")

        if run.status == MigrationRunStatus.VALIDATION_PASSED:
            run.transition_to(MigrationRunStatus.SYNCING)
        elif run.status not in {MigrationRunStatus.SYNCING, MigrationRunStatus.CUTOVER_READY}:
            raise RuntimeError(f"Run {run.run_id} is not ready for CDC initialization")

        if run.cdc_status == CDCJobStatus.NOT_STARTED:
            run.cdc_status = CDCJobStatus.STARTING
            run.cdc_started_at = utc_now_iso()
        self._store.save(run)
        self._emit("cdc_started", "started", run, {"run_id": run.run_id, "table_count": len(table_names)})

        progress_by_table = {item.table_name: item for item in run.cdc_table_progress}
        for table_name in table_names:
            progress = progress_by_table.get(table_name)
            if progress is None:
                progress = CDCTableProgress(
                    table_name=table_name,
                    lag=CDCLagMetrics(observed_at=utc_now_iso()),
                    readiness=CDCCatchupReadiness(
                        lag_threshold_seconds=self._gate.lag_threshold_seconds,
                        stabilization_samples_required=self._gate.stabilization_samples,
                    ),
                )
                run.cdc_table_progress.append(progress)
                progress_by_table[table_name] = progress
            else:
                progress.readiness.lag_threshold_seconds = self._gate.lag_threshold_seconds
                progress.readiness.stabilization_samples_required = self._gate.stabilization_samples

            if progress.status in {CDCTableStatus.REPLICATING, CDCTableStatus.CAUGHT_UP}:
                continue

            progress.status = CDCTableStatus.STARTING
            progress.error_message = None
            if progress.started_at is None:
                progress.started_at = utc_now_iso()

            result = self._adapter.start_replication(table_name=table_name, checkpoint=progress.checkpoint)
            progress.job_id = result.job_id
            progress.checkpoint = result.checkpoint
            progress.watermark = result.watermark
            progress.status = CDCTableStatus.REPLICATING
            progress.updated_at = utc_now_iso()
            self._store.save(run)
            self._emit(
                "table_cdc_started",
                "running",
                run,
                {"run_id": run.run_id, "table": table_name, "job_id": result.job_id},
            )

        run.cdc_status = CDCJobStatus.RUNNING
        run.update_cutover_readiness()
        return self._store.save(run)

    def monitor_once(self, *, run: MigrationRun, table_names: list[str] | None = None) -> MigrationRun:
        """Poll lag once, persist metrics, and update catch-up readiness."""
        if run.cdc_status not in {CDCJobStatus.RUNNING, CDCJobStatus.DEGRADED}:
            raise RuntimeError(f"Run {run.run_id} CDC is not active")

        selected = set(table_names or [item.table_name for item in run.cdc_table_progress])
        max_lag: float | None = None
        max_freshness: float | None = None

        for progress in run.cdc_table_progress:
            if progress.table_name not in selected or not progress.job_id:
                continue
            if progress.status in {CDCTableStatus.FAILED, CDCTableStatus.STOPPED}:
                continue

            try:
                lag = self._adapter.get_replication_lag(table_name=progress.table_name, job_id=progress.job_id)
            except Exception as exc:
                progress.status = CDCTableStatus.FAILED
                progress.error_message = str(exc)
                progress.updated_at = utc_now_iso()
                run.cdc_status = CDCJobStatus.FAILED
                self._store.save(run)
                self._emit(
                    "cdc_failed",
                    "failed",
                    run,
                    {"run_id": run.run_id, "table": progress.table_name, "error": str(exc)},
                )
                raise RuntimeError(f"CDC lag polling failed for {progress.table_name}: {exc}") from exc

            progress.lag = CDCLagMetrics(
                lag_seconds=lag.lag_seconds,
                source_freshness_seconds=lag.source_freshness_seconds,
                observed_at=utc_now_iso(),
            )
            progress.checkpoint = lag.checkpoint or progress.checkpoint
            progress.watermark = lag.watermark or progress.watermark
            progress.updated_at = utc_now_iso()

            if lag.lag_seconds <= progress.readiness.lag_threshold_seconds:
                progress.readiness.stabilization_samples_met += 1
            else:
                progress.readiness.stabilization_samples_met = 0
                progress.readiness.ready = False
                progress.caught_up_at = None

            if (
                progress.readiness.stabilization_samples_met
                >= progress.readiness.stabilization_samples_required
                and not progress.readiness.ready
            ):
                progress.readiness.ready = True
                progress.readiness.ready_at = utc_now_iso()
                progress.status = CDCTableStatus.CAUGHT_UP
                progress.caught_up_at = progress.readiness.ready_at
                self._emit(
                    "catchup_reached",
                    "completed",
                    run,
                    {
                        "run_id": run.run_id,
                        "table": progress.table_name,
                        "lag_seconds": lag.lag_seconds,
                        "checkpoint": progress.checkpoint,
                    },
                )
            elif progress.status != CDCTableStatus.CAUGHT_UP:
                progress.status = CDCTableStatus.REPLICATING

            max_lag = lag.lag_seconds if max_lag is None else max(max_lag, lag.lag_seconds)
            if lag.source_freshness_seconds is not None:
                max_freshness = (
                    lag.source_freshness_seconds
                    if max_freshness is None
                    else max(max_freshness, lag.source_freshness_seconds)
                )

            self._emit(
                "lag_observed",
                "running",
                run,
                {
                    "run_id": run.run_id,
                    "table": progress.table_name,
                    "lag_seconds": lag.lag_seconds,
                    "source_freshness_seconds": lag.source_freshness_seconds,
                },
            )

        run.replication_lag_seconds = max_lag
        run.source_freshness_seconds = max_freshness
        run.replication_checkpoint = _max_checkpoint(run.cdc_table_progress)

        if any(item.status == CDCTableStatus.FAILED for item in run.cdc_table_progress):
            run.cdc_status = CDCJobStatus.FAILED
        elif all(item.readiness.ready for item in run.cdc_table_progress):
            run.cdc_status = CDCJobStatus.RUNNING
        elif any(item.lag.lag_seconds and item.lag.lag_seconds > self._gate.lag_threshold_seconds for item in run.cdc_table_progress):
            run.cdc_status = CDCJobStatus.DEGRADED
        else:
            run.cdc_status = CDCJobStatus.RUNNING

        run.update_cutover_readiness()
        if run.cutover_ready and run.status == MigrationRunStatus.SYNCING:
            run.transition_to(MigrationRunStatus.CUTOVER_READY)
        return self._store.save(run)

    def stop(self, *, run: MigrationRun, table_names: list[str] | None = None) -> MigrationRun:
        """Stop active CDC jobs and persist terminal CDC state."""
        selected = set(table_names or [item.table_name for item in run.cdc_table_progress])
        for progress in run.cdc_table_progress:
            if progress.table_name not in selected or not progress.job_id:
                continue
            if progress.status == CDCTableStatus.STOPPED:
                continue
            self._adapter.stop_replication(table_name=progress.table_name, job_id=progress.job_id)
            progress.status = CDCTableStatus.STOPPED
            progress.updated_at = utc_now_iso()

        run.cdc_status = CDCJobStatus.STOPPED
        run.update_cutover_readiness()
        self._store.save(run)
        self._emit("cdc_stopped", "completed", run, {"run_id": run.run_id, "tables": sorted(selected)})
        return run

    def _emit(self, event_type: str, status: str, run: MigrationRun, payload: dict[str, object]) -> None:
        if not self._collector:
            return
        self._collector.emit(
            event_type=event_type,
            step="cdc_sync_service",
            status=status,
            payload=payload,
        )


class FakeCDCAdapter:
    """In-memory fake CDC adapter for tests and local simulation."""

    def __init__(self, lag_sequences: dict[str, list[float]], failing_tables: set[str] | None = None):
        self._lag_sequences = {table: list(seq) for table, seq in lag_sequences.items()}
        self._failing_tables = set(failing_tables or set())
        self._jobs: dict[str, str] = {}
        self._stopped: set[str] = set()

    def start_replication(self, *, table_name: str, checkpoint: str | None = None) -> CDCStartResult:
        job_id = f"job-{table_name}"
        self._jobs[table_name] = job_id
        return CDCStartResult(table_name=table_name, job_id=job_id, checkpoint=checkpoint, watermark=checkpoint)

    def get_replication_lag(self, *, table_name: str, job_id: str) -> CDCLagSnapshot:
        if table_name in self._failing_tables:
            raise RuntimeError(f"Injected CDC failure for {table_name}")
        if self._jobs.get(table_name) != job_id:
            raise RuntimeError(f"Unknown CDC job for {table_name}")

        sequence = self._lag_sequences.setdefault(table_name, [0.0])
        lag_seconds = sequence.pop(0) if sequence else 0.0
        checkpoint = f"{table_name}:{max(int(1000 - lag_seconds), 0)}"
        return CDCLagSnapshot(
            table_name=table_name,
            job_id=job_id,
            lag_seconds=lag_seconds,
            source_freshness_seconds=lag_seconds,
            checkpoint=checkpoint,
            watermark=checkpoint,
            healthy=True,
        )

    def stop_replication(self, *, table_name: str, job_id: str) -> None:
        if self._jobs.get(table_name) != job_id:
            raise RuntimeError(f"Unknown CDC job for {table_name}")
        self._stopped.add(table_name)


def _max_checkpoint(progress_items: list[CDCTableProgress]) -> str | None:
    checkpoints = sorted(item.checkpoint for item in progress_items if item.checkpoint)
    return checkpoints[-1] if checkpoints else None
