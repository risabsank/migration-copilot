"""Scheduler integration scaffolding for orchestrator-backed migration runs."""

from __future__ import annotations

from dataclasses import dataclass

from sdk.orchestration.models import OrchestrationFinalStatus
from sdk.orchestration.service import MigrationOrchestrator
from sdk.state.models import MigrationRun, OrchestrationPhase, utc_now_iso

_PHASE_ORDER: tuple[OrchestrationPhase, ...] = (
    OrchestrationPhase.PLAN_READY,
    OrchestrationPhase.PROVISIONING,
    OrchestrationPhase.PREFLIGHT_VALIDATION,
    OrchestrationPhase.BACKFILL,
    OrchestrationPhase.POST_BACKFILL_VALIDATION,
    OrchestrationPhase.CDC_START,
    OrchestrationPhase.CDC_CATCHUP,
    OrchestrationPhase.CUTOVER_PRECHECK,
    OrchestrationPhase.CUTOVER,
    OrchestrationPhase.POST_CUTOVER_VALIDATION,
)


@dataclass(frozen=True)
class SchedulerTaskSpec:
    """Task descriptor emitted for external scheduler integration layers."""

    task_id: str
    phase: OrchestrationPhase
    upstream_task_ids: list[str]
    operator: str
    metadata: dict[str, object]


class PhaseTaskMapper:
    """Translate persisted orchestration phase state into scheduler task boundaries."""

    def remaining_phases(self, run: MigrationRun) -> list[OrchestrationPhase]:
        if run.orchestration_phase in {OrchestrationPhase.COMPLETED, OrchestrationPhase.ROLLBACK}:
            return []

        start_index = _PHASE_ORDER.index(run.orchestration_phase)
        return list(_PHASE_ORDER[start_index:])

    def map_run_to_tasks(self, run: MigrationRun, *, scheduler: str) -> list[SchedulerTaskSpec]:
        phases = self.remaining_phases(run)
        operator = self._operator_for_scheduler(scheduler)
        previous_task_id: str | None = None
        specs: list[SchedulerTaskSpec] = []

        for phase in phases:
            task_id = f"migration_{run.run_id}_{phase.value}"
            upstream = [previous_task_id] if previous_task_id else []
            specs.append(
                SchedulerTaskSpec(
                    task_id=task_id,
                    phase=phase,
                    upstream_task_ids=upstream,
                    operator=operator,
                    metadata={
                        "run_id": run.run_id,
                        "plan_id": run.plan_id,
                        "selected_variant": run.selected_variant,
                        "phase": phase.value,
                        "resume_from_checkpoint": run.last_checkpoint,
                        "completed_phases": list(run.completed_phases),
                        "is_resume": bool(run.completed_phases),
                        "generated_at": utc_now_iso(),
                    },
                )
            )
            previous_task_id = task_id
        return specs

    @staticmethod
    def _operator_for_scheduler(scheduler: str) -> str:
        normalized = scheduler.lower()
        if normalized == "airflow":
            return "PythonOperator"
        if normalized == "dagster":
            return "op"
        raise ValueError(f"Unsupported scheduler '{scheduler}'. Expected 'airflow' or 'dagster'.")


def generate_airflow_task_defs(run: MigrationRun, *, dag_id: str) -> dict[str, object]:
    """Build a serializable Airflow-oriented DAG definition from run state."""
    mapper = PhaseTaskMapper()
    tasks = mapper.map_run_to_tasks(run, scheduler="airflow")
    return {
        "dag_id": dag_id,
        "default_args": {"owner": "migration-control-plane", "depends_on_past": False},
        "tasks": [
            {
                "task_id": task.task_id,
                "operator": task.operator,
                "upstream_task_ids": task.upstream_task_ids,
                "phase": task.phase.value,
                "callable": "sdk.orchestration.scheduler_integration.execute_scheduler_task",
                "op_kwargs": {"run_id": run.run_id, "phase": task.phase.value},
                "metadata": task.metadata,
            }
            for task in tasks
        ],
    }


def generate_dagster_job_defs(run: MigrationRun, *, job_name: str) -> dict[str, object]:
    """Build a serializable Dagster-oriented job definition from run state."""
    mapper = PhaseTaskMapper()
    tasks = mapper.map_run_to_tasks(run, scheduler="dagster")
    return {
        "job_name": job_name,
        "ops": [
            {
                "name": task.task_id,
                "phase": task.phase.value,
                "required_resource_keys": ["migration_orchestrator"],
                "inputs": task.upstream_task_ids,
                "config": {"run_id": run.run_id, "phase": task.phase.value},
                "metadata": task.metadata,
            }
            for task in tasks
        ],
    }


class SchedulerTaskExecutor:
    """Entry point invoked by scheduler tasks to re-enter internal orchestrator."""

    def __init__(self, orchestrator: MigrationOrchestrator):
        self._orchestrator = orchestrator

    def execute(self, *, run_id: str, phase: OrchestrationPhase) -> dict[str, object]:
        run_before = self._get_run(run_id)
        if phase.value in run_before.completed_phases:
            return {
                "run_id": run_id,
                "phase": phase.value,
                "status": "skipped_already_completed",
                "completed_phases": list(run_before.completed_phases),
                "checkpoint": run_before.last_checkpoint,
            }

        if run_before.orchestration_phase != phase:
            raise ValueError(
                f"Task phase mismatch for run {run_id}: expected {run_before.orchestration_phase.value}, got {phase.value}"
            )

        result = self._orchestrator.run(run_id=run_id, max_phases=1)
        run_after = self._get_run(run_id)
        return {
            "run_id": run_id,
            "phase": phase.value,
            "status": result.final_status.value,
            "completed_phases": list(run_after.completed_phases),
            "checkpoint": run_after.last_checkpoint,
            "next_phase": run_after.orchestration_phase.value,
            "failed_phase": run_after.failed_phase,
            "terminal": result.final_status in {
                OrchestrationFinalStatus.COMPLETED,
                OrchestrationFinalStatus.FAILED,
            },
        }

    def _get_run(self, run_id: str) -> MigrationRun:
        run = self._orchestrator.store.get(run_id)
        if run is None:
            raise ValueError(f"Migration run {run_id} was not found")
        return run


def execute_scheduler_task(*, orchestrator: MigrationOrchestrator, run_id: str, phase: str) -> dict[str, object]:
    """Convenience wrapper compatible with scheduler task callables."""
    return SchedulerTaskExecutor(orchestrator).execute(run_id=run_id, phase=OrchestrationPhase(phase))
