"""State models for persisted migration run execution."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from sdk.engine.models import MigrationPattern


class MigrationRunStatus(str, Enum):
    """Lifecycle states for a migration execution run."""

    DRAFTED = "drafted"
    APPROVED = "approved"
    PROVISIONING = "provisioning"
    BACKFILLING = "backfilling"
    VALIDATING = "validating"
    VALIDATION_PASSED = "validation_passed"
    VALIDATION_FAILED = "validation_failed"
    SYNCING = "syncing"
    CUTOVER_READY = "cutover_ready"
    CUTOVER_COMPLETE = "cutover_complete"
    ROLLBACK_IN_PROGRESS = "rollback_in_progress"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


class TableExecutionStatus(str, Enum):
    """Execution status for a single table in a migration run."""

    PENDING = "pending"
    RUNNING = "running"
    VALIDATED = "validated"
    COMPLETED = "completed"
    FAILED = "failed"


class MigrationPhase(str, Enum):
    """High-level migration phase for operator visibility."""

    PREPARE = "prepare"
    PROVISION = "provision"
    BACKFILL = "backfill"
    VALIDATE = "validate"
    SYNC = "sync"
    CUTOVER = "cutover"
    ROLLBACK = "rollback"
    COMPLETE = "complete"

class OrchestrationPhase(str, Enum):
    """Ordered orchestration phases for full migration workflow."""

    PLAN_READY = "plan_ready"
    PROVISIONING = "provisioning"
    PREFLIGHT_VALIDATION = "preflight_validation"
    BACKFILL = "backfill"
    POST_BACKFILL_VALIDATION = "post_backfill_validation"
    CDC_START = "cdc_start"
    CDC_CATCHUP = "cdc_catchup"
    CUTOVER_PRECHECK = "cutover_precheck"
    CUTOVER = "cutover"
    POST_CUTOVER_VALIDATION = "post_cutover_validation"
    COMPLETED = "completed"
    ROLLBACK = "rollback"

class ValidationStatus(str, Enum):
    """Validation status for the full migration run."""

    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    PASSED = "passed"
    FAILED = "failed"

class ValidationCheckStatus(str, Enum):
    """Execution status for one validation check."""

    PASSED = "passed"
    FAILED = "failed"
    UNKNOWN = "unknown"

class CDCJobStatus(str, Enum):
    """Lifecycle status for an external CDC replication job."""

    NOT_STARTED = "not_started"
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    FAILED = "failed"
    STOPPED = "stopped"

class CutoverExecutionStatus(str, Enum):
    """Lifecycle status for controlled cutover execution."""

    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

class RollbackStatus(str, Enum):
    """Lifecycle status for rollback planning and execution."""

    NOT_PLANNED = "not_planned"
    READY = "ready"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"

class RollbackTriggerReason(str, Enum):
    """Reason rollback execution was initiated."""

    CUTOVER_FAILED = "cutover_failed"
    POST_CUTOVER_VALIDATION_FAILED = "post_cutover_validation_failed"
    PARTIAL_CUTOVER_DETECTED = "partial_cutover_detected"
    OPERATOR_REQUESTED = "operator_requested"

class CDCTableStatus(str, Enum):
    """Table-level CDC progress state."""

    PENDING = "pending"
    STARTING = "starting"
    REPLICATING = "replicating"
    CAUGHT_UP = "caught_up"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass
class CDCLagMetrics:
    """Replication lag and freshness telemetry for one table."""

    lag_seconds: float | None = None
    source_freshness_seconds: float | None = None
    observed_at: str = field(default_factory=lambda: utc_now_iso())

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CDCCatchupReadiness:
    """Catch-up gate result at table level."""

    lag_threshold_seconds: float
    stabilization_samples_required: int = 1
    stabilization_samples_met: int = 0
    ready: bool = False
    ready_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CDCTableProgress:
    """Mutable table-level CDC replication state."""

    table_name: str
    status: CDCTableStatus = CDCTableStatus.PENDING
    job_id: str | None = None
    lag: CDCLagMetrics = field(default_factory=CDCLagMetrics)
    readiness: CDCCatchupReadiness = field(default_factory=lambda: CDCCatchupReadiness(lag_threshold_seconds=30.0))
    checkpoint: str | None = None
    watermark: str | None = None
    error_message: str | None = None
    started_at: str | None = None
    caught_up_at: str | None = None
    updated_at: str = field(default_factory=lambda: utc_now_iso())

    def as_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "status": self.status.value,
            "job_id": self.job_id,
            "lag": self.lag.as_dict(),
            "readiness": self.readiness.as_dict(),
            "checkpoint": self.checkpoint,
            "watermark": self.watermark,
            "error_message": self.error_message,
            "started_at": self.started_at,
            "caught_up_at": self.caught_up_at,
            "updated_at": self.updated_at,
        }

@dataclass
class ValidationCheck:
    """Result for one executable validation SQL check."""

    check_name: str
    query: str
    status: ValidationCheckStatus
    table_name: str | None = None
    source_value: float | None = None
    target_value: float | None = None
    difference: float | None = None
    threshold: float = 0.0
    details: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "check_name": self.check_name,
            "query": self.query,
            "status": self.status.value,
            "table_name": self.table_name,
            "source_value": self.source_value,
            "target_value": self.target_value,
            "difference": self.difference,
            "threshold": self.threshold,
            "details": self.details,
        }


@dataclass
class ValidationResult:
    """Grouped validation result for a single table."""

    table_name: str
    checks: list[ValidationCheck] = field(default_factory=list)

    @property
    def status(self) -> ValidationCheckStatus:
        statuses = {check.status for check in self.checks}
        if not statuses:
            return ValidationCheckStatus.UNKNOWN
        if ValidationCheckStatus.FAILED in statuses:
            return ValidationCheckStatus.FAILED
        if statuses == {ValidationCheckStatus.PASSED}:
            return ValidationCheckStatus.PASSED
        return ValidationCheckStatus.UNKNOWN

    def as_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "status": self.status.value,
            "checks": [check.as_dict() for check in self.checks],
        }


@dataclass
class ValidationSummary:
    """Aggregate validation execution summary at migration level."""

    status: ValidationCheckStatus = ValidationCheckStatus.UNKNOWN
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    unknown_checks: int = 0
    table_results: list[ValidationResult] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "total_checks": self.total_checks,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "unknown_checks": self.unknown_checks,
            "table_results": [item.as_dict() for item in self.table_results],
        }

@dataclass
class CutoverGateEvaluation:
    """Persisted cutover gate check result."""

    ready: bool = False
    blocking_conditions: list[str] = field(default_factory=list)
    advisory_warnings: list[str] = field(default_factory=list)
    evaluated_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CutoverExecutionState:
    """Persisted timeline and checkpoints for cutover sequence execution."""

    status: CutoverExecutionStatus = CutoverExecutionStatus.NOT_STARTED
    started_at: str | None = None
    freeze_writes_at: str | None = None
    final_sync_completed_at: str | None = None
    final_validation_completed_at: str | None = None
    completed_at: str | None = None
    failed_at: str | None = None
    final_checkpoint: str | None = None
    operator_notes: list[str] = field(default_factory=list)
    hook_trace: list[str] = field(default_factory=list)
    error_message: str | None = None
    recovery_path: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "started_at": self.started_at,
            "freeze_writes_at": self.freeze_writes_at,
            "final_sync_completed_at": self.final_sync_completed_at,
            "final_validation_completed_at": self.final_validation_completed_at,
            "completed_at": self.completed_at,
            "failed_at": self.failed_at,
            "final_checkpoint": self.final_checkpoint,
            "operator_notes": list(self.operator_notes),
            "hook_trace": list(self.hook_trace),
            "error_message": self.error_message,
            "recovery_path": self.recovery_path,
        }

@dataclass
class RollbackStep:
    """Deterministic rollback step progress and checkpoints."""

    step_id: str
    description: str
    status: RollbackStatus = RollbackStatus.READY
    started_at: str | None = None
    completed_at: str | None = None
    checkpoint: str | None = None
    details: str | None = None
    error_message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "description": self.description,
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "checkpoint": self.checkpoint,
            "details": self.details,
            "error_message": self.error_message,
        }


@dataclass
class RollbackPlan:
    """Persisted rollback planning and execution audit state."""

    status: RollbackStatus = RollbackStatus.NOT_PLANNED
    trigger_reason: RollbackTriggerReason | None = None
    initiated_at: str | None = None
    completed_at: str | None = None
    failed_at: str | None = None
    resumed_count: int = 0
    active_step_id: str | None = None
    summary: str | None = None
    operator_summary: str | None = None
    steps: list[RollbackStep] = field(default_factory=list)
    checkpoints: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "trigger_reason": self.trigger_reason.value if self.trigger_reason else None,
            "initiated_at": self.initiated_at,
            "completed_at": self.completed_at,
            "failed_at": self.failed_at,
            "resumed_count": self.resumed_count,
            "active_step_id": self.active_step_id,
            "summary": self.summary,
            "operator_summary": self.operator_summary,
            "steps": [step.as_dict() for step in self.steps],
            "checkpoints": list(self.checkpoints),
        }


@dataclass
class RollbackReadinessState:
    """Persisted evidence that rollback can be safely executed."""

    ready: bool = False
    status: RollbackStatus = RollbackStatus.NOT_PLANNED
    established_at: str | None = None
    strategy: str | None = None
    checklist: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "status": self.status.value,
            "established_at": self.established_at,
            "strategy": self.strategy,
            "checklist": list(self.checklist),
            "notes": list(self.notes),
        }

@dataclass
class TableExecutionProgress:
    """Mutable state for a table-level migration execution."""

    table_name: str
    status: TableExecutionStatus = TableExecutionStatus.PENDING
    progress_percent: float = 0.0
    rows_copied: int = 0
    checkpoint: str | None = None
    watermark: str | None = None
    error_message: str | None = None
    chunks_completed: int = 0
    started_at: str | None = None
    completed_at: str | None = None
    updated_at: str = field(default_factory=lambda: utc_now_iso())
    

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class MigrationRun:
    """Persisted execution state for a migration plan instance."""

    run_id: str
    plan_id: str
    schema: str
    selected_variant: str
    pattern: MigrationPattern
    status: MigrationRunStatus = MigrationRunStatus.DRAFTED
    phase: MigrationPhase = MigrationPhase.PREPARE
    validation_status: ValidationStatus = ValidationStatus.NOT_STARTED
    cutover_ready: bool = False
    rollback_ready: bool = False
    validation_summary: ValidationSummary = field(default_factory=ValidationSummary)
    table_progress: list[TableExecutionProgress] = field(default_factory=list)
    orchestration_phase: OrchestrationPhase = OrchestrationPhase.PLAN_READY
    completed_phases: list[str] = field(default_factory=list)
    pause_requested: bool = False
    paused: bool = False
    failed_phase: str | None = None
    last_checkpoint: str | None = None
    last_watermark: str | None = None
    created_at: str = field(default_factory=lambda: utc_now_iso())
    updated_at: str = field(default_factory=lambda: utc_now_iso())
    cdc_started_at: str | None = None
    cdc_status: CDCJobStatus = CDCJobStatus.NOT_STARTED
    cdc_table_progress: list[CDCTableProgress] = field(default_factory=list)
    replication_lag_seconds: float | None = None
    source_freshness_seconds: float | None = None
    replication_checkpoint: str | None = None
    unresolved_risk_flags: list[str] = field(default_factory=list)
    connector_config_metadata: dict[str, Any] = field(default_factory=dict)
    cutover_evaluation: CutoverGateEvaluation = field(default_factory=CutoverGateEvaluation)
    cutover_execution: CutoverExecutionState = field(default_factory=CutoverExecutionState)
    ops_recommendation_history: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def new(
        cls,
        *,
        plan_id: str,
        schema: str,
        selected_variant: str,
        pattern: MigrationPattern,
        table_names: list[str],
        run_id: str | None = None,
    ) -> "MigrationRun":
        """Create a draft migration run with table-level pending state."""
        now = utc_now_iso()
        return cls(
            run_id=run_id or str(uuid4()),
            plan_id=plan_id,
            schema=schema,
            selected_variant=selected_variant,
            pattern=pattern,
            table_progress=[TableExecutionProgress(table_name=name, updated_at=now) for name in table_names],
            cdc_table_progress=[
                CDCTableProgress(
                    table_name=name,
                    lag=CDCLagMetrics(observed_at=now),
                    readiness=CDCCatchupReadiness(lag_threshold_seconds=30.0),
                    updated_at=now,
                )
                for name in table_names
            ],
            created_at=now,
            updated_at=now,
        )

    def transition_to(self, next_status: MigrationRunStatus) -> None:
        """Transition this run to a valid next lifecycle state."""
        if next_status != self.status and next_status not in _ALLOWED_TRANSITIONS[self.status]:
            raise ValueError(f"Invalid state transition: {self.status.value} -> {next_status.value}")

        self.status = next_status
        self.phase = _STATUS_TO_PHASE[next_status]
        if next_status == MigrationRunStatus.CUTOVER_READY:
            self.cutover_ready = True
        if next_status in (MigrationRunStatus.APPROVED, MigrationRunStatus.PROVISIONING, MigrationRunStatus.BACKFILLING):
            self.rollback_ready = True
        self.touch()

    def touch(self) -> None:
        """Refresh updated timestamp."""
        self.updated_at = utc_now_iso()
    
    def update_cutover_readiness(self) -> bool:
        """Evaluate and persist whether this run is safe for cutover."""
        all_cdc_tables_ready = bool(self.cdc_table_progress) and all(
            item.readiness.ready for item in self.cdc_table_progress
        )
        validation_ready = self.validation_status == ValidationStatus.PASSED or self.status in {
            MigrationRunStatus.VALIDATION_PASSED,
            MigrationRunStatus.SYNCING,
            MigrationRunStatus.CUTOVER_READY,
        }
        self.cutover_ready = (
            validation_ready
            and self.cdc_status in {CDCJobStatus.RUNNING, CDCJobStatus.DEGRADED}
            and all_cdc_tables_ready
        )
        self.touch()
        return self.cutover_ready

    def as_dict(self) -> dict[str, Any]:
        """Serialize run state into a JSON-friendly dictionary."""
        return {
            "run_id": self.run_id,
            "plan_id": self.plan_id,
            "schema": self.schema,
            "selected_variant": self.selected_variant,
            "pattern": self.pattern.value,
            "status": self.status.value,
            "phase": self.phase.value,
            "validation_status": self.validation_status.value,
            "cutover_ready": self.cutover_ready,
            "rollback_ready": self.rollback_ready,
            "validation_summary": self.validation_summary.as_dict(),
            "table_progress": [item.as_dict() for item in self.table_progress],
            "orchestration_phase": self.orchestration_phase.value,
            "completed_phases": self.completed_phases,
            "pause_requested": self.pause_requested,
            "paused": self.paused,
            "failed_phase": self.failed_phase,
            "last_checkpoint": self.last_checkpoint,
            "last_watermark": self.last_watermark,
            "cdc_started_at": self.cdc_started_at,
            "cdc_status": self.cdc_status.value,
            "cdc_table_progress": [item.as_dict() for item in self.cdc_table_progress],
            "replication_lag_seconds": self.replication_lag_seconds,
            "source_freshness_seconds": self.source_freshness_seconds,
            "replication_checkpoint": self.replication_checkpoint,
            "unresolved_risk_flags": self.unresolved_risk_flags,
            "connector_config_metadata": self.connector_config_metadata,
            "cutover_evaluation": self.cutover_evaluation.as_dict(),
            "cutover_execution": self.cutover_execution.as_dict(),
            "ops_recommendation_history": self.ops_recommendation_history,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MigrationRun":
        """Hydrate a migration run from persisted dictionary content."""
        return cls(
            run_id=data["run_id"],
            plan_id=data["plan_id"],
            schema=data["schema"],
            selected_variant=data["selected_variant"],
            pattern=MigrationPattern(data["pattern"]),
            status=MigrationRunStatus(data["status"]),
            phase=MigrationPhase(data["phase"]),
            validation_status=ValidationStatus(data["validation_status"]),
            cutover_ready=data["cutover_ready"],
            rollback_ready=data["rollback_ready"],
            validation_summary=ValidationSummary(
                status=ValidationCheckStatus(data.get("validation_summary", {}).get("status", ValidationCheckStatus.UNKNOWN.value)),
                total_checks=data.get("validation_summary", {}).get("total_checks", 0),
                passed_checks=data.get("validation_summary", {}).get("passed_checks", 0),
                failed_checks=data.get("validation_summary", {}).get("failed_checks", 0),
                unknown_checks=data.get("validation_summary", {}).get("unknown_checks", 0),
                table_results=[
                    ValidationResult(
                        table_name=item["table_name"],
                        checks=[
                            ValidationCheck(
                                check_name=check["check_name"],
                                query=check.get("query", ""),
                                status=ValidationCheckStatus(check["status"]),
                                table_name=check.get("table_name"),
                                source_value=check.get("source_value"),
                                target_value=check.get("target_value"),
                                difference=check.get("difference"),
                                threshold=check.get("threshold", 0.0),
                                details=check.get("details"),
                            )
                            for check in item.get("checks", [])
                        ],
                    )
                    for item in data.get("validation_summary", {}).get("table_results", [])
                ],
            ),
            table_progress=[
                TableExecutionProgress(
                    table_name=item["table_name"],
                    status=TableExecutionStatus(item["status"]),
                    progress_percent=item["progress_percent"],
                    rows_copied=item["rows_copied"],
                    checkpoint=item.get("checkpoint"),
                    watermark=item.get("watermark"),
                    chunks_completed=item.get("chunks_completed", 0),
                    started_at=item.get("started_at"),
                    completed_at=item.get("completed_at"),
                    error_message=item.get("error_message"),
                    updated_at=item["updated_at"],
                )
                for item in data["table_progress"]
            ],
            orchestration_phase=OrchestrationPhase(
                data.get("orchestration_phase", OrchestrationPhase.PLAN_READY.value)
            ),
            completed_phases=list(data.get("completed_phases", [])),
            pause_requested=data.get("pause_requested", False),
            paused=data.get("paused", False),
            failed_phase=data.get("failed_phase"),
            last_checkpoint=data.get("last_checkpoint"),
            last_watermark=data.get("last_watermark"),
            cdc_started_at=data.get("cdc_started_at"),
            cdc_status=CDCJobStatus(data.get("cdc_status", CDCJobStatus.NOT_STARTED.value)),
            cdc_table_progress=[
                CDCTableProgress(
                    table_name=item["table_name"],
                    status=CDCTableStatus(item.get("status", CDCTableStatus.PENDING.value)),
                    job_id=item.get("job_id"),
                    lag=CDCLagMetrics(
                        lag_seconds=item.get("lag", {}).get("lag_seconds"),
                        source_freshness_seconds=item.get("lag", {}).get("source_freshness_seconds"),
                        observed_at=item.get("lag", {}).get("observed_at", utc_now_iso()),
                    ),
                    readiness=CDCCatchupReadiness(
                        lag_threshold_seconds=item.get("readiness", {}).get("lag_threshold_seconds", 30.0),
                        stabilization_samples_required=item.get("readiness", {}).get(
                            "stabilization_samples_required", 1
                        ),
                        stabilization_samples_met=item.get("readiness", {}).get("stabilization_samples_met", 0),
                        ready=item.get("readiness", {}).get("ready", False),
                        ready_at=item.get("readiness", {}).get("ready_at"),
                    ),
                    checkpoint=item.get("checkpoint"),
                    watermark=item.get("watermark"),
                    error_message=item.get("error_message"),
                    started_at=item.get("started_at"),
                    caught_up_at=item.get("caught_up_at"),
                    updated_at=item.get("updated_at", utc_now_iso()),
                )
                for item in data.get("cdc_table_progress", [])
            ],
            replication_lag_seconds=data.get("replication_lag_seconds"),
            source_freshness_seconds=data.get("source_freshness_seconds"),
            replication_checkpoint=data.get("replication_checkpoint"),
            unresolved_risk_flags=list(data.get("unresolved_risk_flags", [])),
            connector_config_metadata=dict(data.get("connector_config_metadata", {})),
            cutover_evaluation=CutoverGateEvaluation(
                ready=data.get("cutover_evaluation", {}).get("ready", False),
                blocking_conditions=list(data.get("cutover_evaluation", {}).get("blocking_conditions", [])),
                advisory_warnings=list(data.get("cutover_evaluation", {}).get("advisory_warnings", [])),
                evaluated_at=data.get("cutover_evaluation", {}).get("evaluated_at"),
            ),
            cutover_execution=CutoverExecutionState(
                status=CutoverExecutionStatus(
                    data.get("cutover_execution", {}).get(
                        "status",
                        CutoverExecutionStatus.NOT_STARTED.value,
                    )
                ),
                started_at=data.get("cutover_execution", {}).get("started_at"),
                freeze_writes_at=data.get("cutover_execution", {}).get("freeze_writes_at"),
                final_sync_completed_at=data.get("cutover_execution", {}).get("final_sync_completed_at"),
                final_validation_completed_at=data.get("cutover_execution", {}).get("final_validation_completed_at"),
                completed_at=data.get("cutover_execution", {}).get("completed_at"),
                failed_at=data.get("cutover_execution", {}).get("failed_at"),
                final_checkpoint=data.get("cutover_execution", {}).get("final_checkpoint"),
                operator_notes=list(data.get("cutover_execution", {}).get("operator_notes", [])),
                hook_trace=list(data.get("cutover_execution", {}).get("hook_trace", [])),
                error_message=data.get("cutover_execution", {}).get("error_message"),
                recovery_path=data.get("cutover_execution", {}).get("recovery_path"),
            ),
            ops_recommendation_history=list(data.get("ops_recommendation_history", [])),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
        )


def utc_now_iso() -> str:
    """Get a stable UTC timestamp string suitable for persistence."""
    return datetime.now(tz=timezone.utc).isoformat()


_ALLOWED_TRANSITIONS: dict[MigrationRunStatus, set[MigrationRunStatus]] = {
    MigrationRunStatus.DRAFTED: {MigrationRunStatus.APPROVED, MigrationRunStatus.FAILED},
    MigrationRunStatus.APPROVED: {MigrationRunStatus.PROVISIONING, MigrationRunStatus.FAILED},
    MigrationRunStatus.PROVISIONING: {MigrationRunStatus.BACKFILLING, MigrationRunStatus.FAILED},
    MigrationRunStatus.BACKFILLING: {MigrationRunStatus.VALIDATING, MigrationRunStatus.ROLLBACK_IN_PROGRESS, MigrationRunStatus.FAILED},
    MigrationRunStatus.VALIDATING: {MigrationRunStatus.VALIDATION_PASSED, MigrationRunStatus.VALIDATION_FAILED, MigrationRunStatus.ROLLBACK_IN_PROGRESS, MigrationRunStatus.FAILED},
    MigrationRunStatus.VALIDATION_PASSED: {MigrationRunStatus.SYNCING, MigrationRunStatus.CUTOVER_READY, MigrationRunStatus.ROLLBACK_IN_PROGRESS, MigrationRunStatus.FAILED},
    MigrationRunStatus.VALIDATION_FAILED: {MigrationRunStatus.ROLLBACK_IN_PROGRESS, MigrationRunStatus.FAILED},
    MigrationRunStatus.SYNCING: {MigrationRunStatus.CUTOVER_READY, MigrationRunStatus.ROLLBACK_IN_PROGRESS, MigrationRunStatus.FAILED},
    MigrationRunStatus.CUTOVER_READY: {MigrationRunStatus.CUTOVER_COMPLETE, MigrationRunStatus.ROLLBACK_IN_PROGRESS, MigrationRunStatus.FAILED},
    MigrationRunStatus.CUTOVER_COMPLETE: set(),
    MigrationRunStatus.ROLLBACK_IN_PROGRESS: {MigrationRunStatus.ROLLED_BACK, MigrationRunStatus.FAILED},
    MigrationRunStatus.ROLLED_BACK: set(),
    MigrationRunStatus.FAILED: {MigrationRunStatus.ROLLBACK_IN_PROGRESS},
}

_STATUS_TO_PHASE: dict[MigrationRunStatus, MigrationPhase] = {
    MigrationRunStatus.DRAFTED: MigrationPhase.PREPARE,
    MigrationRunStatus.APPROVED: MigrationPhase.PREPARE,
    MigrationRunStatus.PROVISIONING: MigrationPhase.PROVISION,
    MigrationRunStatus.BACKFILLING: MigrationPhase.BACKFILL,
    MigrationRunStatus.VALIDATING: MigrationPhase.VALIDATE,
    MigrationRunStatus.VALIDATION_PASSED: MigrationPhase.VALIDATE,
    MigrationRunStatus.VALIDATION_FAILED: MigrationPhase.VALIDATE,
    MigrationRunStatus.SYNCING: MigrationPhase.SYNC,
    MigrationRunStatus.CUTOVER_READY: MigrationPhase.CUTOVER,
    MigrationRunStatus.CUTOVER_COMPLETE: MigrationPhase.COMPLETE,
    MigrationRunStatus.ROLLBACK_IN_PROGRESS: MigrationPhase.ROLLBACK,
    MigrationRunStatus.ROLLED_BACK: MigrationPhase.ROLLBACK,
    MigrationRunStatus.FAILED: MigrationPhase.ROLLBACK,
}
