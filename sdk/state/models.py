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


class ValidationStatus(str, Enum):
    """Validation status for the full migration run."""

    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    PASSED = "passed"
    FAILED = "failed"


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
    table_progress: list[TableExecutionProgress] = field(default_factory=list)
    last_checkpoint: str | None = None
    last_watermark: str | None = None
    created_at: str = field(default_factory=lambda: utc_now_iso())
    updated_at: str = field(default_factory=lambda: utc_now_iso())

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
            "table_progress": [item.as_dict() for item in self.table_progress],
            "last_checkpoint": self.last_checkpoint,
            "last_watermark": self.last_watermark,
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
            last_checkpoint=data.get("last_checkpoint"),
            last_watermark=data.get("last_watermark"),
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
    MigrationRunStatus.VALIDATING: {MigrationRunStatus.SYNCING, MigrationRunStatus.CUTOVER_READY, MigrationRunStatus.ROLLBACK_IN_PROGRESS, MigrationRunStatus.FAILED},
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
    MigrationRunStatus.SYNCING: MigrationPhase.SYNC,
    MigrationRunStatus.CUTOVER_READY: MigrationPhase.CUTOVER,
    MigrationRunStatus.CUTOVER_COMPLETE: MigrationPhase.COMPLETE,
    MigrationRunStatus.ROLLBACK_IN_PROGRESS: MigrationPhase.ROLLBACK,
    MigrationRunStatus.ROLLED_BACK: MigrationPhase.ROLLBACK,
    MigrationRunStatus.FAILED: MigrationPhase.ROLLBACK,
}
