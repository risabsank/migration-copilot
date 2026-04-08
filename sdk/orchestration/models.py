"""Orchestration result models for migration lifecycle workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class OrchestrationFinalStatus(str, Enum):
    """Terminal outcome for orchestration execution."""

    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass(frozen=True)
class OrchestrationResult:
    """High-level result summary returned by MigrationOrchestrator."""

    final_status: OrchestrationFinalStatus
    completed_phases: list[str] = field(default_factory=list)
    failed_phase: str | None = None
    last_checkpoint: str | None = None
