"""Compatibility shim for engine models."""

from __future__ import annotations

from sdk.engine.models import *  # noqa: F401,F403

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class PolicyProfile(str, Enum):
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    FAST = "fast"


class MigrationPattern(str, Enum):
    BIG_BANG = "big_bang"
    BACKFILL_CDC = "backfill_plus_cdc"
    PHASED = "phased"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass(frozen=True)
class MigrationSpec:
    source_type: str
    target_type: str
    objects: list[str]
    downtime_minutes: int | None = None
    policy_profile: PolicyProfile = PolicyProfile.CONSERVATIVE
    low_bandwidth_mode: bool = False


@dataclass(frozen=True)
class TableProfile:
    name: str
    row_count: int
    size_gb: float
    has_primary_key: bool
    primary_key_columns: list[str] = field(default_factory=list)
    column_names: list[str] = field(default_factory=list)
    estimated_writes_per_minute: int | None = None
    upstream_dependencies: list[str] = field(default_factory=list)
    schema_drift_likelihood: float = 0.0


@dataclass(frozen=True)
class SourceProfile:
    tables: list[TableProfile]
    cdc_supported: bool

    def by_name(self) -> dict[str, TableProfile]:
        return {table.name: table for table in self.tables}


@dataclass(frozen=True)
class ResolvedTablePlan:
    table_name: str
    use_cdc: bool
    chunk_size_rows: int
    execution_order: int
    assumptions: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RiskItem:
    key: str
    level: RiskLevel
    rationale: str


@dataclass(frozen=True)
class ResolvedSpec:
    pattern: MigrationPattern
    requires_cdc: bool
    table_plans: list[ResolvedTablePlan]
    assumptions: list[str]
    confidence: float
    confirm_with_team: list[str]
    decision_log: list[str]
    risks: list[RiskItem]


@dataclass(frozen=True)
class PlanStep:
    id: str
    stage: str
    depends_on: list[str]
    details: str


@dataclass(frozen=True)
class MigrationPlan:
    pattern: MigrationPattern
    steps: list[PlanStep]
    rollback_criteria: list[str]


@dataclass(frozen=True)
class EngineResult:
    resolved_spec: ResolvedSpec
    plan: MigrationPlan

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
