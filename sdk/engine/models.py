"""Compatibility shim for engine models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
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

class CriticalityTier(str, Enum):
    TIER0 = "tier0"
    TIER1 = "tier1"
    TIER2 = "tier2"


@dataclass(frozen=True)
class MigrationSpec:
    source_type: str
    target_type: str
    objects: list[str]
    downtime_minutes: int | None = None
    policy_profile: PolicyProfile = PolicyProfile.CONSERVATIVE
    low_bandwidth_mode: bool = False
    source_of_truth: str = "database"


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
    domain: str | None = None
    criticality: CriticalityTier = CriticalityTier.TIER2


@dataclass(frozen=True)
class SourceProfile:
    tables: list[TableProfile]
    cdc_supported: bool
    cdc_log_mode: str | None = None

    def by_name(self) -> dict[str, TableProfile]:
        return {table.name: table for table in self.tables}


@dataclass(frozen=True)
class ResolvedTablePlan:
    table_name: str
    use_cdc: bool
    chunk_size_rows: int
    execution_order: int
    assumptions: list[str] = field(default_factory=list)
    criticality: CriticalityTier = CriticalityTier.TIER2
    cutover_wave: int = 3

@dataclass(frozen=True)
class CDCPlan:
    ready: bool
    log_mode: str
    lag_gate_seconds: int
    lag_stabilization_minutes: int
    reprocessing_strategy: str
    dedupe_strategy: str
    prerequisites: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class StreamingReplayPlan:
    enabled: bool
    source_topic_pattern: str
    projection_rebuild_strategy: str
    cutover_gate: str

@dataclass(frozen=True)
class RiskItem:
    key: str
    level: RiskLevel
    rationale: str

@dataclass(frozen=True)
class CostEstimate:
    estimated_duration_minutes: int
    peak_parallel_workers: int
    compute_credits: float
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ComplianceGate:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class SchemaContractReport:
    backward_compatibility_score: float
    breaking_changes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


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
    plan_variants: list[str]
    selected_variant: str
    cdc_plan: CDCPlan
    streaming_replay_plan: StreamingReplayPlan | None = None
    phased_cutover_groups: list[list[str]] = field(default_factory=list)
    ai_primary: bool = False
    ai_agent_notes: list[str] = field(default_factory=list)
    estimate: CostEstimate | None = None
    compliance_gates: list[ComplianceGate] = field(default_factory=list)
    schema_contract: SchemaContractReport | None = None
    explainability_trace: list[str] = field(default_factory=list)


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
