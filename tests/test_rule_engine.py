from sdk.engine.models import CriticalityTier, MigrationSpec, PolicyProfile, SourceProfile, TableProfile
from sdk.engine.rule_engine import DeterministicDecisionEngine


def _table(name: str, size: float, deps=None, pk=True, writes=100, criticality=CriticalityTier.TIER2):
    return TableProfile(
        name=name,
        row_count=1000,
        size_gb=size,
        has_primary_key=pk,
        primary_key_columns=["id"] if pk else [],
        estimated_writes_per_minute=writes,
        upstream_dependencies=deps or [],
        column_names=["id"],
        criticality=criticality,
    )


def test_rule_branches_and_policy_profile_effects():
    source = SourceProfile(tables=[_table("orders", 120)], cdc_supported=True, cdc_log_mode="wal")
    engine = DeterministicDecisionEngine()

    fast = engine.build(MigrationSpec("pg", "sf", ["orders"], downtime_minutes=8, policy_profile=PolicyProfile.FAST), source)
    conservative = engine.build(MigrationSpec("pg", "sf", ["orders"], downtime_minutes=8, policy_profile=PolicyProfile.CONSERVATIVE), source)

    assert fast.resolved_spec.requires_cdc is True
    assert conservative.resolved_spec.requires_cdc is False
    assert fast.resolved_spec.table_plans[0].chunk_size_rows > conservative.resolved_spec.table_plans[0].chunk_size_rows
    assert len(conservative.plan.rollback_criteria) > len(fast.plan.rollback_criteria)


def test_fk_order_cycle_falls_back_lexicographic():
    source = SourceProfile(
        tables=[
            _table("b", 1, deps=["a"]),
            _table("a", 1, deps=["b"]),
        ],
        cdc_supported=True,
        cdc_log_mode="wal",
    )
    result = DeterministicDecisionEngine().build(MigrationSpec("pg", "bq", ["a", "b"], downtime_minutes=60), source)
    ordered = [p.table_name for p in result.resolved_spec.table_plans]
    assert ordered == ["a", "b"]
    assert any("fk_cycle_detected" in x for x in result.resolved_spec.decision_log)


def test_cdc_readiness_missing_pk_and_unknown_log_mode():
    source = SourceProfile(tables=[_table("events", 2, pk=False)], cdc_supported=True, cdc_log_mode="none")
    result = DeterministicDecisionEngine().build(MigrationSpec("pg", "sf", ["events"], downtime_minutes=2), source)
    assert result.resolved_spec.cdc_plan.ready is False
    assert result.resolved_spec.cdc_plan.prerequisites


def test_chunk_sizing_boundaries():
    source = SourceProfile(
        tables=[
            _table("tiny", 9.9),
            _table("small", 10.0),
            _table("medium", 100.0),
            _table("large", 500.0),
        ],
        cdc_supported=True,
        cdc_log_mode="wal",
    )
    result = DeterministicDecisionEngine().build(MigrationSpec("pg", "sf", [], downtime_minutes=60, policy_profile=PolicyProfile.BALANCED), source)
    chunks = {p.table_name: p.chunk_size_rows for p in result.resolved_spec.table_plans}
    assert chunks["tiny"] == 5_000_000
    assert chunks["small"] == 1_000_000
    assert chunks["medium"] == 200_000
    assert chunks["large"] == 50_000


def test_wave_planner_uses_criticality_tiers():
    source = SourceProfile(
        tables=[
            _table("payments", 1, criticality=CriticalityTier.TIER0),
            _table("users", 1, criticality=CriticalityTier.TIER1),
            _table("logs", 1, criticality=CriticalityTier.TIER2),
        ],
        cdc_supported=True,
        cdc_log_mode="wal",
    )
    result = DeterministicDecisionEngine().build(MigrationSpec("pg", "sf", [], downtime_minutes=60), source)
    waves = {p.table_name: p.cutover_wave for p in result.resolved_spec.table_plans}
    assert waves == {"logs": 3, "payments": 1, "users": 2}
    assert result.resolved_spec.estimate is not None
