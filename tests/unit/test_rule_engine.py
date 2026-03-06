from migration_copilot.sdk.engine.models import (
    MigrationPattern,
    MigrationSpec,
    PolicyProfile,
    RiskLevel,
    SourceProfile,
    TableProfile,
)
from migration_copilot.sdk.engine.rule_engine import DeterministicDecisionEngine


def test_engine_selects_backfill_cdc_with_explanations_for_low_downtime():
    engine = DeterministicDecisionEngine()
    spec = MigrationSpec(
        source_type="postgres",
        target_type="snowflake",
        objects=["users", "orders"],
        downtime_minutes=5,
        policy_profile=PolicyProfile.CONSERVATIVE,
    )
    source = SourceProfile(
        cdc_supported=True,
        tables=[
            TableProfile(
                name="users",
                row_count=2_000_000,
                size_gb=25,
                has_primary_key=True,
                estimated_writes_per_minute=120,
            ),
            TableProfile(
                name="orders",
                row_count=50_000_000,
                size_gb=550,
                has_primary_key=True,
                estimated_writes_per_minute=None,
                upstream_dependencies=["users"],
                schema_drift_likelihood=0.5,
            ),
        ],
    )

    result = engine.build(spec, source)

    assert result.resolved_spec.pattern == MigrationPattern.BACKFILL_CDC
    assert result.resolved_spec.requires_cdc is True
    assert any("downtime<=5m" in line for line in result.resolved_spec.decision_log)
    assert any("write_rate_unknown" in line for line in result.resolved_spec.decision_log)
    assert [t.table_name for t in result.resolved_spec.table_plans] == ["users", "orders"]
    assert [t.chunk_size_rows for t in result.resolved_spec.table_plans] == [1_000_000, 50_000]
    assert any(r.level == RiskLevel.MEDIUM for r in result.resolved_spec.risks)
    assert result.plan.steps[-1].stage == "cutover"


def test_engine_flags_high_risk_for_large_table_low_bandwidth_and_missing_pk():
    engine = DeterministicDecisionEngine()
    spec = MigrationSpec(
        source_type="postgres",
        target_type="bigquery",
        objects=["events"],
        downtime_minutes=2,
        low_bandwidth_mode=True,
    )
    source = SourceProfile(
        cdc_supported=True,
        tables=[
            TableProfile(
                name="events",
                row_count=900_000_000,
                size_gb=300,
                has_primary_key=False,
                estimated_writes_per_minute=10_000,
                schema_drift_likelihood=0.9,
            )
        ],
    )

    result = engine.build(spec, source)

    assert result.resolved_spec.table_plans[0].use_cdc is False
    assert any("no_primary_key" in msg for msg in result.resolved_spec.decision_log)
    assert any("surrogate key strategy" in msg for msg in result.resolved_spec.confirm_with_team)
    high_risks = [r for r in result.resolved_spec.risks if r.level == RiskLevel.HIGH]
    assert any("large_table_low_bandwidth" in r.key for r in high_risks)
    assert any("schema_drift" in r.key for r in high_risks)
    assert result.resolved_spec.confidence < 0.7


def test_engine_is_deterministic_for_same_input():
    engine = DeterministicDecisionEngine()
    spec = MigrationSpec(source_type="postgres", target_type="snowflake", objects=["a", "b"], downtime_minutes=None)
    source = SourceProfile(
        cdc_supported=False,
        tables=[
            TableProfile(name="b", row_count=10, size_gb=1.0, has_primary_key=True, upstream_dependencies=["a"]),
            TableProfile(name="a", row_count=5, size_gb=1.0, has_primary_key=True),
        ],
    )

    first = engine.build(spec, source).as_dict()
    second = engine.build(spec, source).as_dict()

    assert first == second
