import json
from pathlib import Path

from sdk.artifacts.generator import ArtifactBundleGenerator
from sdk.copilot import _render_runbook
from sdk.engine.models import (
    CDCPlan,
    ComplianceGate,
    CostEstimate,
    CriticalityTier,
    EngineResult,
    MigrationPattern,
    MigrationPlan,
    MigrationSpec,
    PlanStep,
    PolicyProfile,
    ResolvedSpec,
    ResolvedTablePlan,
    SchemaContractReport,
    TableProfile,
)


def _engine_result() -> EngineResult:
    resolved = ResolvedSpec(
        pattern=MigrationPattern.BACKFILL_CDC,
        requires_cdc=True,
        table_plans=[
            ResolvedTablePlan(
                table_name="users",
                use_cdc=True,
                chunk_size_rows=1000,
                execution_order=1,
                criticality=CriticalityTier.TIER1,
                cutover_wave=1,
            )
        ],
        assumptions=[],
        confidence=0.9,
        confirm_with_team=["Approve cutover window"],
        decision_log=["deterministic fallback"],
        risks=[],
        plan_variants=["backfill_cdc_sync", "batch_only"],
        selected_variant="backfill_cdc_sync",
        cdc_plan=CDCPlan(
            ready=True,
            log_mode="wal",
            lag_gate_seconds=30,
            lag_stabilization_minutes=10,
            reprocessing_strategy="replay",
            dedupe_strategy="pk",
        ),
        estimate=CostEstimate(estimated_duration_minutes=30, peak_parallel_workers=4, compute_credits=1.2),
        compliance_gates=[ComplianceGate(name="SOX", passed=True, detail="ok")],
        schema_contract=SchemaContractReport(backward_compatibility_score=1.0),
        explainability_trace=["prompt_version=1"],
    )
    plan = MigrationPlan(
        pattern=MigrationPattern.BACKFILL_CDC,
        steps=[PlanStep(id="prepare", stage="prepare", depends_on=[], details="Prepare migration")],
        rollback_criteria=["validation drift > 0.1%"],
    )
    return EngineResult(resolved_spec=resolved, plan=plan)


def test_runbook_snapshot() -> None:
    rendered = _render_runbook(_engine_result())
    snapshot = Path("tests/snapshots/runbook_snapshot.md")
    assert rendered == snapshot.read_text(encoding="utf-8").strip()


def test_validation_sql_snapshot() -> None:
    result = _engine_result()
    table = TableProfile(
        name="users",
        row_count=10,
        size_gb=1.0,
        has_primary_key=True,
        primary_key_columns=["id"],
        column_names=["id", "email"],
    )
    rendered = ArtifactBundleGenerator()._render_validations_sql(result, [table], dialect="postgres")
    snapshot = Path("tests/snapshots/validations_postgres_snapshot.sql")
    assert rendered == snapshot.read_text(encoding="utf-8")


def test_plan_json_shape_snapshot() -> None:
    spec = MigrationSpec(source_type="postgres", target_type="snowflake", objects=["users"], policy_profile=PolicyProfile.BALANCED)
    payload = {
        "spec": {
            "source_type": spec.source_type,
            "target_type": spec.target_type,
            "objects": spec.objects,
            "downtime_minutes": spec.downtime_minutes,
            "policy_profile": spec.policy_profile.value,
        },
        "pattern": _engine_result().plan.pattern.value,
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    snapshot = Path("tests/snapshots/plan_shape_snapshot.json")
    assert rendered == snapshot.read_text(encoding="utf-8").strip()
