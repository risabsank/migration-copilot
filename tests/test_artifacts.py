from pathlib import Path

from sdk.artifacts.generator import ArtifactBundleGenerator
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


def _result() -> EngineResult:
    resolved = ResolvedSpec(
        pattern=MigrationPattern.BACKFILL_CDC,
        requires_cdc=True,
        table_plans=[ResolvedTablePlan(table_name="users", use_cdc=True, chunk_size_rows=1000, execution_order=1, criticality=CriticalityTier.TIER1, cutover_wave=2)],
        assumptions=[],
        confidence=0.9,
        confirm_with_team=[],
        decision_log=[],
        risks=[],
        plan_variants=["batch_only"],
        selected_variant="backfill_cdc_sync",
        cdc_plan=CDCPlan(ready=True, log_mode="wal", lag_gate_seconds=30, lag_stabilization_minutes=10, reprocessing_strategy="r", dedupe_strategy="d"),
        estimate=CostEstimate(estimated_duration_minutes=30, peak_parallel_workers=4, compute_credits=1.2),
        compliance_gates=[ComplianceGate(name="SOX", passed=True, detail="ok")],
        schema_contract=SchemaContractReport(backward_compatibility_score=1.0),
        explainability_trace=["prompt_version=1"],
    )
    return EngineResult(resolved_spec=resolved, plan=MigrationPlan(pattern=MigrationPattern.BACKFILL_CDC, steps=[PlanStep(id="prepare", stage="prepare", depends_on=[], details="d")], rollback_criteria=[]))


def test_artifact_generator_outputs_maturity_files(tmp_path: Path):
    gen = ArtifactBundleGenerator()
    spec = MigrationSpec(source_type="pg", target_type="sf", objects=["users"], policy_profile=PolicyProfile.BALANCED)
    tables = [TableProfile(name="users", row_count=1, size_gb=1.0, has_primary_key=True, primary_key_columns=["id"], column_names=["id"])]
    bundle = gen.generate(output_dir=tmp_path, spec=spec, result=_result(), runbook_markdown="# runbook", tables=tables)

    assert (tmp_path / "sql_packs" / "validations_postgres.sql").exists()
    assert (tmp_path / "sql_packs" / "validations_snowflake.sql").exists()
    assert (tmp_path / "sql_packs" / "validations_bigquery.sql").exists()
    assert (tmp_path / "dags" / "airflow_dag.py").exists()
    assert (tmp_path / "dags" / "dagster_job.py").exists()
    assert (tmp_path / "cdc" / "users.debezium.yaml").exists()
    assert (tmp_path / "cdc" / "users.fivetran.yaml").exists()
    assert (tmp_path / "governance" / "checksums.json").exists()
    assert (tmp_path / "governance" / "bundle.signature").exists()
    assert (tmp_path / "ai" / "evaluation_metrics.json").exists()
    assert bundle.plan_json_path.exists()
