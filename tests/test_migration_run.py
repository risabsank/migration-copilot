from pathlib import Path

import pytest

from sdk.artifacts.generator import ArtifactBundle
from sdk.copilot import PlanOutput
from sdk.engine.models import (
    CDCPlan,
    CriticalityTier,
    EngineResult,
    MigrationPattern,
    MigrationPlan,
    PlanStep,
    ResolvedSpec,
    ResolvedTablePlan,
)
from sdk.state import JsonMigrationRunStore, MigrationRun, MigrationRunStatus, initialize_run_from_plan


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
                cutover_wave=2,
            ),
            ResolvedTablePlan(
                table_name="orders",
                use_cdc=True,
                chunk_size_rows=1000,
                execution_order=2,
                criticality=CriticalityTier.TIER2,
                cutover_wave=3,
            ),
        ],
        assumptions=[],
        confidence=0.92,
        confirm_with_team=[],
        decision_log=[],
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
    )
    plan = MigrationPlan(
        pattern=MigrationPattern.BACKFILL_CDC,
        steps=[PlanStep(id="prepare", stage="prepare", depends_on=[], details="Prepare migration")],
        rollback_criteria=["validation drift > 0.1%"],
    )
    return EngineResult(resolved_spec=resolved, plan=plan)


def test_migration_run_transition_sequence():
    run = MigrationRun.new(
        plan_id="plan-123",
        schema="public",
        selected_variant="backfill_cdc_sync",
        pattern=MigrationPattern.BACKFILL_CDC,
        table_names=["users"],
        run_id="run-123",
    )

    run.transition_to(MigrationRunStatus.APPROVED)
    run.transition_to(MigrationRunStatus.PROVISIONING)
    run.transition_to(MigrationRunStatus.BACKFILLING)
    run.transition_to(MigrationRunStatus.VALIDATING)
    run.transition_to(MigrationRunStatus.CUTOVER_READY)

    assert run.cutover_ready is True
    assert run.rollback_ready is True
    assert run.status == MigrationRunStatus.CUTOVER_READY


def test_migration_run_transition_rejects_invalid_jump():
    run = MigrationRun.new(
        plan_id="plan-123",
        schema="public",
        selected_variant="backfill_cdc_sync",
        pattern=MigrationPattern.BACKFILL_CDC,
        table_names=["users"],
        run_id="run-123",
    )

    with pytest.raises(ValueError):
        run.transition_to(MigrationRunStatus.BACKFILLING)


def test_json_migration_store_roundtrip(tmp_path: Path):
    store = JsonMigrationRunStore(tmp_path / "runs.json")
    run = MigrationRun.new(
        plan_id="plan-123",
        schema="public",
        selected_variant="backfill_cdc_sync",
        pattern=MigrationPattern.BACKFILL_CDC,
        table_names=["users", "orders"],
        run_id="run-abc",
    )

    store.save(run)

    loaded = store.get("run-abc")
    assert loaded is not None
    assert loaded.plan_id == "plan-123"
    assert [item.table_name for item in loaded.table_progress] == ["users", "orders"]


def test_initialize_run_from_plan_output_uses_plan_metadata(tmp_path: Path):
    output = PlanOutput(
        result=_engine_result(),
        runbook_markdown="# Runbook",
        artifact_bundle=ArtifactBundle(
            root=tmp_path,
            plan_json_path=tmp_path / "plan.json",
            runbook_path=tmp_path / "runbook.md",
            validations_path=tmp_path / "validations.sql",
            backfill_dir=tmp_path / "backfill",
            transforms_dir=tmp_path / "transforms",
            cdc_dir=tmp_path / "cdc",
        ),
        plan_id="plan-xyz",
        events=[],
        events_path=tmp_path / "events.jsonl",
    )

    run = initialize_run_from_plan(output, schema="sales")

    assert run.plan_id == "plan-xyz"
    assert run.schema == "sales"
    assert run.selected_variant == "backfill_cdc_sync"
    assert [progress.table_name for progress in run.table_progress] == ["users", "orders"]
