import json
from pathlib import Path

from sdk.engine.models import MigrationSpec, PolicyProfile, SourceProfile, TableProfile
from sdk.engine.rule_engine import DeterministicDecisionEngine


def test_deterministic_plan_snapshot():
    source = SourceProfile(
        tables=[
            TableProfile(name="accounts", row_count=1000, size_gb=12, has_primary_key=True, primary_key_columns=["id"], column_names=["id", "name"]),
            TableProfile(name="orders", row_count=2000, size_gb=120, has_primary_key=True, primary_key_columns=["id"], column_names=["id", "acct_id"], upstream_dependencies=["accounts"]),
        ],
        cdc_supported=True,
        cdc_log_mode="wal",
    )
    spec = MigrationSpec("postgres", "snowflake", ["accounts", "orders"], downtime_minutes=5, policy_profile=PolicyProfile.BALANCED)
    result = DeterministicDecisionEngine().build(spec, source)

    payload = {
        "pattern": result.plan.pattern.value,
        "selected_variant": result.resolved_spec.selected_variant,
        "table_plans": [
            {
                "table": p.table_name,
                "chunk": p.chunk_size_rows,
                "order": p.execution_order,
                "wave": p.cutover_wave,
            }
            for p in result.resolved_spec.table_plans
        ],
        "rollback": result.plan.rollback_criteria,
    }
    text = json.dumps(payload, indent=2, sort_keys=True)

    snapshot = Path("tests/snapshots/plan_snapshot.json")
    assert text == snapshot.read_text(encoding="utf-8").strip()
