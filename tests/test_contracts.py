import json
from pathlib import Path

import pytest

from sdk.adapters.contracts import ColumnInfo, MetadataAdapter, TableMetadata, ValidationAdapter
from sdk.engine.models import MigrationPattern
from sdk.observability import PlanEvent
from sdk.state import JsonMigrationRunStore, MigrationRun


class GoodMetadataAdapter:
    def list_tables(self, schema: str = "public") -> list[str]:
        return ["users"]

    def describe_table(self, table_name: str, schema: str = "public") -> TableMetadata:
        return TableMetadata(
            table_name=table_name,
            row_estimate=42,
            size_bytes_estimate=2048,
            primary_key_columns=["id"],
            columns=[ColumnInfo(name="id", data_type="int", nullable=False)],
        )


class GoodValidationAdapter:
    def execute_query(self, query: str) -> list[dict[str, object]]:
        return [{"metric_value": 1}]


class BrokenMetadataAdapter:
    def list_tables(self, schema: str = "public") -> list[str]:
        return ["users"]


class BrokenValidationAdapter:
    pass


def test_migration_run_payload_contract_has_schema_version(tmp_path: Path) -> None:
    store = JsonMigrationRunStore(tmp_path / "runs.json")
    run = MigrationRun.new(
        plan_id="plan-1",
        schema="public",
        selected_variant="backfill_cdc_sync",
        pattern=MigrationPattern.BACKFILL_CDC,
        table_names=["users"],
        run_id="run-1",
    )

    store.save(run)
    payload = json.loads((tmp_path / "runs.json").read_text(encoding="utf-8"))
    persisted = payload["runs"]["run-1"]

    assert payload["store_schema_version"] == 1
    assert persisted["schema_version"] == 2
    assert persisted["run_id"] == "run-1"
    assert persisted["table_progress"][0]["table_name"] == "users"


def test_migration_run_store_migrates_legacy_payload(tmp_path: Path) -> None:
    legacy_payload = {
        "runs": {
            "run-legacy": {
                "run_id": "run-legacy",
                "plan_id": "plan-legacy",
                "schema": "public",
                "selected_variant": "batch_only",
                "pattern": "big_bang",
                "status": "drafted",
                "phase": "prepare",
                "validation_status": "not_started",
                "cutover_ready": False,
                "rollback_ready": False,
                "validation_summary": {},
                "table_progress": [
                    {
                        "table_name": "users",
                        "status": "pending",
                        "progress_percent": 0.0,
                        "rows_copied": 0,
                        "updated_at": "2026-01-01T00:00:00+00:00",
                    }
                ],
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
            }
        }
    }
    path = tmp_path / "legacy_runs.json"
    path.write_text(json.dumps(legacy_payload), encoding="utf-8")

    loaded = JsonMigrationRunStore(path).get("run-legacy")

    assert loaded is not None
    assert loaded.orchestration_phase.value == "plan_ready"
    assert loaded.cdc_status.value == "not_started"


def test_adapter_protocol_contract_runtime_checks() -> None:
    assert isinstance(GoodMetadataAdapter(), MetadataAdapter)
    assert isinstance(GoodValidationAdapter(), ValidationAdapter)


@pytest.mark.parametrize(
    "adapter, protocol",
    [
        (BrokenMetadataAdapter(), MetadataAdapter),
        (BrokenValidationAdapter(), ValidationAdapter),
    ],
)
def test_adapter_protocol_contract_rejects_invalid_implementations(adapter: object, protocol: object) -> None:
    assert not isinstance(adapter, protocol)  # type: ignore[arg-type]


def test_event_payload_schema_snapshot_stability() -> None:
    event = PlanEvent(
        event_type="agent_step",
        plan_id="plan-123",
        step="strategy_planner",
        status="completed",
        rule_ids=["plan_dag_builder"],
        confidence=0.91,
        payload={"step_count": 4, "selected_variant": "backfill_cdc_sync"},
        ts_utc="2026-01-01T00:00:00+00:00",
    )

    serialized = json.dumps(event.as_dict(), indent=2, sort_keys=True)
    snapshot = Path("tests/snapshots/event_payload_snapshot.json")
    assert serialized == snapshot.read_text(encoding="utf-8").strip()
