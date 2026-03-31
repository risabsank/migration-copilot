from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


from sdk.adapters.contracts import ColumnInfo, ForeignKeyInfo, MetadataAdapter, TableMetadata
from sdk.copilot import MigrationCopilot
from sdk.engine.models import MigrationSpec, PolicyProfile


class StaticMetadataAdapter(MetadataAdapter):
    """Simple metadata adapter loaded from spec YAML/JSON for CI and local examples."""

    def __init__(self, tables: list[dict[str, Any]]):
        self._tables: dict[str, TableMetadata] = {}
        for table in tables:
            name = table["name"]
            columns = [
                ColumnInfo(
                    name=column["name"],
                    data_type=column.get("data_type", "text"),
                    nullable=bool(column.get("nullable", True)),
                )
                for column in table.get("columns", [])
            ]
            foreign_keys = [
                ForeignKeyInfo(
                    column=fk["column"],
                    references_table=fk["references_table"],
                    references_column=fk.get("references_column", "id"),
                )
                for fk in table.get("foreign_keys", [])
            ]
            self._tables[name] = TableMetadata(
                table_name=name,
                row_estimate=int(table.get("row_estimate", 0)),
                size_bytes_estimate=int(table.get("size_bytes_estimate", 0)),
                primary_key_columns=list(table.get("primary_key_columns", [])),
                columns=columns,
                foreign_keys=foreign_keys,
            )

    def list_tables(self, schema: str = "public") -> list[str]:
        del schema
        return sorted(self._tables.keys())

    def describe_table(self, table_name: str, schema: str = "public") -> TableMetadata:
        del schema
        return self._tables[table_name]


def _parse_spec(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(raw)

    try:
        import yaml  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("YAML spec parsing requires PyYAML. Install with `pip install migration-copilot[yaml]` or use JSON spec.") from exc

    return yaml.safe_load(raw)


def _plan_command(args: argparse.Namespace) -> int:
    spec_doc = _parse_spec(Path(args.spec))

    migration_spec = MigrationSpec(
        source_type=spec_doc["source_type"],
        target_type=spec_doc["target_type"],
        objects=list(spec_doc.get("objects", [])),
        downtime_minutes=spec_doc.get("downtime_minutes"),
        policy_profile=PolicyProfile(spec_doc.get("policy_profile", "conservative")),
        low_bandwidth_mode=bool(spec_doc.get("low_bandwidth_mode", False)),
        source_of_truth=spec_doc.get("source_of_truth", "database"),
    )

    adapter = StaticMetadataAdapter(tables=spec_doc.get("tables", []))
    copilot = MigrationCopilot(metadata_adapter=adapter)
    output = copilot.plan(
        spec=migration_spec,
        schema=spec_doc.get("schema", "public"),
        cdc_supported=bool(spec_doc.get("cdc_supported", True)),
        cdc_log_mode=spec_doc.get("cdc_log_mode", "wal"),
        output_dir=args.out,
        plan_id=spec_doc.get("plan_id"),
    )

    summary = {
        "plan_id": output.plan_id,
        "selected_variant": output.result.resolved_spec.selected_variant,
        "confidence": output.result.resolved_spec.confidence,
        "bundle_root": str(output.artifact_bundle.root),
        "events_path": str(output.events_path),
    }
    print(json.dumps(summary, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="migration-copilot")
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="Generate a deterministic migration plan bundle")
    plan_parser.add_argument("--spec", required=True, help="Path to a spec YAML/JSON file")
    plan_parser.add_argument("--out", required=True, help="Output bundle directory")
    plan_parser.set_defaults(func=_plan_command)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
