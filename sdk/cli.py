from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


from sdk.adapters.contracts import ColumnInfo, ForeignKeyInfo, MetadataAdapter, TableMetadata
from sdk.copilot import MigrationCopilot
from sdk.engine.models import MigrationSpec, PolicyProfile

class CliInputError(ValueError):
    """Raised when CLI input is invalid and should be shown as user-facing guidance."""

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
        raise CliInputError(
            "YAML spec parsing requires PyYAML. Install with "
            "`pip install migration-copilot[yaml]` or use JSON spec."
        ) from exc
    return yaml.safe_load(raw)


def _validate_spec_doc(spec_doc: dict[str, Any]) -> None:
    required_keys = ["source_type", "target_type", "objects"]
    missing = [key for key in required_keys if key not in spec_doc]
    if missing:
        raise CliInputError(
            f"Spec is missing required keys: {', '.join(missing)}. "
            "Start from `migration-copilot init-spec --template postgres_snowflake`."
        )

    objects = spec_doc.get("objects", [])
    if not isinstance(objects, list) or not objects:
        raise CliInputError(
            "Spec `objects` must be a non-empty list of tables to migrate."
        )

def _build_migration_spec(spec_doc: dict[str, Any]) -> MigrationSpec:
    _validate_spec_doc(spec_doc)

    try:
        policy_profile = PolicyProfile(spec_doc.get("policy_profile", "conservative"))
    except ValueError as exc:
        valid_profiles = ", ".join(profile.value for profile in PolicyProfile)
        raise CliInputError(
            f"Invalid policy_profile `{spec_doc.get('policy_profile')}`. "
            f"Use one of: {valid_profiles}."
        ) from exc

    return MigrationSpec(
        source_type=spec_doc["source_type"],
        target_type=spec_doc["target_type"],
        objects=list(spec_doc.get("objects", [])),
        downtime_minutes=spec_doc.get("downtime_minutes"),
        policy_profile=policy_profile,
        low_bandwidth_mode=bool(spec_doc.get("low_bandwidth_mode", False)),
        source_of_truth=spec_doc.get("source_of_truth", "database"),
    )

def _plan_command(args: argparse.Namespace) -> int:
    spec_path = Path(args.spec)
    if not spec_path.exists():
        raise CliInputError(
            f"Spec file was not found at `{spec_path}`. "
            "Use `migration-copilot init-spec --out ./spec.json` to create one."
        )

    spec_doc = _parse_spec(spec_path)
    migration_spec = _build_migration_spec(spec_doc)

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

def _doctor_command(args: argparse.Namespace) -> int:
    checks: list[tuple[str, bool, str]] = []

    checks.append(
        (
            "python_version",
            sys.version_info >= (3, 10),
            f"Detected Python {sys.version_info.major}.{sys.version_info.minor}; require >= 3.10",
        )
    )

    spec_path = Path(args.spec) if args.spec else None
    if spec_path is not None:
        checks.append(("spec_exists", spec_path.exists(), f"Spec file exists at {spec_path}"))
        if spec_path.exists():
            try:
                spec_doc = _parse_spec(spec_path)
                _validate_spec_doc(spec_doc)
            except Exception as exc:  # noqa: BLE001
                checks.append(("spec_valid", False, f"Spec validation failed: {exc}"))
            else:
                checks.append(("spec_valid", True, "Spec parsed and passed basic validation"))

    requires_yaml = bool(spec_path and spec_path.suffix.lower() in {".yml", ".yaml"})
    if requires_yaml:
        try:
            import yaml  # type: ignore  # noqa: F401
        except ModuleNotFoundError:
            checks.append(
                (
                    "pyyaml_installed",
                    False,
                    "PyYAML is required for YAML specs; install with `pip install migration-copilot[yaml]`",
                )
            )
        else:
            checks.append(("pyyaml_installed", True, "PyYAML is installed"))

    summary = {
        "ok": all(result for _, result, _ in checks),
        "checks": [
            {"name": name, "ok": ok, "message": message}
            for name, ok, message in checks
        ],
    }
    print(json.dumps(summary, indent=2))
    return 0 if summary["ok"] else 1


def _base_template() -> dict[str, Any]:
    return {
        "source_type": "postgres",
        "target_type": "snowflake",
        "schema": "public",
        "objects": ["orders", "customers"],
        "downtime_minutes": 5,
        "policy_profile": "conservative",
        "cdc_supported": True,
        "cdc_log_mode": "wal",
        "tables": [
            {
                "name": "orders",
                "row_estimate": 250000,
                "size_bytes_estimate": 64000000,
                "primary_key_columns": ["id"],
                "columns": [
                    {"name": "id", "data_type": "bigint", "nullable": False},
                    {"name": "customer_id", "data_type": "bigint", "nullable": False},
                ],
                "foreign_keys": [
                    {
                        "column": "customer_id",
                        "references_table": "customers",
                        "references_column": "id",
                    }
                ],
            },
            {
                "name": "customers",
                "row_estimate": 100000,
                "size_bytes_estimate": 12000000,
                "primary_key_columns": ["id"],
                "columns": [
                    {"name": "id", "data_type": "bigint", "nullable": False},
                    {"name": "email", "data_type": "text", "nullable": False},
                ],
            },
        ],
    }


def _template_for(name: str) -> dict[str, Any]:
    template = _base_template()
    if name == "postgres_bigquery":
        template["target_type"] = "bigquery"
        return template
    if name == "postgres_snowflake":
        return template
    raise CliInputError(
        f"Unknown template `{name}`. Use one of: postgres_snowflake, postgres_bigquery."
    )


def _init_spec_command(args: argparse.Namespace) -> int:
    output_path = Path(args.out)
    if output_path.exists() and not args.force:
        raise CliInputError(
            f"Output file `{output_path}` already exists. Pass --force to overwrite it."
        )

    template_doc = _template_for(args.template)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(template_doc, indent=2) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "created": str(output_path),
                "template": args.template,
                "next_steps": [
                    f"Edit {output_path} with your real tables and constraints",
                    f"Run: migration-copilot doctor --spec {output_path}",
                    f"Run: migration-copilot plan --spec {output_path} --out ./artifacts/demo",
                ],
            },
            indent=2,
        )
    )
    return 0

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="migration-copilot")
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="Generate a deterministic migration plan bundle")
    plan_parser.add_argument("--spec", required=True, help="Path to a spec YAML/JSON file")
    plan_parser.add_argument("--out", required=True, help="Output bundle directory")
    plan_parser.set_defaults(func=_plan_command)

    doctor_parser = subparsers.add_parser("doctor", help="Run preflight checks for a smooth first-run experience")
    doctor_parser.add_argument("--spec", required=False, help="Optional path to a spec YAML/JSON file for validation")
    doctor_parser.set_defaults(func=_doctor_command)

    init_parser = subparsers.add_parser(
        "init-spec",
        help="Generate a starter spec template for quick onboarding",
    )
    init_parser.add_argument(
        "--template",
        choices=["postgres_snowflake", "postgres_bigquery"],
        default="postgres_snowflake",
        help="Starter template to generate",
    )
    init_parser.add_argument("--out", required=True, help="Path to write the JSON spec template")
    init_parser.add_argument("--force", action="store_true", help="Overwrite file if it already exists")
    init_parser.set_defaults(func=_init_spec_command)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except CliInputError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
