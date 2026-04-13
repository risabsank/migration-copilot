from __future__ import annotations

import argparse
import json
import os
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
    if any(not isinstance(obj, str) or not obj.strip() for obj in objects):
        raise CliInputError(
            "Spec `objects` entries must be non-empty table names (strings)."
        )

    normalized = [obj.strip() for obj in objects]
    duplicate_objects = sorted({name for name in normalized if normalized.count(name) > 1})
    if duplicate_objects:
        raise CliInputError(
            "Spec `objects` contains duplicates: "
            f"{', '.join(duplicate_objects)}. Remove duplicates to keep planning deterministic."
        )


def _validate_requested_objects_exist(
    requested_objects: list[str],
    available_tables: list[str],
) -> None:
    missing = sorted(set(requested_objects) - set(available_tables))
    if missing:
        raise CliInputError(
            "Spec references table(s) that were not present in the provided metadata: "
            f"{', '.join(missing)}. Add them under `tables` in your spec or remove them from `objects`."
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
    _validate_requested_objects_exist(migration_spec.objects, adapter.list_tables(schema=spec_doc.get("schema", "public")))
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
                _append_security_spec_checks(checks, spec_doc)

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

    bundle_path = Path(args.bundle) if args.bundle else None
    if bundle_path is not None:
        _append_bundle_checks(checks, bundle_path)

    summary = {
        "ok": all(result for _, result, _ in checks),
        "checks": [
            {"name": name, "ok": ok, "message": message}
            for name, ok, message in checks
        ],
    }
    print(json.dumps(summary, indent=2))
    return 0 if summary["ok"] else 1


def _find_files(directory: Path, pattern: str) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(path for path in directory.glob(pattern) if path.is_file())


def _contains_any(path: Path, tokens: list[str]) -> bool:
    text = path.read_text(encoding="utf-8")
    return any(token in text for token in tokens)


def _is_default_signing_key(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in {"", "dev", "development", "changeme", "default", "test"}


def _append_bundle_checks(checks: list[tuple[str, bool, str]], bundle_path: Path) -> None:
    checks.append(("bundle_exists", bundle_path.exists(), f"Bundle path exists at {bundle_path}"))
    if not bundle_path.exists():
        return

    cdc_files = _find_files(bundle_path / "cdc", "*.yaml")
    if cdc_files:
        unresolved = [path for path in cdc_files if _contains_any(path, ["watermark_column: TODO"])]
        checks.append(
            (
                "cdc_watermark_column_resolved",
                not unresolved,
                (
                    "All CDC YAML files define a real watermark_column"
                    if not unresolved
                    else "Resolve watermark_column TODOs in: " + ", ".join(path.name for path in unresolved)
                ),
            )
        )

    backfill_files = _find_files(bundle_path / "backfill", "*.sql")
    if backfill_files:
        unresolved = [path for path in backfill_files if _contains_any(path, ["source.<table>", "target.<table>"])]
        checks.append(
            (
                "backfill_identifiers_resolved",
                not unresolved,
                (
                    "All backfill SQL files reference real source/target identifiers"
                    if not unresolved
                    else "Replace source.<table>/target.<table> placeholders in: "
                    + ", ".join(path.name for path in unresolved)
                ),
            )
        )

    validations_path = bundle_path / "validations.sql"
    if validations_path.exists():
        unresolved = _contains_any(validations_path, ["source.", "target."])
        checks.append(
            (
                "validation_identifiers_reviewed",
                not unresolved,
                (
                    "Validation SQL appears environment-specific"
                    if not unresolved
                    else "Update environment-specific source/target references in validations.sql"
                ),
            )
        )

    transform_files = _find_files(bundle_path / "transforms", "stg_*.sql")
    if transform_files:
        unresolved = [path for path in transform_files if _contains_any(path, ["select * from source_data"])]
        checks.append(
            (
                "transform_models_customized",
                not unresolved,
                (
                    "Transform models include custom projection logic"
                    if not unresolved
                    else "Replace staging model stubs with business logic in: "
                    + ", ".join(path.name for path in unresolved)
                ),
            )
        )

    dag_paths = [bundle_path / "dags" / "airflow_dag.py", bundle_path / "dags" / "dagster_job.py"]
    existing_dags = [path for path in dag_paths if path.exists()]
    if existing_dags:
        unresolved = [path for path in existing_dags if _contains_any(path, ["EmptyOperator", "@op", "return '"])]
        checks.append(
            (
                "orchestration_dags_customized",
                not unresolved,
                (
                    "Orchestration DAGs appear to contain real tasks and logic"
                    if not unresolved
                    else "Replace orchestration stubs and placeholders in: "
                    + ", ".join(path.name for path in unresolved)
                ),
            )
        )

    signing_key = os.environ.get("MIGRATION_COPILOT_SIGNING_KEY")
    checks.append(
        (
            "signing_key_configured",
            bool(signing_key) and not _is_default_signing_key(signing_key),
            (
                "MIGRATION_COPILOT_SIGNING_KEY is set and not a known dev/default value"
                if signing_key and not _is_default_signing_key(signing_key)
                else "Set MIGRATION_COPILOT_SIGNING_KEY from a secure secret manager value"
            ),
        )
    )

def _append_security_spec_checks(checks: list[tuple[str, bool, str]], spec_doc: dict[str, Any]) -> None:
    security = spec_doc.get("security")
    if not isinstance(security, dict):
        checks.append(
            (
                "security_profile_present",
                False,
                "Add a `security` section to the spec (access_roles, encryption, auditability, residency, retention).",
            )
        )
        return

    checks.append(("security_profile_present", True, "Spec includes a security profile section"))

    access_roles = security.get("access_roles", {})
    required_roles = {"source_reader", "target_writer", "validation_runner", "cutover_approver"}
    missing_roles = sorted(role for role in required_roles if not str(access_roles.get(role, "")).strip())
    checks.append(
        (
            "security_access_roles_defined",
            not missing_roles,
            (
                "All required migration access roles are defined"
                if not missing_roles
                else "Define missing access_roles entries: " + ", ".join(missing_roles)
            ),
        )
    )

    encryption = security.get("encryption", {})
    required_encryption_flags = {"at_rest", "in_transit", "staging", "logs_redacted"}
    missing_encryption = sorted(flag for flag in required_encryption_flags if encryption.get(flag) is not True)
    checks.append(
        (
            "security_encryption_controls_enabled",
            not missing_encryption,
            (
                "Encryption/redaction controls are enabled for all required data planes"
                if not missing_encryption
                else "Set encryption controls to true for: " + ", ".join(missing_encryption)
            ),
        )
    )

    auditability = security.get("auditability", {})
    required_audit_fields = {"record_what", "record_who", "record_when", "record_validations"}
    missing_audit_fields = sorted(field for field in required_audit_fields if auditability.get(field) is not True)
    checks.append(
        (
            "security_auditability_controls_enabled",
            not missing_audit_fields,
            (
                "Auditability controls are enabled for migration evidence collection"
                if not missing_audit_fields
                else "Set auditability controls to true for: " + ", ".join(missing_audit_fields)
            ),
        )
    )

    residency = security.get("residency", {})
    allowed_regions = residency.get("allowed_regions")
    has_regions = isinstance(allowed_regions, list) and any(str(region).strip() for region in allowed_regions)
    checks.append(
        (
            "security_residency_regions_defined",
            has_regions,
            (
                "Residency allowed_regions are defined"
                if has_regions
                else "Define at least one region under security.residency.allowed_regions"
            ),
        )
    )

    retention = security.get("retention", {})
    staging_ttl_hours = retention.get("staging_ttl_hours")
    has_ttl = isinstance(staging_ttl_hours, int) and staging_ttl_hours > 0
    checks.append(
        (
            "security_staging_ttl_defined",
            has_ttl,
            (
                f"Staging TTL is configured to {staging_ttl_hours} hours"
                if has_ttl
                else "Set security.retention.staging_ttl_hours to a positive integer"
            ),
        )
    )

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
        "security": {
            "access_roles": {
                "source_reader": "migration_source_reader",
                "target_writer": "migration_target_writer",
                "validation_runner": "migration_validation_runner",
                "cutover_approver": "migration_cutover_approver",
            },
            "encryption": {
                "at_rest": True,
                "in_transit": True,
                "staging": True,
                "logs_redacted": True,
            },
            "auditability": {
                "record_what": True,
                "record_who": True,
                "record_when": True,
                "record_validations": True,
            },
            "retention": {
                "staging_ttl_hours": 24,
            },
            "residency": {
                "allowed_regions": ["us-east-1"],
            },
        },
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
    doctor_parser.add_argument(
        "--bundle",
        required=False,
        help="Optional artifact bundle path to check unresolved migration templates and key management",
    )
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
