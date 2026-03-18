from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from sdk.engine.models import EngineResult, MigrationSpec, ResolvedTablePlan, TableProfile


@dataclass(frozen=True)
class ArtifactBundle:
    root: Path
    plan_json_path: Path
    runbook_path: Path
    validations_path: Path
    backfill_dir: Path
    transforms_dir: Path
    cdc_dir: Path


class ArtifactBundleGenerator:
    """Create a commit-ready artifact bundle from a resolved migration plan."""

    def generate(
        self,
        *,
        output_dir: str | Path,
        spec: MigrationSpec,
        result: EngineResult,
        runbook_markdown: str,
        tables: list[TableProfile],
    ) -> ArtifactBundle:
        root = Path(output_dir)
        backfill_dir = root / "backfill"
        transforms_dir = root / "transforms"
        cdc_dir = root / "cdc"

        root.mkdir(parents=True, exist_ok=True)
        backfill_dir.mkdir(parents=True, exist_ok=True)
        transforms_dir.mkdir(parents=True, exist_ok=True)
        cdc_dir.mkdir(parents=True, exist_ok=True)

        plan_json_path = root / "plan.json"
        plan_json_path.write_text(
            json.dumps(
                {
                    "spec": {
                        "source_type": spec.source_type,
                        "target_type": spec.target_type,
                        "objects": spec.objects,
                        "downtime_minutes": spec.downtime_minutes,
                        "policy_profile": spec.policy_profile.value,
                        "low_bandwidth_mode": spec.low_bandwidth_mode,
                    },
                    "result": result.as_dict(),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        runbook_path = root / "runbook.md"
        runbook_path.write_text(runbook_markdown, encoding="utf-8")

        validations_path = root / "validations.sql"
        validations_path.write_text(self._render_validations_sql(result, tables), encoding="utf-8")

        table_map = {table.name: table for table in tables}
        for table_plan in result.resolved_spec.table_plans:
            table = table_map[table_plan.table_name]

            backfill_path = backfill_dir / f"{table.name}.sql"
            backfill_path.write_text(self._render_backfill_sql(table_plan, table), encoding="utf-8")

            transform_path = transforms_dir / f"stg_{table.name}.sql"
            transform_path.write_text(self._render_dbt_model(table), encoding="utf-8")

            cdc_path = cdc_dir / f"{table.name}.yaml"
            cdc_path.write_text(self._render_cdc_config(table_plan, table), encoding="utf-8")

        return ArtifactBundle(
            root=root,
            plan_json_path=plan_json_path,
            runbook_path=runbook_path,
            validations_path=validations_path,
            backfill_dir=backfill_dir,
            transforms_dir=transforms_dir,
            cdc_dir=cdc_dir,
        )

    def _render_backfill_sql(self, table_plan: ResolvedTablePlan, table: TableProfile) -> str:
        if table.primary_key_columns:
            pk = table.primary_key_columns[0]
            return "\n".join(
                [
                    f"-- Backfill script for {table.name}",
                    f"-- Chunk target: {table_plan.chunk_size_rows} rows",
                    "-- Fill :lower_pk and :upper_pk for each chunk iteration.",
                    f"INSERT INTO target.{table.name}",
                    f"SELECT * FROM source.{table.name}",
                    f"WHERE {pk} > :lower_pk",
                    f"  AND {pk} <= :upper_pk",
                    f"ORDER BY {pk};",
                    "",
                ]
            )

        return "\n".join(
            [
                f"-- Backfill script for {table.name}",
                "-- No primary key detected. Use time-windowed or full-table copy with snapshot isolation.",
                f"INSERT INTO target.{table.name}",
                f"SELECT * FROM source.{table.name};",
                "",
            ]
        )

    def _render_dbt_model(self, table: TableProfile) -> str:
        column_projection = ",\n    ".join(table.column_names) if table.column_names else "*"
        return "\n".join(
            [
                "{{ config(materialized='incremental', on_schema_change='append_new_columns') }}",
                "",
                f"with source_data as (",
                f"    select",
                f"    {column_projection}",
                f"    from {{ source('raw', '{table.name}') }}",
                "),",
                "",
                "final as (",
                "    select * from source_data",
                ")",
                "",
                "select * from final",
                "",
            ]
        )

    def _render_validations_sql(self, result: EngineResult, tables: list[TableProfile]) -> str:
        lines = ["-- Validation pack generated by migration copilot", ""]
        table_map = {table.name: table for table in tables}

        for table_plan in result.resolved_spec.table_plans:
            table = table_map[table_plan.table_name]
            lines.append(f"-- {table.name}: row count parity")
            lines.append(
                "SELECT '"
                + table.name
                + "' AS table_name, (SELECT COUNT(*) FROM source."
                + table.name
                + ") AS source_count, (SELECT COUNT(*) FROM target."
                + table.name
                + ") AS target_count;"
            )
            lines.append("")

            if table.primary_key_columns:
                pk = table.primary_key_columns[0]
                lines.append(f"-- {table.name}: primary-key checksum parity")
                lines.append(
                    "SELECT '"
                    + table.name
                    + "' AS table_name, "
                    + "(SELECT SUM(hashtext(CAST("
                    + pk
                    + " AS text))) FROM source."
                    + table.name
                    + ") AS source_pk_checksum, "
                    + "(SELECT SUM(hashtext(CAST("
                    + pk
                    + " AS text))) FROM target."
                    + table.name
                    + ") AS target_pk_checksum;"
                )
                lines.append("")

        lines.append("-- Migration gate: ensure all source/target deltas are zero before cutover.")
        return "\n".join(lines) + "\n"

    def _render_cdc_config(self, table_plan: ResolvedTablePlan, table: TableProfile) -> str:
        pk_value = table.primary_key_columns if table.primary_key_columns else []
        return "\n".join(
            [
                f"table: {table.name}",
                f"enabled: {'true' if table_plan.use_cdc else 'false'}",
                "connector: TODO",
                f"primary_key: {pk_value}",
                "watermark_column: TODO",
                "lag_gate_seconds: 60",
                "notes:",
                "  - Generated stub. Fill connector-specific fields before enabling.",
                "",
            ]
        )
