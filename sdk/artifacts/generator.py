from __future__ import annotations

import hashlib
import hmac
import json
import os
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

    def generate(self, *, output_dir: str | Path, spec: MigrationSpec, result: EngineResult, runbook_markdown: str, tables: list[TableProfile]) -> ArtifactBundle:
        root = Path(output_dir)
        backfill_dir = root / "backfill"
        transforms_dir = root / "transforms"
        cdc_dir = root / "cdc"
        sql_packs_dir = root / "sql_packs"
        dags_dir = root / "dags"
        governance_dir = root / "governance"
        ai_dir = root / "ai"

        for d in [root, backfill_dir, transforms_dir, cdc_dir, sql_packs_dir, dags_dir, governance_dir, ai_dir]:
            d.mkdir(parents=True, exist_ok=True)

        plan_json_path = root / "plan.json"
        plan_json_path.write_text(json.dumps({"spec": {"source_type": spec.source_type, "target_type": spec.target_type, "objects": spec.objects, "downtime_minutes": spec.downtime_minutes, "policy_profile": spec.policy_profile.value, "low_bandwidth_mode": spec.low_bandwidth_mode, "source_of_truth": spec.source_of_truth}, "result": result.as_dict()}, indent=2), encoding="utf-8")

        runbook_path = root / "runbook.md"
        runbook_path.write_text(runbook_markdown, encoding="utf-8")

        validations_path = root / "validations.sql"
        validations_path.write_text(self._render_validations_sql(result, tables), encoding="utf-8")

        table_map = {table.name: table for table in tables}
        for table_plan in result.resolved_spec.table_plans:
            table = table_map[table_plan.table_name]
            (backfill_dir / f"{table.name}.sql").write_text(self._render_backfill_sql(table_plan, table), encoding="utf-8")
            (transforms_dir / f"stg_{table.name}.sql").write_text(self._render_dbt_model(table), encoding="utf-8")
            for connector in ["debezium", "fivetran", "native"]:
                (cdc_dir / f"{table.name}.{connector}.yaml").write_text(self._render_cdc_config(table_plan, table, connector), encoding="utf-8")

        for dialect in ["postgres", "snowflake", "bigquery"]:
            (sql_packs_dir / f"validations_{dialect}.sql").write_text(self._render_validations_sql(result, tables, dialect=dialect), encoding="utf-8")

        (dags_dir / "airflow_dag.py").write_text(self._render_airflow_dag(result), encoding="utf-8")
        (dags_dir / "dagster_job.py").write_text(self._render_dagster_dag(result), encoding="utf-8")

        self._write_ai_artifacts(ai_dir, result)
        self._write_governance_artifacts(governance_dir, root)

        return ArtifactBundle(root=root, plan_json_path=plan_json_path, runbook_path=runbook_path, validations_path=validations_path, backfill_dir=backfill_dir, transforms_dir=transforms_dir, cdc_dir=cdc_dir)

    def _write_governance_artifacts(self, governance_dir: Path, root: Path) -> None:
        checksums: dict[str, str] = {}
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.parent != governance_dir:
                checksums[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest = json.dumps({"checksums": checksums}, indent=2)
        (governance_dir / "checksums.json").write_text(manifest, encoding="utf-8")

        signing_key = os.getenv("MIGRATION_COPILOT_SIGNING_KEY", "dev-signing-key")
        signature = hmac.new(signing_key.encode("utf-8"), manifest.encode("utf-8"), hashlib.sha256).hexdigest()
        (governance_dir / "bundle.signature").write_text(signature + "\n", encoding="utf-8")

    def _write_ai_artifacts(self, ai_dir: Path, result: EngineResult) -> None:
        (ai_dir / "explainability_trace.json").write_text(json.dumps({"trace": result.resolved_spec.explainability_trace}, indent=2), encoding="utf-8")
        (ai_dir / "evaluation_metrics.json").write_text(json.dumps({"confidence": result.resolved_spec.confidence, "risk_count": len(result.resolved_spec.risks), "fallback_triggered": any("fallback" in x for x in result.resolved_spec.explainability_trace)}, indent=2), encoding="utf-8")
        (ai_dir / "offline_benchmark.json").write_text(json.dumps({"suite": "baseline-v1", "cases": [{"name": "low_downtime_cdc", "expected_pattern": "backfill_plus_cdc"}, {"name": "high_downtime_batch", "expected_pattern": "big_bang"}]}, indent=2), encoding="utf-8")

    def _render_backfill_sql(self, table_plan: ResolvedTablePlan, table: TableProfile) -> str:
        if table.primary_key_columns:
            pk = table.primary_key_columns[0]
            return f"-- Backfill script for {table.name}\n-- Chunk target: {table_plan.chunk_size_rows} rows\nINSERT INTO target.{table.name}\nSELECT * FROM source.{table.name}\nWHERE {pk} > :lower_pk AND {pk} <= :upper_pk\nORDER BY {pk};\n"
        return f"-- Backfill script for {table.name}\nINSERT INTO target.{table.name}\nSELECT * FROM source.{table.name};\n"

        

    def _render_dbt_model(self, table: TableProfile) -> str:
        cols = ",\n    ".join(table.column_names) if table.column_names else "*"
        return "\n".join(["{{ config(materialized='incremental', on_schema_change='append_new_columns') }}", "", "with source_data as (", "    select", f"    {cols}", f"    from {{ source('raw', '{table.name}') }}", "),", "", "select * from source_data", ""])

    def _render_validations_sql(self, result: EngineResult, tables: list[TableProfile], dialect: str = "postgres") -> str:
        checksum_fn = "hashtext" if dialect == "postgres" else "hash" if dialect == "snowflake" else "farm_fingerprint"
        lines = [f"-- Validation pack ({dialect})", ""]
        table_map = {table.name: table for table in tables}
        for table_plan in result.resolved_spec.table_plans:
            table = table_map[table_plan.table_name]
            lines.append(f"SELECT '{table.name}' AS table_name, (SELECT COUNT(*) FROM source.{table.name}) AS source_count, (SELECT COUNT(*) FROM target.{table.name}) AS target_count;")

            if table.primary_key_columns:
                pk = table.primary_key_columns[0]
                lines.append(f"SELECT '{table.name}' AS table_name, (SELECT SUM({checksum_fn}(CAST({pk} AS STRING))) FROM source.{table.name}) AS source_pk_checksum, (SELECT SUM({checksum_fn}(CAST({pk} AS STRING))) FROM target.{table.name}) AS target_pk_checksum;")
            lines.append("")
        return "\n".join(lines) + "\n"

    
    def _render_cdc_config(self, table_plan: ResolvedTablePlan, table: TableProfile, connector: str) -> str:
        return f"table: {table.name}\nenabled: {'true' if table_plan.use_cdc else 'false'}\nconnector: {connector}\nprimary_key: {table.primary_key_columns if table.primary_key_columns else []}\nwatermark_column: TODO\n"

    def _render_airflow_dag(self, result: EngineResult) -> str:
        return "\n".join([
            "from airflow import DAG",
            "from airflow.operators.empty import EmptyOperator",
            "from datetime import datetime",
            "with DAG('migration_plan', start_date=datetime(2024,1,1), schedule=None, catchup=False) as dag:",
            *[f"    {s.id.replace('-', '_')} = EmptyOperator(task_id='{s.id}')" for s in result.plan.steps],
            "",
        ])

    def _render_dagster_dag(self, result: EngineResult) -> str:
        ops = "\n".join([f"@op\ndef {s.id.replace('-', '_')}():\n    return '{s.stage}'\n" for s in result.plan.steps])
        graph_call = "\n    ".join([f"{s.id.replace('-', '_')}()" for s in result.plan.steps])
        return f"from dagster import graph, op\n\n{ops}\n@graph\ndef migration_graph():\n    {graph_call}\n"