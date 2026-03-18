from __future__ import annotations

from dataclasses import dataclass

from pathlib import Path

from sdk.adapters.contracts import MetadataAdapter
from sdk.artifacts.generator import ArtifactBundle, ArtifactBundleGenerator
from sdk.engine.models import EngineResult, MigrationSpec, SourceProfile, TableProfile
from sdk.engine.rule_engine import DeterministicDecisionEngine


@dataclass(frozen=True)
class PlanOutput:
    result: EngineResult
    runbook_markdown: str
    artifact_bundle: ArtifactBundle


class MigrationCopilot:
    """Facade for host apps: adapter-driven discovery + deterministic planning."""

    def __init__(self, metadata_adapter: MetadataAdapter):
        self._metadata_adapter = metadata_adapter
        self._engine = DeterministicDecisionEngine()
        self._bundle_generator = ArtifactBundleGenerator()

    def plan(
        self,
        spec: MigrationSpec,
        schema: str = "public",
        cdc_supported: bool = True,
        output_dir: str | Path = "artifacts",
    ) -> PlanOutput:
        table_names = self._metadata_adapter.list_tables(schema=schema)
        selected_tables = [name for name in table_names if not spec.objects or name in spec.objects]

        table_profiles: list[TableProfile] = []
        for table_name in selected_tables:
            table_meta = self._metadata_adapter.describe_table(table_name=table_name, schema=schema)
            table_profiles.append(
                TableProfile(
                    name=table_meta.table_name,
                    row_count=max(table_meta.row_estimate, 0),
                    size_gb=round(table_meta.size_bytes_estimate / (1024**3), 3),
                    has_primary_key=bool(table_meta.primary_key_columns),
                    primary_key_columns=table_meta.primary_key_columns,
                    column_names=[column.name for column in table_meta.columns],
                    upstream_dependencies=[fk.references_table for fk in table_meta.foreign_keys],
                )
            )

        source = SourceProfile(tables=table_profiles, cdc_supported=cdc_supported)
        result = self._engine.build(spec, source)
        runbook = _render_runbook(result)

        artifact_bundle = self._bundle_generator.generate(
            output_dir=output_dir,
            spec=spec,
            result=result,
            runbook_markdown=runbook,
            tables=table_profiles,
        )
        return PlanOutput(result=result, runbook_markdown=runbook, artifact_bundle=artifact_bundle)


def _render_runbook(result: EngineResult) -> str:
    lines = [
        "# Migration Runbook",
        "",
        f"Pattern: **{result.plan.pattern.value}**",
        f"Confidence: **{result.resolved_spec.confidence}**",
        "",
        "## Steps",
    ]

    for step in result.plan.steps:
        deps = ", ".join(step.depends_on) if step.depends_on else "none"
        lines.append(f"- **{step.id}** ({step.stage}) — depends on: {deps}. {step.details}")

    lines.append("")
    lines.append("## Step-by-Step Backfill")
    for table_plan in result.resolved_spec.table_plans:
        lines.append(
            f"{table_plan.execution_order}. Backfill **{table_plan.table_name}** in chunks of "
            f"**{table_plan.chunk_size_rows}** rows."
        )

    lines.append("")
    lines.append("## Sync + Validation Gates")
    if result.resolved_spec.requires_cdc:
        lines.append("- Start CDC/incremental sync after initial backfill.")
        lines.append("- Gate 1: replication lag remains below agreed SLO for at least 30 minutes.")
        lines.append("- Gate 2: validation queries in `validations.sql` show zero critical deltas.")
    else:
        lines.append("- CDC is optional for this pattern; run incrementals only if needed.")
        lines.append("- Validation gate: all `validations.sql` checks must pass before cutover.")

    lines.append("")
    lines.append("## Cutover Checklist")
    lines.append("- Freeze schema changes in source system.")
    lines.append("- Confirm latest validation run is successful.")
    lines.append("- Redirect reads and writes to target.")
    lines.append("- Monitor error rate and data freshness for the first hour.")

    lines.append("")
    lines.append("## Rollback Criteria")
    for item in result.plan.rollback_criteria:
        lines.append(f"- {item}")

    lines.append("")
    lines.append("## Confirm With Team")
    if result.resolved_spec.confirm_with_team:
        for item in result.resolved_spec.confirm_with_team:
            lines.append(f"- {item}")
    else:
        lines.append("- None")

    return "\n".join(lines)
