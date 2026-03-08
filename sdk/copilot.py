from __future__ import annotations

from dataclasses import dataclass

from sdk.adapters.contracts import MetadataAdapter
from sdk.engine.models import EngineResult, MigrationSpec, SourceProfile, TableProfile
from sdk.engine.rule_engine import DeterministicDecisionEngine


@dataclass(frozen=True)
class PlanOutput:
    result: EngineResult
    runbook_markdown: str


class MigrationCopilot:
    """Facade for host apps: adapter-driven discovery + deterministic planning."""

    def __init__(self, metadata_adapter: MetadataAdapter):
        self._metadata_adapter = metadata_adapter
        self._engine = DeterministicDecisionEngine()

    def plan(self, spec: MigrationSpec, schema: str = "public", cdc_supported: bool = True) -> PlanOutput:
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
                    upstream_dependencies=[fk.references_table for fk in table_meta.foreign_keys],
                )
            )

        source = SourceProfile(tables=table_profiles, cdc_supported=cdc_supported)
        result = self._engine.build(spec, source)
        runbook = _render_runbook(result)
        return PlanOutput(result=result, runbook_markdown=runbook)


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
