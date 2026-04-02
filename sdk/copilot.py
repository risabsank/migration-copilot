from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sdk.adapters.contracts import MetadataAdapter
from sdk.artifacts.generator import ArtifactBundle, ArtifactBundleGenerator
from sdk.engine.ai_agents import MultiAgentDecisionEngine
from sdk.engine.models import CriticalityTier, EngineResult, MigrationSpec, SourceProfile, TableProfile
from sdk.observability import EventCollector, PlanEvent


@dataclass(frozen=True)
class PlanOutput:
    result: EngineResult
    runbook_markdown: str
    artifact_bundle: ArtifactBundle
    plan_id: str
    events: list[PlanEvent]
    events_path: Path

class MigrationCopilot:
    """Facade for host apps: adapter-driven discovery + AI-first planning."""

    def __init__(self, metadata_adapter: MetadataAdapter):
        self._metadata_adapter = metadata_adapter
        self._engine = MultiAgentDecisionEngine()
        self._bundle_generator = ArtifactBundleGenerator()

    def plan(
        self,
        spec: MigrationSpec,
        schema: str = "public",
        cdc_supported: bool = True,
        cdc_log_mode: str | None = "wal",
        output_dir: str | Path = "artifacts",
        plan_id: str | None = None,
    ) -> PlanOutput:
        collector = EventCollector(plan_id=plan_id)
        collector.emit(
            step="intake_spec_builder",
            status="completed",
            rule_ids=["default_policy_profile", "default_plan_only_mode"],
            payload={
                "source_type": spec.source_type,
                "target_type": spec.target_type,
                "object_count": len(spec.objects),
            },
        )

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
                    criticality=_infer_criticality(table_meta.table_name),
                )
            )
        
        collector.emit(
            step="discovery_profiler",
            status="completed",
            rule_ids=["metadata_adapter_list_tables", "metadata_adapter_describe_table"],
            payload={"schema": schema, "discovered_tables": table_names, "selected_tables": selected_tables},
        )

        source = SourceProfile(tables=table_profiles, cdc_supported=cdc_supported, cdc_log_mode=cdc_log_mode)
        result = self._engine.build(spec, source)

        collector.emit(
            step="ai_strategy_agent",
            status="completed",
            rule_ids=["llm_strategy_reasoning"],
            payload={
                "selected_variant": result.resolved_spec.selected_variant,
                "ai_primary": result.resolved_spec.ai_primary,
            },
        )
        collector.emit(
            step="ai_risk_agent",
            status="completed",
            rule_ids=["llm_risk_reasoning", "risk_merge_guardrails"],
            payload={"risk_count": len(result.resolved_spec.risks)},
        )
        collector.emit(
            step="ai_review_agent",
            status="completed",
            rule_ids=["llm_review_reasoning", "deterministic_guardrails"],
            confidence=result.resolved_spec.confidence,
            payload={"agent_notes": result.resolved_spec.ai_agent_notes},
        )

        collector.emit(
            step="constraint_resolver",
            status="completed",
            rule_ids=[
                "downtime_rules",
                "cdc_readiness_rules",
                "chunk_size_rules",
                "fk_order_rules",
            ],
            confidence=result.resolved_spec.confidence,
            payload={
                "selected_variant": result.resolved_spec.selected_variant,
                "pattern": result.plan.pattern.value,
                "risk_count": len(result.resolved_spec.risks),
            },
        )
        collector.emit(
            step="strategy_planner",
            status="completed",
            rule_ids=["plan_dag_builder", "rollback_criteria_rules"],
            confidence=result.resolved_spec.confidence,
            payload={"step_count": len(result.plan.steps)},
        )

        runbook = _render_runbook(result)
        collector.emit(step="explainer_auditor", status="completed", rule_ids=["runbook_renderer"])

        artifact_bundle = self._bundle_generator.generate(
            output_dir=output_dir,
            spec=spec,
            result=result,
            runbook_markdown=runbook,
            tables=table_profiles,
        )

        collector.emit(
            step="artifact_generator",
            status="completed",
            rule_ids=["bundle_templates", "validation_sql_templates", "cdc_config_templates"],
            payload={"bundle_root": str(artifact_bundle.root)},
        )
        events_path = collector.write_jsonl(artifact_bundle.root / "events.jsonl")

        return PlanOutput(
            result=result,
            runbook_markdown=runbook,
            artifact_bundle=artifact_bundle,
            plan_id=collector.plan_id,
            events=collector.events,
            events_path=events_path,
        )

def _render_runbook(result: EngineResult) -> str:
    lines = [
        "# Migration Runbook",
        "",
        f"Pattern: **{result.plan.pattern.value}**",
        f"Selected plan variant: **{result.resolved_spec.selected_variant}**",
        f"Confidence: **{result.resolved_spec.confidence}**",
        f"AI-first planner: **{'enabled' if result.resolved_spec.ai_primary else 'disabled'}**",
        "",
        "## AI Multi-Agent Notes",
    ]

    if result.resolved_spec.ai_agent_notes:
        for note in result.resolved_spec.ai_agent_notes:
            lines.append(f"- {note}")
    else:
        lines.append("- No AI agent notes captured.")

    lines.extend([
        "",
        "## Available Plan Variants",
    ])

    for variant in result.resolved_spec.plan_variants:
        lines.append(f"- {variant}")

    lines.extend(
        [
            "",
            "## Steps",
        ]
    )

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
    lines.append("## CDC Sync Plan")
    cdc_plan = result.resolved_spec.cdc_plan
    lines.append(f"- CDC readiness: **{'ready' if cdc_plan.ready else 'not ready'}**")
    lines.append(f"- Log mode: **{cdc_plan.log_mode}**")
    lines.append(f"- Lag gate: replication lag <= **{cdc_plan.lag_gate_seconds}s**")
    lines.append(f"- Stabilization window: **{cdc_plan.lag_stabilization_minutes} minutes**")
    lines.append(f"- Reprocessing strategy: {cdc_plan.reprocessing_strategy}")
    lines.append(f"- Dedupe strategy: {cdc_plan.dedupe_strategy}")
    if cdc_plan.prerequisites:
        lines.append("- Readiness prerequisites:")
        for item in cdc_plan.prerequisites:
            lines.append(f"  - {item}")

    lines.append("")
    lines.append("## Sync + Validation Gates")
    if result.resolved_spec.requires_cdc:
        lines.append("- Start CDC/incremental sync after initial backfill.")
        lines.append(
            f"- Gate 1: replication lag remains <= {cdc_plan.lag_gate_seconds}s for {cdc_plan.lag_stabilization_minutes} minutes."
        )
        lines.append("- Gate 2: validation queries in `validations.sql` show zero critical deltas.")
    else:
        lines.append("- CDC is optional for this pattern; run incrementals only if needed.")
        lines.append("- Validation gate: all `validations.sql` checks must pass before cutover.")

    if result.resolved_spec.phased_cutover_groups:
        lines.append("")
        lines.append("## Phased Cutover Groups")
        for idx, group in enumerate(result.resolved_spec.phased_cutover_groups, start=1):
            lines.append(f"- Wave {idx}: {', '.join(group)}")

    if result.resolved_spec.streaming_replay_plan is not None:
        streaming = result.resolved_spec.streaming_replay_plan
        lines.append("")
        lines.append("## Streaming/Event Replay Plan")
        lines.append(f"- Enabled: **{streaming.enabled}**")
        lines.append(f"- Topic pattern: `{streaming.source_topic_pattern}`")
        lines.append(f"- Projection rebuild: {streaming.projection_rebuild_strategy}")
        lines.append(f"- Cutover gate: {streaming.cutover_gate}")
    
    lines.append("")
    lines.append("## Estimate")
    if result.resolved_spec.estimate is not None:
        est = result.resolved_spec.estimate
        lines.append(f"- Duration: **{est.estimated_duration_minutes} minutes**")
        lines.append(f"- Peak workers: **{est.peak_parallel_workers}**")
        lines.append(f"- Compute credits: **{est.compute_credits}**")

    lines.append("")
    lines.append("## Compliance Gates")
    for gate in result.resolved_spec.compliance_gates:
        lines.append(f"- {gate.name}: {'PASS' if gate.passed else 'FAIL'} — {gate.detail}")
    
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

def _infer_criticality(table_name: str) -> CriticalityTier:
    lowered = table_name.lower()
    if any(token in lowered for token in ["payments", "ledger", "orders"]):
        return CriticalityTier.TIER0
    if any(token in lowered for token in ["users", "accounts", "customers"]):
        return CriticalityTier.TIER1
    return CriticalityTier.TIER2