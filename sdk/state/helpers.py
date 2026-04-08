"""Helpers for creating migration execution state from existing plans."""

from __future__ import annotations

from sdk.copilot import PlanOutput
from sdk.state.models import MigrationRun
from sdk.connectors.runtime import ConnectorConfigBundle


def initialize_run_from_plan(
    plan_output: PlanOutput,
    schema: str = "public",
    connector_bundle: ConnectorConfigBundle | None = None,
) -> MigrationRun:
    """Initialize a draft migration run from a generated plan output."""
    table_names = [table_plan.table_name for table_plan in plan_output.result.resolved_spec.table_plans]
    run = MigrationRun.new(
        plan_id=plan_output.plan_id,
        schema=schema,
        selected_variant=plan_output.result.resolved_spec.selected_variant,
        pattern=plan_output.result.plan.pattern,
        table_names=table_names,
    )
    if connector_bundle:
        run.connector_config_metadata = connector_bundle.as_metadata_dict()
    return run
