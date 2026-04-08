"""Helpers for creating migration execution state from existing plans."""

from __future__ import annotations

from sdk.copilot import PlanOutput
from sdk.state.models import MigrationRun


def initialize_run_from_plan(plan_output: PlanOutput, schema: str = "public") -> MigrationRun:
    """Initialize a draft migration run from a generated plan output."""
    table_names = [table_plan.table_name for table_plan in plan_output.result.resolved_spec.table_plans]
    return MigrationRun.new(
        plan_id=plan_output.plan_id,
        schema=schema,
        selected_variant=plan_output.result.resolved_spec.selected_variant,
        pattern=plan_output.result.plan.pattern,
        table_names=table_names,
    )
