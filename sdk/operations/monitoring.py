"""Operational monitoring services for migration run health and incident artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sdk.observability import PlanEvent
from sdk.state.models import (
    IncidentPack,
    MigrationRun,
    MigrationRunStatus,
    MigrationSLOStatus,
    RunHealthSnapshot,
    RollbackStatus,
    TableExecutionStatus,
)


@dataclass(frozen=True)
class SLOTargets:
    """Threshold configuration used to evaluate migration SLO compliance."""

    validation_pass_rate: float = 0.99
    cdc_lag_compliance_rate: float = 1.0
    table_failure_rate: float = 0.0
    retry_count_max: int = 3
    minimum_backfill_rows_per_minute: float = 100.0


class RunHealthMonitoringService:
    """Derive health snapshots and incident artifacts from run state and events."""

    def __init__(self, targets: SLOTargets | None = None):
        self.targets = targets or SLOTargets()

    def compute_snapshot(self, *, run: MigrationRun, events: list[PlanEvent]) -> RunHealthSnapshot:
        """Compute a current health snapshot suitable for operational dashboards."""
        scoped_events = self._filter_events_for_run(events=events, run=run)
        phase_durations = self._derive_phase_durations_seconds(scoped_events)
        retry_count = self._derive_retry_count(scoped_events)

        total_validation_checks = run.validation_summary.total_checks
        validation_pass_rate = (
            run.validation_summary.passed_checks / total_validation_checks
            if total_validation_checks
            else 1.0
        )

        cdc_tables = [item for item in run.cdc_table_progress if item.readiness.lag_threshold_seconds > 0]
        compliant_cdc_tables = [
            item
            for item in cdc_tables
            if item.lag.lag_seconds is not None and item.lag.lag_seconds <= item.readiness.lag_threshold_seconds
        ]
        cdc_lag_compliance_rate = (
            len(compliant_cdc_tables) / len(cdc_tables)
            if cdc_tables
            else 1.0
        )

        table_count = len(run.table_progress)
        failed_tables = [table.table_name for table in run.table_progress if table.status == TableExecutionStatus.FAILED]
        table_failure_rate = len(failed_tables) / table_count if table_count else 0.0

        backfill_minutes = max(phase_durations.get("backfill", 0.0) / 60.0, 0.0)
        total_rows_copied = sum(table.rows_copied for table in run.table_progress)
        backfill_throughput = (
            (total_rows_copied / backfill_minutes)
            if backfill_minutes > 0
            else None
        )

        slo_status = self._evaluate_slos(
            validation_pass_rate=validation_pass_rate,
            cdc_lag_compliance_rate=cdc_lag_compliance_rate,
            table_failure_rate=table_failure_rate,
            retry_count=retry_count,
            backfill_throughput=backfill_throughput,
        )

        latest_failure = self._latest_failure_cause(scoped_events)
        impacted_tables = sorted(set(failed_tables + self._tables_from_failure_events(scoped_events)))
        healthy = slo_status.status == "healthy" and run.status != MigrationRunStatus.FAILED

        summary = self._build_summary(run=run, healthy=healthy, slo_status=slo_status, latest_failure=latest_failure)

        return RunHealthSnapshot(
            run_id=run.run_id,
            plan_id=run.plan_id,
            run_status=run.status.value,
            current_phase=run.phase.value,
            healthy=healthy,
            summary=summary,
            slo_status=slo_status,
            time_spent_per_phase_seconds=phase_durations,
            impacted_tables=impacted_tables,
            latest_failure_cause=latest_failure,
            event_count=len(scoped_events),
        )

    def generate_incident_pack(self, *, run: MigrationRun, events: list[PlanEvent]) -> IncidentPack:
        """Build structured incident context for failed or SLO-breached runs."""
        snapshot = self.compute_snapshot(run=run, events=events)
        timeline = self._build_timeline(events=self._filter_events_for_run(events=events, run=run))
        failure_summary = snapshot.latest_failure_cause or "No explicit failure event was recorded."
        next_actions = self._suggest_next_actions(run=run, snapshot=snapshot)

        return IncidentPack(
            run_id=run.run_id,
            plan_id=run.plan_id,
            failure_cause_summary=failure_summary,
            impacted_tables=snapshot.impacted_tables,
            rollback_status=run.cutover_execution.recovery_path or RollbackStatus.NOT_PLANNED.value,
            suggested_next_actions=next_actions,
            timeline=timeline,
            run_health_snapshot=snapshot,
        )

    def write_incident_artifacts(self, *, incident_pack: IncidentPack, output_dir: str | Path) -> dict[str, Path]:
        """Persist machine-readable JSON and human-readable Markdown incident artifacts."""
        root = Path(output_dir)
        root.mkdir(parents=True, exist_ok=True)

        json_path = root / f"incident_pack_{incident_pack.run_id}.json"
        json_path.write_text(json.dumps(incident_pack.as_dict(), indent=2), encoding="utf-8")

        markdown_path = root / f"incident_pack_{incident_pack.run_id}.md"
        markdown_path.write_text(self._render_incident_markdown(incident_pack), encoding="utf-8")

        return {
            "json": json_path,
            "markdown": markdown_path,
        }

    def _evaluate_slos(
        self,
        *,
        validation_pass_rate: float,
        cdc_lag_compliance_rate: float,
        table_failure_rate: float,
        retry_count: int,
        backfill_throughput: float | None,
    ) -> MigrationSLOStatus:
        breached: list[str] = []
        warnings: list[str] = []

        if validation_pass_rate < self.targets.validation_pass_rate:
            breached.append("validation_pass_rate")

        if cdc_lag_compliance_rate < self.targets.cdc_lag_compliance_rate:
            breached.append("cdc_lag_threshold_compliance")

        if table_failure_rate > self.targets.table_failure_rate:
            breached.append("table_failure_rate")

        if retry_count > self.targets.retry_count_max:
            breached.append("retry_count")
        elif retry_count == self.targets.retry_count_max:
            warnings.append("retry_count")

        if backfill_throughput is not None:
            if backfill_throughput < self.targets.minimum_backfill_rows_per_minute:
                breached.append("backfill_throughput")
            elif backfill_throughput < self.targets.minimum_backfill_rows_per_minute * 1.2:
                warnings.append("backfill_throughput")

        status = "healthy"
        if breached:
            status = "breached"
        elif warnings:
            status = "degraded"

        return MigrationSLOStatus(
            status=status,
            breached_slos=breached,
            warning_slos=warnings,
            validation_pass_rate=validation_pass_rate,
            validation_pass_rate_target=self.targets.validation_pass_rate,
            cdc_lag_compliance_rate=cdc_lag_compliance_rate,
            cdc_lag_compliance_target=self.targets.cdc_lag_compliance_rate,
            table_failure_rate=table_failure_rate,
            table_failure_rate_target=self.targets.table_failure_rate,
            retry_count=retry_count,
            retry_count_target=self.targets.retry_count_max,
            backfill_throughput_rows_per_minute=backfill_throughput,
            backfill_throughput_target_rows_per_minute=self.targets.minimum_backfill_rows_per_minute,
        )

    def _derive_retry_count(self, events: list[PlanEvent]) -> int:
        retry_types = {"retry", "step_retried", "phase_retried"}
        retry_count = sum(1 for event in events if event.event_type in retry_types)
        retry_count += sum(
            int(event.payload.get("retry_attempt", 0))
            for event in events
            if isinstance(event.payload.get("retry_attempt"), int)
        )
        return retry_count

    def _derive_phase_durations_seconds(self, events: list[PlanEvent]) -> dict[str, float]:
        starts: dict[str, datetime] = {}
        durations: dict[str, float] = {}

        for event in sorted(events, key=lambda item: item.ts_utc):
            phase = str(event.payload.get("phase", "")).strip()
            if not phase:
                continue

            timestamp = self._parse_timestamp(event.ts_utc)
            if event.event_type == "phase_started":
                starts[phase] = timestamp
            elif event.event_type in {"phase_completed", "phase_failed"} and phase in starts:
                durations[phase] = durations.get(phase, 0.0) + max(
                    0.0,
                    (timestamp - starts.pop(phase)).total_seconds(),
                )

        return durations

    def _build_timeline(self, *, events: list[PlanEvent]) -> list[dict[str, str]]:
        important_types = {
            "orchestration_started",
            "phase_started",
            "phase_completed",
            "phase_failed",
            "approval_requested",
            "approval_denied",
            "approval_overridden",
            "orchestration_completed",
        }
        timeline = []
        for event in sorted(events, key=lambda item: item.ts_utc):
            if event.event_type not in important_types:
                continue
            phase = str(event.payload.get("phase", "n/a"))
            description = f"{event.event_type} ({event.status})"
            if event.payload.get("error"):
                description += f": {event.payload['error']}"
            timeline.append(
                {
                    "timestamp": event.ts_utc,
                    "phase": phase,
                    "event_type": event.event_type,
                    "description": description,
                }
            )
        return timeline

    def _tables_from_failure_events(self, events: list[PlanEvent]) -> list[str]:
        tables: list[str] = []
        for event in events:
            if event.event_type not in {"phase_failed", "table_failed", "validation_failed"}:
                continue
            table_name = event.payload.get("table_name")
            if isinstance(table_name, str) and table_name:
                tables.append(table_name)
            impacted = event.payload.get("impacted_tables")
            if isinstance(impacted, list):
                tables.extend([str(item) for item in impacted if item])
        return tables

    def _latest_failure_cause(self, events: list[PlanEvent]) -> str | None:
        for event in sorted(events, key=lambda item: item.ts_utc, reverse=True):
            if event.event_type == "phase_failed":
                phase = event.payload.get("phase", "unknown")
                error = event.payload.get("error", "unknown error")
                return f"Phase {phase} failed: {error}"
            if event.status == "failed":
                return f"{event.event_type} failed"
        return None

    def _suggest_next_actions(self, *, run: MigrationRun, snapshot: RunHealthSnapshot) -> list[str]:
        actions = [
            "Acknowledge the incident and assign an incident commander.",
            "Review incident timeline and execution logs for the failed phase.",
        ]

        if snapshot.impacted_tables:
            actions.append(f"Prioritize remediation for impacted tables: {', '.join(snapshot.impacted_tables)}.")

        if run.status == MigrationRunStatus.FAILED:
            actions.append("Prepare or execute rollback according to the approved rollback runbook.")

        if "cdc_lag_threshold_compliance" in snapshot.slo_status.breached_slos:
            actions.append("Stabilize CDC lag before retrying cutover or validation gates.")

        if "validation_pass_rate" in snapshot.slo_status.breached_slos:
            actions.append("Investigate validation diffs and rerun failed checks with fresh checkpoints.")

        return actions

    def _render_incident_markdown(self, incident_pack: IncidentPack) -> str:
        snapshot = incident_pack.run_health_snapshot
        timeline_lines = [
            f"- {item['timestamp']} | {item['phase']} | {item['description']}"
            for item in incident_pack.timeline
        ]
        action_lines = [f"- {item}" for item in incident_pack.suggested_next_actions]
        impacted_tables = ", ".join(incident_pack.impacted_tables) if incident_pack.impacted_tables else "None"

        return "\n".join(
            [
                f"# Incident Pack: {incident_pack.run_id}",
                "",
                "## Failure cause summary",
                incident_pack.failure_cause_summary,
                "",
                "## Impacted tables",
                impacted_tables,
                "",
                "## Rollback status",
                incident_pack.rollback_status,
                "",
                "## Run health snapshot",
                f"- Status: {snapshot.slo_status.status if snapshot else 'unknown'}",
                f"- Summary: {snapshot.summary if snapshot else 'n/a'}",
                f"- Breached SLOs: {', '.join(snapshot.slo_status.breached_slos) if snapshot and snapshot.slo_status.breached_slos else 'none'}",
                "",
                "## Timeline of key events",
                *(timeline_lines or ["- No key events captured."]),
                "",
                "## Suggested next actions",
                *(action_lines or ["- Escalate to migration owner."]),
                "",
            ]
        )

    def _filter_events_for_run(self, *, events: list[PlanEvent], run: MigrationRun) -> list[PlanEvent]:
        scoped: list[PlanEvent] = []
        for event in events:
            payload_run_id = event.payload.get("run_id")
            if payload_run_id is None or payload_run_id == run.run_id:
                scoped.append(event)
        return scoped

    def _build_summary(
        self,
        *,
        run: MigrationRun,
        healthy: bool,
        slo_status: MigrationSLOStatus,
        latest_failure: str | None,
    ) -> str:
        if run.status == MigrationRunStatus.FAILED:
            return latest_failure or "Run failed without explicit failure details."
        if healthy:
            return "Run is healthy and within SLO targets."
        if slo_status.status == "degraded":
            return "Run is degraded and approaching SLO limits."
        return "Run breached one or more SLO thresholds."

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
