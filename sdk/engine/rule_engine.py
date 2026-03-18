from __future__ import annotations

from collections import deque

from .models import (
    EngineResult,
    MigrationPattern,
    MigrationPlan,
    MigrationSpec,
    PlanStep,
    ResolvedSpec,
    ResolvedTablePlan,
    RiskItem,
    RiskLevel,
    SourceProfile,
    TableProfile,
)


class DeterministicDecisionEngine:
    """Deterministic planner from spec + source metadata to plan artifacts."""

    def build(self, spec: MigrationSpec, source: SourceProfile) -> EngineResult:
        decision_log: list[str] = []
        assumptions: list[str] = []
        confirm_with_team: list[str] = []
        risks: list[RiskItem] = []

        requires_cdc, pattern = self._apply_downtime_rules(spec, source, decision_log, assumptions)
        table_order = self._fk_order(source.tables, decision_log)

        if requires_cdc and not source.cdc_supported:
            risks.append(
                RiskItem(
                    key="cdc_not_supported",
                    level=RiskLevel.HIGH,
                    rationale="Downtime requirement implies CDC, but source CDC support is unavailable.",
                )
            )
            confirm_with_team.append("Confirm source CDC capability or relax downtime target.")

        table_plans: list[ResolvedTablePlan] = []
        table_index = source.by_name()
        for order, table_name in enumerate(table_order, start=1):
            table = table_index[table_name]
            use_cdc, table_assumptions, table_confirms = self._apply_cdc_readiness_rules(
                table=table,
                source_cdc_supported=source.cdc_supported,
                requires_cdc=requires_cdc,
                decision_log=decision_log,
            )
            assumptions.extend(table_assumptions)
            confirm_with_team.extend(table_confirms)

            chunk_size = self._chunk_size_for(table, spec.low_bandwidth_mode, decision_log)
            table_plans.append(
                ResolvedTablePlan(
                    table_name=table.name,
                    use_cdc=use_cdc,
                    chunk_size_rows=chunk_size,
                    execution_order=order,
                    assumptions=table_assumptions,
                )
            )
            risks.extend(self._risk_items_for(table=table, use_cdc=use_cdc, low_bandwidth_mode=spec.low_bandwidth_mode))

        confidence = self._confidence_for(risks)
        resolved = ResolvedSpec(
            pattern=pattern,
            requires_cdc=requires_cdc,
            table_plans=table_plans,
            assumptions=_stable_dedup(assumptions),
            confidence=confidence,
            confirm_with_team=_stable_dedup(confirm_with_team),
            decision_log=decision_log,
            risks=_stable_risks(risks),
        )
        plan = self._build_plan(pattern, table_plans)

        return EngineResult(resolved_spec=resolved, plan=plan)

    def _apply_downtime_rules(
        self,
        spec: MigrationSpec,
        source: SourceProfile,
        decision_log: list[str],
        assumptions: list[str],
    ) -> tuple[bool, MigrationPattern]:
        if spec.downtime_minutes is None:
            assumptions.append("Downtime not provided; defaulting to conservative CDC strategy.")
            decision_log.append("downtime=unknown => requires_cdc=True, pattern=backfill_plus_cdc")
            return True, MigrationPattern.BACKFILL_CDC

        if spec.downtime_minutes <= 5:
            decision_log.append("downtime<=5m => requires_cdc=True, pattern=backfill_plus_cdc")
            return True, MigrationPattern.BACKFILL_CDC

        if spec.downtime_minutes <= 30 and source.cdc_supported:
            decision_log.append("5m<downtime<=30m and cdc_supported => requires_cdc=optional, pattern=phased")
            return False, MigrationPattern.PHASED

        decision_log.append("downtime>30m or no cdc => requires_cdc=False, pattern=big_bang")
        return False, MigrationPattern.BIG_BANG

    def _apply_cdc_readiness_rules(
        self,
        table: TableProfile,
        source_cdc_supported: bool,
        requires_cdc: bool,
        decision_log: list[str],
    ) -> tuple[bool, list[str], list[str]]:
        assumptions: list[str] = []
        confirm_with_team: list[str] = []

        if not requires_cdc:
            decision_log.append(f"{table.name}: cdc_not_required")
            return False, assumptions, confirm_with_team

        if not source_cdc_supported:
            assumptions.append(f"{table.name}: CDC requested but source capability is unavailable.")
            decision_log.append(f"{table.name}: cdc_required_but_source_not_ready")
            return False, assumptions, confirm_with_team

        if not table.has_primary_key:
            assumptions.append(f"{table.name}: primary key missing; CDC unsafe without stable key.")
            confirm_with_team.append(f"{table.name}: confirm surrogate key strategy for CDC.")
            decision_log.append(f"{table.name}: no_primary_key => cdc_disabled_for_table")
            return False, assumptions, confirm_with_team

        if table.estimated_writes_per_minute is None:
            assumptions.append(f"{table.name}: write rate unknown; applying cautious CDC throttling.")
            confirm_with_team.append(f"{table.name}: provide write-rate estimate to tune sync lag gates.")
            decision_log.append(f"{table.name}: write_rate_unknown => cdc_enabled_with_caution")
            return True, assumptions, confirm_with_team

        decision_log.append(f"{table.name}: cdc_enabled")
        return True, assumptions, confirm_with_team

    def _chunk_size_for(self, table: TableProfile, low_bandwidth_mode: bool, decision_log: list[str]) -> int:
        if table.size_gb >= 500:
            chunk = 50_000
        elif table.size_gb >= 100:
            chunk = 200_000
        elif table.size_gb >= 10:
            chunk = 1_000_000
        else:
            chunk = 5_000_000

        if low_bandwidth_mode:
            chunk = max(10_000, chunk // 2)
            decision_log.append(f"{table.name}: low_bandwidth_mode => chunk_size={chunk}")
        else:
            decision_log.append(f"{table.name}: chunk_size={chunk}")

        return chunk

    def _fk_order(self, tables: list[TableProfile], decision_log: list[str]) -> list[str]:
        graph: dict[str, set[str]] = {t.name: set() for t in tables}
        indegree: dict[str, int] = {t.name: 0 for t in tables}

        for table in tables:
            for dep in table.upstream_dependencies:
                if dep in graph:
                    graph[dep].add(table.name)
                    indegree[table.name] += 1

        queue = deque(sorted([name for name, degree in indegree.items() if degree == 0]))
        ordered: list[str] = []
        while queue:
            node = queue.popleft()
            ordered.append(node)
            for child in sorted(graph[node]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)

        if len(ordered) != len(tables):
            unresolved = sorted(set(graph.keys()) - set(ordered))
            ordered.extend(unresolved)
            decision_log.append(f"fk_cycle_detected => fallback_lexicographic_for={','.join(unresolved)}")
        else:
            decision_log.append(f"fk_order_resolved => {','.join(ordered)}")

        return ordered

    def _risk_items_for(self, table: TableProfile, use_cdc: bool, low_bandwidth_mode: bool) -> list[RiskItem]:
        risks: list[RiskItem] = []

        if use_cdc and not table.has_primary_key:
            risks.append(
                RiskItem(
                    key=f"{table.name}:no_pk_with_cdc",
                    level=RiskLevel.HIGH,
                    rationale="Table lacks a primary key while CDC is required.",
                )
            )

        if table.size_gb >= 100 and low_bandwidth_mode:
            risks.append(
                RiskItem(
                    key=f"{table.name}:large_table_low_bandwidth",
                    level=RiskLevel.HIGH,
                    rationale="Large table backfill under low bandwidth may miss migration window.",
                )
            )

        if table.schema_drift_likelihood >= 0.7:
            risks.append(
                RiskItem(
                    key=f"{table.name}:schema_drift",
                    level=RiskLevel.HIGH,
                    rationale="High likelihood of schema drift during migration window.",
                )
            )
        elif table.schema_drift_likelihood >= 0.3:
            risks.append(
                RiskItem(
                    key=f"{table.name}:schema_drift",
                    level=RiskLevel.MEDIUM,
                    rationale="Moderate likelihood of schema drift; add DDL monitoring gate.",
                )
            )

        return risks

    def _confidence_for(self, risks: list[RiskItem]) -> float:
        score = 0.95
        for risk in risks:
            if risk.level == RiskLevel.HIGH:
                score -= 0.15
            elif risk.level == RiskLevel.MEDIUM:
                score -= 0.07
            else:
                score -= 0.02
        return max(0.1, round(score, 2))

    def _build_plan(self, pattern: MigrationPattern, table_plans: list[ResolvedTablePlan]) -> MigrationPlan:
        steps: list[PlanStep] = [
            PlanStep(id="prepare", stage="prepare", depends_on=[], details="Freeze schema contracts and configure connections."),
            PlanStep(id="backfill", stage="backfill", depends_on=["prepare"], details="Backfill tables in execution order with configured chunk sizes."),
        ]

        if pattern in {MigrationPattern.BACKFILL_CDC, MigrationPattern.PHASED}:
            steps.append(
                PlanStep(id="sync", stage="sync", depends_on=["backfill"], details="Run incremental sync/CDC until lag gates pass.")
            )
            validation_dep = "sync"
        else:
            validation_dep = "backfill"

        steps.extend(
            [
                PlanStep(
                    id="validate",
                    stage="validation",
                    depends_on=[validation_dep],
                    details="Run row-count, aggregate, checksum, and FK integrity checks.",
                ),
                PlanStep(
                    id="cutover",
                    stage="cutover",
                    depends_on=["validate"],
                    details="Switch reads/writes to target after all validation gates pass.",
                ),
            ]
        )

        rollback = [
            "Abort if critical validation checks fail.",
            "Abort if replication lag does not converge before cutover window.",
            "Abort if schema drift introduces incompatible DDL.",
        ]

        return MigrationPlan(pattern=pattern, steps=steps, rollback_criteria=rollback)


def _stable_dedup(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _stable_risks(risks: list[RiskItem]) -> list[RiskItem]:
    grouped: dict[str, RiskItem] = {}
    for risk in risks:
        grouped[risk.key] = risk
    return [grouped[key] for key in sorted(grouped.keys())]
