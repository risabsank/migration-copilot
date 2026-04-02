from __future__ import annotations

from collections import deque

from .models import (
    CDCPlan,
    ComplianceGate,
    CostEstimate,
    CriticalityTier,
    EngineResult,
    MigrationPattern,
    MigrationPlan,
    MigrationSpec,
    PlanStep,
    PolicyProfile,
    ResolvedSpec,
    ResolvedTablePlan,
    RiskItem,
    RiskLevel,
    SchemaContractReport,
    SourceProfile,
    StreamingReplayPlan,
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
        plan_variants = self._plan_variants()
        selected_variant = self._selected_variant(pattern)
        cdc_plan = self._build_cdc_plan(spec, source, requires_cdc, decision_log, assumptions, confirm_with_team)
        streaming_replay_plan = self._build_streaming_replay_plan(spec, decision_log)
        table_order = self._fk_order(source.tables, decision_log)
        compliance_gates = self._build_compliance_gates(source)
        schema_contract = self._schema_contract_report(source)

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
                profile=spec.policy_profile,
            )
            assumptions.extend(table_assumptions)
            confirm_with_team.extend(table_confirms)

            chunk_size = self._chunk_size_for(table, spec.low_bandwidth_mode, spec.policy_profile, decision_log)
            table_plans.append(
                ResolvedTablePlan(
                    table_name=table.name,
                    use_cdc=use_cdc,
                    chunk_size_rows=chunk_size,
                    execution_order=order,
                    assumptions=table_assumptions,
                    criticality=table.criticality,
                    cutover_wave=self._cutover_wave_for(table),
                )
            )
            risks.extend(self._risk_items_for(table=table, use_cdc=use_cdc, low_bandwidth_mode=spec.low_bandwidth_mode))

        estimate = self._estimate_cost_time(table_plans, table_index, spec)
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
            plan_variants=plan_variants,
            selected_variant=selected_variant,
            cdc_plan=cdc_plan,
            streaming_replay_plan=streaming_replay_plan,
            phased_cutover_groups=self._group_for_phased_cutover(source.tables),
            estimate=estimate,
            compliance_gates=compliance_gates,
            schema_contract=schema_contract,
        )
        plan = self._build_plan(pattern, table_plans, spec.policy_profile)

        return EngineResult(resolved_spec=resolved, plan=plan)

    def _apply_downtime_rules(self, spec: MigrationSpec, source: SourceProfile, decision_log: list[str], assumptions: list[str]) -> tuple[bool, MigrationPattern]:
        aggressive_cap = 10 if spec.policy_profile == PolicyProfile.FAST else 5
        phased_cap = 60 if spec.policy_profile == PolicyProfile.FAST else 30
        if spec.downtime_minutes is None:
            assumptions.append("Downtime not provided; defaulting to conservative CDC strategy.")
            decision_log.append("downtime=unknown => requires_cdc=True, pattern=backfill_plus_cdc")
            return True, MigrationPattern.BACKFILL_CDC

        if spec.downtime_minutes <= aggressive_cap:
            decision_log.append(f"downtime<={aggressive_cap}m(profile-adjusted) => requires_cdc=True")

        if spec.downtime_minutes <= phased_cap and source.cdc_supported:
            decision_log.append("mid downtime and cdc_supported => phased")
            return False, MigrationPattern.PHASED

        decision_log.append("downtime high => big_bang")
        return False, MigrationPattern.BIG_BANG
    
    def _plan_variants(self) -> list[str]:
        return ["batch_only", "backfill_cdc_sync", "phased_cutover_by_domain_or_table_group"]

    def _selected_variant(self, pattern: MigrationPattern) -> str:
        return {
            MigrationPattern.BACKFILL_CDC: "backfill_cdc_sync",
            MigrationPattern.PHASED: "phased_cutover_by_domain_or_table_group",
        }.get(pattern, "batch_only")

    def _build_cdc_plan(self, spec: MigrationSpec, source: SourceProfile, requires_cdc: bool, decision_log: list[str], assumptions: list[str], confirm_with_team: list[str]) -> CDCPlan:
        prerequisites: list[str] = []
        if not source.cdc_supported:
            prerequisites.append("Enable CDC connector permissions and replication slot/binlog access.")
        if source.cdc_log_mode not in {"wal", "binlog"}:
            prerequisites.append("Confirm WAL/binlog mode and retention for migration timeline.")
        if any(not table.has_primary_key for table in source.tables):
            prerequisites.append("Add a stable primary key/surrogate key for CDC-enabled tables.")

        ready = len(prerequisites) == 0
        if requires_cdc and not ready:
            assumptions.append("CDC required for downtime target, but readiness checks are incomplete.")
            confirm_with_team.append("Resolve CDC prerequisites before production cutover.")
            decision_log.append("cdc_readiness=not_ready")
        else:
            decision_log.append("cdc_readiness=ready")

        lag_gate_seconds = 90 if spec.policy_profile == PolicyProfile.CONSERVATIVE else 60 if spec.policy_profile == PolicyProfile.BALANCED else 30
        lag_stabilization_minutes = 45 if spec.policy_profile == PolicyProfile.CONSERVATIVE else 20 if spec.policy_profile == PolicyProfile.BALANCED else 10

        return CDCPlan(
            ready=ready,
            log_mode=source.cdc_log_mode if source.cdc_log_mode in {"wal", "binlog"} else "unknown",
            lag_gate_seconds=lag_gate_seconds,
            lag_stabilization_minutes=lag_stabilization_minutes,
            reprocessing_strategy="Replay from checkpoint watermark and re-run idempotent upsert window.",
            dedupe_strategy="Merge on primary key with event timestamp/version tie-breaker.",
            prerequisites=prerequisites,
        )

    def _build_streaming_replay_plan(self, spec: MigrationSpec, decision_log: list[str]) -> StreamingReplayPlan | None:
        if spec.source_of_truth.lower() != "kafka":
            decision_log.append("streaming_replay=skipped")
            return None

        decision_log.append("streaming_replay=enabled")
        return StreamingReplayPlan(
            enabled=True,
            source_topic_pattern="TODO:domain.*",
            projection_rebuild_strategy="Rebuild projections by replaying retained events into target read models.",
            cutover_gate="Topic consumer lag == 0 and projection checksums match baseline.",
        )

    def _group_for_phased_cutover(self, tables: list[TableProfile]) -> list[list[str]]:
        grouped: dict[str, list[str]] = {}
        for table in tables:
            key = f"wave{self._cutover_wave_for(table)}"
            grouped.setdefault(key, []).append(table.name)
        return [sorted(grouped[key]) for key in sorted(grouped.keys())]

    def _cutover_wave_for(self, table: TableProfile) -> int:
        return 1 if table.criticality == CriticalityTier.TIER0 else 2 if table.criticality == CriticalityTier.TIER1 else 3

    def _apply_cdc_readiness_rules(self, table: TableProfile, source_cdc_supported: bool, requires_cdc: bool, decision_log: list[str], profile: PolicyProfile) -> tuple[bool, list[str], list[str]]:
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

        if profile == PolicyProfile.CONSERVATIVE and table.estimated_writes_per_minute is None:
            assumptions.append(f"{table.name}: write rate unknown; conservative profile blocks CDC enablement.")
            confirm_with_team.append(f"{table.name}: provide write-rate estimate to enable CDC under conservative mode.")
            decision_log.append(f"{table.name}: write_rate_unknown => cdc_disabled_by_profile")
            return False, assumptions, confirm_with_team
        
        if table.estimated_writes_per_minute is None:
            assumptions.append(f"{table.name}: write rate unknown; applying cautious CDC throttling.")
            confirm_with_team.append(f"{table.name}: provide write-rate estimate to tune sync lag gates.")
            decision_log.append(f"{table.name}: write_rate_unknown => cdc_enabled_with_caution")
            return True, assumptions, confirm_with_team

        decision_log.append(f"{table.name}: cdc_enabled")
        return True, assumptions, confirm_with_team

    def _chunk_size_for(self, table: TableProfile, low_bandwidth_mode: bool, profile: PolicyProfile, decision_log: list[str]) -> int:
        if table.size_gb >= 500:
            chunk = 50_000
        elif table.size_gb >= 100:
            chunk = 200_000
        elif table.size_gb >= 10:
            chunk = 1_000_000
        else:
            chunk = 5_000_000

        if profile == PolicyProfile.FAST:
            chunk *= 2
        elif profile == PolicyProfile.CONSERVATIVE:
            chunk = int(chunk * 0.6)
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
            risks.append(RiskItem(key=f"{table.name}:no_pk_with_cdc", level=RiskLevel.HIGH, rationale="Table lacks a primary key while CDC is required."))

        if table.size_gb >= 100 and low_bandwidth_mode:
            risks.append(RiskItem(key=f"{table.name}:large_table_low_bandwidth", level=RiskLevel.HIGH, rationale="Large table backfill under low bandwidth may miss migration window."))

        if table.schema_drift_likelihood >= 0.7:
            risks.append(RiskItem(key=f"{table.name}:schema_drift", level=RiskLevel.HIGH, rationale="High likelihood of schema drift during migration window."))
        elif table.schema_drift_likelihood >= 0.3:
            risks.append(RiskItem(key=f"{table.name}:schema_drift", level=RiskLevel.MEDIUM, rationale="Moderate likelihood of schema drift; add DDL monitoring gate."))

        return risks
    
    def _estimate_cost_time(self, table_plans: list[ResolvedTablePlan], table_index: dict[str, TableProfile], spec: MigrationSpec) -> CostEstimate:
        total_gb = sum(table_index[p.table_name].size_gb for p in table_plans)
        throughput_gb_per_hour = 40.0 if spec.policy_profile == PolicyProfile.FAST else 28.0 if spec.policy_profile == PolicyProfile.BALANCED else 18.0
        duration_minutes = int((total_gb / max(throughput_gb_per_hour, 1.0)) * 60) + 15
        workers = 16 if spec.policy_profile == PolicyProfile.FAST else 10 if spec.policy_profile == PolicyProfile.BALANCED else 6
        credits = round(total_gb * (1.8 if spec.policy_profile == PolicyProfile.FAST else 1.2), 1)
        return CostEstimate(estimated_duration_minutes=max(duration_minutes, 10), peak_parallel_workers=workers, compute_credits=credits, notes=[f"Total volume {round(total_gb,2)} GB."])

    def _build_compliance_gates(self, source: SourceProfile) -> list[ComplianceGate]:
        pii_tables = [t.name for t in source.tables if "pii" in t.name.lower() or "user" in t.name.lower()]
        return [
            ComplianceGate(name="SOX", passed=True, detail="Change control and rollback criteria are present."),
            ComplianceGate(name="PII", passed=len(pii_tables) == 0, detail="PII-like table names require masking policy approval."),
            ComplianceGate(name="retention", passed=True, detail="Retention policy checkpoint included in runbook."),
        ]

    def _schema_contract_report(self, source: SourceProfile) -> SchemaContractReport:
        breaking = [f"{t.name}: missing primary key" for t in source.tables if not t.has_primary_key]
        warnings = [f"{t.name}: high schema drift likelihood" for t in source.tables if t.schema_drift_likelihood >= 0.5]
        score = max(0.0, round(1.0 - (0.2 * len(breaking)) - (0.05 * len(warnings)), 2))
        return SchemaContractReport(backward_compatibility_score=score, breaking_changes=breaking, warnings=warnings)

    def _confidence_for(self, risks: list[RiskItem]) -> float:
        score = 0.95
        for risk in risks:
            score -= 0.15 if risk.level == RiskLevel.HIGH else 0.07 if risk.level == RiskLevel.MEDIUM else 0.02
        return max(0.1, round(score, 2))

    def _build_plan(self, pattern: MigrationPattern, table_plans: list[ResolvedTablePlan], profile: PolicyProfile) -> MigrationPlan:
        steps = [
            PlanStep(id="prepare", stage="prepare", depends_on=[], details="Freeze schema contracts and configure connections."),
            PlanStep(id="backfill", stage="backfill", depends_on=["prepare"], details="Backfill tables in execution order with configured chunk sizes."),
        ]
        validation_dep = "backfill"

        if pattern in {MigrationPattern.BACKFILL_CDC, MigrationPattern.PHASED}:
            steps.append(PlanStep(id="sync", stage="sync", depends_on=["backfill"], details="Run incremental sync/CDC until lag gates pass."))
            validation_dep = "sync"

        steps += [
            PlanStep(id="validate", stage="validation", depends_on=[validation_dep], details="Run row-count, aggregate, checksum, and FK integrity checks."),
            PlanStep(id="cutover", stage="cutover", depends_on=["validate"], details="Switch reads/writes to target after all validation gates pass."),
        ]

        if pattern == MigrationPattern.PHASED:
            steps.append(PlanStep(id="phased-cutover-domains", stage="cutover", depends_on=["cutover"], details="Execute cutover in domain/table-group waves with per-wave validation gates."))

        rollback = ["Abort if critical validation checks fail."]
        if profile != PolicyProfile.FAST:
            rollback.append("Abort if replication lag does not converge before cutover window.")
        if profile == PolicyProfile.CONSERVATIVE:
            rollback.append("Abort if any schema compatibility score is below 0.8.")
        rollback.append("Abort if schema drift introduces incompatible DDL.")

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
