from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, replace
from typing import Protocol
from urllib import error, request

from sdk.engine.models import (
    EngineResult,
    MigrationPattern,
    MigrationSpec,
    RiskItem,
    RiskLevel,
    SourceProfile,
)
from sdk.engine.rule_engine import DeterministicDecisionEngine


class LLMClient(Protocol):
    def complete_json(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> dict:
        """Return a JSON object parsed from an LLM completion."""


@dataclass(frozen=True)
class AgentInsights:
    selected_variant: str
    recommended_pattern: str
    confidence_adjustment: float = 0.0
    table_chunk_overrides: dict[str, int] = field(default_factory=dict)
    extra_risks: list[RiskItem] = field(default_factory=list)
    rationale: str = ""


@dataclass(frozen=True)
class ReviewOutcome:
    approved: bool
    needs_fallback: bool
    reasons: list[str] = field(default_factory=list)


class OpenAICompatibleLLMClient:
    """Minimal OpenAI-compatible JSON client.

    Uses environment variables:
    - MIGRATION_COPILOT_LLM_ENDPOINT
    - MIGRATION_COPILOT_LLM_API_KEY
    - MIGRATION_COPILOT_LLM_MODEL
    """

    def __init__(self) -> None:
        self.endpoint = os.getenv("MIGRATION_COPILOT_LLM_ENDPOINT", "").strip()
        self.api_key = os.getenv("MIGRATION_COPILOT_LLM_API_KEY", "").strip()
        self.model = os.getenv("MIGRATION_COPILOT_LLM_MODEL", "gpt-4o-mini")

    def is_configured(self) -> bool:
        return bool(self.endpoint and self.api_key)

    def complete_json(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> dict:
        payload = {
            "model": self.model,
            "temperature": temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        req = request.Request(
            self.endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )

        with request.urlopen(req, timeout=20) as response:
            body = json.loads(response.read().decode("utf-8"))

        content = body["choices"][0]["message"]["content"]
        return json.loads(content)


class HeuristicLLMClient:
    """Offline deterministic fallback that mimics structured LLM outputs."""

    def complete_json(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> dict:
        del system_prompt, temperature
        payload = json.loads(user_prompt)
        task = payload.get("task", "")

        if task == "identify_risks":
            risks = []
            if payload.get("source_cdc_supported") is False and payload.get("recommended_pattern") == "backfill_plus_cdc":
                risks.append(
                    {
                        "key": "ai_detected_cdc_gap",
                        "level": "high",
                        "rationale": "LLM agent predicts cutover instability because CDC is unavailable for low downtime target.",
                    }
                )
            if payload.get("tables_without_pk"):
                risks.append(
                    {
                        "key": "ai_detected_missing_pk",
                        "level": "medium",
                        "rationale": "Tables without primary keys increase idempotency and dedupe risk during sync.",
                    }
                )
            return {
                "selected_variant": "batch_only",
                "recommended_pattern": payload.get("recommended_pattern", "big_bang"),
                "confidence_adjustment": -0.05 if risks else 0.0,
                "table_chunk_overrides": {},
                "extra_risks": risks,
                "rationale": "Heuristic risk agent evaluated CDC readiness and key integrity risks.",
            }

        downtime = payload.get("downtime_minutes")
        table_sizes = payload.get("table_sizes", [])
        large_table_count = len([size for size in table_sizes if size >= 100])
        if downtime is None or downtime <= 5:
            pattern = "backfill_plus_cdc"
            variant = "backfill_cdc_sync"
        elif large_table_count >= 2:
            pattern = "phased"
            variant = "phased_cutover_by_domain_or_table_group"
        else:
            pattern = "big_bang"
            variant = "batch_only"

        return {
            "selected_variant": variant,
            "recommended_pattern": pattern,
            "confidence_adjustment": 0.05 if pattern != "big_bang" else 0.0,
            "table_chunk_overrides": {},
            "extra_risks": [],
            "rationale": "Heuristic strategy agent selected a strategy balancing downtime and large-table pressure.",
        }


class MultiAgentDecisionEngine:
    """AI-first planner with deterministic guardrails and fallback."""

    def __init__(self, llm_client: LLMClient | None = None) -> None:
        self._deterministic = DeterministicDecisionEngine()
        self._remote_client = llm_client or OpenAICompatibleLLMClient()
        self._local_client = HeuristicLLMClient()

    def build(self, spec: MigrationSpec, source: SourceProfile) -> EngineResult:
        base_result = self._deterministic.build(spec, source)

        strategy = self._run_strategy_agent(spec, source)
        risk = self._run_risk_agent(spec, source, strategy)
        review = self._run_review_agent(base_result, strategy, risk)

        base_resolved = base_result.resolved_spec
        notes = [
            f"strategy_agent: {strategy.rationale}",
            f"risk_agent: {risk.rationale}",
            f"review_agent: {'fallback applied' if review.needs_fallback else 'approved'}",
        ]

        if review.needs_fallback:
            resolved = replace(
                base_resolved,
                ai_primary=True,
                ai_agent_notes=notes,
                decision_log=base_resolved.decision_log + ["review_agent: deterministic fallback triggered"],
                confirm_with_team=base_resolved.confirm_with_team + review.reasons,
            )
            return replace(base_result, resolved_spec=resolved)

        recommended_pattern = _parse_pattern(strategy.recommended_pattern) or base_resolved.pattern
        confidence = base_resolved.confidence
        if strategy.confidence_adjustment:
            confidence = min(1.0, max(0.0, round(confidence + strategy.confidence_adjustment, 2)))

        table_plans = list(base_resolved.table_plans)
        if strategy.table_chunk_overrides:
            table_plans = [
                replace(
                    plan,
                    chunk_size_rows=strategy.table_chunk_overrides.get(plan.table_name, plan.chunk_size_rows),
                )
                for plan in table_plans
            ]

        resolved = replace(
            base_resolved,
            pattern=recommended_pattern,
            selected_variant=strategy.selected_variant,
            confidence=confidence,
            table_plans=table_plans,
            risks=_stable_risks([*base_resolved.risks, *risk.extra_risks]),
            ai_primary=True,
            ai_agent_notes=notes,
        )
        plan = replace(base_result.plan, pattern=recommended_pattern)
        return replace(base_result, resolved_spec=resolved, plan=plan)

    def _run_strategy_agent(self, spec: MigrationSpec, source: SourceProfile) -> AgentInsights:
        prompt = {
            "task": "choose_migration_strategy",
            "downtime_minutes": spec.downtime_minutes,
            "source_cdc_supported": source.cdc_supported,
            "table_sizes": [table.size_gb for table in source.tables],
            "table_count": len(source.tables),
        }
        result = self._complete_json("You are a migration strategy planner.", prompt)
        return _agent_insights_from_json(result)

    def _run_risk_agent(self, spec: MigrationSpec, source: SourceProfile, strategy: AgentInsights) -> AgentInsights:
        prompt = {
            "task": "identify_risks",
            "downtime_minutes": spec.downtime_minutes,
            "source_cdc_supported": source.cdc_supported,
            "recommended_pattern": strategy.recommended_pattern,
            "tables_without_pk": [table.name for table in source.tables if not table.has_primary_key],
        }
        result = self._complete_json("You are a migration risk analyst.", prompt)
        return _agent_insights_from_json(result)

    def _run_review_agent(self, base_result: EngineResult, strategy: AgentInsights, risk: AgentInsights) -> ReviewOutcome:
        reasons: list[str] = []
        approved = True
        if strategy.recommended_pattern == MigrationPattern.BACKFILL_CDC.value and not base_result.resolved_spec.cdc_plan.ready:
            approved = False
            reasons.append("AI strategy requested CDC-first plan but CDC readiness checks failed.")

        if len(risk.extra_risks) >= 3:
            approved = False
            reasons.append("AI risk agent flagged too many critical blockers.")

        return ReviewOutcome(approved=approved, needs_fallback=not approved, reasons=reasons)

    def _complete_json(self, system_prompt: str, payload: dict) -> dict:
        prompt = json.dumps(payload)
        try:
            if isinstance(self._remote_client, OpenAICompatibleLLMClient) and self._remote_client.is_configured():
                return self._remote_client.complete_json(system_prompt=system_prompt, user_prompt=prompt)
        except (error.URLError, TimeoutError, ValueError, KeyError, json.JSONDecodeError):
            pass

        return self._local_client.complete_json(system_prompt=system_prompt, user_prompt=prompt)


def _agent_insights_from_json(data: dict) -> AgentInsights:
    risks = []
    for item in data.get("extra_risks", []):
        if not item:
            continue
        level_value = str(item.get("level", "medium")).lower()
        level = RiskLevel.HIGH if level_value == "high" else RiskLevel.LOW if level_value == "low" else RiskLevel.MEDIUM
        risks.append(
            RiskItem(
                key=str(item.get("key", "ai_risk")),
                level=level,
                rationale=str(item.get("rationale", "AI agent identified risk.")),
            )
        )

    return AgentInsights(
        selected_variant=str(data.get("selected_variant", "batch_only")),
        recommended_pattern=str(data.get("recommended_pattern", MigrationPattern.BIG_BANG.value)),
        confidence_adjustment=float(data.get("confidence_adjustment", 0.0) or 0.0),
        table_chunk_overrides={k: int(v) for k, v in data.get("table_chunk_overrides", {}).items()},
        extra_risks=risks,
        rationale=str(data.get("rationale", "")),
    )


def _parse_pattern(pattern: str) -> MigrationPattern | None:
    for value in MigrationPattern:
        if value.value == pattern:
            return value
    return None


def _stable_risks(risks: list[RiskItem]) -> list[RiskItem]:
    dedup: dict[tuple[str, str], RiskItem] = {}
    for risk in risks:
        dedup[(risk.key, risk.rationale)] = risk
    return list(dedup.values())
