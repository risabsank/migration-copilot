from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field, replace
from typing import Protocol
from urllib import error, request

from sdk.engine.models import EngineResult, MigrationPattern, MigrationSpec, RiskItem, RiskLevel, SourceProfile
from sdk.engine.rule_engine import DeterministicDecisionEngine

PROMPT_VERSION = "2026-04-01.v1"
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
        self.timeout_seconds = float(os.getenv("MIGRATION_COPILOT_LLM_TIMEOUT_SECONDS", "20"))
        self.max_retries = max(0, int(os.getenv("MIGRATION_COPILOT_LLM_MAX_RETRIES", "2")))
        self.endpoint_type = os.getenv("MIGRATION_COPILOT_LLM_ENDPOINT_TYPE", "auto").strip().lower() or "auto"
        self.extra_headers = _parse_extra_headers(os.getenv("MIGRATION_COPILOT_LLM_HEADERS", ""))

    def is_configured(self) -> bool:
        return bool(self.endpoint and self.api_key)

    def complete_json(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> dict:
        payload = _build_payload(
            endpoint_type=self.endpoint_type,
            model=self.model,
            temperature=temperature,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"}
        headers.update(self.extra_headers)

        for attempt in range(self.max_retries + 1):
            req = request.Request(self.endpoint, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    body = json.loads(response.read().decode("utf-8"))
                return _extract_json_content(body)
            except (error.URLError, TimeoutError, json.JSONDecodeError, KeyError, ValueError):
                if attempt >= self.max_retries:
                    raise
                time.sleep(0.2 * (attempt + 1))


class HeuristicLLMClient:
    """Offline deterministic fallback that mimics structured LLM outputs."""

    def complete_json(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> dict:
        del system_prompt, temperature
        payload = json.loads(user_prompt)
        task = payload.get("task", "")

        if task == "identify_risks":
            risks = []
            if payload.get("source_cdc_supported") is False and payload.get("recommended_pattern") == "backfill_plus_cdc":
                risks.append({"key": "ai_detected_cdc_gap", "level": "high", "rationale": "CDC unavailable for low downtime target."})
            return {"selected_variant": "batch_only", "recommended_pattern": payload.get("recommended_pattern", "big_bang"), "confidence_adjustment": -0.05 if risks else 0.0, "table_chunk_overrides": {}, "extra_risks": risks, "rationale": "Heuristic risk agent output."}

        downtime = payload.get("downtime_minutes")
        if downtime is None or downtime <= 5:
            return {"selected_variant": "backfill_cdc_sync", "recommended_pattern": "backfill_plus_cdc", "confidence_adjustment": 0.03, "table_chunk_overrides": {}, "extra_risks": [], "rationale": "Heuristic strategy output."}
        return {"selected_variant": "batch_only", "recommended_pattern": "big_bang", "confidence_adjustment": 0.0, "table_chunk_overrides": {}, "extra_risks": [], "rationale": "Heuristic strategy output."}


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

        traces = [
            f"prompt_version={PROMPT_VERSION}",
            f"strategy={strategy.recommended_pattern}",
            f"review_fallback={review.needs_fallback}",
        ]

        notes = [f"strategy_agent: {strategy.rationale}", f"risk_agent: {risk.rationale}", f"review_agent: {'fallback applied' if review.needs_fallback else 'approved'}"]
        base_resolved = base_result.resolved_spec

        if review.needs_fallback:
            resolved = replace(base_resolved, ai_primary=True, ai_agent_notes=notes, decision_log=base_resolved.decision_log + ["review_agent: deterministic fallback triggered"], confirm_with_team=base_resolved.confirm_with_team + review.reasons, explainability_trace=traces + ["override=deterministic_guardrail"])
            return replace(base_result, resolved_spec=resolved)
        
        recommended_pattern = _parse_pattern(strategy.recommended_pattern) or base_resolved.pattern
        confidence = min(1.0, max(0.0, round(base_resolved.confidence + strategy.confidence_adjustment, 2)))
        table_plans = [replace(plan, chunk_size_rows=strategy.table_chunk_overrides.get(plan.table_name, plan.chunk_size_rows)) for plan in base_resolved.table_plans]
        resolved = replace(base_resolved, pattern=recommended_pattern, selected_variant=strategy.selected_variant, confidence=confidence, table_plans=table_plans, risks=_stable_risks([*base_resolved.risks, *risk.extra_risks]), ai_primary=True, ai_agent_notes=notes, explainability_trace=traces + ["override=none"])
        return replace(base_result, resolved_spec=resolved, plan=replace(base_result.plan, pattern=recommended_pattern))
    
    def _run_strategy_agent(self, spec: MigrationSpec, source: SourceProfile) -> AgentInsights:
        prompt = {"prompt_version": PROMPT_VERSION, "task": "choose_migration_strategy", "downtime_minutes": spec.downtime_minutes, "source_cdc_supported": source.cdc_supported, "table_sizes": [t.size_gb for t in source.tables]}
        return _agent_insights_from_json(self._complete_json("You are a migration strategy planner.", prompt))

    def _run_risk_agent(self, spec: MigrationSpec, source: SourceProfile, strategy: AgentInsights) -> AgentInsights:
        prompt = {"prompt_version": PROMPT_VERSION, "task": "identify_risks", "downtime_minutes": spec.downtime_minutes, "source_cdc_supported": source.cdc_supported, "recommended_pattern": strategy.recommended_pattern, "tables_without_pk": [t.name for t in source.tables if not t.has_primary_key]}
        return _agent_insights_from_json(self._complete_json("You are a migration risk analyst.", prompt))

    def _run_review_agent(self, base_result: EngineResult, strategy: AgentInsights, risk: AgentInsights) -> ReviewOutcome:
        reasons = []
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
                response = self._remote_client.complete_json(system_prompt=system_prompt, user_prompt=prompt)
                _validate_agent_response(response)
                return response
        except (error.URLError, TimeoutError, ValueError, KeyError, json.JSONDecodeError):
            pass

        response = self._local_client.complete_json(system_prompt=system_prompt, user_prompt=prompt)
        _validate_agent_response(response)
        return response


def _agent_insights_from_json(data: dict) -> AgentInsights:
    risks = []
    for item in data.get("extra_risks", []):
        if not item:
            continue
        level_value = str(item.get("level", "medium")).lower()
        level = RiskLevel.HIGH if level_value == "high" else RiskLevel.LOW if level_value == "low" else RiskLevel.MEDIUM
        risks.append(RiskItem(key=str(item.get("key", "ai_risk")), level=level, rationale=str(item.get("rationale", "AI agent identified risk."))))
    return AgentInsights(selected_variant=str(data.get("selected_variant", "batch_only")), recommended_pattern=str(data.get("recommended_pattern", MigrationPattern.BIG_BANG.value)), confidence_adjustment=float(data.get("confidence_adjustment", 0.0) or 0.0), table_chunk_overrides={k: int(v) for k, v in data.get("table_chunk_overrides", {}).items()}, extra_risks=risks, rationale=str(data.get("rationale", "")))


def _parse_pattern(pattern: str) -> MigrationPattern | None:
    return next((v for v in MigrationPattern if v.value == pattern), None)


def _stable_risks(risks: list[RiskItem]) -> list[RiskItem]:
    dedup: dict[tuple[str, str], RiskItem] = {}
    for risk in risks:
        dedup[(risk.key, risk.rationale)] = risk
    return list(dedup.values())

def _validate_agent_response(data: dict) -> None:
    required = ["selected_variant", "recommended_pattern"]
    for key in required:
        if key not in data:
            raise ValueError(f"LLM response missing required key: {key}")

def _extract_json_content(body: dict) -> dict:
    if "choices" in body:
        content = body["choices"][0]["message"]["content"]
        return json.loads(content)
    if "output_text" in body:
        return json.loads(body["output_text"])
    if "output" in body:
        for item in body["output"]:
            if item.get("type") != "message":
                continue
            for content in item.get("content", []):
                if content.get("type") in {"output_text", "text"} and content.get("text"):
                    return json.loads(content["text"])
    raise ValueError("Unsupported LLM response body format.")


def _build_payload(
    *,
    endpoint_type: str,
    model: str,
    temperature: float,
    system_prompt: str,
    user_prompt: str,
) -> dict:
    if endpoint_type == "responses":
        return {
            "model": model,
            "temperature": temperature,
            "text": {"format": {"type": "json_object"}},
            "input": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        }
    return {
        "model": model,
        "temperature": temperature,
        "response_format": {"type": "json_object"},
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
    }


def _parse_extra_headers(raw_headers: str) -> dict[str, str]:
    if not raw_headers.strip():
        return {}
    parsed = json.loads(raw_headers)
    if not isinstance(parsed, dict):
        raise ValueError("MIGRATION_COPILOT_LLM_HEADERS must be a JSON object.")
    return {str(k): str(v) for k, v in parsed.items()}