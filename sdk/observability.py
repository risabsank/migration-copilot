from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class PlanEvent:
    event_type: str
    plan_id: str
    step: str
    status: str
    rule_ids: list[str] = field(default_factory=list)
    confidence: float | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    ts_utc: str = field(default_factory=_utc_timestamp)

    def as_dict(self) -> dict[str, Any]:
        data = {
            "event_type": self.event_type,
            "plan_id": self.plan_id,
            "step": self.step,
            "status": self.status,
            "rule_ids": self.rule_ids,
            "confidence": self.confidence,
            "payload": self.payload,
            "ts_utc": self.ts_utc,
        }
        return {key: value for key, value in data.items() if value is not None}


class EventCollector:
    """Collects per-step planning events and can persist them as JSONL."""

    def __init__(self, plan_id: str | None = None):
        self.plan_id = plan_id or str(uuid.uuid4())
        self._events: list[PlanEvent] = []

    @property
    def events(self) -> list[PlanEvent]:
        return list(self._events)

    def emit(
        self,
        *,
        step: str,
        status: str,
        event_type: str = "agent_step",
        rule_ids: list[str] | None = None,
        confidence: float | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._events.append(
            PlanEvent(
                event_type=event_type,
                plan_id=self.plan_id,
                step=step,
                status=status,
                rule_ids=rule_ids or [],
                confidence=confidence,
                payload=payload or {},
            )
        )

    def write_jsonl(self, output_path: str | Path) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for event in self._events:
                handle.write(json.dumps(event.as_dict()) + "\n")
        return path
