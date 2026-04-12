"""Persistence layer for migration run state."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Protocol, cast

from sdk.state.models import MigrationRun, PERSISTED_RUN_SCHEMA_VERSION

STORE_SCHEMA_VERSION = 1


class MigrationRunStore(Protocol):
    """Interface for migration run persistence backends."""

    def get(self, run_id: str) -> MigrationRun | None:
        """Load a migration run by identifier."""

    def list(self) -> list[MigrationRun]:
        """List all known migration runs."""

    def save(self, run: MigrationRun) -> MigrationRun:
        """Persist a migration run."""


class JsonMigrationRunStore:
    """JSON file-backed migration run repository with atomic writes."""

    def __init__(self, path: str | Path = ".migration_runs.json"):
        self._path = Path(path)

    def get(self, run_id: str) -> MigrationRun | None:
        return self._load_all().get(run_id)

    def list(self) -> list[MigrationRun]:
        runs = list(self._load_all().values())
        return sorted(runs, key=lambda item: item.created_at)

    def save(self, run: MigrationRun) -> MigrationRun:
        runs = self._load_all()
        run.touch()
        runs[run.run_id] = run
        self._write_all(runs)
        return run

    def _load_all(self) -> dict[str, MigrationRun]:
        if not self._path.exists():
            return {}
        data = json.loads(self._path.read_text(encoding="utf-8"))
        runs_payload = self._extract_runs_payload(data)
        hydrated: dict[str, MigrationRun] = {}
        for run_id, payload in runs_payload.items():
            migrated = self._migrate_run_payload(payload)
            hydrated[run_id] = MigrationRun.from_dict(migrated)
        return hydrated

    def _write_all(self, runs: dict[str, MigrationRun]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "store_schema_version": STORE_SCHEMA_VERSION,
            "runs": {run_id: run.as_dict() for run_id, run in runs.items()},
        }

        with NamedTemporaryFile(mode="w", encoding="utf-8", dir=self._path.parent, delete=False) as tmp:
            tmp.write(json.dumps(payload, indent=2, sort_keys=True))
            tmp.write("\n")
            tmp_path = Path(tmp.name)

        tmp_path.replace(self._path)
    
    def _extract_runs_payload(self, data: dict[str, Any]) -> dict[str, dict[str, Any]]:
        raw_runs = data.get("runs", {})
        if not isinstance(raw_runs, dict):
            raise ValueError("Invalid run store payload: 'runs' must be a mapping")
        return cast(dict[str, dict[str, Any]], raw_runs)

    def _migrate_run_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        run_payload = dict(payload)
        schema_version = int(run_payload.get("schema_version", 1))

        if schema_version == 1:
            run_payload["schema_version"] = PERSISTED_RUN_SCHEMA_VERSION
            run_payload.setdefault("orchestration_phase", "plan_ready")
            run_payload.setdefault("completed_phases", [])
            run_payload.setdefault("pause_requested", False)
            run_payload.setdefault("paused", False)
            run_payload.setdefault("cdc_status", "not_started")

        if int(run_payload.get("schema_version", 0)) > PERSISTED_RUN_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported migration run schema version: {run_payload.get('schema_version')}"
            )
        return run_payload
