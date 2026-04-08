"""Persistence layer for migration run state."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Protocol

from sdk.state.models import MigrationRun


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
        return {run_id: MigrationRun.from_dict(payload) for run_id, payload in data.get("runs", {}).items()}

    def _write_all(self, runs: dict[str, MigrationRun]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"runs": {run_id: run.as_dict() for run_id, run in runs.items()}}

        with NamedTemporaryFile(mode="w", encoding="utf-8", dir=self._path.parent, delete=False) as tmp:
            tmp.write(json.dumps(payload, indent=2, sort_keys=True))
            tmp.write("\n")
            tmp_path = Path(tmp.name)

        tmp_path.replace(self._path)
