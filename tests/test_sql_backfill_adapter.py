import sqlite3
from pathlib import Path

import pytest

from sdk.engine.models import CriticalityTier, MigrationPattern, ResolvedTablePlan
from sdk.execution.backfill import BackfillExecutor
from sdk.execution.sql_backfill import SQLBackfillExecutionAdapter, SQLiteSourceAdapter, SQLiteTargetAdapter
from sdk.state.models import MigrationRun, MigrationRunStatus, TableExecutionStatus
from sdk.state.store import JsonMigrationRunStore


def _create_source_db(path: Path, *, row_count: int = 5) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);")
        connection.executemany(
            "INSERT INTO users(id, email) VALUES (?, ?);",
            [(idx, f"user-{idx}@example.com") for idx in range(1, row_count + 1)],
        )
        connection.commit()


def _create_target_db(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);")
        connection.commit()


def _count_rows(path: Path, table: str) -> int:
    with sqlite3.connect(path) as connection:
        row = connection.execute(f"SELECT COUNT(*) FROM {table};").fetchone()
        return int(row[0])


def _build_run() -> MigrationRun:
    run = MigrationRun.new(
        plan_id="plan-1",
        schema="main",
        selected_variant="backfill_cdc_sync",
        pattern=MigrationPattern.BACKFILL_CDC,
        table_names=["users"],
        run_id="run-1",
    )
    run.transition_to(MigrationRunStatus.APPROVED)
    run.transition_to(MigrationRunStatus.PROVISIONING)
    run.transition_to(MigrationRunStatus.BACKFILLING)
    return run


def _table_plan(chunk_size_rows: int = 2) -> list[ResolvedTablePlan]:
    return [
        ResolvedTablePlan(
            table_name="users",
            use_cdc=True,
            chunk_size_rows=chunk_size_rows,
            execution_order=1,
            criticality=CriticalityTier.TIER1,
            cutover_wave=1,
        )
    ]


def test_sql_backfill_successful_table_copy(tmp_path: Path):
    source_db = tmp_path / "source.db"
    target_db = tmp_path / "target.db"
    _create_source_db(source_db, row_count=5)
    _create_target_db(target_db)

    adapter = SQLBackfillExecutionAdapter(
        source=SQLiteSourceAdapter(database_path=str(source_db)),
        target=SQLiteTargetAdapter(database_path=str(target_db)),
    )
    store = JsonMigrationRunStore(tmp_path / "runs.json")
    executor = BackfillExecutor(adapter=adapter, store=store)

    run = executor.execute(run=_build_run(), table_plans=_table_plan(chunk_size_rows=2))

    assert run.status == MigrationRunStatus.VALIDATING
    assert _count_rows(target_db, "users") == 5
    progress = run.table_progress[0]
    assert progress.status == TableExecutionStatus.COMPLETED
    assert progress.rows_copied == 5
    assert progress.checkpoint == "5"


def test_sql_backfill_checkpoint_resume_from_adapter(tmp_path: Path):
    source_db = tmp_path / "source.db"
    target_db = tmp_path / "target.db"
    _create_source_db(source_db, row_count=4)
    _create_target_db(target_db)

    adapter = SQLBackfillExecutionAdapter(
        source=SQLiteSourceAdapter(database_path=str(source_db)),
        target=SQLiteTargetAdapter(database_path=str(target_db)),
    )

    first = adapter.run_backfill_chunk(table_name="users", chunk_size_rows=2, checkpoint=None)
    second = adapter.run_backfill_chunk(table_name="users", chunk_size_rows=2, checkpoint=first.checkpoint)

    assert first.rows_copied == 2
    assert first.checkpoint == "2"
    assert first.completed is False
    assert second.rows_copied == 2
    assert second.checkpoint == "4"
    assert second.completed is False
    assert _count_rows(target_db, "users") == 4


class _FailSecondWriteTarget(SQLiteTargetAdapter):
    def __init__(self, *, database_path: str):
        super().__init__(database_path=database_path)
        self.calls = 0

    def upsert_rows(self, *, table_name, rows, primary_key):  # type: ignore[override]
        self.calls += 1
        if self.calls == 2:
            raise sqlite3.IntegrityError("simulated write failure")
        return super().upsert_rows(table_name=table_name, rows=rows, primary_key=primary_key)


def test_sql_backfill_target_write_failure_persists_partial_progress(tmp_path: Path):
    source_db = tmp_path / "source.db"
    target_db = tmp_path / "target.db"
    _create_source_db(source_db, row_count=5)
    _create_target_db(target_db)

    target = _FailSecondWriteTarget(database_path=str(target_db))
    adapter = SQLBackfillExecutionAdapter(
        source=SQLiteSourceAdapter(database_path=str(source_db)),
        target=target,
    )
    store_path = tmp_path / "runs.json"
    store = JsonMigrationRunStore(store_path)
    executor = BackfillExecutor(adapter=adapter, store=store)

    with pytest.raises(RuntimeError):
        executor.execute(run=_build_run(), table_plans=_table_plan(chunk_size_rows=2))

    persisted = JsonMigrationRunStore(store_path).get("run-1")
    assert persisted is not None
    assert persisted.status == MigrationRunStatus.FAILED
    progress = persisted.table_progress[0]
    assert progress.rows_copied == 2
    assert progress.checkpoint == "2"
    assert _count_rows(target_db, "users") == 2


def test_sql_backfill_duplicate_safe_rerun(tmp_path: Path):
    source_db = tmp_path / "source.db"
    target_db = tmp_path / "target.db"
    _create_source_db(source_db, row_count=3)
    _create_target_db(target_db)

    adapter = SQLBackfillExecutionAdapter(
        source=SQLiteSourceAdapter(database_path=str(source_db)),
        target=SQLiteTargetAdapter(database_path=str(target_db)),
    )
    store = JsonMigrationRunStore(tmp_path / "runs.json")
    executor = BackfillExecutor(adapter=adapter, store=store)

    first_run = _build_run()
    first_run.run_id = "run-first"
    executor.execute(run=first_run, table_plans=_table_plan(chunk_size_rows=2))

    second_run = _build_run()
    second_run.run_id = "run-second"
    executor.execute(run=second_run, table_plans=_table_plan(chunk_size_rows=2))

    assert _count_rows(target_db, "users") == 3


def test_sql_backfill_partial_progress_checkpoint_can_continue(tmp_path: Path):
    source_db = tmp_path / "source.db"
    target_db = tmp_path / "target.db"
    _create_source_db(source_db, row_count=6)
    _create_target_db(target_db)

    adapter = SQLBackfillExecutionAdapter(
        source=SQLiteSourceAdapter(database_path=str(source_db)),
        target=SQLiteTargetAdapter(database_path=str(target_db)),
    )

    first = adapter.run_backfill_chunk(table_name="users", chunk_size_rows=3, checkpoint=None)
    resumed = adapter.run_backfill_chunk(table_name="users", chunk_size_rows=3, checkpoint=first.checkpoint)
    completed = adapter.run_backfill_chunk(table_name="users", chunk_size_rows=3, checkpoint=resumed.checkpoint)

    assert first.checkpoint == "3"
    assert resumed.checkpoint == "6"
    assert completed.completed is True
    assert _count_rows(target_db, "users") == 6
