"""Concrete SQL backfill adapters for bounded source extraction and target loading."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol, Sequence

from sdk.connectors import ResolvedConnectionSettings
from sdk.execution.backfill import BackfillChunkResult, BackfillExecutionAdapter
from sdk.observability import EventCollector


class SqlAdapterError(RuntimeError):
    """Base exception for SQL execution adapter failures."""


class RetryableSqlAdapterError(SqlAdapterError):
    """Raised for transient failures safe to retry."""


class NonRetryableSqlAdapterError(SqlAdapterError):
    """Raised for permanent failures that should fail the table run."""


class SqlMetricsHook(Protocol):
    """Simple metrics hook to integrate with host telemetry systems."""

    def __call__(self, *, metric: str, value: float, tags: Mapping[str, str]) -> None:
        """Record one metric sample."""


@dataclass(frozen=True)
class TableSyncConfig:
    """Per-table SQL sync settings used by source and target adapters."""

    source_table: str
    target_table: str
    primary_key: str = "id"


class SQLiteSourceAdapter:
    """SQLite source adapter supporting row counts and bounded chunk extraction."""

    def __init__(self, *, database_path: str):
        self._database_path = database_path

    def configure_connections(self, *, settings: ResolvedConnectionSettings) -> None:
        source_db = settings.source.get("database")
        if isinstance(source_db, str) and source_db:
            self._database_path = source_db

    def count_rows(self, *, table_name: str) -> int:
        with sqlite3.connect(self._database_path) as connection:
            cursor = connection.execute(f"SELECT COUNT(*) AS row_count FROM {table_name};")
            row = cursor.fetchone()
            return int(row[0]) if row else 0

    def read_chunk(
        self,
        *,
        table_name: str,
        primary_key: str,
        chunk_size_rows: int,
        checkpoint: str | None,
    ) -> list[dict[str, Any]]:
        with sqlite3.connect(self._database_path) as connection:
            connection.row_factory = sqlite3.Row
            if checkpoint is None:
                cursor = connection.execute(
                    f"SELECT * FROM {table_name} ORDER BY {primary_key} ASC LIMIT ?;",
                    (chunk_size_rows,),
                )
            else:
                cursor = connection.execute(
                    f"SELECT * FROM {table_name} WHERE {primary_key} > ? ORDER BY {primary_key} ASC LIMIT ?;",
                    (checkpoint, chunk_size_rows),
                )
            return [dict(row) for row in cursor.fetchall()]


class SQLiteTargetAdapter:
    """SQLite target adapter supporting retry-safe batch upserts."""

    def __init__(self, *, database_path: str):
        self._database_path = database_path

    def configure_connections(self, *, settings: ResolvedConnectionSettings) -> None:
        target_db = settings.target.get("database")
        if isinstance(target_db, str) and target_db:
            self._database_path = target_db

    def upsert_rows(self, *, table_name: str, rows: Sequence[Mapping[str, Any]], primary_key: str) -> int:
        if not rows:
            return 0

        columns = list(rows[0].keys())
        placeholders = ", ".join("?" for _ in columns)
        assignments = ", ".join(f"{column}=excluded.{column}" for column in columns if column != primary_key)
        sql = (
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT({primary_key}) DO UPDATE SET {assignments};"
        )
        payload = [tuple(row[column] for column in columns) for row in rows]
        with sqlite3.connect(self._database_path) as connection:
            connection.executemany(sql, payload)
            connection.commit()
        return len(payload)


class SQLBackfillExecutionAdapter(BackfillExecutionAdapter):
    """Concrete backfill adapter composed from SQL source and target adapters."""

    def __init__(
        self,
        *,
        source: SQLiteSourceAdapter,
        target: SQLiteTargetAdapter,
        table_configs: Mapping[str, TableSyncConfig] | None = None,
        collector: EventCollector | None = None,
        metrics_hook: SqlMetricsHook | None = None,
        retryable_classifier: Callable[[Exception], bool] | None = None,
    ):
        self._source = source
        self._target = target
        self._table_configs = dict(table_configs or {})
        self._collector = collector
        self._metrics_hook = metrics_hook
        self._retryable_classifier = retryable_classifier or self._is_retryable_sqlite_error

    def configure_connections(self, *, settings: ResolvedConnectionSettings) -> None:
        self._source.configure_connections(settings=settings)
        self._target.configure_connections(settings=settings)

    def estimate_total_rows(self, *, table_name: str) -> int | None:
        config = self._config_for(table_name)
        try:
            row_count = self._source.count_rows(table_name=config.source_table)
            self._emit_event("source_row_count", "completed", table_name, {"row_count": row_count})
            self._record_metric("sql_backfill.source_row_count", float(row_count), table_name)
            return row_count
        except Exception as exc:  # pragma: no cover - protected by integration error tests.
            raise self._map_error(exc, stage="estimate_total_rows", table_name=table_name) from exc

    def run_backfill_chunk(
        self,
        *,
        table_name: str,
        chunk_size_rows: int,
        checkpoint: str | None,
    ) -> BackfillChunkResult:
        config = self._config_for(table_name)
        self._emit_event(
            "chunk_started",
            "running",
            table_name,
            {"checkpoint": checkpoint, "chunk_size_rows": chunk_size_rows},
        )

        try:
            rows = self._source.read_chunk(
                table_name=config.source_table,
                primary_key=config.primary_key,
                chunk_size_rows=chunk_size_rows,
                checkpoint=checkpoint,
            )
        except Exception as exc:
            raise self._map_error(exc, stage="source_read", table_name=table_name) from exc

        if not rows:
            self._record_metric("sql_backfill.chunk_rows", 0.0, table_name)
            self._emit_event("chunk_completed", "completed", table_name, {"rows_copied": 0, "completed": True})
            return BackfillChunkResult(rows_copied=0, checkpoint=checkpoint, completed=True)

        try:
            rows_written = self._target.upsert_rows(
                table_name=config.target_table,
                rows=rows,
                primary_key=config.primary_key,
            )
        except Exception as exc:
            raise self._map_error(exc, stage="target_write", table_name=table_name) from exc

        last_pk = str(rows[-1][config.primary_key])
        completed = rows_written < chunk_size_rows
        self._record_metric("sql_backfill.chunk_rows", float(rows_written), table_name)
        self._emit_event(
            "chunk_completed",
            "completed" if completed else "running",
            table_name,
            {"rows_copied": rows_written, "checkpoint": last_pk, "completed": completed},
        )
        return BackfillChunkResult(rows_copied=rows_written, checkpoint=last_pk, completed=completed)

    def _config_for(self, table_name: str) -> TableSyncConfig:
        return self._table_configs.get(
            table_name,
            TableSyncConfig(source_table=table_name, target_table=table_name),
        )

    def _emit_event(self, event_type: str, status: str, table_name: str, payload: dict[str, Any]) -> None:
        if not self._collector:
            return
        self._collector.emit(
            event_type=event_type,
            step="sql_backfill_adapter",
            status=status,
            payload={"table": table_name, **payload},
        )

    def _record_metric(self, metric: str, value: float, table_name: str) -> None:
        if not self._metrics_hook:
            return
        self._metrics_hook(metric=metric, value=value, tags={"table": table_name})

    def _map_error(self, error: Exception, *, stage: str, table_name: str) -> SqlAdapterError:
        message = f"SQL backfill adapter error during {stage} for table '{table_name}': {error}"
        if self._retryable_classifier(error):
            return RetryableSqlAdapterError(message)
        return NonRetryableSqlAdapterError(message)

    @staticmethod
    def _is_retryable_sqlite_error(error: Exception) -> bool:
        if not isinstance(error, sqlite3.OperationalError):
            return False
        lowered = str(error).lower()
        retryable_tokens = ("database is locked", "database is busy", "interrupted")
        return any(token in lowered for token in retryable_tokens)
