from __future__ import annotations

from typing import Any

from .contracts import ColumnInfo, ForeignKeyInfo, MetadataAdapter, TableMetadata


class PostgresMetadataAdapter(MetadataAdapter):
    """Reference MetadataAdapter implementation for PostgreSQL."""

    def __init__(self, connection: Any):
        self._connection = connection

    def list_tables(self, schema: str = "public") -> list[str]:
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        with self._connection.cursor() as cursor:
            cursor.execute(query, (schema,))
            return [row[0] for row in cursor.fetchall()]

    def describe_table(self, table_name: str, schema: str = "public") -> TableMetadata:
        columns = self._fetch_columns(schema, table_name)
        pk_columns = self._fetch_primary_keys(schema, table_name)
        foreign_keys = self._fetch_foreign_keys(schema, table_name)
        row_estimate, size_bytes_estimate = self._fetch_table_stats(schema, table_name)

        return TableMetadata(
            table_name=table_name,
            row_estimate=row_estimate,
            size_bytes_estimate=size_bytes_estimate,
            primary_key_columns=pk_columns,
            columns=columns,
            foreign_keys=foreign_keys,
        )

    def _fetch_columns(self, schema: str, table_name: str) -> list[ColumnInfo]:
        query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        with self._connection.cursor() as cursor:
            cursor.execute(query, (schema, table_name))
            rows = cursor.fetchall()

        return [
            ColumnInfo(name=row[0], data_type=row[1], nullable=(row[2] == "YES"))
            for row in rows
        ]

    def _fetch_primary_keys(self, schema: str, table_name: str) -> list[str]:
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
        """
        with self._connection.cursor() as cursor:
            cursor.execute(query, (schema, table_name))
            return [row[0] for row in cursor.fetchall()]

    def _fetch_foreign_keys(self, schema: str, table_name: str) -> list[ForeignKeyInfo]:
        query = """
            SELECT kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
        """
        with self._connection.cursor() as cursor:
            cursor.execute(query, (schema, table_name))
            rows = cursor.fetchall()

        return [
            ForeignKeyInfo(column=row[0], references_table=row[1], references_column=row[2])
            for row in rows
        ]

    def _fetch_table_stats(self, schema: str, table_name: str) -> tuple[int, int]:
        query = """
            SELECT COALESCE(c.reltuples::bigint, 0) AS row_estimate,
                   COALESCE(pg_total_relation_size(c.oid), 0) AS size_bytes_estimate
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """
        with self._connection.cursor() as cursor:
            cursor.execute(query, (schema, table_name))
            result = cursor.fetchone()

        if not result:
            return 0, 0
        return int(result[0]), int(result[1])
