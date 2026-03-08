from __future__ import annotations

from typing import Any

from .contracts import ValidationAdapter


class GenericSQLValidationAdapter(ValidationAdapter):
    """Reference validation adapter using a DB-API compatible connection."""

    def __init__(self, connection: Any):
        self._connection = connection

    def execute_query(self, query: str) -> list[dict[str, Any]]:
        with self._connection.cursor() as cursor:
            cursor.execute(query)
            description = cursor.description or []
            column_names = [col[0] for col in description]
            rows = cursor.fetchall()

        return [dict(zip(column_names, row)) for row in rows]
