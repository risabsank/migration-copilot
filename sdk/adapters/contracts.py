from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool


@dataclass(frozen=True)
class ForeignKeyInfo:
    column: str
    references_table: str
    references_column: str


@dataclass(frozen=True)
class TableMetadata:
    table_name: str
    row_estimate: int
    size_bytes_estimate: int
    primary_key_columns: list[str] = field(default_factory=list)
    columns: list[ColumnInfo] = field(default_factory=list)
    foreign_keys: list[ForeignKeyInfo] = field(default_factory=list)


class MetadataAdapter(Protocol):
    """Adapter contract required by the SDK to inspect source metadata."""

    def list_tables(self, schema: str = "public") -> list[str]:
        """Return table names for a schema."""

    def describe_table(self, table_name: str, schema: str = "public") -> TableMetadata:
        """Return columns, PK/FK metadata, and size estimates for a single table."""


class ValidationAdapter(Protocol):
    """Optional adapter used by the SDK for runtime validations."""

    def execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute SQL and return rows as dictionaries."""
