"""Typed connector configuration models with secret-aware serialization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


REDACTED = "***REDACTED***"


@dataclass(frozen=True)
class SecretRef:
    """Reference to a secret value in an external secret store."""

    key: str
    provider: str = "env"

    def as_dict(self) -> dict[str, str]:
        return {"provider": self.provider, "key": self.key}


@dataclass(frozen=True)
class SecretValue:
    """In-memory secret value that redacts itself in repr/str output."""

    value: str = field(repr=False)

    def reveal(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return "SecretValue(**redacted**)"

    def __str__(self) -> str:
        return REDACTED


SecretInput = SecretRef | SecretValue


@dataclass(frozen=True)
class SourceConnectionConfig:
    """Source-system connection config with secret references."""

    system: str
    host: str
    port: int
    database: str
    username: str
    password: SecretInput
    schema: str = "public"
    options: dict[str, Any] = field(default_factory=dict)

    def as_metadata_dict(self) -> dict[str, Any]:
        return _serialize_config_metadata(
            {
                "system": self.system,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "username": self.username,
                "schema": self.schema,
                "password": self.password,
                "options": self.options,
            }
        )


@dataclass(frozen=True)
class TargetConnectionConfig:
    """Target-system connection config with secret references."""

    system: str
    host: str
    port: int
    database: str
    username: str
    password: SecretInput
    schema: str = "public"
    options: dict[str, Any] = field(default_factory=dict)

    def as_metadata_dict(self) -> dict[str, Any]:
        return _serialize_config_metadata(
            {
                "system": self.system,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "username": self.username,
                "schema": self.schema,
                "password": self.password,
                "options": self.options,
            }
        )


@dataclass(frozen=True)
class CDCConnectionConfig:
    """CDC control-plane connection config with secret references."""

    system: str
    endpoint: str
    username: str
    token: SecretInput
    options: dict[str, Any] = field(default_factory=dict)

    def as_metadata_dict(self) -> dict[str, Any]:
        return _serialize_config_metadata(
            {
                "system": self.system,
                "endpoint": self.endpoint,
                "username": self.username,
                "token": self.token,
                "options": self.options,
            }
        )


@dataclass(frozen=True)
class ResolvedConnectionSettings:
    """Runtime-only resolved connector settings for adapter execution."""

    source: dict[str, Any]
    target: dict[str, Any]
    cdc: dict[str, Any] | None = None


def _serialize_config_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, SecretRef):
            serialized[key] = value.as_dict()
        elif isinstance(value, SecretValue):
            serialized[key] = REDACTED
        elif isinstance(value, dict):
            serialized[key] = {
                item_key: REDACTED if isinstance(item_value, SecretValue) else item_value
                for item_key, item_value in value.items()
            }
        else:
            serialized[key] = value
    return serialized
