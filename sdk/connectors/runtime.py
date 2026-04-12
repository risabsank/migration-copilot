"""Runtime connector resolution and adapter configuration hooks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from sdk.connectors.models import (
    CDCConnectionConfig,
    ResolvedConnectionSettings,
    SecretRef,
    SecretValue,
    SourceConnectionConfig,
    TargetConnectionConfig,
)
from sdk.connectors.secrets import SecretResolver


@dataclass(frozen=True)
class ConnectorConfigBundle:
    """Connector configuration bundle persisted with non-secret metadata."""

    source: SourceConnectionConfig
    target: TargetConnectionConfig
    cdc: CDCConnectionConfig | None = None

    def as_metadata_dict(self) -> dict[str, Any]:
        return {
            "source": self.source.as_metadata_dict(),
            "target": self.target.as_metadata_dict(),
            "cdc": self.cdc.as_metadata_dict() if self.cdc else None,
        }


@runtime_checkable
class ConnectionConfigurableAdapter(Protocol):
    """Optional adapter hook for receiving resolved connection settings."""

    def configure_connections(self, *, settings: ResolvedConnectionSettings) -> None:
        """Inject resolved runtime connection settings prior to execution."""


def validate_connector_bundle(bundle: ConnectorConfigBundle) -> None:
    """Fail early when required connector fields are missing."""

    _require_fields(
        "source",
        {
            "system": bundle.source.system,
            "host": bundle.source.host,
            "port": bundle.source.port,
            "database": bundle.source.database,
            "username": bundle.source.username,
            "password": bundle.source.password,
        },
    )
    _require_fields(
        "target",
        {
            "system": bundle.target.system,
            "host": bundle.target.host,
            "port": bundle.target.port,
            "database": bundle.target.database,
            "username": bundle.target.username,
            "password": bundle.target.password,
        },
    )
    if bundle.cdc:
        _require_fields(
            "cdc",
            {
                "system": bundle.cdc.system,
                "endpoint": bundle.cdc.endpoint,
                "username": bundle.cdc.username,
                "token": bundle.cdc.token,
            },
        )


def resolve_connector_bundle(bundle: ConnectorConfigBundle, *, resolver: SecretResolver) -> ResolvedConnectionSettings:
    """Resolve secret references into runtime-safe settings for adapters."""

    validate_connector_bundle(bundle)
    source = _resolve_config_dict(bundle.source.as_metadata_dict(), original_password=bundle.source.password, resolver=resolver)
    target = _resolve_config_dict(bundle.target.as_metadata_dict(), original_password=bundle.target.password, resolver=resolver)
    cdc = None
    if bundle.cdc:
        cdc = _resolve_config_dict(bundle.cdc.as_metadata_dict(), original_password=bundle.cdc.token, resolver=resolver, secret_key="token")
    return ResolvedConnectionSettings(source=source, target=target, cdc=cdc)


def configure_adapter_connections(
    *,
    adapter: object,
    settings: ResolvedConnectionSettings,
) -> None:
    """Configure adapters that support runtime connection injection."""

    if isinstance(adapter, ConnectionConfigurableAdapter):
        adapter.configure_connections(settings=settings)


def _resolve_config_dict(
    metadata: dict[str, Any],
    *,
    original_password: SecretRef | SecretValue,
    resolver: SecretResolver,
    secret_key: str = "password",
) -> dict[str, Any]:
    resolved = dict(metadata)
    if isinstance(original_password, SecretRef):
        resolved[secret_key] = resolver.resolve(original_password).reveal()
    elif isinstance(original_password, SecretValue):
        resolved[secret_key] = original_password.reveal()
    return resolved


def _require_fields(section: str, required: dict[str, Any]) -> None:
    missing = [key for key, value in required.items() if value is None or value == ""]
    if missing:
        joined = ", ".join(sorted(missing))
        raise ValueError(f"Connector config '{section}' is missing required fields: {joined}.")
