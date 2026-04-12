"""Connector configuration and secret resolution primitives."""

from sdk.connectors.models import (
    CDCConnectionConfig,
    REDACTED,
    ResolvedConnectionSettings,
    SecretRef,
    SecretValue,
    SourceConnectionConfig,
    TargetConnectionConfig,
)
from sdk.connectors.runtime import (
    ConnectionConfigurableAdapter,
    ConnectorConfigBundle,
    configure_adapter_connections,
    resolve_connector_bundle,
    validate_connector_bundle,
)
from sdk.connectors.secrets import EnvSecretResolver, MissingSecretError, SecretResolver

__all__ = [
    "CDCConnectionConfig",
    "ConnectionConfigurableAdapter",
    "ConnectorConfigBundle",
    "EnvSecretResolver",
    "MissingSecretError",
    "REDACTED",
    "ResolvedConnectionSettings",
    "SecretRef",
    "SecretResolver",
    "SecretValue",
    "SourceConnectionConfig",
    "TargetConnectionConfig",
    "configure_adapter_connections",
    "resolve_connector_bundle",
    "validate_connector_bundle",
]
