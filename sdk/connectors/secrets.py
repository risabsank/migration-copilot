"""Secret resolution abstractions for connector runtime configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol

from sdk.connectors.models import SecretRef, SecretValue


class MissingSecretError(ValueError):
    """Raised when a required secret cannot be resolved."""


class SecretResolver(Protocol):
    """Abstraction for resolving secret references to concrete values."""

    def resolve(self, ref: SecretRef) -> SecretValue:
        """Resolve a secret reference to a value."""


@dataclass(frozen=True)
class EnvSecretResolver:
    """Environment variable resolver implementation for local and CI usage."""

    prefix: str = ""

    def resolve(self, ref: SecretRef) -> SecretValue:
        if ref.provider != "env":
            raise MissingSecretError(
                f"Unsupported secret provider '{ref.provider}' for key '{ref.key}'. "
                "Only env provider is currently supported."
            )

        env_key = f"{self.prefix}{ref.key}"
        value = os.getenv(env_key)
        if value is None or value == "":
            raise MissingSecretError(f"Required secret '{ref.key}' was not found in environment variable '{env_key}'.")
        return SecretValue(value=value)
