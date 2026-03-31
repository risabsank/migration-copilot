"""Engine namespace under migration_copilot.sdk."""

from sdk.engine import *  # noqa: F401,F403


from .models import (
    EngineResult,
    MigrationPattern,
    MigrationPlan,
    MigrationSpec,
    PolicyProfile,
    ResolvedSpec,
    SourceProfile,
    TableProfile,
)
from .validation import (
    AggregateCheck,
    SamplingConfig,
    SamplingStrategy,
    TableValidationConfig,
    ValidationOrchestrator,
    ValidationReport,
    ValidationThreshold,
)
from .rule_engine import DeterministicDecisionEngine
from .ai_agents import MultiAgentDecisionEngine

__all__ = [
    "DeterministicDecisionEngine",
    "MultiAgentDecisionEngine",
    "EngineResult",
    "MigrationPattern",
    "MigrationPlan",
    "MigrationSpec",
    "PolicyProfile",
    "ResolvedSpec",
    "SourceProfile",
    "TableProfile",
    "AggregateCheck",
    "SamplingConfig",
    "SamplingStrategy",
    "TableValidationConfig",
    "ValidationOrchestrator",
    "ValidationReport",
    "ValidationThreshold",
]
