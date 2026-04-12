"""Phase transition policy for migration orchestration."""

from __future__ import annotations

from dataclasses import dataclass

from sdk.state.models import OrchestrationPhase


@dataclass(frozen=True)
class PhaseTransitionPolicy:
    """Validates orchestration phase transitions."""

    def assert_transition(self, current: OrchestrationPhase, next_phase: OrchestrationPhase) -> None:
        allowed = _ALLOWED_TRANSITIONS[current]
        if next_phase not in allowed:
            allowed_values = ", ".join(item.value for item in sorted(allowed, key=lambda phase: phase.value))
            raise ValueError(
                f"Invalid orchestration phase transition: {current.value} -> {next_phase.value}. "
                f"Allowed transitions: [{allowed_values}]"
            )


_ALLOWED_TRANSITIONS: dict[OrchestrationPhase, set[OrchestrationPhase]] = {
    OrchestrationPhase.PLAN_READY: {OrchestrationPhase.PROVISIONING, OrchestrationPhase.ROLLBACK},
    OrchestrationPhase.PROVISIONING: {OrchestrationPhase.PREFLIGHT_VALIDATION, OrchestrationPhase.ROLLBACK},
    OrchestrationPhase.PREFLIGHT_VALIDATION: {OrchestrationPhase.BACKFILL, OrchestrationPhase.ROLLBACK},
    OrchestrationPhase.BACKFILL: {OrchestrationPhase.POST_BACKFILL_VALIDATION, OrchestrationPhase.ROLLBACK},
    OrchestrationPhase.POST_BACKFILL_VALIDATION: {OrchestrationPhase.CDC_START, OrchestrationPhase.ROLLBACK},
    OrchestrationPhase.CDC_START: {OrchestrationPhase.CDC_CATCHUP, OrchestrationPhase.ROLLBACK},
    OrchestrationPhase.CDC_CATCHUP: {OrchestrationPhase.CUTOVER_PRECHECK, OrchestrationPhase.ROLLBACK},
    OrchestrationPhase.CUTOVER_PRECHECK: {OrchestrationPhase.CUTOVER, OrchestrationPhase.ROLLBACK},
    OrchestrationPhase.CUTOVER: {OrchestrationPhase.POST_CUTOVER_VALIDATION, OrchestrationPhase.ROLLBACK},
    OrchestrationPhase.POST_CUTOVER_VALIDATION: {OrchestrationPhase.COMPLETED, OrchestrationPhase.ROLLBACK},
    OrchestrationPhase.COMPLETED: set(),
    OrchestrationPhase.ROLLBACK: set(),
}
