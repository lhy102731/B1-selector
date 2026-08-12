"""Durable final evaluation saga (P8R3 T7).

States: REQUEST_FROZEN -> AUTHORIZED -> CONSUMED -> EVALUATING ->
RESULT_STAGED -> CLOSED -> AUTHORITY_TERMINAL.  RESULT_STAGED only holds
when the same-volume content object, the per-ticket fixed claim and the
Authority claim CAS are all bound.  Outcome is derived from the worker
result; the caller can never choose it.  Recovery reads only durable state,
never reopens holdout bytes and never reissues.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .contracts import canonical_json


SAGA_STATES = (
    "REQUEST_FROZEN",
    "AUTHORIZED",
    "CONSUMED",
    "EVALUATING",
    "RESULT_STAGED",
    "CLOSED",
    "AUTHORITY_TERMINAL",
)
SAGA_TRANSITIONS = frozenset(
    {
        ("REQUEST_FROZEN", "AUTHORIZED"),
        ("AUTHORIZED", "CONSUMED"),
        ("CONSUMED", "EVALUATING"),
        ("EVALUATING", "RESULT_STAGED"),
        ("RESULT_STAGED", "CLOSED"),
        ("CLOSED", "AUTHORITY_TERMINAL"),
    }
)
TERMINAL_OUTCOMES = frozenset({"SUCCEEDED", "FAILED", "TIMEOUT", "CRASHED"})


class FinalEvalSagaError(RuntimeError):
    """Base error for the final evaluation saga."""


class FinalEvalSagaTransitionError(FinalEvalSagaError):
    """A saga state transition is illegal or out of order."""


class FinalEvalSagaOutcomeRejected(FinalEvalSagaError):
    """The outcome was caller-supplied or invalid."""


def require_saga_transition(
    current: str,
    next_state: str,
) -> None:
    """Validate one saga state transition (fail closed on anything else)."""
    if current not in SAGA_STATES or next_state not in SAGA_STATES:
        raise FinalEvalSagaTransitionError("unknown saga state")
    if (current, next_state) not in SAGA_TRANSITIONS:
        raise FinalEvalSagaTransitionError(
            f"illegal saga transition: {current} -> {next_state}"
        )


def derive_outcome(
    *,
    worker_payload: Mapping[str, object],
    caller_outcome: str | None = None,
) -> str:
    """Derive the terminal outcome from the worker result.

    The caller can never supply the outcome; an explicit caller outcome is
    rejected.  Mapping: backend valid -> SUCCEEDED; explicit compute failure
    -> FAILED; child timeout -> TIMEOUT; child abnormal exit -> CRASHED.
    """
    if caller_outcome is not None:
        raise FinalEvalSagaOutcomeRejected(
            "caller cannot specify the final evaluation outcome"
        )
    if not isinstance(worker_payload, Mapping):
        raise FinalEvalSagaOutcomeRejected("worker payload must be a mapping")
    outcome = worker_payload.get("outcome")
    if outcome not in TERMINAL_OUTCOMES:
        raise FinalEvalSagaOutcomeRejected("worker outcome is invalid")
    return str(outcome)


@dataclass(frozen=True, slots=True)
class FinalEvalSagaStep:
    """One durable saga step with the state before and after."""

    step: str
    state_before: str
    state_after: str
    detail: Mapping[str, object] | None = None

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": "control_plane.final_eval_saga_step.v1",
            "step": self.step,
            "state_before": self.state_before,
            "state_after": self.state_after,
            "detail": dict(self.detail) if self.detail else {},
        }


__all__ = [
    "FinalEvalSagaError",
    "FinalEvalSagaOutcomeRejected",
    "FinalEvalSagaStep",
    "FinalEvalSagaTransitionError",
    "SAGA_STATES",
    "SAGA_TRANSITIONS",
    "TERMINAL_OUTCOMES",
    "derive_outcome",
    "require_saga_transition",
]
