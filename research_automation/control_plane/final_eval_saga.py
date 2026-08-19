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


@dataclass(frozen=True, slots=True)
class WorkerResult:
    """THE immutable worker result shared by every consumer (CR-010 B-01).

    One exit-code validation + outcome derivation happens exactly once per
    worker execution; the same object (exit code + outcome) is consumed by
    the runtime, the evaluator, the orchestrator, the evidence publisher,
    the Authority CAS and the reconciler -- a single final evaluation can
    never produce two different outcomes in different authoritative
    records.
    """

    exit_code: int
    outcome: str


def validate_worker_exit_code(code: object) -> int:
    """Return the exit code iff it is a real int in the allowed range.

    ``bool`` is a subclass of ``int``, so the strict type check rejects it
    too.  Anything else (None/str/float/negative/>255) fails closed BEFORE
    the holdout is opened or consumed.
    """
    if type(code) is not int:
        raise FinalEvalSagaOutcomeRejected(
            "worker exit code must be an integer, got "
            + type(code).__name__
        )
    if code < 0 or code > 255:
        raise FinalEvalSagaOutcomeRejected(
            f"worker exit code is out of range: {code}"
        )
    return code


def derive_worker_outcome(exit_code: int) -> str:
    """Map a validated worker exit code to the terminal outcome.

    The mapping is FIXED and shared by every consumer: 0 -> SUCCEEDED,
    124 -> TIMEOUT, any other non-zero -> FAILED.  The caller can never
    choose the outcome.
    """
    if exit_code == 0:
        return "SUCCEEDED"
    if exit_code == 124:
        return "TIMEOUT"
    return "FAILED"


def derive_worker_result(code: object) -> WorkerResult:
    """Validate the exit code and derive the immutable worker result once."""
    exit_code = validate_worker_exit_code(code)
    return WorkerResult(
        exit_code=exit_code,
        outcome=derive_worker_outcome(exit_code),
    )


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
    "WorkerResult",
    "derive_outcome",
    "derive_worker_outcome",
    "derive_worker_result",
    "require_saga_transition",
    "validate_worker_exit_code",
]
