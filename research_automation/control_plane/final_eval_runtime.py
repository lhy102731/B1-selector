"""The only trusted final evaluation runtime factory (P8R3 T8).

The factory accepts only in-memory Authority capability, an opaque root
capability, an approved worker launcher and an evidence sink — never
command-line secrets or paths.  Ordinary Runners, AG2, Prompt, Memory and
ops exports cannot construct it; the entry policy is the only gate that can
declare OPEN_HOLDOUT.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from .final_eval_saga import derive_outcome, require_saga_transition


class FinalEvalRuntimeError(RuntimeError):
    """Base error for the final evaluation runtime."""


class FinalEvalRuntimeRejected(FinalEvalRuntimeError):
    """The factory rejected an unsafe construction input."""


@dataclass(frozen=True, slots=True)
class FinalEvalRuntimeInputs:
    """Frozen, opaque construction inputs for the runtime."""

    authority_capability: object
    root_capability: object
    worker_launcher: Callable[[], int]
    evidence_sink: Callable[[Mapping[str, object]], str]


class FinalEvalRuntime:
    """Single-process trusted final evaluation runtime.

    Executes the saga through the injected launcher and sink; never opens
    holdout bytes by path, never accepts a caller outcome and never
    reissues.  The recovery lease cannot construct this runtime.
    """

    def __init__(
        self,
        *,
        inputs: FinalEvalRuntimeInputs,
    ) -> None:
        if not isinstance(inputs, FinalEvalRuntimeInputs):
            raise FinalEvalRuntimeRejected("inputs must be FinalEvalRuntimeInputs")
        self._inputs = inputs
        self._saga_state = "REQUEST_FROZEN"
        self._steps: list[dict[str, object]] = []

    def _advance(self, next_state: str, step: str, detail: object = None) -> None:
        require_saga_transition(self._saga_state, next_state)
        self._steps.append(
            {
                "step": step,
                "state_before": self._saga_state,
                "state_after": next_state,
                "detail": dict(detail) if isinstance(detail, Mapping) else {},
            }
        )
        self._saga_state = next_state

    def run(self) -> dict[str, object]:
        """Run the happy-path saga and return the terminal outcome."""
        self._advance("AUTHORIZED", "authorize")
        self._advance("CONSUMED", "consume")
        self._advance("EVALUATING", "evaluate")
        exit_code = self._inputs.worker_launcher()
        if exit_code == 124:
            worker_payload = {"outcome": "TIMEOUT"}
        elif exit_code != 0:
            worker_payload = {"outcome": "CRASHED"}
        else:
            worker_payload = {"outcome": "SUCCEEDED"}
        outcome = derive_outcome(worker_payload=worker_payload)
        self._advance("RESULT_STAGED", "stage_result", {"outcome": outcome})
        self._advance("CLOSED", "close", {"outcome": outcome})
        self._advance("AUTHORITY_TERMINAL", "authority_terminal")
        return {
            "schema_version": "control_plane.final_eval_runtime_result.v1",
            "outcome": outcome,
            "saga_state": self._saga_state,
            "steps": self._steps,
            "evidence_ref": None,
        }


__all__ = [
    "FinalEvalRuntime",
    "FinalEvalRuntimeError",
    "FinalEvalRuntimeInputs",
    "FinalEvalRuntimeRejected",
]
