"""Durable final evaluation orchestration (P8 CR-009 Gate D).

The orchestrator drives a FinalEval binding through the durable Authority
state machine (AUTHORIZED -> CONSUMED -> EVALUATING -> RESULT_STAGED;
REQUEST_FROZEN is the conceptual pre-bind state and is never persisted)
using the versioned CAS primitives in stores.py.  Every
transition commits durably before the next step starts, so a crash at any
of the fixed crash points leaves the binding in a known durable state that
a fresh process can observe and a bounded reconciler can close.

The orchestrator never opens holdout bytes by path, never accepts a caller
outcome and never reissues.  The worker launcher is the only subprocess
entry; the evidence sink is the only write path for staged results.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from .contracts import canonical_json
from .stores import (
    FinalEvalBindingStateError,
    FinalEvalBindingSnapshot,
    _AuthorityStore,
)


class FinalEvalOrchestrationError(RuntimeError):
    """Base error for the final evaluation orchestrator."""


# Fixed hard-crash points: one per durable state transition boundary.
# A harness child that hard-exits at CRASH_AFTER.<STATE> proves the
# preceding transition committed durably (the fresh recovery process can
# observe it) and the next transition was never applied.
CRASH_POINTS = (
    "CRASH_AFTER.AUTHORIZED",
    "CRASH_AFTER.CONSUMED",
    "CRASH_AFTER.EVALUATING",
    "CRASH_AFTER.RESULT_STAGED",
    "CRASH_AFTER.CLOSED",
    "CRASH_AFTER.AUTHORITY_TERMINAL",
)


@dataclass(frozen=True, slots=True)
class OrchestrationInputs:
    """Opaque inputs for one durable orchestration run."""

    authority: _AuthorityStore
    binding_id: str
    expected_version: int
    worker_launcher: Callable[[], int]
    evidence_sink: Callable[[Mapping[str, object]], str]
    crash_hook: Callable[[str], None] | None = None


def _maybe_crash(inputs: OrchestrationInputs, state: str) -> None:
    if inputs.crash_hook is not None:
        inputs.crash_hook(state)


def orchestrate(inputs: OrchestrationInputs) -> FinalEvalBindingSnapshot:
    """Drive one binding to RESULT_STAGED (or CLOSED) with durable CAS steps.

    Crash semantics: if the harness hard-exits after any state, the binding
    remains in that durable state; a fresh process re-runs `orchestrate` and
    the CAS advance from the observed state continues -- never reopening
    holdout bytes, never recomputing a staged result and never reissuing.
    """

    if not isinstance(inputs, OrchestrationInputs):
        raise FinalEvalOrchestrationError(
            "orchestration inputs must be OrchestrationInputs"
        )
    authority = inputs.authority
    binding_id = inputs.binding_id
    snapshot = authority.final_eval_binding_snapshot(binding_id)
    state = snapshot.saga_state
    version = snapshot.saga_version

    # Idempotent replay: if the binding is already past the requested work,
    # return the durable state without re-doing anything.
    if state in ("RESULT_STAGED", "CLOSED", "AUTHORITY_TERMINAL"):
        return snapshot

    # Bindings are created at AUTHORIZED (the authority broker binds the
    # request, which consumes to CONSUMED in the same transaction), so the
    # orchestrator's durable work starts from CONSUMED.  REQUEST_FROZEN is
    # the conceptual pre-bind state only; it is never persisted and the
    # stores transition map has no entry for it, so fail closed instead of
    # attempting an impossible advance.
    if state == "REQUEST_FROZEN":
        raise FinalEvalOrchestrationError(
            "binding is in the pre-bind REQUEST_FROZEN state; "
            "it was never durably created"
        )

    # AUTHORIZED -> CONSUMED
    if snapshot.saga_state == "AUTHORIZED":
        snapshot = authority._advance_final_eval_binding(
            binding_id,
            expected_state="AUTHORIZED",
            next_state="CONSUMED",
            expected_version=snapshot.saga_version,
        )
        _maybe_crash(inputs, "CRASH_AFTER.CONSUMED")

    # CONSUMED -> EVALUATING
    if snapshot.saga_state == "CONSUMED":
        snapshot = authority._advance_final_eval_binding(
            binding_id,
            expected_state="CONSUMED",
            next_state="EVALUATING",
            expected_version=snapshot.saga_version,
        )
        _maybe_crash(inputs, "CRASH_AFTER.EVALUATING")

    # EVALUATING -> RESULT_STAGED (worker runs; result is derived, never
    # caller-supplied; the sink writes the evidence and returns its ref).
    if snapshot.saga_state == "EVALUATING":
        exit_code = inputs.worker_launcher()
        result_document = {
            "schema_version": "control_plane.final_eval_worker_result.v1",
            "binding_id": binding_id,
            "exit_code": exit_code,
            "outcome": (
                "SUCCEEDED" if exit_code == 0 else "FAILED"
            ),
        }
        result_ref = inputs.evidence_sink(result_document)
        snapshot = authority._stage_final_eval_result(
            binding_id,
            expected_version=snapshot.saga_version,
            result_object_ref=result_ref,
            result_object_sha256=_document_sha256(result_document),
            result_claim_ref=result_ref,
            result_claim_sha256=_document_sha256(result_document),
        )
        _maybe_crash(inputs, "CRASH_AFTER.RESULT_STAGED")

    return snapshot


def _document_sha256(document: Mapping[str, object]) -> str:
    import hashlib

    return hashlib.sha256(canonical_json(document).encode("utf-8")).hexdigest()
