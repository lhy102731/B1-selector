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
from pathlib import Path

from .contracts import canonical_json
from .stores import (
    FinalEvalBindingStateError,
    FinalEvalBindingSnapshot,
    _AuthorityStore,
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class FinalEvalOrchestrationError(RuntimeError):
    """Base error for the final evaluation orchestrator."""


# Fixed hard-crash points: one per real durable boundary of the saga.
# A harness child that hard-exits at CRASH_AFTER.<POINT> proves the
# preceding durable step committed (a fresh recovery process can observe
# it) and the next step was never applied.  Each name below has exactly
# one trigger site in the durable path (CR-010 F-02: removed names with no
# reachable trigger -- AUTHORIZED/CLOSED are transactional in-between
# states never persisted; added CLAIM_WRITTEN and RECOVERY_LEASE).
CRASH_POINTS = (
    "CRASH_AFTER.CONSUMED",          # bind committed (binding observable)
    "CRASH_AFTER.EVALUATING",        # CONSUMED -> EVALUATING committed
    "CRASH_AFTER.CLAIM_WRITTEN",     # sink wrote the claim, not yet staged
    "CRASH_AFTER.RESULT_STAGED",     # claim staged durably
    "CRASH_AFTER.RECOVERY_LEASE",    # recovery lease issued, recover not run
    "CRASH_AFTER.CLOSED",            # RESULT_STAGED -> CLOSED committed,
                                     # Authority finish not yet applied
    "CRASH_AFTER.AUTHORITY_TERMINAL",  # recover committed terminal state
)


@dataclass(frozen=True, slots=True)
class OrchestrationInputs:
    """Opaque inputs for one durable orchestration run."""

    authority: _AuthorityStore
    binding_id: str
    expected_version: int
    worker_launcher: Callable[[], int]
    evidence_sink: Callable[[Mapping[str, object]], dict[str, object]]
    crash_hook: Callable[[str], None] | None = None
    repository_root: str | os.PathLike[str] | None = None


def _maybe_crash(inputs: OrchestrationInputs, state: str) -> None:
    if inputs.crash_hook is not None:
        inputs.crash_hook(state)


# CR-010 B-01: the exit-code validation and outcome mapping are the SHARED
# saga functions -- the orchestrator, the runtime and every consumer derive
# the same immutable WorkerResult from the same worker execution.  bool is
# a subclass of int, so the strict type check also rejects booleans.
from .final_eval_saga import (
    FinalEvalSagaOutcomeRejected,
    derive_worker_result,
)


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

    # CR010-R03: a stale/wrong expected_version must fail closed -- the
    # caller's expected version has to match the durable saga version or
    # the orchestration refuses to touch the binding.
    if type(inputs.expected_version) is not int or inputs.expected_version < 1:
        raise FinalEvalOrchestrationError(
            "expected_version must be a positive integer"
        )
    if inputs.expected_version != version:
        raise FinalEvalOrchestrationError(
            f"stale expected_version {inputs.expected_version} does not "
            f"match the durable saga version {version}"
        )

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

    # Crash boundary: the binding is durably observable in CONSUMED (the
    # broker's atomic AUTHORIZED -> CONSUMED commit has landed).  A harness
    # child hard-exits here to prove a fresh process sees the created
    # binding; the recovery process runs without a crash hook and replays.
    if snapshot.saga_state == "CONSUMED":
        _maybe_crash(inputs, "CRASH_AFTER.CONSUMED")

    # AUTHORIZED -> CONSUMED (defensive: only reachable for a binding that
    # was externally created without the atomic broker consume; the broker
    # path commits AUTHORIZED and CONSUMED in one transaction).
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
    # caller-supplied; the sink publishes the content-addressed object +
    # per-ticket fixed claim and returns the four refs).
    if snapshot.saga_state == "EVALUATING":
        try:
            worker_result = derive_worker_result(inputs.worker_launcher())
        except FinalEvalSagaOutcomeRejected as error:
            raise FinalEvalOrchestrationError(str(error)) from error
        result_document = {
            "schema_version": "control_plane.final_eval_worker_result.v1",
            "binding_id": binding_id,
            "ticket_id": binding_id,
            "exit_code": worker_result.exit_code,
            "outcome": worker_result.outcome,
        }
        sink_result = inputs.evidence_sink(result_document)
        if not isinstance(sink_result, Mapping) or not all(
            field_name in sink_result
            for field_name in (
                "object_ref",
                "object_sha256",
                "claim_ref",
                "claim_sha256",
            )
        ):
            raise FinalEvalOrchestrationError(
                "evidence sink must return object_ref/object_sha256/"
                "claim_ref/claim_sha256 for the staged result"
            )
        # Crash boundary: the claim blob exists on disk but the binding has
        # not been staged yet; a fresh process re-runs the worker and sink
        # idempotently (create-only sink must tolerate an existing blob).
        _maybe_crash(inputs, "CRASH_AFTER.CLAIM_WRITTEN")
        # CR010-R02: verify the committed object + fixed claim BEFORE the
        # binding may enter RESULT_STAGED (dangling refs and wrong hashes
        # fail closed here, never in the Authority CAS).
        from .final_eval_evidence import (
            FinalEvalEvidenceError,
            verify_result_evidence,
        )

        repository_root = (
            inputs.repository_root
            if inputs.repository_root is not None
            else _REPOSITORY_ROOT
        )
        try:
            verify_result_evidence(
                binding_id,
                ticket_id=binding_id,
                object_ref=str(sink_result["object_ref"]),
                object_sha256=str(sink_result["object_sha256"]),
                claim_ref=str(sink_result["claim_ref"]),
                claim_sha256=str(sink_result["claim_sha256"]),
                repository_root=repository_root,
                expected_outcome=worker_result.outcome,
            )
        except FinalEvalEvidenceError as error:
            raise FinalEvalOrchestrationError(str(error)) from error
        snapshot = authority._stage_final_eval_result(
            binding_id,
            expected_version=snapshot.saga_version,
            result_object_ref=str(sink_result["object_ref"]),
            result_object_sha256=str(sink_result["object_sha256"]),
            result_claim_ref=str(sink_result["claim_ref"]),
            result_claim_sha256=str(sink_result["claim_sha256"]),
            repository_root=repository_root,
            expected_outcome=worker_result.outcome,
        )
        _maybe_crash(inputs, "CRASH_AFTER.RESULT_STAGED")

    return snapshot


def _document_sha256(document: Mapping[str, object]) -> str:
    import hashlib

    return hashlib.sha256(canonical_json(document).encode("utf-8")).hexdigest()
