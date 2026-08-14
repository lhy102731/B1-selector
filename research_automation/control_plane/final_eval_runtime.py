"""The only trusted final evaluation runtime factory (P8R3 T8, CR010-R04).

The runtime is the ONLY production caller of the OPEN_HOLDOUT seam
(``TrustedEvaluator.evaluate_v2``): ordinary Runners, AG2, Prompt, Memory
and ops exports cannot construct it; only the sanctioned factory inputs
(in-memory Authority capability, opaque root capability, approved worker
launcher and evidence sink) are accepted -- never command-line secrets or
paths.

``run()`` drives the DURABLE saga end to end:

  1. Authority bind        -- the request is bound through the broker
                              (CONSUMED, durable ticket + lease);
  2. worker execution      -- the approved launcher runs exactly once
                              (its result is cached for the replay);
  3. OPEN_HOLDOUT seam     -- evaluate_v2 consumes the holdout nonce
                              atomically with the derived outcome;
  4. durable orchestration -- EVALUATING -> RESULT_STAGED with the
                              committed content-addressed object and the
                              per-ticket fixed claim (verified before
                              staging);
  5. reconciler            -- CLOSED -> AUTHORITY_TERMINAL + Authority
                              finish with the fixed claim as evidence.

The returned result carries the REAL committed claim ref as
``evidence_ref`` -- never None -- and the observed durable states as steps.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .final_eval_authority import (
    AuthorityFinalEvalBroker,
    FinalEvalBindingV2,
    FinalEvalRequestV2,
)
from .final_eval_orchestrator import OrchestrationInputs, orchestrate
from .final_eval_reconciler import reconcile
from .final_eval_saga import derive_outcome
from .final_evaluator import (
    TrustedEvaluator,
    TrustedEvaluatorDataRoot,
)
from .stores import (
    Actor,
    AuthorityIdentity,
    Phase,
    SideEffect,
    TaskExecutionLease,
    _AuthorityStore,
)


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
    evidence_sink: Callable[[Mapping[str, object]], Mapping[str, object]]
    repository_root: str | os.PathLike[str] | None = None
    attempt_id: str = "final-eval-runtime"


class _OnceWorker:
    """Run the approved worker exactly once; replay returns the cached
    exit code (the orchestrator + outcome derivation share one execution)."""

    __slots__ = ("_launcher", "_exit_code", "_ran")

    def __init__(self, launcher: Callable[[], int]) -> None:
        self._launcher = launcher
        self._exit_code: int | None = None
        self._ran = False

    def __call__(self) -> int:
        if not self._ran:
            code = self._launcher()
            if type(code) is not int:
                raise FinalEvalRuntimeRejected(
                    "worker launcher must return an integer exit code"
                )
            self._exit_code = code
            self._ran = True
        assert self._exit_code is not None
        return self._exit_code

    @property
    def ran(self) -> bool:
        return self._ran


class FinalEvalRuntime:
    """Single trusted factory that drives the durable final evaluation.

    Executes the saga through the Authority broker, the durable
    orchestrator and the bounded reconciler; never opens holdout bytes by
    path, never accepts a caller outcome and never reissues.  The recovery
    lease cannot construct this runtime.
    """

    def __init__(
        self,
        *,
        inputs: FinalEvalRuntimeInputs,
    ) -> None:
        if not isinstance(inputs, FinalEvalRuntimeInputs):
            raise FinalEvalRuntimeRejected(
                "inputs must be FinalEvalRuntimeInputs"
            )
        self._inputs = inputs

    def _authority(self) -> _AuthorityStore:
        capability = self._inputs.authority_capability
        if not isinstance(capability, str) or len(capability) < 32:
            raise FinalEvalRuntimeRejected(
                "authority capability must be a strong in-memory secret"
            )
        return _AuthorityStore(root_secret=capability)

    def _maintenance_lease(
        self,
        authority: _AuthorityStore,
        identity: AuthorityIdentity,
        idempotency_key: str,
    ) -> TaskExecutionLease:
        """Provision a bounded maintenance authorization for the
        reconciler (P0 maintenance grant, read-effect task ticket)."""
        maintenance_actor = Actor(
            "final-eval-runtime-maintenance",
            "automation",
            "final-eval-runtime-maint-001",
        )
        envelope = authority._provision_authorization(
            phase=Phase.P0,
            attempt_id="final-eval-runtime-maint",
            actor=maintenance_actor,
            identity=identity,
            expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
            allowed_side_effects=(SideEffect.READ,),
        )
        maintenance_grant = authority.claim_authorization(
            envelope,
            expected_phase=Phase.P0,
            expected_attempt_id="final-eval-runtime-maint",
            actor=maintenance_actor,
            identity=identity,
        )
        ticket = authority._issue_task_ticket(
            maintenance_grant,
            {
                "task_id": "P8-RUNTIME-RECONCILER-MAINT",
                "objective": "bounded reconciler maintenance for the runtime",
                "dependencies": [],
                "idempotency_key": idempotency_key,
                "task_spec_ref": "manifest.json",
                "task_spec_sha256": "1" * 64,
                "requirements": {
                    "required_test_receipt_ids": [],
                    "required_review_receipt_ids": [],
                    "required_evidence_ids": [],
                },
                "allowed_files": ["research_automation/control_plane/"],
                "forbidden_files": ["data/"],
                "baseline_ref": "manifest.json",
                "baseline_sha256": "1" * 64,
                "input_evidence_refs": [],
            },
            allowed_side_effects=(SideEffect.READ,),
        )
        return authority._begin_task(ticket)

    def run(
        self,
        *,
        request: FinalEvalRequestV2,
        grant: object,
        nonce: str,
        actor: Actor,
        identity: AuthorityIdentity,
        idempotency_key: str,
        task_spec_ref: str = "manifest.json",
        task_spec_sha256: str = "1" * 64,
        binding: FinalEvalBindingV2 | None = None,
        evaluator: TrustedEvaluator | None = None,
        evaluator_request: object | None = None,
        data_root: TrustedEvaluatorDataRoot | None = None,
    ) -> dict[str, object]:
        """Drive the durable saga and return the terminal result.

        The worker runs exactly once (cached for replay); the derived
        outcome is consumed atomically through the OPEN_HOLDOUT seam and
        staged through the durable orchestrator with committed evidence.
        A caller-provided binding is validated (fail closed) instead of
        re-bound; otherwise the runtime binds through the broker.
        """
        if not isinstance(request, FinalEvalRequestV2):
            raise FinalEvalRuntimeRejected(
                "request must be FinalEvalRequestV2"
            )
        if not isinstance(actor, Actor):
            raise FinalEvalRuntimeRejected("actor must be an Actor")
        if not isinstance(identity, AuthorityIdentity):
            raise FinalEvalRuntimeRejected(
                "identity must be an AuthorityIdentity"
            )
        if not isinstance(nonce, str) or not nonce:
            raise FinalEvalRuntimeRejected("nonce must be non-empty")
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise FinalEvalRuntimeRejected(
                "idempotency_key must be non-empty"
            )

        authority = self._authority()
        broker = AuthorityFinalEvalBroker(
            authority=authority,
            grant=grant,
            attempt_id=self._inputs.attempt_id,
            identity=identity,
        )
        if binding is None:
            binding = broker.bind(
                request=request,
                nonce=nonce,
                actor=actor,
                idempotency_key=idempotency_key,
                task_spec_ref=task_spec_ref,
                task_spec_sha256=task_spec_sha256,
            )
        else:
            # fail-closed validation of the caller-provided binding: the
            # binding must correspond to THIS request/nonce/grant
            if not isinstance(binding, FinalEvalBindingV2):
                raise FinalEvalRuntimeRejected(
                    "binding must be FinalEvalBindingV2"
                )
            # the binding stores the Authority fingerprint (HMAC domain),
            # which differs from the request's SHA-256 fingerprint
            from .stores import _final_eval_nonce_fingerprint

            expected_fingerprint = _final_eval_nonce_fingerprint(
                authority._root_secret._reveal_for_authority_check(),
                nonce,
            )
            grant_plan = getattr(grant, "identity", None)
            grant_plan_hash = (
                grant_plan.plan_hash
                if grant_plan is not None
                else None
            )
            if (
                binding.nonce_fingerprint != expected_fingerprint
                or binding.authority_plan_hash != grant_plan_hash
                or binding.campaign_id != request.campaign_id
                or binding.holdout_id != request.holdout_id
                or binding.research_plan_sha256 != request.research_plan_sha256
                or binding.saga_state != "CONSUMED"
            ):
                raise FinalEvalRuntimeRejected(
                    "binding does not match the request/nonce/grant"
                )
        observed_states = ["CONSUMED"]

        # worker executes exactly once; both the outcome derivation and the
        # orchestrator share the single real execution
        once_worker = _OnceWorker(self._inputs.worker_launcher)
        exit_code = once_worker()
        if exit_code == 124:
            worker_payload: dict[str, object] = {"outcome": "TIMEOUT"}
        elif exit_code != 0:
            worker_payload = {"outcome": "CRASHED"}
        else:
            worker_payload = {"outcome": "SUCCEEDED"}
        outcome = derive_outcome(worker_payload=worker_payload)

        # OPEN_HOLDOUT seam: the runtime is the ONLY caller of evaluate_v2;
        # the holdout nonce is consumed atomically with the derived outcome
        if evaluator is not None:
            if evaluator_request is None or data_root is None:
                raise FinalEvalRuntimeRejected(
                    "evaluator requires evaluator_request and data_root"
                )
            evaluated = evaluator.evaluate_v2(
                evaluator_request,
                data_root=data_root,
                worker_payload=worker_payload,
            )
            if evaluated.outcome != outcome:
                raise FinalEvalRuntimeRejected(
                    "evaluator outcome disagrees with the worker result"
                )

        repository_root = (
            self._inputs.repository_root
            if self._inputs.repository_root is not None
            else Path(__file__).resolve().parents[2]
        )
        staged = orchestrate(
            OrchestrationInputs(
                authority=authority,
                binding_id=binding.ticket_id,
                expected_version=binding.saga_version,
                worker_launcher=once_worker,
                evidence_sink=self._inputs.evidence_sink,
                repository_root=repository_root,
            )
        )
        observed_states.append("EVALUATING")
        observed_states.append("RESULT_STAGED")
        if staged.result_claim_ref is None:
            raise FinalEvalRuntimeRejected(
                "orchestration did not stage a fixed claim"
            )

        # reconciler: CLOSED -> AUTHORITY_TERMINAL + Authority finish
        lease = self._maintenance_lease(
            authority,
            identity,
            idempotency_key + "-runtime-maint",
        )
        reconcile(
            authority,
            lease,
            evidence_ref_for={
                binding.ticket_id: staged.result_claim_ref
            },
            repository_root=repository_root,
        )
        terminal = authority.final_eval_binding_snapshot(binding.ticket_id)
        observed_states.append("CLOSED")
        observed_states.append("AUTHORITY_TERMINAL")

        return {
            "schema_version": "control_plane.final_eval_runtime_result.v1",
            "outcome": terminal.terminal_binding or outcome,
            "saga_state": terminal.saga_state,
            "binding_id": binding.ticket_id,
            "evidence_ref": staged.result_claim_ref,
            "steps": [
                {
                    "step": state,
                    "state": state,
                }
                for state in observed_states
            ],
        }


__all__ = [
    "FinalEvalRuntime",
    "FinalEvalRuntimeError",
    "FinalEvalRuntimeInputs",
    "FinalEvalRuntimeRejected",
]
