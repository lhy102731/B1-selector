"""The only trusted final evaluation runtime factory (P8R3 T8, CR010-R04).

The runtime is the ONLY production caller of the OPEN_HOLDOUT seam
(``TrustedEvaluator.evaluate_v2``): ordinary Runners, AG2, Prompt, Memory
and ops exports cannot construct it; only the sanctioned factory inputs
(in-memory Authority capability, sealed root capability, approved worker
launcher and evidence sink) are accepted -- never command-line secrets or
paths.

CR-010 F-02/F-03/F-04 hardening:

  - the evaluator is MANDATORY: a run without the OPEN_HOLDOUT seam is
    rejected, so no final evaluation can complete without the runtime;
  - the repository root is never caller-injected: it is derived from the
    sealed ``FinalEvalRootCapability``, verified against the authority
    capability's root secret;
  - the durable binding identity covers the FULL request identity
    (candidate freeze, code, execution spec, features, roster, model,
    threshold, generation, actor and invocation);
  - the maintenance ticket is unique per run and is finished after the
    reconciler, so consecutive runs never collide; a repeated run of an
    already-terminal binding is an idempotent replay, and a crash-recovery
    replay (RESULT_STAGED/CLOSED) skips the worker + holdout seam and
    closes through the reconciler.

``run()`` drives the DURABLE saga end to end:

  1. Authority bind        -- the request is bound through the broker
                              (CONSUMED, durable ticket + lease);
  2. worker execution      -- the approved launcher runs exactly once
                              (its result is cached for the replay);
  3. OPEN_HOLDOUT seam     -- evaluate_v2 consumes the holdout nonce
                              atomically with the derived outcome (the
                              runtime is the only caller);
  4. durable orchestration -- EVALUATING -> RESULT_STAGED with the
                              committed content-addressed object and the
                              per-ticket fixed claim (verified before
                              staging, outcome bound to the worker result);
  5. reconciler            -- CLOSED -> AUTHORITY_TERMINAL + Authority
                              finish with the fixed claim as evidence;
  6. maintenance close     -- the per-run maintenance ticket is finished
                              with the terminal claim as evidence.

The returned result carries the REAL committed claim ref as
``evidence_ref`` -- never None -- and the observed durable states as steps.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .final_eval_authority import (
    AuthorityFinalEvalBroker,
    FinalEvalBindingV2,
    FinalEvalRequestRejected,
    FinalEvalRequestV2,
    FinalEvalUniquenessRejected,
)
from .final_eval_orchestrator import OrchestrationInputs, orchestrate
from .final_eval_reconciler import reconcile
from .final_eval_saga import (
    TERMINAL_OUTCOMES,
    FinalEvalSagaOutcomeRejected,
)
from .final_evaluator import (
    TrustedEvaluator,
    TrustedEvaluatorDataRoot,
    TrustedEvaluatorError,
)
from .stores import (
    Actor,
    AuthorityIdentity,
    FinalEvalBindingConflictError,
    Phase,
    SideEffect,
    TaskExecutionLease,
    _AuthorityStore,
    _final_eval_nonce_fingerprint,
    _final_eval_request_sha256,
)


class FinalEvalRuntimeError(RuntimeError):
    """Base error for the final evaluation runtime."""


class FinalEvalRuntimeRejected(FinalEvalRuntimeError):
    """The factory rejected an unsafe construction input or run."""


_ROOT_CAPABILITY_DOMAIN = b"control_plane.final_eval_root_capability.v1\0"
_MAINTENANCE_DOMAIN = b"control_plane.final_eval_runtime_maintenance.v1\0"


@dataclass(frozen=True, slots=True)
class FinalEvalRootCapability:
    """Sealed binding of the repository root to the root secret.

    The caller can NEVER inject an arbitrary repository path: the
    capability must be produced by ``create`` (which requires the root
    secret) and the runtime re-verifies the seal against the authority
    capability before using the root.
    """

    repository_root: str
    seal: str

    @classmethod
    def create(
        cls,
        *,
        root_secret: str,
        repository_root: str | os.PathLike[str],
    ) -> "FinalEvalRootCapability":
        if not isinstance(root_secret, str) or len(root_secret) < 32:
            raise FinalEvalRuntimeRejected(
                "root secret must be a strong in-memory secret"
            )
        canonical = str(Path(repository_root).resolve())
        seal = hmac.new(
            root_secret.encode("utf-8"),
            _ROOT_CAPABILITY_DOMAIN + canonical.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return cls(repository_root=canonical, seal=seal)

    def verify(self, root_secret: str) -> str:
        """Return the sealed repository root iff the seal verifies."""
        if not isinstance(root_secret, str) or len(root_secret) < 32:
            raise FinalEvalRuntimeRejected(
                "root secret must be a strong in-memory secret"
            )
        expected = hmac.new(
            root_secret.encode("utf-8"),
            _ROOT_CAPABILITY_DOMAIN + self.repository_root.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(self.seal, expected):
            raise FinalEvalRuntimeRejected(
                "root capability seal does not match the authority capability"
            )
        root = Path(self.repository_root)
        if not root.is_dir():
            raise FinalEvalRuntimeRejected(
                "sealed repository root is unavailable"
            )
        return self.repository_root


@dataclass(frozen=True, slots=True)
class FinalEvalRuntimeInputs:
    """Frozen, opaque construction inputs for the runtime.

    The repository root is NOT an input: it comes exclusively from the
    sealed root capability, verified against the authority capability.
    ``material_resolver`` is the SEALED material source for the production
    evaluator projection (CR-010 F-03): production composition always
    resolves the V2 materials through it; a caller-supplied V1 request is
    only ever accepted through the explicit test-only adapter in ``run``.
    """

    authority_capability: object
    root_capability: FinalEvalRootCapability
    worker_launcher: Callable[[], int]
    evidence_sink: Callable[[Mapping[str, object]], Mapping[str, object]]
    attempt_id: str = "final-eval-runtime"
    material_resolver: Callable[[FinalEvalRequestV2], object] | None = None


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

    def _repository_root(self, authority: _AuthorityStore) -> str:
        """Derive the repository root from the SEALED root capability.

        CR-010 F-02: the caller can never inject a repository path; the
        root is verified against the authority capability's root secret.
        """
        capability = self._inputs.root_capability
        if not isinstance(capability, FinalEvalRootCapability):
            raise FinalEvalRuntimeRejected(
                "root_capability must be a sealed FinalEvalRootCapability"
            )
        return capability.verify(
            authority._root_secret._reveal_for_authority_check()
        )

    def _maintenance_lease(
        self,
        authority: _AuthorityStore,
        identity: AuthorityIdentity,
        idempotency_key: str,
    ) -> TaskExecutionLease:
        """Provision a UNIQUE bounded maintenance authorization for the
        reconciler (P0 maintenance grant, read-effect task ticket).

        CR-010 F-04: every run uses a fresh attempt/actor/idempotency key,
        so consecutive runs never collide with the Authority uniqueness
        constraints; the ticket is finished by the caller after the
        reconciler completes.
        """
        unique = secrets.token_hex(8)
        maintenance_actor = Actor(
            "final-eval-runtime-maintenance",
            "automation",
            f"final-eval-runtime-maint-{unique}",
        )
        attempt_id = f"final-eval-runtime-maint-{unique}"
        envelope = authority._provision_authorization(
            phase=Phase.P0,
            attempt_id=attempt_id,
            actor=maintenance_actor,
            identity=identity,
            expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
            allowed_side_effects=(SideEffect.READ,),
        )
        maintenance_grant = authority.claim_authorization(
            envelope,
            expected_phase=Phase.P0,
            expected_attempt_id=attempt_id,
            actor=maintenance_actor,
            identity=identity,
        )
        ticket = authority._issue_task_ticket(
            maintenance_grant,
            {
                "task_id": "P8-RUNTIME-RECONCILER-MAINT",
                "objective": "bounded reconciler maintenance for the runtime",
                "dependencies": [],
                "idempotency_key": (
                    idempotency_key + "-maint-" + unique
                ),
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

    def _expected_request_identity_sha256(
        self,
        authority: _AuthorityStore,
        *,
        request: FinalEvalRequestV2,
        grant: object,
        nonce: str,
        task_spec_ref: str,
        task_spec_sha256: str,
    ) -> str:
        """The FULL durable request identity digest for a request: nonce
        fingerprint + authority plan + candidate/code/spec/features/roster/
        model/threshold/generation/actor/invocation + task spec + research
        plan/campaign/holdout."""
        expected_fingerprint = _final_eval_nonce_fingerprint(
            authority._root_secret._reveal_for_authority_check(),
            nonce,
        )
        grant_plan = getattr(grant, "identity", None)
        grant_plan_hash = (
            grant_plan.plan_hash if grant_plan is not None else None
        )
        return _final_eval_request_sha256(
            authority_plan_hash=grant_plan_hash,
            identity_scope_hash=request.identity_scope_hash,
            identity_instruction_policy_hash=(
                request.identity_instruction_policy_hash
            ),
            research_plan_sha256=request.research_plan_sha256,
            campaign_id=request.campaign_id,
            campaign_sha256=request.campaign_sha256,
            holdout_id=request.holdout_id,
            holdout_sha256=request.holdout_sha256,
            nonce_fingerprint=expected_fingerprint,
            task_spec_ref=task_spec_ref,
            task_spec_sha256=task_spec_sha256,
            candidate_freeze_ref=request.candidate_freeze_ref,
            candidate_freeze_sha256=request.candidate_freeze_sha256,
            code_ref=request.code_ref,
            code_sha256=request.code_sha256,
            execution_spec_ref=request.execution_spec_ref,
            execution_spec_sha256=request.execution_spec_sha256,
            features_ref=request.features_ref,
            features_sha256=request.features_sha256,
            model_id=request.model,
            model_sha256=request.model_sha256,
            threshold_ref=request.threshold_ref,
            threshold_sha256=request.threshold_sha256,
            roster_ref=request.roster_ref,
            roster_sha256=request.roster_sha256,
            generation_id=request.generation,
            generation_sha256=request.generation_sha256,
            actor_id=request.actor_id,
            actor_type=request.actor_type,
            invocation_id=request.invocation_id,
            # CR-010 F-02: the attempt in the durable identity is the
            # AUTHORIZED grant attempt (the store commits grant.attempt_id),
            # never a caller-chosen request string.
            attempt_id=getattr(grant, "attempt_id", "") or request.attempt_id,
            request_schema=request.schema_version,
            request_digest=request.request_sha256,
        )

    def _verify_binding_identity(
        self,
        authority: _AuthorityStore,
        binding: FinalEvalBindingV2,
        *,
        request: FinalEvalRequestV2,
        grant: object,
        nonce: str,
        task_spec_ref: str,
        task_spec_sha256: str,
    ) -> None:
        """Fail closed unless the durable binding matches the FULL request
        identity (CR-010 F-03): nonce fingerprint, authority plan, the
        complete request identity digest (candidate/code/spec/features/
        roster/model/threshold/generation/actor/invocation) and the
        research-plan/campaign/holdout identity."""
        expected_fingerprint = _final_eval_nonce_fingerprint(
            authority._root_secret._reveal_for_authority_check(),
            nonce,
        )
        grant_plan = getattr(grant, "identity", None)
        grant_plan_hash = (
            grant_plan.plan_hash if grant_plan is not None else None
        )
        expected_request_sha256 = self._expected_request_identity_sha256(
            authority,
            request=request,
            grant=grant,
            nonce=nonce,
            task_spec_ref=task_spec_ref,
            task_spec_sha256=task_spec_sha256,
        )
        if (
            binding.nonce_fingerprint != expected_fingerprint
            or binding.authority_plan_hash != grant_plan_hash
            or binding.request_sha256 != expected_request_sha256
            or binding.research_plan_sha256 != request.research_plan_sha256
            or binding.campaign_id != request.campaign_id
            or binding.holdout_id != request.holdout_id
        ):
            raise FinalEvalRuntimeRejected(
                "binding does not match the request identity "
                "(fingerprint/plan/request digest/campaign/holdout)"
            )

    def _terminal_result(
        self,
        binding: FinalEvalBindingV2,
        terminal: object,
        claim_ref: str,
        observed_states: list[str],
        outcome: str,
    ) -> dict[str, object]:
        return {
            "schema_version": "control_plane.final_eval_runtime_result.v1",
            "outcome": outcome,
            "saga_state": terminal.saga_state,
            "binding_id": binding.ticket_id,
            "evidence_ref": claim_ref,
            "steps": [
                {"step": state, "state": state} for state in observed_states
            ],
        }

    def _resolve_evaluator_projection(
        self,
        request: FinalEvalRequestV2,
        *,
        authority: _AuthorityStore,
        identity: AuthorityIdentity,
        evaluator_request: object,
    ) -> object:
        """Resolve the evaluator projection (CR-010 F-03).

        Production composition supplies the sealed material resolver; the
        projection recomputes every digest and rejects any mismatch before
        the evaluator consumes anything.  A caller-supplied V1 request is
        accepted ONLY through the explicit test-only adapter.  The
        projection's attempt/identity must match the runtime inputs and
        the grant lineage -- an attempt id alone is never an authorization.
        """
        from .final_eval_request_projection import (
            FinalEvalMaterialBundle,
            adapt_evaluator_request_v1_test_only,
            build_evaluator_request_v2,
        )

        resolver = self._inputs.material_resolver
        if resolver is not None:
            materials = resolver(request)
            if not isinstance(materials, FinalEvalMaterialBundle):
                raise FinalEvalRuntimeRejected(
                    "material resolver must return a FinalEvalMaterialBundle"
                )
            projection = build_evaluator_request_v2(
                request,
                materials,
                root_secret=authority._root_secret._reveal_for_authority_check(),
            )
        elif evaluator_request is not None:
            projection = adapt_evaluator_request_v1_test_only(
                evaluator_request,
                request,
                root_secret=authority._root_secret._reveal_for_authority_check(),
                attempt_id=self._inputs.attempt_id,
                identity=identity,
            )
        else:
            raise FinalEvalRuntimeRejected(
                "run requires a sealed material resolver (production) or "
                "an explicit test-only V1 evaluator request"
            )
        if projection.attempt_id != self._inputs.attempt_id:
            raise FinalEvalRuntimeRejected(
                "evaluator projection attempt does not match the runtime "
                "attempt"
            )
        if (
            projection.identity_scope_hash != identity.scope_hash
            or projection.identity_instruction_policy_hash
            != identity.instruction_policy_hash
        ):
            raise FinalEvalRuntimeRejected(
                "evaluator projection identity does not match the runtime "
                "identity"
            )
        return projection

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
        evaluator: TrustedEvaluator,
        evaluator_request: object = None,
        data_root: TrustedEvaluatorDataRoot,
    ) -> dict[str, object]:
        """Drive the durable saga and return the terminal result.

        The worker runs exactly once (cached for replay); the derived
        outcome is consumed atomically through the OPEN_HOLDOUT seam and
        staged through the durable orchestrator with committed evidence.
        A caller-provided binding is validated against the FULL request
        identity (fail closed) instead of re-bound; otherwise the runtime
        binds through the broker.  An already-terminal binding is an
        idempotent replay; a crash-recovery binding (RESULT_STAGED/CLOSED)
        is closed through the reconciler without re-running the worker.

        The evaluator request is DERIVED from the V2 request (CR-010 F-03):
        production resolves the sealed material bundle; ``evaluator_request``
        (a V1 request) is an explicitly test-only adapter input.
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
        # CR-010 F-02: the OPEN_HOLDOUT seam is MANDATORY -- a run without
        # the evaluator is rejected, so no final evaluation completes
        # without the runtime driving evaluate_v2.
        if not isinstance(evaluator, TrustedEvaluator):
            raise FinalEvalRuntimeRejected(
                "evaluator must be a TrustedEvaluator (OPEN_HOLDOUT seam)"
            )
        if data_root is None:
            raise FinalEvalRuntimeRejected("data_root is required")
        if (
            evaluator_request is None
            and self._inputs.material_resolver is None
        ):
            raise FinalEvalRuntimeRejected(
                "a sealed material resolver (production) or an explicit "
                "test-only V1 evaluator request is required"
            )

        authority = self._authority()
        repository_root = self._repository_root(authority)
        broker = AuthorityFinalEvalBroker(
            authority=authority,
            grant=grant,
            attempt_id=self._inputs.attempt_id,
            identity=identity,
        )
        binding_created_now = False
        if binding is None:
            try:
                binding = broker.bind(
                    request=request,
                    nonce=nonce,
                    actor=actor,
                    idempotency_key=idempotency_key,
                    task_spec_ref=task_spec_ref,
                    task_spec_sha256=task_spec_sha256,
                )
                binding_created_now = True
            except (FinalEvalUniquenessRejected, FinalEvalBindingConflictError):
                # CR-010 F-04: a repeated run with the same identity is an
                # idempotent replay -- find the committed binding and reuse
                # its terminal result instead of failing on the uniqueness
                # constraint.
                existing = self._find_existing_binding(
                    authority,
                    request,
                    grant=grant,
                    nonce=nonce,
                    task_spec_ref=task_spec_ref,
                    task_spec_sha256=task_spec_sha256,
                )
                if existing is None:
                    raise
                binding = existing
        else:
            # fail-closed validation of the caller-provided binding: the
            # binding must correspond to THIS request/nonce/grant identity.
            # A caller-provided binding is the documented TEST-ONLY seam
            # (the production entry never passes one): the caller asserts
            # the binding was just created by THIS logical run, so its
            # CONSUMED/EVALUATING state is fresh, never a crash tombstone.
            if not isinstance(binding, FinalEvalBindingV2):
                raise FinalEvalRuntimeRejected(
                    "binding must be FinalEvalBindingV2"
                )
            self._verify_binding_identity(
                authority,
                binding,
                request=request,
                grant=grant,
                nonce=nonce,
                task_spec_ref=task_spec_ref,
                task_spec_sha256=task_spec_sha256,
            )
            binding_created_now = True

        terminal_snapshot = authority.final_eval_binding_snapshot(
            binding.ticket_id
        )
        # Idempotent replay: the binding is already terminal; return the
        # committed result (never re-run the worker, never re-open the
        # holdout, never reissue).
        if terminal_snapshot.saga_state == "AUTHORITY_TERMINAL":
            return self._terminal_result(
                binding,
                terminal_snapshot,
                terminal_snapshot.result_claim_ref or "",
                ["AUTHORITY_TERMINAL"],
                terminal_snapshot.terminal_binding or "IN_DOUBT",
            )

        # CR-010 F-03 (functional closure): a durable CONSUMED/EVALUATING
        # state that was NOT created by THIS run is a crash tombstone from
        # a previous process.  It is NEVER allowed to fall through to the
        # evaluator path (that would reopen the Holdout, recompute the
        # worker result and reissue the consume).  The binding is failed
        # closed durably (AUTHORITY_TERMINAL/FAILED) with a finished
        # maintenance ticket -- a clearly recorded failed terminal with no
        # reopen path.
        if (
            terminal_snapshot.saga_state in ("CONSUMED", "EVALUATING")
            and not binding_created_now
        ):
            from .stores import FinalEvalFailureLease

            failure_lease = FinalEvalFailureLease.issue(
                authority=authority,
                binding_id=binding.ticket_id,
                expected_saga_version=terminal_snapshot.saga_version,
                identity=identity,
            )
            failed = authority._fail_final_eval_binding(
                failure_lease,
                binding_id=binding.ticket_id,
                expected_version=terminal_snapshot.saga_version,
                failure_reason=(
                    "crash recovery: durable binding was left in "
                    + terminal_snapshot.saga_state
                    + " by a previous process; reopening/recomputing/"
                    "reissuing is never allowed"
                ),
            )
            return self._terminal_result(
                binding,
                failed,
                "",
                ["CONSUMED", "FAILED_MAINTENANCE", "AUTHORITY_TERMINAL"],
                "FAILED",
            )

        observed_states = ["CONSUMED"]
        worker_payload: dict[str, object] | None = None
        once_worker = _OnceWorker(self._inputs.worker_launcher)

        if terminal_snapshot.saga_state in ("RESULT_STAGED", "CLOSED"):
            # Crash-recovery replay: the worker result was already staged;
            # skip the worker + OPEN_HOLDOUT seam and close through the
            # reconciler (never recompute).
            observed_states.append("RESULT_STAGED")
        else:
            # CR-010 C0 (Phase B): the Authority-backed consume receipt was
            # committed by the begin transaction -- the runtime reads it
            # from the DURABLE store (never a second consume, never a
            # backend call) and evaluate_v2 opens exactly one synthetic
            # staging artifact BEFORE the approved worker launches.  A
            # malformed worker result (illegal exit code, failed artifact
            # open, failed launcher) fails closed into a DURABLE
            # failed-maintenance terminal -- no reusable CONSUMED binding,
            # no IN_PROGRESS maintenance task, no second consume, and a
            # retry observes AUTHORITY_TERMINAL and never reopens.
            from .final_eval_holdout_store import SqliteHoldoutStore

            consumption = SqliteHoldoutStore(
                authority=authority
            ).read_consumption(binding.ticket_id)
            projection = self._resolve_evaluator_projection(
                request,
                authority=authority,
                identity=identity,
                evaluator_request=evaluator_request,
            )
            try:
                evaluated = evaluator.evaluate_v2(
                    projection,
                    data_root=data_root,
                    worker_launcher=once_worker,
                    consumption=consumption,
                    durable_ticket_id=binding.ticket_id,
                    durable_request_sha256=binding.request_sha256,
                    durable_nonce_fingerprint=binding.nonce_fingerprint,
                )
            except (FinalEvalSagaOutcomeRejected, TrustedEvaluatorError) as error:
                # CR-010 F-04/A2: the malformed worker result must never
                # leave a reusable CONSUMED binding or an IN_PROGRESS task:
                # issue the typed failure lease, write the durable FAILED
                # tombstone and return the explicit terminal failure.
                from .stores import FinalEvalFailureLease

                failure_lease = FinalEvalFailureLease.issue(
                    authority=authority,
                    binding_id=binding.ticket_id,
                    expected_saga_version=terminal_snapshot.saga_version,
                    identity=identity,
                )
                failed = authority._fail_final_eval_binding(
                    failure_lease,
                    binding_id=binding.ticket_id,
                    expected_version=terminal_snapshot.saga_version,
                    failure_reason="malformed worker result: " + str(error),
                )
                return self._terminal_result(
                    binding,
                    failed,
                    "",
                    ["CONSUMED", "EVALUATING", "FAILED_MAINTENANCE",
                     "AUTHORITY_TERMINAL"],
                    "FAILED",
                )
            if evaluated.outcome not in TERMINAL_OUTCOMES:
                raise FinalEvalRuntimeRejected(
                    "evaluator outcome is not a terminal outcome"
                )
            outcome = str(evaluated.outcome)
            worker_payload = {"outcome": outcome}
            observed_states.append("EVALUATING")

        staged = orchestrate(
            OrchestrationInputs(
                authority=authority,
                binding_id=binding.ticket_id,
                expected_version=terminal_snapshot.saga_version,
                worker_launcher=once_worker,
                evidence_sink=self._inputs.evidence_sink,
                repository_root=repository_root,
            )
        )
        observed_states.append("RESULT_STAGED")
        if staged.result_claim_ref is None:
            raise FinalEvalRuntimeRejected(
                "orchestration did not stage a fixed claim"
            )

        # reconciler: CLOSED -> AUTHORITY_TERMINAL + Authority finish
        maintenance_lease = self._maintenance_lease(
            authority,
            identity,
            idempotency_key,
        )
        maintenance_outcome = "FAILED"
        report = None
        try:
            report = reconcile(
                authority,
                maintenance_lease,
                evidence_ref_for={
                    binding.ticket_id: staged.result_claim_ref
                },
                repository_root=repository_root,
            )
        finally:
            # CR-010 B-06: the maintenance ticket is finished with the
            # REAL terminal state -- SUCCEEDED ONLY when the binding
            # reached AUTHORITY_TERMINAL and the reconciler recovered/
            # skipped it as terminal; any exception or unresolved result
            # marks the maintenance ticket FAILED (never a blanket
            # SUCCEEDED in a finally block).
            try:
                terminal_check = authority.final_eval_binding_snapshot(
                    binding.ticket_id
                )
            except Exception:  # noqa: BLE001
                terminal_check = None
            if (
                terminal_check is not None
                and terminal_check.saga_state == "AUTHORITY_TERMINAL"
                and report is not None
                and binding.ticket_id
                in set(report.recovered) | set(report.skipped)
            ):
                maintenance_outcome = "SUCCEEDED"
            authority._finish_task(
                maintenance_lease,
                outcome=maintenance_outcome,
                evidence_ref=str(staged.result_claim_ref),
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

    def _find_existing_binding(
        self,
        authority: _AuthorityStore,
        request: FinalEvalRequestV2,
        *,
        grant: object,
        nonce: str,
        task_spec_ref: str,
        task_spec_sha256: str,
    ) -> FinalEvalBindingV2 | None:
        """Locate the durable binding ONLY for an IDENTICAL request
        identity (CR-010 B-02).

        Replay may never reuse a result by plan/campaign/holdout alone:
        the candidate must carry the exact FULL request digest (which
        binds nonce fingerprint, authority plan, task spec, candidate/
        code/execution/features/model/threshold/roster/generation/actor/
        invocation) AND then pass the complete identity verification again.
        """
        expected_request_sha256 = self._expected_request_identity_sha256(
            authority,
            request=request,
            grant=grant,
            nonce=nonce,
            task_spec_ref=task_spec_ref,
            task_spec_sha256=task_spec_sha256,
        )
        for snapshot in authority._scan_final_eval_bindings():
            if snapshot.request_sha256 != expected_request_sha256:
                continue
            candidate = FinalEvalBindingV2(
                ticket_id=snapshot.ticket_id,
                request_sha256=snapshot.request_sha256,
                authority_plan_hash=snapshot.authority_plan_hash,
                research_plan_sha256=snapshot.research_plan_sha256,
                campaign_id=snapshot.campaign_id,
                holdout_id=snapshot.holdout_id,
                nonce_fingerprint=snapshot.nonce_fingerprint,
                saga_state=snapshot.saga_state,
                saga_version=snapshot.saga_version,
            )
            # CR-010 B-02: a hit still requires the complete identity
            # verification (never trust the digest match alone).
            self._verify_binding_identity(
                authority,
                candidate,
                request=request,
                grant=grant,
                nonce=nonce,
                task_spec_ref=task_spec_ref,
                task_spec_sha256=task_spec_sha256,
            )
            return candidate
        return None


__all__ = [
    "FinalEvalRootCapability",
    "FinalEvalRuntime",
    "FinalEvalRuntimeError",
    "FinalEvalRuntimeInputs",
    "FinalEvalRuntimeRejected",
]
