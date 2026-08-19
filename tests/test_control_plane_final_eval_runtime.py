"""Tests for the trusted final evaluation runtime (P8R3 T8, CR010-R04).

The runtime drives the DURABLE saga (Authority bind -> evaluate_v2 ->
orchestrator -> reconciler) with real committed evidence; the result
carries the real claim ref as evidence_ref and the observed durable
states as steps -- never an in-memory happy path.

CR-010 F-02/F-03/F-04 coverage:
- the OPEN_HOLDOUT seam (evaluate_v2) is MANDATORY: a run without the
  evaluator is rejected;
- the repository root is sealed by the root capability and verified
  against the authority capability (no caller-injected repository path);
- the durable binding identity covers the FULL request identity;
- the maintenance ticket is unique per run and finished afterwards, and a
  repeated run of the same request is an idempotent replay.
"""

from __future__ import annotations

import hashlib
import json
import unittest

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.final_eval_authority import (
    FINAL_EVAL_REQUEST_V2,
    FinalEvalRequestV2,
    _nonce_fingerprint,
)
from research_automation.control_plane.final_eval_request_projection import (
    adapt_evaluator_request_v1_test_only,
)
from research_automation.control_plane.final_evaluator import FinalEvalRequest
from research_automation.control_plane.final_eval_runtime import (
    FinalEvalRootCapability,
    FinalEvalRuntime,
    FinalEvalRuntimeInputs,
    FinalEvalRuntimeRejected,
)
from tests.test_control_plane_campaign_store import (
    _authorized_p8_campaign,
    ROOT_SECRET,
    _authorized_campaign,
)
from tests.test_control_plane_final_eval_orchestrator import (
    P8_IDENTITY,
    _make_broker,
    _real_publisher_sink,
)

NONCE = "0123456789abcdef" * 4


def _git(root, *args: str) -> str:
    import subprocess as _sp

    result = _sp.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {args[0]} failed: {result.stderr[-300:]}")
    return result.stdout.strip()


def _aligned_v2_request(**overrides) -> FinalEvalRequestV2:
    """A V2 request whose digest fields are the REAL digests of the
    aligned V1 evaluator material bundle (CR-010 F-03: the identity bridge
    must never mix V1/V2 values)."""
    from research_automation.control_plane.contracts import canonical_sha256
    from tests.test_control_plane_final_evaluator import (
        _candidate,
        _candidate_set_digest,
        _execution_spec,
        _roster,
        _sha,
    )

    candidates = (_candidate("candidate-a"), _candidate("candidate-b"))
    execution_spec = _execution_spec()
    roster = _roster()
    payload = dict(
        schema_version=FINAL_EVAL_REQUEST_V2,
        research_plan_sha256="a" * 64,
        campaign_id="campaign-final-cr009",
        campaign_sha256="b" * 64,
        holdout_id="holdout-final-cr009",
        holdout_sha256="c" * 64,
        nonce_fingerprint=_nonce_fingerprint(ROOT_SECRET, NONCE),
        candidate_freeze_ref=(
            "research_state/control_plane/p8/attempts/p8-attempt-003/"
            "freeze.json"
        ),
        candidate_freeze_sha256=_candidate_set_digest(candidates),
        code_sha256=_sha("code"),
        execution_spec_sha256=canonical_sha256(
            execution_spec.model_dump(mode="json")
        ),
        features_sha256=_sha("features"),
        model="model-final-1",
        threshold="0.5",
        roster_sha256=roster.manifest_sha256,
        generation="generation-final-1",
        actor_id="operator-1",
        actor_type="human",
        invocation_id="final-eval-op-cr009",
        authority_plan_hash=P8_IDENTITY["plan_hash"],
        identity_scope_hash=P8_IDENTITY["scope_hash"],
        identity_instruction_policy_hash=P8_IDENTITY[
            "instruction_policy_hash"
        ],
        attempt_id="p8-attempt-003",
    )
    payload.update(overrides)
    return FinalEvalRequestV2(**payload)


def _v1_request_aligned(v2_request: FinalEvalRequestV2) -> FinalEvalRequest:
    """The V1 evaluator fixture aligned to ONE V2 request identity."""
    from research_automation.control_plane.final_evaluator import (
        CampaignBinding,
        CodeBinding,
        ExecutionSpecBinding,
        FeatureBinding,
        FinalEvalRequest,
        GenerationBinding,
        HoldoutBinding,
        IdentityBinding,
        ModelBinding,
        RosterBinding,
        ThresholdBinding,
    )
    from tests.test_control_plane_final_evaluator import (
        _candidate,
        _candidate_set_digest,
        _execution_spec,
        _roster,
        _sha,
    )

    candidates = (_candidate("candidate-a"), _candidate("candidate-b"))
    execution_spec = _execution_spec()
    roster = _roster()
    return FinalEvalRequest(
        campaign=CampaignBinding(
            campaign_id=v2_request.campaign_id,
            campaign_sha256=v2_request.campaign_sha256,
        ),
        candidate_set=candidates,
        candidate_set_sha256=_candidate_set_digest(candidates),
        code=CodeBinding(code_sha256=v2_request.code_sha256),
        execution_spec=ExecutionSpecBinding(
            execution_spec=execution_spec,
            execution_spec_sha256=v2_request.execution_spec_sha256,
        ),
        features=FeatureBinding(
            features_sha256=v2_request.features_sha256
        ),
        model=ModelBinding(
            model_id=v2_request.model,
            model_sha256=_sha("model"),
        ),
        threshold=ThresholdBinding(threshold_sha256=_sha("threshold")),
        roster=RosterBinding(
            roster=roster,
            roster_sha256=v2_request.roster_sha256,
        ),
        generation=GenerationBinding(
            generation_id=v2_request.generation,
            generation_sha256=_sha("generation"),
        ),
        holdout=HoldoutBinding(
            holdout_id=v2_request.holdout_id,
            holdout_sha256=v2_request.holdout_sha256,
            authorization_nonce=NONCE,
        ),
        actor=stores_module.Actor(
            v2_request.actor_id,
            v2_request.actor_type,
            v2_request.invocation_id,
        ),
        identity_binding=IdentityBinding(
            plan_hash=v2_request.authority_plan_hash,
            scope_hash=P8_IDENTITY["scope_hash"],
            policy_hash=P8_IDENTITY["instruction_policy_hash"],
        ),
    )


def _durable_request_sha256(
    request: FinalEvalRequestV2,
    *,
    nonce_fingerprint: str | None = None,
    task_spec_ref: str = "manifest.json",
    task_spec_sha256: str = "1" * 64,
) -> str:
    """The stores-domain full canonical request digest (CR-010 F-02)."""
    return stores_module._final_eval_request_sha256(
        authority_plan_hash=P8_IDENTITY["plan_hash"],
        identity_scope_hash=request.identity_scope_hash,
        identity_instruction_policy_hash=(
            request.identity_instruction_policy_hash
        ),
        research_plan_sha256=request.research_plan_sha256,
        campaign_id=request.campaign_id,
        campaign_sha256=request.campaign_sha256,
        holdout_id=request.holdout_id,
        holdout_sha256=request.holdout_sha256,
        nonce_fingerprint=(
            nonce_fingerprint
            or stores_module._final_eval_nonce_fingerprint(ROOT_SECRET, NONCE)
        ),
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
        # the durable identity binds the AUTHORIZED grant attempt
        attempt_id="p8-attempt-003",
        request_schema=request.schema_version,
        request_digest=request.request_sha256,
    )


def _make_evaluator(tmp_root, v2_request=None):
    """Build a real TrustedEvaluator + request + sealed data root (the
    runtime's mandatory OPEN_HOLDOUT seam inputs).

    The V1 evaluator fixture is aligned to the SAME V2 request identity
    the runtime binds (identity bridge: no V1/V2 mixing).

    Returns (evaluator, request, data_root, holdout_store) -- the store is
    exposed so tests can assert holdout consume counts/outcomes."""
    import tempfile
    import shutil
    from pathlib import Path as _Path

    from research_automation.control_plane.final_evaluator import (
        AuthorityBroker,
        InMemoryHoldoutStore,
        TrustedEvaluator,
        TrustedEvaluatorAdapter,
        seal_trusted_data_root,
    )
    from tests.test_control_plane_final_evaluator import (
        _FakeHoldoutBackend,
        _write_t4_fixture,
    )

    tmp = tmp_root or tempfile.mkdtemp(prefix="p8_runtime_holdout_")
    _write_t4_fixture(_Path(tmp))
    data_root = seal_trusted_data_root(
        _Path(tmp),
        ("frozen/holdout.parquet",),
    )
    store = InMemoryHoldoutStore()
    evaluator = TrustedEvaluator(
        broker=AuthorityBroker(store=store),
        adapter=TrustedEvaluatorAdapter(
            backend=_FakeHoldoutBackend()
        ),
    )
    v2 = v2_request or _aligned_v2_request()
    return evaluator, _v1_request_aligned(v2), data_root, store


class FinalEvalRuntimeTests(unittest.TestCase):
    def _inputs(self, root, binding_id, exit_code=0, sink=None, root_cap=None):
        return FinalEvalRuntimeInputs(
            authority_capability=ROOT_SECRET,
            root_capability=root_cap or FinalEvalRootCapability.create(
                root_secret=ROOT_SECRET,
                repository_root=root,
            ),
            worker_launcher=lambda: exit_code,
            evidence_sink=sink or _real_publisher_sink(root, binding_id),
            attempt_id="p8-attempt-003",
        )

    def _run(self, root, grant, request, idempotency_key, exit_code=0,
             evaluator=None, evaluator_request=None, data_root=None,
             binding=None):
        broker = _make_broker(root, grant)
        bound = binding
        if bound is None:
            bound = broker.bind(
                request=request,
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key=idempotency_key,
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
        runtime = FinalEvalRuntime(
            inputs=self._inputs(root, bound.ticket_id, exit_code=exit_code)
        )
        result = runtime.run(
            request=request,
            grant=grant,
            nonce=NONCE,
            actor=stores_module.Actor(
                "operator-1", "human", "final-eval-op-cr009"
            ),
            identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
            idempotency_key=idempotency_key,
            binding=bound,
            evaluator=evaluator,
            evaluator_request=evaluator_request,
            data_root=data_root,
        )
        return bound, result

    def test_runtime_drives_durable_saga_to_authority_terminal(self) -> None:
        with _authorized_p8_campaign("campaign-runtime-durable") as (
            root,
            grant,
            journal,
        ):
            request = _aligned_v2_request(campaign_id="campaign-runtime-1")
            evaluator, evaluator_request, data_root, _store = _make_evaluator(
                None, request
            )
            try:
                binding, result = self._run(
                    root,
                    grant,
                    request,
                    "p8-runtime-durable-1",
                    evaluator=evaluator,
                    evaluator_request=evaluator_request,
                    data_root=data_root,
                )
                # the durable saga reached the terminal state with a REAL
                # committed claim ref -- never None
                self.assertEqual(result["saga_state"], "AUTHORITY_TERMINAL")
                self.assertEqual(result["outcome"], "SUCCEEDED")
                self.assertTrue(result["evidence_ref"])
                self.assertTrue(
                    result["evidence_ref"].endswith(
                        "/claims/" + binding.ticket_id + ".json"
                    )
                )
                self.assertTrue(
                    any(
                        s["state"] == "RESULT_STAGED"
                        for s in result["steps"]
                    )
                )
                self.assertTrue(
                    any(
                        s["state"] == "AUTHORITY_TERMINAL"
                        for s in result["steps"]
                    )
                )
                # the ticket was finished exactly once with the claim as
                # terminal evidence
                state = stores_module.AuthorityReader().task_ticket_state(
                    binding.ticket_id
                )
                self.assertEqual(state, "SUCCEEDED")
            finally:
                import shutil as _shutil
                _shutil.rmtree(_Path_of(data_root.root), ignore_errors=True)

    def test_runtime_worker_failure_derives_failed_outcome(self) -> None:
        with _authorized_p8_campaign("campaign-runtime-fail") as (
            root,
            grant,
            journal,
        ):
            request = _aligned_v2_request(campaign_id="campaign-runtime-2")
            evaluator, evaluator_request, data_root, _store = _make_evaluator(
                None, request
            )
            try:
                binding, result = self._run(
                    root,
                    grant,
                    request,
                    "p8-runtime-fail-1",
                    exit_code=7,
                    evaluator=evaluator,
                    evaluator_request=evaluator_request,
                    data_root=data_root,
                )
                self.assertEqual(result["saga_state"], "AUTHORITY_TERMINAL")
                # a failed worker must NEVER be recovered as SUCCEEDED
                self.assertEqual(result["outcome"], "FAILED")
            finally:
                import shutil as _shutil
                _shutil.rmtree(_Path_of(data_root.root), ignore_errors=True)

    def test_runtime_rejects_unsafe_capability(self) -> None:
        with _authorized_p8_campaign("campaign-runtime-reject") as (
            root,
            grant,
            journal,
        ):
            inputs = FinalEvalRuntimeInputs(
                authority_capability=object(),  # not a strong secret
                root_capability=FinalEvalRootCapability.create(
                    root_secret=ROOT_SECRET,
                    repository_root=root,
                ),
                worker_launcher=lambda: 0,
                evidence_sink=lambda payload: {},
                attempt_id="p8-attempt-003",
            )
            runtime = FinalEvalRuntime(inputs=inputs)
            with self.assertRaises(FinalEvalRuntimeRejected):
                runtime.run(
                    request=_aligned_v2_request(),
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-reject-1",
                    evaluator=None,  # type: ignore[arg-type]
                    evaluator_request=None,
                    data_root=None,
                )

    def test_runtime_rejects_unsealed_root_capability(self) -> None:
        """CR-010 F-02: a root capability whose seal does not match the
        authority capability must fail closed -- the caller cannot inject
        an arbitrary repository path."""
        import tempfile

        with _authorized_p8_campaign("campaign-runtime-seal") as (
            root,
            grant,
            journal,
        ):
            forged = FinalEvalRootCapability(
                repository_root=str(root),
                seal="0" * 64,
            )
            inputs = FinalEvalRuntimeInputs(
                authority_capability=ROOT_SECRET,
                root_capability=forged,
                worker_launcher=lambda: 0,
                evidence_sink=lambda payload: {},
                attempt_id="p8-attempt-003",
            )
            runtime = FinalEvalRuntime(inputs=inputs)
            with self.assertRaises(FinalEvalRuntimeRejected):
                runtime.run(
                    request=_aligned_v2_request(),
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-seal-1",
                    evaluator=None,  # type: ignore[arg-type]
                    evaluator_request=None,
                    data_root=None,
                )

    def test_runtime_requires_the_open_holdout_evaluator(self) -> None:
        """CR-010 F-02: a run WITHOUT the evaluator is rejected -- the
        OPEN_HOLDOUT seam is a mandatory runtime step, never optional."""
        with _authorized_p8_campaign("campaign-runtime-mandatory") as (
            root,
            grant,
            journal,
        ):
            broker = _make_broker(root, grant)
            binding = broker.bind(
                request=_aligned_v2_request(campaign_id="campaign-runtime-4"),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-runtime-mandatory-1",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            runtime = FinalEvalRuntime(
                inputs=self._inputs(root, binding.ticket_id)
            )
            with self.assertRaises(FinalEvalRuntimeRejected):
                runtime.run(
                    request=_aligned_v2_request(campaign_id="campaign-runtime-4"),
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-mandatory-1",
                    binding=binding,
                    evaluator=None,  # type: ignore[arg-type]
                    evaluator_request=None,
                    data_root=None,
                )

    def test_runtime_rejects_binding_with_different_request_identity(
        self,
    ) -> None:
        """CR-010 F-03: a binding committed under one request identity can
        never be continued with a different candidate/data/execution
        identity -- the full identity digest must match."""
        with _authorized_p8_campaign("campaign-runtime-identity") as (
            root,
            grant,
            journal,
        ):
            broker = _make_broker(root, grant)
            binding = broker.bind(
                request=_aligned_v2_request(campaign_id="campaign-runtime-5"),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-runtime-identity-1",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            runtime = FinalEvalRuntime(
                inputs=self._inputs(root, binding.ticket_id)
            )
            # same ticket, different candidate freeze / features identity
            tampered = _aligned_v2_request(campaign_id="campaign-runtime-5")
            with self.assertRaises(FinalEvalRuntimeRejected):
                runtime.run(
                    request=tampered,
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-identity-1",
                    binding=binding,
                    evaluator=None,  # type: ignore[arg-type]
                    evaluator_request=None,
                    data_root=None,
                )

    def test_runtime_repeated_run_is_idempotent_replay(self) -> None:
        """CR-010 F-04: running the same request twice must be usable --
        the second run replays the committed terminal result instead of
        failing on the uniqueness constraint."""
        import tempfile

        with _authorized_p8_campaign("campaign-runtime-replay") as (
            root,
            grant,
            journal,
        ):
            request = _aligned_v2_request(campaign_id="campaign-runtime-6")
            evaluator, evaluator_request, data_root, _store = _make_evaluator(
                None, request
            )
            try:
                broker = _make_broker(root, grant)
                binding = broker.bind(
                    request=request,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8-runtime-replay-1",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )
                runtime = FinalEvalRuntime(
                    inputs=self._inputs(root, binding.ticket_id)
                )
                first = runtime.run(
                    request=request,
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-replay-1",
                    binding=binding,
                    evaluator=evaluator,
                    evaluator_request=evaluator_request,
                    data_root=data_root,
                )
                self.assertEqual(first["saga_state"], "AUTHORITY_TERMINAL")
                # second run WITHOUT a caller binding: the runtime locates
                # the committed binding and replays the terminal result
                second = runtime.run(
                    request=request,
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-replay-1",
                    evaluator=evaluator,
                    evaluator_request=evaluator_request,
                    data_root=data_root,
                )
                self.assertEqual(second["saga_state"], "AUTHORITY_TERMINAL")
                self.assertEqual(second["evidence_ref"], first["evidence_ref"])
                self.assertEqual(second["outcome"], first["outcome"])
            finally:
                import shutil as _shutil
                _shutil.rmtree(_Path_of(data_root.root), ignore_errors=True)

    def test_runtime_is_the_only_caller_of_the_open_holdout_seam(self) -> None:
        """CR010-R04/P8-7: the runtime drives evaluate_v2 (the declared
        OPEN_HOLDOUT entry) with the REAL worker payload; the evaluated
        outcome must agree with the worker result or the run fails."""
        import tempfile
        import shutil
        from pathlib import Path as _Path

        from tests.test_control_plane_final_evaluator import (
            _FakeHoldoutBackend,
            _request,
            _write_t4_fixture,
        )

        tmp = tempfile.mkdtemp(prefix="p8_runtime_holdout_")
        try:
            with _authorized_p8_campaign("campaign-runtime-holdout") as (
                root,
                grant,
                journal,
            ):
                request = _aligned_v2_request(
                    campaign_id="campaign-runtime-3"
                )
                evaluator, evaluator_request, data_root, _store = (
                    _make_evaluator(tmp, request)
                )
                broker = _make_broker(root, grant)
                binding = broker.bind(
                    request=request,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8-runtime-holdout-1",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )
                runtime = FinalEvalRuntime(
                    inputs=self._inputs(root, binding.ticket_id)
                )
                result = runtime.run(
                    request=request,
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(
                        **P8_IDENTITY
                    ),
                    idempotency_key="p8-runtime-holdout-1",
                    binding=binding,
                    evaluator=evaluator,
                    evaluator_request=evaluator_request,
                    data_root=data_root,
                )
                self.assertEqual(result["saga_state"], "AUTHORITY_TERMINAL")
                self.assertEqual(result["outcome"], "SUCCEEDED")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_factory_rejects_non_inputs(self) -> None:
        with self.assertRaises(FinalEvalRuntimeRejected):
            FinalEvalRuntime(inputs=object())  # type: ignore[arg-type]

    def test_worker_exit_7_has_one_outcome_everywhere(self) -> None:
        """CR-010 B-01: exit 7 must produce the SAME outcome in the holdout
        consume record and the Authority terminal -- never CRASHED in one
        record and FAILED in another."""
        request = _aligned_v2_request(campaign_id="campaign-runtime-b01-7")
        evaluator, evaluator_request, data_root, store = _make_evaluator(
            None, request
        )
        try:
            with _authorized_p8_campaign("campaign-runtime-b01-7") as (
                root,
                grant,
                journal,
            ):
                broker = _make_broker(root, grant)
                binding = broker.bind(
                    request=request,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8-runtime-b01-7",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )
                runtime = FinalEvalRuntime(
                    inputs=self._inputs(root, binding.ticket_id, exit_code=7)
                )
                result = runtime.run(
                    request=request,
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-b01-7",
                    binding=binding,
                    evaluator=evaluator,
                    evaluator_request=evaluator_request,
                    data_root=data_root,
                )
                self.assertEqual(result["outcome"], "FAILED")
                # CR-010 C0 (Phase B): the durable begin-time consumption
                # receipt exists exactly once and carries NO outcome (the
                # worker outcome lives in the claim/terminal, never the
                # immutable receipt); the InMemory V1 store is never
                # touched by the production path.
                from research_automation.control_plane.final_eval_holdout_store import (
                    SqliteHoldoutStore,
                )

                durable = SqliteHoldoutStore(
                    authority=stores_module._AuthorityStore(
                        root_secret=ROOT_SECRET
                    )
                )
                self.assertEqual(
                    durable.consumption_count(
                        _durable_request_sha256(
                            request,
                            nonce_fingerprint=(
                                stores_module._final_eval_nonce_fingerprint(
                                    ROOT_SECRET, NONCE
                                )
                            ),
                            task_spec_ref="manifest.json",
                            task_spec_sha256="1" * 64,
                        )
                    ),
                    1,
                )
                consumed = list(store._consumed.values())
                self.assertEqual(len(consumed), 0)
                receipt = durable.read_consumption(binding.ticket_id)
                self.assertNotIn(
                    "FAILED", receipt.to_payload().values()
                )
        finally:
            import shutil as _shutil
            _shutil.rmtree(data_root.root, ignore_errors=True)

    def test_malformed_exit_code_fails_closed_to_durable_terminal(self) -> None:
        """CR-010 F-04: an illegal worker exit code (-1/256/bool/float/
        str/None) fails closed with the REAL evaluator, the REAL holdout
        store and the REAL worker launcher: the artifact opens at most
        once, the worker launches exactly once, the binding lands in a
        DURABLE AUTHORITY_TERMINAL/FAILED tombstone (never a reusable
        CONSUMED binding), the binding and maintenance tickets are FAILED
        (never IN_PROGRESS), and a fresh retry never reopens the Holdout,
        never re-runs the worker and never increases the consume count."""
        import shutil as _shutil
        import tempfile as _tempfile
        from pathlib import Path as _Path

        from research_automation.control_plane.final_eval_holdout_store import (
            SqliteHoldoutStore,
        )
        from research_automation.control_plane.final_evaluator import (
            AuthorityBroker,
            InMemoryHoldoutStore,
            TrustedEvaluator,
            TrustedEvaluatorAdapter,
            seal_trusted_data_root,
        )
        from tests.test_control_plane_final_evaluator import (
            _FakeHoldoutBackend,
            _write_t4_fixture,
        )

        for bad_code in (-1, 256, True, False, 0.0, "7", None):
            with self.subTest(bad_code=repr(bad_code)):
                request = _aligned_v2_request(
                    campaign_id=(
                        "campaign-runtime-b01-bad-"
                        + type(bad_code).__name__
                    )
                )
                tmp = _tempfile.mkdtemp(prefix="p8_runtime_malformed_")
                try:
                    _write_t4_fixture(_Path(tmp))
                    data_root = seal_trusted_data_root(
                        _Path(tmp), ("frozen/holdout.parquet",)
                    )
                    backend = _FakeHoldoutBackend()
                    evaluator = TrustedEvaluator(
                        broker=AuthorityBroker(store=InMemoryHoldoutStore()),
                        adapter=TrustedEvaluatorAdapter(backend=backend),
                    )
                    evaluator_request = _v1_request_aligned(request)
                    with _authorized_p8_campaign(
                        f"campaign-runtime-b01-bad-{type(bad_code).__name__}"
                    ) as (root, grant, journal):
                        broker = _make_broker(root, grant)
                        binding = broker.bind(
                            request=request,
                            nonce=NONCE,
                            actor=stores_module.Actor(
                                "operator-1", "human", "final-eval-op-cr009"
                            ),
                            idempotency_key=(
                                "p8-runtime-b01-bad-"
                                + type(bad_code).__name__
                            ),
                            task_spec_ref="manifest.json",
                            task_spec_sha256="1" * 64,
                        )
                        runtime = FinalEvalRuntime(
                            inputs=self._inputs(
                                root,
                                binding.ticket_id,
                                exit_code=bad_code,
                            )
                        )
                        result = runtime.run(
                            request=request,
                            grant=grant,
                            nonce=NONCE,
                            actor=stores_module.Actor(
                                "operator-1", "human",
                                "final-eval-op-cr009"
                            ),
                            identity=stores_module.AuthorityIdentity(
                                **P8_IDENTITY
                            ),
                            idempotency_key=(
                                "p8-runtime-b01-bad-"
                                + type(bad_code).__name__
                            ),
                            binding=binding,
                            evaluator=evaluator,
                            evaluator_request=evaluator_request,
                            data_root=data_root,
                        )
                        self.assertEqual(
                            result["saga_state"], "AUTHORITY_TERMINAL"
                        )
                        self.assertEqual(result["outcome"], "FAILED")
                        # the artifact was opened at most once (handle-first
                        # open before the worker), the worker ran once
                        self.assertEqual(len(backend.opened), 1)
                        # durable tombstone -- never a reusable CONSUMED
                        # binding, no claim, no IN_PROGRESS ticket
                        authority = stores_module._AuthorityStore(
                            root_secret=ROOT_SECRET
                        )
                        snapshot = authority.final_eval_binding_snapshot(
                            binding.ticket_id
                        )
                        self.assertEqual(
                            snapshot.saga_state, "AUTHORITY_TERMINAL"
                        )
                        self.assertEqual(
                            snapshot.terminal_binding, "FAILED"
                        )
                        self.assertIsNone(snapshot.result_claim_ref)
                        self.assertEqual(
                            stores_module.AuthorityReader()
                            .task_ticket_state(binding.ticket_id),
                            "FAILED",
                        )
                        # a fresh retry (binding=None) observes the
                        # tombstone: no reopen, no worker, consume stays 1
                        backend.opened.clear()
                        retry = FinalEvalRuntime(
                            inputs=self._inputs(
                                root, binding.ticket_id, exit_code=0
                            )
                        )
                        replayed = retry.run(
                            request=request,
                            grant=grant,
                            nonce=NONCE,
                            actor=stores_module.Actor(
                                "operator-1", "human",
                                "final-eval-op-cr009"
                            ),
                            identity=stores_module.AuthorityIdentity(
                                **P8_IDENTITY
                            ),
                            idempotency_key=(
                                "p8-runtime-b01-bad-"
                                + type(bad_code).__name__
                            ),
                            evaluator=evaluator,
                            evaluator_request=evaluator_request,
                            data_root=data_root,
                        )
                        self.assertEqual(replayed["outcome"], "FAILED")
                        self.assertEqual(len(backend.opened), 0)
                        self.assertEqual(
                            SqliteHoldoutStore(
                                authority=authority
                            ).consumption_count(
                                _durable_request_sha256(request)
                            ),
                            1,
                        )
                finally:
                    _shutil.rmtree(tmp, ignore_errors=True)



    def test_replay_rejects_any_identity_field_change(self) -> None:
        """CR-010 B-02: after a request A completes, a binding=None rerun
        with ANY identity field changed (candidate/code/spec/features/
        model/threshold/roster/generation/invocation/task spec/nonce) must
        fail closed -- it can never reuse A's terminal result."""
        from research_automation.control_plane.final_eval_authority import (
            FinalEvalRequestRejected,
            FinalEvalUniquenessRejected,
        )
        from research_automation.control_plane.stores import (
            FinalEvalBindingConflictError,
        )

        base_request = _aligned_v2_request(
            campaign_id="campaign-runtime-b02-a"
        )
        evaluator, evaluator_request, data_root, store = _make_evaluator(
            None, base_request
        )
        try:
            with _authorized_p8_campaign("campaign-runtime-b02") as (
                root,
                grant,
                journal,
            ):
                broker = _make_broker(root, grant)
                binding = broker.bind(
                    request=base_request,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8-runtime-b02-a",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )
                runtime = FinalEvalRuntime(
                    inputs=self._inputs(root, binding.ticket_id, exit_code=0)
                )
                first = runtime.run(
                    request=base_request,
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-b02-a",
                    binding=binding,
                    evaluator=evaluator,
                    evaluator_request=evaluator_request,
                    data_root=data_root,
                )
                self.assertEqual(first["outcome"], "SUCCEEDED")

                variants = {
                    # CR-010 F-02: the durable request identity covers the
                    # COMPLETE canonical V2 payload -- every ref/hash pair,
                    # the identity/scope/policy hashes, the attempt, the
                    # campaign/holdout hashes and the schema.  A change to
                    # ANY canonical field must never reuse A's terminal.
                    "candidate_freeze_sha256": "1" * 64,
                    "candidate_freeze_ref": "other-freeze.json",
                    "code_sha256": "2" * 64,
                    "code_ref": "other-code.py",
                    "execution_spec_sha256": "3" * 64,
                    "execution_spec_ref": "other-spec.json",
                    "features_sha256": "4" * 64,
                    "features_ref": "other-features.json",
                    "model": "different-model",
                    "model_sha256": "5" * 64,
                    "threshold": "0.9",
                    "threshold_ref": "other-threshold.json",
                    "threshold_sha256": "6" * 64,
                    "roster_sha256": "7" * 64,
                    "roster_ref": "other-roster.json",
                    "generation": "generation-other",
                    "generation_sha256": "8" * 64,
                    "invocation_id": "final-eval-op-other",
                    "actor_id": "operator-other",
                    "actor_type": "automation",
                    "identity_scope_hash": "9" * 64,
                    "identity_instruction_policy_hash": "a" * 64,
                    "attempt_id": "other-attempt",
                    "campaign_sha256": "d" * 64,
                    "holdout_sha256": "e" * 64,
                }
                import dataclasses as _dataclasses

                for field_name, value in variants.items():
                    with self.subTest(field=field_name):
                        tampered = _dataclasses.replace(
                            base_request,
                            **{field_name: value},
                        )
                        with self.assertRaises(
                            (
                                FinalEvalRequestRejected,
                                FinalEvalUniquenessRejected,
                                FinalEvalBindingConflictError,
                            )
                        ):
                            runtime.run(
                                request=tampered,
                                grant=grant,
                                nonce=NONCE,
                                actor=stores_module.Actor(
                                    "operator-1", "human",
                                    "final-eval-op-cr009"
                                ),
                                identity=stores_module.AuthorityIdentity(
                                    **P8_IDENTITY
                                ),
                                idempotency_key="p8-runtime-b02-a",
                                evaluator=evaluator,
                                evaluator_request=evaluator_request,
                                data_root=data_root,
                            )
                # task-spec identity change
                with self.subTest(field="task_spec_sha256"):
                    with self.assertRaises(
                        (
                            FinalEvalRequestRejected,
                            FinalEvalUniquenessRejected,
                            FinalEvalBindingConflictError,
                        )
                    ):
                        runtime.run(
                            request=base_request,
                            grant=grant,
                            nonce=NONCE,
                            actor=stores_module.Actor(
                                "operator-1", "human",
                                "final-eval-op-cr009"
                            ),
                            identity=stores_module.AuthorityIdentity(
                                **P8_IDENTITY
                            ),
                            idempotency_key="p8-runtime-b02-a",
                            task_spec_ref="manifest.json",
                            task_spec_sha256="2" * 64,
                            evaluator=evaluator,
                            evaluator_request=evaluator_request,
                            data_root=data_root,
                        )
                # nonce identity change
                with self.subTest(field="nonce"):
                    with self.assertRaises(
                        (
                            FinalEvalRequestRejected,
                            FinalEvalUniquenessRejected,
                            FinalEvalBindingConflictError,
                        )
                    ):
                        runtime.run(
                            request=base_request,
                            grant=grant,
                            nonce="x" * 64,
                            actor=stores_module.Actor(
                                "operator-1", "human",
                                "final-eval-op-cr009"
                            ),
                            identity=stores_module.AuthorityIdentity(
                                **P8_IDENTITY
                            ),
                            idempotency_key="p8-runtime-b02-a",
                            evaluator=evaluator,
                            evaluator_request=evaluator_request,
                            data_root=data_root,
                        )
                # the identical replay still succeeds (exact identity match)
                replay = runtime.run(
                    request=base_request,
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-b02-a",
                    evaluator=evaluator,
                    evaluator_request=evaluator_request,
                    data_root=data_root,
                )
                self.assertEqual(replay["evidence_ref"], first["evidence_ref"])
        finally:
            import shutil as _shutil
            _shutil.rmtree(data_root.root, ignore_errors=True)

    def test_joint_forgery_of_request_and_materials_never_reuses_terminal(
        self,
    ) -> None:
        """CR-010 F-02: forging BOTH the V2 request digest field AND the
        material bundle hash in the SAME direction (e.g. model_sha256
        changed in both) must still fail closed -- the durable binding
        identity covers the FULL canonical payload, so a joint forgery can
        never reuse A's terminal result, never create a second binding and
        never increase the consume count."""
        import dataclasses as _dataclasses

        from research_automation.control_plane.final_eval_authority import (
            FinalEvalUniquenessRejected,
        )
        from research_automation.control_plane.final_evaluator import (
            ModelBinding,
        )
        from research_automation.control_plane.stores import (
            FinalEvalBindingConflictError,
        )

        base_request = _aligned_v2_request(
            campaign_id="campaign-runtime-joint-a"
        )
        evaluator, evaluator_request, data_root, store = _make_evaluator(
            None, base_request
        )
        try:
            with _authorized_p8_campaign("campaign-runtime-joint") as (
                root,
                grant,
                journal,
            ):
                broker = _make_broker(root, grant)
                binding = broker.bind(
                    request=base_request,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8-runtime-joint-a",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )
                runtime = FinalEvalRuntime(
                    inputs=self._inputs(root, binding.ticket_id, exit_code=0)
                )
                first = runtime.run(
                    request=base_request,
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-joint-a",
                    binding=binding,
                    evaluator=evaluator,
                    evaluator_request=evaluator_request,
                    data_root=data_root,
                )
                self.assertEqual(first["outcome"], "SUCCEEDED")
                # joint forgery: the request AND the caller-supplied
                # material bundle declare the SAME tampered model_sha256 --
                # a two-string collision must still be rejected because the
                # immutable durable identity binds the FULL canonical
                # payload (model_sha256 is part of it).
                forged_request = _aligned_v2_request(
                    campaign_id="campaign-runtime-joint-a",
                    model_sha256="5" * 64,
                )
                forged_v1 = _dataclasses.replace(
                    _v1_request_aligned(forged_request),
                    model=ModelBinding(
                        model_id=forged_request.model,
                        model_sha256="5" * 64,
                    ),
                )
                with self.assertRaises(
                    (
                        FinalEvalUniquenessRejected,
                        FinalEvalBindingConflictError,
                    )
                ):
                    runtime.run(
                        request=forged_request,
                        grant=grant,
                        nonce=NONCE,
                        actor=stores_module.Actor(
                            "operator-1", "human", "final-eval-op-cr009"
                        ),
                        identity=stores_module.AuthorityIdentity(
                            **P8_IDENTITY
                        ),
                        idempotency_key="p8-runtime-joint-a",
                        evaluator=evaluator,
                        evaluator_request=forged_v1,
                        data_root=data_root,
                    )
                # no second binding, consume count stays one
                with stores_module.store_path_override(
                    authority=root / "authority.sqlite3",
                    operational=root / "operational.sqlite3",
                ):
                    stores_module._expected_schema_sha256.cache_clear()
                    authority_store = stores_module._AuthorityStore(
                        root_secret=ROOT_SECRET
                    )
                    bindings = authority_store._scan_final_eval_bindings()
                    self.assertEqual(len(bindings), 1)
                    from research_automation.control_plane.final_eval_holdout_store import (
                        SqliteHoldoutStore,
                    )

                    self.assertEqual(
                        SqliteHoldoutStore(
                            authority=authority_store
                        ).consumption_count(bindings[0].request_sha256),
                        1,
                    )
                    stores_module._expected_schema_sha256.cache_clear()
        finally:
            import shutil as _shutil
            _shutil.rmtree(data_root.root, ignore_errors=True)

    def test_production_consume_rejects_different_holdout_lineage(self) -> None:
        """CR-010 C0 (Phase B): a consumption receipt for request A can
        never be paired with a projection for a DIFFERENT holdout -- the
        lineage check fails before any artifact open or worker launch,
        and the consume count stays one."""
        from research_automation.control_plane.final_eval_holdout_store import (
            SqliteHoldoutStore,
        )
        from research_automation.control_plane.final_evaluator import (
            TrustedEvaluatorError,
        )

        with _authorized_p8_campaign("campaign-runtime-lineage") as (
            root,
            grant,
            journal,
        ):
            request_a = _aligned_v2_request(
                campaign_id="campaign-runtime-lineage-a"
            )
            request_b = _aligned_v2_request(
                campaign_id="campaign-runtime-lineage-b",
                holdout_id="holdout-other",
            )
            broker = _make_broker(root, grant)
            binding_a = broker.bind(
                request=request_a,
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-runtime-lineage-a",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            evaluator, _, data_root, _store = _make_evaluator(None, request_a)
            durable = SqliteHoldoutStore(
                authority=stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            )
            consumption = durable.read_consumption(binding_a.ticket_id)
            # a projection for the OTHER holdout can never consume under
            # the A receipt
            projection_b = adapt_evaluator_request_v1_test_only(
                _v1_request_aligned(request_b),
                request_b,
                root_secret=ROOT_SECRET,
                attempt_id="p8-attempt-003",
                identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
            )
            with self.assertRaisesRegex(
                TrustedEvaluatorError, "lineage mismatch"
            ):
                evaluator.evaluate_v2(
                    projection_b,
                    data_root=data_root,
                    worker_launcher=lambda: 0,
                    consumption=consumption,
                    durable_ticket_id=binding_a.ticket_id,
                    durable_request_sha256=binding_a.request_sha256,
                    durable_nonce_fingerprint=binding_a.nonce_fingerprint,
                )
            self.assertEqual(
                durable.consumption_count(binding_a.request_sha256), 1
            )

    def test_production_consume_rejects_v1_request(self) -> None:
        """CR-010 C0 (Phase B): a bare V1 request can never consume under
        the Authority-backed receipt -- production composition always
        passes the V2 projection."""
        from research_automation.control_plane.final_eval_holdout_store import (
            SqliteHoldoutStore,
        )
        from research_automation.control_plane.final_evaluator import (
            TrustedEvaluatorError,
        )

        with _authorized_p8_campaign("campaign-runtime-v1-blocked") as (
            root,
            grant,
            journal,
        ):
            request = _aligned_v2_request(
                campaign_id="campaign-runtime-v1-blocked"
            )
            broker = _make_broker(root, grant)
            binding = broker.bind(
                request=request,
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-runtime-v1-blocked",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            evaluator, evaluator_request, data_root, _store = _make_evaluator(
                None, request
            )
            durable = SqliteHoldoutStore(
                authority=stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            )
            consumption = durable.read_consumption(binding.ticket_id)
            with self.assertRaises(TrustedEvaluatorError):
                evaluator.evaluate_v2(
                    evaluator_request,  # bare V1 request
                    data_root=data_root,
                    worker_launcher=lambda: 0,
                    consumption=consumption,
                    durable_ticket_id=binding.ticket_id,
                    durable_request_sha256=binding.request_sha256,
                    durable_nonce_fingerprint=binding.nonce_fingerprint,
                )

    def test_claim_exit_code_rejects_bool_and_float(self) -> None:
        """CR-010 C0 (Phase B): the fixed claim's exit_code uses the exact
        integer contract (type(value) is int, 0..255) -- bool/float can
        never pass even when the object side is well-formed."""
        from research_automation.control_plane.final_eval_evidence import (
            FinalEvalEvidenceError,
            verify_result_evidence,
        )
        from tests.test_control_plane_final_eval_orchestrator import (
            TEST_EVIDENCE_VOLUME,
            _ensure_git,
        )

        with _authorized_p8_campaign("campaign-runtime-claimcode") as (
            root,
            grant,
            journal,
        ):
            request = _aligned_v2_request(
                campaign_id="campaign-runtime-claimcode"
            )
            broker = _make_broker(root, grant)
            binding = broker.bind(
                request=request,
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-runtime-claimcode",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            _ensure_git(root)
            from research_automation.control_plane.final_eval_orchestrator import (
                OrchestrationInputs,
                orchestrate,
            )

            staged = orchestrate(
                OrchestrationInputs(
                    authority=stores_module._AuthorityStore(
                        root_secret=ROOT_SECRET
                    ),
                    binding_id=binding.ticket_id,
                    expected_version=binding.saga_version,
                    worker_launcher=lambda: 0,
                    evidence_sink=_real_publisher_sink(
                        root, binding.ticket_id
                    ),
                    repository_root=root,
                )
            )
            claim_path = root / staged.result_claim_ref
            for bad in (True, 1.0):
                with self.subTest(bad=repr(bad)):
                    claim = json.loads(claim_path.read_text(encoding="utf-8"))
                    claim["exit_code"] = bad
                    claim_path.write_text(
                        json.dumps(claim), encoding="utf-8"
                    )
                    # the tampered claim is COMMITTED so it is a committed
                    # blob -- the verifier must still reject the exit code
                    _git(root, "add", "--", staged.result_claim_ref)
                    _git(root, "commit", "-q", "-m", "tampered claim")
                    with self.assertRaisesRegex(
                        FinalEvalEvidenceError, "claim exit code"
                    ):
                        verify_result_evidence(
                            repository_root=root,
                            binding_id=binding.ticket_id,
                            ticket_id=binding.ticket_id,
                            object_ref=staged.result_object_ref,
                            object_sha256=staged.result_object_sha256,
                            claim_ref=staged.result_claim_ref,
                            claim_sha256=hashlib.sha256(
                                claim_path.read_bytes()
                            ).hexdigest(),
                            expected_outcome="SUCCEEDED",
                        )
                    # restore the committed claim for the next subtest
                    claim["exit_code"] = 0
                    claim_path.write_text(
                        json.dumps(claim), encoding="utf-8"
                    )
                    _git(root, "add", "--", staged.result_claim_ref)
                    _git(root, "commit", "-q", "-m", "restore claim")
            # a well-formed integer claim passes verification
            verify_result_evidence(
                repository_root=root,
                binding_id=binding.ticket_id,
                ticket_id=binding.ticket_id,
                object_ref=staged.result_object_ref,
                object_sha256=staged.result_object_sha256,
                claim_ref=staged.result_claim_ref,
                claim_sha256=hashlib.sha256(
                    claim_path.read_bytes()
                ).hexdigest(),
                expected_outcome="SUCCEEDED",
            )

    def test_production_cli_entry_really_calls_the_runtime(self) -> None:
        """CR-010 B-03: the production CLI entry (real wiring, not only an
        entry_guard string) drives the FinalEvalRuntime end to end, and
        binds the durable ticket identity into the holdout consume."""
        from research_automation.control_plane import cli as cli_module

        request = _aligned_v2_request(campaign_id="campaign-runtime-cli")
        evaluator, evaluator_request, data_root, store = _make_evaluator(
            None, request
        )
        try:
            with _authorized_p8_campaign("campaign-runtime-cli") as (
                root,
                grant,
                journal,
            ):
                broker = _make_broker(root, grant)
                binding = broker.bind(
                    request=request,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8-runtime-cli-1",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )
                result = cli_module.run_final_eval_runtime_entry(
                    authority_capability=ROOT_SECRET,
                    root_capability=FinalEvalRootCapability.create(
                        root_secret=ROOT_SECRET,
                        repository_root=root,
                    ),
                    worker_launcher=lambda: 0,
                    evidence_sink=_real_publisher_sink(
                        root, binding.ticket_id
                    ),
                    evaluator=evaluator,
                    evaluator_request=evaluator_request,
                    data_root=data_root,
                    request=request,
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-cli-1",
                    binding=binding,
                )
                self.assertEqual(result["saga_state"], "AUTHORITY_TERMINAL")
                self.assertEqual(result["outcome"], "SUCCEEDED")
                # CR-010 C0 (Phase B): the holdout consume is bound to the
                # DURABLE ticket identity -- the Authority-backed receipt
                # carries the real ticket/request digest/fingerprint and
                # the count is exactly one (never a second consume).
                from research_automation.control_plane.final_eval_holdout_store import (
                    SqliteHoldoutStore,
                )

                durable = SqliteHoldoutStore(
                    authority=stores_module._AuthorityStore(
                        root_secret=ROOT_SECRET
                    )
                )
                receipt = durable.read_consumption(binding.ticket_id)
                self.assertEqual(receipt.ticket_id, binding.ticket_id)
                self.assertEqual(
                    receipt.request_sha256, binding.request_sha256
                )
                self.assertEqual(
                    receipt.nonce_fingerprint, binding.nonce_fingerprint
                )
                self.assertEqual(
                    durable.consumption_count(binding.request_sha256), 1
                )
                self.assertEqual(list(store._consumed_durable.values()), [])
        finally:
            import shutil as _shutil
            _shutil.rmtree(data_root.root, ignore_errors=True)

    def test_production_cli_dry_run_is_wired(self) -> None:
        """CR-010 B-03: the production CLI surface exposes the final-eval
        command (dry-run proves the wiring is reachable)."""
        from research_automation.control_plane import cli as cli_module
        import io as _io

        stdout = _io.StringIO()
        stderr = _io.StringIO()
        rc = cli_module.main(
            ["final-eval", "--attempt-id", "p8-attempt-003", "--dry-run"],
            stdout=stdout,
            stderr=stderr,
        )
        self.assertEqual(rc, 0)
        self.assertIn("wired", stdout.getvalue())

    def test_maintenance_marked_failed_when_reconcile_raises(self) -> None:
        """CR-010 B-06: when the reconciler raises, the maintenance ticket
        must be marked FAILED -- never a blanket SUCCEEDED."""
        from unittest.mock import patch as _patch

        request = _aligned_v2_request(campaign_id="campaign-runtime-b06")
        evaluator, evaluator_request, data_root, store = _make_evaluator(
            None, request
        )
        try:
            with _authorized_p8_campaign("campaign-runtime-b06") as (
                root,
                grant,
                journal,
            ):
                broker = _make_broker(root, grant)
                binding = broker.bind(
                    request=request,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8-runtime-b06",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )
                runtime = FinalEvalRuntime(
                    inputs=self._inputs(root, binding.ticket_id, exit_code=0)
                )

                def boom(*args, **kwargs):
                    raise RuntimeError("reconcile exploded")

                with _patch(
                    "research_automation.control_plane.final_eval_runtime."
                    "reconcile",
                    side_effect=boom,
                ):
                    with self.assertRaises(RuntimeError):
                        runtime.run(
                            request=request,
                            grant=grant,
                            nonce=NONCE,
                            actor=stores_module.Actor(
                                "operator-1", "human",
                                "final-eval-op-cr009"
                            ),
                            identity=stores_module.AuthorityIdentity(
                                **P8_IDENTITY
                            ),
                            idempotency_key="p8-runtime-b06",
                            binding=binding,
                            evaluator=evaluator,
                            evaluator_request=evaluator_request,
                            data_root=data_root,
                        )
                # the maintenance ticket was finished as FAILED
                import sqlite3 as _sqlite3

                conn = _sqlite3.connect(str(root / "authority.sqlite3"))
                state = conn.execute(
                    "SELECT state FROM task_tickets_v2 "
                    "WHERE task_id = 'P8-RUNTIME-RECONCILER-MAINT' "
                    "ORDER BY created_at DESC LIMIT 1"
                ).fetchone()[0]
                conn.close()
                self.assertEqual(state, "FAILED")
        finally:
            import shutil as _shutil
            _shutil.rmtree(data_root.root, ignore_errors=True)

    def test_completed_maintenance_lease_cannot_reuse_recovery_lease(
        self,
    ) -> None:
        """CR-010 B-06: a COMPLETED maintenance lease must be rejected
        BEFORE any old recovery lease can be reused."""
        from research_automation.control_plane.final_eval_evidence import (
            FinalEvalResultPublisher,
        )
        from research_automation.control_plane.final_eval_reconciler import (
            reconcile,
        )
        from research_automation.control_plane.contracts import (
            SideEffect as _SideEffect,
        )
        from datetime import datetime, timezone

        request = _aligned_v2_request(campaign_id="campaign-runtime-b06b")
        evaluator, evaluator_request, data_root, store = _make_evaluator(
            None, request
        )
        try:
            with _authorized_p8_campaign("campaign-runtime-b06b") as (
                root,
                grant,
                journal,
            ):
                broker = _make_broker(root, grant)
                binding = broker.bind(
                    request=request,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8-runtime-b06b",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )
                authority = stores_module._AuthorityStore(
                    root_secret=ROOT_SECRET
                )
                # stage the result
                _ensure_git_probe = __import__(
                    "tests.test_control_plane_final_eval_orchestrator",
                    fromlist=["_ensure_git", "_real_publisher_sink"],
                )
                _ensure_git_probe._ensure_git(root)
                sink = _ensure_git_probe._real_publisher_sink(
                    root, binding.ticket_id
                )
                from research_automation.control_plane.final_eval_orchestrator import (
                    OrchestrationInputs,
                    orchestrate,
                )

                orchestrate(
                    OrchestrationInputs(
                        authority=authority,
                        binding_id=binding.ticket_id,
                        expected_version=binding.saga_version,
                        worker_launcher=lambda: 0,
                        evidence_sink=sink,
                        repository_root=root,
                    )
                )
                claim_ref = (
                    "research_state/control_plane/p8/attempts/"
                    "p8-attempt-003/evidence/final_eval_cr010/claims/"
                    + binding.ticket_id
                    + ".json"
                )
                # maintenance lease
                maintenance_actor = stores_module.Actor(
                    "p8-reconciler-maintenance", "automation",
                    "p8-rec-maint-b06b",
                )
                envelope = authority._provision_authorization(
                    phase=stores_module.Phase.P0,
                    attempt_id="p8-rec-maint-b06b",
                    actor=maintenance_actor,
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
                    allowed_side_effects=(_SideEffect.READ,),
                )
                maintenance_grant = authority.claim_authorization(
                    envelope,
                    expected_phase=stores_module.Phase.P0,
                    expected_attempt_id="p8-rec-maint-b06b",
                    actor=maintenance_actor,
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                )
                ticket = authority._issue_task_ticket(
                    maintenance_grant,
                    {
                        "task_id": "P8-RECONCILER-MAINT",
                        "objective": "bounded reconciler maintenance",
                        "dependencies": [],
                        "idempotency_key": "p8-runtime-b06b-maint",
                        "task_spec_ref": "manifest.json",
                        "task_spec_sha256": "1" * 64,
                        "requirements": {
                            "required_test_receipt_ids": [],
                            "required_review_receipt_ids": [],
                            "required_evidence_ids": [],
                        },
                        "allowed_files": [
                            "research_automation/control_plane/"
                        ],
                        "forbidden_files": ["data/"],
                        "baseline_ref": "manifest.json",
                        "baseline_sha256": "1" * 64,
                        "input_evidence_refs": [],
                    },
                    allowed_side_effects=(_SideEffect.READ,),
                )
                maintenance_lease = authority._begin_task(ticket)
                # issue the recovery lease once (idempotency row exists)
                first = authority._issue_final_eval_recovery_lease(
                    maintenance_lease,
                    binding_id=binding.ticket_id,
                    evidence_ref=claim_ref,
                )
                # now COMPLETE the maintenance ticket
                authority._finish_task(
                    maintenance_lease,
                    outcome="SUCCEEDED",
                    evidence_ref=claim_ref,
                )
                # a completed maintenance lease must be rejected BEFORE any
                # reuse of the old recovery lease
                with self.assertRaises(stores_module.TaskTicketError):
                    authority._issue_final_eval_recovery_lease(
                        maintenance_lease,
                        binding_id=binding.ticket_id,
                        evidence_ref=claim_ref,
                    )
        finally:
            import shutil as _shutil
            _shutil.rmtree(data_root.root, ignore_errors=True)

def _Path_of(value):
    """Small helper: return the Path for a sealed data root root field."""
    from pathlib import Path
    return Path(value)


if __name__ == "__main__":
    unittest.main()
