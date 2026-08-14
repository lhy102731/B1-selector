"""Tests for the trusted final evaluation runtime (P8R3 T8, CR010-R04).

The runtime drives the DURABLE saga (Authority bind -> evaluate_v2 ->
orchestrator -> reconciler) with real committed evidence; the result
carries the real claim ref as evidence_ref and the observed durable
states as steps -- never an in-memory happy path.
"""

from __future__ import annotations

import unittest

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.final_eval_runtime import (
    FinalEvalRuntime,
    FinalEvalRuntimeInputs,
    FinalEvalRuntimeRejected,
)
from tests.test_control_plane_campaign_store import (
    ROOT_SECRET,
    _authorized_campaign,
)
from tests.test_control_plane_final_eval_orchestrator import (
    P8_IDENTITY,
    _make_broker,
    _make_request,
    _real_publisher_sink,
)

NONCE = "n" * 64


class FinalEvalRuntimeTests(unittest.TestCase):
    def _inputs(self, root, binding_id, exit_code=0, sink=None):
        return FinalEvalRuntimeInputs(
            authority_capability=ROOT_SECRET,
            root_capability=object(),
            worker_launcher=lambda: exit_code,
            evidence_sink=sink or _real_publisher_sink(root, binding_id),
            repository_root=root,
            attempt_id="p8-attempt-003",
        )

    def _run(self, root, grant, request, idempotency_key, exit_code=0):
        broker = _make_broker(root, grant)
        binding = broker.bind(
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
            inputs=self._inputs(root, binding.ticket_id, exit_code=exit_code)
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
            binding=binding,
        )
        return binding, result

    def test_runtime_drives_durable_saga_to_authority_terminal(self) -> None:
        with _authorized_campaign("campaign-runtime-durable") as (
            root,
            grant,
            journal,
        ):
            request = _make_request(campaign_id="campaign-runtime-1")
            binding, result = self._run(
                root, grant, request, "p8-runtime-durable-1"
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

    def test_runtime_worker_failure_derives_failed_outcome(self) -> None:
        with _authorized_campaign("campaign-runtime-fail") as (
            root,
            grant,
            journal,
        ):
            request = _make_request(campaign_id="campaign-runtime-2")
            binding, result = self._run(
                root, grant, request, "p8-runtime-fail-1", exit_code=7
            )
            self.assertEqual(result["saga_state"], "AUTHORITY_TERMINAL")
            # a failed worker must NEVER be recovered as SUCCEEDED
            self.assertEqual(result["outcome"], "FAILED")

    def test_runtime_rejects_unsafe_capability(self) -> None:
        with _authorized_campaign("campaign-runtime-reject") as (
            root,
            grant,
            journal,
        ):
            inputs = FinalEvalRuntimeInputs(
                authority_capability=object(),  # not a strong secret
                root_capability=object(),
                worker_launcher=lambda: 0,
                evidence_sink=lambda payload: {},
                repository_root=root,
            )
            runtime = FinalEvalRuntime(inputs=inputs)
            with self.assertRaises(FinalEvalRuntimeRejected):
                runtime.run(
                    request=_make_request(),
                    grant=grant,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    identity=stores_module.AuthorityIdentity(**P8_IDENTITY),
                    idempotency_key="p8-runtime-reject-1",
                )

    def test_runtime_is_the_only_caller_of_the_open_holdout_seam(self) -> None:
        """CR010-R04/P8-7: the runtime drives evaluate_v2 (the declared
        OPEN_HOLDOUT entry) with the REAL worker payload; the evaluated
        outcome must agree with the worker result or the run fails."""
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
            _request,
            _write_t4_fixture,
        )

        tmp = tempfile.mkdtemp(prefix="p8_runtime_holdout_")
        try:
            _write_t4_fixture(_Path(tmp))
            data_root = seal_trusted_data_root(
                _Path(tmp),
                ("frozen/holdout.parquet",),
            )
            evaluator = TrustedEvaluator(
                broker=AuthorityBroker(store=InMemoryHoldoutStore()),
                adapter=TrustedEvaluatorAdapter(
                    backend=_FakeHoldoutBackend()
                ),
            )
            with _authorized_campaign("campaign-runtime-holdout") as (
                root,
                grant,
                journal,
            ):
                request = _make_request(
                    campaign_id="campaign-runtime-3"
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
                    evaluator_request=_request(),
                    data_root=data_root,
                )
                self.assertEqual(result["saga_state"], "AUTHORITY_TERMINAL")
                self.assertEqual(result["outcome"], "SUCCEEDED")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_factory_rejects_non_inputs(self) -> None:
        with self.assertRaises(FinalEvalRuntimeRejected):
            FinalEvalRuntime(inputs=object())  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
