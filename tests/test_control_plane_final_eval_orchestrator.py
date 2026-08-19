"""P8 CR-009 Gate D: durable orchestration + hard-crash recovery harness.

Covers:
- durable saga orchestration through the Authority CAS (CONSUMED ->
  EVALUATING -> RESULT_STAGED), with worker result derived, never caller
  supplied;
- 6+ fixed hard-crash points: a child process hard-exits after each durable
  transition; a FRESH process observes the committed state and continues
  idempotently (CAS replay), never reopening holdout bytes;
- bounded reconciler: recovers only RESULT_STAGED/CLOSED bindings with a
  fixed claim; never recomputes, never reissues the original lease, never
  touches intermediate states.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import (
    SideEffect,
    canonical_json,
)
from research_automation.control_plane.final_eval_authority import (
    FINAL_EVAL_REQUEST_V2,
    AuthorityFinalEvalBroker,
    FinalEvalRequestV2,
    _nonce_fingerprint,
)
from research_automation.control_plane.final_eval_orchestrator import (
    CRASH_POINTS,
    FinalEvalOrchestrationError,
    OrchestrationInputs,
    orchestrate,
)
from research_automation.control_plane.final_eval_reconciler import (
    FinalEvalReconcilerError,
    reconcile,
)
from tests.test_control_plane_campaign_store import (
    _authorized_p8_campaign,
    ROOT_SECRET,
    _authorized_campaign,
)

P8_IDENTITY = {
    "plan_hash": "0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
    "scope_hash": "8c6b4a7547275728c7beef294cd8e5d56fdddf5da82509e09e88162e8c6243be",
    "instruction_policy_hash": "0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
}
NONCE = "n" * 64

# Child harness: creates a binding via the broker, then runs the
# orchestrator with a crash hook that hard-exits at the requested point.
# The fresh process then observes the durable state and continues.
CHILD = """
import os
import sys
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.final_eval_authority import (
    FINAL_EVAL_REQUEST_V2,
    AuthorityFinalEvalBroker,
    FinalEvalRequestV2,
    _nonce_fingerprint,
)
from research_automation.control_plane.final_eval_orchestrator import (
    OrchestrationInputs,
    orchestrate,
)
from research_automation.control_plane.contracts import Actor, canonical_json

root = Path(sys.argv[1])
crash_point = sys.argv[2]
plan_hash = sys.argv[3]
scope_hash = sys.argv[4]
instr_hash = sys.argv[5]

patch.multiple(
    stores_module,
    _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
    _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
).start()
stores_module._expected_schema_sha256.cache_clear()
authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)

def broker_for(grant):
    identity = stores_module.AuthorityIdentity(
        plan_hash=plan_hash, scope_hash=scope_hash,
        instruction_policy_hash=instr_hash,
    )
    return AuthorityFinalEvalBroker(
        authority=authority, grant=grant, attempt_id="p8-attempt-003",
        identity=identity,
    )

def crash_hook(state):
    if state == crash_point:
        os._exit(9)

def worker():
    return 0

def sink(document):
    return "research_state/control_plane/p8/attempts/p8-attempt-003/evidence/worker_result.json"
"""


def _ensure_git(root: Path) -> None:
    """Idempotent git repo in the fixture root for committed evidence."""
    if not (root / ".git").exists():
        subprocess.run(["git", "init", "--quiet"], cwd=root, check=True,
                       capture_output=True)
        subprocess.run(["git", "config", "user.name", "Control Plane Tests"],
                       cwd=root, check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email",
                        "control-plane@example.invalid"],
                       cwd=root, check=True, capture_output=True)


TEST_EVIDENCE_VOLUME = (
    "research_state/control_plane/p8/attempts/p8-attempt-003/"
    "evidence/final_eval_cr010"
)


def _real_publisher_sink(root: Path, binding_id: str):
    """CR010-R02: real create-only object + per-ticket fixed claim sink.

    Publishes the content-addressed object and the fixed claim (committed,
    same volume) and returns the four refs the orchestrator stages.
    """
    from research_automation.control_plane.final_eval_evidence import (
        FinalEvalResultPublisher,
    )

    _ensure_git(root)
    publisher = FinalEvalResultPublisher(
        repository_root=root,
        evidence_volume=TEST_EVIDENCE_VOLUME,
    )

    def sink(document):
        outcome = str(document.get("outcome", "SUCCEEDED"))
        refs = publisher.publish(
            binding_id,
            binding_id,
            dict(document),
            outcome=outcome,
        )
        return refs.to_payload()

    return sink


def _claim_ref_for(binding_id: str) -> str:
    return TEST_EVIDENCE_VOLUME + "/claims/" + binding_id + ".json"



def _make_request(
    *,
    campaign_id: str = "campaign-final-cr009",
    holdout_id: str = "holdout-final-cr009",
    plan_sha256: str = "a" * 64,
    holdout_sha256: str = "c" * 64,
    nonce: str = NONCE,
) -> FinalEvalRequestV2:
    return FinalEvalRequestV2(
        schema_version=FINAL_EVAL_REQUEST_V2,
        research_plan_sha256=plan_sha256,
        campaign_id=campaign_id,
        campaign_sha256="b" * 64,
        holdout_id=holdout_id,
        holdout_sha256=holdout_sha256,
        nonce_fingerprint=_nonce_fingerprint(ROOT_SECRET, nonce),
        candidate_freeze_ref=(
            "research_state/control_plane/p8/attempts/p8-attempt-003/"
            "freeze.json"
        ),
        candidate_freeze_sha256="d" * 64,
        code_sha256="e" * 64,
        execution_spec_sha256="f" * 64,
        features_sha256="9" * 64,
        model="final-model-cr009",
        threshold="0.5",
        roster_sha256="5" * 64,
        generation="generation-final-cr009",
        actor_id="operator-1",
        actor_type="human",
        invocation_id="final-eval-op-cr009",
        authority_plan_hash=P8_IDENTITY["plan_hash"],
    )


def _make_broker(root, grant):
    authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
    identity = stores_module.AuthorityIdentity(**P8_IDENTITY)
    return AuthorityFinalEvalBroker(
        authority=authority,
        grant=grant,
        attempt_id="p8-attempt-003",
        identity=identity,
    )


class FinalEvalOrchestrationTests(unittest.TestCase):
    def _broker(self, root, grant):
        return _make_broker(root, grant)

    def _request(self) -> FinalEvalRequestV2:
        return _make_request()

    def test_orchestrator_advances_consumed_to_result_staged(self) -> None:
        campaign_id = "campaign-orch-ok"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=self._request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-orch-ok",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            self.assertEqual(binding.saga_state, "CONSUMED")
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            results = {}
            sink = _real_publisher_sink(root, binding.ticket_id)

            def recording_sink(document):
                results["document"] = document
                return sink(document)

            snapshot = orchestrate(
                OrchestrationInputs(
                    authority=authority,
                    binding_id=binding.ticket_id,
                    expected_version=binding.saga_version,
                    worker_launcher=lambda: 0,
                    evidence_sink=recording_sink,
                    repository_root=root,
                )
            )
            self.assertEqual(snapshot.saga_state, "RESULT_STAGED")
            self.assertEqual(
                snapshot.result_claim_ref,
                _claim_ref_for(binding.ticket_id),
            )
            self.assertEqual(
                snapshot.result_object_ref,
                TEST_EVIDENCE_VOLUME + "/objects/" +
                snapshot.result_object_sha256 + ".json",
            )
            self.assertEqual(results["document"]["binding_id"], binding.ticket_id)
            self.assertEqual(results["document"]["exit_code"], 0)
            self.assertEqual(results["document"]["outcome"], "SUCCEEDED")

    def test_orchestrator_rejects_dangling_result_ref(self) -> None:
        """CR010-R02: a sink returning a nonexistent object/claim path must
        fail closed -- dangling refs never enter RESULT_STAGED."""
        with _authorized_p8_campaign("campaign-orch-dangling") as (root, grant, journal):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=self._request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-orch-dangling",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)

            def dangling_sink(document):
                return {
                    "object_ref": TEST_EVIDENCE_VOLUME + "/objects/" + "d" * 64 + ".json",
                    "object_sha256": "d" * 64,
                    "claim_ref": TEST_EVIDENCE_VOLUME + "/claims/" + binding.ticket_id + ".json",
                    "claim_sha256": "e" * 64,
                }

            with self.assertRaises(FinalEvalOrchestrationError):
                orchestrate(
                    OrchestrationInputs(
                        authority=authority,
                        binding_id=binding.ticket_id,
                        expected_version=binding.saga_version,
                        worker_launcher=lambda: 0,
                        evidence_sink=dangling_sink,
                        repository_root=root,
                    )
                )
            observed = authority.final_eval_binding_snapshot(binding.ticket_id)
            self.assertEqual(observed.saga_state, "EVALUATING")

    def test_orchestrator_rejects_wrong_result_hash(self) -> None:
        """CR010-R02: a committed object whose bytes do not match the
        declared content hash must fail closed."""
        from research_automation.control_plane.final_eval_evidence import (
            FinalEvalResultPublisher,
        )

        with _authorized_p8_campaign("campaign-orch-badhash") as (root, grant, journal):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=self._request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-orch-badhash",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            _ensure_git(root)
            publisher = FinalEvalResultPublisher(
                repository_root=root,
                evidence_volume=TEST_EVIDENCE_VOLUME,
            )
            real = publisher.publish(
                binding.ticket_id,
                binding.ticket_id,
                {"binding_id": binding.ticket_id, "outcome": "SUCCEEDED"},
                outcome="SUCCEEDED",
            )

            def wrong_hash_sink(document):
                payload = real.to_payload()
                payload["object_sha256"] = "f" * 64
                return payload

            with self.assertRaises(FinalEvalOrchestrationError):
                orchestrate(
                    OrchestrationInputs(
                        authority=authority,
                        binding_id=binding.ticket_id,
                        expected_version=binding.saga_version,
                        worker_launcher=lambda: 0,
                        evidence_sink=wrong_hash_sink,
                        repository_root=root,
                    )
                )
            observed = authority.final_eval_binding_snapshot(binding.ticket_id)
            self.assertEqual(observed.saga_state, "EVALUATING")

    def test_orchestrator_rejects_stale_expected_version(self) -> None:
        """CR010-R03: a stale/wrong expected_version fails closed before
        any durable advance."""
        with _authorized_p8_campaign("campaign-orch-stale") as (root, grant, journal):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=self._request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-orch-stale",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            with self.assertRaises(FinalEvalOrchestrationError):
                orchestrate(
                    OrchestrationInputs(
                        authority=authority,
                        binding_id=binding.ticket_id,
                        expected_version=binding.saga_version + 99,
                        worker_launcher=lambda: 0,
                        evidence_sink=_real_publisher_sink(root, binding.ticket_id),
                        repository_root=root,
                    )
                )
            observed = authority.final_eval_binding_snapshot(binding.ticket_id)
            self.assertEqual(observed.saga_state, "CONSUMED")

    def test_orchestrator_rejects_sink_forged_success_for_failed_worker(
        self,
    ) -> None:
        """CR-010 F-01: a worker that failed MUST NOT be able to stage a
        SUCCEEDED claim.  The pre-staging verification binds the claim
        outcome to the worker-derived outcome; a dishonest sink publishing
        SUCCEEDED for a failed worker fails closed and the binding stays
        EVALUATING."""
        with _authorized_p8_campaign("campaign-orch-forge") as (root, grant, journal):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=self._request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-orch-forge",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            from research_automation.control_plane.final_eval_evidence import (
                FinalEvalResultPublisher,
            )

            _ensure_git(root)
            publisher = FinalEvalResultPublisher(
                repository_root=root,
                evidence_volume=TEST_EVIDENCE_VOLUME,
            )

            def forging_sink(document):
                # the sink IGNORES the worker result and publishes SUCCEEDED
                return publisher.publish(
                    binding.ticket_id,
                    binding.ticket_id,
                    dict(document),
                    outcome="SUCCEEDED",
                ).to_payload()

            with self.assertRaises(FinalEvalOrchestrationError):
                orchestrate(
                    OrchestrationInputs(
                        authority=authority,
                        binding_id=binding.ticket_id,
                        expected_version=binding.saga_version,
                        worker_launcher=lambda: 7,  # worker FAILED
                        evidence_sink=forging_sink,
                        repository_root=root,
                    )
                )
            observed = authority.final_eval_binding_snapshot(binding.ticket_id)
            # the forged SUCCEEDED claim never entered RESULT_STAGED
            self.assertEqual(observed.saga_state, "EVALUATING")

    def test_orchestrator_rejects_malformed_worker_exit_codes(self) -> None:
        """CR-010 F-06: the orchestrator itself rejects a non-int or
        out-of-range worker exit code -- None, str, float, bool and
        negative/>255 values fail closed HERE, not only in an upper
        runtime layer."""
        for bad_code in (None, "0", 1.5, True, -1, 256):
            with self.subTest(bad_code=repr(bad_code)):
                campaign_id = f"campaign-orch-badcode-{type(bad_code).__name__}"
                with _authorized_p8_campaign(campaign_id) as (
                    root,
                    grant,
                    journal,
                ):
                    broker = self._broker(root, grant)
                    binding = broker.bind(
                        request=_make_request(
                            campaign_id=campaign_id,
                            holdout_id=campaign_id + "-holdout",
                            plan_sha256="2" * 64,
                            holdout_sha256="3" * 64,
                            nonce="4" * 64,
                        ),
                        nonce="4" * 64,
                        actor=stores_module.Actor(
                            "operator-1", "human", "final-eval-op-cr009"
                        ),
                        idempotency_key="p8-cr009-badcode-" + campaign_id,
                        task_spec_ref="manifest.json",
                        task_spec_sha256="1" * 64,
                    )
                    authority = stores_module._AuthorityStore(
                        root_secret=ROOT_SECRET
                    )
                    with self.assertRaises(FinalEvalOrchestrationError):
                        orchestrate(
                            OrchestrationInputs(
                                authority=authority,
                                binding_id=binding.ticket_id,
                                expected_version=binding.saga_version,
                                worker_launcher=lambda: bad_code,
                                evidence_sink=_real_publisher_sink(
                                    root, binding.ticket_id
                                ),
                                repository_root=root,
                            )
                        )
                    observed = authority.final_eval_binding_snapshot(
                        binding.ticket_id
                    )
                    self.assertEqual(observed.saga_state, "EVALUATING")

    def test_orchestrator_maps_worker_exit_codes_to_fixed_outcomes(self) -> None:
        """CR-010 F-06: the outcome mapping is fixed -- 0 -> SUCCEEDED,
        124 -> TIMEOUT, any other non-zero -> FAILED.  The caller can never
        choose the outcome."""
        from research_automation.control_plane.final_eval_evidence import (
            FinalEvalResultPublisher,
        )

        for exit_code, expected_outcome in (
            (0, "SUCCEEDED"),
            (124, "TIMEOUT"),
            (7, "FAILED"),
        ):
            with self.subTest(exit_code=exit_code):
                campaign_id = f"campaign-orch-map-{exit_code}"
                with _authorized_p8_campaign(campaign_id) as (
                    root,
                    grant,
                    journal,
                ):
                    broker = self._broker(root, grant)
                    binding = broker.bind(
                        request=_make_request(
                            campaign_id=campaign_id,
                            holdout_id=campaign_id + "-holdout",
                            plan_sha256="5" * 64,
                            holdout_sha256="6" * 64,
                            nonce="7" * 64,
                        ),
                        nonce="7" * 64,
                        actor=stores_module.Actor(
                            "operator-1", "human", "final-eval-op-cr009"
                        ),
                        idempotency_key="p8-cr009-map-" + campaign_id,
                        task_spec_ref="manifest.json",
                        task_spec_sha256="1" * 64,
                    )
                    authority = stores_module._AuthorityStore(
                        root_secret=ROOT_SECRET
                    )
                    results = {}
                    sink = _real_publisher_sink(root, binding.ticket_id)

                    def recording_sink(document):
                        results["document"] = document
                        return sink(document)

                    orchestrate(
                        OrchestrationInputs(
                            authority=authority,
                            binding_id=binding.ticket_id,
                            expected_version=binding.saga_version,
                            worker_launcher=lambda: exit_code,
                            evidence_sink=recording_sink,
                            repository_root=root,
                        )
                    )
                    self.assertEqual(
                        results["document"]["outcome"], expected_outcome
                    )
                    observed = authority.final_eval_binding_snapshot(
                        binding.ticket_id
                    )
                    self.assertEqual(observed.saga_state, "RESULT_STAGED")

    def test_recovery_lease_is_idempotent_no_orphan_reissue(self) -> None:
        """CR-010 F-04: issuing the recovery lease twice for the same
        binding must reuse the existing lease -- never a second orphan
        ISSUED row."""
        with _authorized_p8_campaign("campaign-orch-leaseidem") as (
            root,
            grant,
            journal,
        ):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=self._request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-leaseidem",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            orchestrate(
                OrchestrationInputs(
                    authority=authority,
                    binding_id=binding.ticket_id,
                    expected_version=binding.saga_version,
                    worker_launcher=lambda: 0,
                    evidence_sink=_real_publisher_sink(
                        root, binding.ticket_id
                    ),
                    repository_root=root,
                )
            )
            claim_ref = _claim_ref_for(binding.ticket_id)
            lease = self._maintenance_lease(
                root, grant, "p8-cr009-leaseidem-maint"
            )
            first = authority._issue_final_eval_recovery_lease(
                lease,
                binding_id=binding.ticket_id,
                evidence_ref=claim_ref,
            )
            second = authority._issue_final_eval_recovery_lease(
                lease,
                binding_id=binding.ticket_id,
                evidence_ref=claim_ref,
            )
            self.assertEqual(first.lease_id, second.lease_id)
            conn = sqlite3.connect(str(root / "authority.sqlite3"))
            count = conn.execute(
                "SELECT COUNT(*) FROM final_eval_recovery_leases_v1 "
                "WHERE binding_id = ?",
                (binding.ticket_id,),
            ).fetchone()[0]
            conn.close()
            self.assertEqual(count, 1)

    def test_orchestrator_rejects_orphan_claim(self) -> None:
        """CR010-R02: a claim that does not reference the staged object is
        an orphan and must fail closed."""
        with _authorized_p8_campaign("campaign-orch-orphan") as (root, grant, journal):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=self._request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-orch-orphan",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)

            def orphan_sink(document):
                return {
                    "object_ref": TEST_EVIDENCE_VOLUME + "/objects/" + "a" * 64 + ".json",
                    "object_sha256": "a" * 64,
                    "claim_ref": _claim_ref_for(binding.ticket_id),
                    "claim_sha256": "b" * 64,
                }

            with self.assertRaises(FinalEvalOrchestrationError):
                orchestrate(
                    OrchestrationInputs(
                        authority=authority,
                        binding_id=binding.ticket_id,
                        expected_version=binding.saga_version,
                        worker_launcher=lambda: 0,
                        evidence_sink=orphan_sink,
                        repository_root=root,
                    )
                )

    def test_orchestrator_rejects_wrong_inputs(self) -> None:
        with self.assertRaises(FinalEvalOrchestrationError):
            orchestrate(object())  # type: ignore[arg-type]

    def test_reconciler_requires_maintenance_lease(self) -> None:
        with _authorized_p8_campaign("campaign-rec-no-lease") as (root, grant, journal):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            with self.assertRaises(FinalEvalReconcilerError):
                reconcile(authority, object())  # type: ignore[arg-type]

    def test_reconciler_recovers_staged_binding_without_reopen(self) -> None:
        campaign_id = "campaign-rec-ok"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=self._request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-rec-ok",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            claim_ref = _claim_ref_for(binding.ticket_id)
            orchestrate(
                OrchestrationInputs(
                    authority=authority,
                    binding_id=binding.ticket_id,
                    expected_version=binding.saga_version,
                    worker_launcher=lambda: 0,
                    evidence_sink=_real_publisher_sink(root, binding.ticket_id),
                    repository_root=root,
                )
            )
            # Fresh maintenance ticket + lease for the reconciler.
            lease = self._maintenance_lease(root, grant, "p8-cr009-rec-maint")
            report = reconcile(
                authority,
                lease,
                evidence_ref_for={binding.ticket_id: claim_ref},
                repository_root=root,
            )
            self.assertIn(binding.ticket_id, report.recovered)
            snapshot = authority.final_eval_binding_snapshot(binding.ticket_id)
            self.assertEqual(snapshot.saga_state, "AUTHORITY_TERMINAL")
            self.assertEqual(snapshot.terminal_binding, "SUCCEEDED")

    def _maintenance_lease(self, root, grant, idempotency_key):
        authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
        # A P0 maintenance grant avoids the active-entry-policy requirement
        # for non-P0 tickets; the reconciler only needs a real IN_PROGRESS
        # lease to issue bounded recovery leases.
        from datetime import datetime, timezone

        maintenance_actor = stores_module.Actor(
            "p8-reconciler-maintenance", "automation", "p8-rec-maint-001"
        )
        maintenance_identity = stores_module.AuthorityIdentity(
            **P8_IDENTITY
        )
        envelope = authority._provision_authorization(
            phase=stores_module.Phase.P0,
            attempt_id="p8-reconciler-maint",
            actor=maintenance_actor,
            identity=maintenance_identity,
            expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
            allowed_side_effects=(SideEffect.READ,),
        )
        maintenance_grant = authority.claim_authorization(
            envelope,
            expected_phase=stores_module.Phase.P0,
            expected_attempt_id="p8-reconciler-maint",
            actor=maintenance_actor,
            identity=maintenance_identity,
        )
        ticket = authority._issue_task_ticket(
            maintenance_grant,
            {
                "task_id": "P8-RECONCILER-MAINT",
                "objective": "bounded reconciler maintenance",
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

    def test_reconciler_leaves_intermediate_states_unresolved(self) -> None:
        campaign_id = "campaign-rec-intermediate"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=self._request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-rec-int",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            lease = self._maintenance_lease(root, grant, "p8-cr009-rec-int-maint")
            # Binding is CONSUMED (intermediate): reconciler must NOT touch it.
            report = reconcile(authority, lease)
            self.assertIn(binding.ticket_id, report.unresolved)
            snapshot = authority.final_eval_binding_snapshot(binding.ticket_id)
            self.assertEqual(snapshot.saga_state, "CONSUMED")


    def test_object_claim_semantic_conflict_fails_closed(self) -> None:
        """CR-010 B-07: an object whose outcome contradicts the claim
        (both committed, both hash-correct) can never enter RESULT_STAGED
        -- the object document is parsed and strictly bound to the claim
        and the worker-derived outcome."""
        from research_automation.control_plane.final_eval_evidence import (
            FINAL_EVAL_RESULT_CLAIM_SCHEMA,
            FinalEvalResultPublisher,
            _CLAIMS_DIR,
            _OBJECTS_DIR,
        )
        from research_automation.control_plane.contracts import canonical_json

        campaign_id = "campaign-orch-b07"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            broker = _make_broker(root, grant)
            binding = broker.bind(
                request=_make_request(campaign_id=campaign_id),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-b07",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            _ensure_git(root)
            volume = TEST_EVIDENCE_VOLUME
            # object claims SUCCEEDED, claim claims FAILED (worker exit 7)
            object_document = {
                "schema_version": "control_plane.final_eval_worker_result.v1",
                "binding_id": binding.ticket_id,
                "ticket_id": binding.ticket_id,
                "exit_code": 7,
                "outcome": "SUCCEEDED",
            }
            object_raw = canonical_json(object_document).encode("utf-8")
            import hashlib as _hashlib

            object_sha = _hashlib.sha256(object_raw).hexdigest()
            object_ref = f"{volume}/{_OBJECTS_DIR}/{object_sha}.json"
            claim_document = {
                "schema_version": FINAL_EVAL_RESULT_CLAIM_SCHEMA,
                "binding_id": binding.ticket_id,
                "ticket_id": binding.ticket_id,
                "object_ref": object_ref,
                "object_sha256": object_sha,
                "exit_code": 7,
                "outcome": "FAILED",
            }
            claim_raw = canonical_json(claim_document).encode("utf-8")
            claim_ref = f"{volume}/{_CLAIMS_DIR}/{binding.ticket_id}.json"
            for ref, raw in ((object_ref, object_raw), (claim_ref, claim_raw)):
                path = root / ref
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(raw)
            subprocess.run(
                ["git", "add", "--", object_ref, claim_ref],
                cwd=root, check=True, capture_output=True,
            )
            subprocess.run(
                ["git", "-c", "user.name=Control Plane Tests",
                 "-c", "user.email=control-plane@example.invalid",
                 "commit", "--quiet", "-m",
                 "audit: b07 conflicting object/claim"],
                cwd=root, check=True, capture_output=True,
            )

            def conflicting_sink(document):
                return {
                    "object_ref": object_ref,
                    "object_sha256": object_sha,
                    "claim_ref": claim_ref,
                    "claim_sha256": _hashlib.sha256(
                        claim_raw
                    ).hexdigest(),
                }

            with self.assertRaises(FinalEvalOrchestrationError):
                orchestrate(
                    OrchestrationInputs(
                        authority=authority,
                        binding_id=binding.ticket_id,
                        expected_version=binding.saga_version,
                        worker_launcher=lambda: 7,
                        evidence_sink=conflicting_sink,
                        repository_root=root,
                    )
                )
            observed = authority.final_eval_binding_snapshot(
                binding.ticket_id
            )
            self.assertEqual(observed.saga_state, "EVALUATING")

    def test_publisher_crash_mid_write_leaves_no_partial_final(self) -> None:
        """CR-010 B-07: a hard crash DURING the publisher's write leaves
        only an orphan temp -- the final object path never appears with
        partial bytes."""
        import hashlib as _hashlib
        import os as _os
        import subprocess as _sp
        import sys as _sys

        from research_automation.control_plane.final_eval_evidence import (
            FinalEvalResultPublisher,
        )
        from research_automation.control_plane.contracts import canonical_json

        campaign_id = "campaign-orch-b07crash"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            _ensure_git(root)
            volume = TEST_EVIDENCE_VOLUME
            binding_id = "ticket-b07-crash"
            publisher = FinalEvalResultPublisher(
                repository_root=root,
                evidence_volume=volume,
            )
            document = {
                "schema_version": "control_plane.final_eval_worker_result.v1",
                "binding_id": binding_id,
                "ticket_id": binding_id,
                "exit_code": 0,
                "outcome": "SUCCEEDED",
            }
            raw = canonical_json(document).encode("utf-8")
            sha = _hashlib.sha256(raw).hexdigest()
            final_path = root / volume / "objects" / f"{sha}.json"
            child = """import os, sys
from pathlib import Path
sys.path.insert(0, '.')
from research_automation.control_plane.final_eval_evidence import FinalEvalResultPublisher
p = FinalEvalResultPublisher(repository_root=Path(sys.argv[1]), evidence_volume=sys.argv[2])
ref = sys.argv[3]
raw = bytes.fromhex(sys.argv[4])
path = p._repository_root / ref
path.parent.mkdir(parents=True, exist_ok=True)
temp = path.with_name('.tmp-' + path.name + '-' + str(os.getpid()) + '-crash')
fd = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
with os.fdopen(fd, 'wb') as stream:
    stream.write(raw[:len(raw)//2])
    stream.flush()
    os.fsync(stream.fileno())
os._exit(9)  # hard crash BEFORE the link
"""
            ref = f"{volume}/objects/{sha}.json"
            result = _sp.run(
                [_sys.executable, "-c", child, str(root), volume, ref, raw.hex()],
                capture_output=True, text=True, timeout=120,
                cwd=Path(__file__).resolve().parents[1],
            )
            self.assertEqual(result.returncode, 9)
            self.assertFalse(final_path.exists())
            # a fresh publish of the same bytes succeeds via the atomic
            # temp+link path
            outcome = publisher.publish(
                binding_id,
                binding_id,
                dict(document),
                outcome="SUCCEEDED",
            )
            self.assertTrue(final_path.exists())
            self.assertEqual(outcome.object_sha256, sha)

if __name__ == "__main__":
    unittest.main()


class FinalEvalHardCrashHarnessTests(unittest.TestCase):
    """Fresh-process hard-crash recovery for the durable saga (CR-010 F-02).

    Each of the 6 fixed crash points maps to exactly one REACHABLE durable
    boundary of the saga:

      CONSUMED          -- broker bind committed (binding observable);
      EVALUATING        -- CONSUMED -> EVALUATING committed;
      CLAIM_WRITTEN     -- the sink wrote the claim blob, not yet staged;
      RESULT_STAGED     -- the claim was staged durably;
      RECOVERY_LEASE    -- the reconciler issued the recovery lease, the
                           binding is still untouched;
      AUTHORITY_TERMINAL-- the recover transaction committed.

    A child process hard-exits AFTER the boundary commits; a FRESH process
    observes the committed state and either continues the CAS replay
    (orchestrator) or closes the binding (reconciler), asserting that the
    next state never happened.  The sink really writes and commits the
    claim blob, so the fresh process can dereference it (same-volume
    object + fixed claim + Authority CAS binding).
    """

    CRASH_POINT_CASES = (
        # (crash point, expected durable state after crash, terminal?)
        ("CRASH_AFTER.CONSUMED", "CONSUMED", False),
        ("CRASH_AFTER.EVALUATING", "EVALUATING", False),
        ("CRASH_AFTER.CLAIM_WRITTEN", "EVALUATING", False),
        ("CRASH_AFTER.RESULT_STAGED", "RESULT_STAGED", True),
        ("CRASH_AFTER.RECOVERY_LEASE", "RESULT_STAGED", False),
        ("CRASH_AFTER.CLOSED", "CLOSED", True),
        ("CRASH_AFTER.AUTHORITY_TERMINAL", "AUTHORITY_TERMINAL", True),
    )

    _CHILD = """
import os
import sys
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.final_eval_authority import (
    FINAL_EVAL_REQUEST_V2,
    AuthorityFinalEvalBroker,
    FinalEvalRequestV2,
    _nonce_fingerprint,
)
from research_automation.control_plane.final_eval_orchestrator import (
    OrchestrationInputs,
    orchestrate,
)
from research_automation.control_plane.contracts import Actor, canonical_json

root = Path(sys.argv[1])
crash_point = sys.argv[2]
ROOT_SECRET = sys.argv[3]
binding_id = sys.argv[4]
expected_version = int(sys.argv[5])
maintenance_id = sys.argv[6]

patch.multiple(
    stores_module,
    _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
    _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
).start()
stores_module._expected_schema_sha256.cache_clear()
authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)

def crash_hook(state):
    if state == crash_point:
        os._exit(9)

def worker():
    return 0

def sink(document):
    # CR010-R02: real create-only object + fixed claim publisher (committed
    # evidence in the fixture root's git repo, same volume).
    from research_automation.control_plane.final_eval_evidence import (
        FinalEvalResultPublisher,
    )
    import subprocess as _sp
    if not (root / ".git").exists():
        _sp.run(["git", "init", "--quiet"], cwd=root, check=True,
                capture_output=True)
        _sp.run(["git", "config", "user.name", "Control Plane Tests"],
                cwd=root, check=True, capture_output=True)
        _sp.run(["git", "config", "user.email",
                 "control-plane@example.invalid"],
                cwd=root, check=True, capture_output=True)
    volume = ("research_state/control_plane/p8/attempts/p8-attempt-003/"
              "evidence/final_eval_cr010")
    publisher = FinalEvalResultPublisher(
        repository_root=root,
        evidence_volume=volume,
    )
    refs = publisher.publish(
        binding_id,
        binding_id,
        dict(document),
        outcome=str(document.get("outcome", "SUCCEEDED")),
    )
    return refs.to_payload()

try:
    orchestrate(
        OrchestrationInputs(
            authority=authority,
            binding_id=binding_id,
            expected_version=expected_version,
            worker_launcher=worker,
            evidence_sink=sink,
            crash_hook=crash_hook,
            repository_root=root,
        )
    )
    if crash_point in (
        "CRASH_AFTER.RECOVERY_LEASE",
        "CRASH_AFTER.CLOSED",
        "CRASH_AFTER.AUTHORITY_TERMINAL",
    ):
        from research_automation.control_plane.final_eval_reconciler import (
            reconcile,
        )
        from research_automation.control_plane.contracts import SideEffect
        from datetime import datetime, timezone

        maintenance_actor = stores_module.Actor(
            "p8-reconciler-maintenance", "automation",
            "p8-rec-maint-" + maintenance_id[-8:],
        )
        maintenance_identity = stores_module.AuthorityIdentity(
            plan_hash="0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
            scope_hash="8c6b4a7547275728c7beef294cd8e5d56fdddf5da82509e09e88162e8c6243be",
            instruction_policy_hash="0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
        )
        envelope = authority._provision_authorization(
            phase=stores_module.Phase.P0,
            attempt_id="p8-reconciler-maint",
            actor=maintenance_actor,
            identity=maintenance_identity,
            expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
            allowed_side_effects=(SideEffect.READ,),
        )
        maintenance_grant = authority.claim_authorization(
            envelope,
            expected_phase=stores_module.Phase.P0,
            expected_attempt_id="p8-reconciler-maint",
            actor=maintenance_actor,
            identity=maintenance_identity,
        )
        ticket = authority._issue_task_ticket(
            maintenance_grant,
            {
                "task_id": "P8-RECONCILER-MAINT",
                "objective": "bounded reconciler maintenance",
                "dependencies": [],
                "idempotency_key": maintenance_id,
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
        maintenance_lease = authority._begin_task(ticket)
        claim_ref = ("research_state/control_plane/p8/attempts/"
                     "p8-attempt-003/evidence/final_eval_cr010/claims/"
                     + binding_id + ".json")
        reconcile(
            authority,
            maintenance_lease,
            evidence_ref_for={binding_id: claim_ref},
            repository_root=root,
            crash_hook=crash_hook,
        )
    print("ORCHESTRATED", binding_id)
except Exception as exc:
    print("CHILD_ERROR", type(exc).__name__, str(exc)[:200])
    sys.exit(2)
"""

    def _run_child(
        self,
        root,
        crash_point,
        binding_id,
        expected_version,
        maintenance_id,
    ):
        from tests.test_control_plane_campaign_store import ROOT_SECRET

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                self._CHILD,
                str(root),
                crash_point,
                ROOT_SECRET,
                binding_id,
                str(expected_version),
                maintenance_id,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        return result

    def _observe(self, root, binding_id):
        """Read the durable binding in a FRESH process (disk only)."""
        from tests.test_control_plane_campaign_store import ROOT_SECRET

        observer = """
import sys
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module

root = Path(sys.argv[1])
ROOT_SECRET = sys.argv[2]
binding_id = sys.argv[3]

patch.multiple(
    stores_module,
    _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
    _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
).start()
stores_module._expected_schema_sha256.cache_clear()
authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
snapshot = authority.final_eval_binding_snapshot(binding_id)
print("|".join([snapshot.saga_state, str(snapshot.saga_version),
                snapshot.terminal_binding or "",
                snapshot.result_claim_ref or ""]))
"""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                observer,
                str(root),
                ROOT_SECRET,
                binding_id,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        parts = result.stdout.strip().split("|")
        return {
            "state": parts[0],
            "version": int(parts[1]),
            "terminal": parts[2],
            "claim_ref": parts[3],
        }

    def _recover(
        self,
        root,
        binding_id,
        crash_point,
        maintenance_id,
    ):
        """Continue the saga in a FRESH process without any crash hook."""
        from tests.test_control_plane_campaign_store import ROOT_SECRET

        if crash_point in (
            "CRASH_AFTER.RECOVERY_LEASE",
            "CRASH_AFTER.CLOSED",
            "CRASH_AFTER.AUTHORITY_TERMINAL",
        ):
            # Re-run the reconciler (bounded, no crash hook): recover the
            # binding or observe it already terminal.
            recoverer = """
import sys
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module

root = Path(sys.argv[1])
ROOT_SECRET = sys.argv[2]
binding_id = sys.argv[3]
maintenance_id = sys.argv[4]

patch.multiple(
    stores_module,
    _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
    _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
).start()
stores_module._expected_schema_sha256.cache_clear()
authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
from research_automation.control_plane.final_eval_reconciler import reconcile
from research_automation.control_plane.contracts import SideEffect, Actor
from datetime import datetime, timezone

maintenance_actor = stores_module.Actor(
    "p8-reconciler-maintenance", "automation",
    "p8-rec-maint-" + maintenance_id[-8:] + "-rec",
)
maintenance_identity = stores_module.AuthorityIdentity(
    plan_hash="0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
    scope_hash="8c6b4a7547275728c7beef294cd8e5d56fdddf5da82509e09e88162e8c6243be",
    instruction_policy_hash="0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
)
envelope = authority._provision_authorization(
    phase=stores_module.Phase.P0,
    attempt_id="p8-reconciler-maint",
    actor=maintenance_actor,
    identity=maintenance_identity,
    expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
    allowed_side_effects=(SideEffect.READ,),
)
maintenance_grant = authority.claim_authorization(
    envelope,
    expected_phase=stores_module.Phase.P0,
    expected_attempt_id="p8-reconciler-maint",
    actor=maintenance_actor,
    identity=maintenance_identity,
)
ticket = authority._issue_task_ticket(
    maintenance_grant,
    {
        "task_id": "P8-RECONCILER-MAINT",
        "objective": "bounded reconciler maintenance",
        "dependencies": [],
        "idempotency_key": maintenance_id,
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
maintenance_lease = authority._begin_task(ticket)
claim_ref = ("research_state/control_plane/p8/attempts/"
             "p8-attempt-003/evidence/final_eval_cr010/claims/"
             + binding_id + ".json")
report = reconcile(
    authority,
    maintenance_lease,
    evidence_ref_for={binding_id: claim_ref},
    repository_root=root,
)
snapshot = authority.final_eval_binding_snapshot(binding_id)
print(snapshot.saga_state, snapshot.saga_version,
      snapshot.terminal_binding or "", "|", ",".join(report.recovered))
"""
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    recoverer,
                    str(root),
                    ROOT_SECRET,
                    binding_id,
                    maintenance_id,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            line = result.stdout.strip()
            state, version, terminal, rest = line.split(" ", 3)
            return {
                "state": state,
                "version": int(version),
                "terminal": terminal,
                "recovered": rest.split("|")[1].split(","),
                "claim_ref": "",
            }
        # Orchestrator path: continue the CAS replay in a fresh process.
        replayer = """
import sys
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module

root = Path(sys.argv[1])
ROOT_SECRET = sys.argv[2]
binding_id = sys.argv[3]

patch.multiple(
    stores_module,
    _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
    _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
).start()
stores_module._expected_schema_sha256.cache_clear()
authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
from research_automation.control_plane.final_eval_orchestrator import (
    OrchestrationInputs,
    orchestrate,
)

snapshot = authority.final_eval_binding_snapshot(binding_id)

def worker():
    return 0

def sink(document):
    from research_automation.control_plane.final_eval_evidence import (
        FinalEvalResultPublisher,
    )
    import subprocess as _sp
    if not (root / ".git").exists():
        _sp.run(["git", "init", "--quiet"], cwd=root, check=True,
                capture_output=True)
        _sp.run(["git", "config", "user.name", "Control Plane Tests"],
                cwd=root, check=True, capture_output=True)
        _sp.run(["git", "config", "user.email",
                 "control-plane@example.invalid"],
                cwd=root, check=True, capture_output=True)
    volume = ("research_state/control_plane/p8/attempts/p8-attempt-003/"
              "evidence/final_eval_cr010")
    publisher = FinalEvalResultPublisher(
        repository_root=root,
        evidence_volume=volume,
    )
    refs = publisher.publish(
        binding_id,
        binding_id,
        dict(document),
        outcome=str(document.get("outcome", "SUCCEEDED")),
    )
    return refs.to_payload()

final = orchestrate(
    OrchestrationInputs(
        authority=authority,
        binding_id=binding_id,
        expected_version=snapshot.saga_version,
        worker_launcher=worker,
        evidence_sink=sink,
        repository_root=root,
    )
)
print("|".join([final.saga_state, str(final.saga_version),
                final.terminal_binding or ""]))
"""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                replayer,
                str(root),
                ROOT_SECRET,
                binding_id,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        state, version, terminal = result.stdout.strip().split("|")
        return {
            "state": state,
            "version": int(version),
            "terminal": terminal,
            "claim_ref": "",
        }

    def test_hard_crash_at_each_fixed_point_recovers_in_fresh_process(
        self,
    ) -> None:
        campaign_id = "campaign-crash-matrix"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            for index, (crash_point, expected_state, terminal) in enumerate(
                self.CRASH_POINT_CASES
            ):
                with self.subTest(crash_point=crash_point):
                    # bind (in this process, to get a real ticket id)
                    broker = _make_broker(root, grant)
                    binding = broker.bind(
                        request=_make_request(
                            campaign_id=f"campaign-crash-{index}",
                            holdout_id=f"holdout-crash-{index}",
                            plan_sha256=format(index, "064x"),
                            holdout_sha256=format(index + 100, "064x"),
                            nonce=format(index + 200, "064x"),
                        ),
                        nonce=format(index + 200, "064x"),
                        actor=stores_module.Actor(
                            "operator-1", "human", "final-eval-op-cr009"
                        ),
                        idempotency_key=f"p8-cr009-crash-{index}",
                        task_spec_ref="manifest.json",
                        task_spec_sha256="1" * 64,
                    )
                    maintenance_id = f"p8-cr009-crash-maint-{index}"
                    # 1. child hard-exits AFTER the boundary committed
                    result = self._run_child(
                        root,
                        crash_point,
                        binding.ticket_id,
                        binding.saga_version,
                        maintenance_id,
                    )
                    self.assertEqual(
                        result.returncode, 9,
                        "child must hard-exit at the crash point:\n"
                        + result.stdout
                        + result.stderr,
                    )
                    # 2. fresh process observes the committed durable state
                    observed = self._observe(root, binding.ticket_id)
                    self.assertEqual(
                        observed["state"], expected_state,
                        "crash must leave the binding in the durable state "
                        "committed BEFORE the crash point",
                    )
                    # 3. fresh process continues; next state never happened
                    recovered = self._recover(
                        root,
                        binding.ticket_id,
                        crash_point,
                        maintenance_id,
                    )
                    if crash_point in (
                        "CRASH_AFTER.RECOVERY_LEASE",
                        "CRASH_AFTER.CLOSED",
                        "CRASH_AFTER.AUTHORITY_TERMINAL",
                    ):
                        if crash_point == "CRASH_AFTER.RECOVERY_LEASE":
                            self.assertIn(
                                binding.ticket_id, recovered["recovered"]
                            )
                        self.assertEqual(
                            recovered["state"], "AUTHORITY_TERMINAL"
                        )
                        self.assertEqual(recovered["terminal"], "SUCCEEDED")
                    else:
                        self.assertEqual(recovered["state"], "RESULT_STAGED")
                        self.assertEqual(recovered["terminal"], "")

    def test_crash_points_are_six_fixed_boundaries(self) -> None:
        self.assertGreaterEqual(len(CRASH_POINTS), 6)
        self.assertEqual(
            set(CRASH_POINTS),
            {case[0] for case in self.CRASH_POINT_CASES},
        )
        for case in self.CRASH_POINT_CASES:
            self.assertIn(case[0], CRASH_POINTS)

    _RECONCILER = """
import sys
import json
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module

root = Path(sys.argv[1])
ROOT_SECRET = sys.argv[2]
binding_id = sys.argv[3]
maintenance_id = sys.argv[4]

patch.multiple(
    stores_module,
    _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
    _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
).start()
stores_module._expected_schema_sha256.cache_clear()
authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
from research_automation.control_plane.final_eval_reconciler import reconcile
from research_automation.control_plane.contracts import SideEffect, Actor
from datetime import datetime, timezone

maintenance_actor = stores_module.Actor(
    "p8-reconciler-maintenance", "automation",
    "p8-rec-maint-" + maintenance_id[-8:] + "-f5",
)
maintenance_identity = stores_module.AuthorityIdentity(
    plan_hash="0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
    scope_hash="8c6b4a7547275728c7beef294cd8e5d56fdddf5da82509e09e88162e8c6243be",
    instruction_policy_hash="0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
)
envelope = authority._provision_authorization(
    phase=stores_module.Phase.P0,
    attempt_id="p8-reconciler-maint",
    actor=maintenance_actor,
    identity=maintenance_identity,
    expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
    allowed_side_effects=(SideEffect.READ,),
)
maintenance_grant = authority.claim_authorization(
    envelope,
    expected_phase=stores_module.Phase.P0,
    expected_attempt_id="p8-reconciler-maint",
    actor=maintenance_actor,
    identity=maintenance_identity,
)
ticket = authority._issue_task_ticket(
    maintenance_grant,
    {
        "task_id": "P8-RECONCILER-MAINT",
        "objective": "bounded reconciler maintenance",
        "dependencies": [],
        "idempotency_key": maintenance_id + "-f5",
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
maintenance_lease = authority._begin_task(ticket)
report = reconcile(
    authority,
    maintenance_lease,
    evidence_ref_for={
        binding_id: ("research_state/control_plane/p8/attempts/"
                     "p8-attempt-003/evidence/final_eval_cr010/claims/"
                     + binding_id + ".json")
    },
    repository_root=root,
)
print(json.dumps(report.to_dict(), sort_keys=True))
"""

    _ORCH_REPLAY = """
import sys
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module

root = Path(sys.argv[1])
ROOT_SECRET = sys.argv[2]
binding_id = sys.argv[3]
expected_version = int(sys.argv[4])

patch.multiple(
    stores_module,
    _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
    _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
).start()
stores_module._expected_schema_sha256.cache_clear()
authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
from research_automation.control_plane.final_eval_orchestrator import (
    OrchestrationInputs,
    orchestrate,
)
from research_automation.control_plane.final_eval_evidence import (
    FinalEvalResultPublisher,
)
import subprocess as _sp
if not (root / ".git").exists():
    _sp.run(["git", "init", "--quiet"], cwd=root, check=True,
            capture_output=True)
    _sp.run(["git", "config", "user.name", "Control Plane Tests"],
            cwd=root, check=True, capture_output=True)
    _sp.run(["git", "config", "user.email",
             "control-plane@example.invalid"],
            cwd=root, check=True, capture_output=True)
volume = ("research_state/control_plane/p8/attempts/p8-attempt-003/"
          "evidence/final_eval_cr010")
publisher = FinalEvalResultPublisher(
    repository_root=root,
    evidence_volume=volume,
)

def sink(document):
    refs = publisher.publish(
        binding_id,
        binding_id,
        dict(document),
        outcome=str(document.get("outcome", "SUCCEEDED")),
    )
    return refs.to_payload()

orchestrate(
    OrchestrationInputs(
        authority=authority,
        binding_id=binding_id,
        expected_version=expected_version,
        worker_launcher=lambda: 0,
        evidence_sink=sink,
        repository_root=root,
    )
)
print("REPLAYED")
"""

    def _reconcile_fresh(self, root, binding_id, maintenance_id):
        from tests.test_control_plane_campaign_store import ROOT_SECRET

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                self._RECONCILER,
                str(root),
                ROOT_SECRET,
                binding_id,
                maintenance_id,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        import json as _json

        return _json.loads(result.stdout.strip().splitlines()[-1])

    def _replay_orchestrator_fresh(self, root, binding_id, expected_version):
        from tests.test_control_plane_campaign_store import ROOT_SECRET

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                self._ORCH_REPLAY,
                str(root),
                ROOT_SECRET,
                binding_id,
                str(expected_version),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("REPLAYED", result.stdout)

    def _recovery_lease_count(self, root, binding_id):
        conn = sqlite3.connect(str(root / "authority.sqlite3"))
        try:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM final_eval_recovery_leases_v1 "
                    "WHERE binding_id = ?",
                    (binding_id,),
                ).fetchone()[0]
            )
        finally:
            conn.close()

    def test_fresh_process_reconciler_terminates_every_crash_point(
        self,
    ) -> None:
        """CR-010 F-05: EVERY hard-crash point must be terminable by a
        FRESH process that acquires a maintenance lease and runs the
        reconciler -- with no reopen (intermediate states stay untouched
        until the orchestrator replay), no reissue (exactly one recovery
        lease row) and no open-count regression (the saga version never
        decreases)."""
        campaign_id = "campaign-crash-matrix-f5"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            for index, (crash_point, expected_state, terminal) in enumerate(
                self.CRASH_POINT_CASES
            ):
                with self.subTest(crash_point=crash_point):
                    broker = _make_broker(root, grant)
                    binding = broker.bind(
                        request=_make_request(
                            campaign_id=f"campaign-crash-f5-{index}",
                            holdout_id=f"holdout-crash-f5-{index}",
                            plan_sha256=format(index + 300, "064x"),
                            holdout_sha256=format(index + 400, "064x"),
                            nonce=format(index + 500, "064x"),
                        ),
                        nonce=format(index + 500, "064x"),
                        actor=stores_module.Actor(
                            "operator-1", "human", "final-eval-op-cr009"
                        ),
                        idempotency_key=f"p8-cr009-f5-crash-{index}",
                        task_spec_ref="manifest.json",
                        task_spec_sha256="1" * 64,
                    )
                    maintenance_id = f"p8-cr009-f5-maint-{index}"
                    result = self._run_child(
                        root,
                        crash_point,
                        binding.ticket_id,
                        binding.saga_version,
                        maintenance_id,
                    )
                    self.assertEqual(result.returncode, 9, msg=result.stderr)
                    observed = self._observe(root, binding.ticket_id)
                    self.assertEqual(observed["state"], expected_state)
                    version_after_crash = observed["version"]

                    # 1. fresh-process reconciler pass (no crash hook)
                    report = self._reconcile_fresh(
                        root,
                        binding.ticket_id,
                        maintenance_id + "-pass1",
                    )
                    if crash_point in (
                        "CRASH_AFTER.CONSUMED",
                        "CRASH_AFTER.EVALUATING",
                        "CRASH_AFTER.CLAIM_WRITTEN",
                    ):
                        # no-reopen: the reconciler never advances an
                        # intermediate state; only the orchestrator replay can
                        self.assertIn(
                            binding.ticket_id, report["unresolved"]
                        )
                        # 2. fresh-process orchestrator replay
                        self._replay_orchestrator_fresh(
                            root,
                            binding.ticket_id,
                            version_after_crash,
                        )
                        staged = self._observe(root, binding.ticket_id)
                        self.assertEqual(staged["state"], "RESULT_STAGED")
                        # 3. fresh-process reconciler completes the terminal
                        report = self._reconcile_fresh(
                            root,
                            binding.ticket_id,
                            maintenance_id + "-pass2",
                        )
                        self.assertIn(
                            binding.ticket_id, report["recovered"]
                        )
                    else:
                        # RESULT_STAGED / RECOVERY_LEASE / CLOSED are
                        # reconciler-owned; AUTHORITY_TERMINAL is skipped
                        # (already closed by the crashed child)
                        if crash_point == "CRASH_AFTER.AUTHORITY_TERMINAL":
                            self.assertIn(
                                binding.ticket_id, report["skipped"]
                            )
                        else:
                            self.assertIn(
                                binding.ticket_id, report["recovered"]
                            )

                    final = self._observe(root, binding.ticket_id)
                    self.assertEqual(
                        final["state"], "AUTHORITY_TERMINAL",
                        "every crash point must terminate",
                    )
                    self.assertEqual(final["terminal"], "SUCCEEDED")
                    # no-reopen: the saga version never decreases
                    self.assertGreaterEqual(
                        final["version"], version_after_crash
                    )
                    # no-reissue: exactly one recovery lease row exists
                    self.assertEqual(
                        self._recovery_lease_count(root, binding.ticket_id),
                        1,
                        "recovery leases must never be reissued",
                    )


class FinalEvalFreshProcessRecoveryTests(unittest.TestCase):
    """Recovery from a genuinely fresh subprocess (B-03 hardening)."""

    def test_recovery_in_fresh_subprocess_after_crash(self) -> None:
        campaign_id = "campaign-fresh-recovery"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            from tests.test_control_plane_campaign_store import ROOT_SECRET

            # 1. bind in the parent process
            broker = _make_broker(root, grant)
            binding = broker.bind(
                request=_make_request(
                    campaign_id="campaign-fresh-1",
                    holdout_id="holdout-fresh-1",
                    plan_sha256="1" * 64,
                    holdout_sha256="2" * 64,
                    nonce="3" * 64,
                ),
                nonce="3" * 64,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-fresh-1",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            # 2. hard-exit child after RESULT_STAGED
            child = """
import os
import sys
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.final_eval_orchestrator import (
    OrchestrationInputs,
    orchestrate,
)

root = Path(sys.argv[1])
ROOT_SECRET = sys.argv[2]
binding_id = sys.argv[3]
expected_version = int(sys.argv[4])

patch.multiple(
    stores_module,
    _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
    _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
).start()
stores_module._expected_schema_sha256.cache_clear()
authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)

def crash_hook(state):
    if state == "CRASH_AFTER.RESULT_STAGED":
        os._exit(9)

def worker():
    return 0

def sink(document):
    import subprocess as _sp
    if not (root / ".git").exists():
        _sp.run(["git", "init", "--quiet"], cwd=root, check=True,
                capture_output=True)
        _sp.run(["git", "config", "user.name", "Control Plane Tests"],
                cwd=root, check=True, capture_output=True)
        _sp.run(["git", "config", "user.email",
                 "control-plane@example.invalid"],
                cwd=root, check=True, capture_output=True)
    from research_automation.control_plane.final_eval_evidence import (
        FinalEvalResultPublisher,
    )
    publisher = FinalEvalResultPublisher(
        repository_root=root,
        evidence_volume=("research_state/control_plane/p8/attempts/"
                         "p8-attempt-003/evidence/final_eval_cr010"),
    )
    refs = publisher.publish(
        binding_id, binding_id, dict(document),
        outcome=str(document.get("outcome", "SUCCEEDED")),
    )
    return refs.to_payload()

orchestrate(
    OrchestrationInputs(
        authority=authority,
        binding_id=binding_id,
        expected_version=expected_version,
        worker_launcher=worker,
        evidence_sink=sink,
        crash_hook=crash_hook,
        repository_root=root,
    )
)
"""
            result = subprocess.run(
                [
                    sys.executable, "-c", child,
                    str(root), ROOT_SECRET, binding.ticket_id,
                    str(binding.saga_version),
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            self.assertEqual(result.returncode, 9)  # hard exit at crash point

            # 3. recover in a FRESH subprocess (new env, only disk state)
            recovery_child = """
import sys
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module

root = Path(sys.argv[1])
ROOT_SECRET = sys.argv[2]
binding_id = sys.argv[3]

patch.multiple(
    stores_module,
    _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
    _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
).start()
stores_module._expected_schema_sha256.cache_clear()
authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
snapshot = authority.final_eval_binding_snapshot(binding_id)
print(snapshot.saga_state, snapshot.saga_version)
"""
            recovery = subprocess.run(
                [
                    sys.executable, "-c", recovery_child,
                    str(root), ROOT_SECRET, binding.ticket_id,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            self.assertEqual(recovery.returncode, 0)
            state, version = recovery.stdout.strip().split()
            # The fresh process sees the durable RESULT_STAGED state.
            self.assertEqual(state, "RESULT_STAGED")
            self.assertEqual(int(version), 4)


class FinalEvalOutcomeIntegrityTests(unittest.TestCase):
    """CR-009 overall-review CRITICAL-1: a staged FAILED result must never
    be recovered as SUCCEEDED."""

    def test_failed_staged_result_recovered_as_failed(self) -> None:
        campaign_id = "campaign-outcome-failed"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            broker = _make_broker(root, grant)
            binding = broker.bind(
                request=_make_request(
                    campaign_id="campaign-fail-1",
                    holdout_id="holdout-fail-1",
                    plan_sha256="4" * 64,
                    holdout_sha256="5" * 64,
                    nonce="6" * 64,
                ),
                nonce="6" * 64,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8-cr009-fail-1",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            claim_ref = _claim_ref_for(binding.ticket_id)

            # Worker exits non-zero -> staged outcome FAILED.
            orchestrate(
                OrchestrationInputs(
                    authority=authority,
                    binding_id=binding.ticket_id,
                    expected_version=binding.saga_version,
                    worker_launcher=lambda: 7,
                    evidence_sink=_real_publisher_sink(root, binding.ticket_id),
                    repository_root=root,
                )
            )
            lease = FinalEvalOrchestrationTests()._maintenance_lease(
                root, grant, "p8-cr009-fail-maint"
            )
            report = reconcile(
                authority,
                lease,
                evidence_ref_for={binding.ticket_id: claim_ref},
                repository_root=root,
            )
            self.assertIn(binding.ticket_id, report.recovered)
            snapshot = authority.final_eval_binding_snapshot(binding.ticket_id)
            self.assertEqual(snapshot.saga_state, "AUTHORITY_TERMINAL")
            # The failed evaluation must NOT be recorded as SUCCEEDED.
            self.assertEqual(snapshot.terminal_binding, "FAILED")
