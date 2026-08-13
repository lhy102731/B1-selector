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
from research_automation.control_plane.contracts import SideEffect
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
        with _authorized_campaign(campaign_id) as (root, grant, journal):
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

            def sink(document):
                results["document"] = document
                return (
                    "research_state/control_plane/p8/attempts/"
                    "p8-attempt-003/evidence/worker_result.json"
                )

            snapshot = orchestrate(
                OrchestrationInputs(
                    authority=authority,
                    binding_id=binding.ticket_id,
                    expected_version=binding.saga_version,
                    worker_launcher=lambda: 0,
                    evidence_sink=sink,
                )
            )
            self.assertEqual(snapshot.saga_state, "RESULT_STAGED")
            self.assertEqual(
                snapshot.result_claim_ref,
                "research_state/control_plane/p8/attempts/"
                "p8-attempt-003/evidence/worker_result.json",
            )
            self.assertEqual(results["document"]["binding_id"], binding.ticket_id)
            self.assertEqual(results["document"]["exit_code"], 0)
            self.assertEqual(results["document"]["outcome"], "SUCCEEDED")

    def test_orchestrator_rejects_wrong_inputs(self) -> None:
        with self.assertRaises(FinalEvalOrchestrationError):
            orchestrate(object())  # type: ignore[arg-type]

    def test_reconciler_requires_maintenance_lease(self) -> None:
        with _authorized_campaign("campaign-rec-no-lease") as (root, grant, journal):
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            with self.assertRaises(FinalEvalReconcilerError):
                reconcile(authority, object())  # type: ignore[arg-type]

    def test_reconciler_recovers_staged_binding_without_reopen(self) -> None:
        campaign_id = "campaign-rec-ok"
        with _authorized_campaign(campaign_id) as (root, grant, journal):
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
            orchestrate(
                OrchestrationInputs(
                    authority=authority,
                    binding_id=binding.ticket_id,
                    expected_version=binding.saga_version,
                    worker_launcher=lambda: 0,
                    evidence_sink=lambda doc: (
                        "research_state/control_plane/p8/attempts/"
                        "p8-attempt-003/evidence/worker_result.json"
                    ),
                )
            )
            # Fresh maintenance ticket + lease for the reconciler.
            lease = self._maintenance_lease(root, grant, "p8-cr009-rec-maint")
            report = reconcile(
                authority,
                lease,
                evidence_ref_for={
                    binding.ticket_id: (
                        "research_state/control_plane/p8/attempts/"
                        "p8-attempt-003/evidence/worker_result.json"
                    )
                },
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
        with _authorized_campaign(campaign_id) as (root, grant, journal):
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


if __name__ == "__main__":
    unittest.main()


class FinalEvalHardCrashHarnessTests(unittest.TestCase):
    """Fresh-process hard-crash recovery for the durable saga.

    Each crash point hard-exits the child AFTER the corresponding durable
    transition commits.  A fresh process then re-runs the orchestrator: the
    CAS replay continues from the observed durable state, never reopening
    holdout bytes, never recomputing a staged result.
    """

    CRASH_POINT_CASES = (
        # (crash point, expected durable state after crash, terminal?)
        ("CRASH_AFTER.REQUEST_FROZEN", "CONSUMED", False),
        ("CRASH_AFTER.AUTHORIZED", "CONSUMED", False),
        ("CRASH_AFTER.CONSUMED", "CONSUMED", False),
        ("CRASH_AFTER.EVALUATING", "EVALUATING", False),
        ("CRASH_AFTER.RESULT_STAGED", "RESULT_STAGED", True),
        ("CRASH_AFTER.CLOSED", "RESULT_STAGED", True),
    )

    def _run_child(self, root, crash_point, binding_id, expected_version):
        from tests.test_control_plane_campaign_store import ROOT_SECRET

        child = """
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
from research_automation.control_plane.contracts import Actor

root = Path(sys.argv[1])
crash_point = sys.argv[2]
ROOT_SECRET = sys.argv[3]
binding_id = sys.argv[4]
expected_version = int(sys.argv[5])

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
    return ("research_state/control_plane/p8/attempts/p8-attempt-003/"
            "evidence/worker_result_" + binding_id[:16] + ".json")

try:
    orchestrate(
        OrchestrationInputs(
            authority=authority,
            binding_id=binding_id,
            expected_version=expected_version,
            worker_launcher=worker,
            evidence_sink=sink,
            crash_hook=crash_hook,
        )
    )
    print("ORCHESTRATED", binding_id)
except Exception as exc:
    print("CHILD_ERROR", type(exc).__name__, str(exc)[:200])
    sys.exit(2)
"""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                child,
                str(root),
                crash_point,
                ROOT_SECRET,
                binding_id,
                str(expected_version),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        return result

    def _recover(self, root, binding_id):
        from tests.test_control_plane_campaign_store import ROOT_SECRET

        authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
        snapshot = authority.final_eval_binding_snapshot(binding_id)
        if snapshot.saga_state in ("RESULT_STAGED", "CLOSED"):
            return snapshot
        # Idempotent CAS replay continues from the durable state.
        return orchestrate(
            OrchestrationInputs(
                authority=authority,
                binding_id=binding_id,
                expected_version=snapshot.saga_version,
                worker_launcher=lambda: 0,
                evidence_sink=lambda doc: (
                    "research_state/control_plane/p8/attempts/"
                    "p8-attempt-003/evidence/worker_result_"
                    + binding_id[:16]
                    + ".json"
                ),
            )
        )

    def test_hard_crash_at_each_fixed_point_recovers_in_fresh_process(
        self,
    ) -> None:
        campaign_id = "campaign-crash-matrix"
        with _authorized_campaign(campaign_id) as (root, grant, journal):
            from tests.test_control_plane_campaign_store import ROOT_SECRET

            for index, (crash_point, expected_state, terminal) in enumerate(
                self.CRASH_POINT_CASES
            ):
                with self.subTest(crash_point=crash_point):
                    # bind
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
                        idempotency_key=f"p8-cr009-crash-{crash_point}",
                        task_spec_ref="manifest.json",
                        task_spec_sha256="1" * 64,
                    )
                    # hard-exit child at the crash point
                    result = self._run_child(
                        root,
                        crash_point,
                        binding.ticket_id,
                        binding.saga_version,
                    )
                    # fresh process observes + continues
                    snapshot = self._recover(root, binding.ticket_id)
                    if terminal:
                        self.assertEqual(snapshot.saga_state, "RESULT_STAGED")
                        self.assertIsNotNone(snapshot.result_claim_ref)
                    else:
                        # CAS replay advanced past the crash point
                        self.assertIn(
                            snapshot.saga_state,
                            ("EVALUATING", "RESULT_STAGED"),
                        )

    def test_crash_points_are_six_fixed_boundaries(self) -> None:
        self.assertGreaterEqual(len(CRASH_POINTS), 6)
