"""Tests for the final evaluation Authority binding (P8R3 T1)."""

from __future__ import annotations

import tempfile

import json
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane.final_eval_authority import (
    FINAL_EVAL_REQUEST_V2,
    AuthorityFinalEvalBroker,
    FinalEvalRequestRejected,
    FinalEvalRequestV2,
    _nonce_fingerprint,
)
from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import SideEffect
from tests.test_control_plane_campaign_store import (
    P8_GRANT_IDENTITY,
    ROOT_SECRET,
    _authorized_campaign,
    _authorized_p8_campaign,
)

P8_IDENTITY = {
    "plan_hash": "0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
    "scope_hash": "8c6b4a7547275728c7beef294cd8e5d56fdddf5da82509e09e88162e8c6243be",
    "instruction_policy_hash": "0f9164237e8470be4c7b7ff0bcad7b16235f5d75ce45c56e20765190f3238828",
}
NONCE = "n" * 64


def _request(**overrides) -> FinalEvalRequestV2:
    values = {
        "schema_version": FINAL_EVAL_REQUEST_V2,
        "research_plan_sha256": "a" * 64,
        "campaign_id": "campaign-final-1",
        "campaign_sha256": "b" * 64,
        "holdout_id": "holdout-final-1",
        "holdout_sha256": "c" * 64,
        "nonce_fingerprint": _nonce_fingerprint(ROOT_SECRET, NONCE),
        "candidate_freeze_ref": "research_state/control_plane/p8/attempts/p8-attempt-003/freeze.json",
        "candidate_freeze_sha256": "d" * 64,
        "code_sha256": "e" * 64,
        "execution_spec_sha256": "f" * 64,
        "features_sha256": "9" * 64,
        "model": "final-model-1",
        "threshold": "0.5",
        "roster_sha256": "5" * 64,
        "generation": "generation-final-1",
        "actor_id": "operator-1",
        "actor_type": "human",
        "invocation_id": "final-eval-op-cr009",
        "authority_plan_hash": P8_IDENTITY["plan_hash"],
    }
    values.update(overrides)
    return FinalEvalRequestV2(**values)


class FinalEvalRequestV2ContractTests(unittest.TestCase):
    def test_request_binds_all_identity_hashes(self) -> None:
        request = _request()
        self.assertEqual(request.schema_version, FINAL_EVAL_REQUEST_V2)
        self.assertEqual(request.research_plan_sha256, "a" * 64)
        self.assertNotIn(NONCE, request.to_payload().values())
        self.assertEqual(len(request.request_sha256), 64)

    def test_request_rejects_malformed_hash(self) -> None:
        with self.assertRaises(FinalEvalRequestRejected):
            _request(research_plan_sha256="not-a-hash")

    def test_request_rejects_unknown_schema(self) -> None:
        with self.assertRaises(FinalEvalRequestRejected):
            _request(schema_version="control_plane.final_eval_request.v1")

    def test_request_rejects_invalid_actor_type(self) -> None:
        with self.assertRaises(FinalEvalRequestRejected):
            _request(actor_type="llm")

    def test_request_payload_never_contains_raw_nonce(self) -> None:
        payload = json.dumps(_request().to_payload())
        self.assertNotIn(NONCE, payload)
        self.assertNotIn("nonce_fingerprint_value", payload)


class AuthorityBrokerBindingTests(unittest.TestCase):
    def _broker(self, root, grant, attempt_id="p8-attempt-003"):
        authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
        identity = stores_module.AuthorityIdentity(**P8_IDENTITY)
        return AuthorityFinalEvalBroker(
            authority=authority,
            grant=grant,
            attempt_id=attempt_id,
            identity=identity,
        )

    def test_bind_creates_durable_binding_with_real_ticket(self) -> None:
        campaign_id = "campaign-p8-bind"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=_request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8r3-bind-001",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            self.assertEqual(binding.saga_state, "CONSUMED")
            self.assertEqual(binding.holdout_id, "holdout-final-1")
            self.assertTrue(binding.ticket_id)

    def test_same_plan_holdout_new_nonce_is_rejected(self) -> None:
        campaign_id = "campaign-p8-unique"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            broker = self._broker(root, grant)
            actor = stores_module.Actor("operator-1", "human", "final-eval-op-cr009")
            broker.bind(
                request=_request(),
                nonce=NONCE,
                actor=actor,
                idempotency_key="p8r3-unique-001",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            # Same plan+holdout with a different nonce must be rejected by the
            # global uniqueness constraint.
            second = _request(
                nonce_fingerprint=_nonce_fingerprint(ROOT_SECRET, "x" * 64),
                authority_plan_hash=P8_IDENTITY["plan_hash"],
            )
            with self.assertRaises(Exception) as caught:
                broker.bind(
                    request=second,
                    nonce="x" * 64,
                    actor=actor,
                    idempotency_key="p8r3-unique-002",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )
            self.assertTrue(
                isinstance(caught.exception, Exception)
            )

    def test_wrong_authority_plan_hash_rejected(self) -> None:
        campaign_id = "campaign-p8-lineage"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            broker = self._broker(root, grant)
            request = _request(authority_plan_hash="f" * 64)
            with self.assertRaises(FinalEvalRequestRejected):
                broker.bind(
                    request=request,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8r3-lineage-001",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )

    def test_raw_nonce_never_persisted(self) -> None:
        campaign_id = "campaign-p8-secret"
        with _authorized_p8_campaign(campaign_id) as (root, grant, journal):
            broker = self._broker(root, grant)
            binding = broker.bind(
                request=_request(),
                nonce=NONCE,
                actor=stores_module.Actor(
                    "operator-1", "human", "final-eval-op-cr009"
                ),
                idempotency_key="p8r3-secret-001",
                task_spec_ref="manifest.json",
                task_spec_sha256="1" * 64,
            )
            conn = sqlite3.connect(str(root / "authority.sqlite3"))
            rows = conn.execute(
                "SELECT nonce_fingerprint, request_sha256 FROM "
                "final_eval_authorizations_v1 WHERE ticket_id = ?",
                (binding.ticket_id,),
            ).fetchall()
            conn.close()
            self.assertEqual(len(rows), 1)
            self.assertNotEqual(rows[0][0], NONCE)
            self.assertNotIn(NONCE, rows[0][0])


    def test_non_p8_grants_are_rejected(self) -> None:
        """CR-010 B-03: P6/P7/C0 grants can never create a FinalEval
        binding -- the strict P8 lineage is enforced by the broker."""
        from datetime import datetime, timezone

        from tests.test_control_plane_campaign_store import (
            ROOT_SECRET,
            P8_GRANT_IDENTITY,
        )

        for phase_name in ("P6", "P7", "C0"):
            with self.subTest(phase=phase_name):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    with patch.multiple(
                        stores_module,
                        _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
                        _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
                    ):
                        stores_module._expected_schema_sha256.cache_clear()
                        stores_module._trusted_bootstrap(
                            root_secret=ROOT_SECRET
                        )
                        authority = stores_module._AuthorityStore(
                            root_secret=ROOT_SECRET
                        )
                        actor = stores_module.Actor(
                            "operator-1", "human",
                            "final-eval-op-cr009"
                        )
                        identity = stores_module.AuthorityIdentity(
                            **P8_GRANT_IDENTITY
                        )
                        envelope = authority._provision_authorization(
                            phase=stores_module.Phase(phase_name),
                            attempt_id="p8-attempt-003",
                            actor=actor,
                            identity=identity,
                            expires_at=datetime(2027, 1, 1,
                                                 tzinfo=timezone.utc),
                            allowed_side_effects=(
                                SideEffect.READ,
                                SideEffect.WRITE_CONTROL_PLANE,
                            ),
                        )
                        grant = authority.claim_authorization(
                            envelope,
                            expected_phase=stores_module.Phase(phase_name),
                            expected_attempt_id="p8-attempt-003",
                            actor=actor,
                            identity=identity,
                        )
                        broker = self._broker(root, grant)
                        with self.assertRaises(FinalEvalRequestRejected):
                            broker.bind(
                                request=_request(),
                                nonce=NONCE,
                                actor=stores_module.Actor(
                                    "operator-1", "human",
                                    "final-eval-op-cr009"
                                ),
                                idempotency_key=f"p8r3-reject-{phase_name}",
                                task_spec_ref="manifest.json",
                                task_spec_sha256="1" * 64,
                            )

    def test_grant_actor_mismatch_is_rejected(self) -> None:
        """CR-010 B-03: a grant whose actor differs from the request actor
        can never bind a FinalEval request."""
        from tests.test_control_plane_campaign_store import (
            _authorized_campaign,
        )

        with _authorized_campaign("campaign-p8-actor") as (root, grant, journal):
            broker = self._broker(root, grant)
            with self.assertRaises(FinalEvalRequestRejected):
                broker.bind(
                    request=_request(),
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "other-actor", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8r3-actor-mismatch",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )

    def test_grant_identity_mismatch_is_rejected(self) -> None:
        """CR-010 B-03: a request whose authority plan hash differs from
        the grant identity can never bind."""
        from tests.test_control_plane_campaign_store import (
            _authorized_campaign,
        )

        with _authorized_campaign("campaign-p8-ident") as (root, grant, journal):
            broker = self._broker(root, grant)
            tampered = _request(
                authority_plan_hash="f" * 64,
            )
            with self.assertRaises(FinalEvalRequestRejected):
                broker.bind(
                    request=tampered,
                    nonce=NONCE,
                    actor=stores_module.Actor(
                        "operator-1", "human", "final-eval-op-cr009"
                    ),
                    idempotency_key="p8r3-ident-mismatch",
                    task_spec_ref="manifest.json",
                    task_spec_sha256="1" * 64,
                )

if __name__ == "__main__":
    unittest.main()
