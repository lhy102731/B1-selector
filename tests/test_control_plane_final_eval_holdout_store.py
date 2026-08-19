"""CR-010 C0 (Phase B): Authority-backed durable Holdout consume tests.

The begin-time consume receipt is ONE immutable row in the SAME Authority
database/transaction as the ticket/binding/outbox rows.  These tests
prove: reopen-after-exit retention, one-winner concurrency, no second
consume/count growth, tamper rejection on every identity field, outcome
immutability, transaction atomicity and the raw-nonce-free surface.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import unittest

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.final_eval_authority import (
    AuthorityFinalEvalBroker,
    FinalEvalBindingV2,
    FinalEvalRequestV2,
)
from research_automation.control_plane.final_eval_holdout_store import (
    FinalEvalConsumptionRejected,
    SqliteHoldoutStore,
    consumption_receipt_sha256,
    verify_consumption_receipt,
)
from research_automation.control_plane.final_evaluator import (
    FinalEvalRequest,
)
from tests.test_control_plane_campaign_store import (
    _authorized_p8_campaign,
    ROOT_SECRET,
)
from tests.test_control_plane_final_eval_orchestrator import (
    NONCE,
    _make_broker,
    _make_request,
)

CONSUMPTION_FIELDS = (
    "ticket_id",
    "request_sha256",
    "nonce_fingerprint",
    "holdout_id",
    "holdout_sha256",
    "attempt_id",
    "actor_id",
    "actor_type",
    "invocation_id",
    "consumed_at_utc",
    "receipt_sha256",
)


def _actor() -> stores_module.Actor:
    return stores_module.Actor(
        "operator-1", "human", "final-eval-op-cr009"
    )


def _durable_request_sha256(request: FinalEvalRequestV2) -> str:
    """The stores-domain request digest stored in the consumption row."""
    from tests.test_control_plane_final_eval_orchestrator import P8_IDENTITY

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
        nonce_fingerprint=stores_module._final_eval_nonce_fingerprint(
            ROOT_SECRET, NONCE
        ),
        task_spec_ref="manifest.json",
        task_spec_sha256="1" * 64,
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


class FinalEvalHoldoutStoreTests(unittest.TestCase):
    def _bind(self, root, grant, request=None, idempotency_key="p8-holdout-store"):
        broker = _make_broker(root, grant)
        return broker.bind(
            request=request or _make_request(
                campaign_id="campaign-holdout-store"
            ),
            nonce=NONCE,
            actor=_actor(),
            idempotency_key=idempotency_key,
            task_spec_ref="manifest.json",
            task_spec_sha256="1" * 64,
        )

    def _store(self) -> SqliteHoldoutStore:
        return SqliteHoldoutStore(
            authority=stores_module._AuthorityStore(root_secret=ROOT_SECRET)
        )

    def test_reopen_after_process_exit_retains_receipt(self) -> None:
        """The begin-time consume receipt survives a fresh process-like
        reopen: a brand-new store instance reads the SAME committed row."""
        with _authorized_p8_campaign("campaign-holdout-reopen") as (
            root,
            grant,
            journal,
        ):
            request = _make_request(campaign_id="campaign-holdout-reopen")
            binding = self._bind(
                root,
                grant,
                request=request,
                idempotency_key="p8-holdout-reopen",
            )
            consumption = binding.consumption
            self.assertIsNotNone(consumption)
            self.assertEqual(consumption.ticket_id, binding.ticket_id)
            self.assertEqual(
                consumption.request_sha256,
                _durable_request_sha256(request),
            )
            self.assertEqual(
                consumption.nonce_fingerprint, binding.nonce_fingerprint
            )
            self.assertEqual(consumption.attempt_id, grant.attempt_id)
            self.assertEqual(
                consumption.holdout_id, binding.holdout_id
            )
            # fresh store instance == fresh process state
            reread = self._store().read_consumption(binding.ticket_id)
            self.assertEqual(reread.receipt_sha256, consumption.receipt_sha256)
            self.assertEqual(reread.to_payload(), consumption.to_payload())
            # replay reads the receipt WITHOUT any count growth
            self.assertEqual(
                self._store().consumption_count(
                    _durable_request_sha256(request)
                ),
                1,
            )

    def test_concurrent_bind_produces_one_winner(self) -> None:
        with _authorized_p8_campaign("campaign-holdout-race") as (
            root,
            grant,
            journal,
        ):
            request = _make_request(campaign_id="campaign-holdout-race")
            outcomes: list[str] = []
            barrier = threading.Barrier(2)

            def attempt() -> None:
                try:
                    barrier.wait(timeout=10)
                    self._bind(
                        root,
                        grant,
                        request=request,
                        idempotency_key="p8-holdout-race",
                    )
                    outcomes.append("won")
                except Exception as error:  # noqa: BLE001
                    outcomes.append(type(error).__name__)

            threads = [
                threading.Thread(target=attempt) for _ in range(2)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)
            self.assertEqual(sorted(outcomes), ["FinalEvalBindingConflictError", "won"])
            self.assertEqual(
                self._store().consumption_count(
                    _durable_request_sha256(request)
                ),
                1,
            )

    def test_second_consume_rejected_count_unchanged(self) -> None:
        with _authorized_p8_campaign("campaign-holdout-dup") as (
            root,
            grant,
            journal,
        ):
            request = _make_request(campaign_id="campaign-holdout-dup")
            first = self._bind(
                root, grant, request=request, idempotency_key="p8-holdout-dup"
            )
            with self.assertRaises(stores_module.FinalEvalBindingConflictError):
                self._bind(
                    root, grant, request=request,
                    idempotency_key="p8-holdout-dup",
                )
            self.assertEqual(
                self._store().consumption_count(
                    _durable_request_sha256(request)
                ),
                1,
            )
            # the winner's receipt is still intact and readable
            reread = self._store().read_consumption(first.ticket_id)
            self.assertEqual(
                reread.receipt_sha256, first.consumption.receipt_sha256
            )

    def test_changed_identity_field_rejected(self) -> None:
        """Any changed ticket/request digest/nonce fingerprint/holdout
        digest/actor/attempt rejects on read (receipt hash recompute)."""
        with _authorized_p8_campaign("campaign-holdout-tamper") as (
            root,
            grant,
            journal,
        ):
            binding = self._bind(
                root, grant, idempotency_key="p8-holdout-tamper"
            )
            ticket_id = binding.ticket_id
            authority_db = root / "authority.sqlite3"
            connection = sqlite3.connect(str(authority_db))
            try:
                row = connection.execute(
                    "SELECT * FROM final_eval_holdout_consumptions_v1 "
                    "WHERE ticket_id = ?",
                    (ticket_id,),
                ).fetchone()
                original = {
                    CONSUMPTION_FIELDS[i]: row[i]
                    for i in range(len(CONSUMPTION_FIELDS))
                }
            finally:
                connection.close()
            mutations = {
                "ticket_id": "t" * 64,
                "request_sha256": "1" * 64,
                "nonce_fingerprint": "2" * 64,
                "holdout_id": "other-holdout",
                "holdout_sha256": "3" * 64,
                "attempt_id": "other-attempt",
                "actor_id": "other-actor",
                "actor_type": "automation",
                "invocation_id": "other-invocation",
                "consumed_at_utc": "2030-01-01T00:00:00Z",
            }
            for field_name, value in mutations.items():
                with self.subTest(field=field_name):
                    connection = sqlite3.connect(str(authority_db))
                    try:
                        connection.execute(
                            f"UPDATE final_eval_holdout_consumptions_v1 "
                            f"SET {field_name} = ? WHERE ticket_id = ?",
                            (value, ticket_id),
                        )
                        connection.commit()
                    finally:
                        connection.close()
                    # a changed ticket id moves the row; the receipt must
                    # still be rejected under EITHER key
                    lookup = value if field_name == "ticket_id" else ticket_id
                    with self.assertRaises(FinalEvalConsumptionRejected):
                        self._store().read_consumption(lookup)
                    # restore the field (ticket_id is the row key)
                    connection = sqlite3.connect(str(authority_db))
                    try:
                        where = value if field_name == "ticket_id" else ticket_id
                        connection.execute(
                            f"UPDATE final_eval_holdout_consumptions_v1 "
                            f"SET {field_name} = ? WHERE ticket_id = ?",
                            (original[field_name], where),
                        )
                        connection.commit()
                    finally:
                        connection.close()
            # fully restored -> readable again with the ORIGINAL hash
            reread = self._store().read_consumption(ticket_id)
            self.assertEqual(
                reread.receipt_sha256, binding.consumption.receipt_sha256
            )

    def test_worker_outcome_cannot_mutate_immutable_receipt(self) -> None:
        """Advancing the saga (EVALUATING -> RESULT_STAGED) must leave the
        begin-time consume receipt byte-identical."""
        with _authorized_p8_campaign("campaign-holdout-outcome") as (
            root,
            grant,
            journal,
        ):
            binding = self._bind(
                root, grant, idempotency_key="p8-holdout-outcome"
            )
            original_receipt = binding.consumption.receipt_sha256
            # stage a REAL worker result through the durable orchestrator
            from research_automation.control_plane.final_eval_orchestrator import (
                OrchestrationInputs,
                orchestrate,
            )
            from tests.test_control_plane_final_eval_orchestrator import (
                _ensure_git,
                _real_publisher_sink,
            )

            _ensure_git(root)
            authority = stores_module._AuthorityStore(root_secret=ROOT_SECRET)
            staged = orchestrate(
                OrchestrationInputs(
                    authority=authority,
                    binding_id=binding.ticket_id,
                    expected_version=binding.saga_version,
                    worker_launcher=lambda: 0,
                    evidence_sink=_real_publisher_sink(root, binding.ticket_id),
                    repository_root=root,
                )
            )
            self.assertIsNotNone(staged.result_claim_ref)
            reread = self._store().read_consumption(binding.ticket_id)
            self.assertEqual(reread.receipt_sha256, original_receipt)
            # the terminal outcome lives in the BINDING, never the receipt
            self.assertNotIn("SUCCEEDED", reread.to_payload().values())

    def test_transaction_fault_leaves_no_partial_rows(self) -> None:
        """A rollback of the begin transaction leaves NEITHER a receipt
        without its binding NOR a binding without its receipt."""
        with _authorized_p8_campaign("campaign-holdout-fault") as (
            root,
            grant,
            journal,
        ):
            request = _make_request(campaign_id="campaign-holdout-fault")
            durable_digest = _durable_request_sha256(request)
            # simulate a fault AFTER the receipt insert, BEFORE commit
            authority_db = root / "authority.sqlite3"
            connection = sqlite3.connect(str(authority_db))
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO final_eval_holdout_consumptions_v1 "
                    "(ticket_id, request_sha256, nonce_fingerprint, "
                    "holdout_id, holdout_sha256, attempt_id, actor_id, "
                    "actor_type, invocation_id, consumed_at_utc, "
                    "receipt_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "fault-ticket",
                        durable_digest,
                        "f" * 64,
                        request.holdout_id,
                        request.holdout_sha256,
                        grant.attempt_id,
                        "operator-1",
                        "human",
                        "final-eval-op-cr009",
                        "2026-08-16T00:00:00Z",
                        "0" * 64,
                    ),
                )
                connection.execute("ROLLBACK")
            finally:
                connection.close()
            self.assertEqual(
                self._store().consumption_count(durable_digest), 0
            )
            # a real bind commits BOTH the binding and the receipt
            binding = self._bind(
                root, grant, request=request,
                idempotency_key="p8-holdout-fault",
            )
            self.assertIsNotNone(binding.consumption)
            self.assertEqual(
                self._store().consumption_count(durable_digest), 1
            )

    def test_raw_nonce_never_appears_in_rows_json_or_outbox(self) -> None:
        with _authorized_p8_campaign("campaign-holdout-nonce") as (
            root,
            grant,
            journal,
        ):
            binding = self._bind(
                root, grant, idempotency_key="p8-holdout-nonce"
            )
            self.assertNotIn(NONCE, json.dumps(binding.consumption.to_payload()))
            self.assertNotIn(NONCE, json.dumps(binding.to_payload()))
            authority_db = root / "authority.sqlite3"
            connection = sqlite3.connect(str(authority_db))
            try:
                tables = [
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table' "
                        "AND name NOT LIKE 'sqlite_%'"
                    )
                ]
                for table in tables:
                    for row in connection.execute(
                        f"SELECT * FROM {table}"
                    ):
                        self.assertNotIn(
                            NONCE,
                            str(tuple(row)),
                            f"raw nonce leaked into table {table}",
                        )
            finally:
                connection.close()

    def test_receipt_hash_is_independently_recomputable(self) -> None:
        with _authorized_p8_campaign("campaign-holdout-recompute") as (
            root,
            grant,
            journal,
        ):
            binding = self._bind(
                root, grant, idempotency_key="p8-holdout-recompute"
            )
            consumption = binding.consumption
            expected = consumption_receipt_sha256(
                ticket_id=consumption.ticket_id,
                request_sha256=consumption.request_sha256,
                nonce_fingerprint=consumption.nonce_fingerprint,
                holdout_id=consumption.holdout_id,
                holdout_sha256=consumption.holdout_sha256,
                attempt_id=consumption.attempt_id,
                actor_id=consumption.actor_id,
                actor_type=consumption.actor_type,
                invocation_id=consumption.invocation_id,
                consumed_at_utc=consumption.consumed_at_utc,
            )
            self.assertEqual(expected, consumption.receipt_sha256)
            verify_consumption_receipt(
                stores_module.FinalEvalHoldoutConsumption(
                    ticket_id=consumption.ticket_id,
                    request_sha256=consumption.request_sha256,
                    nonce_fingerprint=consumption.nonce_fingerprint,
                    holdout_id=consumption.holdout_id,
                    holdout_sha256=consumption.holdout_sha256,
                    attempt_id=consumption.attempt_id,
                    actor_id=consumption.actor_id,
                    actor_type=consumption.actor_type,
                    invocation_id=consumption.invocation_id,
                    consumed_at_utc=consumption.consumed_at_utc,
                    receipt_sha256=consumption.receipt_sha256,
                )
            )


if __name__ == "__main__":
    unittest.main()
