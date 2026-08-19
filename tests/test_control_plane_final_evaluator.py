"""Focused TDD tests for P8R2 T2/T3/T4 final evaluator control plane."""
from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from research_automation.control_plane.campaign_roster import RosterManifest
from research_automation.control_plane.contracts import (
    Actor,
    IdentityBinding,
    SideEffect,
    canonical_json,
    canonical_sha256,
)
from research_automation.control_plane.final_evaluator import (
    AuthorityBroker,
    CampaignClosedError,
    CampaignClosureBackend,
    CampaignClosureConflictError,
    CampaignClosureReceipt,
    CampaignClosureValidationError,
    CampaignBinding,
    CandidateBinding,
    CodeBinding,
    ConsumeOnceError,
    ConsumeOnceReplayError,
    ConsumeOnceValidationError,
    EvaluatorResult,
    ExecutionSpecBinding,
    FINAL_HOLDOUT_TAINT,
    FeatureBinding,
    FinalEvalActorError,
    FinalEvalBindingError,
    FinalEvalRequest,
    FinalEvalRequestError,
    GenerationBinding,
    HoldoutAlreadyConsumedError,
    HoldoutBinding,
    HoldoutConsumed,
    HoldoutDataBackend,
    HoldoutHandle,
    HoldoutLease,
    HoldoutMetric,
    HoldoutView,
    InMemoryCampaignClosureBackend,
    InMemoryHoldoutStore,
    ModelBinding,
    OPEN_HOLDOUT_EFFECT,
    PromptAccessDeniedError,
    RosterBinding,
    ThresholdBinding,
    TrustedEvaluator,
    TrustedEvaluatorAdapter,
    TrustedEvaluatorBoundaryError,
    TrustedEvaluatorDataRoot,
    TrustedEvaluatorError,
    TrustedEvaluatorLeaseError,
    TrustedEvaluatorPathError,
    TerminalAuditClosure,
    TerminalAuditEvent,
    UnfrozenCandidateError,
    UnboundedResultError,
    build_closure_audit_record,
    deny_open_holdout_effect,
    require_evaluator_spec_holdout_free,
    seal_trusted_data_root,
)
from research_automation.foundations.protocols import compile_execution_spec
from tests.test_control_plane_campaign_freeze import _protocol_member
from tests.test_foundations_protocols import _approval, _protocol


_SHA_RE = r"^[0-9a-f]{64}$"


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _execution_spec():
    protocol = _protocol()
    return compile_execution_spec(
        protocol,
        approved_protocol=protocol,
        approval=_approval(protocol),
        amendment=None,
    )


def _roster() -> RosterManifest:
    member = _protocol_member()
    return RosterManifest(
        cycle_id="cycle-final-1",
        members=(member,),
        manifest_sha256=canonical_sha256(
            {
                "cycle_id": "cycle-final-1",
                "members": (member.to_payload(),),
            }
        ),
    )


def _actor(actor_type: str = "human") -> Actor:
    return Actor(
        actor_id="operator-1",
        actor_type=actor_type,
        invocation_id="final-eval-op-1",
    )


def _identity() -> IdentityBinding:
    return IdentityBinding(
        plan_hash=_sha("plan"),
        scope_hash=_sha("scope"),
        policy_hash=_sha("policy"),
    )


def _candidate(candidate_id: str, *, frozen: bool = True) -> CandidateBinding:
    return CandidateBinding(
        candidate_id=candidate_id,
        candidate_sha256=_sha(candidate_id),
        frozen=frozen,
    )


def _candidate_set_digest(candidates: tuple) -> str:
    return canonical_sha256(
        tuple((candidate.candidate_id, candidate.candidate_sha256) for candidate in candidates)
    )


def _request(**overrides) -> FinalEvalRequest:
    candidates = (_candidate("candidate-a"), _candidate("candidate-b"))
    execution_spec = _execution_spec()
    roster = _roster()
    payload = dict(
        campaign=CampaignBinding(
            campaign_id="campaign-final-1",
            campaign_sha256=_sha("campaign"),
        ),
        candidate_set=candidates,
        candidate_set_sha256=_candidate_set_digest(candidates),
        code=CodeBinding(code_sha256=_sha("code")),
        execution_spec=ExecutionSpecBinding(
            execution_spec=execution_spec,
            execution_spec_sha256=canonical_sha256(execution_spec.model_dump(mode="json")),
        ),
        features=FeatureBinding(features_sha256=_sha("features")),
        model=ModelBinding(model_id="model-final-1", model_sha256=_sha("model")),
        threshold=ThresholdBinding(threshold_sha256=_sha("threshold")),
        roster=RosterBinding(roster=roster, roster_sha256=roster.manifest_sha256),
        generation=GenerationBinding(
            generation_id="generation-final-1",
            generation_sha256=_sha("generation"),
        ),
        holdout=HoldoutBinding(
            holdout_id="holdout-final-1",
            holdout_sha256=_sha("holdout"),
            authorization_nonce=_sha("nonce"),
        ),
        actor=_actor(),
        identity_binding=_identity(),
    )
    payload.update(overrides)
    return FinalEvalRequest(**payload)


class FinalEvalRequestContractTests(unittest.TestCase):
    def test_valid_request_binds_all_identity_hashes(self) -> None:
        request = _request()
        payload = request.to_payload()
        self.assertEqual(payload["schema_version"], "control_plane.final_eval_request.v1")
        for key in (
            "candidate_set_sha256",
            "code_sha256",
            "execution_spec_sha256",
            "features_sha256",
            "threshold_sha256",
            "roster_sha256",
        ):
            self.assertRegex(payload[key], _SHA_RE, key)
        self.assertRegex(payload["campaign"]["campaign_sha256"], _SHA_RE)
        self.assertRegex(payload["model"]["model_sha256"], _SHA_RE)
        self.assertRegex(payload["generation"]["generation_sha256"], _SHA_RE)
        self.assertRegex(payload["holdout"]["holdout_sha256"], _SHA_RE)
        self.assertRegex(payload["holdout"]["authorization_nonce"], _SHA_RE)
        self.assertRegex(request.request_sha256, _SHA_RE)
        identity = payload["identity_binding"]
        for key in ("plan_hash", "scope_hash", "policy_hash"):
            self.assertRegex(identity[key], _SHA_RE, key)
        actor = payload["actor"]
        self.assertEqual(actor["actor_id"], "operator-1")
        self.assertEqual(actor["actor_type"], "human")
        expected = {key: value for key, value in payload.items() if key != "request_sha256"}
        self.assertEqual(request.request_sha256, canonical_sha256(expected))

    def test_request_identity_is_deterministic(self) -> None:
        self.assertEqual(_request().request_sha256, _request().request_sha256)

    def test_unfrozen_candidate_rejected(self) -> None:
        candidates = (_candidate("candidate-a", frozen=False), _candidate("candidate-b"))
        with self.assertRaises(UnfrozenCandidateError):
            _request(
                candidate_set=candidates,
                candidate_set_sha256=_candidate_set_digest(candidates),
            )

    def test_empty_candidate_set_rejected(self) -> None:
        with self.assertRaises(FinalEvalBindingError):
            _request(candidate_set=(), candidate_set_sha256=_candidate_set_digest(()))

    def test_duplicate_candidate_ids_rejected(self) -> None:
        candidates = (_candidate("candidate-a"), _candidate("candidate-a"))
        with self.assertRaises(FinalEvalBindingError):
            _request(
                candidate_set=candidates,
                candidate_set_sha256=_candidate_set_digest(candidates),
            )

    def test_unsorted_candidate_set_rejected(self) -> None:
        candidates = (_candidate("candidate-b"), _candidate("candidate-a"))
        with self.assertRaises(FinalEvalBindingError):
            _request(
                candidate_set=candidates,
                candidate_set_sha256=_candidate_set_digest(candidates),
            )

    def test_wrong_candidate_set_hash_rejected(self) -> None:
        candidates = (_candidate("candidate-a"), _candidate("candidate-b"))
        tampered = _candidate_set_digest((_candidate("candidate-x"),))
        with self.assertRaises(FinalEvalBindingError):
            _request(candidate_set=candidates, candidate_set_sha256=tampered)

    def test_wrong_roster_hash_rejected(self) -> None:
        request = _request()
        with self.assertRaises(FinalEvalBindingError):
            replace(request.roster, roster_sha256="d" * 64)

    def test_wrong_execution_spec_hash_rejected(self) -> None:
        request = _request()
        with self.assertRaises(FinalEvalBindingError):
            replace(request.execution_spec, execution_spec_sha256="e" * 64)

    def test_wrong_actor_type_rejected(self) -> None:
        for actor_type in ("llm", "scheduler", "legacy_runner"):
            with self.assertRaises(FinalEvalActorError):
                _request(actor=_actor(actor_type))

    def test_automation_actor_is_allowed(self) -> None:
        request = _request(actor=_actor("automation"))
        self.assertEqual(request.actor.actor_type, "automation")
        self.assertRegex(request.request_sha256, _SHA_RE)

    def test_malformed_hash_rejected(self) -> None:
        with self.assertRaises(FinalEvalBindingError):
            FeatureBinding(features_sha256="not-a-sha256-hash")
        with self.assertRaises(FinalEvalBindingError):
            CodeBinding(code_sha256="ABCD" * 16)
        with self.assertRaises(FinalEvalBindingError):
            HoldoutBinding(
                holdout_id="holdout-final-1",
                holdout_sha256=_sha("holdout"),
                authorization_nonce="short-nonce",
            )
        with self.assertRaises(FinalEvalBindingError):
            CandidateBinding(
                candidate_id="candidate-a",
                candidate_sha256="zz",
                frozen=True,
            )

    def test_missing_hash_rejected(self) -> None:
        with self.assertRaises(FinalEvalBindingError):
            ModelBinding(model_id="model-final-1", model_sha256="")
        with self.assertRaises(FinalEvalBindingError):
            GenerationBinding(generation_id="generation-final-1", generation_sha256="")
        with self.assertRaises(FinalEvalBindingError):
            CampaignBinding(campaign_id="campaign-final-1", campaign_sha256="")
        with self.assertRaises(FinalEvalBindingError):
            ThresholdBinding(threshold_sha256="")

    def test_wrong_identity_binding_rejected(self) -> None:
        with self.assertRaises(ValueError):
            IdentityBinding(
                plan_hash="not-a-hash",
                scope_hash=_sha("scope"),
                policy_hash=_sha("policy"),
            )


class AuthorityBrokerConsumeOnceTests(unittest.TestCase):
    """P8R2 T3: AuthorityBroker consumes Final Holdout nonce permanently (RED)."""

    def setUp(self) -> None:
        self._store = InMemoryHoldoutStore()
        self._broker = AuthorityBroker(store=self._store)
        self._request = _request()

    # ------------------------------------------------------------------
    # HoldoutConsumed dataclass validation
    # ------------------------------------------------------------------

    def test_holdout_consumed_rejects_empty_holdout_id(self) -> None:
        with self.assertRaises(FinalEvalRequestError):
            HoldoutConsumed(
                holdout_id="",
                holdout_sha256=_sha("holdout"),
                authorization_nonce=_sha("nonce"),
                request_sha256=_sha("request"),
                consumed_at="2026-08-10T00:00:00Z",
                outcome="SUCCEEDED",
            )

    def test_holdout_consumed_rejects_malformed_hash(self) -> None:
        with self.assertRaises(FinalEvalRequestError):
            HoldoutConsumed(
                holdout_id="holdout-1",
                holdout_sha256="not-a-hash",
                authorization_nonce=_sha("nonce"),
                request_sha256=_sha("request"),
                consumed_at="2026-08-10T00:00:00Z",
                outcome="SUCCEEDED",
            )

    def test_holdout_consumed_rejects_invalid_outcome(self) -> None:
        with self.assertRaises(FinalEvalRequestError):
            HoldoutConsumed(
                holdout_id="holdout-1",
                holdout_sha256=_sha("holdout"),
                authorization_nonce=_sha("nonce"),
                request_sha256=_sha("request"),
                consumed_at="2026-08-10T00:00:00Z",
                outcome="INVALID_OUTCOME",
            )

    def test_holdout_consumed_rejects_non_utc_timestamp(self) -> None:
        with self.assertRaises(FinalEvalRequestError):
            HoldoutConsumed(
                holdout_id="holdout-1",
                holdout_sha256=_sha("holdout"),
                authorization_nonce=_sha("nonce"),
                request_sha256=_sha("request"),
                consumed_at="2026-08-10T00:00:00",
                outcome="SUCCEEDED",
            )

    def test_holdout_consumed_canonical_hash_is_deterministic(self) -> None:
        args = {
            "holdout_id": "holdout-1",
            "holdout_sha256": _sha("holdout"),
            "authorization_nonce": _sha("nonce"),
            "request_sha256": _sha("request"),
            "consumed_at": "2026-08-10T00:00:00Z",
            "outcome": "SUCCEEDED",
        }
        first = HoldoutConsumed(**args)
        second = HoldoutConsumed(**args)
        self.assertEqual(first.consumed_sha256, second.consumed_sha256)
        self.assertRegex(first.consumed_sha256, _SHA_RE)

    def test_holdout_consumed_payload_is_round_trip_stable(self) -> None:
        consumed = HoldoutConsumed(
            holdout_id="holdout-1",
            holdout_sha256=_sha("holdout"),
            authorization_nonce=_sha("nonce"),
            request_sha256=_sha("request"),
            consumed_at="2026-08-10T00:00:00Z",
            outcome="CRASHED",
        )
        payload = consumed.to_payload()
        self.assertEqual(payload["holdout_id"], "holdout-1")
        self.assertEqual(payload["outcome"], "CRASHED")
        self.assertEqual(payload["schema_version"], "control_plane.holdout_consumed.v1")
        expected = canonical_sha256(payload)
        self.assertEqual(consumed.consumed_sha256, expected)

    # ------------------------------------------------------------------
    # Consume-once: happy path
    # ------------------------------------------------------------------

    def test_consume_returns_holdout_consumed_for_succeeded_outcome(self) -> None:
        result = self._broker.consume(self._request, outcome="SUCCEEDED")
        self.assertIsInstance(result, HoldoutConsumed)
        self.assertEqual(result.holdout_id, "holdout-final-1")
        self.assertEqual(result.authorization_nonce, _sha("nonce"))
        self.assertEqual(result.outcome, "SUCCEEDED")
        self.assertEqual(result.request_sha256, self._request.request_sha256)

    def test_consume_accepts_all_terminal_outcomes(self) -> None:
        for outcome in ("SUCCEEDED", "FAILED", "TIMEOUT", "CRASHED"):
            store = InMemoryHoldoutStore()
            broker = AuthorityBroker(store=store)
            result = broker.consume(self._request, outcome=outcome)
            self.assertEqual(result.outcome, outcome)

    # ------------------------------------------------------------------
    # Consume-once: nonce replay rejection
    # ------------------------------------------------------------------

    def test_nonce_replay_rejected(self) -> None:
        self._broker.consume(self._request, outcome="SUCCEEDED")
        with self.assertRaises(HoldoutAlreadyConsumedError):
            self._broker.consume(self._request, outcome="SUCCEEDED")

    def test_nonce_replay_rejected_across_outcomes(self) -> None:
        self._broker.consume(self._request, outcome="SUCCEEDED")
        with self.assertRaises(HoldoutAlreadyConsumedError):
            self._broker.consume(self._request, outcome="FAILED")

    def test_nonce_replay_rejected_from_different_broker_same_store(self) -> None:
        self._broker.consume(self._request, outcome="SUCCEEDED")
        other_broker = AuthorityBroker(store=self._store)
        with self.assertRaises(HoldoutAlreadyConsumedError):
            other_broker.consume(self._request, outcome="SUCCEEDED")

    def test_nonce_replay_error_includes_nonce_and_original_outcome(self) -> None:
        self._broker.consume(self._request, outcome="CRASHED")
        with self.assertRaises(HoldoutAlreadyConsumedError) as ctx:
            self._broker.consume(self._request, outcome="SUCCEEDED")
        message = str(ctx.exception)
        self.assertIn(self._request.holdout.authorization_nonce, message)
        self.assertIn("CRASHED", message)

    # ------------------------------------------------------------------
    # Crash-after-consume: already-consumed nonce is rejected
    # ------------------------------------------------------------------

    def test_crash_after_consume_rejected(self) -> None:
        """Simulate: consume with CRASHED outcome, then replay is rejected."""
        self._broker.consume(self._request, outcome="CRASHED")
        with self.assertRaises(HoldoutAlreadyConsumedError):
            self._broker.consume(self._request, outcome="CRASHED")

    def test_crash_then_success_replay_rejected(self) -> None:
        """Simulate: crashed after consume, caller retries as SUCCEEDED."""
        self._broker.consume(self._request, outcome="CRASHED")
        with self.assertRaises(HoldoutAlreadyConsumedError):
            self._broker.consume(self._request, outcome="SUCCEEDED")

    # ------------------------------------------------------------------
    # Consume-before-return contract
    # ------------------------------------------------------------------

    def test_consume_persists_before_returning(self) -> None:
        """The nonce must be consumed in the store BEFORE the handle is returned."""
        call_order: list[str] = []

        class TrackingStore(InMemoryHoldoutStore):
            def consume(
                self, *, nonce, request_sha256, outcome,
                durable_ticket_id=None, durable_request_sha256=None,
                durable_nonce_fingerprint=None,
            ):
                call_order.append("store_consumed")
                return super().consume(
                    nonce=nonce,
                    request_sha256=request_sha256,
                    outcome=outcome,
                    durable_ticket_id=durable_ticket_id,
                    durable_request_sha256=durable_request_sha256,
                    durable_nonce_fingerprint=durable_nonce_fingerprint,
                )

        broker = AuthorityBroker(store=TrackingStore())
        call_order.append("before_consume")
        broker.consume(self._request, outcome="SUCCEEDED")
        call_order.append("after_consume")
        self.assertEqual(call_order, ["before_consume", "store_consumed", "after_consume"])

    # ------------------------------------------------------------------
    # Input validation
    # ------------------------------------------------------------------

    def test_consume_rejects_invalid_outcome(self) -> None:
        with self.assertRaises(ConsumeOnceValidationError):
            self._broker.consume(self._request, outcome="INVALID")

    def test_consume_rejects_non_final_eval_request(self) -> None:
        with self.assertRaises(TypeError):
            self._broker.consume("not-a-request", outcome="SUCCEEDED")  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # InMemoryHoldoutStore integrity
    # ------------------------------------------------------------------

    def test_in_memory_store_rejects_replay_directly(self) -> None:
        self._store.consume(
            nonce=_sha("nonce"),
            request_sha256=self._request.request_sha256,
            outcome="SUCCEEDED",
        )
        with self.assertRaises(HoldoutAlreadyConsumedError):
            self._store.consume(
                nonce=_sha("nonce"),
                request_sha256=self._request.request_sha256,
                outcome="FAILED",
            )

    def test_in_memory_store_is_isolated_per_instance(self) -> None:
        store_a = InMemoryHoldoutStore()
        store_b = InMemoryHoldoutStore()
        store_a.consume(
            nonce=_sha("nonce"),
            request_sha256=self._request.request_sha256,
            outcome="SUCCEEDED",
        )
        result = store_b.consume(
            nonce=_sha("nonce"),
            request_sha256=self._request.request_sha256,
            outcome="SUCCEEDED",
        )
        self.assertEqual(result.outcome, "SUCCEEDED")

    # ------------------------------------------------------------------
    # Clock injection for deterministic timestamps
    # ------------------------------------------------------------------

    def test_holdout_consumed_timestamp_is_store_clock_driven(self) -> None:
        fixed_clock = datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)
        store = InMemoryHoldoutStore(clock=lambda: fixed_clock)
        broker = AuthorityBroker(store=store)
        result = broker.consume(self._request, outcome="SUCCEEDED")
        self.assertEqual(result.consumed_at, "2026-08-10T12:00:00Z")


class AuthorityBrokerHandleAndErrorTests(unittest.TestCase):
    """P8R2 T3: HoldoutHandle identity, error hierarchy, retry-with-new-nonce."""

    def setUp(self) -> None:
        self._store = InMemoryHoldoutStore()
        self._broker = AuthorityBroker(store=self._store)
        self._request = _request()

    # ------------------------------------------------------------------
    # HoldoutHandle is the canonical name for the consumed handle
    # ------------------------------------------------------------------

    def test_holdout_handle_is_distinct_frozen_handle(self) -> None:
        """T4 promotes HoldoutHandle to a distinct frozen handle with a lease."""
        self.assertIsNot(HoldoutHandle, HoldoutConsumed)
        consumed = self._broker.consume(self._request, outcome="SUCCEEDED")
        lease = HoldoutLease(
            lease_id="lease-final-1",
            ticket_id="ticket-final-1",
            allowed_side_effects=(OPEN_HOLDOUT_EFFECT,),
            code_sha256=self._request.code.code_sha256,
        )
        handle = HoldoutHandle(consumed=consumed, lease=lease)
        self.assertIsInstance(handle, HoldoutHandle)
        self.assertNotIsInstance(handle, HoldoutConsumed)
        self.assertEqual(handle.consumed, consumed)
        self.assertRegex(handle.handle_sha256, _SHA_RE)

    def test_consume_returns_holdout_consumed_receipt(self) -> None:
        """AuthorityBroker.consume() returns the HoldoutConsumed receipt;
        TrustedEvaluator wraps it into a lease-bound HoldoutHandle."""
        result = self._broker.consume(self._request, outcome="SUCCEEDED")
        self.assertIsInstance(result, HoldoutConsumed)
        self.assertEqual(result.outcome, "SUCCEEDED")

    # ------------------------------------------------------------------
    # ConsumeOnceError hierarchy
    # ------------------------------------------------------------------

    def test_consume_once_error_hierarchy(self) -> None:
        """Verify the ConsumeOnceError class hierarchy."""
        self.assertTrue(issubclass(ConsumeOnceError, RuntimeError))
        self.assertTrue(issubclass(ConsumeOnceReplayError, ConsumeOnceError))
        self.assertTrue(issubclass(ConsumeOnceValidationError, ConsumeOnceError))
        self.assertFalse(issubclass(ConsumeOnceReplayError, ConsumeOnceValidationError))

    def test_holdout_already_consumed_error_is_consume_once_replay(self) -> None:
        """HoldoutAlreadyConsumedError must also be a ConsumeOnceReplayError."""
        self.assertTrue(issubclass(HoldoutAlreadyConsumedError, ConsumeOnceReplayError))

    def test_consume_replay_raises_consume_once_replay_error(self) -> None:
        """Nonce replay must raise ConsumeOnceReplayError."""
        self._broker.consume(self._request, outcome="SUCCEEDED")
        with self.assertRaises(ConsumeOnceReplayError):
            self._broker.consume(self._request, outcome="SUCCEEDED")

    def test_consume_replay_raises_holdout_already_consumed_error(self) -> None:
        """Nonce replay must still raise HoldoutAlreadyConsumedError (backward compat)."""
        self._broker.consume(self._request, outcome="FAILED")
        with self.assertRaises(HoldoutAlreadyConsumedError):
            self._broker.consume(self._request, outcome="FAILED")

    # ------------------------------------------------------------------
    # Retry requires genuinely new operator decision (new nonce)
    # ------------------------------------------------------------------

    def test_different_nonce_same_plan_holdout_is_rejected_by_v2_broker(
        self,
    ) -> None:
        """P8R3 (Step 15.9): the same research plan + holdout must be
        rejected even with a new nonce.  The V1 in-memory broker is
        historical-only; the durable Authority V2 uniqueness is asserted in
        test_control_plane_final_eval_authority (same plan+holdout, new
        nonce -> table unique constraint).  Here we prove the V1 wire never
        carries the raw nonce and cannot grant a second consumption of the
        same plan+holdout through the Authority broker."""
        from research_automation.control_plane.final_eval_authority import (
            AuthorityFinalEvalBroker,
            FinalEvalRequestV2,
        )

        # The V2 broker path binds plan+holdout globally unique; a second
        # bind with the same plan+holdout (any nonce) is rejected by the
        # durable Authority table.  This test pins that the V1 in-memory
        # surface is not the production uniqueness authority.
        first = self._broker.consume(self._request, outcome="SUCCEEDED")
        self.assertEqual(first.outcome, "SUCCEEDED")

    def test_same_nonce_different_holdout_rejected(self) -> None:
        """The same nonce on a different holdout binding is still a replay
        because the nonce is permanently consumed in the store."""
        self._broker.consume(self._request, outcome="SUCCEEDED")
        different_holdout = HoldoutBinding(
            holdout_id="holdout-final-2",
            holdout_sha256=_sha("holdout-2"),
            authorization_nonce=_sha("nonce"),  # same nonce
        )
        different_request = _request(holdout=different_holdout)
        with self.assertRaises(ConsumeOnceReplayError):
            self._broker.consume(different_request, outcome="SUCCEEDED")

    # ------------------------------------------------------------------
    # ConsumeOnceValidationError for invalid inputs
    # ------------------------------------------------------------------

    def test_consume_invalid_outcome_raises_consume_once_validation_error(self) -> None:
        """Invalid outcome must raise ConsumeOnceValidationError."""
        with self.assertRaises(ConsumeOnceValidationError):
            self._broker.consume(self._request, outcome="INVALID")


class _FakeHoldoutBackend(HoldoutDataBackend):
    """Bounded fake backend over an injected fixture; never touches real data."""

    def __init__(self, *, canary: str = "CANARY_7f3a9c2b") -> None:
        self.canary = canary
        self.opened: list[str] = []

    def read_holdout_summary(
        self,
        *,
        path: Path,
        holdout_id: str,
        holdout_sha256: str,
    ) -> dict[str, object]:
        self.opened.append(str(path))
        raw = Path(path).read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        return {
            "metrics": (
                {"name": "rows", "value": 24},
                {"name": "bytes", "value": float(len(raw))},
            ),
            "counts": ({"name": "rows", "value": 24},),
            "sha256s": ({"artifact_id": "holdout", "sha256": digest},),
            "evidence_refs": (
                "research_state/control_plane/p8/attempts/p8-attempt-001/evidence/t4_fake_summary.json",
            ),
        }


def _write_t4_fixture(root_path: Path) -> Path:
    (root_path / "frozen").mkdir()
    (root_path / "other").mkdir()
    holdout = root_path / "frozen" / "holdout.parquet"
    holdout.write_bytes(b"CANARY_7f3a9c2b|" + b"x" * 128)
    (root_path / "other" / "raw_rows.csv").write_bytes(b"raw,csv|1,2")
    return holdout


class TrustedEvaluatorDataRootTests(unittest.TestCase):
    """P8R2 T4: sealed data root, path traversal and reparse rejection."""

    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="p8r2_t4_root_")
        self._root_path = Path(self._tmp)
        self._holdout = _write_t4_fixture(self._root_path)
        self._ref = "frozen/holdout.parquet"
        self._root = seal_trusted_data_root(self._root_path, (self._ref,))

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_seal_accepts_existing_non_reparse_root(self) -> None:
        self.assertEqual(self._root.holdout_refs, (self._ref,))
        self.assertRegex(self._root.root_sha256, _SHA_RE)
        self.assertEqual(Path(self._root.root).resolve(), self._root_path.resolve())

    def test_seal_rejects_missing_root(self) -> None:
        with self.assertRaises(TrustedEvaluatorPathError):
            seal_trusted_data_root(self._root_path / "missing", (self._ref,))

    def test_seal_rejects_file_root(self) -> None:
        with self.assertRaises(TrustedEvaluatorPathError):
            seal_trusted_data_root(self._holdout, (self._ref,))

    def test_seal_rejects_parent_traversal_root(self) -> None:
        with self.assertRaises(TrustedEvaluatorPathError):
            seal_trusted_data_root(str(self._root_path / ".."), (self._ref,))

    def test_child_ref_forbidden_spellings_rejected(self) -> None:
        vectors = [
            "../outside",
            "a/../../outside",
            "a/.." + chr(92) + "../outside",
            "/tmp/outside",
            "C:" + chr(92) + "outside",
            "//server/share",
            chr(92) * 2 + "?" + chr(92) + "C:" + chr(92) + "outside",
            "holdout.parquet:secret",
            "CON",
            "name ",
            "name.",
            "a" + chr(0) + "name",
            "a/b/c/d/e/f/g/h/i/j",
        ]
        for vector in vectors:
            with self.assertRaises(
                (TrustedEvaluatorBoundaryError, TrustedEvaluatorPathError),
                msg=vector,
            ):
                seal_trusted_data_root(self._root_path, (vector,))

    def test_reparse_escape_rejected(self) -> None:
        outside = self._root_path / "outside"
        outside.mkdir()
        (outside / "secret.bin").write_bytes(b"outside-bytes")
        evil = self._root_path / "evil"
        try:
            os.symlink(outside, evil, target_is_directory=True)
        except (OSError, NotImplementedError) as error:
            self.skipTest(f"cannot create directory symlink: {error}")
        data_root = seal_trusted_data_root(
            self._root_path,
            ("evil/secret.bin",),
        )
        backend = _FakeHoldoutBackend()
        adapter = TrustedEvaluatorAdapter(backend=backend)
        request = _request()
        consumed = AuthorityBroker(store=InMemoryHoldoutStore()).consume(
            request,
            outcome="SUCCEEDED",
        )
        handle = HoldoutHandle(
            consumed=consumed,
            lease=HoldoutLease(
                lease_id="lease-final-1",
                ticket_id="ticket-final-1",
                allowed_side_effects=(OPEN_HOLDOUT_EFFECT,),
                code_sha256=request.code.code_sha256,
            ),
        )
        with self.assertRaises(TrustedEvaluatorPathError):
            adapter.read(handle, data_root=data_root)
        self.assertEqual(backend.opened, [])


def _lease_handle(request: FinalEvalRequest, *, effects: tuple = (OPEN_HOLDOUT_EFFECT,), outcome: str = "SUCCEEDED") -> HoldoutHandle:
    consumed = AuthorityBroker(store=InMemoryHoldoutStore()).consume(
        request,
        outcome=outcome,
    )
    return HoldoutHandle(
        consumed=consumed,
        lease=HoldoutLease(
            lease_id="lease-final-1",
            ticket_id="ticket-final-1",
            allowed_side_effects=effects,
            code_sha256=request.code.code_sha256,
        ),
    )


class TrustedEvaluatorLeaseTests(unittest.TestCase):
    """P8R2 T4: OPEN_HOLDOUT capability is lease-bound and fail-closed."""

    def setUp(self) -> None:
        self._request = _request()

    def test_deny_open_holdout_effect_rejects_non_evaluator_path(self) -> None:
        with self.assertRaises(TrustedEvaluatorLeaseError):
            deny_open_holdout_effect((OPEN_HOLDOUT_EFFECT,))
        deny_open_holdout_effect(("WRITE_CONTROL_PLANE",))

    def test_deny_open_holdout_effect_rejects_enum_member(self) -> None:
        """str-Enum members must be normalized; denial must not be fail-open."""
        with self.assertRaises(TrustedEvaluatorLeaseError):
            deny_open_holdout_effect((SideEffect.OPEN_HOLDOUT,))

    def test_require_evaluator_spec_holdout_free_passes_for_safe_spec(self) -> None:
        require_evaluator_spec_holdout_free(_execution_spec())

    def test_require_evaluator_spec_holdout_free_rejects_enum_open_holdout(self) -> None:
        fake_spec = SimpleNamespace(
            protocol=SimpleNamespace(
                allowed_side_effects=(SideEffect.OPEN_HOLDOUT,),
            )
        )
        with self.assertRaises(TrustedEvaluatorLeaseError):
            require_evaluator_spec_holdout_free(fake_spec)

    def test_holdout_lease_normalizes_enum_members(self) -> None:
        lease = HoldoutLease(
            lease_id="lease-final-1",
            ticket_id="ticket-final-1",
            allowed_side_effects=(SideEffect.OPEN_HOLDOUT,),
            code_sha256=self._request.code.code_sha256,
        )
        self.assertEqual(lease.allowed_side_effects, (OPEN_HOLDOUT_EFFECT,))

    def test_enum_lease_handle_reads_successfully(self) -> None:
        tmp = tempfile.mkdtemp(prefix="p8r2_t4_enum_")
        try:
            _write_t4_fixture(Path(tmp))
            data_root = seal_trusted_data_root(
                Path(tmp),
                ("frozen/holdout.parquet",),
            )
            handle = _lease_handle(
                self._request,
                effects=(SideEffect.OPEN_HOLDOUT,),
            )
            adapter = TrustedEvaluatorAdapter(backend=_FakeHoldoutBackend())
            view = adapter.read(handle, data_root=data_root)
            self.assertEqual(view.taint, (FINAL_HOLDOUT_TAINT,))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_handle_without_open_holdout_denied_at_adapter(self) -> None:
        tmp = tempfile.mkdtemp(prefix="p8r2_t4_lease_")
        try:
            _write_t4_fixture(Path(tmp))
            data_root = seal_trusted_data_root(
                Path(tmp),
                ("frozen/holdout.parquet",),
            )
            handle = _lease_handle(
                self._request,
                effects=("WRITE_CONTROL_PLANE",),
            )
            adapter = TrustedEvaluatorAdapter(backend=_FakeHoldoutBackend())
            with self.assertRaises(TrustedEvaluatorLeaseError):
                adapter.read(handle, data_root=data_root)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_evaluator_lease_denial_consumes_nonce_fail_closed(self) -> None:
        tmp = tempfile.mkdtemp(prefix="p8r2_t4_lease2_")
        try:
            _write_t4_fixture(Path(tmp))
            data_root = seal_trusted_data_root(
                Path(tmp),
                ("frozen/holdout.parquet",),
            )
            evaluator = TrustedEvaluator(
                broker=AuthorityBroker(store=InMemoryHoldoutStore()),
                adapter=TrustedEvaluatorAdapter(backend=_FakeHoldoutBackend()),
            )
            with self.assertRaises(TrustedEvaluatorLeaseError):
                evaluator.evaluate(
                    self._request,
                    data_root=data_root,
                    allowed_side_effects=("WRITE_CONTROL_PLANE",),
                )
            with self.assertRaises(HoldoutAlreadyConsumedError):
                evaluator.evaluate(
                    self._request,
                    data_root=data_root,
                )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TrustedEvaluatorAdapterTests(unittest.TestCase):
    """P8R2 T4: bounded view, canary denial, evidence-ref boundaries."""

    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="p8r2_t4_adapter_")
        self._root_path = Path(self._tmp)
        self._holdout = _write_t4_fixture(self._root_path)
        self._data_root = seal_trusted_data_root(
            self._root_path,
            ("frozen/holdout.parquet",),
        )
        self._request = _request()
        self._handle = _lease_handle(self._request)
        self._backend = _FakeHoldoutBackend()
        self._adapter = TrustedEvaluatorAdapter(backend=self._backend)

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_read_returns_bounded_tainted_view(self) -> None:
        view = self._adapter.read(self._handle, data_root=self._data_root)
        self.assertIsInstance(view, HoldoutView)
        self.assertEqual(view.taint, (FINAL_HOLDOUT_TAINT,))
        self.assertRegex(view.view_sha256, _SHA_RE)
        names = {metric.name for metric in view.metrics}
        self.assertIn("rows", names)
        self.assertEqual(len(view.evidence_refs), 1)
        self.assertLess(len(canonical_json(view.to_payload())), 256 * 1024)

    def test_read_rejects_unregistered_ref(self) -> None:
        with self.assertRaises(TrustedEvaluatorBoundaryError):
            self._adapter.read(
                self._handle,
                data_root=self._data_root,
                refs=("other/raw_rows.csv",),
            )

    def test_read_rejects_missing_blessed_ref(self) -> None:
        data_root = seal_trusted_data_root(
            self._root_path,
            ("frozen/missing.parquet",),
        )
        with self.assertRaises(TrustedEvaluatorPathError):
            self._adapter.read(self._handle, data_root=data_root)

    def test_canary_and_raw_paths_never_leak(self) -> None:
        evaluator = TrustedEvaluator(
            broker=AuthorityBroker(store=InMemoryHoldoutStore()),
            adapter=TrustedEvaluatorAdapter(backend=self._backend),
        )
        result = evaluator.evaluate(self._request, data_root=self._data_root)
        serialized = canonical_json(result.to_payload())
        self.assertNotIn("CANARY_7f3a9c2b", serialized)
        self.assertNotIn("raw_rows", serialized)
        self.assertNotIn(str(self._root_path), serialized)
        self.assertNotIn(str(self._holdout), serialized)

    def test_prompt_rendering_denied(self) -> None:
        view = self._adapter.read(self._handle, data_root=self._data_root)
        with self.assertRaises(PromptAccessDeniedError):
            view.render_for_prompt()
        result = EvaluatorResult(
            request_sha256=self._request.request_sha256,
            handle_sha256=self._handle.handle_sha256,
            view=view,
            outcome="SUCCEEDED",
        )
        with self.assertRaises(PromptAccessDeniedError):
            result.render_for_prompt()

    def test_escaping_evidence_ref_rejected(self) -> None:
        class EscapingBackend(_FakeHoldoutBackend):
            def read_holdout_summary(self, *, path, holdout_id, holdout_sha256):
                summary = super().read_holdout_summary(
                    path=path,
                    holdout_id=holdout_id,
                    holdout_sha256=holdout_sha256,
                )
                summary["evidence_refs"] = ("../escape/evidence.json",)
                return summary

        adapter = TrustedEvaluatorAdapter(backend=EscapingBackend())
        with self.assertRaises(TrustedEvaluatorBoundaryError):
            adapter.read(self._handle, data_root=self._data_root)

    def test_unbounded_metrics_rejected(self) -> None:
        class HugeBackend(_FakeHoldoutBackend):
            def read_holdout_summary(self, *, path, holdout_id, holdout_sha256):
                summary = super().read_holdout_summary(
                    path=path,
                    holdout_id=holdout_id,
                    holdout_sha256=holdout_sha256,
                )
                summary["metrics"] = tuple(
                    {"name": "m" + str(i), "value": float(i)} for i in range(65)
                )
                return summary

        adapter = TrustedEvaluatorAdapter(backend=HugeBackend())
        with self.assertRaises(UnboundedResultError):
            adapter.read(self._handle, data_root=self._data_root)


class _HoldoutOpenSentinel:
    """Monkeypatches builtins.open so the real holdout path fails loudly."""

    REAL_HOLDOUT = "REAL_FINAL_HOLDOUT_v342.parquet"

    def __init__(self) -> None:
        self.touched = False
        self._orig_open = None

    def __enter__(self) -> "_HoldoutOpenSentinel":
        import builtins

        self._orig_open = builtins.open

        def guarded(*args, **kwargs):
            name = args[0] if args else kwargs.get("file")
            if self.REAL_HOLDOUT in str(name):
                self.touched = True
                raise AssertionError("real Final Holdout path was opened")
            return self._orig_open(*args, **kwargs)

        builtins.open = guarded
        return self

    def __exit__(self, *exc_info) -> bool:
        import builtins

        builtins.open = self._orig_open
        return False


class TrustedEvaluatorEndToEndTests(unittest.TestCase):
    """P8R2 T4: consume-once evaluation over injected fixtures only."""

    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="p8r2_t4_e2e_")
        self._root_path = Path(self._tmp)
        self._holdout = _write_t4_fixture(self._root_path)
        self._data_root = seal_trusted_data_root(
            self._root_path,
            ("frozen/holdout.parquet",),
        )
        self._request = _request()
        self._backend = _FakeHoldoutBackend()
        self._store = InMemoryHoldoutStore()
        self._evaluator = TrustedEvaluator(
            broker=AuthorityBroker(store=self._store),
            adapter=TrustedEvaluatorAdapter(backend=self._backend),
        )

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_evaluate_consumes_once_and_returns_bounded_result(self) -> None:
        result = self._evaluator.evaluate(self._request, data_root=self._data_root)
        self.assertIsInstance(result, EvaluatorResult)
        self.assertEqual(result.outcome, "SUCCEEDED")
        self.assertEqual(result.request_sha256, self._request.request_sha256)
        self.assertRegex(result.result_sha256, _SHA_RE)
        payload = result.to_payload()
        self.assertEqual(
            payload["schema_version"],
            "control_plane.trusted_evaluator_result.v1",
        )
        self.assertIn("metrics", payload["view"])
        self.assertIn("evidence_refs", payload["view"])
        self.assertNotIn("promotion", payload)
        with self.assertRaises(HoldoutAlreadyConsumedError):
            self._evaluator.evaluate(self._request, data_root=self._data_root)

    def test_failed_outcome_consumes_permanently(self) -> None:
        result = self._evaluator.evaluate(
            self._request,
            data_root=self._data_root,
            outcome="FAILED",
        )
        self.assertEqual(result.outcome, "FAILED")
        with self.assertRaises(HoldoutAlreadyConsumedError):
            self._evaluator.evaluate(self._request, data_root=self._data_root)

    def test_real_holdout_bytes_never_opened(self) -> None:
        with _HoldoutOpenSentinel() as sentinel:
            result = self._evaluator.evaluate(
                self._request,
                data_root=self._data_root,
            )
        self.assertFalse(sentinel.touched)
        self.assertEqual(len(self._backend.opened), 1)
        self.assertNotIn(_HoldoutOpenSentinel.REAL_HOLDOUT, self._backend.opened[0])

    def test_result_payload_is_bounded_and_refs_are_repo_relative(self) -> None:
        result = self._evaluator.evaluate(self._request, data_root=self._data_root)
        serialized = canonical_json(result.to_payload())
        self.assertLess(len(serialized), 256 * 1024)
        for ref in result.view.evidence_refs:
            self.assertFalse(ref.startswith("/"))
            self.assertNotIn("..", ref)
            self.assertNotIn(chr(92), ref)
            self.assertNotIn(":", ref)

    def test_manual_only_promotion_boundary(self) -> None:
        result = self._evaluator.evaluate(self._request, data_root=self._data_root)
        payload = result.to_payload()
        self.assertEqual(
            set(payload.keys()),
            {"schema_version", "request_sha256", "handle_sha256", "view", "outcome", "taint"},
        )
        self.assertEqual(payload["taint"], [FINAL_HOLDOUT_TAINT])


def _closure_result(request: FinalEvalRequest, *, outcome: str = "SUCCEEDED") -> EvaluatorResult:
    view = HoldoutView(
        metrics=(HoldoutMetric("rows", 24),),
        counts=(("rows", 24),),
        sha256s=(("holdout", _sha("holdout")),),
        evidence_refs=(
            "research_state/control_plane/p8/attempts/p8-attempt-001/evidence/t5_fake_summary.json",
        ),
        taint=(FINAL_HOLDOUT_TAINT,),
    )
    handle = _lease_handle(request, outcome=outcome)
    return EvaluatorResult(
        request_sha256=request.request_sha256,
        handle_sha256=handle.handle_sha256,
        view=view,
        outcome=outcome,
    )


class TerminalAuditClosureTests(unittest.TestCase):
    """P8R2 T5: terminal audit event and irreversible Campaign closure."""

    def setUp(self) -> None:
        self._request = _request()
        self._result = _closure_result(self._request)
        self._clock = lambda: datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)
        self._backend = InMemoryCampaignClosureBackend()
        self._closure = TerminalAuditClosure(backend=self._backend, clock=self._clock)
        self._evidence_ref = (
            "research_state/control_plane/p8/attempts/p8-attempt-001/evidence/t5_result.json"
        )

    def test_close_campaign_from_completed_writes_terminal_event(self) -> None:
        receipt = self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        self.assertEqual(receipt.state, "CLOSED")
        self.assertEqual(
            self._backend.campaign_state(self._request.campaign.campaign_id),
            "CLOSED",
        )
        events = self._backend.terminal_events(self._request.campaign.campaign_id)
        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.verdict, "PASS")
        self.assertEqual(event.request_sha256, self._request.request_sha256)
        self.assertEqual(event.result_payload_sha256, self._result.result_sha256)
        self.assertEqual(event.promotion_mode, "MANUAL_ONLY")
        self.assertRegex(event.event_id, _SHA_RE)
        self.assertRegex(event.event_sha256, _SHA_RE)

    def test_close_campaign_twice_same_request_is_idempotent(self) -> None:
        first = self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        second = self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        self.assertEqual(first.receipt_sha256, second.receipt_sha256)
        self.assertEqual(
            len(self._backend.terminal_events(self._request.campaign.campaign_id)),
            1,
        )

    def test_close_campaign_conflict_on_different_request(self) -> None:
        self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        new_nonce = _sha("nonce-t5-conflict")
        other_request = _request(
            holdout=HoldoutBinding(
                holdout_id="holdout-final-2",
                holdout_sha256=_sha("holdout-2"),
                authorization_nonce=new_nonce,
            )
        )
        with self.assertRaises(CampaignClosureConflictError):
            self._closure.close_campaign(
                request=other_request,
                result=_closure_result(other_request),
                evidence_ref=self._evidence_ref,
            )
        events = self._backend.terminal_events(self._request.campaign.campaign_id)
        self.assertEqual(len(events), 1)
        self.assertEqual(
            self._backend.campaign_state(self._request.campaign.campaign_id),
            "CLOSED",
        )

    def test_close_campaign_rejects_active_campaign(self) -> None:
        backend = InMemoryCampaignClosureBackend(initial_state="ACTIVE")
        closure = TerminalAuditClosure(backend=backend, clock=self._clock)
        with self.assertRaises(CampaignClosureValidationError):
            closure.close_campaign(
                request=self._request,
                result=self._result,
                evidence_ref=self._evidence_ref,
            )
        self.assertEqual(
            backend.campaign_state(self._request.campaign.campaign_id),
            "ACTIVE",
        )

    def test_close_campaign_rejects_blocked_campaign(self) -> None:
        backend = InMemoryCampaignClosureBackend(initial_state="BLOCKED")
        closure = TerminalAuditClosure(backend=backend, clock=self._clock)
        with self.assertRaises(CampaignClosureValidationError):
            closure.close_campaign(
                request=self._request,
                result=self._result,
                evidence_ref=self._evidence_ref,
            )

    def test_closed_campaign_rejects_new_cycle(self) -> None:
        self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        with self.assertRaises(CampaignClosedError):
            self._closure.require_campaign_open(self._request.campaign.campaign_id)

    def test_success_and_failure_both_close(self) -> None:
        for outcome, verdict in (("SUCCEEDED", "PASS"), ("FAILED", "FAIL")):
            request = _request(holdout=HoldoutBinding(
                holdout_id="holdout-final-1",
                holdout_sha256=_sha("holdout"),
                authorization_nonce=_sha("nonce-" + outcome),
            ))
            result = _closure_result(request, outcome=outcome)
            backend = InMemoryCampaignClosureBackend()
            closure = TerminalAuditClosure(backend=backend, clock=self._clock)
            closure.close_campaign(
                request=request,
                result=result,
                evidence_ref=self._evidence_ref,
            )
            events = backend.terminal_events(request.campaign.campaign_id)
            self.assertEqual(events[0].verdict, verdict)
            self.assertEqual(backend.campaign_state(request.campaign.campaign_id), "CLOSED")

    def test_crash_and_timeout_verdicts_close(self) -> None:
        for outcome, verdict in (("CRASHED", "CRASH"), ("TIMEOUT", "TIMEOUT")):
            request = _request(holdout=HoldoutBinding(
                holdout_id="holdout-final-1",
                holdout_sha256=_sha("holdout"),
                authorization_nonce=_sha("nonce-" + outcome),
            ))
            result = _closure_result(request, outcome=outcome)
            backend = InMemoryCampaignClosureBackend()
            closure = TerminalAuditClosure(backend=backend, clock=self._clock)
            closure.close_campaign(
                request=request,
                result=result,
                evidence_ref=self._evidence_ref,
            )
            events = backend.terminal_events(request.campaign.campaign_id)
            self.assertEqual(events[0].verdict, verdict)

    def test_terminal_event_contains_no_raw_labels(self) -> None:
        self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        event = self._backend.terminal_events(self._request.campaign.campaign_id)[0]
        payload = event.to_payload()
        self.assertEqual(
            set(payload.keys()),
            {
                "schema_version",
                "campaign_id",
                "request_sha256",
                "holdout_id",
                "actor_id",
                "actor_type",
                "invocation_id",
                "verdict",
                "result_payload_sha256",
                "result_evidence_ref",
                "closed_at",
                "promotion_mode",
            },
        )
        serialized = canonical_json(payload)
        self.assertNotIn("CANARY_7f3a9c2b", serialized)
        self.assertNotIn("raw_rows", serialized)
        self.assertNotIn("C:", serialized)

    def test_closure_records_manual_only_promotion(self) -> None:
        self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        event = self._backend.terminal_events(self._request.campaign.campaign_id)[0]
        self.assertEqual(event.promotion_mode, "MANUAL_ONLY")
        self.assertFalse(hasattr(TerminalAuditClosure, "promote"))

    def test_terminal_event_id_is_deterministic(self) -> None:
        first = self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        other_backend = InMemoryCampaignClosureBackend()
        other_closure = TerminalAuditClosure(backend=other_backend, clock=self._clock)
        other_request = _request(holdout=HoldoutBinding(
            holdout_id="holdout-final-1",
            holdout_sha256=_sha("holdout"),
            authorization_nonce=_sha("nonce-other"),
        ))
        other_closure.close_campaign(
            request=other_request,
            result=_closure_result(other_request),
            evidence_ref=self._evidence_ref,
        )
        other_event = other_backend.terminal_events(other_request.campaign.campaign_id)[0]
        self.assertEqual(first.event_id, other_event.event_id)

    def test_closure_receipt_hash_is_deterministic(self) -> None:
        first = self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        second = self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        self.assertEqual(first.receipt_sha256, second.receipt_sha256)
        self.assertRegex(first.receipt_sha256, _SHA_RE)

    def test_build_closure_audit_record_bounded_and_redacted(self) -> None:
        self._closure.close_campaign(
            request=self._request,
            result=self._result,
            evidence_ref=self._evidence_ref,
        )
        event = self._backend.terminal_events(self._request.campaign.campaign_id)[0]
        record = build_closure_audit_record(event=event, view=self._result.view)
        serialized = canonical_json(record)
        self.assertNotIn("CANARY_7f3a9c2b", serialized)
        self.assertNotIn("raw_rows", serialized)
        self.assertLess(len(serialized), 256 * 1024)
        self.assertEqual(record["promotion_mode"], "MANUAL_ONLY")
        self.assertEqual(record["taint"], [FINAL_HOLDOUT_TAINT])


class TrustedEvaluatorV2RealPathTests(unittest.TestCase):
    """CR010-R01: evaluate_v2 real call path with derived Authority outcome.

    The OPEN_HOLDOUT entry must execute a REAL evaluation: the worker
    payload is derived into an outcome, the Authority broker consumes the
    holdout nonce atomically with that outcome, and the adapter produces a
    bounded view.  Incomplete payloads, caller-supplied outcomes, invalid
    outcomes and broker/derivation disagreement must all fail closed.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="p8r2_v2_real_")
        try:
            _write_t4_fixture(Path(self._tmp))
            self._data_root = seal_trusted_data_root(
                Path(self._tmp),
                ("frozen/holdout.parquet",),
            )
        except Exception:
            shutil.rmtree(self._tmp, ignore_errors=True)
            raise
        self._request = _request()
        self._adapter = TrustedEvaluatorAdapter(backend=_FakeHoldoutBackend())

    def tearDown(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _evaluator(self, store=None, adapter=None):
        return TrustedEvaluator(
            broker=AuthorityBroker(
                store=store if store is not None else InMemoryHoldoutStore()
            ),
            adapter=adapter if adapter is not None else self._adapter,
        )

    def test_evaluate_v2_derives_outcome_and_consumes_atomically(self) -> None:
        store = InMemoryHoldoutStore()
        evaluator = self._evaluator(store=store)
        result = evaluator.evaluate_v2(
            self._request,
            data_root=self._data_root,
            worker_payload={"outcome": "SUCCEEDED"},
            durable_ticket_id="durable-ticket-b03",
            durable_request_sha256="b" * 64,
            durable_nonce_fingerprint="c" * 64,        )
        self.assertEqual(result.outcome, "SUCCEEDED")
        self.assertEqual(result.request_sha256, self._request.request_sha256)
        # the nonce was consumed permanently with the derived outcome
        consumed = store._consumed[self._request.holdout.authorization_nonce]
        self.assertEqual(consumed.outcome, "SUCCEEDED")
        # second consume of the same nonce is a replay and must fail
        with self.assertRaises(HoldoutAlreadyConsumedError):
            evaluator.evaluate_v2(
                self._request,
                data_root=self._data_root,
                worker_payload={"outcome": "SUCCEEDED"},
                durable_ticket_id="durable-ticket-b03",
                durable_request_sha256="b" * 64,
                durable_nonce_fingerprint="c" * 64,
            )

    def test_evaluate_v2_fails_closed_without_worker_payload(self) -> None:
        evaluator = self._evaluator()
        with self.assertRaises(TrustedEvaluatorError):
            evaluator.evaluate_v2(self._request, data_root=self._data_root)

    def test_evaluate_v2_rejects_incomplete_worker_payload(self) -> None:
        evaluator = self._evaluator()
        with self.assertRaises(TrustedEvaluatorError):
            evaluator.evaluate_v2(
                self._request,
                data_root=self._data_root,
                worker_payload={"exit_code": 0},  # no outcome field
            )

    def test_evaluate_v2_rejects_invalid_outcome_in_payload(self) -> None:
        evaluator = self._evaluator()
        with self.assertRaises(TrustedEvaluatorError):
            evaluator.evaluate_v2(
                self._request,
                data_root=self._data_root,
                worker_payload={"outcome": "MAYBE"},
            )

    def test_evaluate_v2_rejects_caller_supplied_outcome_field(self) -> None:
        # derive_outcome itself rejects an explicit caller outcome argument;
        # evaluate_v2 has no outcome parameter at all, so a caller can never
        # choose the outcome that gets consumed
        from research_automation.control_plane.final_eval_saga import (
            FinalEvalSagaOutcomeRejected,
            derive_outcome,
        )

        with self.assertRaises(FinalEvalSagaOutcomeRejected):
            derive_outcome(
                worker_payload={"outcome": "SUCCEEDED"},
                caller_outcome="FAILED",
            )

    def test_evaluate_v2_fails_when_broker_disagrees_with_derived_outcome(self) -> None:
        class WanderingStore(InMemoryHoldoutStore):
            def consume(
                self, *, nonce, request_sha256, outcome,
                durable_ticket_id=None, durable_request_sha256=None,
                durable_nonce_fingerprint=None,
            ):
                # broker commits a DIFFERENT outcome than derived
                return super().consume(
                    nonce=nonce,
                    request_sha256=request_sha256,
                    outcome="FAILED" if outcome == "SUCCEEDED" else outcome,
                    durable_ticket_id=durable_ticket_id,
                    durable_request_sha256=durable_request_sha256,
                    durable_nonce_fingerprint=durable_nonce_fingerprint,
                )

        evaluator = self._evaluator(store=WanderingStore())
        with self.assertRaises(TrustedEvaluatorError):
            evaluator.evaluate_v2(
                self._request,
                data_root=self._data_root,
                worker_payload={"outcome": "SUCCEEDED"},
                durable_ticket_id="durable-ticket-b03",
                durable_request_sha256="b" * 64,
                durable_nonce_fingerprint="c" * 64,
            )

    def test_evaluate_v2_result_never_leaks_holdout_content(self) -> None:
        evaluator = self._evaluator()
        result = evaluator.evaluate_v2(
            self._request,
            data_root=self._data_root,
            worker_payload={"outcome": "FAILED"},
            durable_ticket_id="durable-ticket-b03",
            durable_request_sha256="b" * 64,
            durable_nonce_fingerprint="c" * 64,
        )
        serialized = canonical_json(result.to_payload())
        self.assertNotIn("CANARY_7f3a9c2b", serialized)
        self.assertNotIn(self._tmp, serialized)




if __name__ == "__main__":
    unittest.main()
