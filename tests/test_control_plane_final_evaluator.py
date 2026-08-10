"""Focused TDD tests for P8R2 T2 FinalEvalRequest + T3 AuthorityBroker consume-once."""
from __future__ import annotations

import hashlib
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import Mock

from research_automation.control_plane.campaign_roster import RosterManifest
from research_automation.control_plane.contracts import Actor, IdentityBinding, canonical_sha256
from research_automation.control_plane.final_evaluator import (
    AuthorityBroker,
    CampaignBinding,
    CandidateBinding,
    CodeBinding,
    ConsumeOnceError,
    ConsumeOnceReplayError,
    ConsumeOnceValidationError,
    ExecutionSpecBinding,
    FeatureBinding,
    FinalEvalActorError,
    FinalEvalBindingError,
    FinalEvalRequest,
    FinalEvalRequestError,
    GenerationBinding,
    HoldoutAlreadyConsumedError,
    HoldoutBinding,
    HoldoutConsumed,
    HoldoutHandle,
    InMemoryHoldoutStore,
    ModelBinding,
    RosterBinding,
    ThresholdBinding,
    UnfrozenCandidateError,
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
            def consume(self, *, nonce, request_sha256, outcome):
                call_order.append("store_consumed")
                return super().consume(
                    nonce=nonce,
                    request_sha256=request_sha256,
                    outcome=outcome,
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

    def test_holdout_handle_is_same_type_as_holdout_consumed(self) -> None:
        """HoldoutHandle must be importable and be the same type as HoldoutConsumed."""
        self.assertIs(HoldoutHandle, HoldoutConsumed)

    def test_consume_returns_holdout_handle(self) -> None:
        """AuthorityBroker.consume() returns a HoldoutHandle instance."""
        result = self._broker.consume(self._request, outcome="SUCCEEDED")
        self.assertIsInstance(result, HoldoutHandle)
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

    def test_different_nonce_succeeds_after_first_consumed(self) -> None:
        """A genuinely new nonce (new operator decision) must succeed after
        the first nonce was permanently consumed.  This is the proof that
        retry requires a new operator decision, not the same nonce."""
        first = self._broker.consume(self._request, outcome="SUCCEEDED")
        self.assertEqual(first.outcome, "SUCCEEDED")

        # Build a request with a different nonce (new operator decision).
        new_nonce = _sha("nonce-v2-operator-decision")
        new_holdout = HoldoutBinding(
            holdout_id="holdout-final-1",
            holdout_sha256=_sha("holdout"),
            authorization_nonce=new_nonce,
        )
        new_request = _request(holdout=new_holdout)

        second = self._broker.consume(new_request, outcome="SUCCEEDED")
        self.assertEqual(second.outcome, "SUCCEEDED")
        self.assertNotEqual(
            first.authorization_nonce,
            second.authorization_nonce,
        )
        # Both handles belong to the same holdout but different nonces.
        self.assertEqual(first.holdout_id, second.holdout_id)

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


if __name__ == "__main__":
    unittest.main()
