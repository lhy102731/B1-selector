"""Focused TDD tests for P8R2 T2: FinalEvalRequest contract (RED)."""
from __future__ import annotations

import hashlib
import unittest
from dataclasses import replace

from research_automation.control_plane.campaign_roster import RosterManifest
from research_automation.control_plane.contracts import Actor, IdentityBinding, canonical_sha256
from research_automation.control_plane.final_evaluator import (
    CampaignBinding,
    CandidateBinding,
    CodeBinding,
    ExecutionSpecBinding,
    FeatureBinding,
    FinalEvalActorError,
    FinalEvalBindingError,
    FinalEvalRequest,
    GenerationBinding,
    HoldoutBinding,
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


if __name__ == "__main__":
    unittest.main()
