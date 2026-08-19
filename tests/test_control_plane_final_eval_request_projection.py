"""CR-010 F-03: the V2 evaluator request projection tests.

``build_evaluator_request_v2`` is the ONLY projection entry point: it
recomputes every material digest, carries the source V2 ``request_sha256``
and rejects ANY identity drift BEFORE the adapter request is constructed.
Mutating one actual field per subtest -- in the V2 request or in the
material bundle -- must fail closed before ``evaluate_v2`` consumes
anything.  A V1 evaluator request for a different campaign/holdout can
never be paired with the V2 request, and the raw nonce never appears in
payloads, logs or exceptions.
"""

from __future__ import annotations

import unittest
from dataclasses import replace

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import (
    Actor,
    canonical_sha256,
)
from research_automation.control_plane.final_eval_authority import (
    FINAL_EVAL_REQUEST_V2,
    FinalEvalRequestRejected,
    FinalEvalRequestV2,
    _nonce_fingerprint,
)
from research_automation.control_plane.final_eval_request_projection import (
    FinalEvalMaterialBundle,
    adapt_evaluator_request_v1_test_only,
    build_evaluator_request_v2,
)
from research_automation.control_plane.final_evaluator import (
    CandidateBinding,
    FinalEvalRequest,
)
from research_automation.control_plane.stores import AuthorityIdentity
from tests.test_control_plane_campaign_store import ROOT_SECRET
from tests.test_control_plane_final_eval_orchestrator import P8_IDENTITY
from tests.test_control_plane_final_evaluator import (
    _candidate,
    _candidate_set_digest,
    _execution_spec,
    _roster,
    _sha,
)

NONCE = "0123456789abcdef" * 4
ATTEMPT = "p8-attempt-003"
FREEZE_REF = (
    "research_state/control_plane/p8/attempts/p8-attempt-003/freeze.json"
)


def _valid_request(**overrides) -> FinalEvalRequestV2:
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
        candidate_freeze_ref=FREEZE_REF,
        candidate_freeze_sha256=_candidate_set_digest(candidates),
        code_ref="research_automation/control_plane/final_evaluator.py",
        code_sha256=_sha("code"),
        execution_spec_ref="research_state/control_plane/p8/spec.json",
        execution_spec_sha256=canonical_sha256(
            execution_spec.model_dump(mode="json")
        ),
        features_ref="research_state/control_plane/p8/features.json",
        features_sha256=_sha("features"),
        model="model-final-1",
        model_sha256=_sha("model"),
        threshold="0.5",
        threshold_ref="research_state/control_plane/p8/threshold.json",
        threshold_sha256=_sha("threshold"),
        roster_ref="research_state/control_plane/p8/roster.json",
        roster_sha256=roster.manifest_sha256,
        generation="generation-final-1",
        generation_sha256=_sha("generation"),
        actor_id="operator-1",
        actor_type="human",
        invocation_id="final-eval-op-cr009",
        authority_plan_hash=P8_IDENTITY["plan_hash"],
        identity_scope_hash=P8_IDENTITY["scope_hash"],
        identity_instruction_policy_hash=(
            P8_IDENTITY["instruction_policy_hash"]
        ),
        attempt_id=ATTEMPT,
    )
    payload.update(overrides)
    return FinalEvalRequestV2(**payload)


def _valid_materials(**overrides) -> FinalEvalMaterialBundle:
    execution_spec = _execution_spec()
    roster = _roster()
    payload = dict(
        campaign_id="campaign-final-cr009",
        campaign_sha256="b" * 64,
        holdout_id="holdout-final-cr009",
        holdout_sha256="c" * 64,
        authorization_nonce=NONCE,
        candidate_freeze_ref=FREEZE_REF,
        candidate_set=(_candidate("candidate-a"), _candidate("candidate-b")),
        code_ref="research_automation/control_plane/final_evaluator.py",
        code_sha256=_sha("code"),
        execution_spec=execution_spec,
        execution_spec_ref="research_state/control_plane/p8/spec.json",
        execution_spec_sha256=canonical_sha256(
            execution_spec.model_dump(mode="json")
        ),
        features_ref="research_state/control_plane/p8/features.json",
        features_sha256=_sha("features"),
        model_id="model-final-1",
        model_sha256=_sha("model"),
        threshold_ref="research_state/control_plane/p8/threshold.json",
        threshold_sha256=_sha("threshold"),
        roster=roster,
        roster_ref="research_state/control_plane/p8/roster.json",
        roster_sha256=roster.manifest_sha256,
        generation_id="generation-final-1",
        generation_sha256=_sha("generation"),
        actor=Actor("operator-1", "human", "final-eval-op-cr009"),
        identity=AuthorityIdentity(**P8_IDENTITY),
        attempt_id=ATTEMPT,
    )
    payload.update(overrides)
    return FinalEvalMaterialBundle(**payload)


class EvaluatorRequestProjectionTests(unittest.TestCase):
    def test_valid_projection_carries_v2_identity(self) -> None:
        request = _valid_request()
        projection = build_evaluator_request_v2(
            request,
            _valid_materials(),
            root_secret=ROOT_SECRET,
        )
        self.assertEqual(projection.v2_request_sha256, request.request_sha256)
        self.assertEqual(projection.campaign_id, request.campaign_id)
        self.assertEqual(projection.holdout_id, request.holdout_id)
        self.assertEqual(
            projection.nonce_fingerprint, request.nonce_fingerprint
        )
        self.assertEqual(projection.model_id, request.model)
        self.assertEqual(projection.attempt_id, ATTEMPT)
        # the projection NEVER carries the raw nonce (v1_request is
        # repr-redacted; the declared fields are fingerprints only)
        self.assertNotIn(NONCE, str(projection))
        # the V1 adapter request was rebuilt from the verified materials
        self.assertIsInstance(projection.v1_request, FinalEvalRequest)
        self.assertEqual(
            projection.v1_request.campaign.campaign_id,
            request.campaign_id,
        )

    def test_request_field_drift_rejected(self) -> None:
        """Mutating ONE actual V2 request field per subtest must fail
        closed before the adapter request is constructed."""
        mutations = {
            "campaign_id": "campaign-other",
            "campaign_sha256": "1" * 64,
            "holdout_id": "holdout-other",
            "holdout_sha256": "2" * 64,
            "nonce_fingerprint": "3" * 64,
            "candidate_freeze_ref": "other-freeze.json",
            "candidate_freeze_sha256": "4" * 64,
            "code_ref": "other-code.py",
            "code_sha256": "5" * 64,
            "execution_spec_ref": "other-spec.json",
            "execution_spec_sha256": "6" * 64,
            "features_ref": "other-features.json",
            "features_sha256": "7" * 64,
            "model": "other-model",
            "model_sha256": "8" * 64,
            "threshold_ref": "other-threshold.json",
            "threshold_sha256": "9" * 64,
            "roster_ref": "other-roster.json",
            "roster_sha256": "a" * 64,
            "generation": "other-generation",
            "generation_sha256": "b" * 64,
            "actor_id": "other-actor",
            "actor_type": "automation",
            "invocation_id": "other-invocation",
            "authority_plan_hash": "c" * 64,
            "identity_scope_hash": "d" * 64,
            "identity_instruction_policy_hash": "e" * 64,
            "attempt_id": "other-attempt",
        }
        for field_name, value in mutations.items():
            with self.subTest(field=field_name):
                request = _valid_request(**{field_name: value})
                with self.assertRaises(FinalEvalRequestRejected):
                    build_evaluator_request_v2(
                        request,
                        _valid_materials(),
                        root_secret=ROOT_SECRET,
                    )

    def test_material_field_drift_rejected(self) -> None:
        """Mutating ONE actual material field per subtest must fail closed
        -- the projection recomputes digests, it never trusts the bundle."""
        mutations = {
            "campaign_id": "campaign-other",
            "campaign_sha256": "1" * 64,
            "holdout_id": "holdout-other",
            "holdout_sha256": "2" * 64,
            "authorization_nonce": "m" * 64,
            "candidate_freeze_ref": "other-freeze.json",
            "code_ref": "other-code.py",
            "code_sha256": "5" * 64,
            "execution_spec_ref": "other-spec.json",
            "execution_spec_sha256": "6" * 64,
            "features_ref": "other-features.json",
            "features_sha256": "7" * 64,
            "model_id": "other-model",
            "model_sha256": "8" * 64,
            "threshold_ref": "other-threshold.json",
            "threshold_sha256": "9" * 64,
            "roster_ref": "other-roster.json",
            "roster_sha256": "a" * 64,
            "generation_id": "other-generation",
            "generation_sha256": "b" * 64,
            "actor": Actor("other-actor", "human", "final-eval-op-cr009"),
            "attempt_id": "other-attempt",
        }
        for field_name, value in mutations.items():
            with self.subTest(field=field_name):
                materials = _valid_materials(**{field_name: value})
                with self.assertRaises(FinalEvalRequestRejected):
                    build_evaluator_request_v2(
                        _valid_request(),
                        materials,
                        root_secret=ROOT_SECRET,
                    )

    def test_mutated_candidate_set_content_rejected(self) -> None:
        """The candidate SET content is recomputed -- a different candidate
        binding with the old digest cannot pass."""
        materials = _valid_materials(
            candidate_set=(
                _candidate("candidate-a"),
                _candidate("candidate-c"),
            ),
        )
        with self.assertRaises(FinalEvalRequestRejected):
            build_evaluator_request_v2(
                _valid_request(),
                materials,
                root_secret=ROOT_SECRET,
            )

    def test_v1_request_for_different_campaign_rejected(self) -> None:
        """CR-010 F-03: a V1 evaluator request for a DIFFERENT campaign/
        holdout can never be paired with the V2 request."""
        from tests.test_control_plane_final_evaluator import _request

        v1 = _request()  # campaign-final-1 / holdout-final-1
        with self.assertRaisesRegex(
            FinalEvalRequestRejected, "campaign"
        ):
            adapt_evaluator_request_v1_test_only(
                v1,
                _valid_request(),
                root_secret=ROOT_SECRET,
                attempt_id=ATTEMPT,
                identity=AuthorityIdentity(**P8_IDENTITY),
            )
        with self.assertRaisesRegex(FinalEvalRequestRejected, "holdout"):
            adapt_evaluator_request_v1_test_only(
                v1,
                _valid_request(campaign_id="campaign-final-1"),
                root_secret=ROOT_SECRET,
                attempt_id=ATTEMPT,
                identity=AuthorityIdentity(**P8_IDENTITY),
            )

    def test_aligned_v1_request_projection_passes(self) -> None:
        """An ALIGNED V1 request (same identity values) adapts into the
        projection; every declared field is populated."""
        from tests.test_control_plane_final_evaluator import (
            CampaignBinding,
            CodeBinding,
            ExecutionSpecBinding,
            FeatureBinding,
            GenerationBinding,
            HoldoutBinding,
            IdentityBinding,
            ModelBinding,
            RosterBinding,
            ThresholdBinding,
        )

        request = _valid_request()
        execution_spec = _execution_spec()
        roster = _roster()
        candidates = (_candidate("candidate-a"), _candidate("candidate-b"))
        v1 = FinalEvalRequest(
            campaign=CampaignBinding(
                campaign_id=request.campaign_id,
                campaign_sha256=request.campaign_sha256,
            ),
            candidate_set=candidates,
            candidate_set_sha256=_candidate_set_digest(candidates),
            code=CodeBinding(code_sha256=request.code_sha256),
            execution_spec=ExecutionSpecBinding(
                execution_spec=execution_spec,
                execution_spec_sha256=request.execution_spec_sha256,
            ),
            features=FeatureBinding(
                features_sha256=request.features_sha256
            ),
            model=ModelBinding(
                model_id=request.model,
                model_sha256=request.model_sha256,
            ),
            threshold=ThresholdBinding(
                threshold_sha256=request.threshold_sha256
            ),
            roster=RosterBinding(
                roster=roster,
                roster_sha256=request.roster_sha256,
            ),
            generation=GenerationBinding(
                generation_id=request.generation,
                generation_sha256=request.generation_sha256,
            ),
            holdout=HoldoutBinding(
                holdout_id=request.holdout_id,
                holdout_sha256=request.holdout_sha256,
                authorization_nonce=NONCE,
            ),
            actor=Actor(
                request.actor_id,
                request.actor_type,
                request.invocation_id,
            ),
            identity_binding=IdentityBinding(
                plan_hash=request.authority_plan_hash,
                scope_hash=request.identity_scope_hash,
                policy_hash=request.identity_instruction_policy_hash,
            ),
        )
        projection = adapt_evaluator_request_v1_test_only(
            v1,
            request,
            root_secret=ROOT_SECRET,
            attempt_id=ATTEMPT,
            identity=AuthorityIdentity(**P8_IDENTITY),
        )
        self.assertEqual(projection.v2_request_sha256, request.request_sha256)
        self.assertNotIn(NONCE, str(projection))
        self.assertEqual(projection.attempt_id, ATTEMPT)

    def test_raw_nonce_never_appears_in_rejection(self) -> None:
        """The raw nonce never appears in exceptions even when the
        fingerprint drifts."""
        materials = replace(
            _valid_materials(),
            authorization_nonce="z" * 64,
        )
        try:
            build_evaluator_request_v2(
                _valid_request(),
                materials,
                root_secret=ROOT_SECRET,
            )
            self.fail("expected FinalEvalRequestRejected")
        except FinalEvalRequestRejected as error:
            self.assertNotIn("z" * 64, str(error))
            self.assertNotIn(NONCE, str(error))


if __name__ == "__main__":
    unittest.main()
