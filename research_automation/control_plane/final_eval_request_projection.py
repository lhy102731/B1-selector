"""V2 evaluator request projection (CR-010 F-03).

The ONLY bridge from the durable V2 request identity to the evaluator
adapter request.  ``build_evaluator_request_v2`` recomputes every material
digest, carries the source V2 ``request_sha256``, populates every declared
identity field and rejects ANY mismatch BEFORE the adapter request is
constructed -- ``evaluate_v2`` can never consume under a drifted identity.
The raw nonce exists ONLY in memory while deriving the fingerprints; it
never appears in payloads, logs, exceptions or evidence.

The test-only V1 adapter (``adapt_evaluator_request_v1_test_only``) wraps
a caller-supplied V1 request for unit fixtures; it validates every
cross-identity field against the V2 request so a V1 request for a
different campaign/holdout can never be paired with the V2 request.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .contracts import canonical_sha256
from .final_eval_authority import (
    FINAL_EVAL_REQUEST_V2,
    FinalEvalRequestRejected,
    FinalEvalRequestV2,
    _nonce_fingerprint,
)
from .final_evaluator import (
    CampaignBinding,
    CandidateBinding,
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
from .stores import (
    Actor,
    AuthorityIdentity,
    _final_eval_nonce_fingerprint as _durable_nonce_fingerprint,
)

if TYPE_CHECKING:  # pragma: no cover -- runtime imports stay lazy
    from research_automation.foundations.protocols import ExecutionSpec
    from research_automation.control_plane.campaign_roster import RosterManifest


def _candidate_set_sha256(candidates: tuple[CandidateBinding, ...]) -> str:
    """Recomputed digest of the frozen candidate set (never caller-held)."""
    return canonical_sha256(
        tuple(
            (candidate.candidate_id, candidate.candidate_sha256)
            for candidate in candidates
        )
    )


@dataclass(frozen=True, slots=True)
class FinalEvalMaterialBundle:
    """Sealed evaluator materials behind one V2 request identity.

    The actual frozen artifacts; every digest is recomputed from these
    objects by the projection.  The raw holdout authorization nonce exists
    ONLY in memory -- it is consumed to derive the fingerprints and never
    stored on the projection or any payload.
    """

    campaign_id: str
    campaign_sha256: str
    holdout_id: str
    holdout_sha256: str
    # the raw holdout authorization nonce NEVER appears in payloads, logs
    # or reprs -- it is consumed only to derive the fingerprints
    authorization_nonce: str = field(repr=False)
    candidate_freeze_ref: str
    candidate_set: tuple[CandidateBinding, ...]
    code_ref: str
    code_sha256: str
    execution_spec: object
    execution_spec_ref: str
    execution_spec_sha256: str
    features_ref: str
    features_sha256: str
    model_id: str
    model_sha256: str
    threshold_ref: str
    threshold_sha256: str
    roster: object
    roster_ref: str
    roster_sha256: str
    generation_id: str
    generation_sha256: str
    actor: Actor
    identity: AuthorityIdentity
    attempt_id: str


@dataclass(frozen=True, slots=True)
class EvaluatorRequestProjectionV2:
    """The evaluator-facing projection of ONE V2 request.

    Carries the V1 adapter request (built from verified materials), the
    source V2 request digest -- the authoritative result identity -- and
    every declared identity field for cross-layer validation.  Never
    carries the raw nonce in its repr/payload fields; both fingerprints
    are derived in memory.  ``v1_request`` is repr-redacted because the V1
    adapter contract historically embeds the nonce; the projection's own
    surface exposes only fingerprints.
    """

    v1_request: FinalEvalRequest = field(repr=False)
    v2_request_sha256: str
    campaign_id: str
    campaign_sha256: str
    holdout_id: str
    holdout_sha256: str
    nonce_fingerprint: str
    durable_nonce_fingerprint: str
    candidate_freeze_ref: str
    candidate_freeze_sha256: str
    code_ref: str
    code_sha256: str
    execution_spec_ref: str
    execution_spec_sha256: str
    features_ref: str
    features_sha256: str
    model_id: str
    model_sha256: str
    threshold_ref: str
    threshold_sha256: str
    roster_ref: str
    roster_sha256: str
    generation_id: str
    generation_sha256: str
    actor_id: str
    actor_type: str
    invocation_id: str
    authority_plan_hash: str
    identity_scope_hash: str
    identity_instruction_policy_hash: str
    attempt_id: str


def _build_projection(
    *,
    v1_request: FinalEvalRequest,
    request: FinalEvalRequestV2,
    nonce_fingerprint: str,
    durable_fingerprint: str,
    candidate_freeze_ref: str,
    code_ref: str,
    execution_spec_ref: str,
    features_ref: str,
    threshold_ref: str,
    roster_ref: str,
    model_sha256: str,
    threshold_sha256: str,
    generation_sha256: str,
    identity: AuthorityIdentity,
    attempt_id: str,
) -> EvaluatorRequestProjectionV2:
    return EvaluatorRequestProjectionV2(
        v1_request=v1_request,
        v2_request_sha256=request.request_sha256,
        campaign_id=str(v1_request.campaign.campaign_id),
        campaign_sha256=str(v1_request.campaign.campaign_sha256),
        holdout_id=str(v1_request.holdout.holdout_id),
        holdout_sha256=str(v1_request.holdout.holdout_sha256),
        nonce_fingerprint=nonce_fingerprint,
        durable_nonce_fingerprint=durable_fingerprint,
        candidate_freeze_ref=candidate_freeze_ref,
        candidate_freeze_sha256=str(v1_request.candidate_set_sha256),
        code_ref=code_ref,
        code_sha256=str(v1_request.code.code_sha256),
        execution_spec_ref=execution_spec_ref,
        execution_spec_sha256=str(
            v1_request.execution_spec.execution_spec_sha256
        ),
        features_ref=features_ref,
        features_sha256=str(v1_request.features.features_sha256),
        model_id=str(v1_request.model.model_id),
        model_sha256=model_sha256,
        threshold_ref=threshold_ref,
        threshold_sha256=threshold_sha256,
        roster_ref=roster_ref,
        roster_sha256=str(v1_request.roster.roster_sha256),
        generation_id=str(v1_request.generation.generation_id),
        generation_sha256=generation_sha256,
        actor_id=str(v1_request.actor.actor_id),
        actor_type=str(v1_request.actor.actor_type),
        invocation_id=str(v1_request.actor.invocation_id),
        authority_plan_hash=str(v1_request.identity_binding.plan_hash),
        identity_scope_hash=str(identity.scope_hash),
        identity_instruction_policy_hash=str(
            identity.instruction_policy_hash
        ),
        attempt_id=attempt_id,
    )


def build_evaluator_request_v2(
    request: FinalEvalRequestV2,
    materials: FinalEvalMaterialBundle,
    *,
    root_secret: str,
) -> EvaluatorRequestProjectionV2:
    """The ONLY production projection entry point (CR-010 F-03).

    Recomputes every material digest, carries the source V2
    ``request_sha256``, populates every declared identity field and
    rejects any mismatch BEFORE the adapter request is constructed.  The
    raw nonce is used only in memory to derive the fingerprints.
    """
    if not isinstance(request, FinalEvalRequestV2):
        raise FinalEvalRequestRejected("request must be FinalEvalRequestV2")
    if not isinstance(materials, FinalEvalMaterialBundle):
        raise FinalEvalRequestRejected(
            "materials must be a FinalEvalMaterialBundle"
        )
    if not isinstance(materials.actor, Actor):
        raise FinalEvalRequestRejected("materials actor is invalid")
    if not isinstance(materials.identity, AuthorityIdentity):
        raise FinalEvalRequestRejected("materials identity is invalid")
    # --- recompute every material digest (never caller-held values) -----
    try:
        candidate_set_sha256 = _candidate_set_sha256(materials.candidate_set)
        execution_spec_sha256 = canonical_sha256(
            materials.execution_spec.model_dump(mode="json")
        )
    except (AttributeError, TypeError, ValueError) as error:
        raise FinalEvalRequestRejected(
            "material digests are not recomputable"
        ) from error
    roster_sha256 = str(
        getattr(materials.roster, "manifest_sha256", "")
    )
    if not roster_sha256 or len(roster_sha256) != 64:
        raise FinalEvalRequestRejected("material roster digest is invalid")
    if execution_spec_sha256 != materials.execution_spec_sha256:
        raise FinalEvalRequestRejected(
            "execution-spec digest does not match the material content"
        )
    if roster_sha256 != materials.roster_sha256:
        raise FinalEvalRequestRejected(
            "roster digest does not match the material content"
        )
    # --- derive the fingerprints IN MEMORY (raw nonce never leaves) -----
    nonce_fingerprint = _nonce_fingerprint(
        root_secret,
        materials.authorization_nonce,
    )
    durable_fingerprint = _durable_nonce_fingerprint(
        root_secret,
        materials.authorization_nonce,
    )
    mismatches: list[str] = []
    if materials.campaign_id != request.campaign_id:
        mismatches.append("campaign_id")
    if materials.campaign_sha256 != request.campaign_sha256:
        mismatches.append("campaign_sha256")
    if materials.holdout_id != request.holdout_id:
        mismatches.append("holdout_id")
    if materials.holdout_sha256 != request.holdout_sha256:
        mismatches.append("holdout_sha256")
    if nonce_fingerprint != request.nonce_fingerprint:
        mismatches.append("nonce_fingerprint")
    if materials.candidate_freeze_ref != request.candidate_freeze_ref:
        mismatches.append("candidate_freeze_ref")
    if candidate_set_sha256 != request.candidate_freeze_sha256:
        mismatches.append("candidate_freeze_sha256")
    if materials.code_ref != request.code_ref:
        mismatches.append("code_ref")
    if materials.code_sha256 != request.code_sha256:
        mismatches.append("code_sha256")
    if materials.execution_spec_ref != request.execution_spec_ref:
        mismatches.append("execution_spec_ref")
    if execution_spec_sha256 != request.execution_spec_sha256:
        mismatches.append("execution_spec_sha256")
    if materials.features_ref != request.features_ref:
        mismatches.append("features_ref")
    if materials.features_sha256 != request.features_sha256:
        mismatches.append("features_sha256")
    if materials.model_id != request.model:
        mismatches.append("model_id")
    if materials.model_sha256 != request.model_sha256:
        mismatches.append("model_sha256")
    if materials.threshold_ref != request.threshold_ref:
        mismatches.append("threshold_ref")
    if materials.threshold_sha256 != request.threshold_sha256:
        mismatches.append("threshold_sha256")
    if materials.roster_ref != request.roster_ref:
        mismatches.append("roster_ref")
    if roster_sha256 != request.roster_sha256:
        mismatches.append("roster_sha256")
    if materials.generation_id != request.generation:
        mismatches.append("generation_id")
    if materials.generation_sha256 != request.generation_sha256:
        mismatches.append("generation_sha256")
    if materials.actor.actor_id != request.actor_id:
        mismatches.append("actor_id")
    if materials.actor.actor_type != request.actor_type:
        mismatches.append("actor_type")
    if materials.actor.invocation_id != request.invocation_id:
        mismatches.append("invocation_id")
    if materials.identity.plan_hash != request.authority_plan_hash:
        mismatches.append("authority_plan_hash")
    if materials.identity.scope_hash != request.identity_scope_hash:
        mismatches.append("identity_scope_hash")
    if (
        materials.identity.instruction_policy_hash
        != request.identity_instruction_policy_hash
    ):
        mismatches.append("identity_instruction_policy_hash")
    if materials.attempt_id != request.attempt_id:
        mismatches.append("attempt_id")
    if mismatches:
        raise FinalEvalRequestRejected(
            "evaluator materials do not match the V2 request identity: "
            + "; ".join(sorted(mismatches))
        )
    # --- construct the adapter request ONLY after every check passed ----
    v1_request = FinalEvalRequest(
        campaign=CampaignBinding(
            campaign_id=materials.campaign_id,
            campaign_sha256=materials.campaign_sha256,
        ),
        candidate_set=materials.candidate_set,
        candidate_set_sha256=candidate_set_sha256,
        code=CodeBinding(code_sha256=materials.code_sha256),
        execution_spec=ExecutionSpecBinding(
            execution_spec=materials.execution_spec,
            execution_spec_sha256=execution_spec_sha256,
        ),
        features=FeatureBinding(features_sha256=materials.features_sha256),
        model=ModelBinding(
            model_id=materials.model_id,
            model_sha256=materials.model_sha256,
        ),
        threshold=ThresholdBinding(
            threshold_sha256=materials.threshold_sha256
        ),
        roster=RosterBinding(
            roster=materials.roster,
            roster_sha256=roster_sha256,
        ),
        generation=GenerationBinding(
            generation_id=materials.generation_id,
            generation_sha256=materials.generation_sha256,
        ),
        holdout=HoldoutBinding(
            holdout_id=materials.holdout_id,
            holdout_sha256=materials.holdout_sha256,
            authorization_nonce=materials.authorization_nonce,
        ),
        actor=materials.actor,
        identity_binding=IdentityBinding(
            plan_hash=materials.identity.plan_hash,
            scope_hash=materials.identity.scope_hash,
            policy_hash=materials.identity.instruction_policy_hash,
        ),
    )
    return _build_projection(
        v1_request=v1_request,
        request=request,
        nonce_fingerprint=nonce_fingerprint,
        durable_fingerprint=durable_fingerprint,
        candidate_freeze_ref=materials.candidate_freeze_ref,
        code_ref=materials.code_ref,
        execution_spec_ref=materials.execution_spec_ref,
        features_ref=materials.features_ref,
        threshold_ref=materials.threshold_ref,
        roster_ref=materials.roster_ref,
        model_sha256=materials.model_sha256,
        threshold_sha256=materials.threshold_sha256,
        generation_sha256=materials.generation_sha256,
        identity=materials.identity,
        attempt_id=materials.attempt_id,
    )


def adapt_evaluator_request_v1_test_only(
    v1_request: FinalEvalRequest,
    request: FinalEvalRequestV2,
    *,
    root_secret: str,
    attempt_id: str,
    identity: AuthorityIdentity,
) -> EvaluatorRequestProjectionV2:
    """Explicitly TEST-ONLY adapter for caller-supplied V1 requests.

    Wraps the V1 request into a projection AFTER validating every
    cross-identity field against the V2 request: a V1 request for a
    different campaign/holdout (or any other identity drift) fails closed
    before the evaluator consumes anything.  Production composition never
    uses this adapter -- it always resolves materials.
    """
    if not isinstance(v1_request, FinalEvalRequest):
        raise FinalEvalRequestRejected(
            "evaluator_request must be a FinalEvalRequest"
        )
    if not isinstance(identity, AuthorityIdentity):
        raise FinalEvalRequestRejected("identity must be an AuthorityIdentity")
    mismatches: list[str] = []
    if v1_request.campaign.campaign_id != request.campaign_id:
        mismatches.append("campaign_id")
    if v1_request.campaign.campaign_sha256 != request.campaign_sha256:
        mismatches.append("campaign_sha256")
    if v1_request.holdout.holdout_id != request.holdout_id:
        mismatches.append("holdout_id")
    if v1_request.holdout.holdout_sha256 != request.holdout_sha256:
        mismatches.append("holdout_sha256")
    if v1_request.candidate_set_sha256 != request.candidate_freeze_sha256:
        mismatches.append("candidate_freeze_sha256")
    if v1_request.code.code_sha256 != request.code_sha256:
        mismatches.append("code_sha256")
    if (
        v1_request.execution_spec.execution_spec_sha256
        != request.execution_spec_sha256
    ):
        mismatches.append("execution_spec_sha256")
    if v1_request.features.features_sha256 != request.features_sha256:
        mismatches.append("features_sha256")
    if v1_request.model.model_id != request.model:
        mismatches.append("model_id")
    if v1_request.roster.roster_sha256 != request.roster_sha256:
        mismatches.append("roster_sha256")
    if v1_request.generation.generation_id != request.generation:
        mismatches.append("generation_id")
    if v1_request.actor.actor_id != request.actor_id:
        mismatches.append("actor_id")
    if v1_request.actor.actor_type != request.actor_type:
        mismatches.append("actor_type")
    if v1_request.actor.invocation_id != request.invocation_id:
        mismatches.append("invocation_id")
    if v1_request.identity_binding.plan_hash != request.authority_plan_hash:
        mismatches.append("authority_plan_hash")
    nonce_fingerprint = _nonce_fingerprint(
        root_secret,
        v1_request.holdout.authorization_nonce,
    )
    if nonce_fingerprint != request.nonce_fingerprint:
        mismatches.append("nonce_fingerprint")
    if mismatches:
        raise FinalEvalRequestRejected(
            "V1 evaluator request does not match the V2 request identity: "
            + "; ".join(sorted(mismatches))
        )
    durable_fingerprint = _durable_nonce_fingerprint(
        root_secret,
        v1_request.holdout.authorization_nonce,
    )
    return _build_projection(
        v1_request=v1_request,
        request=request,
        nonce_fingerprint=nonce_fingerprint,
        durable_fingerprint=durable_fingerprint,
        candidate_freeze_ref=request.candidate_freeze_ref,
        code_ref="",
        execution_spec_ref="",
        features_ref="",
        threshold_ref="",
        roster_ref="",
        model_sha256=str(v1_request.model.model_sha256),
        threshold_sha256=str(v1_request.threshold.threshold_sha256),
        generation_sha256=str(v1_request.generation.generation_sha256),
        identity=identity,
        attempt_id=attempt_id,
    )


__all__ = [
    "EvaluatorRequestProjectionV2",
    "FinalEvalMaterialBundle",
    "adapt_evaluator_request_v1_test_only",
    "build_evaluator_request_v2",
]
