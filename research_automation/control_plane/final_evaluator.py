"""P8 Trusted Final Evaluator request contract.

T2 slice: FinalEvalRequest binds the Campaign, candidate set, code,
ExecutionSpec, features, model, threshold, roster, generation, holdout and
actor together with every identity hash.  Validation is fail-closed: missing,
malformed or mismatched hashes, unfrozen candidates and non-operator actors
are all rejected before any evaluation can start.

Later P8 slices extend this module with the consume-once AuthorityBroker
(T3), the low-privilege TrustedEvaluator adapter (T4) and terminal audit
closure (T5).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from research_automation.control_plane.campaign_roster import RosterManifest
from research_automation.control_plane.contracts import (
    Actor,
    IdentityBinding,
    canonical_sha256,
)
from research_automation.foundations.protocols import ExecutionSpec


FINAL_EVAL_REQUEST_SCHEMA = "control_plane.final_eval_request.v1"

_HEX64_RE = re.compile(r"[0-9a-f]{64}")
_ALLOWED_REQUEST_ACTOR_TYPES = frozenset({"human", "automation"})


class FinalEvalRequestError(ValueError):
    """Base error for fail-closed final evaluation request validation."""


class FinalEvalBindingError(FinalEvalRequestError):
    """Raised when a bound identity is missing, malformed or mismatched."""


class FinalEvalActorError(FinalEvalRequestError):
    """Raised when the requesting actor cannot perform a final evaluation."""


class UnfrozenCandidateError(FinalEvalRequestError):
    """Raised when the candidate set contains an unfrozen candidate."""


def _require_sha256(value: str, field_name: str) -> str:
    if not isinstance(value, str) or _HEX64_RE.fullmatch(value) is None:
        raise FinalEvalBindingError(
            f"{field_name} must be a 64-character lowercase SHA-256 hex digest"
        )
    return value


def _require_identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FinalEvalBindingError(f"{field_name} must be a non-empty identifier")
    return value.strip()


@dataclass(frozen=True, slots=True)
class CampaignBinding:
    """Frozen Campaign identity: id plus content/authority hash."""

    campaign_id: str
    campaign_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "campaign_id", _require_identifier(self.campaign_id, "campaign_id"))
        object.__setattr__(self, "campaign_sha256", _require_sha256(self.campaign_sha256, "campaign_sha256"))


@dataclass(frozen=True, slots=True)
class CandidateBinding:
    """One frozen research candidate bound to its content hash."""

    candidate_id: str
    candidate_sha256: str
    frozen: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidate_id", _require_identifier(self.candidate_id, "candidate_id"))
        object.__setattr__(self, "candidate_sha256", _require_sha256(self.candidate_sha256, "candidate_sha256"))
        if not isinstance(self.frozen, bool):
            raise FinalEvalBindingError("candidate frozen flag must be a boolean")


@dataclass(frozen=True, slots=True)
class CodeBinding:
    """Frozen runner/code identity hash."""

    code_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "code_sha256", _require_sha256(self.code_sha256, "code_sha256"))


@dataclass(frozen=True, slots=True)
class ExecutionSpecBinding:
    """Frozen ExecutionSpec bound to its canonical content hash."""

    execution_spec: ExecutionSpec
    execution_spec_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.execution_spec, ExecutionSpec):
            raise FinalEvalBindingError("execution_spec must be an ExecutionSpec")
        expected = canonical_sha256(self.execution_spec.model_dump(mode="json"))
        if self.execution_spec_sha256 != expected:
            raise FinalEvalBindingError(
                "execution_spec_sha256 does not match the frozen ExecutionSpec content"
            )
        object.__setattr__(
            self,
            "execution_spec_sha256",
            _require_sha256(self.execution_spec_sha256, "execution_spec_sha256"),
        )


@dataclass(frozen=True, slots=True)
class FeatureBinding:
    """Frozen feature set identity hash."""

    features_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "features_sha256", _require_sha256(self.features_sha256, "features_sha256"))


@dataclass(frozen=True, slots=True)
class ModelBinding:
    """Frozen model identity: id plus artifact hash."""

    model_id: str
    model_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "model_id", _require_identifier(self.model_id, "model_id"))
        object.__setattr__(self, "model_sha256", _require_sha256(self.model_sha256, "model_sha256"))


@dataclass(frozen=True, slots=True)
class ThresholdBinding:
    """Frozen evaluation threshold identity hash."""

    threshold_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "threshold_sha256", _require_sha256(self.threshold_sha256, "threshold_sha256"))


@dataclass(frozen=True, slots=True)
class RosterBinding:
    """Frozen roster bound to the RosterManifest's own digest."""

    roster: RosterManifest
    roster_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.roster, RosterManifest):
            raise FinalEvalBindingError("roster must be a RosterManifest")
        if self.roster_sha256 != self.roster.manifest_sha256:
            raise FinalEvalBindingError("roster_sha256 does not match the RosterManifest digest")
        object.__setattr__(self, "roster_sha256", _require_sha256(self.roster_sha256, "roster_sha256"))


@dataclass(frozen=True, slots=True)
class GenerationBinding:
    """Frozen data generation identity: id plus manifest hash."""

    generation_id: str
    generation_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "generation_id", _require_identifier(self.generation_id, "generation_id"))
        object.__setattr__(self, "generation_sha256", _require_sha256(self.generation_sha256, "generation_sha256"))


@dataclass(frozen=True, slots=True)
class HoldoutBinding:
    """Final Holdout identity and one-time authorization nonce."""

    holdout_id: str
    holdout_sha256: str
    authorization_nonce: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "holdout_id", _require_identifier(self.holdout_id, "holdout_id"))
        object.__setattr__(self, "holdout_sha256", _require_sha256(self.holdout_sha256, "holdout_sha256"))
        object.__setattr__(
            self,
            "authorization_nonce",
            _require_sha256(self.authorization_nonce, "authorization_nonce"),
        )


@dataclass(frozen=True, slots=True)
class FinalEvalRequest:
    """Immutable, self-hashed request for the one-time final evaluation.

    Every identity is bound at construction; the derived ``request_sha256``
    is the canonical digest of the complete request payload.  Unfrozen
    candidates, wrong hashes and non-operator actors are rejected.
    """

    campaign: CampaignBinding
    candidate_set: tuple[CandidateBinding, ...]
    candidate_set_sha256: str
    code: CodeBinding
    execution_spec: ExecutionSpecBinding
    features: FeatureBinding
    model: ModelBinding
    threshold: ThresholdBinding
    roster: RosterBinding
    generation: GenerationBinding
    holdout: HoldoutBinding
    actor: Actor
    identity_binding: IdentityBinding
    request_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name, binding in (
            ("campaign", self.campaign),
            ("code", self.code),
            ("execution_spec", self.execution_spec),
            ("features", self.features),
            ("model", self.model),
            ("threshold", self.threshold),
            ("roster", self.roster),
            ("generation", self.generation),
            ("holdout", self.holdout),
        ):
            if not isinstance(binding, tuple(_BINDING_TYPES_BY_FIELD[field_name])):
                raise FinalEvalBindingError(f"{field_name} binding type is invalid")
        if not isinstance(self.candidate_set, tuple) or not self.candidate_set:
            raise FinalEvalBindingError("candidate_set must be a non-empty tuple")
        if not all(isinstance(candidate, CandidateBinding) for candidate in self.candidate_set):
            raise FinalEvalBindingError("candidate_set must contain only CandidateBinding values")
        candidate_ids = [candidate.candidate_id for candidate in self.candidate_set]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise FinalEvalBindingError("candidate_set must not contain duplicate candidate ids")
        if candidate_ids != sorted(candidate_ids):
            raise FinalEvalBindingError("candidate_set must be sorted by candidate id")
        if not all(candidate.frozen for candidate in self.candidate_set):
            raise UnfrozenCandidateError("final evaluation requires a fully frozen candidate set")
        expected_set_hash = canonical_sha256(
            tuple(
                (candidate.candidate_id, candidate.candidate_sha256)
                for candidate in self.candidate_set
            )
        )
        if self.candidate_set_sha256 != expected_set_hash:
            raise FinalEvalBindingError("candidate_set_sha256 does not match the bound candidate set")
        object.__setattr__(
            self,
            "candidate_set_sha256",
            _require_sha256(self.candidate_set_sha256, "candidate_set_sha256"),
        )
        if not isinstance(self.actor, Actor):
            raise FinalEvalActorError("actor must be an Actor")
        if self.actor.actor_type not in _ALLOWED_REQUEST_ACTOR_TYPES:
            raise FinalEvalActorError(
                "final evaluation may only be requested by a human operator or approved automation"
            )
        if not isinstance(self.identity_binding, IdentityBinding):
            raise FinalEvalBindingError("identity_binding must be an IdentityBinding")
        object.__setattr__(self, "request_sha256", canonical_sha256(self.to_payload()))

    def to_payload(self) -> dict[str, Any]:
        """Return the deterministic JSON-safe payload without the derived hash."""
        return {
            "schema_version": FINAL_EVAL_REQUEST_SCHEMA,
            "campaign": {
                "campaign_id": self.campaign.campaign_id,
                "campaign_sha256": self.campaign.campaign_sha256,
            },
            "candidate_set": [
                {
                    "candidate_id": candidate.candidate_id,
                    "candidate_sha256": candidate.candidate_sha256,
                    "frozen": candidate.frozen,
                }
                for candidate in self.candidate_set
            ],
            "candidate_set_sha256": self.candidate_set_sha256,
            "code_sha256": self.code.code_sha256,
            "execution_spec_sha256": self.execution_spec.execution_spec_sha256,
            "features_sha256": self.features.features_sha256,
            "model": {
                "model_id": self.model.model_id,
                "model_sha256": self.model.model_sha256,
            },
            "threshold_sha256": self.threshold.threshold_sha256,
            "roster_sha256": self.roster.roster_sha256,
            "generation": {
                "generation_id": self.generation.generation_id,
                "generation_sha256": self.generation.generation_sha256,
            },
            "holdout": {
                "holdout_id": self.holdout.holdout_id,
                "holdout_sha256": self.holdout.holdout_sha256,
                "authorization_nonce": self.holdout.authorization_nonce,
            },
            "actor": {
                "actor_id": self.actor.actor_id,
                "actor_type": self.actor.actor_type,
                "invocation_id": self.actor.invocation_id,
            },
            "identity_binding": {
                "plan_hash": self.identity_binding.plan_hash,
                "scope_hash": self.identity_binding.scope_hash,
                "policy_hash": self.identity_binding.policy_hash,
            },
        }


_BINDING_TYPES_BY_FIELD = {
    "campaign": (CampaignBinding,),
    "code": (CodeBinding,),
    "execution_spec": (ExecutionSpecBinding,),
    "features": (FeatureBinding,),
    "model": (ModelBinding,),
    "threshold": (ThresholdBinding,),
    "roster": (RosterBinding,),
    "generation": (GenerationBinding,),
    "holdout": (HoldoutBinding,),
}


__all__ = [
    "CampaignBinding",
    "CandidateBinding",
    "CodeBinding",
    "ExecutionSpecBinding",
    "FINAL_EVAL_REQUEST_SCHEMA",
    "FeatureBinding",
    "FinalEvalActorError",
    "FinalEvalBindingError",
    "FinalEvalRequest",
    "FinalEvalRequestError",
    "GenerationBinding",
    "HoldoutBinding",
    "ModelBinding",
    "RosterBinding",
    "ThresholdBinding",
    "UnfrozenCandidateError",
]
