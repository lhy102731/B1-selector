"""Final evaluation Authority binding (P8R3 T1).

Binds the FinalEval domain layer to the durable Authority v2 contract:
nonce HMAC fingerprints (never raw), global research-plan+holdout
uniqueness, real ticket/lease lineage and CAS state transitions.  V2 wire
contracts never carry raw nonces, caller-chosen outcomes or fake lease ids;
V1 payloads are historical-read only.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .contracts import canonical_json
from .stores import (
    Actor,
    AuthorityIdentity,
    Phase,
    SideEffect,
    _AuthorityStore,
)


FINAL_EVAL_REQUEST_V2 = "control_plane.final_eval_request.v2"
FINAL_EVAL_BINDING_V2 = "control_plane.final_eval_binding.v2"


class FinalEvalAuthorityError(RuntimeError):
    """Base error for final evaluation Authority binding."""


class FinalEvalRequestRejected(FinalEvalAuthorityError):
    """A V2 request failed binding validation."""


class FinalEvalUniquenessRejected(FinalEvalAuthorityError):
    """The same research plan + holdout was already consumed."""


@dataclass(frozen=True, slots=True)
class FinalEvalRequestV2:
    """V2 request bound to Authority-issued identity (no raw nonce).

    CR-010 F-03: the declared identity covers the FULL evaluator lineage
    -- material refs/hashes (candidate freeze, code, execution spec,
    features, model, threshold, roster, generation) and the grant/binding
    plan/scope/instruction-policy/attempt fields.  The new ref/hash and
    identity fields default to empty so legacy constructions remain valid;
    the projection entry point requires exact matches for every declared
    field before any evaluator consume.
    """

    schema_version: str
    research_plan_sha256: str
    campaign_id: str
    campaign_sha256: str
    holdout_id: str
    holdout_sha256: str
    nonce_fingerprint: str
    candidate_freeze_ref: str
    candidate_freeze_sha256: str
    code_sha256: str
    execution_spec_sha256: str
    features_sha256: str
    model: str
    threshold: str
    roster_sha256: str
    generation: str
    actor_id: str
    actor_type: str
    invocation_id: str
    authority_plan_hash: str
    code_ref: str = ""
    execution_spec_ref: str = ""
    features_ref: str = ""
    model_sha256: str = ""
    threshold_ref: str = ""
    threshold_sha256: str = ""
    roster_ref: str = ""
    generation_sha256: str = ""
    identity_scope_hash: str = ""
    identity_instruction_policy_hash: str = ""
    attempt_id: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != FINAL_EVAL_REQUEST_V2:
            raise FinalEvalRequestRejected("unsupported request schema")
        for field_name in (
            "research_plan_sha256",
            "campaign_sha256",
            "holdout_sha256",
            "nonce_fingerprint",
            "candidate_freeze_sha256",
            "code_sha256",
            "execution_spec_sha256",
            "features_sha256",
            "roster_sha256",
            "authority_plan_hash",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(char not in "0123456789abcdef" for char in value)
            ):
                raise FinalEvalRequestRejected(
                    f"{field_name} must be a lowercase SHA-256 digest"
                )
        for field_name in (
            "model_sha256",
            "threshold_sha256",
            "generation_sha256",
            "identity_scope_hash",
            "identity_instruction_policy_hash",
        ):
            value = getattr(self, field_name)
            if value == "":
                continue
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(char not in "0123456789abcdef" for char in value)
            ):
                raise FinalEvalRequestRejected(
                    f"{field_name} must be a lowercase SHA-256 digest"
                )
        if not isinstance(self.model, str) or not self.model.strip():
            raise FinalEvalRequestRejected("model must be a non-empty string")
        if not isinstance(self.actor_type, str) or self.actor_type not in {
            "human",
            "automation",
        }:
            raise FinalEvalRequestRejected("actor_type must be human or automation")

    def to_payload(self) -> dict[str, object]:
        payload = {
            "schema_version": self.schema_version,
            "research_plan_sha256": self.research_plan_sha256,
            "campaign_id": self.campaign_id,
            "campaign_sha256": self.campaign_sha256,
            "holdout_id": self.holdout_id,
            "holdout_sha256": self.holdout_sha256,
            "nonce_fingerprint": self.nonce_fingerprint,
            "candidate_freeze_ref": self.candidate_freeze_ref,
            "candidate_freeze_sha256": self.candidate_freeze_sha256,
            "code_ref": self.code_ref,
            "code_sha256": self.code_sha256,
            "execution_spec_ref": self.execution_spec_ref,
            "execution_spec_sha256": self.execution_spec_sha256,
            "features_ref": self.features_ref,
            "features_sha256": self.features_sha256,
            "model": self.model,
            "model_sha256": self.model_sha256,
            "threshold": self.threshold,
            "threshold_ref": self.threshold_ref,
            "threshold_sha256": self.threshold_sha256,
            "roster_ref": self.roster_ref,
            "roster_sha256": self.roster_sha256,
            "generation": self.generation,
            "generation_sha256": self.generation_sha256,
            "actor_id": self.actor_id,
            "actor_type": self.actor_type,
            "invocation_id": self.invocation_id,
            "authority_plan_hash": self.authority_plan_hash,
            "identity_scope_hash": self.identity_scope_hash,
            "identity_instruction_policy_hash": (
                self.identity_instruction_policy_hash
            ),
            "attempt_id": self.attempt_id,
        }
        return payload

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(
            b"control_plane.final_eval_request.v2\0"
            + canonical_json(self.to_payload()).encode("utf-8")
        ).hexdigest()


def _nonce_fingerprint(root_secret: str, nonce: str) -> str:
    """HMAC fingerprint of the nonce (raw nonce never persisted)."""
    if not isinstance(nonce, str) or not nonce:
        raise FinalEvalRequestRejected("nonce must be a non-empty string")
    return hashlib.sha256(
        b"control_plane.final_eval_nonce.v1\0"
        + root_secret.encode("utf-8")
        + b"\0"
        + nonce.encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class FinalEvalBindingV2:
    """Durable binding receipt with real ticket/lease lineage.

    CR-010 C0 (Phase B): ``consumption`` carries the immutable begin-time
    holdout consume receipt (durable identifiers only) committed in the
    same Authority transaction as the binding.
    """

    ticket_id: str
    request_sha256: str
    authority_plan_hash: str
    research_plan_sha256: str
    campaign_id: str
    holdout_id: str
    nonce_fingerprint: str
    saga_state: str
    saga_version: int
    consumption: object | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": FINAL_EVAL_BINDING_V2,
            "ticket_id": self.ticket_id,
            "request_sha256": self.request_sha256,
            "authority_plan_hash": self.authority_plan_hash,
            "research_plan_sha256": self.research_plan_sha256,
            "campaign_id": self.campaign_id,
            "holdout_id": self.holdout_id,
            "nonce_fingerprint": self.nonce_fingerprint,
            "saga_state": self.saga_state,
            "saga_version": self.saga_version,
        }
        if self.consumption is not None:
            payload["consumption"] = self.consumption.to_payload()
        return payload


class AuthorityFinalEvalBroker:
    """Broker that binds V2 requests to the durable Authority contract.

    Validates the request against the grant lineage and the sealed Authority
    begin CAS, then returns the real ticket/lease binding.  Global uniqueness
    (research plan + holdout, and nonce fingerprint) is enforced by the
    Authority table constraints; raw nonces never enter the wire.
    """

    def __init__(
        self,
        *,
        authority: _AuthorityStore,
        grant: object,
        attempt_id: str,
        identity: AuthorityIdentity,
    ) -> None:
        if not isinstance(authority, _AuthorityStore):
            raise TypeError("authority must be an _AuthorityStore")
        self._authority = authority
        self._grant = grant
        self._attempt_id = attempt_id
        self._identity = identity

    def bind(
        self,
        *,
        request: FinalEvalRequestV2,
        nonce: str,
        actor: Actor,
        idempotency_key: str,
        task_spec_ref: str,
        task_spec_sha256: str,
    ) -> FinalEvalBindingV2:
        """Bind one V2 request to a real Authority ticket/lease."""
        if not isinstance(request, FinalEvalRequestV2):
            raise FinalEvalRequestRejected("request must be FinalEvalRequestV2")
        if not isinstance(actor, Actor):
            raise FinalEvalRequestRejected("actor is invalid")
        if request.actor_id != actor.actor_id or request.actor_type != actor.actor_type:
            raise FinalEvalRequestRejected("request actor does not match grant actor")
        if request.authority_plan_hash != self._identity.plan_hash:
            raise FinalEvalRequestRejected(
                "authority_plan_hash does not match grant lineage"
            )
        if self._grant is None:
            raise FinalEvalAuthorityError("no P8 grant is available for binding")
        # CR-010 B-03: the FinalEval bind requires a STRICT P8 lineage.
        grant = self._grant
        if grant.phase is not Phase.P8:
            raise FinalEvalRequestRejected(
                "FinalEval binding requires a P8 grant, got "
                + grant.phase.value
            )
        if (
            grant.actor.actor_id != request.actor_id
            or grant.actor.actor_type != request.actor_type
            or grant.actor.invocation_id != request.invocation_id
        ):
            raise FinalEvalRequestRejected(
                "grant actor does not match the request actor"
            )
        if (
            grant.identity.plan_hash != request.authority_plan_hash
            or grant.identity.scope_hash != self._identity.scope_hash
            or grant.identity.instruction_policy_hash
            != self._identity.instruction_policy_hash
        ):
            raise FinalEvalRequestRejected(
                "grant identity does not match the broker identity"
            )
        if grant.attempt_id != self._attempt_id:
            raise FinalEvalRequestRejected(
                "grant attempt does not match the FinalEval attempt"
            )
        fingerprint = _nonce_fingerprint(
            self._authority._root_secret._reveal_for_authority_check(),
            nonce,
        )
        if fingerprint != request.nonce_fingerprint:
            raise FinalEvalRequestRejected(
                "nonce fingerprint does not match the request binding"
            )
        # Bind through the sealed Authority CAS using the injected grant.
        # CR-010 F-02 (functional closure): the COMPLETE V2 request identity
        # (every material ref/hash, identity/scope/policy, attempt, schema
        # and the source request digest) is committed into the binding's
        # durable request identity, so the runtime can fail closed on ANY
        # single-field drift -- a changed material hash can never replay an
        # earlier terminal result.
        receipt = self._authority._begin_final_eval_binding(
            self._grant,
            research_plan_sha256=request.research_plan_sha256,
            campaign_id=request.campaign_id,
            campaign_sha256=request.campaign_sha256,
            holdout_id=request.holdout_id,
            holdout_sha256=request.holdout_sha256,
            nonce=nonce,
            idempotency_key=idempotency_key,
            task_spec_ref=task_spec_ref,
            task_spec_sha256=task_spec_sha256,
            candidate_freeze_sha256=request.candidate_freeze_sha256,
            candidate_freeze_ref=request.candidate_freeze_ref,
            code_sha256=request.code_sha256,
            code_ref=request.code_ref,
            execution_spec_sha256=request.execution_spec_sha256,
            execution_spec_ref=request.execution_spec_ref,
            features_sha256=request.features_sha256,
            features_ref=request.features_ref,
            model=request.model,
            model_sha256=request.model_sha256,
            threshold=request.threshold,
            threshold_ref=request.threshold_ref,
            threshold_sha256=request.threshold_sha256,
            roster_sha256=request.roster_sha256,
            roster_ref=request.roster_ref,
            generation=request.generation,
            generation_sha256=request.generation_sha256,
            actor_id=request.actor_id,
            actor_type=request.actor_type,
            invocation_id=request.invocation_id,
            identity_scope_hash=request.identity_scope_hash,
            identity_instruction_policy_hash=(
                request.identity_instruction_policy_hash
            ),
            request_schema=request.schema_version,
            request_digest=request.request_sha256,
        )
        return FinalEvalBindingV2(
            ticket_id=receipt.binding.ticket_id,
            request_sha256=receipt.binding.request_sha256,
            authority_plan_hash=receipt.binding.authority_plan_hash,
            research_plan_sha256=receipt.binding.research_plan_sha256,
            campaign_id=receipt.binding.campaign_id,
            holdout_id=receipt.binding.holdout_id,
            nonce_fingerprint=receipt.binding.nonce_fingerprint,
            saga_state=receipt.binding.saga_state,
            saga_version=receipt.binding.saga_version,
            consumption=receipt.consumption,
        )


__all__ = [
    "AuthorityFinalEvalBroker",
    "FINAL_EVAL_BINDING_V2",
    "FINAL_EVAL_REQUEST_V2",
    "FinalEvalAuthorityError",
    "FinalEvalBindingV2",
    "FinalEvalRequestRejected",
    "FinalEvalRequestV2",
    "FinalEvalUniquenessRejected",
    "_nonce_fingerprint",
]
