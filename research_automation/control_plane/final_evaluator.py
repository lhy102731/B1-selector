"""P8 Trusted Final Evaluator request contract.

T2 slice: FinalEvalRequest binds the Campaign, candidate set, code,
ExecutionSpec, features, model, threshold, roster, generation, holdout and
actor together with every identity hash.  Validation is fail-closed: missing,
malformed or mismatched hashes, unfrozen candidates and non-operator actors
are all rejected before any evaluation can start.

T3 slice: AuthorityBroker consumes the Final Holdout nonce permanently
before returning any handle.  Success, failure, timeout and crash all
consume; nonce replay and crash-after-consume are rejected.  The broker
uses an injectable HoldoutStore backend so tests can verify consume-once
semantics without touching real Authority storage.

T4 slice: TrustedEvaluator opens the consumed Final Holdout only through a
separate low-privilege data-root adapter.  Path traversal and reparse escape
are rejected, prompt/LLM access is denied, and the evaluation result carries
only bounded structured metrics plus repo-relative evidence references.
HoldoutHandle is promoted to a distinct frozen dataclass carrying the lease
capability so OPEN_HOLDOUT can never be exercised by a non-evaluator path.
Terminal audit closure is T5.
"""
from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from research_automation.control_plane import entry_guard as _entry_guard
from research_automation.control_plane.campaign_roster import RosterManifest
from research_automation.control_plane.contracts import (
    Actor,
    IdentityBinding,
    Phase,
    SideEffect,
    canonical_json,
    canonical_sha256,
)
from research_automation.control_plane.final_eval_saga import (
    FinalEvalSagaError,
    derive_outcome,
    derive_worker_result,
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


class ConsumeOnceError(RuntimeError):
    """Base error for AuthorityBroker consume-once operations."""


class ConsumeOnceReplayError(ConsumeOnceError):
    """A nonce was already permanently consumed; retry needs a new operator decision."""


class ConsumeOnceValidationError(ConsumeOnceError, FinalEvalRequestError):
    """The consume request or outcome failed validation."""


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


HOLDOUT_CONSUMED_SCHEMA = "control_plane.holdout_consumed.v1"

_ALLOWED_CONSUME_OUTCOMES = frozenset({"SUCCEEDED", "FAILED", "TIMEOUT", "CRASHED"})


class HoldoutAlreadyConsumedError(ConsumeOnceReplayError):
    """Raised when a holdout authorization_nonce was already permanently consumed."""


@dataclass(frozen=True, slots=True)
class HoldoutConsumed:
    """Immutable, self-hashed receipt proving one Final Holdout nonce was consumed.

    This is the handle returned by the AuthorityBroker after the nonce has
    been permanently recorded.  The ``consumed_sha256`` is the canonical
    digest of the full receipt payload.
    """

    holdout_id: str
    holdout_sha256: str
    authorization_nonce: str
    request_sha256: str
    consumed_at: str
    outcome: str
    consumed_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "holdout_id", _require_identifier(self.holdout_id, "holdout_id"))
        object.__setattr__(self, "holdout_sha256", _require_sha256(self.holdout_sha256, "holdout_sha256"))
        object.__setattr__(
            self,
            "authorization_nonce",
            _require_sha256(self.authorization_nonce, "authorization_nonce"),
        )
        object.__setattr__(
            self,
            "request_sha256",
            _require_sha256(self.request_sha256, "request_sha256"),
        )
        if not isinstance(self.consumed_at, str) or not self.consumed_at.strip():
            raise FinalEvalRequestError("consumed_at must be a non-empty string")
        try:
            parsed = datetime.fromisoformat(self.consumed_at.replace("Z", "+00:00"))
        except ValueError as error:
            raise FinalEvalRequestError("consumed_at must be a valid ISO-8601 timestamp") from error
        if parsed.tzinfo is None or self.consumed_at != parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"):
            raise FinalEvalRequestError("consumed_at must be a canonical UTC ISO-8601 timestamp ending in Z")
        if self.outcome not in _ALLOWED_CONSUME_OUTCOMES:
            raise FinalEvalRequestError(
                f"outcome must be one of {sorted(_ALLOWED_CONSUME_OUTCOMES)}"
            )
        object.__setattr__(self, "consumed_sha256", canonical_sha256(self.to_payload()))

    def to_payload(self) -> dict[str, Any]:
        """Return the deterministic JSON-safe payload without the derived hash."""
        return {
            "schema_version": HOLDOUT_CONSUMED_SCHEMA,
            "holdout_id": self.holdout_id,
            "holdout_sha256": self.holdout_sha256,
            "authorization_nonce": self.authorization_nonce,
            "request_sha256": self.request_sha256,
            "consumed_at": self.consumed_at,
            "outcome": self.outcome,
        }


class HoldoutStore:
    """Injectable persistence contract for holdout consume-once semantics.

    The store is the single atomic decision point: once a nonce is recorded
    the holdout is permanently consumed regardless of what the caller does
    with the returned handle.
    """

    def consume(
        self,
        *,
        nonce: str,
        request_sha256: str,
        outcome: str,
        durable_ticket_id: str | None = None,
        durable_request_sha256: str | None = None,
        durable_nonce_fingerprint: str | None = None,
    ) -> HoldoutConsumed:
        """Atomically consume one holdout nonce.

        Returns a ``HoldoutConsumed`` receipt proving the nonce was spent.
        Raises ``HoldoutAlreadyConsumedError`` if the nonce was already
        consumed.

        CR-010 B-03: the optional DURABLE identity binds this consume to
        the durable P8 FinalEval ticket/request digest/nonce fingerprint --
        the holdout consume record and the Authority binding share one
        lineage.
        """
        raise NotImplementedError("HoldoutStore is an injectable contract")


class InMemoryHoldoutStore(HoldoutStore):
    """In-memory holdout store for testing; never used in production.

    Each instance is fully isolated.  The clock is injectable so tests can
    assert deterministic timestamps on consumed receipts.
    """

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._consumed: dict[str, HoldoutConsumed] = {}
        self._consumed_durable: dict[str, dict[str, str]] = {}
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def consume(
        self,
        *,
        nonce: str,
        request_sha256: str,
        outcome: str,
        durable_ticket_id: str | None = None,
        durable_request_sha256: str | None = None,
        durable_nonce_fingerprint: str | None = None,
    ) -> HoldoutConsumed:
        if nonce in self._consumed:
            existing = self._consumed[nonce]
            raise HoldoutAlreadyConsumedError(
                f"Holdout nonce {nonce} was already permanently consumed "
                f"with outcome {existing.outcome}"
            )
        if durable_ticket_id is not None or durable_request_sha256 is not None:
            # CR-010 B-03: the consume record carries the durable P8
            # ticket/request identity (all-or-nothing).
            if not (
                durable_ticket_id
                and durable_request_sha256
                and durable_nonce_fingerprint
            ):
                raise ValueError(
                    "durable ticket/request/fingerprint must be provided "
                    "together"
                )
            self._consumed_durable[nonce] = {
                "durable_ticket_id": durable_ticket_id,
                "durable_request_sha256": durable_request_sha256,
                "durable_nonce_fingerprint": durable_nonce_fingerprint,
            }
        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise FinalEvalRequestError("store clock must return a timezone-aware datetime")
        consumed_at = now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        consumed = HoldoutConsumed(
            holdout_id="holdout-final-1",
            holdout_sha256=_require_sha256(
                _sha_dummy("holdout"),
                "holdout_sha256",
            ),
            authorization_nonce=nonce,
            request_sha256=request_sha256,
            consumed_at=consumed_at,
            outcome=outcome,
        )
        self._consumed[nonce] = consumed
        return consumed


class AuthorityBroker:
    """Consumes the Final Holdout nonce permanently before returning any handle.

    Success, failure, timeout and crash all consume.  Nonce replay and
    crash-after-consume are both rejected with ``HoldoutAlreadyConsumedError``.

    The broker delegates the atomic consume decision to an injectable
    ``HoldoutStore`` so that tests can verify the contract without touching
    real Authority storage.
    """

    def __init__(self, *, store: HoldoutStore) -> None:
        if not isinstance(store, HoldoutStore):
            raise TypeError("store must be a HoldoutStore")
        self._store = store

    def consume(
        self,
        request: FinalEvalRequest,
        *,
        outcome: str,
        durable_ticket_id: str | None = None,
        durable_request_sha256: str | None = None,
        durable_nonce_fingerprint: str | None = None,
    ) -> HoldoutConsumed:
        """Consume the holdout nonce and return a ``HoldoutConsumed`` receipt.

        The nonce is consumed atomically in the store *before* this method
        returns.  All terminal outcomes (SUCCEEDED, FAILED, TIMEOUT,
        CRASHED) consume permanently.

        Raises ``HoldoutAlreadyConsumedError`` when the nonce was already
        consumed (nonce replay or crash-after-consume).

        CR-010 B-03: the durable P8 ticket/request/fingerprint identity is
        bound into the consume record -- the OPEN_HOLDOUT consume and the
        durable Authority binding share one lineage.
        """
        if not isinstance(request, FinalEvalRequest):
            raise TypeError("request must be a FinalEvalRequest")
        if outcome not in _ALLOWED_CONSUME_OUTCOMES:
            raise ConsumeOnceValidationError(
                f"outcome must be one of {sorted(_ALLOWED_CONSUME_OUTCOMES)}"
            )
        return self._store.consume(
            nonce=request.holdout.authorization_nonce,
            request_sha256=request.request_sha256,
            outcome=outcome,
            durable_ticket_id=durable_ticket_id,
            durable_request_sha256=durable_request_sha256,
            durable_nonce_fingerprint=durable_nonce_fingerprint,
        )


def _sha_dummy(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# P8R2 T4: low-privilege TrustedEvaluator data-root adapter
# ---------------------------------------------------------------------------

TRUSTED_HOLDOUT_HANDLE_SCHEMA = "control_plane.trusted_holdout_handle.v1"
TRUSTED_HOLDOUT_VIEW_SCHEMA = "control_plane.trusted_holdout_view.v1"
TRUSTED_EVALUATOR_RESULT_SCHEMA = "control_plane.trusted_evaluator_result.v1"
TRUSTED_DATA_ROOT_SCHEMA = "control_plane.trusted_evaluator_data_root.v1"
FINAL_HOLDOUT_TAINT = "FINAL_HOLDOUT"
OPEN_HOLDOUT_EFFECT = "OPEN_HOLDOUT"
_TERMINAL_OUTCOMES = frozenset({"SUCCEEDED", "FAILED", "TIMEOUT", "CRASHED"})

_MAX_METRICS = 64
_MAX_COUNTS = 64
_MAX_SHA256S = 64
_MAX_EVIDENCE_REFS = 32
_MAX_REF_LENGTH = 512
_MAX_CHILD_DEPTH = 8
_MAX_RESULT_PAYLOAD_BYTES = 256 * 1024

_SAFE_REF_RE = re.compile(r"^[A-Za-z0-9_./-]{1,512}$")
_SAFE_METRIC_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_RESERVED_WINDOWS_NAMES = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    }
)


class TrustedEvaluatorError(RuntimeError):
    """Base error for the low-privilege TrustedEvaluator adapter."""


class TrustedEvaluatorLeaseError(TrustedEvaluatorError):
    """Raised when a lease lacks OPEN_HOLDOUT or a non-evaluator path declares it."""


class TrustedEvaluatorPathError(TrustedEvaluatorError, ValueError):
    """Raised when a holdout path fails lexical, reparse or containment checks."""


class TrustedEvaluatorBoundaryError(TrustedEvaluatorError, ValueError):
    """Raised when a ref is unregistered, unbounded or not repo-relative."""


class PromptAccessDeniedError(TrustedEvaluatorError):
    """Raised when holdout-derived content is offered to a prompt/LLM surface."""


class UnboundedResultError(TrustedEvaluatorError):
    """Raised when an evaluation result exceeds the fixed bound."""


def _normalize_effect(effect: object) -> str:
    """Return the canonical side-effect spelling for any input shape.

    SideEffect is a str-Enum: str(SideEffect.OPEN_HOLDOUT) is
    "SideEffect.OPEN_HOLDOUT", while the enum member compares equal to
    "OPEN_HOLDOUT".  Normalizing through the value attribute keeps the lease
    and the denial guard consistent for both plain strings and enum members.
    """
    if isinstance(effect, SideEffect):
        return effect.value
    return str(effect)


def _is_repo_relative_evidence_ref(ref: object) -> bool:
    if not isinstance(ref, str) or _SAFE_REF_RE.fullmatch(ref) is None:
        return False
    if ref.startswith("/") or ref.startswith(chr(92)):
        return False
    parts = PurePosixPath(ref).parts
    return bool(parts) and all(part not in ("", ".", "..") for part in parts)


def _validate_child_ref(ref: object) -> str:
    if not isinstance(ref, str) or not ref or len(ref) > _MAX_REF_LENGTH:
        raise TrustedEvaluatorBoundaryError(
            "holdout ref must be a bounded non-empty string"
        )
    if chr(0) in ref or chr(92) in ref or ":" in ref or ref.startswith("/"):
        raise TrustedEvaluatorBoundaryError("holdout ref uses a forbidden spelling")
    parts = PurePosixPath(ref).parts
    if any(part in ("", ".", "..") for part in parts):
        raise TrustedEvaluatorBoundaryError(
            "holdout ref contains traversal or empty segments"
        )
    if len(parts) > _MAX_CHILD_DEPTH:
        raise TrustedEvaluatorBoundaryError(
            "holdout ref exceeds the maximum child depth"
        )
    if os.name == "nt":
        for part in parts:
            if part.endswith((" ", ".")):
                raise TrustedEvaluatorBoundaryError(
                    "holdout ref uses a trailing dot or space component"
                )
            if part.rstrip(". ").upper() in _RESERVED_WINDOWS_NAMES:
                raise TrustedEvaluatorBoundaryError(
                    "holdout ref contains a reserved Windows name"
                )
    return "/".join(parts)


@dataclass(frozen=True, slots=True)
class HoldoutMetric:
    """One bounded numeric metric derived from a Final Holdout artifact."""

    name: str
    value: float

    def __post_init__(self) -> None:
        if (
            not isinstance(self.name, str)
            or _SAFE_METRIC_NAME_RE.fullmatch(self.name) is None
        ):
            raise TrustedEvaluatorBoundaryError(
                "holdout metric name must be bounded and safe"
            )
        if isinstance(self.value, bool) or not isinstance(self.value, (int, float)):
            raise TrustedEvaluatorBoundaryError(
                "holdout metric value must be numeric"
            )
        numeric = float(self.value)
        if not math.isfinite(numeric):
            raise TrustedEvaluatorBoundaryError(
                "holdout metric value must be finite"
            )
        object.__setattr__(self, "value", numeric)


@dataclass(frozen=True, slots=True)
class HoldoutLease:
    """Capability lease bound to a holdout ticket and frozen evaluator code."""

    lease_id: str
    ticket_id: str
    allowed_side_effects: tuple[str, ...]
    code_sha256: str

    def __post_init__(self) -> None:
        for label, value in (
            ("lease_id", self.lease_id),
            ("ticket_id", self.ticket_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise TrustedEvaluatorLeaseError(
                    f"{label} must be a non-empty identifier"
                )
        if (
            not isinstance(self.allowed_side_effects, tuple)
            or not self.allowed_side_effects
        ):
            raise TrustedEvaluatorLeaseError(
                "lease must declare at least one allowed side effect"
            )
        normalized = tuple(_normalize_effect(effect) for effect in self.allowed_side_effects)
        if any(not value.strip() for value in normalized):
            raise TrustedEvaluatorLeaseError(
                "lease side effects must be non-empty strings"
            )
        if len(set(normalized)) != len(normalized) or normalized != tuple(
            sorted(normalized)
        ):
            raise TrustedEvaluatorLeaseError(
                "lease side effects must be unique and sorted"
            )
        object.__setattr__(self, "allowed_side_effects", normalized)
        if _HEX64_RE.fullmatch(self.code_sha256) is None:
            raise TrustedEvaluatorLeaseError(
                "lease code_sha256 must be a 64-character lowercase SHA-256 digest"
            )


@dataclass(frozen=True, slots=True)
class HoldoutHandle:
    """Distinct frozen handle binding a consumed receipt to its lease.

    T4 promotes HoldoutHandle from a plain alias of HoldoutConsumed to
    this wrapper so OPEN_HOLDOUT can be enforced per-lease before any data-root
    path is resolved.
    """

    consumed: HoldoutConsumed
    lease: HoldoutLease
    handle_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.consumed, HoldoutConsumed):
            raise TypeError("consumed must be a HoldoutConsumed receipt")
        if not isinstance(self.lease, HoldoutLease):
            raise TypeError("lease must be a HoldoutLease")
        object.__setattr__(self, "handle_sha256", canonical_sha256(self.to_payload()))

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": TRUSTED_HOLDOUT_HANDLE_SCHEMA,
            "consumed": self.consumed.to_payload(),
            "lease": {
                "lease_id": self.lease.lease_id,
                "ticket_id": self.lease.ticket_id,
                "allowed_side_effects": self.lease.allowed_side_effects,
                "code_sha256": self.lease.code_sha256,
            },
        }


def require_open_holdout_lease(handle: HoldoutHandle) -> None:
    """Require the lease to hold OPEN_HOLDOUT before any data-root access."""
    if not isinstance(handle, HoldoutHandle):
        raise TypeError("handle must be a HoldoutHandle")
    if OPEN_HOLDOUT_EFFECT not in handle.lease.allowed_side_effects:
        raise TrustedEvaluatorLeaseError(
            "OPEN_HOLDOUT is denied for this lease; only the TrustedEvaluator "
            "data-root adapter may open the Final Holdout"
        )


def deny_open_holdout_effect(effects: object) -> None:
    """Reject any non-evaluator path that would declare OPEN_HOLDOUT."""
    if not isinstance(effects, (tuple, frozenset, set, list)):
        raise TypeError("effects must be a collection of side effects")
    if OPEN_HOLDOUT_EFFECT in {_normalize_effect(effect) for effect in effects}:
        raise TrustedEvaluatorLeaseError(
            "non-evaluator paths must never declare OPEN_HOLDOUT"
        )


def require_evaluator_spec_holdout_free(execution_spec: ExecutionSpec) -> None:
    """Fail closed unless the runner-facing spec is disjoint from OPEN_HOLDOUT."""
    try:
        effects = execution_spec.protocol.allowed_side_effects
    except AttributeError as error:
        raise TrustedEvaluatorLeaseError(
            "execution spec must expose a frozen protocol with allowed_side_effects"
        ) from error
    deny_open_holdout_effect(effects)


@dataclass(frozen=True, slots=True)
class TrustedEvaluatorDataRoot:
    """Sealed, non-reparse data root plus the only blessed holdout refs."""

    root: str
    holdout_refs: tuple[str, ...]
    root_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.root, str) or not self.root:
            raise TrustedEvaluatorPathError(
                "sealed data root must be a non-empty absolute path"
            )
        if not isinstance(self.holdout_refs, tuple):
            raise TrustedEvaluatorBoundaryError("holdout_refs must be a tuple")
        normalized = tuple(_validate_child_ref(ref) for ref in self.holdout_refs)
        if len(normalized) != len(set(normalized)):
            raise TrustedEvaluatorBoundaryError("holdout_refs must be unique")
        object.__setattr__(self, "holdout_refs", normalized)
        object.__setattr__(self, "root_sha256", canonical_sha256(self.to_payload()))

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": TRUSTED_DATA_ROOT_SCHEMA,
            "root": self.root,
            "holdout_refs": self.holdout_refs,
        }


def seal_trusted_data_root(
    root: str | Path,
    holdout_refs: tuple[str, ...] = (),
) -> TrustedEvaluatorDataRoot:
    """Seal one existing, non-reparse directory as the trusted data root."""
    try:
        raw_root = _entry_guard._validate_resource_path_lexically(root)
    except Exception as error:
        raise TrustedEvaluatorPathError(
            "trusted data root path is not lexically safe"
        ) from error
    root_path = Path(raw_root)
    if not root_path.is_dir():
        raise TrustedEvaluatorPathError(
            "trusted data root must be an existing directory"
        )
    if _entry_guard._resource_path_has_reparse_point(root_path):
        raise TrustedEvaluatorPathError(
            "trusted data root must not be a reparse point"
        )
    resolved = _entry_guard._resolve_validated_resource(raw_root)
    if _entry_guard._resource_path_has_reparse_point(resolved):
        raise TrustedEvaluatorPathError(
            "resolved trusted data root must not be a reparse point"
        )
    return TrustedEvaluatorDataRoot(
        root=str(resolved),
        holdout_refs=tuple(holdout_refs),
    )


def _resolve_blessed_child(root: TrustedEvaluatorDataRoot, ref: str) -> Path:
    if not isinstance(root, TrustedEvaluatorDataRoot):
        raise TypeError("root must be a TrustedEvaluatorDataRoot")
    relative = _validate_child_ref(ref)
    if relative not in root.holdout_refs:
        raise TrustedEvaluatorBoundaryError(
            f"holdout ref {relative!r} is not registered on the sealed data root"
        )
    candidate = Path(root.root).joinpath(*PurePosixPath(relative).parts)
    try:
        _entry_guard._validate_resource_path_lexically(str(candidate))
    except Exception as error:
        raise TrustedEvaluatorPathError(
            "holdout path is not lexically safe"
        ) from error
    if _entry_guard._resource_path_has_reparse_point(candidate):
        raise TrustedEvaluatorPathError("holdout path contains a reparse point")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        raise TrustedEvaluatorPathError(
            "holdout path cannot be resolved inside the sealed data root"
        ) from error
    try:
        resolved.relative_to(Path(root.root))
    except ValueError as error:
        raise TrustedEvaluatorPathError(
            "holdout path escapes the sealed data root"
        ) from error
    if _entry_guard._resource_path_has_reparse_point(resolved):
        raise TrustedEvaluatorPathError(
            "resolved holdout path contains a reparse point"
        )
    return resolved


class HoldoutDataBackend:
    """Injectable contract for reading bounded summaries from one artifact."""

    def read_holdout_summary(
        self,
        *,
        path: Path,
        holdout_id: str,
        holdout_sha256: str,
    ) -> dict[str, object]:
        raise NotImplementedError("HoldoutDataBackend is an injectable contract")


def _parse_metrics(value: object) -> tuple[HoldoutMetric, ...]:
    if not isinstance(value, (tuple, list)):
        raise TrustedEvaluatorBoundaryError("backend metrics must be a sequence")
    parsed: list[HoldoutMetric] = []
    for item in value:
        if not isinstance(item, dict) or "name" not in item or "value" not in item:
            raise TrustedEvaluatorBoundaryError(
                "backend metric entries must carry name and value"
            )
        parsed.append(HoldoutMetric(name=str(item["name"]), value=item["value"]))
    return tuple(parsed)


def _parse_counts(value: object) -> tuple[tuple[str, int], ...]:
    if not isinstance(value, (tuple, list)):
        raise TrustedEvaluatorBoundaryError("backend counts must be a sequence")
    parsed: list[tuple[str, int]] = []
    for item in value:
        if not isinstance(item, dict) or "name" not in item or "value" not in item:
            raise TrustedEvaluatorBoundaryError(
                "backend count entries must carry name and value"
            )
        name = str(item["name"])
        count = item["value"]
        if (
            _SAFE_METRIC_NAME_RE.fullmatch(name) is None
            or isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
        ):
            raise TrustedEvaluatorBoundaryError(
                "backend count entries must be safe non-negative integers"
            )
        parsed.append((name, count))
    return tuple(parsed)


def _parse_digests(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, (tuple, list)):
        raise TrustedEvaluatorBoundaryError("backend digests must be a sequence")
    parsed: list[tuple[str, str]] = []
    for item in value:
        if (
            not isinstance(item, dict)
            or "artifact_id" not in item
            or "sha256" not in item
        ):
            raise TrustedEvaluatorBoundaryError(
                "backend digest entries must carry artifact_id and sha256"
            )
        artifact_id = str(item["artifact_id"])
        digest = str(item["sha256"])
        if (
            _SAFE_METRIC_NAME_RE.fullmatch(artifact_id) is None
            or _HEX64_RE.fullmatch(digest) is None
        ):
            raise TrustedEvaluatorBoundaryError(
                "backend digest entries are malformed"
            )
        parsed.append((artifact_id, digest))
    return tuple(parsed)


def _parse_evidence_refs(value: object) -> tuple[str, ...]:
    if not isinstance(value, (tuple, list)):
        raise TrustedEvaluatorBoundaryError(
            "backend evidence refs must be a sequence"
        )
    parsed: list[str] = []
    for ref in value:
        if not isinstance(ref, str) or not _is_repo_relative_evidence_ref(ref):
            raise TrustedEvaluatorBoundaryError(
                "backend evidence refs must be repo-relative and bounded"
            )
        parsed.append(ref)
    return tuple(parsed)


@dataclass(frozen=True, slots=True)
class HoldoutView:
    """Bounded structured metrics plus evidence refs; never raw holdout bytes."""

    metrics: tuple[HoldoutMetric, ...]
    counts: tuple[tuple[str, int], ...]
    sha256s: tuple[tuple[str, str], ...]
    evidence_refs: tuple[str, ...]
    taint: tuple[str, ...]
    view_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.metrics, tuple) or len(self.metrics) > _MAX_METRICS:
            raise UnboundedResultError("holdout metrics exceed the fixed bound")
        if not all(isinstance(metric, HoldoutMetric) for metric in self.metrics):
            raise TypeError("metrics must contain only HoldoutMetric values")
        if not isinstance(self.counts, tuple) or len(self.counts) > _MAX_COUNTS:
            raise UnboundedResultError("holdout counts exceed the fixed bound")
        if not isinstance(self.sha256s, tuple) or len(self.sha256s) > _MAX_SHA256S:
            raise UnboundedResultError("holdout digests exceed the fixed bound")
        if (
            not isinstance(self.evidence_refs, tuple)
            or len(self.evidence_refs) > _MAX_EVIDENCE_REFS
        ):
            raise UnboundedResultError("holdout evidence refs exceed the fixed bound")
        if not all(
            _is_repo_relative_evidence_ref(ref) for ref in self.evidence_refs
        ):
            raise TrustedEvaluatorBoundaryError(
                "holdout evidence refs must be repo-relative and bounded"
            )
        if not isinstance(self.taint, tuple) or FINAL_HOLDOUT_TAINT not in self.taint:
            raise TrustedEvaluatorBoundaryError(
                "holdout view must carry FINAL_HOLDOUT taint"
            )
        payload = self.to_payload()
        if len(canonical_json(payload)) > _MAX_RESULT_PAYLOAD_BYTES:
            raise UnboundedResultError(
                "holdout view payload exceeds the fixed byte bound"
            )
        object.__setattr__(self, "view_sha256", canonical_sha256(payload))

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": TRUSTED_HOLDOUT_VIEW_SCHEMA,
            "metrics": [
                {"name": metric.name, "value": metric.value}
                for metric in self.metrics
            ],
            "counts": [
                {"name": name, "value": value} for name, value in self.counts
            ],
            "sha256s": [
                {"artifact_id": artifact_id, "sha256": digest}
                for artifact_id, digest in self.sha256s
            ],
            "evidence_refs": list(self.evidence_refs),
            "taint": list(self.taint),
        }

    def render_for_prompt(self) -> str:
        raise PromptAccessDeniedError(
            "holdout-derived content must never be rendered into a prompt/LLM surface"
        )


class TrustedEvaluatorAdapter:
    """Low-privilege seam that maps one consumed handle to a bounded view."""

    def __init__(self, *, backend: HoldoutDataBackend) -> None:
        if not isinstance(backend, HoldoutDataBackend):
            raise TypeError("backend must be a HoldoutDataBackend")
        self._backend = backend

    def read(
        self,
        handle: HoldoutHandle,
        *,
        data_root: TrustedEvaluatorDataRoot,
        refs: tuple[str, ...] | None = None,
    ) -> HoldoutView:
        require_open_holdout_lease(handle)
        if not isinstance(data_root, TrustedEvaluatorDataRoot):
            raise TypeError("data_root must be a sealed TrustedEvaluatorDataRoot")
        selected = data_root.holdout_refs if refs is None else tuple(refs)
        if not selected:
            raise TrustedEvaluatorBoundaryError(
                "at least one registered holdout ref is required"
            )
        metrics: list[HoldoutMetric] = []
        counts: list[tuple[str, int]] = []
        sha256s: list[tuple[str, str]] = []
        evidence_refs: list[str] = []
        for ref in selected:
            resolved = _resolve_blessed_child(data_root, ref)
            if _entry_guard._resource_path_has_reparse_point(resolved):
                raise TrustedEvaluatorPathError(
                    "holdout path changed to a reparse point before open"
                )
            summary = self._backend.read_holdout_summary(
                path=resolved,
                holdout_id=handle.consumed.holdout_id,
                holdout_sha256=handle.consumed.holdout_sha256,
            )
            if not isinstance(summary, dict):
                raise TrustedEvaluatorBoundaryError(
                    "holdout backend must return a bounded summary dict"
                )
            metrics.extend(_parse_metrics(summary.get("metrics", ())))
            counts.extend(_parse_counts(summary.get("counts", ())))
            sha256s.extend(_parse_digests(summary.get("sha256s", ())))
            evidence_refs.extend(
                _parse_evidence_refs(summary.get("evidence_refs", ()))
            )
        return HoldoutView(
            metrics=tuple(metrics),
            counts=tuple(counts),
            sha256s=tuple(sha256s),
            evidence_refs=tuple(evidence_refs),
            taint=(FINAL_HOLDOUT_TAINT,),
        )


@dataclass(frozen=True, slots=True)
class EvaluatorResult:
    """Terminal evaluation result: bounded metrics and evidence refs only."""

    request_sha256: str
    handle_sha256: str
    view: HoldoutView
    outcome: str
    result_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if _HEX64_RE.fullmatch(self.request_sha256) is None:
            raise TrustedEvaluatorBoundaryError(
                "result request_sha256 must be a SHA-256 digest"
            )
        if _HEX64_RE.fullmatch(self.handle_sha256) is None:
            raise TrustedEvaluatorBoundaryError(
                "result handle_sha256 must be a SHA-256 digest"
            )
        if not isinstance(self.view, HoldoutView):
            raise TypeError("result view must be a HoldoutView")
        if self.outcome not in _ALLOWED_CONSUME_OUTCOMES:
            raise TrustedEvaluatorBoundaryError(
                "result outcome must be a terminal consume outcome"
            )
        payload = self.to_payload()
        if len(canonical_json(payload)) > _MAX_RESULT_PAYLOAD_BYTES:
            raise UnboundedResultError(
                "evaluation result payload exceeds the fixed byte bound"
            )
        object.__setattr__(self, "result_sha256", canonical_sha256(payload))

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": TRUSTED_EVALUATOR_RESULT_SCHEMA,
            "request_sha256": self.request_sha256,
            "handle_sha256": self.handle_sha256,
            "view": self.view.to_payload(),
            "outcome": self.outcome,
            "taint": list(self.view.taint),
        }

    def render_for_prompt(self) -> str:
        raise PromptAccessDeniedError(
            "evaluation results must never be rendered into a prompt/LLM surface"
        )


class TrustedEvaluator:
    """Top-level final evaluator: consume once, then open only via the adapter."""

    def __init__(
        self,
        *,
        broker: AuthorityBroker,
        adapter: TrustedEvaluatorAdapter,
    ) -> None:
        if not isinstance(broker, AuthorityBroker):
            raise TypeError("broker must be an AuthorityBroker")
        if not isinstance(adapter, TrustedEvaluatorAdapter):
            raise TypeError("adapter must be a TrustedEvaluatorAdapter")
        self._broker = broker
        self._adapter = adapter

    def evaluate(
        self,
        request: FinalEvalRequest,
        *,
        data_root: TrustedEvaluatorDataRoot,
        refs: tuple[str, ...] | None = None,
        outcome: str = "SUCCEEDED",
        allowed_side_effects: tuple[str, ...] = (OPEN_HOLDOUT_EFFECT,),
        lease_id: str = "lease-final-1",
        ticket_id: str = "ticket-final-1",
    ) -> EvaluatorResult:
        """V1 historical path (caller-controlled inputs retained for tests).

        P8R3 removes caller-controlled semantics from production: use
        ``evaluate_v2`` which derives outcome from the worker result and
        binds a real Authority lease.
        """
        if not isinstance(request, FinalEvalRequest):
            raise TypeError("request must be a FinalEvalRequest")
        require_evaluator_spec_holdout_free(request.execution_spec.execution_spec)
        consumed = self._broker.consume(request, outcome=outcome)
        lease = HoldoutLease(
            lease_id=lease_id,
            ticket_id=ticket_id,
            allowed_side_effects=tuple(
                sorted(_normalize_effect(effect) for effect in allowed_side_effects)
            ),
            code_sha256=request.code.code_sha256,
        )
        handle = HoldoutHandle(consumed=consumed, lease=lease)
        view = self._adapter.read(handle, data_root=data_root, refs=refs)
        return EvaluatorResult(
            request_sha256=request.request_sha256,
            handle_sha256=handle.handle_sha256,
            view=view,
            outcome=outcome,
        )

    def evaluate_v2(
        self,
        request: object,
        *,
        data_root: TrustedEvaluatorDataRoot,
        refs: tuple[str, ...] | None = None,
        worker_payload: Mapping[str, object] | None = None,
        worker_launcher: Callable[[], int] | None = None,
        consumption: object | None = None,
        durable_ticket_id: str | None = None,
        durable_request_sha256: str | None = None,
        durable_nonce_fingerprint: str | None = None,
    ) -> EvaluatorResult:
        """V2 production path: outcome derived from the REAL worker result.

        The caller can never specify the outcome: ``worker_payload`` must
        be the real worker's result mapping, OR (production) the approved
        ``worker_launcher`` is invoked AFTER the synthetic artifact handle
        is open and the outcome is derived from its exit code;
        ``derive_outcome`` rejects caller-supplied outcomes and an
        incomplete payload fails closed.

        PRODUCTION consume path (``consumption`` = the Authority-backed
        ``HoldoutConsumedV2`` receipt): the nonce was already consumed by
        the begin-time Authority transaction -- evaluate_v2 NEVER consumes
        a second nonce and never creates fixed ``authority-bound-ticket``/
        ``authority-bound-lease`` values.  It opens exactly one synthetic
        staging artifact through the sealed data root, then launches the
        approved worker, and returns the worker-derived outcome and the V2
        request digest (the authoritative result identity) plus the
        durable claim lineage.

        TEST-ONLY path (no ``consumption``): the broker consumes the
        in-memory holdout nonce atomically with the derived outcome (unit
        fixtures only; production composition always passes the durable
        receipt).

        CR-010 B-03: the OPEN_HOLDOUT consume is bound to the DURABLE P8
        ticket -- ``durable_ticket_id`` / ``durable_request_sha256`` /
        ``durable_nonce_fingerprint`` are REQUIRED and must match the
        durable Authority binding/consumption receipt (never fixed
        placeholder tickets or a standalone in-memory store disconnected
        from the binding).
        """
        from .final_eval_holdout_store import HoldoutConsumedV2
        from .final_eval_request_projection import (
            EvaluatorRequestProjectionV2,
        )

        if isinstance(request, EvaluatorRequestProjectionV2):
            v1_request = request.v1_request
            result_identity = request.v2_request_sha256
        elif isinstance(request, FinalEvalRequest):
            # explicitly TEST-ONLY adapter input (unit fixtures); the
            # production composition root always passes the V2 projection
            v1_request = request
            result_identity = request.request_sha256
        else:
            raise TypeError(
                "request must be an EvaluatorRequestProjectionV2 or a "
                "FinalEvalRequest (test-only)"
            )
        if consumption is not None:
            if not isinstance(request, EvaluatorRequestProjectionV2):
                raise TrustedEvaluatorError(
                    "the Authority-backed consume path requires the V2 "
                    "projection -- a V1 request can never consume under "
                    "the durable receipt"
                )
            if not isinstance(consumption, HoldoutConsumedV2):
                raise TypeError(
                    "consumption must be a HoldoutConsumedV2 receipt"
                )
            if not (
                durable_ticket_id
                and durable_request_sha256
                and durable_nonce_fingerprint
            ):
                raise TrustedEvaluatorError(
                    "evaluate_v2 requires the durable P8 ticket identity "
                    "(ticket id, request digest, nonce fingerprint) -- the "
                    "holdout consume must bind the durable Authority binding"
                )
            # CR-010 C0 (Phase B): the full cross-layer lineage -- the
            # projection, the durable binding ids and the immutable
            # consumption receipt must agree on EVERY durable identifier.
            lineage_mismatches: list[str] = []
            if durable_ticket_id != consumption.ticket_id:
                lineage_mismatches.append("ticket")
            if durable_request_sha256 != consumption.request_sha256:
                lineage_mismatches.append("request_digest")
            if durable_nonce_fingerprint != consumption.nonce_fingerprint:
                lineage_mismatches.append("nonce_fingerprint")
            if (
                request.durable_nonce_fingerprint
                != consumption.nonce_fingerprint
            ):
                lineage_mismatches.append("projection_fingerprint")
            if v1_request.holdout.holdout_id != consumption.holdout_id:
                lineage_mismatches.append("holdout_id")
            if (
                v1_request.holdout.holdout_sha256
                != consumption.holdout_sha256
            ):
                lineage_mismatches.append("holdout_sha256")
            if v1_request.actor.actor_id != consumption.actor_id:
                lineage_mismatches.append("actor_id")
            if v1_request.actor.actor_type != consumption.actor_type:
                lineage_mismatches.append("actor_type")
            if v1_request.actor.invocation_id != consumption.invocation_id:
                lineage_mismatches.append("invocation_id")
            if request.attempt_id != consumption.attempt_id:
                lineage_mismatches.append("attempt")
            if lineage_mismatches:
                raise TrustedEvaluatorError(
                    "evaluate_v2 consumption lineage mismatch: "
                    + "; ".join(sorted(lineage_mismatches))
                )
        elif not (
            durable_ticket_id
            and durable_request_sha256
            and durable_nonce_fingerprint
        ):
            raise TrustedEvaluatorError(
                "evaluate_v2 requires the durable P8 ticket identity "
                "(ticket id, request digest, nonce fingerprint) -- the "
                "holdout consume must bind the durable Authority binding"
            )
        require_evaluator_spec_holdout_free(
            v1_request.execution_spec.execution_spec
        )
        if consumption is not None:
            # ---- PRODUCTION open: exactly one synthetic staging artifact
            # ---- is opened BEFORE the approved worker launches.  The V1
            # handle is a transient adapter artifact; its outcome slot is
            # never read by the adapter and never authoritative -- the
            # worker-derived outcome is produced below.
            open_consumed = HoldoutConsumed(
                holdout_id=consumption.holdout_id,
                holdout_sha256=consumption.holdout_sha256,
                authorization_nonce=consumption.nonce_fingerprint,
                request_sha256=consumption.request_sha256,
                consumed_at=consumption.consumed_at_utc,
                outcome="SUCCEEDED",
            )
            lease = HoldoutLease(
                lease_id=durable_ticket_id,
                ticket_id=durable_ticket_id,
                allowed_side_effects=(OPEN_HOLDOUT_EFFECT,),
                code_sha256=v1_request.code.code_sha256,
            )
            handle = HoldoutHandle(consumed=open_consumed, lease=lease)
            try:
                view = self._adapter.read(
                    handle, data_root=data_root, refs=refs
                )
            except OSError as error:
                # CR-010 F-01 (functional closure): an artifact open that
                # fails at the filesystem level (deleted/missing holdout)
                # is a controlled fail-closed error, never a raw OSError
                # escaping the OPEN_HOLDOUT seam.
                raise TrustedEvaluatorError(
                    "holdout artifact open failed (sealed artifact "
                    "unavailable or unreadable)"
                ) from error
            if worker_payload is not None:
                raise TrustedEvaluatorError(
                    "the production consume path derives the outcome from "
                    "the approved worker launcher -- a caller-supplied "
                    "payload is never accepted"
                )
            if worker_launcher is None:
                raise TrustedEvaluatorError(
                    "the production consume path requires the approved "
                    "worker launcher"
                )
            try:
                code = worker_launcher()
            except Exception as error:  # noqa: BLE001
                raise TrustedEvaluatorError(
                    "approved worker launcher failed"
                ) from error
            if type(code) is not int:
                raise TrustedEvaluatorError(
                    "worker launcher must return an integer exit code"
                )
            worker_result = derive_worker_result(code)
            outcome = derive_outcome(
                worker_payload={"outcome": worker_result.outcome}
            )
            return EvaluatorResult(
                request_sha256=result_identity,
                handle_sha256=handle.handle_sha256,
                view=view,
                outcome=outcome,
            )
        if not isinstance(worker_payload, Mapping):
            raise TrustedEvaluatorError(
                "evaluate_v2 requires the real worker result payload; "
                "a caller-supplied outcome is never accepted"
            )
        try:
            outcome = derive_outcome(worker_payload=worker_payload)
        except FinalEvalSagaError as error:
            raise TrustedEvaluatorError(
                "worker payload did not derive a terminal outcome"
            ) from error
        consumed = self._broker.consume(
            v1_request,
            outcome=outcome,
            durable_ticket_id=durable_ticket_id,
            durable_request_sha256=durable_request_sha256,
            durable_nonce_fingerprint=durable_nonce_fingerprint,
        )
        if consumed.outcome != outcome:
            raise TrustedEvaluatorError(
                "Authority consumed a different outcome than the worker "
                "result derived"
            )
        # CR-010 B-03: the lease/ticket carry the REAL durable ticket id --
        # never fixed placeholder identities.
        lease = HoldoutLease(
            lease_id=durable_ticket_id,
            ticket_id=durable_ticket_id,
            allowed_side_effects=(OPEN_HOLDOUT_EFFECT,),
            code_sha256=v1_request.code.code_sha256,
        )
        handle = HoldoutHandle(consumed=consumed, lease=lease)
        view = self._adapter.read(handle, data_root=data_root, refs=refs)
        return EvaluatorResult(
            request_sha256=result_identity,
            handle_sha256=handle.handle_sha256,
            view=view,
            outcome=consumed.outcome,
        )


# ---------------------------------------------------------------------------
# P8R2 T5: terminal audit closure (irreversible Campaign close)
# ---------------------------------------------------------------------------

TERMINAL_AUDIT_EVENT_SCHEMA = "control_plane.terminal_audit_event.v1"
CAMPAIGN_CLOSURE_RECEIPT_SCHEMA = "control_plane.campaign_closure_receipt.v1"
CLOSURE_AUDIT_RECORD_SCHEMA = "control_plane.closure_audit_record.v1"
_VERDICT_BY_OUTCOME = {
    "SUCCEEDED": "PASS",
    "FAILED": "FAIL",
    "TIMEOUT": "TIMEOUT",
    "CRASHED": "CRASH",
}
_TERMINAL_VERDICTS = frozenset(_VERDICT_BY_OUTCOME.values())
_CAMPAIGN_STATES = frozenset({"ACTIVE", "COMPLETED", "BLOCKED", "CLOSED"})


class CampaignClosedError(TrustedEvaluatorError):
    """Raised when a closed Campaign rejects further cycles or closure."""


class CampaignClosureConflictError(CampaignClosedError):
    """Raised when an already-CLOSED Campaign is closed with different content."""


class CampaignClosureValidationError(CampaignClosedError, ValueError):
    """Raised when the Campaign state or closure inputs are invalid."""


@dataclass(frozen=True, slots=True)
class TerminalAuditEvent:
    """Immutable terminal audit event; hashes and identifiers only, never labels."""

    campaign_id: str
    request_sha256: str
    holdout_id: str
    actor_id: str
    actor_type: str
    invocation_id: str
    verdict: str
    result_payload_sha256: str
    result_evidence_ref: str
    closed_at: str
    promotion_mode: str = "MANUAL_ONLY"
    event_id: str = field(init=False)
    event_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        for label, value in (
            ("campaign_id", self.campaign_id),
            ("holdout_id", self.holdout_id),
            ("actor_id", self.actor_id),
            ("actor_type", self.actor_type),
            ("invocation_id", self.invocation_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise CampaignClosureValidationError(
                    f"{label} must be a non-empty identifier"
                )
        for label, value in (
            ("request_sha256", self.request_sha256),
            ("result_payload_sha256", self.result_payload_sha256),
        ):
            if _HEX64_RE.fullmatch(value) is None:
                raise CampaignClosureValidationError(
                    f"{label} must be a 64-character lowercase SHA-256 digest"
                )
        if self.verdict not in _TERMINAL_VERDICTS:
            raise CampaignClosureValidationError(
                f"verdict must be one of {sorted(_TERMINAL_VERDICTS)}"
            )
        if not _is_repo_relative_evidence_ref(self.result_evidence_ref):
            raise CampaignClosureValidationError(
                "result_evidence_ref must be a repo-relative bounded path"
            )
        try:
            parsed = datetime.fromisoformat(self.closed_at.replace("Z", "+00:00"))
        except ValueError as error:
            raise CampaignClosureValidationError(
                "closed_at must be a valid ISO-8601 timestamp"
            ) from error
        if (
            parsed.tzinfo is None
            or self.closed_at
            != parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        ):
            raise CampaignClosureValidationError(
                "closed_at must be a canonical UTC ISO-8601 timestamp ending in Z"
            )
        if self.promotion_mode != "MANUAL_ONLY":
            raise CampaignClosureValidationError(
                "promotion_mode must be MANUAL_ONLY; production promotion is manual-only"
            )
        object.__setattr__(
            self,
            "event_id",
            canonical_sha256(
                {
                    "namespace": "control_plane",
                    "aggregate_type": "FINAL_EVAL_AUDIT",
                    "role": "FINAL_EVAL_TERMINAL",
                    "campaign_id": self.campaign_id,
                }
            ),
        )
        object.__setattr__(self, "event_sha256", canonical_sha256(self.to_payload()))

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": TERMINAL_AUDIT_EVENT_SCHEMA,
            "campaign_id": self.campaign_id,
            "request_sha256": self.request_sha256,
            "holdout_id": self.holdout_id,
            "actor_id": self.actor_id,
            "actor_type": self.actor_type,
            "invocation_id": self.invocation_id,
            "verdict": self.verdict,
            "result_payload_sha256": self.result_payload_sha256,
            "result_evidence_ref": self.result_evidence_ref,
            "closed_at": self.closed_at,
            "promotion_mode": self.promotion_mode,
        }


class CampaignClosureBackend:
    """Injectable contract for atomic terminal-event append and Campaign close."""

    def campaign_state(self, campaign_id: str) -> str:
        raise NotImplementedError("CampaignClosureBackend is an injectable contract")

    def terminal_events(self, campaign_id: str) -> tuple[TerminalAuditEvent, ...]:
        raise NotImplementedError("CampaignClosureBackend is an injectable contract")

    def close_campaign(self, *, event: TerminalAuditEvent) -> None:
        """Append the terminal event and transition the Campaign to CLOSED."""
        raise NotImplementedError("CampaignClosureBackend is an injectable contract")

    def reject_closed_campaign(self, campaign_id: str) -> None:
        """Raise CampaignClosedError when the Campaign is already CLOSED."""
        raise NotImplementedError("CampaignClosureBackend is an injectable contract")


class InMemoryCampaignClosureBackend(CampaignClosureBackend):
    """In-memory closure backend for tests; never used in production."""

    def __init__(self, *, initial_state: str = "COMPLETED") -> None:
        if initial_state not in _CAMPAIGN_STATES:
            raise CampaignClosureValidationError(
                f"initial_state must be one of {sorted(_CAMPAIGN_STATES)}"
            )
        self._default_state = initial_state
        self._states: dict[str, str] = {}
        self._events: dict[str, list[TerminalAuditEvent]] = {}

    def _state(self, campaign_id: str) -> str:
        return self._states.get(campaign_id, self._default_state)

    def campaign_state(self, campaign_id: str) -> str:
        return self._state(campaign_id)

    def terminal_events(self, campaign_id: str) -> tuple[TerminalAuditEvent, ...]:
        return tuple(self._events.get(campaign_id, ()))

    def close_campaign(self, *, event: TerminalAuditEvent) -> None:
        if not isinstance(event, TerminalAuditEvent):
            raise TypeError("event must be a TerminalAuditEvent")
        state = self._state(event.campaign_id)
        existing = self._events.get(event.campaign_id, ())
        if state == "CLOSED":
            if (
                existing
                and existing[-1].request_sha256 == event.request_sha256
                and existing[-1].result_payload_sha256 == event.result_payload_sha256
                and existing[-1].verdict == event.verdict
                and existing[-1].result_evidence_ref == event.result_evidence_ref
            ):
                return
            raise CampaignClosureConflictError(
                "Campaign is already CLOSED with different terminal content"
            )
        if state != "COMPLETED":
            raise CampaignClosureValidationError(
                f"Campaign must be COMPLETED before terminal closure; "
                f"current state is {state}"
            )
        if any(
            prior.request_sha256 != event.request_sha256
            or prior.result_payload_sha256 != event.result_payload_sha256
            for prior in existing
        ):
            raise CampaignClosureConflictError(
                "terminal event collision on the same Campaign"
            )
        self._events.setdefault(event.campaign_id, []).append(event)
        self._states[event.campaign_id] = "CLOSED"

    def reject_closed_campaign(self, campaign_id: str) -> None:
        if self._state(campaign_id) == "CLOSED":
            raise CampaignClosedError(
                "Campaign is CLOSED by final evaluation; no further cycles allowed"
            )


@dataclass(frozen=True, slots=True)
class CampaignClosureReceipt:
    """Receipt proving one Campaign was irreversibly closed by a terminal event."""

    campaign_id: str
    event_id: str
    state: str
    closed_at: str
    receipt_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.campaign_id, str) or not self.campaign_id.strip():
            raise CampaignClosureValidationError(
                "campaign_id must be a non-empty identifier"
            )
        if _HEX64_RE.fullmatch(self.event_id) is None:
            raise CampaignClosureValidationError(
                "event_id must be a 64-character lowercase SHA-256 digest"
            )
        if self.state != "CLOSED":
            raise CampaignClosureValidationError(
                "closure receipt state must be CLOSED"
            )
        object.__setattr__(
            self,
            "receipt_sha256",
            canonical_sha256(self.to_payload()),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": CAMPAIGN_CLOSURE_RECEIPT_SCHEMA,
            "campaign_id": self.campaign_id,
            "event_id": self.event_id,
            "state": self.state,
            "closed_at": self.closed_at,
        }


class TerminalAuditClosure:
    """Appends the terminal audit event and closes the Campaign irreversibly."""

    def __init__(
        self,
        *,
        backend: CampaignClosureBackend,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(backend, CampaignClosureBackend):
            raise TypeError("backend must be a CampaignClosureBackend")
        self._backend = backend
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def close_campaign(
        self,
        *,
        request: FinalEvalRequest,
        result: EvaluatorResult,
        evidence_ref: str,
    ) -> CampaignClosureReceipt:
        if not isinstance(request, FinalEvalRequest):
            raise TypeError("request must be a FinalEvalRequest")
        if not isinstance(result, EvaluatorResult):
            raise TypeError("result must be an EvaluatorResult")
        if not isinstance(evidence_ref, str) or not _is_repo_relative_evidence_ref(
            evidence_ref
        ):
            raise CampaignClosureValidationError(
                "evidence_ref must be a repo-relative bounded path"
            )
        if result.outcome not in _VERDICT_BY_OUTCOME:
            raise CampaignClosureValidationError(
                f"result outcome must be one of {sorted(_VERDICT_BY_OUTCOME)}"
            )
        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise CampaignClosureValidationError(
                "closure clock must return a timezone-aware datetime"
            )
        closed_at = now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        event = TerminalAuditEvent(
            campaign_id=request.campaign.campaign_id,
            request_sha256=request.request_sha256,
            holdout_id=request.holdout.holdout_id,
            actor_id=request.actor.actor_id,
            actor_type=request.actor.actor_type,
            invocation_id=request.actor.invocation_id,
            verdict=_VERDICT_BY_OUTCOME[result.outcome],
            result_payload_sha256=result.result_sha256,
            result_evidence_ref=evidence_ref,
            closed_at=closed_at,
        )
        self._backend.close_campaign(event=event)
        return CampaignClosureReceipt(
            campaign_id=event.campaign_id,
            event_id=event.event_id,
            state="CLOSED",
            closed_at=event.closed_at,
        )

    def require_campaign_open(self, campaign_id: str) -> None:
        self._backend.reject_closed_campaign(campaign_id)


def build_closure_audit_record(
    *,
    event: TerminalAuditEvent,
    view: HoldoutView,
) -> dict[str, Any]:
    """Bounded audit-export record: hashes, refs and metrics; never raw labels."""
    if not isinstance(event, TerminalAuditEvent):
        raise TypeError("event must be a TerminalAuditEvent")
    if not isinstance(view, HoldoutView):
        raise TypeError("view must be a HoldoutView")
    return {
        "schema_version": CLOSURE_AUDIT_RECORD_SCHEMA,
        "campaign_id": event.campaign_id,
        "event_id": event.event_id,
        "verdict": event.verdict,
        "result_payload_sha256": event.result_payload_sha256,
        "result_evidence_ref": event.result_evidence_ref,
        "promotion_mode": event.promotion_mode,
        "closed_at": event.closed_at,
        "metrics": [
            {"name": metric.name, "value": metric.value} for metric in view.metrics
        ],
        "counts": [
            {"name": name, "value": value} for name, value in view.counts
        ],
        "sha256s": [
            {"artifact_id": artifact_id, "sha256": digest}
            for artifact_id, digest in view.sha256s
        ],
        "taint": list(view.taint),
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
    "AuthorityBroker",
    "CAMPAIGN_CLOSURE_RECEIPT_SCHEMA",
    "CLOSURE_AUDIT_RECORD_SCHEMA",
    "CampaignClosedError",
    "CampaignClosureBackend",
    "CampaignClosureConflictError",
    "CampaignClosureReceipt",
    "CampaignClosureValidationError",
    "ConsumeOnceError",
    "ConsumeOnceReplayError",
    "ConsumeOnceValidationError",
    "EvaluatorResult",
    "FINAL_HOLDOUT_TAINT",
    "HoldoutHandle",
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
    "HOLDOUT_CONSUMED_SCHEMA",
    "HoldoutAlreadyConsumedError",
    "HoldoutBinding",
    "HoldoutConsumed",
    "HoldoutDataBackend",
    "HoldoutLease",
    "HoldoutMetric",
    "HoldoutView",
    "InMemoryHoldoutStore",
    "ModelBinding",
    "OPEN_HOLDOUT_EFFECT",
    "PromptAccessDeniedError",
    "RosterBinding",
    "ThresholdBinding",
    "TRUSTED_DATA_ROOT_SCHEMA",
    "TRUSTED_EVALUATOR_RESULT_SCHEMA",
    "TRUSTED_HOLDOUT_HANDLE_SCHEMA",
    "TRUSTED_HOLDOUT_VIEW_SCHEMA",
    "TERMINAL_AUDIT_EVENT_SCHEMA",
    "TerminalAuditClosure",
    "TerminalAuditEvent",
    "TrustedEvaluator",
    "TrustedEvaluatorAdapter",
    "TrustedEvaluatorBoundaryError",
    "TrustedEvaluatorDataRoot",
    "TrustedEvaluatorError",
    "TrustedEvaluatorLeaseError",
    "TrustedEvaluatorPathError",
    "UnfrozenCandidateError",
    "UnboundedResultError",
    "InMemoryCampaignClosureBackend",
    "build_closure_audit_record",
    "deny_open_holdout_effect",
    "require_evaluator_spec_holdout_free",
    "require_open_holdout_lease",
    "seal_trusted_data_root",
]
