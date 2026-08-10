"""Pure, deterministic contracts shared by the research control plane."""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum


class Phase(str, Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"
    P4 = "P4"
    P5 = "P5"
    P6 = "P6"
    P7 = "P7"
    P8 = "P8"
    C0 = "C0"
    C1 = "C1"


class SideEffect(str, Enum):
    READ = "READ"
    WRITE_STAGING = "WRITE_STAGING"
    WRITE_CONTROL_PLANE = "WRITE_CONTROL_PLANE"
    RUN_RESEARCH = "RUN_RESEARCH"
    WRITE_KBASE = "WRITE_KBASE"
    OPEN_HOLDOUT = "OPEN_HOLDOUT"
    GIT_MUTATION = "GIT_MUTATION"
    WRITE_PRODUCTION_DATA = "WRITE_PRODUCTION_DATA"
    WRITE_PRODUCTION_CONFIG = "WRITE_PRODUCTION_CONFIG"
    DELETE_PATH = "DELETE_PATH"
    NETWORK_EGRESS = "NETWORK_EGRESS"
    START_SUBPROCESS = "START_SUBPROCESS"
    START_BACKGROUND_WORK = "START_BACKGROUND_WORK"
    SEND_NOTIFICATION = "SEND_NOTIFICATION"
    EXPOSE_SERVICE = "EXPOSE_SERVICE"


class PolicyMismatchError(ValueError):
    """Raised when policy provenance is missing or internally inconsistent."""


def _normalize_sha256(value: str | None, *, field_name: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized and re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
        raise PolicyMismatchError(f"{field_name} must be a 64-character SHA-256 hex digest")
    return normalized


ACTOR_TYPES = frozenset({"human", "automation", "llm", "scheduler", "legacy_runner"})
PLAN_SCOPE_DYNAMIC_FIELDS = frozenset(
    {
        "created_at",
        "updated_at",
        "verified_at",
        "progress",
        "token_count",
        "provider_reported_tokens",
        "estimated_tokens",
    }
)


@dataclass(frozen=True)
class PolicyBinding:
    source: str
    sha256: str
    workspace_mismatch: bool

    def __post_init__(self) -> None:
        if self.source not in {"invocation", "workspace"}:
            raise PolicyMismatchError("policy source must be 'invocation' or 'workspace'")
        normalized = _normalize_sha256(self.sha256, field_name="sha256")
        if not normalized:
            raise PolicyMismatchError("sha256 must not be empty")
        object.__setattr__(self, "sha256", normalized)
        if not isinstance(self.workspace_mismatch, bool):
            raise PolicyMismatchError("workspace_mismatch must be a boolean")
        if self.workspace_mismatch:
            raise PolicyMismatchError("a mismatched workspace policy cannot form a binding")


@dataclass(frozen=True)
class IdentityBinding:
    """Approved plan, scope, and policy identities carried by P0 authority."""

    plan_hash: str
    scope_hash: str
    policy_hash: str

    def __post_init__(self) -> None:
        for field_name in ("plan_hash", "scope_hash", "policy_hash"):
            normalized = _normalize_sha256(
                getattr(self, field_name),
                field_name=field_name,
            )
            if not normalized:
                raise ValueError(f"{field_name} must not be empty")
            object.__setattr__(self, field_name, normalized)


def resolve_policy_source(
    *,
    invocation_sha256: str | None,
    workspace_sha256: str | None,
    canonical_source: str | None = None,
) -> PolicyBinding:
    """Resolve the authoritative policy source without silently choosing a version."""
    invocation = _normalize_sha256(invocation_sha256, field_name="invocation_sha256")
    workspace = _normalize_sha256(workspace_sha256, field_name="workspace_sha256")
    if not invocation or not workspace:
        raise PolicyMismatchError("both invocation and workspace policy hashes are required")
    if invocation != workspace:
        raise PolicyMismatchError(
            "invocation and workspace policy hashes differ; execution is fail-closed"
        )
    if canonical_source is None:
        canonical_source = "workspace"
    source = canonical_source.strip().lower()
    if source not in {"invocation", "workspace"}:
        raise PolicyMismatchError(f"unsupported canonical policy source: {canonical_source!r}")
    selected = invocation if source == "invocation" else workspace
    if not selected:
        raise PolicyMismatchError(f"canonical policy source {source!r} has no hash")
    return PolicyBinding(
        source=source,
        sha256=selected,
        workspace_mismatch=bool(invocation and workspace and invocation != workspace),
    )


@dataclass(frozen=True)
class Actor:
    """Auditable identity of the human or process requesting an action."""

    actor_id: str
    actor_type: str
    invocation_id: str

    def __post_init__(self) -> None:
        for field_name in ("actor_id", "actor_type", "invocation_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip())
        if self.actor_type not in ACTOR_TYPES:
            raise ValueError(f"actor_type must be one of {sorted(ACTOR_TYPES)}")


@dataclass(frozen=True)
class PhaseGrant:
    """In-memory bearer capability returned by a successful phase claim."""

    grant_id: str
    bearer_secret: str = field(repr=False)
    authorization_ref: str
    phase: Phase
    actor: Actor
    identity_binding: IdentityBinding
    allowed_side_effects: tuple[SideEffect, ...]

    def __post_init__(self) -> None:
        for field_name in ("grant_id", "bearer_secret", "authorization_ref"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if not isinstance(self.phase, Phase):
            raise ValueError("phase must be a Phase")
        if not isinstance(self.actor, Actor):
            raise ValueError("actor must be an Actor")
        if not isinstance(self.identity_binding, IdentityBinding):
            raise ValueError("identity_binding must be an IdentityBinding")
        if not all(isinstance(effect, SideEffect) for effect in self.allowed_side_effects):
            raise ValueError("allowed_side_effects must contain only SideEffect values")


@dataclass(frozen=True)
class TaskTicket:
    """One-entry, one-effect bearer capability issued from a phase grant."""

    ticket_id: str
    bearer_secret: str = field(repr=False)
    grant_id: str
    authorization_ref: str
    entry_id: str
    effect: SideEffect
    resource_scope: str
    idempotency_key: str
    actor: Actor
    identity_binding: IdentityBinding

    def __post_init__(self) -> None:
        for field_name in (
            "ticket_id",
            "bearer_secret",
            "grant_id",
            "authorization_ref",
            "entry_id",
            "resource_scope",
            "idempotency_key",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if not isinstance(self.effect, SideEffect):
            raise ValueError("effect must be a SideEffect")
        if not isinstance(self.actor, Actor):
            raise ValueError("actor must be an Actor")
        if not isinstance(self.identity_binding, IdentityBinding):
            raise ValueError("identity_binding must be an IdentityBinding")


@dataclass(frozen=True)
class SideEffectLease:
    """Capability proving one task ticket won the atomic effect-start race."""

    lease_id: str
    bearer_secret: str = field(repr=False)
    ticket_id: str
    grant_id: str
    authorization_ref: str
    entry_id: str
    effect: SideEffect
    resource_scope: str
    actor: Actor
    identity_binding: IdentityBinding

    def __post_init__(self) -> None:
        for field_name in (
            "lease_id",
            "bearer_secret",
            "ticket_id",
            "grant_id",
            "authorization_ref",
            "entry_id",
            "resource_scope",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if not isinstance(self.effect, SideEffect):
            raise ValueError("effect must be a SideEffect")
        if not isinstance(self.actor, Actor):
            raise ValueError("actor must be an Actor")
        if not isinstance(self.identity_binding, IdentityBinding):
            raise ValueError("identity_binding must be an IdentityBinding")


@dataclass(frozen=True)
class TicketSnapshot:
    """Non-authorizing terminal projection of one task ticket."""

    ticket_id: str
    state: str
    evidence_ref: str

    def __post_init__(self) -> None:
        if not isinstance(self.ticket_id, str) or not self.ticket_id.strip():
            raise ValueError("ticket_id must be a non-empty string")
        if self.state not in {"SUCCEEDED", "FAILED", "IN_DOUBT"}:
            raise ValueError("ticket snapshot state is invalid")
        if not isinstance(self.evidence_ref, str) or not self.evidence_ref.strip():
            raise ValueError("evidence_ref must be a non-empty string")


def _is_path_field(field_name: str | None) -> bool:
    if field_name is None:
        return False
    return (
        field_name in {"path", "paths", "root", "roots", "relative_path"}
        or field_name.endswith(("_path", "_paths", "_root", "_roots"))
    )


def _canonicalize(
    value: object,
    *,
    field_name: str | None = None,
    excluded_fields: frozenset[str] = frozenset(),
) -> object:
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical mappings require string keys")
            if key in excluded_fields:
                continue
            result[key] = _canonicalize(
                item,
                field_name=key,
                excluded_fields=excluded_fields,
            )
        return result
    if isinstance(value, (list, tuple)):
        return [
            _canonicalize(
                item,
                field_name=field_name,
                excluded_fields=excluded_fields,
            )
            for item in value
        ]
    if isinstance(value, str) and _is_path_field(field_name):
        return value.replace("\\", "/")
    return value


def _canonical_json_with_exclusions(
    value: object,
    *,
    excluded_fields: frozenset[str],
) -> str:
    return json.dumps(
        _canonicalize(value, excluded_fields=excluded_fields),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_json(value: object) -> str:
    """Return the unique UTF-8 JSON representation used for identities."""
    return _canonical_json_with_exclusions(value, excluded_fields=frozenset())


def canonical_sha256(value: object) -> str:
    """Hash a value after canonical JSON serialization."""
    payload = canonical_json(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def canonical_plan_scope_sha256(value: object) -> str:
    """Hash plan/scope semantics while excluding the fixed dynamic-field set."""
    payload = _canonical_json_with_exclusions(
        value,
        excluded_fields=PLAN_SCOPE_DYNAMIC_FIELDS,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
