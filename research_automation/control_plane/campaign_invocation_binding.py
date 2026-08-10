"""campaign_invocation_binding.py - trusted profile/pricing/retry/spawn binding.

P6 campaign provider calls must be bound to a frozen roster member BEFORE any
provider object is constructed or invoked. This module owns that binding:

- ``TrustedInvocationBinding`` freezes the provider profile id, provider/model
  identity, pricing snapshot, logical retry policy, and the current spawn
  process identity into one canonical payload with a deterministic sha256;
- ``build_invocation_binding`` derives the binding from a frozen roster member,
  the model call limits, and the process identity provider;
- ``require_provider_binding`` fails closed when an injected provider's
  identity attributes disagree with the binding;
- ``require_invocation_binding`` composes both checks and is the only gate
  through which the controller may construct a provider executor;
- ``verify_spawn_identity`` re-checks the live process identity immediately
  before provider construction and fails closed on drift;
- ``construct_provider`` refuses to build a provider except from a validated
  ``TrustedInvocationBinding``.

This module performs no I/O, no subprocesses, and no provider SDK imports.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re

from .campaign_lease import ProcessIdentity, ProcessIdentityProvider


_SCHEMA_VERSION = "control_plane.trusted_invocation_binding.v1"
_DOMAIN = b"control_plane.trusted_invocation_binding.v1"
_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_HEX_SHA256_RE = re.compile(r"[0-9a-fA-F]{64}\Z")
_MAX_COST_TEXT_CHARS = 128
_MAX_INT = 2**63 - 1
_MAX_PID = 2**31 - 1


class InvocationBindingError(ValueError):
    """Raised when a trusted invocation binding is invalid or conflicts."""


def _identifier(value: object, name: str) -> str:
    if type(value) is not str or not _IDENTIFIER_RE.fullmatch(value):
        raise InvocationBindingError(f"{name} is not a valid identifier")
    return value


def _sha256_hex(value: object, name: str) -> str:
    if type(value) is not str or not _HEX_SHA256_RE.fullmatch(value):
        raise InvocationBindingError(
            f"{name} must be a 64-character hex digest"
        )
    return value


def _bounded_int(
    value: object,
    name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise InvocationBindingError(
            f"{name} must be an integer from {minimum} through {maximum}"
        )
    return value


def _cost_text(value: object, name: str = "max_cost") -> str:
    if value is None:
        raise InvocationBindingError(f"{name} is required")
    if type(value) is str:
        candidate = value.strip()
    elif type(value) is int:
        candidate = str(value)
    elif type(value) is float:
        if not math.isfinite(value):
            raise InvocationBindingError(f"{name} must be finite")
        candidate = repr(value)
    elif isinstance(value, Decimal):
        if not value.is_finite():
            raise InvocationBindingError(f"{name} must be finite")
        candidate = str(value)
    else:
        raise InvocationBindingError(f"{name} has an unsupported type")
    if not candidate or len(candidate) > _MAX_COST_TEXT_CHARS:
        raise InvocationBindingError(f"{name} is invalid")
    try:
        parsed = Decimal(candidate)
    except InvalidOperation:
        raise InvocationBindingError(f"{name} is invalid") from None
    if not parsed.is_finite() or parsed < 0:
        raise InvocationBindingError(
            f"{name} must be a finite non-negative decimal"
        )
    return candidate


@dataclass(frozen=True, slots=True)
class TrustedInvocationBinding:
    """One canonical, validated binding for a trusted provider construction."""

    provider_profile_id: str
    provider: str
    profile: str
    model: str
    role: str
    config_sha256: str
    capability_sha256: str
    currency: str
    max_input_tokens: int
    max_output_tokens: int
    max_cost: str
    max_attempts: int
    max_wall_time_ms: int
    host_id: str
    pid: int
    process_started_at_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "provider_profile_id",
            _identifier(self.provider_profile_id, "provider_profile_id"),
        )
        object.__setattr__(
            self,
            "provider",
            _identifier(self.provider, "provider"),
        )
        object.__setattr__(
            self,
            "profile",
            _identifier(self.profile, "profile"),
        )
        object.__setattr__(
            self,
            "model",
            _identifier(self.model, "model"),
        )
        object.__setattr__(
            self,
            "role",
            _identifier(self.role, "role"),
        )
        object.__setattr__(
            self,
            "config_sha256",
            _sha256_hex(self.config_sha256, "config_sha256"),
        )
        object.__setattr__(
            self,
            "capability_sha256",
            _sha256_hex(self.capability_sha256, "capability_sha256"),
        )
        object.__setattr__(
            self,
            "currency",
            _identifier(self.currency, "currency"),
        )
        object.__setattr__(
            self,
            "max_input_tokens",
            _bounded_int(
                self.max_input_tokens,
                "max_input_tokens",
                minimum=0,
                maximum=_MAX_INT,
            ),
        )
        object.__setattr__(
            self,
            "max_output_tokens",
            _bounded_int(
                self.max_output_tokens,
                "max_output_tokens",
                minimum=0,
                maximum=_MAX_INT,
            ),
        )
        object.__setattr__(
            self,
            "max_cost",
            _cost_text(self.max_cost),
        )
        object.__setattr__(
            self,
            "max_attempts",
            _bounded_int(
                self.max_attempts,
                "max_attempts",
                minimum=1,
                maximum=100,
            ),
        )
        object.__setattr__(
            self,
            "max_wall_time_ms",
            _bounded_int(
                self.max_wall_time_ms,
                "max_wall_time_ms",
                minimum=1,
                maximum=_MAX_INT,
            ),
        )
        object.__setattr__(
            self,
            "host_id",
            _identifier(self.host_id, "host_id"),
        )
        object.__setattr__(
            self,
            "pid",
            _bounded_int(
                self.pid,
                "pid",
                minimum=1,
                maximum=_MAX_PID,
            ),
        )
        object.__setattr__(
            self,
            "process_started_at_ns",
            _bounded_int(
                self.process_started_at_ns,
                "process_started_at_ns",
                minimum=1,
                maximum=_MAX_INT,
            ),
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": _SCHEMA_VERSION,
            "provider_profile_id": self.provider_profile_id,
            "provider": self.provider,
            "profile": self.profile,
            "model": self.model,
            "role": self.role,
            "config_sha256": self.config_sha256,
            "capability_sha256": self.capability_sha256,
            "pricing": {
                "currency": self.currency,
                "max_input_tokens": self.max_input_tokens,
                "max_output_tokens": self.max_output_tokens,
                "max_cost": self.max_cost,
            },
            "retry": {
                "max_attempts": self.max_attempts,
                "max_wall_time_ms": self.max_wall_time_ms,
            },
            "spawn_identity": {
                "host_id": self.host_id,
                "pid": self.pid,
                "process_started_at_ns": self.process_started_at_ns,
            },
        }


def invocation_binding_sha256(binding: TrustedInvocationBinding) -> str:
    """Return the deterministic identity hash of one trusted binding."""
    if type(binding) is not TrustedInvocationBinding:
        raise TypeError("binding must be a TrustedInvocationBinding")
    canonical = json.dumps(
        binding.to_payload(),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(
        _DOMAIN + b"\0" + canonical.encode("utf-8")
    ).hexdigest()


def build_invocation_binding(
    *,
    member: object,
    limits: object,
    identity_provider: ProcessIdentityProvider,
) -> TrustedInvocationBinding:
    """Derive the trusted binding from a frozen roster member and call limits."""
    for attr in (
        "member_id",
        "provider",
        "profile",
        "model",
        "role",
        "config_sha256",
        "capability_sha256",
    ):
        if not hasattr(member, attr):
            raise InvocationBindingError(f"roster member is missing {attr}")
    for attr in (
        "currency",
        "max_input_tokens",
        "max_output_tokens",
        "max_cost",
        "max_wall_time_ms",
        "max_attempts",
    ):
        if not hasattr(limits, attr):
            raise InvocationBindingError(f"model call limits are missing {attr}")
    if not isinstance(identity_provider, ProcessIdentityProvider):
        raise TypeError(
            "identity_provider must provide current and probe methods"
        )
    current = identity_provider.current()
    if not isinstance(current, ProcessIdentity):
        raise TypeError("current process identity is invalid")
    return TrustedInvocationBinding(
        provider_profile_id=member.profile,
        provider=member.provider,
        profile=member.profile,
        model=member.model,
        role=member.role,
        config_sha256=member.config_sha256,
        capability_sha256=member.capability_sha256,
        currency=limits.currency,
        max_input_tokens=limits.max_input_tokens,
        max_output_tokens=limits.max_output_tokens,
        max_cost=limits.max_cost,
        max_attempts=limits.max_attempts,
        max_wall_time_ms=limits.max_wall_time_ms,
        host_id=current.host_id,
        pid=current.pid,
        process_started_at_ns=current.process_started_at_ns,
    )


def require_provider_binding(
    provider: object,
    binding: TrustedInvocationBinding,
) -> None:
    """Fail closed when an injected provider disagrees with the binding."""
    if type(binding) is not TrustedInvocationBinding:
        raise TypeError("binding must be a TrustedInvocationBinding")
    if not callable(getattr(provider, "invoke", None)):
        raise TypeError("provider must expose a callable invoke method")
    provider_identity = tuple(
        getattr(provider, field_name, None)
        for field_name in (
            "provider_name",
            "profile",
            "model",
            "config_sha256",
            "capability_sha256",
        )
    )
    if any(type(value) is not str for value in provider_identity):
        raise InvocationBindingError("provider binding identity is invalid")
    expected = (
        binding.provider,
        binding.profile,
        binding.model,
        binding.config_sha256,
        binding.capability_sha256,
    )
    if provider_identity != expected:
        raise InvocationBindingError(
            "provider binding conflicts with the frozen roster"
        )


def verify_spawn_identity(
    binding: TrustedInvocationBinding,
    identity_provider: ProcessIdentityProvider,
) -> None:
    """Re-verify the live process identity against the binding's spawn identity."""
    if type(binding) is not TrustedInvocationBinding:
        raise TypeError("binding must be a TrustedInvocationBinding")
    if not isinstance(identity_provider, ProcessIdentityProvider):
        raise TypeError(
            "identity_provider must provide current and probe methods"
        )
    current = identity_provider.current()
    if not isinstance(current, ProcessIdentity):
        raise TypeError("current process identity is invalid")
    observed = (
        current.host_id,
        current.pid,
        current.process_started_at_ns,
    )
    expected = (
        binding.host_id,
        binding.pid,
        binding.process_started_at_ns,
    )
    if observed != expected:
        raise InvocationBindingError(
            "spawn identity conflicts with the current process"
        )


def require_invocation_binding(
    *,
    provider: object,
    member: object,
    limits: object,
    identity_provider: ProcessIdentityProvider,
) -> TrustedInvocationBinding:
    """Build and verify the trusted binding for one provider construction."""
    binding = build_invocation_binding(
        member=member,
        limits=limits,
        identity_provider=identity_provider,
    )
    require_provider_binding(provider, binding)
    return binding


def construct_provider(
    binding: TrustedInvocationBinding,
    factory: Callable[[dict[str, object]], object],
) -> object:
    """Construct a provider only from a validated trusted binding."""
    if type(binding) is not TrustedInvocationBinding:
        raise TypeError(
            "provider construction requires a TrustedInvocationBinding"
        )
    if not callable(factory):
        raise TypeError("provider factory must be callable")
    provider = factory(binding.to_payload())
    require_provider_binding(provider, binding)
    return provider


__all__ = [
    "InvocationBindingError",
    "TrustedInvocationBinding",
    "build_invocation_binding",
    "construct_provider",
    "invocation_binding_sha256",
    "require_invocation_binding",
    "require_provider_binding",
    "verify_spawn_identity",
]
