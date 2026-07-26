"""Mechanical, fail-closed phase-gate reports for P0 through P8."""

from __future__ import annotations

from collections.abc import Mapping


_COMPUTED_FIELDS = frozenset(
    {
        "schema_version",
        "verdict",
        "reason_codes",
        "auto_advance",
        "created_at",
        "gate_report_sha256",
    }
)


class GateError(RuntimeError):
    """Base error for generic phase-gate operations."""


class GateBuildError(GateError):
    """Raised when an untrusted gate draft cannot be built safely."""


class PhaseGateBuilder:
    """Build a gate candidate while keeping verdict fields controller-owned."""

    __slots__ = ()

    def build(self, draft: Mapping[str, object]) -> dict[str, object]:
        if not isinstance(draft, Mapping):
            raise GateBuildError("gate draft must be a mapping")
        supplied_computed_fields = set(draft) & _COMPUTED_FIELDS
        if supplied_computed_fields:
            names = ", ".join(sorted(supplied_computed_fields))
            raise GateBuildError(
                f"gate draft contains computed fields: {names}"
            )
        raise GateBuildError("gate draft is incomplete")


__all__ = [
    "GateBuildError",
    "GateError",
    "PhaseGateBuilder",
]
