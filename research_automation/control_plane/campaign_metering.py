"""campaign_metering.py - deterministic resource observations for fenced execution.

The P6 Campaign controller freezes one deterministic ResourceObservation into
each Cycle's operational execution usage receipt. The observation is bounded,
canonically serialized, and never leaves the control-plane boundary:

- ResourceObservation holds three non-negative integer counters:
  tool_attempts, data_exposures and disk_growth_bytes;
- to_payload / from_payload are canonical and round-trip stable;
- resource_observation_sha256 binds the observation deterministically;
- validate_resource_observation fails closed whenever an observed counter
  exceeds its reservation limit (BudgetExceededError is raised by the
  controller around ResourceObservationLimitError).

This module performs no I/O, no subprocess, and no provider SDK imports.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

_MAX_RESOURCE_COUNTER = 2**63 - 1

_SCHEMA_VERSION = "control_plane.resource_observation.v1"
_DOMAIN = b"control_plane.resource_observation.v1"


class ResourceObservationLimitError(ValueError):
    """Raised when a deterministic observation exceeds a reservation limit."""


@dataclass(frozen=True, slots=True)
class ResourceObservation:
    """Deterministic, bounded resource counters observed for one Cycle."""

    tool_attempts: int
    data_exposures: int
    disk_growth_bytes: int

    def __post_init__(self) -> None:
        for field_name, value in (
            ("tool_attempts", self.tool_attempts),
            ("data_exposures", self.data_exposures),
            ("disk_growth_bytes", self.disk_growth_bytes),
        ):
            if (
                type(value) is not int
                or value < 0
                or value > _MAX_RESOURCE_COUNTER
            ):
                raise ValueError(
                    f"{field_name} must be an integer from 0 through "
                    f"{_MAX_RESOURCE_COUNTER}"
                )

    @classmethod
    def zero(cls) -> "ResourceObservation":
        return cls(tool_attempts=0, data_exposures=0, disk_growth_bytes=0)

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": _SCHEMA_VERSION,
            "tool_attempts": self.tool_attempts,
            "data_exposures": self.data_exposures,
            "disk_growth_bytes": self.disk_growth_bytes,
        }

    @classmethod
    def from_payload(cls, payload: object) -> "ResourceObservation":
        if (
            not isinstance(payload, dict)
            or set(payload)
            != {
                "schema_version",
                "tool_attempts",
                "data_exposures",
                "disk_growth_bytes",
            }
            or payload.get("schema_version") != _SCHEMA_VERSION
        ):
            raise ValueError("resource observation payload is invalid")
        return cls(
            tool_attempts=payload["tool_attempts"],
            data_exposures=payload["data_exposures"],
            disk_growth_bytes=payload["disk_growth_bytes"],
        )

    def to_canonical_json(self) -> str:
        return json.dumps(
            self.to_payload(),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )


def resource_observation_sha256(
    observation: ResourceObservation,
) -> str:
    """Return the deterministic identity hash of one observation."""
    if type(observation) is not ResourceObservation:
        raise TypeError("observation must be a ResourceObservation")
    return hashlib.sha256(
        _DOMAIN + b"\0" + observation.to_canonical_json().encode("utf-8")
    ).hexdigest()


def validate_resource_observation(
    observation: ResourceObservation,
    *,
    max_tool_attempts: int,
    max_data_exposures: int,
    max_disk_growth_bytes: int,
) -> None:
    """Fail closed when any observed counter exceeds its limit."""
    if type(observation) is not ResourceObservation:
        raise TypeError("observation must be a ResourceObservation")
    for field_name, observed, limit in (
        ("tool_attempts", observation.tool_attempts, max_tool_attempts),
        ("data_exposures", observation.data_exposures, max_data_exposures),
        (
            "disk_growth_bytes",
            observation.disk_growth_bytes,
            max_disk_growth_bytes,
        ),
    ):
        if type(limit) is not int or limit < 0:
            raise ValueError(
                f"{field_name} limit must be a non-negative integer"
            )
        if observed > limit:
            raise ResourceObservationLimitError(
                f"resource observation {field_name}={observed} exceeds "
                f"limit {limit}"
            )


__all__ = [
    "ResourceObservation",
    "ResourceObservationLimitError",
    "resource_observation_sha256",
    "validate_resource_observation",
]
