"""Controller-owned provenance stamps for legacy research outputs.

P0R2 does not retroactively promote artifacts produced by the old runners.
Every result emitted by a legacy entry point therefore carries an explicit,
non-overridable provenance stamp.  The helper returns a copy so callers cannot
silently mutate a shared payload while applying the stamp.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


LEGACY_RESULT_PROVENANCE: dict[str, object] = {
    "controller_created": False,
    "trust_state": "legacy_unaudited",
    "promotion_eligible": False,
}


def stamp_legacy_result(payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return ``payload`` with the immutable legacy provenance fields applied."""
    if payload is None:
        result: dict[str, Any] = {}
    elif isinstance(payload, Mapping):
        result = dict(payload)
    else:
        raise TypeError("legacy result payload must be a mapping or None")
    # Deliberately overwrite caller-provided values.  A legacy runner cannot
    # self-assert controller provenance or promotion eligibility.
    result.update(LEGACY_RESULT_PROVENANCE)
    return result


__all__ = ["LEGACY_RESULT_PROVENANCE", "stamp_legacy_result"]
