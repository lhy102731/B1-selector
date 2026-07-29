"""Strict contracts for immutable market-data generations."""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Literal

from pydantic import field_validator

from research_automation.control_plane.contracts import canonical_json
from research_automation.foundations.contract_registry import (
    ContractRegistry,
    StrictContractModel,
)


GENERATION_MANIFEST_V1 = "research.data_generation.generation_manifest.v1"
_GENERATION_ID_DOMAIN = b"research.data_generation.generation_manifest.v1\0"


class GenerationManifest(StrictContractModel):
    """Identity bindings required to interpret one data generation."""

    schema_version: Literal[
        "research.data_generation.generation_manifest.v1"
    ]
    csv_cutoff: str
    trading_calendar_identity: str
    point_in_time_universe_identity: str
    adjustment_scheme: str
    missing_data_policy: str
    cache_manifest_references: tuple[str, ...]

    @field_validator(
        "csv_cutoff",
        "trading_calendar_identity",
        "point_in_time_universe_identity",
        "adjustment_scheme",
        "missing_data_policy",
    )
    @classmethod
    def _require_canonical_text(cls, value: str) -> str:
        if not value or value != value.strip():
            raise ValueError("generation identity text must be canonical")
        return value

    @field_validator("csv_cutoff")
    @classmethod
    def _require_canonical_cutoff_date(cls, value: str) -> str:
        try:
            parsed = date.fromisoformat(value)
        except ValueError as error:
            raise ValueError("csv_cutoff must be a valid ISO date") from error
        if parsed.isoformat() != value:
            raise ValueError("csv_cutoff must use YYYY-MM-DD")
        return value

    @field_validator("cache_manifest_references")
    @classmethod
    def _require_canonical_cache_references(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        if any(
            not reference or reference != reference.strip()
            for reference in value
        ):
            raise ValueError("cache manifest references must be canonical")
        if len(value) != len(set(value)):
            raise ValueError("cache manifest references must be unique")
        return value

    @property
    def generation_id(self) -> str:
        payload = canonical_json(self.model_dump(mode="json")).encode("utf-8")
        return hashlib.sha256(_GENERATION_ID_DOMAIN + payload).hexdigest()


def generation_contract_registry() -> ContractRegistry:
    """Return the versioned registry for data-generation contracts."""
    return ContractRegistry(
        version="research.data_generation.contract_registry.v1",
        contracts={GENERATION_MANIFEST_V1: GenerationManifest},
    )


__all__ = [
    "GENERATION_MANIFEST_V1",
    "GenerationManifest",
    "generation_contract_registry",
]
