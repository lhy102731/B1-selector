"""Strict contracts for immutable market-data generations."""

from __future__ import annotations

from typing import Literal

from research_automation.foundations.contract_registry import (
    ContractRegistry,
    StrictContractModel,
)


GENERATION_MANIFEST_V1 = "research.data_generation.generation_manifest.v1"


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
