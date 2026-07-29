"""Generation-bound identities shared by derived cache formats."""

from __future__ import annotations

import hashlib
import re
from typing import Literal

from pydantic import field_validator, model_validator

from research_automation.control_plane.contracts import canonical_json
from research_automation.foundations.artifact_identity import ArtifactIdentity
from research_automation.foundations.contract_registry import StrictContractModel

from .generation import GenerationPin


CACHE_IDENTITY_V1 = "research.data_generation.cache_identity.v1"
_CACHE_ID_DOMAIN = b"research.data_generation.cache_identity.v1\0"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CACHE_NAMESPACES = frozenset({"production", "research"})
_CACHE_KINDS = frozenset({"raw_parquet", "indicator", "signal"})


class CacheIdentity(StrictContractModel):
    """Bind one cache artifact to its generation and interpretation contract."""

    schema_version: Literal["research.data_generation.cache_identity.v1"]
    generation_id: str
    data_snapshot_id: str
    cache_namespace: Literal["production", "research"]
    cache_kind: Literal["raw_parquet", "indicator", "signal"]
    artifact: ArtifactIdentity
    source_artifact_ids: tuple[str, ...]
    feature_contract_id: str
    trading_calendar_identity: str
    point_in_time_universe_identity: str
    adjustment_scheme: str

    @field_validator(
        "generation_id",
        "data_snapshot_id",
        "feature_contract_id",
    )
    @classmethod
    def _require_sha256(cls, value: str) -> str:
        if _SHA256.fullmatch(value) is None:
            raise ValueError("cache identity digest must be lowercase SHA-256")
        return value

    @field_validator("source_artifact_ids")
    @classmethod
    def _require_ordered_source_ids(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        if not value:
            raise ValueError("source artifact ids must not be empty")
        if any(_SHA256.fullmatch(item) is None for item in value):
            raise ValueError("source artifact id must be lowercase SHA-256")
        if tuple(sorted(value)) != value or len(value) != len(set(value)):
            raise ValueError("source artifact ids must be sorted and unique")
        return value

    @field_validator(
        "trading_calendar_identity",
        "point_in_time_universe_identity",
        "adjustment_scheme",
    )
    @classmethod
    def _require_canonical_text(cls, value: str) -> str:
        if not value or value != value.strip():
            raise ValueError("cache identity text must be canonical")
        return value

    @model_validator(mode="after")
    def _bind_generation(self) -> CacheIdentity:
        if self.data_snapshot_id != self.generation_id:
            raise ValueError("data snapshot must equal the pinned generation")
        if self.artifact.generation != self.generation_id:
            raise ValueError("cache artifact must bind the pinned generation")
        if self.artifact.kind != f"{self.cache_kind}_cache":
            raise ValueError("cache artifact kind must match cache_kind")
        return self

    @property
    def cache_id(self) -> str:
        payload = canonical_json(self.model_dump(mode="json")).encode("utf-8")
        return hashlib.sha256(_CACHE_ID_DOMAIN + payload).hexdigest()


def _validate_build_request(
    *,
    cache_namespace: str,
    cache_kind: str,
    source_artifact_ids: tuple[str, ...],
    feature_contract_id: str,
    content_schema: str,
    producer: str,
    logical_role: str,
) -> tuple[str, ...]:
    """Reject pure request errors before a pin records touched semantics."""
    if cache_namespace not in _CACHE_NAMESPACES:
        raise ValueError("unsupported cache namespace")
    if cache_kind not in _CACHE_KINDS:
        raise ValueError("unsupported cache kind")
    if not isinstance(source_artifact_ids, tuple):
        raise ValueError("source artifact ids must be a tuple")
    if not source_artifact_ids:
        raise ValueError("source artifact ids must not be empty")
    if any(
        type(artifact_id) is not str
        or _SHA256.fullmatch(artifact_id) is None
        for artifact_id in source_artifact_ids
    ):
        raise ValueError("source artifact id must be lowercase SHA-256")
    if len(source_artifact_ids) != len(set(source_artifact_ids)):
        raise ValueError("source artifact ids must be unique")
    if (
        type(feature_contract_id) is not str
        or _SHA256.fullmatch(feature_contract_id) is None
    ):
        raise ValueError("feature contract id must be lowercase SHA-256")
    if any(
        type(value) is not str or not value or value != value.strip()
        for value in (content_schema, producer, logical_role)
    ):
        raise ValueError("cache artifact semantics must be canonical")
    return tuple(sorted(source_artifact_ids))


def build_cache_identity(
    pin: GenerationPin,
    *,
    relative_path: str,
    cache_namespace: Literal["production", "research"],
    cache_kind: Literal["raw_parquet", "indicator", "signal"],
    source_artifact_ids: tuple[str, ...],
    feature_contract_id: str,
    content_schema: str,
    producer: str,
    logical_role: str,
) -> CacheIdentity:
    """Content-bind one touched cache without scanning its surrounding tree."""
    if not isinstance(pin, GenerationPin):
        raise TypeError("pin must be a GenerationPin")
    verified_request_sources = _validate_build_request(
        cache_namespace=cache_namespace,
        cache_kind=cache_kind,
        source_artifact_ids=source_artifact_ids,
        feature_contract_id=feature_contract_id,
        content_schema=content_schema,
        producer=producer,
        logical_role=logical_role,
    )
    artifact = pin.verify_artifact(
        relative_path,
        content_schema=content_schema,
        producer=producer,
        kind=f"{cache_kind}_cache",
        logical_role=logical_role,
    )
    verified_source_ids = pin.verify_touched_artifact_ids(
        verified_request_sources,
        exclude_artifact_id=artifact.artifact_id,
    )
    manifest = pin.manifest
    return CacheIdentity(
        schema_version=CACHE_IDENTITY_V1,
        generation_id=pin.generation_id,
        data_snapshot_id=pin.data_snapshot_id,
        cache_namespace=cache_namespace,
        cache_kind=cache_kind,
        artifact=artifact,
        source_artifact_ids=verified_source_ids,
        feature_contract_id=feature_contract_id,
        trading_calendar_identity=manifest.trading_calendar_identity,
        point_in_time_universe_identity=(
            manifest.point_in_time_universe_identity
        ),
        adjustment_scheme=manifest.adjustment_scheme,
    )


__all__ = [
    "CACHE_IDENTITY_V1",
    "CacheIdentity",
    "build_cache_identity",
]
