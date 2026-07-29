"""Generation-bound identities shared by derived cache formats."""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import tempfile
from pathlib import Path
from typing import Literal

from pydantic import field_validator, model_validator

from research_automation.control_plane.contracts import canonical_json
from research_automation.foundations.artifact_identity import ArtifactIdentity
from research_automation.foundations.contract_registry import (
    ContractRegistry,
    ContractValidationError,
    StrictContractModel,
)

from .generation import GenerationMutatedError, GenerationPin


CACHE_IDENTITY_V1 = "research.data_generation.cache_identity.v1"
MAX_CACHE_IDENTITY_BYTES = 1024 * 1024
_CACHE_ID_DOMAIN = b"research.data_generation.cache_identity.v1\0"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CACHE_NAMESPACES = frozenset({"production", "research"})
_CACHE_KINDS = frozenset({"raw_parquet", "indicator", "signal"})
_SIDECAR_SUFFIX = ".cache-identity.json"


class CacheIdentitySidecarError(ValueError):
    """Base error for missing, malformed, or byte-mismatched sidecars."""


class CacheIdentityMissingError(CacheIdentitySidecarError):
    """Raised when a trusted cache has no identity sidecar."""


class CacheIdentityInvalidError(CacheIdentitySidecarError):
    """Raised when sidecar bytes fail the strict cache contract."""


class CacheIdentityMismatchError(CacheIdentitySidecarError):
    """Raised when cache bytes do not match the supplied identity."""


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


def cache_identity_contract_registry() -> ContractRegistry:
    """Return the strict versioned parser for cache identity sidecars."""
    return ContractRegistry(
        version="research.data_generation.cache_identity_registry.v1",
        contracts={CACHE_IDENTITY_V1: CacheIdentity},
        max_json_bytes=MAX_CACHE_IDENTITY_BYTES,
    )


def cache_identity_sidecar_path(cache_path: str | Path) -> Path:
    """Return the unambiguous adjacent identity path for one cache file."""
    path = Path(cache_path)
    if not path.name:
        raise ValueError("cache path must name a file")
    return path.with_name(f"{path.name}{_SIDECAR_SUFFIX}")


def _is_reparse_path(path: Path) -> bool:
    try:
        return path.is_symlink() or getattr(
            path,
            "is_junction",
            lambda: False,
        )()
    except OSError:
        return True


def read_cache_identity_sidecar(cache_path: str | Path) -> CacheIdentity:
    """Strictly parse one cache identity sidecar without trusting its claims."""
    sidecar = cache_identity_sidecar_path(cache_path)
    if _is_reparse_path(sidecar):
        raise CacheIdentityInvalidError("CACHE_IDENTITY_INVALID")
    try:
        raw = sidecar.read_bytes()
    except FileNotFoundError as error:
        raise CacheIdentityMissingError("CACHE_IDENTITY_MISSING") from error
    except OSError as error:
        raise CacheIdentityInvalidError("CACHE_IDENTITY_INVALID") from error
    try:
        parsed = cache_identity_contract_registry().parse_json(
            CACHE_IDENTITY_V1,
            raw,
        )
    except ContractValidationError as error:
        raise CacheIdentityInvalidError("CACHE_IDENTITY_INVALID") from error
    if not isinstance(parsed, CacheIdentity):
        raise CacheIdentityInvalidError("CACHE_IDENTITY_INVALID")
    return parsed


def write_cache_identity_sidecar(
    pin: GenerationPin,
    *,
    relative_path: str,
    identity: CacheIdentity,
) -> Path:
    """Atomically replace a sidecar only after rechecking exact cache bytes."""
    if not isinstance(pin, GenerationPin):
        raise TypeError("pin must be a GenerationPin")
    if not isinstance(identity, CacheIdentity):
        raise TypeError("identity must be a CacheIdentity")
    payload = canonical_json(identity.model_dump(mode="json")).encode("utf-8")
    try:
        parsed = cache_identity_contract_registry().parse_json(
            CACHE_IDENTITY_V1,
            payload,
        )
    except ContractValidationError as error:
        raise CacheIdentityInvalidError("CACHE_IDENTITY_INVALID") from error
    if parsed != identity:
        raise CacheIdentityInvalidError("CACHE_IDENTITY_INVALID")
    try:
        current = pin.verify_artifact(
            relative_path,
            content_schema=identity.artifact.content_schema,
            producer=identity.artifact.producer,
            kind=identity.artifact.kind,
            logical_role=identity.artifact.logical_role,
        )
        pin.verify_touched_artifact_ids(
            identity.source_artifact_ids,
            exclude_artifact_id=identity.artifact.artifact_id,
        )
    except (GenerationMutatedError, ValueError) as error:
        raise CacheIdentityMismatchError("CACHE_IDENTITY_MISMATCH") from error
    if not hmac.compare_digest(
        current.artifact_id,
        identity.artifact.artifact_id,
    ):
        raise CacheIdentityMismatchError("CACHE_IDENTITY_MISMATCH")

    cache_path = pin.artifact_path(relative_path)
    destination = cache_identity_sidecar_path(cache_path)
    if _is_reparse_path(destination):
        raise CacheIdentityInvalidError("CACHE_IDENTITY_INVALID")
    descriptor: int | None = None
    temporary_path: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
        )
        temporary_path = Path(temporary_name)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, destination)
        temporary_path = None
    except OSError as error:
        raise CacheIdentitySidecarError("CACHE_IDENTITY_WRITE_FAILED") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    if read_cache_identity_sidecar(cache_path) != identity:
        raise CacheIdentityInvalidError("CACHE_IDENTITY_INVALID")
    return destination


def load_verified_cache_identity(
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
    """Rebuild the expected pinned identity and match one strict sidecar."""
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
    try:
        expected = build_cache_identity(
            pin,
            relative_path=relative_path,
            cache_namespace=cache_namespace,
            cache_kind=cache_kind,
            source_artifact_ids=verified_request_sources,
            feature_contract_id=feature_contract_id,
            content_schema=content_schema,
            producer=producer,
            logical_role=logical_role,
        )
    except GenerationMutatedError as error:
        raise CacheIdentityMismatchError("CACHE_IDENTITY_MISMATCH") from error
    stored = read_cache_identity_sidecar(pin.artifact_path(relative_path))
    if not hmac.compare_digest(stored.cache_id, expected.cache_id):
        raise CacheIdentityMismatchError("CACHE_IDENTITY_MISMATCH")
    return stored


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
    verified_source_ids = pin.verify_touched_artifact_ids(
        verified_request_sources,
    )
    artifact = pin.verify_artifact(
        relative_path,
        content_schema=content_schema,
        producer=producer,
        kind=f"{cache_kind}_cache",
        logical_role=logical_role,
    )
    if artifact.artifact_id in verified_source_ids:
        raise ValueError("cache artifact cannot be its own source")
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
    "MAX_CACHE_IDENTITY_BYTES",
    "CacheIdentity",
    "CacheIdentityInvalidError",
    "CacheIdentityMismatchError",
    "CacheIdentityMissingError",
    "CacheIdentitySidecarError",
    "build_cache_identity",
    "cache_identity_contract_registry",
    "cache_identity_sidecar_path",
    "load_verified_cache_identity",
    "read_cache_identity_sidecar",
    "write_cache_identity_sidecar",
]
