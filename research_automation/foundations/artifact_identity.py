"""Location-independent identities for research artifacts."""

from __future__ import annotations

import hashlib
import hmac
import os
import stat as stat_module
from pathlib import Path
from collections.abc import Iterable
from typing import Annotated, Literal

from pydantic import Field, StringConstraints, field_validator, model_validator

from research_automation.control_plane.contracts import canonical_json

from .contract_registry import StrictContractModel


ARTIFACT_IDENTITY_V1 = "research.artifact_identity.v1"
_ARTIFACT_ID_DOMAIN = b"research.artifact_identity.v1\0"
_INVENTORY_FINGERPRINT_DOMAIN = b"research.inventory_fingerprint.v1\0"
_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_WINDOWS_RESERVED_STEMS = frozenset(
    {"con", "prn", "aux", "nul", "clock$"}
    | {f"com{number}" for number in range(1, 10)}
    | {f"lpt{number}" for number in range(1, 10)}
)
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
NonNegativeInt = Annotated[int, Field(ge=0)]


def _normalize_relative_path(value: str) -> str:
    if not value or value != value.strip():
        raise ValueError("artifact path must be a canonical relative path")
    normalized = value.replace("\\", "/")
    parts = normalized.split("/")
    if (
        normalized.startswith("/")
        or ":" in normalized
        or any(part in {"", ".", ".."} for part in parts)
        or any(character in '<>"|?*' for character in normalized)
        or any(ord(character) < 32 or ord(character) == 127 for character in normalized)
        or any(part.endswith((" ", ".")) for part in parts)
        or any(part.split(".", 1)[0].casefold() in _WINDOWS_RESERVED_STEMS for part in parts)
    ):
        raise ValueError("artifact path must stay relative and Windows-safe")
    return normalized


class ArtifactLocator(StrictContractModel):
    """Observed location metadata; never part of an artifact content ID."""

    schema_version: Literal["research.artifact_locator.v1"]
    storage_root: str
    path: str
    size_bytes: NonNegativeInt
    mtime_ns: NonNegativeInt

    @field_validator("storage_root")
    @classmethod
    def _canonical_absolute_root(cls, value: str) -> str:
        if not value or value != value.strip():
            raise ValueError("storage_root must be a canonical absolute path")
        normalized = value.replace("\\", "/")
        root_path = Path(normalized)
        if not root_path.is_absolute():
            raise ValueError("storage_root must be absolute")
        canonical = root_path.as_posix()
        anchor = Path(root_path.anchor).as_posix()
        return canonical if canonical == anchor else canonical.rstrip("/")

    @field_validator("path")
    @classmethod
    def _canonical_relative_path(cls, value: str) -> str:
        return _normalize_relative_path(value)


class ArtifactLocationError(ValueError):
    """Raised when locator bytes are missing, unsafe, or changed in-flight."""


class DirectoryManifestEntry(StrictContractModel):
    """One fully hashed file in a directory manifest."""

    path: str
    content_sha256: Sha256
    byte_length: NonNegativeInt

    @field_validator("path")
    @classmethod
    def _canonical_path(cls, value: str) -> str:
        return _normalize_relative_path(value)


class DirectoryManifest(StrictContractModel):
    """Canonical ordered file manifest used as directory content."""

    schema_version: Literal["research.directory_manifest.v1"]
    entries: tuple[DirectoryManifestEntry, ...]

    @model_validator(mode="after")
    def _require_ordered_unique_paths(self) -> "DirectoryManifest":
        paths = [entry.path for entry in self.entries]
        if paths != sorted(paths):
            raise ValueError("directory manifest entries must be sorted by path")
        folded = [path.casefold() for path in paths]
        if len(folded) != len(set(folded)):
            raise ValueError("directory manifest paths must be Windows-unique")
        return self


class InventoryOnlyFingerprint(StrictContractModel):
    """Cheap locator fingerprint that is structurally ineligible for authority."""

    schema_version: Literal["research.inventory_fingerprint.v1"]
    profile: Literal["locator_size_mtime.v1"]
    locator: ArtifactLocator
    fingerprint_sha256: Sha256
    authorization_eligible: Literal[False]


class ArtifactIdentity(StrictContractModel):
    """Semantic and byte identity; storage location is intentionally absent."""

    schema_version: Literal["research.artifact_identity.v1"]
    content_sha256: Sha256
    byte_length: NonNegativeInt
    content_schema: str
    producer: str
    generation: str
    kind: str
    logical_role: str

    @field_validator(
        "content_schema",
        "producer",
        "generation",
        "kind",
        "logical_role",
    )
    @classmethod
    def _require_canonical_text(cls, value: str) -> str:
        if not value or value != value.strip():
            raise ValueError("identity text fields must be non-empty canonical strings")
        return value

    @property
    def artifact_id(self) -> str:
        payload = canonical_json(self.model_dump(mode="json")).encode("utf-8")
        return hashlib.sha256(_ARTIFACT_ID_DOMAIN + payload).hexdigest()


class ArtifactIdentityMismatchError(ValueError):
    """Raised when current bytes differ from a supplied full identity."""


def artifact_identity_from_bytes(
    content: bytes,
    *,
    content_schema: str,
    producer: str,
    generation: str,
    kind: str,
    logical_role: str,
) -> ArtifactIdentity:
    """Build a full identity from exact bytes and explicit semantic bindings."""
    if not isinstance(content, bytes):
        raise TypeError("artifact content must be bytes")
    return ArtifactIdentity(
        schema_version=ARTIFACT_IDENTITY_V1,
        content_sha256=hashlib.sha256(content).hexdigest(),
        byte_length=len(content),
        content_schema=content_schema,
        producer=producer,
        generation=generation,
        kind=kind,
        logical_role=logical_role,
    )


def build_directory_manifest(
    entries: Iterable[DirectoryManifestEntry],
) -> DirectoryManifest:
    """Canonicalize an explicit entry collection without scanning a directory."""
    collected = tuple(entries)
    if not all(isinstance(entry, DirectoryManifestEntry) for entry in collected):
        raise TypeError("directory entries must be DirectoryManifestEntry values")
    return DirectoryManifest(
        schema_version="research.directory_manifest.v1",
        entries=tuple(sorted(collected, key=lambda entry: entry.path)),
    )


def directory_identity_from_manifest(
    manifest: DirectoryManifest,
    *,
    producer: str,
    generation: str,
    logical_role: str,
) -> ArtifactIdentity:
    """Identify a directory by the canonical bytes of its ordered manifest."""
    if not isinstance(manifest, DirectoryManifest):
        raise TypeError("manifest must be a DirectoryManifest")
    manifest_bytes = canonical_json(manifest.model_dump(mode="json")).encode("utf-8")
    return artifact_identity_from_bytes(
        manifest_bytes,
        content_schema="research.directory_manifest.v1",
        producer=producer,
        generation=generation,
        kind="directory",
        logical_role=logical_role,
    )


def inventory_fingerprint(locator: ArtifactLocator) -> InventoryOnlyFingerprint:
    """Fingerprint locator metadata without claiming the underlying content."""
    if not isinstance(locator, ArtifactLocator):
        raise TypeError("locator must be an ArtifactLocator")
    payload = canonical_json(locator.model_dump(mode="json")).encode("utf-8")
    return InventoryOnlyFingerprint(
        schema_version="research.inventory_fingerprint.v1",
        profile="locator_size_mtime.v1",
        locator=locator,
        fingerprint_sha256=hashlib.sha256(
            _INVENTORY_FINGERPRINT_DOMAIN + payload
        ).hexdigest(),
        authorization_eligible=False,
    )


def _is_reparse_point(path: Path) -> bool:
    try:
        if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except OSError as error:
        raise ArtifactLocationError(f"unable to inspect artifact path: {path}") from error
    return bool(attributes & _FILE_ATTRIBUTE_REPARSE_POINT)


def resolve_bounded_locator(locator: ArtifactLocator) -> Path:
    """Resolve a file locator without following a reparse point inside its root."""
    root = Path(locator.storage_root)
    if _is_reparse_point(root) or not root.is_dir():
        raise ArtifactLocationError("artifact storage root is unsafe or unavailable")
    current = root
    for component in locator.path.split("/"):
        current = current / component
        if _is_reparse_point(current):
            raise ArtifactLocationError("artifact path contains a reparse point")
    try:
        resolved_root = root.resolve(strict=True)
        resolved = current.resolve(strict=True)
        resolved.relative_to(resolved_root)
    except (OSError, ValueError) as error:
        raise ArtifactLocationError("artifact path escaped or disappeared") from error
    if not resolved.is_file():
        raise ArtifactLocationError("artifact path is not a regular file")
    return resolved


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
    )


def _hash_stable_file(path: Path, locator: ArtifactLocator) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    try:
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            if not stat_module.S_ISREG(before.st_mode):
                raise ArtifactLocationError("artifact path is not a regular file")
            if (
                before.st_size != locator.size_bytes
                or before.st_mtime_ns != locator.mtime_ns
            ):
                raise ArtifactLocationError("artifact locator metadata is stale")
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
                total += len(chunk)
            after = os.fstat(handle.fileno())
        path_after = path.stat()
    except ArtifactLocationError:
        raise
    except OSError as error:
        raise ArtifactLocationError("artifact became unavailable while hashing") from error
    if (
        _stat_identity(before) != _stat_identity(after)
        or _stat_identity(after) != _stat_identity(path_after)
        or total != after.st_size
    ):
        raise ArtifactLocationError("artifact changed while being hashed")
    return digest.hexdigest(), total


def identify_file(
    locator: ArtifactLocator,
    *,
    content_schema: str,
    producer: str,
    generation: str,
    kind: str,
    logical_role: str,
) -> ArtifactIdentity:
    """Hash one explicitly located stable file and bind its semantic identity."""
    if not isinstance(locator, ArtifactLocator):
        raise TypeError("locator must be an ArtifactLocator")
    content_sha256, byte_length = _hash_stable_file(
        resolve_bounded_locator(locator),
        locator,
    )
    return ArtifactIdentity(
        schema_version=ARTIFACT_IDENTITY_V1,
        content_sha256=content_sha256,
        byte_length=byte_length,
        content_schema=content_schema,
        producer=producer,
        generation=generation,
        kind=kind,
        logical_role=logical_role,
    )


def verify_file_identity(
    locator: ArtifactLocator,
    identity: ArtifactIdentity,
) -> None:
    """Re-hash one located file and reject anything but a full exact identity."""
    if not isinstance(identity, ArtifactIdentity):
        raise TypeError("identity must be an ArtifactIdentity")
    current = identify_file(
        locator,
        content_schema=identity.content_schema,
        producer=identity.producer,
        generation=identity.generation,
        kind=identity.kind,
        logical_role=identity.logical_role,
    )
    if not hmac.compare_digest(current.artifact_id, identity.artifact_id):
        raise ArtifactIdentityMismatchError("artifact bytes do not match the identity")


__all__ = [
    "ARTIFACT_IDENTITY_V1",
    "ArtifactLocationError",
    "ArtifactIdentity",
    "ArtifactIdentityMismatchError",
    "ArtifactLocator",
    "DirectoryManifest",
    "DirectoryManifestEntry",
    "InventoryOnlyFingerprint",
    "artifact_identity_from_bytes",
    "build_directory_manifest",
    "directory_identity_from_manifest",
    "identify_file",
    "inventory_fingerprint",
    "resolve_bounded_locator",
    "verify_file_identity",
]
