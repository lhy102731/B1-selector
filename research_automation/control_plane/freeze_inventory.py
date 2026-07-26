"""Stable, read-only construction of P0R2 freeze and inventory artifacts."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from .contracts import canonical_sha256
from .entry_guard import EntryInventory, EntryRecord


_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400


class UnstableInventoryError(RuntimeError):
    """Raised when the bounded executable surface is unsafe or changes in-flight."""


@dataclass(frozen=True, slots=True)
class StableFreezeInventoryArtifacts:
    freeze_manifest: dict[str, object]
    final_inventory: dict[str, object]


@dataclass(frozen=True, slots=True)
class _StableFile:
    sha256: str
    bytes: int
    identity: tuple[int, int, int, int, int]


def _is_reparse_point(path: Path) -> bool:
    try:
        if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except OSError as error:
        raise UnstableInventoryError(
            f"unable to inspect bounded path: {path}"
        ) from error
    return bool(attributes & _FILE_ATTRIBUTE_REPARSE_POINT)


def _resolve_stable_file(root: Path, relative: str) -> Path:
    candidate = root.joinpath(*relative.split("/"))
    current = root
    for component in relative.split("/"):
        current = current / component
        if _is_reparse_point(current):
            raise UnstableInventoryError(
                f"bounded source path contains a reparse point: {relative}"
            )
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as error:
        raise UnstableInventoryError(
            f"bounded source path escaped or disappeared: {relative}"
        ) from error
    if not resolved.is_file():
        raise UnstableInventoryError(
            f"bounded source path is not a regular file: {relative}"
        )
    return resolved


def _read_stable_bytes(
    path: Path,
    relative: str,
) -> tuple[bytes, tuple[int, int, int, int, int]]:
    try:
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
    except OSError as error:
        raise UnstableInventoryError(
            f"bounded source file became unavailable: {relative}"
        ) from error
    before_identity = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    after_identity = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if before_identity != after_identity or len(raw) != after.st_size:
        raise UnstableInventoryError(
            f"bounded source file changed while being read: {relative}"
        )
    return raw, after_identity


def _capture_files(
    root: Path,
    records: tuple[EntryRecord, ...],
) -> dict[str, _StableFile]:
    expected_by_path: dict[str, str] = {}
    for record in records:
        if record.kind == "external_scheduler":
            continue
        if record.content_sha256 is None:
            raise UnstableInventoryError(
                f"bounded entry is missing a content hash: {record.entry_id}"
            )
        previous = expected_by_path.setdefault(record.path, record.content_sha256)
        if previous != record.content_sha256:
            raise UnstableInventoryError(
                f"bounded entries disagree about one file: {record.path}"
            )

    captured: dict[str, _StableFile] = {}
    for relative in sorted(expected_by_path):
        path = _resolve_stable_file(root, relative)
        raw, identity = _read_stable_bytes(path, relative)
        digest = hashlib.sha256(raw).hexdigest()
        if digest != expected_by_path[relative]:
            raise UnstableInventoryError(
                f"bounded source changed during inventory scan: {relative}"
            )
        captured[relative] = _StableFile(
            sha256=digest,
            bytes=len(raw),
            identity=identity,
        )
    return captured


def _entry_payload(record: EntryRecord) -> dict[str, object]:
    if record.content_sha256 is None:
        raise UnstableInventoryError(
            f"entry is missing a content hash: {record.entry_id}"
        )
    return {
        "entry_id": record.entry_id,
        "path": record.path.replace("\\", "/"),
        "kind": record.kind,
        "callable_name": record.callable_name,
        "actor_type": record.actor_type,
        "content_sha256": record.content_sha256,
        "disposition": record.disposition,
        "trust_state": record.trust_state,
        "declared_side_effects": [
            effect.value for effect in record.declared_side_effects
        ],
        "declared_phase": (
            None if record.declared_phase is None else record.declared_phase.value
        ),
        "resource_roots": list(record.resource_roots),
        "external_metadata": dict(record.external_metadata),
        "source": record.source,
    }


def build_stable_freeze_inventory(
    repository_root: str | os.PathLike[str],
    *,
    plan_version: str,
    phase: str,
    attempt_id: str,
    identity_binding: Mapping[str, str],
    scheduler_records: Iterable[dict[str, str]],
) -> StableFreezeInventoryArtifacts:
    """Return matching manifests only when two bounded scans are identical."""
    candidate_root = Path(repository_root)
    if _is_reparse_point(candidate_root) or not candidate_root.is_dir():
        raise UnstableInventoryError(
            "repository root must be an existing non-reparse directory"
        )
    root = candidate_root.resolve(strict=True)
    scheduler_snapshot = tuple(dict(record) for record in scheduler_records)

    first_records = EntryInventory.scan(
        root,
        scheduler_records=scheduler_snapshot,
    )
    first_files = _capture_files(root, first_records)
    second_records = EntryInventory.scan(
        root,
        scheduler_records=scheduler_snapshot,
    )
    second_files = _capture_files(root, second_records)
    if first_records != second_records or first_files != second_files:
        raise UnstableInventoryError(
            "bounded executable file set, hash, size, or metadata changed during scan"
        )

    identity = dict(identity_binding)
    freeze_payload: dict[str, object] = {
        "schema_version": "control_plane.code_freeze_manifest.v1",
        "plan_version": plan_version,
        "phase": phase,
        "attempt_id": attempt_id,
        "identity_binding": identity,
        "files": [
            {
                "path": path,
                "sha256": snapshot.sha256,
                "bytes": snapshot.bytes,
            }
            for path, snapshot in sorted(second_files.items())
        ],
        "file_count": len(second_files),
    }
    freeze_payload["freeze_payload_sha256"] = canonical_sha256(freeze_payload)

    entries = [_entry_payload(record) for record in second_records]
    inventory_payload: dict[str, object] = {
        "schema_version": "control_plane.entry_inventory.v2",
        "plan_version": plan_version,
        "phase": phase,
        "attempt_id": attempt_id,
        "identity_binding": identity,
        "freeze_payload_sha256": freeze_payload["freeze_payload_sha256"],
        "entries": entries,
        "entry_count": len(entries),
    }
    inventory_payload["inventory_payload_sha256"] = canonical_sha256(
        inventory_payload
    )
    return StableFreezeInventoryArtifacts(
        freeze_manifest=freeze_payload,
        final_inventory=inventory_payload,
    )


__all__ = [
    "StableFreezeInventoryArtifacts",
    "UnstableInventoryError",
    "build_stable_freeze_inventory",
]
