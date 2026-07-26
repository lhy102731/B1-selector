"""Stable, read-only construction of P0R2 freeze and inventory artifacts."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from .artifact_semantics import (
    ArtifactSemanticError,
    validate_code_freeze_manifest,
    validate_final_inventory,
)
from .contracts import canonical_json, canonical_sha256
from .entry_guard import EntryInventory, EntryRecord


_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_BYTE_IDENTITY_POLICY_PATH = ".gitattributes"
_BOUNDED_TOP_LEVEL_DIRECTORIES = frozenset(
    {
        "ag2_research",
        "apps",
        "l2",
        "research",
        "research_automation",
        "strategy",
        "tests",
        "tools",
        "utils",
    }
)
_EXCLUDED_TOP_LEVEL_DIRECTORIES = frozenset(
    {
        ".agents",
        ".claude",
        ".codex_pydeps",
        ".git",
        ".github",
        ".idea",
        ".pytest_cache",
        ".venv",
        ".vscode",
        "__pycache__",
        "archive",
        "artifacts",
        "config",
        "data",
        "discussions",
        "docs",
        "knowledge",
        "models",
        "node_modules",
        "research_state",
        "tmp",
        "venv",
        "web",
    }
)


class UnstableInventoryError(RuntimeError):
    """Raised when the bounded executable surface is unsafe or changes in-flight."""


@dataclass(frozen=True, slots=True)
class _StableFile:
    sha256: str
    bytes: int
    identity: tuple[int, int, int, int, int]


@dataclass(frozen=True, slots=True)
class _StableSnapshot:
    records: tuple[EntryRecord, ...]
    files: dict[str, _StableFile]


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


def _assert_bounded_layout(root: Path) -> None:
    try:
        children = tuple(root.iterdir())
    except OSError as error:
        raise UnstableInventoryError(
            "unable to enumerate repository root"
        ) from error
    for child in children:
        name = child.name.casefold()
        if name in _EXCLUDED_TOP_LEVEL_DIRECTORIES:
            continue
        if name in _BOUNDED_TOP_LEVEL_DIRECTORIES:
            if _is_reparse_point(child) or not child.is_dir():
                raise UnstableInventoryError(
                    f"bounded source root is unsafe: {child.name}"
                )
            continue
        if child.is_dir():
            raise UnstableInventoryError(
                f"unknown top-level directory requires a scope decision: {child.name}"
            )


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

    expected_by_path.setdefault(_BYTE_IDENTITY_POLICY_PATH, "")
    captured: dict[str, _StableFile] = {}
    for relative in sorted(expected_by_path):
        path = _resolve_stable_file(root, relative)
        raw, identity = _read_stable_bytes(path, relative)
        digest = hashlib.sha256(raw).hexdigest()
        expected_digest = expected_by_path[relative]
        if expected_digest and digest != expected_digest:
            raise UnstableInventoryError(
                f"bounded source changed during inventory scan: {relative}"
            )
        captured[relative] = _StableFile(
            sha256=digest,
            bytes=len(raw),
            identity=identity,
        )
    return captured


def _stable_scan(
    root: Path,
    *,
    scheduler_records: tuple[dict[str, str], ...],
) -> _StableSnapshot:
    _assert_bounded_layout(root)
    first_records = EntryInventory.scan(
        root,
        scheduler_records=scheduler_records,
    )
    first_files = _capture_files(root, first_records)
    _assert_bounded_layout(root)
    second_records = EntryInventory.scan(
        root,
        scheduler_records=scheduler_records,
    )
    second_files = _capture_files(root, second_records)
    _assert_bounded_layout(root)
    if first_records != second_records or first_files != second_files:
        raise UnstableInventoryError(
            "bounded executable file set, hash, size, or metadata changed during scan"
        )
    return _StableSnapshot(records=second_records, files=second_files)


def _entry_payload(record: EntryRecord) -> dict[str, object]:
    content_sha256 = record.content_sha256
    if content_sha256 is None and record.kind == "external_scheduler":
        content_sha256 = unavailable_scheduler_sha256(record.path)
    if content_sha256 is None:
        raise UnstableInventoryError(
            f"entry is missing a content hash: {record.entry_id}"
        )
    return {
        "entry_id": record.entry_id,
        "path": record.path.replace("\\", "/"),
        "kind": record.kind,
        "callable_name": record.callable_name,
        "actor_type": record.actor_type,
        "content_sha256": content_sha256,
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


def _byte_identity_policy_entry(snapshot: _StableSnapshot) -> EntryRecord:
    policy_file = snapshot.files[_BYTE_IDENTITY_POLICY_PATH]
    return EntryRecord(
        entry_id=f"file:{_BYTE_IDENTITY_POLICY_PATH}",
        path=_BYTE_IDENTITY_POLICY_PATH,
        kind="repository_policy",
        callable_name="<byte-identity-policy>",
        actor_type="human",
        content_sha256=policy_file.sha256,
        disposition="ADMIN_ONLY",
        trust_state="control_plane_policy",
        source="filesystem_inventory",
    )


def _repository_root(repository_root: str | os.PathLike[str]) -> Path:
    candidate_root = Path(repository_root)
    if _is_reparse_point(candidate_root) or not candidate_root.is_dir():
        raise UnstableInventoryError(
            "repository root must be an existing non-reparse directory"
        )
    return candidate_root.resolve(strict=True)


def unavailable_scheduler_sha256(task_path: str) -> str:
    """Hash an explicit unavailable-evidence marker, never fake task bytes."""
    normalized = str(task_path or "<unknown>").strip().replace("\\", "/")
    return hashlib.sha256(
        b"control_plane.external_scheduler.unavailable.v1\0"
        + normalized.encode("utf-8")
    ).hexdigest()


def build_code_freeze_manifest(
    repository_root: str | os.PathLike[str],
    *,
    plan_version: str,
    phase: str,
    attempt_id: str,
    identity_binding: Mapping[str, str],
) -> dict[str, object]:
    """Build the T7 source freeze without scheduler or file-write coupling."""
    root = _repository_root(repository_root)
    identity = dict(identity_binding)
    snapshot = _stable_scan(root, scheduler_records=())
    freeze_payload: dict[str, object] = {
        "schema_version": "control_plane.code_freeze_manifest.v1",
        "plan_version": plan_version,
        "phase": phase,
        "attempt_id": attempt_id,
        "identity_binding": identity,
        "files": [
            {
                "path": path,
                "sha256": file_snapshot.sha256,
                "bytes": file_snapshot.bytes,
            }
            for path, file_snapshot in sorted(snapshot.files.items())
        ],
        "file_count": len(snapshot.files),
    }
    freeze_payload["freeze_payload_sha256"] = canonical_sha256(freeze_payload)
    return validate_code_freeze_manifest(
        canonical_json(freeze_payload).encode("utf-8"),
        expected_plan_version=plan_version,
        expected_phase=phase,
        expected_attempt_id=attempt_id,
        expected_identity=identity,
        repository_root=root,
    )


def build_final_entry_inventory(
    repository_root: str | os.PathLike[str],
    *,
    plan_version: str,
    phase: str,
    attempt_id: str,
    identity_binding: Mapping[str, str],
    freeze_manifest: Mapping[str, object],
    scheduler_records: Iterable[dict[str, str]],
) -> dict[str, object]:
    """Build T8 inventory from a new stable scan matching the T7 freeze."""
    root = _repository_root(repository_root)
    identity = dict(identity_binding)
    try:
        validated_freeze = validate_code_freeze_manifest(
            canonical_json(dict(freeze_manifest)).encode("utf-8"),
            expected_plan_version=plan_version,
            expected_phase=phase,
            expected_attempt_id=attempt_id,
            expected_identity=identity,
            repository_root=root,
        )
    except ArtifactSemanticError as error:
        raise UnstableInventoryError(
            "code freeze is invalid or no longer matches the repository"
        ) from error
    scheduler_snapshot = tuple(dict(record) for record in scheduler_records)
    snapshot = _stable_scan(root, scheduler_records=scheduler_snapshot)
    frozen_files = {
        str(item["path"]): (str(item["sha256"]), int(item["bytes"]))
        for item in validated_freeze["files"]
    }
    current_files = {
        path: (item.sha256, item.bytes)
        for path, item in snapshot.files.items()
    }
    if current_files != frozen_files:
        raise UnstableInventoryError(
            "final inventory source set differs from the code freeze"
        )

    records = tuple(
        sorted(
            snapshot.records + (_byte_identity_policy_entry(snapshot),),
            key=lambda item: (item.kind, item.path, item.entry_id),
        )
    )
    entries = [_entry_payload(record) for record in records]
    inventory_payload: dict[str, object] = {
        "schema_version": "control_plane.entry_inventory.v2",
        "plan_version": plan_version,
        "phase": phase,
        "attempt_id": attempt_id,
        "identity_binding": identity,
        "freeze_payload_sha256": validated_freeze["freeze_payload_sha256"],
        "entries": entries,
        "entry_count": len(entries),
    }
    inventory_payload["inventory_payload_sha256"] = canonical_sha256(
        inventory_payload
    )
    return validate_final_inventory(
        canonical_json(inventory_payload).encode("utf-8"),
        expected_plan_version=plan_version,
        expected_phase=phase,
        expected_attempt_id=attempt_id,
        expected_identity=identity,
        freeze_manifest=validated_freeze,
    )


__all__ = [
    "UnstableInventoryError",
    "build_code_freeze_manifest",
    "build_final_entry_inventory",
    "unavailable_scheduler_sha256",
]
