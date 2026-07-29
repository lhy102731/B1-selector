"""Stable construction of Git source identities and executable inventories."""

from __future__ import annotations

import hashlib
import os
import subprocess
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from .artifact_semantics import (
    ArtifactSemanticError,
    parse_strict_json,
    validate_code_freeze_manifest,
    validate_final_inventory,
)
from .contracts import canonical_json, canonical_sha256
from .entry_guard import EntryInventory, EntryNotDeclaredError, EntryRecord


_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_BYTE_IDENTITY_POLICY_PATH = ".gitattributes"
_LEGACY_ENTRY_POLICY_PATH = "research_automation/control_plane/entry_policy.json"
_THS_BRIDGE_MARKER_PATH = "utils/ths_yuanhang_bridge.py"
_EXTERNAL_RUNTIME_PATHS = frozenset(
    {"tools/ths_yuanhang_bridge/YuanhangBridge.dll"}
)
_QUARANTINE_ELIGIBLE_CLASSIFICATIONS = frozenset(
    {
        ("LEGACY_UNAUDITED", "legacy_unaudited"),
        ("ADMIN_ONLY", "admin_only"),
    }
)
_NONBLOCKING_TRACKED_DOCUMENT_PATHS = frozenset(
    {"CHANGELOG.md", "docs/b1_v3_results.md"}
)
_EXECUTION_EXCLUDED_DIRECTORY_NAMES = frozenset(
    {
        ".agents",
        ".cache",
        ".claude",
        ".codex_pydeps",
        ".git",
        ".idea",
        ".pytest_cache",
        ".venv",
        ".vscode",
        "__pycache__",
        "_output",
        "archive",
        "artifacts",
        "build",
        "cache",
        "caches",
        "dist",
        "node_modules",
        "output",
        "outputs",
        "research_state",
        "tmp",
        "venv",
    }
)
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
        "data_pre_ths_backup_20260727_110350",
        "data_ths",
        "discussions",
        "docs",
        "knowledge",
        "models",
        "node_modules",
        "outputs",
        "research_state",
        "ths-rebuild-1s3f37j2",
        "ths-rebuild-2f8lznck",
        "ths-rebuild-gzsqa360",
        "ths-rebuild-rn72aj5e",
        "tmp",
        "venv",
        "web",
    }
)
_APPROVED_NON_EXECUTABLE_TOP_LEVEL_DIRECTORIES = frozenset(
    {
        "data_pre_ths_backup_20260727_110350",
        "data_ths",
        "outputs",
        "ths-rebuild-1s3f37j2",
        "ths-rebuild-2f8lznck",
        "ths-rebuild-gzsqa360",
        "ths-rebuild-rn72aj5e",
    }
)
_FORBIDDEN_DATA_TREE_EXECUTABLE_SUFFIXES = frozenset(
    {
        ".bat",
        ".cmd",
        ".cs",
        ".dll",
        ".exe",
        ".jar",
        ".js",
        ".ps1",
        ".pyd",
        ".py",
        ".pyw",
        ".sh",
        ".so",
        ".ts",
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


def _assert_approved_data_tree_is_non_executable(path: Path) -> None:
    if _is_reparse_point(path) or not path.is_dir():
        raise UnstableInventoryError(
            f"approved data/output root is unsafe: {path.name}"
        )

    def fail_walk(error: OSError) -> None:
        raise UnstableInventoryError(
            f"unable to inspect approved data/output root: {path.name}"
        ) from error

    for current, directory_names, file_names in os.walk(
        path,
        topdown=True,
        followlinks=False,
        onerror=fail_walk,
    ):
        current_path = Path(current)
        for directory_name in directory_names:
            child = current_path / directory_name
            if _is_reparse_point(child) or not child.is_dir():
                raise UnstableInventoryError(
                    f"approved data/output root contains an unsafe directory: {child}"
                )
        for file_name in file_names:
            child = current_path / file_name
            if _is_reparse_point(child) or not child.is_file():
                raise UnstableInventoryError(
                    f"approved data/output root contains an unsafe file: {child}"
                )
            if child.suffix.casefold() in _FORBIDDEN_DATA_TREE_EXECUTABLE_SUFFIXES:
                raise UnstableInventoryError(
                    f"approved data/output root contains an executable file: {child}"
                )


def _assert_bounded_layout(root: Path) -> None:
    try:
        children = tuple(root.iterdir())
    except OSError as error:
        raise UnstableInventoryError(
            "unable to enumerate repository root"
        ) from error
    for child in children:
        name = child.name.casefold()
        if name in _APPROVED_NON_EXECUTABLE_TOP_LEVEL_DIRECTORIES:
            _assert_approved_data_tree_is_non_executable(child)
            continue
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


def _byte_identity_policy_entry_from_root(root: Path) -> EntryRecord:
    path = _resolve_stable_file(root, _BYTE_IDENTITY_POLICY_PATH)
    raw, _ = _read_stable_bytes(path, _BYTE_IDENTITY_POLICY_PATH)
    return EntryRecord(
        entry_id=f"file:{_BYTE_IDENTITY_POLICY_PATH}",
        path=_BYTE_IDENTITY_POLICY_PATH,
        kind="repository_policy",
        callable_name="<byte-identity-policy>",
        actor_type="human",
        content_sha256=hashlib.sha256(raw).hexdigest(),
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


def _run_git(root: Path, *arguments: str) -> bytes:
    environment = os.environ.copy()
    repository_selectors = {
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_ATTR_SOURCE",
        "GIT_COMMON_DIR",
        "GIT_DIR",
        "GIT_INDEX_FILE",
        "GIT_NAMESPACE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_WORK_TREE",
    }
    for key in tuple(environment):
        if key in repository_selectors or key.startswith("GIT_CONFIG_"):
            environment.pop(key, None)
    environment["GIT_NO_REPLACE_OBJECTS"] = "1"
    environment["GIT_OPTIONAL_LOCKS"] = "0"
    try:
        completed = subprocess.run(
            [
                "git",
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.untrackedCache=false",
                *arguments,
            ],
            cwd=root,
            check=True,
            capture_output=True,
            timeout=15,
            env=environment,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise UnstableInventoryError(
            "repository Git identity is unavailable"
        ) from error
    return completed.stdout


def _git_object_id(root: Path, revision: str) -> str:
    try:
        value = _run_git(root, "rev-parse", "--verify", revision).decode(
            "ascii",
            errors="strict",
        ).strip()
    except UnicodeDecodeError as error:
        raise UnstableInventoryError("repository Git identity is malformed") from error
    if len(value) != 40 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise UnstableInventoryError("repository Git identity is malformed")
    return value


def _assert_git_toplevel(root: Path) -> None:
    raw = _run_git(root, "rev-parse", "--show-toplevel")
    try:
        value = raw.decode("utf-8", errors="strict").strip()
        if not value or "\n" in value or "\r" in value or "\0" in value:
            raise ValueError
        top_level = Path(value).resolve(strict=True)
    except (UnicodeDecodeError, OSError, ValueError) as error:
        raise UnstableInventoryError(
            "repository Git top-level is malformed"
        ) from error
    if top_level != root:
        raise UnstableInventoryError(
            "repository root does not match the Git top-level"
        )


def _tracked_dirty_paths(root: Path) -> tuple[list[str], list[str]]:
    worktree_raw = _run_git(
        root,
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--no-renames",
        "--name-only",
        "-z",
        "HEAD",
        "--",
    )
    index_raw = _run_git(
        root,
        "diff",
        "--cached",
        "--no-ext-diff",
        "--no-textconv",
        "--no-renames",
        "--name-only",
        "-z",
        "HEAD",
        "--",
    )
    paths = sorted(
        set(_decode_git_paths(worktree_raw)) | set(_decode_git_paths(index_raw))
    )
    active: list[str] = []
    nonblocking: list[str] = []
    for path in paths:
        target = (
            nonblocking
            if _is_nonblocking_tracked_document(path)
            else active
        )
        target.append(path)
    return active, nonblocking


def _unsafe_index_flag_paths(root: Path) -> list[str]:
    raw = _run_git(root, "ls-files", "-v", "-z", "--")
    unsafe: list[str] = []
    for record in raw.split(b"\0"):
        if not record:
            continue
        if len(record) < 3 or record[1:2] != b" ":
            raise UnstableInventoryError("repository Git index output is malformed")
        try:
            path = record[2:].decode("utf-8", errors="strict").replace("\\", "/")
        except UnicodeDecodeError as error:
            raise UnstableInventoryError(
                "repository Git index output is malformed"
            ) from error
        tag = record[:1]
        if (tag == b"S" or b"a" <= tag <= b"z") and not (
            _is_nonblocking_tracked_document(path)
        ):
            unsafe.append(path)
    return sorted(set(unsafe))


def _assert_safe_tracked_index_modes(root: Path) -> None:
    raw = _run_git(root, "ls-files", "--stage", "-z", "--")
    for record in raw.split(b"\0"):
        if not record:
            continue
        try:
            header, raw_path = record.split(b"\t", 1)
            mode, _object_id, stage = header.split(b" ")
            path = raw_path.decode("utf-8", errors="strict").replace("\\", "/")
        except (UnicodeDecodeError, ValueError) as error:
            raise UnstableInventoryError(
                "repository Git index output is malformed"
            ) from error
        if mode not in {b"100644", b"100755"} or stage != b"0":
            raise UnstableInventoryError(
                f"tracked path uses an unsafe Git mode: {path}"
            )


def _is_nonblocking_tracked_document(path: str) -> bool:
    return path in _NONBLOCKING_TRACKED_DOCUMENT_PATHS


def _decode_git_paths(raw: bytes) -> list[str]:
    try:
        return sorted(
            {
                item.decode("utf-8", errors="strict").replace("\\", "/")
                for item in raw.split(b"\0")
                if item
            }
        )
    except UnicodeDecodeError as error:
        raise UnstableInventoryError(
            "repository Git path output is malformed"
        ) from error


def _untracked_executable_paths(root: Path) -> list[str]:
    repository_pathspecs: list[str] = []
    for suffix in sorted(_FORBIDDEN_DATA_TREE_EXECUTABLE_SUFFIXES):
        root_pattern = f":(top,icase,glob)*{suffix}"
        repository_pathspecs.extend(
            (root_pattern, f":(top,icase,glob)**/*{suffix}")
        )
    repository_exclusions = [
        ":(exclude,top,glob)**/_output/**",
        ":(exclude,top,glob)**/archive/**",
        ":(exclude,top,glob)artifacts/**",
        ":(exclude,top,glob)data/**",
        ":(exclude,top,glob)data_pre_ths_backup_*/**",
        ":(exclude,top,glob)data_ths/**",
        ":(exclude,top,glob)models/**",
        ":(exclude,top,glob)outputs/**",
        ":(exclude,top,glob)research_state/**",
        ":(exclude,top,glob)ths-rebuild-*/**",
        ":(exclude,top,glob)tmp/**",
    ]
    visible = _run_git(
        root,
        "ls-files",
        "--others",
        "--exclude-standard",
        "-z",
        "--",
        *repository_pathspecs,
        *repository_exclusions,
    )
    ignored = _run_git(
        root,
        "ls-files",
        "--others",
        "--ignored",
        "--exclude-standard",
        "-z",
        "--",
        *repository_pathspecs,
        *repository_exclusions,
    )
    paths = set(_decode_git_paths(visible)) | set(_decode_git_paths(ignored))
    return sorted(
        path
        for path in paths
        if not _execution_path_is_excluded(path)
    )


def _execution_path_is_excluded(path: str) -> bool:
    directory_parts = [part.casefold() for part in path.split("/")[:-1]]
    if any(
        part in _EXECUTION_EXCLUDED_DIRECTORY_NAMES
        or part.endswith(".egg-info")
        for part in directory_parts
    ):
        return True
    if not directory_parts:
        return False
    top_level = directory_parts[0]
    return top_level.startswith("data_pre_ths_backup_") or top_level.startswith(
        "ths-rebuild-"
    )


def _policy_records_by_path(
    policy_raw: bytes,
) -> tuple[dict[str, dict[str, str]], frozenset[str]]:
    payload = parse_strict_json(policy_raw, artifact_name="legacy_entry_policy")
    if (
        payload.get("schema_version") != "control_plane.entry_policy.v1"
        or payload.get("review_state") != "APPROVED"
        or not isinstance(payload.get("entries"), list)
    ):
        raise UnstableInventoryError("legacy entry policy is invalid")
    eligible_value = payload.get("quarantine_eligible_paths")
    eligible_digest = payload.get("quarantine_eligible_paths_sha256")
    if not isinstance(eligible_value, list) or any(
        not isinstance(path, str)
        or not path
        or path != path.strip()
        or path.startswith("/")
        or ":" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
        for path in eligible_value
    ):
        raise UnstableInventoryError("legacy quarantine eligibility is invalid")
    eligible_paths = [path.replace("\\", "/") for path in eligible_value]
    if eligible_paths != sorted(set(eligible_paths)):
        raise UnstableInventoryError("legacy quarantine eligibility is invalid")
    expected_eligibility_digest = canonical_sha256(
        {
            "schema_version": "control_plane.legacy_quarantine_paths.v1",
            "paths": eligible_paths,
        }
    )
    if eligible_digest != expected_eligibility_digest:
        raise UnstableInventoryError("legacy quarantine eligibility is invalid")
    records: dict[str, dict[str, str]] = {}
    for item in payload["entries"]:
        if not isinstance(item, Mapping):
            raise UnstableInventoryError("legacy entry policy is invalid")
        try:
            path = str(item["path"]).replace("\\", "/")
            digest = str(item["content_sha256"])
            disposition = str(item["disposition"])
            trust_state = str(item["trust_state"])
            source = str(item["source"])
        except KeyError as error:
            raise UnstableInventoryError("legacy entry policy is invalid") from error
        if source == "external_scheduler_inventory":
            continue
        if (
            not path
            or path.startswith("/")
            or ":" in path
            or any(part in {"", ".", ".."} for part in path.split("/"))
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or not disposition
            or not trust_state
        ):
            raise UnstableInventoryError("legacy entry policy is invalid")
        candidate = {
            "path": path,
            "sha256": digest,
            "disposition": disposition,
            "trust_state": trust_state,
        }
        previous = records.setdefault(path, candidate)
        if previous != candidate:
            raise UnstableInventoryError(
                f"legacy entry policy disagrees about executable: {path}"
            )
    if any(path not in records for path in eligible_paths):
        raise UnstableInventoryError("legacy quarantine eligibility is invalid")
    return records, frozenset(eligible_paths)


def _verify_untracked_executables(
    root: Path,
    *,
    policy_raw: bytes,
) -> list[dict[str, str]]:
    policy_records, eligible_paths = _policy_records_by_path(policy_raw)
    verified: list[dict[str, str]] = []
    for relative in _untracked_executable_paths(root):
        if relative in _EXTERNAL_RUNTIME_PATHS:
            continue
        declared = policy_records.get(relative)
        if declared is None:
            raise UnstableInventoryError(
                f"untracked executable is not in the legacy policy: {relative}"
            )
        if relative not in eligible_paths:
            raise UnstableInventoryError(
                f"untracked executable is not quarantine-eligible: {relative}"
            )
        if (
            declared["disposition"],
            declared["trust_state"],
        ) not in _QUARANTINE_ELIGIBLE_CLASSIFICATIONS:
            raise UnstableInventoryError(
                f"untracked executable is not quarantine-eligible: {relative}"
            )
        path = _resolve_stable_file(root, relative)
        raw, _ = _read_stable_bytes(path, relative)
        actual_sha256 = hashlib.sha256(raw).hexdigest()
        if actual_sha256 != declared["sha256"]:
            raise UnstableInventoryError(
                f"untracked executable differs from the legacy policy: {relative}"
            )
        verified.append(dict(declared))
    return verified


def _capture_external_runtime_dependencies(root: Path) -> list[dict[str, str]]:
    marker = root.joinpath(*_THS_BRIDGE_MARKER_PATH.split("/"))
    if not marker.exists():
        return []
    _resolve_stable_file(root, _THS_BRIDGE_MARKER_PATH)
    captured: list[dict[str, str]] = []
    for relative in sorted(_EXTERNAL_RUNTIME_PATHS):
        path = _resolve_stable_file(root, relative)
        raw, _ = _read_stable_bytes(path, relative)
        captured.append(
            {
                "path": relative,
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
    return captured


def _capture_git_source_identity(
    root: Path,
) -> tuple[dict[str, object], list[str]]:
    _assert_git_toplevel(root)
    git_commit = _git_object_id(root, "HEAD^{commit}")
    git_tree = _git_object_id(root, "HEAD^{tree}")
    _assert_safe_tracked_index_modes(root)
    unsafe_index_paths = _unsafe_index_flag_paths(root)
    if unsafe_index_paths:
        raise UnstableInventoryError(
            "tracked source uses unsafe index flags: "
            + ", ".join(unsafe_index_paths)
        )
    active_dirty_paths, nonblocking_dirty_paths = _tracked_dirty_paths(root)
    if active_dirty_paths:
        raise UnstableInventoryError(
            "tracked source is dirty: " + ", ".join(active_dirty_paths)
        )
    policy_path = _resolve_stable_file(root, _LEGACY_ENTRY_POLICY_PATH)
    policy_raw, _ = _read_stable_bytes(policy_path, _LEGACY_ENTRY_POLICY_PATH)
    policy_sha256 = hashlib.sha256(policy_raw).hexdigest()
    untracked_executables = _verify_untracked_executables(
        root,
        policy_raw=policy_raw,
    )
    runtime_dependencies = _capture_external_runtime_dependencies(root)
    quarantine_sha256 = canonical_sha256(
        {
            "legacy_policy_sha256": policy_sha256,
            "untracked_executables": untracked_executables,
        }
    )
    return (
        {
            "schema_version": "control_plane.git_source_identity.v1",
            "git_commit": git_commit,
            "git_tree": git_tree,
            "active_tracked_dirty_paths": [],
            "untracked_executables": untracked_executables,
            "runtime_dependencies": runtime_dependencies,
            "legacy_policy_path": _LEGACY_ENTRY_POLICY_PATH,
            "legacy_policy_sha256": policy_sha256,
            "legacy_quarantine_sha256": quarantine_sha256,
        },
        nonblocking_dirty_paths,
    )


def build_code_freeze_manifest(
    repository_root: str | os.PathLike[str],
    *,
    plan_version: str,
    phase: str,
    attempt_id: str,
    identity_binding: Mapping[str, str],
) -> dict[str, object]:
    """Build the T7 Git source identity without copying tracked file hashes."""
    root = _repository_root(repository_root)
    identity = dict(identity_binding)
    source_identity, _ = _capture_git_source_identity(root)
    try:
        confirmed_identity, nonblocking_dirty_paths = (
            _capture_git_source_identity(root)
        )
    except UnstableInventoryError as error:
        raise UnstableInventoryError(
            "repository changed during source identity capture"
        ) from error
    if confirmed_identity != source_identity:
        raise UnstableInventoryError(
            "repository changed during source identity capture"
        )
    freeze_payload: dict[str, object] = {
        "schema_version": "control_plane.code_freeze_manifest.v2",
        "plan_version": plan_version,
        "phase": phase,
        "attempt_id": attempt_id,
        "identity_binding": identity,
        "git_commit": source_identity["git_commit"],
        "git_tree": source_identity["git_tree"],
        "active_tracked_dirty_paths": [],
        "nonblocking_tracked_dirty_paths": nonblocking_dirty_paths,
        "untracked_executables": source_identity["untracked_executables"],
        "runtime_dependencies": source_identity["runtime_dependencies"],
        "legacy_policy_path": _LEGACY_ENTRY_POLICY_PATH,
        "legacy_policy_sha256": source_identity["legacy_policy_sha256"],
        "legacy_quarantine_sha256": source_identity[
            "legacy_quarantine_sha256"
        ],
        "source_identity_sha256": canonical_sha256(source_identity),
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


def build_legacy_code_freeze_manifest(
    repository_root: str | os.PathLike[str],
    *,
    plan_version: str,
    phase: str,
    attempt_id: str,
    identity_binding: Mapping[str, str],
) -> dict[str, object]:
    """Reconstruct a historical v1 freeze for compatibility verification only."""
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
    if (
        validated_freeze["schema_version"]
        == "control_plane.code_freeze_manifest.v1"
    ):
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
        inventory_schema = "control_plane.entry_inventory.v2"
        binding_field = "freeze_payload_sha256"
        binding_value = validated_freeze["freeze_payload_sha256"]
    else:
        source_before, _ = _capture_git_source_identity(root)
        if canonical_sha256(source_before) != validated_freeze[
            "source_identity_sha256"
        ]:
            raise UnstableInventoryError(
                "current Git source identity differs from the code freeze"
            )
        scanned_records = EntryInventory.scan(
            root,
            scheduler_records=scheduler_snapshot,
        )
        byte_policy_entry = _byte_identity_policy_entry_from_root(root)
        try:
            source_after, _ = _capture_git_source_identity(root)
        except UnstableInventoryError as error:
            raise UnstableInventoryError(
                "repository changed during final inventory scan"
            ) from error
        if source_after != source_before:
            raise UnstableInventoryError(
                "repository changed during final inventory scan"
            )
        records = tuple(
            sorted(
                scanned_records + (byte_policy_entry,),
                key=lambda item: (item.kind, item.path, item.entry_id),
            )
        )
        inventory_schema = "control_plane.entry_inventory.v3"
        binding_field = "source_identity_sha256"
        binding_value = validated_freeze["source_identity_sha256"]
    entries = [_entry_payload(record) for record in records]
    inventory_payload: dict[str, object] = {
        "schema_version": inventory_schema,
        "plan_version": plan_version,
        "phase": phase,
        "attempt_id": attempt_id,
        "identity_binding": identity,
        binding_field: binding_value,
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


def verify_current_git_inventory(
    repository_root: str | os.PathLike[str],
    *,
    freeze_manifest: Mapping[str, object],
    final_inventory: Mapping[str, object],
) -> None:
    """Verify v2/v3 evidence against one stable bounded live-source scan."""
    if (
        freeze_manifest.get("schema_version")
        != "control_plane.code_freeze_manifest.v2"
        or final_inventory.get("schema_version")
        != "control_plane.entry_inventory.v3"
    ):
        raise UnstableInventoryError(
            "operational verification requires Git source identity evidence"
        )
    expected_identity = freeze_manifest.get("source_identity_sha256")
    if not isinstance(expected_identity, str):
        raise UnstableInventoryError("Git source identity evidence is invalid")
    root = _repository_root(repository_root)
    source_before, _ = _capture_git_source_identity(root)
    if canonical_sha256(source_before) != expected_identity:
        raise UnstableInventoryError(
            "current executable surface differs from the Git source identity"
        )
    try:
        scanned_records = EntryInventory.scan(root, scheduler_records=())
        byte_policy_entry = _byte_identity_policy_entry_from_root(root)
    except EntryNotDeclaredError as error:
        raise UnstableInventoryError(
            "current bounded entry inventory is unavailable"
        ) from error
    source_after, _ = _capture_git_source_identity(root)
    if source_after != source_before:
        raise UnstableInventoryError(
            "repository changed during current entry inventory verification"
        )
    current_entries = sorted(
        (
            _entry_payload(record)
            for record in scanned_records + (byte_policy_entry,)
            if record.kind != "external_scheduler"
        ),
        key=lambda item: (str(item["kind"]), str(item["path"]), str(item["entry_id"])),
    )
    recorded = final_inventory.get("entries")
    if not isinstance(recorded, list) or any(
        not isinstance(entry, Mapping) for entry in recorded
    ):
        raise UnstableInventoryError("final inventory entries are invalid")
    recorded_entries = sorted(
        (
            dict(entry)
            for entry in recorded
            if entry.get("kind") != "external_scheduler"
        ),
        key=lambda item: (str(item["kind"]), str(item["path"]), str(item["entry_id"])),
    )
    if recorded_entries != current_entries:
        raise UnstableInventoryError(
            "final inventory differs from the current bounded entry surface"
        )


__all__ = [
    "UnstableInventoryError",
    "build_code_freeze_manifest",
    "build_final_entry_inventory",
    "unavailable_scheduler_sha256",
    "verify_current_git_inventory",
]
