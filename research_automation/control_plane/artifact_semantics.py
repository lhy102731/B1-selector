"""Strict, role-specific validation for phase-gate evidence artifacts.

The generic gate report only carries a repository reference and a byte hash.
This module validates the meaning of those bytes as well.  It is deliberately
side-effect free: callers provide bytes that have already been read through a
bounded, canonical repository path.
"""

from __future__ import annotations

import json
import hashlib
import re
from collections.abc import Mapping
from pathlib import Path

from .contracts import canonical_sha256


MAX_ARTIFACT_JSON_DEPTH = 64
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
GIT_COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")


class ArtifactSemanticError(ValueError):
    """Raised when an artifact's bytes do not satisfy its role contract."""


def _reject_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ArtifactSemanticError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_nonfinite_constant(value: str) -> object:
    raise ArtifactSemanticError(f"non-finite JSON constant is forbidden: {value}")


def _check_depth(value: object, *, depth: int = 1) -> None:
    if depth > MAX_ARTIFACT_JSON_DEPTH:
        raise ArtifactSemanticError("artifact JSON exceeds its nesting limit")
    if isinstance(value, Mapping):
        for child in value.values():
            _check_depth(child, depth=depth + 1)
    elif isinstance(value, list):
        for child in value:
            _check_depth(child, depth=depth + 1)


def parse_strict_json(raw: bytes, *, artifact_name: str) -> dict[str, object]:
    """Parse a bounded strict UTF-8 JSON object without last-write-wins keys."""
    if not isinstance(raw, bytes):
        raise ArtifactSemanticError(f"{artifact_name} must be bytes")
    try:
        text = raw.decode("utf-8", errors="strict")
        payload = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonfinite_constant,
        )
    except ArtifactSemanticError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, TypeError) as error:
        raise ArtifactSemanticError(
            f"{artifact_name} must be strict UTF-8 JSON"
        ) from error
    _check_depth(payload)
    if not isinstance(payload, dict):
        raise ArtifactSemanticError(f"{artifact_name} JSON must be an object")
    return payload


def _exact_mapping(
    value: object,
    fields: frozenset[str],
    field_name: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ArtifactSemanticError(f"{field_name} has an invalid field contract")
    return dict(value)


def _string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ArtifactSemanticError(f"{field_name} must be a canonical string")
    return value


def _sha256(value: object, field_name: str) -> str:
    normalized = _string(value, field_name)
    if SHA256_RE.fullmatch(normalized) is None:
        raise ArtifactSemanticError(f"{field_name} must be a lowercase SHA-256")
    return normalized


def _exact_bool(value: object, field_name: str) -> bool:
    if type(value) is not bool:
        raise ArtifactSemanticError(f"{field_name} must be a boolean")
    return value


def _exact_nonnegative_int(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ArtifactSemanticError(
            f"{field_name} must be a non-negative exact integer"
        )
    return value


def _repo_path(value: object, field_name: str) -> str:
    path = _string(value, field_name).replace("\\", "/")
    if (
        path.startswith("/")
        or ":" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
        or any(character in '<>"|?*' for character in path)
        or any(ord(character) < 32 or ord(character) == 127 for character in path)
    ):
        raise ArtifactSemanticError(f"{field_name} must be repository-relative")
    return path


_BASELINE_FIELDS = frozenset(
    {
        "attempt_id",
        "git_head",
        "branch",
        "tracked_user_status_sha256",
        "tracked_user_status_line_count",
        "protected_tracked_changes",
        "file_state_count",
        "file_states",
        "data_scan_policy",
        "large_data_scanned",
        "production_or_research_task_started",
    }
)
_BASELINE_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "plan_version",
        "phase",
        "baseline_payload_hash_algorithm",
        "baseline_payload_sha256",
        "baseline",
    }
)
_FILE_STATE_FIELDS = frozenset({"sha256", "bytes"})
_IDENTITY_FIELDS = frozenset(
    {"plan_hash", "scope_hash", "instruction_policy_hash"}
)
_FREEZE_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "plan_version",
        "phase",
        "attempt_id",
        "identity_binding",
        "files",
        "file_count",
        "freeze_payload_sha256",
    }
)
_FREEZE_FILE_FIELDS = frozenset({"path", "sha256", "bytes"})
_FORBIDDEN_FREEZE_ROOTS = frozenset(
    {
        ".git",
        "archive",
        "artifacts",
        "data",
        "research_state",
        "tmp",
    }
)
_BASELINE_HASH_ALGORITHM = (
    "sha256(canonical UTF-8 JSON of the baseline member; sorted object keys; "
    "semantic array order preserved; compact separators)"
)


def validate_implementation_baseline(
    raw: bytes,
    *,
    expected_plan_version: str,
    expected_phase: str,
    expected_attempt_id: str,
    repository_root: str | Path | None = None,
) -> dict[str, object]:
    """Validate the V2 P0R2 implementation baseline and return its payload."""
    payload = parse_strict_json(raw, artifact_name="implementation_baseline")
    if set(payload) != _BASELINE_TOP_LEVEL_FIELDS:
        raise ArtifactSemanticError(
            "implementation_baseline has an invalid top-level contract"
        )
    if payload["schema_version"] != "control_plane.implementation_baseline.v2":
        raise ArtifactSemanticError("unsupported implementation baseline schema")
    if payload["plan_version"] != expected_plan_version:
        raise ArtifactSemanticError("implementation baseline plan identity mismatch")
    if payload["phase"] != expected_phase:
        raise ArtifactSemanticError("implementation baseline phase mismatch")
    if payload["baseline_payload_hash_algorithm"] != _BASELINE_HASH_ALGORITHM:
        raise ArtifactSemanticError("implementation baseline hash algorithm mismatch")
    _sha256(payload["baseline_payload_sha256"], "baseline_payload_sha256")
    baseline = _exact_mapping(payload["baseline"], _BASELINE_FIELDS, "baseline")
    if baseline["attempt_id"] != expected_attempt_id:
        raise ArtifactSemanticError("implementation baseline attempt mismatch")
    _sha256(
        baseline["tracked_user_status_sha256"],
        "baseline.tracked_user_status_sha256",
    )
    git_head = _string(baseline["git_head"], "baseline.git_head")
    if GIT_COMMIT_RE.fullmatch(git_head) is None:
        raise ArtifactSemanticError("baseline.git_head must be a Git commit SHA")
    _string(baseline["branch"], "baseline.branch")
    _exact_nonnegative_int(
        baseline["tracked_user_status_line_count"],
        "baseline.tracked_user_status_line_count",
    )
    changes = baseline["protected_tracked_changes"]
    if (
        not isinstance(changes, list)
        or any(
            not isinstance(item, str)
            or len(item) < 4
            or item[2] != " "
            or not item[3:].strip()
            for item in changes
        )
        or len(changes) != len(set(changes))
    ):
        raise ArtifactSemanticError(
            "baseline.protected_tracked_changes must be unique canonical strings"
        )
    file_states = baseline["file_states"]
    if not isinstance(file_states, Mapping):
        raise ArtifactSemanticError("baseline.file_states must be an object")
    _exact_nonnegative_int(baseline["file_state_count"], "baseline.file_state_count")
    if baseline["file_state_count"] != len(file_states):
        raise ArtifactSemanticError("baseline.file_state_count does not match file_states")
    for path, state in file_states.items():
        _repo_path(path, "baseline.file_states path")
        if state is None:
            continue
        state_map = _exact_mapping(state, _FILE_STATE_FIELDS, "baseline.file_state")
        _sha256(state_map["sha256"], "baseline.file_state.sha256")
        _exact_nonnegative_int(state_map["bytes"], "baseline.file_state.bytes")
    _string(baseline["data_scan_policy"], "baseline.data_scan_policy")
    if _exact_bool(baseline["large_data_scanned"], "baseline.large_data_scanned"):
        raise ArtifactSemanticError("implementation baseline must not scan large data")
    if _exact_bool(
        baseline["production_or_research_task_started"],
        "baseline.production_or_research_task_started",
    ):
        raise ArtifactSemanticError(
            "implementation baseline records a started production/research task"
        )
    if repository_root is not None:
        try:
            root = Path(repository_root).resolve(strict=True)
        except (OSError, ValueError) as error:
            raise ArtifactSemanticError(
                "implementation baseline repository root is unavailable"
            ) from error
        protected_paths: set[str] = set()
        for change in changes:
            # Git porcelain entries in the baseline use a two-character
            # status followed by a space and the repository-relative path.
            protected_paths.add(change[3:] if len(change) >= 3 else change)
        for path, state in file_states.items():
            if state is None or path in protected_paths:
                continue
            candidate = root.joinpath(*str(path).split("/"))
            try:
                resolved = candidate.resolve(strict=True)
                resolved.relative_to(root)
                if resolved.is_symlink() or not resolved.is_file():
                    raise ArtifactSemanticError(
                        f"baseline file state is not a regular file: {path}"
                    )
                current = resolved.read_bytes()
            except ArtifactSemanticError:
                raise
            except (OSError, ValueError) as error:
                raise ArtifactSemanticError(
                    f"baseline file state is unavailable: {path}"
                ) from error
            state_map = state
            assert isinstance(state_map, Mapping)
            if (
                len(current) != state_map["bytes"]
                or hashlib.sha256(current).hexdigest() != state_map["sha256"]
            ):
                raise ArtifactSemanticError(
                    f"baseline file state does not match current bytes: {path}"
                )
    expected_payload_hash = canonical_sha256(baseline)
    if payload["baseline_payload_sha256"] != expected_payload_hash:
        raise ArtifactSemanticError("implementation baseline payload hash mismatch")
    return payload


def _validate_identity(
    value: object,
    *,
    expected: Mapping[str, str],
    field_name: str = "identity_binding",
) -> dict[str, str]:
    identity = _exact_mapping(value, _IDENTITY_FIELDS, field_name)
    result = {
        key: _sha256(identity[key], f"{field_name}.{key}")
        for key in sorted(_IDENTITY_FIELDS)
    }
    if result != dict(expected):
        raise ArtifactSemanticError(f"{field_name} does not match gate identity")
    return result


def _freeze_path(value: object, field_name: str) -> str:
    path = _repo_path(value, field_name)
    first = path.split("/", 1)[0].casefold()
    if first in _FORBIDDEN_FREEZE_ROOTS or path.casefold().startswith(
        "research_automation/_output/"
    ):
        raise ArtifactSemanticError(f"{field_name} is outside the freeze scope")
    return path


def validate_code_freeze_manifest(
    raw: bytes,
    *,
    expected_plan_version: str,
    expected_phase: str,
    expected_attempt_id: str,
    expected_identity: Mapping[str, str],
    repository_root: str | Path | None = None,
) -> dict[str, object]:
    """Validate a source/test code-freeze manifest and current file bytes."""
    payload = parse_strict_json(raw, artifact_name="code_freeze_manifest")
    if set(payload) != _FREEZE_TOP_LEVEL_FIELDS:
        raise ArtifactSemanticError(
            "code_freeze_manifest has an invalid top-level contract"
        )
    if payload["schema_version"] != "control_plane.code_freeze_manifest.v1":
        raise ArtifactSemanticError("unsupported code-freeze manifest schema")
    if payload["plan_version"] != expected_plan_version:
        raise ArtifactSemanticError("code-freeze plan identity mismatch")
    if payload["phase"] != expected_phase:
        raise ArtifactSemanticError("code-freeze phase mismatch")
    if payload["attempt_id"] != expected_attempt_id:
        raise ArtifactSemanticError("code-freeze attempt mismatch")
    _validate_identity(payload["identity_binding"], expected=expected_identity)
    files = payload["files"]
    if not isinstance(files, list):
        raise ArtifactSemanticError("code-freeze files must be a list")
    _exact_nonnegative_int(payload["file_count"], "code-freeze file_count")
    if payload["file_count"] != len(files):
        raise ArtifactSemanticError("code-freeze file_count does not match files")
    seen_paths: set[str] = set()
    previous_path: str | None = None
    normalized_files: list[dict[str, object]] = []
    for index, item in enumerate(files):
        entry = _exact_mapping(item, _FREEZE_FILE_FIELDS, f"code-freeze files[{index}]")
        path = _freeze_path(entry["path"], f"code-freeze files[{index}].path")
        if path in seen_paths or (previous_path is not None and path < previous_path):
            raise ArtifactSemanticError("code-freeze file paths must be unique and sorted")
        previous_path = path
        seen_paths.add(path)
        entry_sha = _sha256(entry["sha256"], f"code-freeze files[{index}].sha256")
        entry_bytes = _exact_nonnegative_int(
            entry["bytes"],
            f"code-freeze files[{index}].bytes",
        )
        normalized_files.append(
            {"path": path, "sha256": entry_sha, "bytes": entry_bytes}
        )
        if repository_root is not None:
            try:
                root = Path(repository_root).resolve(strict=True)
                candidate = root.joinpath(*path.split("/"))
                if candidate.is_symlink():
                    raise ArtifactSemanticError(
                        f"code-freeze file is a reparse/symlink: {path}"
                    )
                resolved = candidate.resolve(strict=True)
                resolved.relative_to(root)
                if not resolved.is_file():
                    raise ArtifactSemanticError(
                        f"code-freeze file is not a regular file: {path}"
                    )
                current = resolved.read_bytes()
            except ArtifactSemanticError:
                raise
            except (OSError, ValueError) as error:
                raise ArtifactSemanticError(
                    f"code-freeze file is unavailable: {path}"
                ) from error
            if len(current) != entry_bytes or hashlib.sha256(current).hexdigest() != entry_sha:
                raise ArtifactSemanticError(
                    f"code-freeze file changed after freeze: {path}"
                )
    payload_without_hash = dict(payload)
    payload_without_hash["files"] = normalized_files
    payload_without_hash.pop("freeze_payload_sha256", None)
    if payload["freeze_payload_sha256"] != canonical_sha256(payload_without_hash):
        raise ArtifactSemanticError("code-freeze payload hash mismatch")
    return payload


__all__ = [
    "ArtifactSemanticError",
    "MAX_ARTIFACT_JSON_DEPTH",
    "parse_strict_json",
    "validate_code_freeze_manifest",
    "validate_implementation_baseline",
]
