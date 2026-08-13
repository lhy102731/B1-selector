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
from datetime import datetime
from pathlib import Path

from .contracts import ACTOR_TYPES, Phase, SideEffect, canonical_json, canonical_sha256


MAX_ARTIFACT_JSON_DEPTH = 64
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
GIT_COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")
_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400


class ArtifactSemanticError(ValueError):
    """Raised when an artifact's bytes do not satisfy its role contract."""


class ArtifactBindingError(ArtifactSemanticError):
    """Raised when valid artifact semantics bind to another gate identity."""


def _path_is_reparse_point(path: Path) -> bool:
    try:
        if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except OSError as error:
        raise ArtifactSemanticError(
            f"unable to inspect freeze path for reparse points: {path}"
        ) from error
    return bool(attributes & _FILE_ATTRIBUTE_REPARSE_POINT)


def _reject_reparse_components(root: Path, relative: str) -> Path:
    candidate = root
    for component in relative.split("/"):
        candidate = candidate / component
        if _path_is_reparse_point(candidate):
            raise ArtifactSemanticError(
                f"code-freeze path contains a reparse point: {relative}"
            )
    return candidate


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
_GIT_FREEZE_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "plan_version",
        "phase",
        "attempt_id",
        "identity_binding",
        "git_commit",
        "git_tree",
        "active_tracked_dirty_paths",
        "nonblocking_tracked_dirty_paths",
        "untracked_executables",
        "runtime_dependencies",
        "legacy_policy_path",
        "legacy_policy_sha256",
        "legacy_quarantine_sha256",
        "source_identity_sha256",
        "freeze_payload_sha256",
    }
)
_GIT_FREEZE_EXECUTABLE_FIELDS = frozenset(
    {"path", "sha256", "disposition", "trust_state"}
)
_GIT_FREEZE_RUNTIME_FIELDS = frozenset({"path", "sha256"})
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
_ENTRY_RECORD_FIELDS = frozenset(
    {
        "entry_id",
        "path",
        "kind",
        "callable_name",
        "actor_type",
        "content_sha256",
        "disposition",
        "trust_state",
        "declared_side_effects",
        "declared_phase",
        "resource_roots",
        "external_metadata",
        "source",
    }
)
_ENTRY_DISPOSITIONS = frozenset(
    {
        "CONTROLLED_RESEARCH",
        "LEGACY_UNAUDITED",
        "PRODUCTION_DAILY",
        "ADMIN_ONLY",
        "TEST_ONLY",
        "DENIED_WEB",
    }
)
_INVENTORY_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "plan_version",
        "phase",
        "attempt_id",
        "identity_binding",
        "freeze_payload_sha256",
        "entries",
        "entry_count",
        "inventory_payload_sha256",
    }
)
_GIT_INVENTORY_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "plan_version",
        "phase",
        "attempt_id",
        "identity_binding",
        "source_identity_sha256",
        "entries",
        "entry_count",
        "inventory_payload_sha256",
    }
)
_POLICY_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "plan_version",
        "phase",
        "attempt_id",
        "identity_binding",
        "review_state",
        "reviewer_id",
        "review_receipt_sha256",
        "inventory_payload_sha256",
        "entries",
        "entry_count",
        "policy_payload_sha256",
    }
)
_REQUIRED_IMPORT_SEAM_BINDINGS = {
    "callable:research_automation.autonomous_runner:AutonomousRunnerV1.run": {
        "path": "research_automation/autonomous_runner.py",
        "callable_name": "AutonomousRunnerV1.run",
        "declared_side_effects": [
            "READ",
            "WRITE_STAGING",
            "RUN_RESEARCH",
            "WRITE_KBASE",
            "GIT_MUTATION",
        ],
    },
    "callable:research_automation.discovery_execution_bridge:execute_plan": {
        "path": "research_automation/discovery_execution_bridge.py",
        "callable_name": "execute_plan",
        "declared_side_effects": ["WRITE_STAGING", "RUN_RESEARCH"],
    },
    "callable:research_automation.kbase_ag2_full_cycle:run_kbase_ag2_full_cycle": {
        "path": "research_automation/kbase_ag2_full_cycle.py",
        "callable_name": "run_kbase_ag2_full_cycle",
        "declared_side_effects": [
            "READ",
            "WRITE_STAGING",
            "RUN_RESEARCH",
            "GIT_MUTATION",
        ],
    },
    # P8 CR-009 (GPT F-03): the TrustedEvaluator is the only seam that
    # declares OPEN_HOLDOUT; the reviewed entry policy must bind it exactly.
    "callable:research_automation.control_plane.final_evaluator:TrustedEvaluator.evaluate_v2": {
        "path": "research_automation/control_plane/final_evaluator.py",
        "callable_name": "TrustedEvaluator.evaluate_v2",
        "declared_side_effects": ["OPEN_HOLDOUT"],
    },
}
_REQUIRED_IMPORT_SEAM_IDS = frozenset(_REQUIRED_IMPORT_SEAM_BINDINGS)
_REQUIRED_SCHEDULER_ENTRY_ID = "external:scheduler:/A\u80a1\u9009\u80a1"
_SCHEDULER_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "phase",
        "observed_at",
        "collection_mode",
        "task_path",
        "task_state",
        "operational_classification",
        "task_xml",
        "action",
        "principal",
        "trigger",
        "acl",
        "altered_by_p0",
        "unresolved_risk",
    }
)
_SCHEDULER_TASK_XML_FIELDS = frozenset({"path", "sha256"})
_SCHEDULER_ACTION_FIELDS = frozenset(
    {"execute", "arguments", "working_directory", "content_sha256"}
)
_SCHEDULER_PRINCIPAL_FIELDS = frozenset(
    {"user_id", "logon_type", "run_level"}
)
_SCHEDULER_TRIGGER_FIELDS = frozenset(
    {"type", "start_boundary", "enabled", "days_interval"}
)
_SCHEDULER_ACL_FIELDS = frozenset({"owner", "sddl"})
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
        raise ArtifactBindingError("implementation baseline plan identity mismatch")
    if payload["phase"] != expected_phase:
        raise ArtifactBindingError("implementation baseline phase mismatch")
    if payload["baseline_payload_hash_algorithm"] != _BASELINE_HASH_ALGORITHM:
        raise ArtifactSemanticError("implementation baseline hash algorithm mismatch")
    _sha256(payload["baseline_payload_sha256"], "baseline_payload_sha256")
    baseline = _exact_mapping(payload["baseline"], _BASELINE_FIELDS, "baseline")
    if baseline["attempt_id"] != expected_attempt_id:
        raise ArtifactBindingError("implementation baseline attempt mismatch")
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
            Path(repository_root).resolve(strict=True)
        except (OSError, ValueError) as error:
            raise ArtifactSemanticError(
                "implementation baseline repository root is unavailable"
            ) from error
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
        raise ArtifactBindingError(f"{field_name} does not match gate identity")
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
    """Validate freeze semantics; v1 also checks bytes when a root is supplied.

    Operational v2 callers separately recapture and compare the live Git source
    identity plus bounded entry inventory.
    """
    payload = parse_strict_json(raw, artifact_name="code_freeze_manifest")
    if payload.get("schema_version") == "control_plane.code_freeze_manifest.v2":
        return _validate_git_code_freeze_manifest(
            payload,
            expected_plan_version=expected_plan_version,
            expected_phase=expected_phase,
            expected_attempt_id=expected_attempt_id,
            expected_identity=expected_identity,
        )
    if set(payload) != _FREEZE_TOP_LEVEL_FIELDS:
        raise ArtifactSemanticError(
            "code_freeze_manifest has an invalid top-level contract"
        )
    if payload["schema_version"] != "control_plane.code_freeze_manifest.v1":
        raise ArtifactSemanticError("unsupported code-freeze manifest schema")
    if payload["plan_version"] != expected_plan_version:
        raise ArtifactBindingError("code-freeze plan identity mismatch")
    if payload["phase"] != expected_phase:
        raise ArtifactBindingError("code-freeze phase mismatch")
    if payload["attempt_id"] != expected_attempt_id:
        raise ArtifactBindingError("code-freeze attempt mismatch")
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
                candidate_root = Path(repository_root)
                if _path_is_reparse_point(candidate_root):
                    raise ArtifactSemanticError(
                        "code-freeze repository root is a reparse point"
                    )
                root = candidate_root.resolve(strict=True)
                candidate = _reject_reparse_components(root, path)
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


def _validate_git_code_freeze_manifest(
    payload: dict[str, object],
    *,
    expected_plan_version: str,
    expected_phase: str,
    expected_attempt_id: str,
    expected_identity: Mapping[str, str],
) -> dict[str, object]:
    if set(payload) != _GIT_FREEZE_TOP_LEVEL_FIELDS:
        raise ArtifactSemanticError(
            "code_freeze_manifest has an invalid top-level contract"
        )
    if payload["plan_version"] != expected_plan_version:
        raise ArtifactBindingError("code-freeze plan identity mismatch")
    if payload["phase"] != expected_phase:
        raise ArtifactBindingError("code-freeze phase mismatch")
    if payload["attempt_id"] != expected_attempt_id:
        raise ArtifactBindingError("code-freeze attempt mismatch")
    _validate_identity(payload["identity_binding"], expected=expected_identity)
    for field_name in ("git_commit", "git_tree"):
        value = _string(payload[field_name], f"code-freeze {field_name}")
        if GIT_COMMIT_RE.fullmatch(value) is None:
            raise ArtifactSemanticError(
                f"code-freeze {field_name} must be a lowercase Git object id"
            )
    dirty_paths: dict[str, list[str]] = {}
    for field_name in (
        "active_tracked_dirty_paths",
        "nonblocking_tracked_dirty_paths",
    ):
        values = payload[field_name]
        if not isinstance(values, list):
            raise ArtifactSemanticError(
                f"code-freeze {field_name} must be a list"
            )
        normalized = [
            _repo_path(value, f"code-freeze {field_name}[{index}]")
            for index, value in enumerate(values)
        ]
        if normalized != sorted(set(normalized)):
            raise ArtifactSemanticError(
                f"code-freeze {field_name} must be unique and sorted"
            )
        dirty_paths[field_name] = normalized
    if dirty_paths["active_tracked_dirty_paths"]:
        raise ArtifactSemanticError("code-freeze tracked source is dirty")
    executable_entries = payload["untracked_executables"]
    if not isinstance(executable_entries, list):
        raise ArtifactSemanticError(
            "code-freeze untracked_executables must be a list"
        )
    normalized_executables: list[dict[str, str]] = []
    for index, item in enumerate(executable_entries):
        entry = _exact_mapping(
            item,
            _GIT_FREEZE_EXECUTABLE_FIELDS,
            f"code-freeze untracked_executables[{index}]",
        )
        normalized_executables.append(
            {
                "path": _repo_path(
                    entry["path"],
                    f"code-freeze untracked_executables[{index}].path",
                ),
                "sha256": _sha256(
                    entry["sha256"],
                    f"code-freeze untracked_executables[{index}].sha256",
                ),
                "disposition": _string(
                    entry["disposition"],
                    f"code-freeze untracked_executables[{index}].disposition",
                ),
                "trust_state": _string(
                    entry["trust_state"],
                    f"code-freeze untracked_executables[{index}].trust_state",
                ),
            }
        )
    if normalized_executables != sorted(
        normalized_executables,
        key=lambda item: item["path"],
    ) or len({item["path"] for item in normalized_executables}) != len(
        normalized_executables
    ):
        raise ArtifactSemanticError(
            "code-freeze untracked executables must be unique and sorted"
        )
    runtime_entries = payload["runtime_dependencies"]
    if not isinstance(runtime_entries, list):
        raise ArtifactSemanticError(
            "code-freeze runtime_dependencies must be a list"
        )
    normalized_runtime: list[dict[str, str]] = []
    for index, item in enumerate(runtime_entries):
        entry = _exact_mapping(
            item,
            _GIT_FREEZE_RUNTIME_FIELDS,
            f"code-freeze runtime_dependencies[{index}]",
        )
        normalized_runtime.append(
            {
                "path": _repo_path(
                    entry["path"],
                    f"code-freeze runtime_dependencies[{index}].path",
                ),
                "sha256": _sha256(
                    entry["sha256"],
                    f"code-freeze runtime_dependencies[{index}].sha256",
                ),
            }
        )
    if normalized_runtime != sorted(
        normalized_runtime,
        key=lambda item: item["path"],
    ) or len({item["path"] for item in normalized_runtime}) != len(
        normalized_runtime
    ):
        raise ArtifactSemanticError(
            "code-freeze runtime dependencies must be unique and sorted"
        )
    legacy_policy_path = _repo_path(
        payload["legacy_policy_path"],
        "code-freeze legacy_policy_path",
    )
    legacy_policy_sha256 = _sha256(
        payload["legacy_policy_sha256"],
        "code-freeze legacy_policy_sha256",
    )
    legacy_quarantine_sha256 = _sha256(
        payload["legacy_quarantine_sha256"],
        "code-freeze legacy_quarantine_sha256",
    )
    expected_source_identity = canonical_sha256(
        {
            "schema_version": "control_plane.git_source_identity.v1",
            "git_commit": payload["git_commit"],
            "git_tree": payload["git_tree"],
            "active_tracked_dirty_paths": dirty_paths[
                "active_tracked_dirty_paths"
            ],
            "untracked_executables": normalized_executables,
            "runtime_dependencies": normalized_runtime,
            "legacy_policy_path": legacy_policy_path,
            "legacy_policy_sha256": legacy_policy_sha256,
            "legacy_quarantine_sha256": legacy_quarantine_sha256,
        }
    )
    if payload["source_identity_sha256"] != expected_source_identity:
        raise ArtifactSemanticError("code-freeze source identity hash mismatch")
    payload_without_hash = dict(payload)
    payload_without_hash.pop("freeze_payload_sha256", None)
    if payload["freeze_payload_sha256"] != canonical_sha256(payload_without_hash):
        raise ArtifactSemanticError("code-freeze payload hash mismatch")
    return payload


def _string_array(value: object, field_name: str) -> list[str]:
    if (
        not isinstance(value, list)
        or any(
            not isinstance(item, str) or not item or item != item.strip()
            for item in value
        )
        or len(value) != len(set(value))
    ):
        raise ArtifactSemanticError(
            f"{field_name} must contain unique canonical strings"
        )
    return list(value)


def _validate_entry_records(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise ArtifactSemanticError("entry inventory entries must be a list")
    normalized: list[dict[str, object]] = []
    entry_ids: set[str] = set()
    previous_sort_key: tuple[str, str, str] | None = None
    for index, item in enumerate(value):
        entry = _exact_mapping(
            item,
            _ENTRY_RECORD_FIELDS,
            f"entries[{index}]",
        )
        entry_id = _string(entry["entry_id"], f"entries[{index}].entry_id")
        if entry_id in entry_ids:
            raise ArtifactSemanticError("entry inventory contains duplicate entry ids")
        entry_ids.add(entry_id)
        kind = _string(entry["kind"], f"entries[{index}].kind")
        source = _string(entry["source"], f"entries[{index}].source")
        if kind == "external_scheduler":
            path = _string(entry["path"], f"entries[{index}].path").replace(
                "\\", "/"
            )
            if not path.startswith("/") or source != "external_scheduler_inventory":
                raise ArtifactSemanticError("external scheduler entry identity is invalid")
        else:
            path = _freeze_path(entry["path"], f"entries[{index}].path")
        actor_type = _string(
            entry["actor_type"],
            f"entries[{index}].actor_type",
        )
        if actor_type not in ACTOR_TYPES:
            raise ArtifactSemanticError("entry actor_type is outside the closed set")
        disposition = _string(
            entry["disposition"],
            f"entries[{index}].disposition",
        )
        if disposition not in _ENTRY_DISPOSITIONS:
            raise ArtifactSemanticError("entry disposition is outside the closed set")
        content_sha256 = _sha256(
            entry["content_sha256"],
            f"entries[{index}].content_sha256",
        )
        effects = _string_array(
            entry["declared_side_effects"],
            f"entries[{index}].declared_side_effects",
        )
        if any(effect not in {item.value for item in SideEffect} for effect in effects):
            raise ArtifactSemanticError("entry declared_side_effects is invalid")
        declared_phase = entry["declared_phase"]
        if declared_phase is not None and (
            not isinstance(declared_phase, str)
            or declared_phase not in {item.value for item in Phase}
        ):
            raise ArtifactSemanticError("entry declared_phase is invalid")
        roots = _string_array(
            entry["resource_roots"],
            f"entries[{index}].resource_roots",
        )
        metadata = entry["external_metadata"]
        if (
            not isinstance(metadata, Mapping)
            or any(
                not isinstance(key, str)
                or not key
                or key != key.strip()
                or not isinstance(metadata_value, str)
                or not metadata_value
                or metadata_value != metadata_value.strip()
                for key, metadata_value in metadata.items()
            )
        ):
            raise ArtifactSemanticError("entry external_metadata is invalid")
        normalized_entry = {
            "entry_id": entry_id,
            "path": path,
            "kind": kind,
            "callable_name": _string(
                entry["callable_name"],
                f"entries[{index}].callable_name",
            ),
            "actor_type": actor_type,
            "content_sha256": content_sha256,
            "disposition": disposition,
            "trust_state": _string(
                entry["trust_state"],
                f"entries[{index}].trust_state",
            ),
            "declared_side_effects": effects,
            "declared_phase": declared_phase,
            "resource_roots": roots,
            "external_metadata": dict(sorted(metadata.items())),
            "source": source,
        }
        sort_key = (kind, path, entry_id)
        if previous_sort_key is not None and sort_key < previous_sort_key:
            raise ArtifactSemanticError("entry inventory entries must be sorted")
        previous_sort_key = sort_key
        normalized.append(normalized_entry)
    return normalized


def validate_final_inventory(
    raw: bytes,
    *,
    expected_plan_version: str,
    expected_phase: str,
    expected_attempt_id: str,
    expected_identity: Mapping[str, str],
    freeze_manifest: Mapping[str, object],
) -> dict[str, object]:
    """Validate a final executable inventory against the code freeze."""
    payload = parse_strict_json(raw, artifact_name="final_inventory")
    inventory_schema = payload.get("schema_version")
    if inventory_schema == "control_plane.entry_inventory.v2":
        expected_fields = _INVENTORY_TOP_LEVEL_FIELDS
    elif inventory_schema == "control_plane.entry_inventory.v3":
        expected_fields = _GIT_INVENTORY_TOP_LEVEL_FIELDS
    else:
        raise ArtifactSemanticError("unsupported final inventory schema")
    if set(payload) != expected_fields:
        raise ArtifactSemanticError(
            "final_inventory has an invalid top-level contract"
        )
    if payload["plan_version"] != expected_plan_version:
        raise ArtifactBindingError("final inventory plan identity mismatch")
    if payload["phase"] != expected_phase:
        raise ArtifactBindingError("final inventory phase mismatch")
    if payload["attempt_id"] != expected_attempt_id:
        raise ArtifactBindingError("final inventory attempt mismatch")
    _validate_identity(payload["identity_binding"], expected=expected_identity)
    if inventory_schema == "control_plane.entry_inventory.v2":
        freeze_digest = _sha256(
            payload["freeze_payload_sha256"],
            "final_inventory.freeze_payload_sha256",
        )
        if freeze_digest != freeze_manifest.get("freeze_payload_sha256"):
            raise ArtifactSemanticError(
                "final inventory is not bound to the code freeze"
            )
    else:
        if (
            freeze_manifest.get("schema_version")
            != "control_plane.code_freeze_manifest.v2"
        ):
            raise ArtifactSemanticError(
                "Git final inventory requires a Git code freeze"
            )
        source_identity_sha256 = _sha256(
            payload["source_identity_sha256"],
            "final_inventory.source_identity_sha256",
        )
        if source_identity_sha256 != freeze_manifest.get(
            "source_identity_sha256"
        ):
            raise ArtifactSemanticError(
                "final inventory is not bound to the Git source identity"
            )
    entries = _validate_entry_records(payload["entries"])
    _exact_nonnegative_int(payload["entry_count"], "final inventory entry_count")
    if payload["entry_count"] != len(entries):
        raise ArtifactSemanticError("final inventory entry_count mismatch")
    entry_ids = {str(entry["entry_id"]) for entry in entries}
    missing_seams = _REQUIRED_IMPORT_SEAM_IDS - entry_ids
    if missing_seams:
        raise ArtifactSemanticError("final inventory is missing required import seams")
    entries_by_id = {str(entry["entry_id"]): entry for entry in entries}
    for entry_id, expected in _REQUIRED_IMPORT_SEAM_BINDINGS.items():
        entry = entries_by_id[entry_id]
        actual_binding = {
            "path": entry["path"],
            "kind": entry["kind"],
            "callable_name": entry["callable_name"],
            "actor_type": entry["actor_type"],
            "disposition": entry["disposition"],
            "trust_state": entry["trust_state"],
            "declared_side_effects": entry["declared_side_effects"],
            "declared_phase": entry["declared_phase"],
            "resource_roots": entry["resource_roots"],
            "external_metadata": entry["external_metadata"],
            "source": entry["source"],
        }
        expected_binding = {
            **expected,
            "kind": "python_callable",
            "actor_type": "legacy_runner",
            "disposition": "LEGACY_UNAUDITED",
            "trust_state": "legacy_unaudited",
            "declared_phase": None,
            "resource_roots": [],
            "external_metadata": {},
            "source": "required_import_seam",
        }
        if actual_binding != expected_binding:
            raise ArtifactSemanticError(
                f"required import seam binding is invalid: {entry_id}"
            )
    if _REQUIRED_SCHEDULER_ENTRY_ID not in entry_ids:
        raise ArtifactSemanticError("final inventory is missing scheduler evidence")
    scheduler_entry = entries_by_id[_REQUIRED_SCHEDULER_ENTRY_ID]
    scheduler_binding = {
        "path": scheduler_entry["path"],
        "kind": scheduler_entry["kind"],
        "actor_type": scheduler_entry["actor_type"],
        "disposition": scheduler_entry["disposition"],
        "trust_state": scheduler_entry["trust_state"],
        "declared_side_effects": scheduler_entry["declared_side_effects"],
        "declared_phase": scheduler_entry["declared_phase"],
        "resource_roots": scheduler_entry["resource_roots"],
        "source": scheduler_entry["source"],
    }
    if scheduler_binding != {
        "path": "/A股选股",
        "kind": "external_scheduler",
        "actor_type": "scheduler",
        "disposition": "PRODUCTION_DAILY",
        "trust_state": "production_daily",
        "declared_side_effects": [],
        "declared_phase": None,
        "resource_roots": [],
        "source": "external_scheduler_inventory",
    }:
        raise ArtifactSemanticError("required scheduler binding is invalid")
    if inventory_schema == "control_plane.entry_inventory.v3":
        frozen_untracked = freeze_manifest.get("untracked_executables")
        frozen_runtime = freeze_manifest.get("runtime_dependencies")
        if not isinstance(frozen_untracked, list) or not isinstance(
            frozen_runtime,
            list,
        ):
            raise ArtifactSemanticError(
                "Git code freeze non-Git dependencies are unavailable"
            )
        for frozen in frozen_untracked:
            if not isinstance(frozen, Mapping):
                raise ArtifactSemanticError(
                    "Git code freeze quarantined executable is invalid"
                )
            matching = [
                entry
                for entry in entries
                if entry["path"] == frozen.get("path")
                and entry["content_sha256"] == frozen.get("sha256")
                and entry["disposition"] == frozen.get("disposition")
                and entry["trust_state"] == frozen.get("trust_state")
                and entry["source"] == "filesystem_inventory"
            ]
            if len(matching) != 1:
                raise ArtifactSemanticError(
                    "final inventory is missing a quarantined executable"
                )
        for frozen in frozen_runtime:
            if not isinstance(frozen, Mapping):
                raise ArtifactSemanticError(
                    "Git code freeze runtime dependency is invalid"
                )
            matching = [
                entry
                for entry in entries
                if entry["path"] == frozen.get("path")
                and entry["content_sha256"] == frozen.get("sha256")
                and entry["kind"] == "runtime_dependency"
                and entry["disposition"] == "PRODUCTION_DAILY"
                and entry["trust_state"] == "production_daily"
                and entry["source"] == "runtime_dependency_inventory"
            ]
            if len(matching) != 1:
                raise ArtifactSemanticError(
                    "final inventory is missing a runtime dependency"
                )
    if inventory_schema == "control_plane.entry_inventory.v2":
        freeze_files = freeze_manifest.get("files")
        if not isinstance(freeze_files, list):
            raise ArtifactSemanticError("code-freeze files are unavailable")
        freeze_by_path = {
            str(item["path"]): (str(item["sha256"]), int(item["bytes"]))
            for item in freeze_files
            if isinstance(item, Mapping)
        }
        inventory_by_path: dict[str, str] = {}
        for entry in entries:
            if entry["kind"] == "external_scheduler":
                continue
            path = str(entry["path"])
            digest = str(entry["content_sha256"])
            prior = inventory_by_path.setdefault(path, digest)
            if prior != digest:
                raise ArtifactSemanticError(
                    "inventory records disagree about one frozen file"
                )
            if path not in freeze_by_path or freeze_by_path[path][0] != digest:
                raise ArtifactSemanticError(
                    f"final inventory differs from the code freeze: {path}"
                )
        if set(inventory_by_path) != set(freeze_by_path):
            raise ArtifactSemanticError(
                "final inventory and code-freeze file sets differ"
            )
    payload_without_hash = dict(payload)
    payload_without_hash["entries"] = entries
    payload_without_hash.pop("inventory_payload_sha256", None)
    if payload["inventory_payload_sha256"] != canonical_sha256(payload_without_hash):
        raise ArtifactSemanticError("final inventory payload hash mismatch")
    return payload


def validate_reviewed_entry_policy(
    raw: bytes,
    *,
    expected_plan_version: str,
    expected_phase: str,
    expected_attempt_id: str,
    expected_identity: Mapping[str, str],
    final_inventory: Mapping[str, object],
) -> dict[str, object]:
    """Validate an independently reviewed, identity-bound entry policy."""
    payload = parse_strict_json(raw, artifact_name="reviewed_entry_policy")
    if set(payload) != _POLICY_TOP_LEVEL_FIELDS:
        raise ArtifactSemanticError(
            "reviewed_entry_policy has an invalid top-level contract"
        )
    if payload["schema_version"] != "control_plane.entry_policy.v1":
        raise ArtifactSemanticError("unsupported reviewed entry policy schema")
    if payload["plan_version"] != expected_plan_version:
        raise ArtifactBindingError("reviewed policy plan identity mismatch")
    if payload["phase"] != expected_phase:
        raise ArtifactBindingError("reviewed policy phase mismatch")
    if payload["attempt_id"] != expected_attempt_id:
        raise ArtifactBindingError("reviewed policy attempt mismatch")
    _validate_identity(payload["identity_binding"], expected=expected_identity)
    if payload["review_state"] != "APPROVED":
        raise ArtifactSemanticError("reviewed policy is not APPROVED")
    reviewer_id = _string(payload["reviewer_id"], "reviewed_policy.reviewer_id")
    if reviewer_id.casefold() in {"scanner", "automatic", "auto"}:
        raise ArtifactSemanticError("scanner output cannot self-approve a policy")
    review_receipt_sha256 = _sha256(
        payload["review_receipt_sha256"],
        "reviewed_policy.review_receipt_sha256",
    )
    inventory_digest = _sha256(
        payload["inventory_payload_sha256"],
        "reviewed_policy.inventory_payload_sha256",
    )
    if inventory_digest != final_inventory.get("inventory_payload_sha256"):
        raise ArtifactSemanticError("reviewed policy is not bound to final inventory")
    entries = _validate_entry_records(payload["entries"])
    _exact_nonnegative_int(payload["entry_count"], "reviewed policy entry_count")
    if payload["entry_count"] != len(entries):
        raise ArtifactSemanticError("reviewed policy entry_count mismatch")
    inventory_entries = _validate_entry_records(final_inventory.get("entries"))
    if entries != inventory_entries:
        raise ArtifactSemanticError("reviewed policy entries differ from final inventory")
    if review_receipt_sha256 != reviewed_policy_receipt_sha256(payload):
        raise ArtifactSemanticError(
            "reviewed policy review receipt binding is invalid"
        )
    payload_without_hash = dict(payload)
    payload_without_hash["entries"] = entries
    payload_without_hash.pop("policy_payload_sha256", None)
    if payload["policy_payload_sha256"] != canonical_sha256(payload_without_hash):
        raise ArtifactSemanticError("reviewed policy payload hash mismatch")
    return payload


def reviewed_policy_receipt_sha256(policy: Mapping[str, object]) -> str:
    """Bind one independent reviewer to one exact inventory-derived policy."""
    if not isinstance(policy, Mapping):
        raise ArtifactSemanticError("reviewed policy receipt input is invalid")
    required = {
        "plan_version",
        "phase",
        "attempt_id",
        "identity_binding",
        "reviewer_id",
        "inventory_payload_sha256",
        "entries",
    }
    if not required.issubset(policy):
        raise ArtifactSemanticError("reviewed policy receipt input is incomplete")
    binding = {
        "schema_version": "control_plane.entry_policy_review_binding.v1",
        "plan_version": policy["plan_version"],
        "phase": policy["phase"],
        "attempt_id": policy["attempt_id"],
        "identity_binding": policy["identity_binding"],
        "reviewer_id": policy["reviewer_id"],
        "inventory_payload_sha256": policy["inventory_payload_sha256"],
        "entries_sha256": canonical_sha256(policy["entries"]),
    }
    return hashlib.sha256(
        b"control_plane.entry_policy_review_binding.v1\0"
        + canonical_json(binding).encode("utf-8")
    ).hexdigest()


def _optional_string(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _string(value, field_name)


def validate_scheduler_inventory(
    raw: bytes,
    *,
    expected_phase: str,
    final_inventory: Mapping[str, object],
) -> tuple[dict[str, object], str]:
    """Validate scheduler evidence and derive VERIFIED/UNKNOWN/INVALID."""
    payload = parse_strict_json(raw, artifact_name="scheduler_inventory")
    if set(payload) != _SCHEDULER_TOP_LEVEL_FIELDS:
        raise ArtifactSemanticError(
            "scheduler_inventory has an invalid top-level contract"
        )
    if payload["schema_version"] != "control_plane.external_scheduler_inventory.v1":
        raise ArtifactSemanticError("unsupported scheduler inventory schema")
    if payload["phase"] != expected_phase:
        raise ArtifactBindingError("scheduler inventory phase mismatch")
    observed_at = _string(payload["observed_at"], "scheduler.observed_at")
    try:
        parsed_at = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ArtifactSemanticError("scheduler observed_at is invalid") from error
    if parsed_at.tzinfo is None or parsed_at.utcoffset() is None:
        raise ArtifactSemanticError("scheduler observed_at must include timezone")
    collection_mode = _string(
        payload["collection_mode"],
        "scheduler.collection_mode",
    )
    if collection_mode not in {"READ_ONLY", "UNAVAILABLE"}:
        raise ArtifactSemanticError("scheduler collection_mode is invalid")
    task_path = _string(payload["task_path"], "scheduler.task_path").replace(
        "\\", "/"
    )
    if task_path != "/A\u80a1\u9009\u80a1":
        raise ArtifactSemanticError("scheduler task_path is not the required task")
    task_state = _string(payload["task_state"], "scheduler.task_state")
    classification = _string(
        payload["operational_classification"],
        "scheduler.operational_classification",
    )
    task_xml = _exact_mapping(
        payload["task_xml"],
        _SCHEDULER_TASK_XML_FIELDS,
        "scheduler.task_xml",
    )
    _string(task_xml["path"], "scheduler.task_xml.path")
    task_xml_sha = _sha256(task_xml["sha256"], "scheduler.task_xml.sha256")
    action = _exact_mapping(
        payload["action"],
        _SCHEDULER_ACTION_FIELDS,
        "scheduler.action",
    )
    action_execute = _string(action["execute"], "scheduler.action.execute")
    _optional_string(action["arguments"], "scheduler.action.arguments")
    _optional_string(
        action["working_directory"],
        "scheduler.action.working_directory",
    )
    action_content_sha256 = _sha256(
        action["content_sha256"],
        "scheduler.action.content_sha256",
    )
    principal = _exact_mapping(
        payload["principal"],
        _SCHEDULER_PRINCIPAL_FIELDS,
        "scheduler.principal",
    )
    for field_name in sorted(_SCHEDULER_PRINCIPAL_FIELDS):
        _string(principal[field_name], f"scheduler.principal.{field_name}")
    trigger = _exact_mapping(
        payload["trigger"],
        _SCHEDULER_TRIGGER_FIELDS,
        "scheduler.trigger",
    )
    _string(trigger["type"], "scheduler.trigger.type")
    _string(trigger["start_boundary"], "scheduler.trigger.start_boundary")
    _exact_bool(trigger["enabled"], "scheduler.trigger.enabled")
    days_interval = _exact_nonnegative_int(
        trigger["days_interval"],
        "scheduler.trigger.days_interval",
    )
    if days_interval < 1:
        raise ArtifactSemanticError("scheduler trigger days_interval must be positive")
    acl = _exact_mapping(
        payload["acl"],
        _SCHEDULER_ACL_FIELDS,
        "scheduler.acl",
    )
    for field_name in sorted(_SCHEDULER_ACL_FIELDS):
        _string(acl[field_name], f"scheduler.acl.{field_name}")
    altered = _exact_bool(payload["altered_by_p0"], "scheduler.altered_by_p0")
    _string(payload["unresolved_risk"], "scheduler.unresolved_risk")

    inventory_entries = _validate_entry_records(final_inventory.get("entries"))
    scheduler_entries = [
        entry
        for entry in inventory_entries
        if entry["entry_id"] == _REQUIRED_SCHEDULER_ENTRY_ID
    ]
    if len(scheduler_entries) != 1:
        raise ArtifactSemanticError("scheduler inventory entry is not unique")
    inventory_scheduler = scheduler_entries[0]
    action_entries = [
        entry
        for entry in inventory_entries
        if entry["entry_id"] == "file:run_select.bat"
        and entry["path"] == "run_select.bat"
        and entry["kind"] == "batch"
        and entry["disposition"] == "PRODUCTION_DAILY"
        and entry["source"] == "filesystem_inventory"
    ]
    if len(action_entries) != 1:
        raise ArtifactSemanticError(
            "scheduler action entry is not uniquely bound to run_select.bat"
        )
    if (
        action_execute.replace("\\", "/").rsplit("/", 1)[-1].casefold()
        != "run_select.bat"
        or action_entries[0]["content_sha256"] != action_content_sha256
    ):
        raise ArtifactSemanticError(
            "scheduler action content differs from the final inventory"
        )
    expected_metadata = {
        "acl_summary": f"owner={acl['owner']};sddl={acl['sddl']}",
        "action": action_execute,
        "principal": (
            f"{principal['user_id']}|{principal['logon_type']}|"
            f"{principal['run_level']}"
        ),
        "state": task_state,
        "trigger": (
            f"{trigger['type']}|start={trigger['start_boundary']}|"
            f"days_interval={days_interval}|"
            f"enabled={str(trigger['enabled']).lower()}"
        ),
    }
    if (
        inventory_scheduler["content_sha256"] != task_xml_sha
        or inventory_scheduler["callable_name"] != action_execute
        or inventory_scheduler["external_metadata"] != expected_metadata
    ):
        raise ArtifactSemanticError(
            "scheduler evidence differs from the final inventory"
        )
    if collection_mode == "UNAVAILABLE" or task_state == "UNKNOWN":
        status = "UNKNOWN"
    elif altered or classification != "PRODUCTION_DAILY":
        status = "INVALID"
    else:
        status = "VERIFIED"
    return payload, status


__all__ = [
    "ArtifactBindingError",
    "ArtifactSemanticError",
    "MAX_ARTIFACT_JSON_DEPTH",
    "parse_strict_json",
    "reviewed_policy_receipt_sha256",
    "validate_code_freeze_manifest",
    "validate_final_inventory",
    "validate_reviewed_entry_policy",
    "validate_scheduler_inventory",
    "validate_implementation_baseline",
]
