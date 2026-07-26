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

from .contracts import ACTOR_TYPES, Phase, SideEffect, canonical_sha256


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
_REQUIRED_IMPORT_SEAM_IDS = frozenset(
    {
        "callable:research_automation.autonomous_runner:AutonomousRunnerV1.run",
        "callable:research_automation.discovery_execution_bridge:execute_plan",
        "callable:research_automation.kbase_ag2_full_cycle:run_kbase_ag2_full_cycle",
    }
)
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
    """Validate a source/test code-freeze manifest and current file bytes."""
    payload = parse_strict_json(raw, artifact_name="code_freeze_manifest")
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
    if set(payload) != _INVENTORY_TOP_LEVEL_FIELDS:
        raise ArtifactSemanticError("final_inventory has an invalid top-level contract")
    if payload["schema_version"] != "control_plane.entry_inventory.v2":
        raise ArtifactSemanticError("unsupported final inventory schema")
    if payload["plan_version"] != expected_plan_version:
        raise ArtifactBindingError("final inventory plan identity mismatch")
    if payload["phase"] != expected_phase:
        raise ArtifactBindingError("final inventory phase mismatch")
    if payload["attempt_id"] != expected_attempt_id:
        raise ArtifactBindingError("final inventory attempt mismatch")
    _validate_identity(payload["identity_binding"], expected=expected_identity)
    freeze_digest = _sha256(
        payload["freeze_payload_sha256"],
        "final_inventory.freeze_payload_sha256",
    )
    if freeze_digest != freeze_manifest.get("freeze_payload_sha256"):
        raise ArtifactSemanticError("final inventory is not bound to the code freeze")
    entries = _validate_entry_records(payload["entries"])
    _exact_nonnegative_int(payload["entry_count"], "final inventory entry_count")
    if payload["entry_count"] != len(entries):
        raise ArtifactSemanticError("final inventory entry_count mismatch")
    entry_ids = {str(entry["entry_id"]) for entry in entries}
    missing_seams = _REQUIRED_IMPORT_SEAM_IDS - entry_ids
    if missing_seams:
        raise ArtifactSemanticError("final inventory is missing required import seams")
    if _REQUIRED_SCHEDULER_ENTRY_ID not in entry_ids:
        raise ArtifactSemanticError("final inventory is missing scheduler evidence")
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
    _sha256(payload["review_receipt_sha256"], "reviewed_policy.review_receipt_sha256")
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
    payload_without_hash = dict(payload)
    payload_without_hash["entries"] = entries
    payload_without_hash.pop("policy_payload_sha256", None)
    if payload["policy_payload_sha256"] != canonical_sha256(payload_without_hash):
        raise ArtifactSemanticError("reviewed policy payload hash mismatch")
    return payload


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
    _sha256(action["content_sha256"], "scheduler.action.content_sha256")
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
    "validate_code_freeze_manifest",
    "validate_final_inventory",
    "validate_reviewed_entry_policy",
    "validate_scheduler_inventory",
    "validate_implementation_baseline",
]
