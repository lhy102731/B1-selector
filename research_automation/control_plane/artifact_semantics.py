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

from .contracts import ACTOR_TYPES, Phase, SideEffect, canonical_sha256


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
        raise ArtifactSemanticError("final inventory plan identity mismatch")
    if payload["phase"] != expected_phase:
        raise ArtifactSemanticError("final inventory phase mismatch")
    if payload["attempt_id"] != expected_attempt_id:
        raise ArtifactSemanticError("final inventory attempt mismatch")
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
        raise ArtifactSemanticError("reviewed policy plan identity mismatch")
    if payload["phase"] != expected_phase:
        raise ArtifactSemanticError("reviewed policy phase mismatch")
    if payload["attempt_id"] != expected_attempt_id:
        raise ArtifactSemanticError("reviewed policy attempt mismatch")
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


__all__ = [
    "ArtifactSemanticError",
    "MAX_ARTIFACT_JSON_DEPTH",
    "parse_strict_json",
    "validate_code_freeze_manifest",
    "validate_final_inventory",
    "validate_reviewed_entry_policy",
    "validate_implementation_baseline",
]
