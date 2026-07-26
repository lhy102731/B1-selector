"""Exact-byte classification for immutable P0R1 TaskReport evidence.

Legacy reports never enter the TaskReport V2 validator and never grant phase,
ticket, lease, gate, or execution authority.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass


P0R1_T1_RAW_SHA256 = (
    "9beac163f88c1aeadee0c864a2a3dc9ad68bc06200225eff1bb26a13c882771a"
)
P0R1_T2_RAW_SHA256 = (
    "5c5052099ce22f2f0c0fd463bf5dfc62a949bbc05b44cbe2a5d4605034baeb9d"
)
P0R1_PLAN_HASH = (
    "6603e962c65d81274fe2e9a2e4a2c9e987ae65837bc415ab4144058c38ab8d34"
)
P0R1_SCOPE_HASH = (
    "ec0410ba5925474ca728bce4bc960f889fce405da3e1015bd5306c190d317027"
)
P0R1_POLICY_HASH = (
    "5db5bfb72acf41a9f42d0d4f557036d28d0c4f4f9ef584e92e306922b6808d82"
)
MAX_LEGACY_REPORT_BYTES = 64 * 1024
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class LegacyAdoptionError(ValueError):
    """Raised when immutable legacy evidence cannot be classified safely."""


@dataclass(frozen=True)
class LegacyReportSnapshot:
    source_id: str
    task_id: str
    schema_version: str
    raw_sha256: str
    source_result: str
    source_status: str
    source_gate_status: str | None
    adoption_status: str
    execution_eligible: bool
    inventory_disposition: str | None
    entry_policy_final_gate_eligible: bool | None
    missing_source_fields: tuple[str, ...]
    known_total_tokens: int
    unknown_usage_count: int
    usage_owner_source_id: str
    count_in_target_total: bool


@dataclass(frozen=True)
class LegacyFileExpectation:
    path: str
    expected_sha256: str
    source_id: str
    source_sequence: int


@dataclass(frozen=True)
class LegacyFileCheck:
    path: str
    expected_sha256: str
    actual_sha256: str | None
    expected_source_id: str
    status: str


@dataclass(frozen=True)
class P0R1AdoptionSnapshot:
    source_order: tuple[str, ...]
    source_snapshots: tuple[LegacyReportSnapshot, ...]
    overwritten_paths: tuple[str, ...]
    file_checks: tuple[LegacyFileCheck, ...]
    code_delta_status: str
    adoption_status: str
    ready_for_test_revalidation: bool
    execution_eligible: bool
    legacy_gate_eligible: bool
    p0r1_evidence_directly_counted: bool
    inventory_disposition: str
    entry_policy_final_gate_eligible: bool
    known_total_tokens: int
    unknown_usage_count: int
    usage_owner_source_id: str
    count_in_target_total: bool


def _parse_exact_known_report(
    raw: bytes,
    *,
    source_label: str,
    expected_sha256: str,
) -> Mapping[str, object]:
    if not isinstance(raw, bytes):
        raise LegacyAdoptionError(f"{source_label} input must be bytes")
    if len(raw) > MAX_LEGACY_REPORT_BYTES:
        raise LegacyAdoptionError(
            f"{source_label} exceeds {MAX_LEGACY_REPORT_BYTES} byte limit"
        )
    actual_sha256 = hashlib.sha256(raw).hexdigest()
    if not hmac.compare_digest(actual_sha256, expected_sha256):
        raise LegacyAdoptionError(f"{source_label} raw SHA-256 mismatch")
    try:
        payload = json.loads(raw.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LegacyAdoptionError(
            f"{source_label} exact bytes are not valid UTF-8 JSON"
        ) from error
    if not isinstance(payload, Mapping):
        raise LegacyAdoptionError(f"{source_label} JSON must be an object")
    return payload


def _require_exact_field(
    payload: Mapping[str, object],
    field_name: str,
    expected: str,
    source_label: str,
) -> None:
    if payload.get(field_name) != expected:
        raise LegacyAdoptionError(
            f"{source_label} {field_name} does not match known evidence"
        )


def _require_mapping_field(
    payload: Mapping[str, object],
    field_name: str,
    source_label: str,
) -> Mapping[str, object]:
    value = payload.get(field_name)
    if not isinstance(value, Mapping):
        raise LegacyAdoptionError(f"{source_label} {field_name} must be an object")
    return value


def classify_known_p0r1_t1(raw: bytes) -> LegacyReportSnapshot:
    """Classify the one hash-bound P0R1 T1 report without promoting it."""
    source_label = "P0R1 T1"
    payload = _parse_exact_known_report(
        raw,
        source_label=source_label,
        expected_sha256=P0R1_T1_RAW_SHA256,
    )
    expected_fields = {
        "schema_version": "control_plane.task_report.v1",
        "task_id": "P0-T1-CONTRACT-AUTHORITY",
        "phase": "P0",
        "task_status": "PASS",
        "p0_gate_status": "NOT_COMPUTED",
    }
    for field_name, expected in expected_fields.items():
        _require_exact_field(payload, field_name, expected, source_label)
    identity = _require_mapping_field(payload, "identity_binding", source_label)
    for field_name, expected in {
        "plan_hash": P0R1_PLAN_HASH,
        "scope_hash": P0R1_SCOPE_HASH,
        "policy_hash": P0R1_POLICY_HASH,
    }.items():
        _require_exact_field(identity, field_name, expected, source_label)
    token_accounting = _require_mapping_field(
        payload,
        "external_model_token_accounting",
        source_label,
    )
    if token_accounting.get("known_total_tokens") != 58734:
        raise LegacyAdoptionError(
            "P0R1 T1 known token total does not match known evidence"
        )
    unknown_profiles = token_accounting.get("unknown_usage_profiles")
    if not isinstance(unknown_profiles, list) or len(unknown_profiles) != 1:
        raise LegacyAdoptionError(
            "P0R1 T1 unknown usage does not match known evidence"
        )
    return LegacyReportSnapshot(
        source_id="P0R1-T1",
        task_id="P0-T1-CONTRACT-AUTHORITY",
        schema_version="control_plane.task_report.v1",
        raw_sha256=P0R1_T1_RAW_SHA256,
        source_result="PASS",
        source_status="PASS",
        source_gate_status="NOT_COMPUTED",
        adoption_status="REVALIDATION_REQUIRED",
        execution_eligible=False,
        inventory_disposition=None,
        entry_policy_final_gate_eligible=None,
        missing_source_fields=(),
        known_total_tokens=58734,
        unknown_usage_count=1,
        usage_owner_source_id="P0R1-T1",
        count_in_target_total=False,
    )


def classify_known_p0r1_t2(raw: bytes) -> LegacyReportSnapshot:
    """Classify the hash-bound P0R1 T2 report while preserving its block."""
    source_label = "P0R1 T2"
    payload = _parse_exact_known_report(
        raw,
        source_label=source_label,
        expected_sha256=P0R1_T2_RAW_SHA256,
    )
    expected_fields = {
        "schema_version": "control_plane.p0_task_report.v1",
        "task_id": "P0-T2-INVENTORY-IMPORT",
        "phase": "P0",
        "plan_version": "V3.4.1-P0R1",
        "plan_hash": P0R1_PLAN_HASH,
        "scope_hash": P0R1_SCOPE_HASH,
        "policy_hash": P0R1_POLICY_HASH,
        "task_level_result": "GREEN_CURRENT_SNAPSHOT",
        "status": "BLOCKED_BY_PLAN_REVISION",
    }
    for field_name, expected in expected_fields.items():
        _require_exact_field(payload, field_name, expected, source_label)
    missing_source_fields = tuple(
        field_name
        for field_name in ("authorization_ref", "started_at")
        if field_name not in payload
    )
    if missing_source_fields != ("authorization_ref", "started_at"):
        raise LegacyAdoptionError(
            "P0R1 T2 missing-field classification does not match known evidence"
        )
    inventory = _require_mapping_field(payload, "inventory_summary", source_label)
    if inventory.get("record_count") != 368:
        raise LegacyAdoptionError(
            "P0R1 T2 inventory count does not match known evidence"
        )
    _require_exact_field(
        inventory,
        "entry_policy_sha256",
        "7e1d2f185fd926879a17d10ff3fa419cfd4f46fd45a8c3c4c96ac0f9c322643f",
        source_label,
    )
    usage = _require_mapping_field(payload, "external_model_usage", source_label)
    if usage.get("known_total_tokens") != 58734:
        raise LegacyAdoptionError(
            "P0R1 T2 known token total does not match known evidence"
        )
    if usage.get("unknown_usage_count") != 1:
        raise LegacyAdoptionError(
            "P0R1 T2 unknown usage count does not match known evidence"
        )
    return LegacyReportSnapshot(
        source_id="P0R1-T2",
        task_id="P0-T2-INVENTORY-IMPORT",
        schema_version="control_plane.p0_task_report.v1",
        raw_sha256=P0R1_T2_RAW_SHA256,
        source_result="GREEN_CURRENT_SNAPSHOT",
        source_status="BLOCKED_BY_PLAN_REVISION",
        source_gate_status=None,
        adoption_status="BLOCKED_SOURCE_PRESERVED",
        execution_eligible=False,
        inventory_disposition="INITIAL_PROVISIONAL_ONLY",
        entry_policy_final_gate_eligible=False,
        missing_source_fields=missing_source_fields,
        known_total_tokens=58734,
        unknown_usage_count=1,
        usage_owner_source_id="P0R1-T1",
        count_in_target_total=False,
    )


def _require_legacy_path(value: object, source_label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.startswith("/")
        or "\\" in value
        or any(part in ("", ".", "..") for part in value.split("/"))
    ):
        raise LegacyAdoptionError(
            f"{source_label} changed-file path is not repository-relative POSIX"
        )
    return value


def _require_legacy_sha256(value: object, source_label: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise LegacyAdoptionError(
            f"{source_label} changed-file hash is not lowercase SHA-256"
        )
    return value


def _t1_file_expectations(raw: bytes) -> tuple[LegacyFileExpectation, ...]:
    classify_known_p0r1_t1(raw)
    payload = _parse_exact_known_report(
        raw,
        source_label="P0R1 T1",
        expected_sha256=P0R1_T1_RAW_SHA256,
    )
    changed_files = payload.get("changed_files")
    if not isinstance(changed_files, list):
        raise LegacyAdoptionError("P0R1 T1 changed_files must be an array")
    expectations: list[LegacyFileExpectation] = []
    seen_paths: set[str] = set()
    for changed_file in changed_files:
        if not isinstance(changed_file, Mapping):
            raise LegacyAdoptionError("P0R1 T1 changed-file entry must be an object")
        path = _require_legacy_path(changed_file.get("path"), "P0R1 T1")
        if path in seen_paths:
            raise LegacyAdoptionError("P0R1 T1 changed_files contains duplicate paths")
        seen_paths.add(path)
        expectations.append(
            LegacyFileExpectation(
                path=path,
                expected_sha256=_require_legacy_sha256(
                    changed_file.get("current_sha256"),
                    "P0R1 T1",
                ),
                source_id="P0R1-T1",
                source_sequence=1,
            )
        )
    return tuple(expectations)


def _t2_file_expectations(raw: bytes) -> tuple[LegacyFileExpectation, ...]:
    classify_known_p0r1_t2(raw)
    payload = _parse_exact_known_report(
        raw,
        source_label="P0R1 T2",
        expected_sha256=P0R1_T2_RAW_SHA256,
    )
    changed_files = payload.get("changed_files")
    if not isinstance(changed_files, Mapping):
        raise LegacyAdoptionError("P0R1 T2 changed_files must be an object")
    expectations: list[LegacyFileExpectation] = []
    for raw_path, raw_sha256 in changed_files.items():
        path = _require_legacy_path(raw_path, "P0R1 T2")
        expectations.append(
            LegacyFileExpectation(
                path=path,
                expected_sha256=_require_legacy_sha256(
                    raw_sha256,
                    "P0R1 T2",
                ),
                source_id="P0R1-T2",
                source_sequence=2,
            )
        )
    return tuple(expectations)


def derive_ordered_p0r1_file_expectations(
    t1_raw: bytes,
    t2_raw: bytes,
) -> tuple[LegacyFileExpectation, ...]:
    """Return final file expectations after applying T1 then T2."""
    final_by_path: dict[str, LegacyFileExpectation] = {}
    for expectation in _t1_file_expectations(t1_raw):
        final_by_path[expectation.path] = expectation
    for expectation in _t2_file_expectations(t2_raw):
        final_by_path[expectation.path] = expectation
    return tuple(final_by_path[path] for path in sorted(final_by_path))


def revalidate_ordered_p0r1_files(
    t1_raw: bytes,
    t2_raw: bytes,
    current_file_sha256: Mapping[str, object],
) -> P0R1AdoptionSnapshot:
    """Compare current hashes using T1 then T2 without promoting P0R1."""
    if not isinstance(current_file_sha256, Mapping):
        raise LegacyAdoptionError("current_file_sha256 must be a mapping")
    t1_snapshot = classify_known_p0r1_t1(t1_raw)
    t2_snapshot = classify_known_p0r1_t2(t2_raw)
    t1_expectations = _t1_file_expectations(t1_raw)
    t2_expectations = _t2_file_expectations(t2_raw)
    final_by_path = {
        expectation.path: expectation for expectation in t1_expectations
    }
    overwritten_paths: list[str] = []
    for expectation in t2_expectations:
        if expectation.path in final_by_path:
            overwritten_paths.append(expectation.path)
        final_by_path[expectation.path] = expectation

    checks: list[LegacyFileCheck] = []
    for path in sorted(final_by_path):
        expectation = final_by_path[path]
        raw_actual = current_file_sha256.get(path)
        if raw_actual is None:
            actual_sha256 = None
            status = "MISSING"
        else:
            actual_sha256 = _require_legacy_sha256(
                raw_actual,
                "current file map",
            )
            status = (
                "MATCH"
                if hmac.compare_digest(
                    actual_sha256,
                    expectation.expected_sha256,
                )
                else "MISMATCH"
            )
        checks.append(
            LegacyFileCheck(
                path=path,
                expected_sha256=expectation.expected_sha256,
                actual_sha256=actual_sha256,
                expected_source_id=expectation.source_id,
                status=status,
            )
        )

    ready = all(check.status == "MATCH" for check in checks)
    return P0R1AdoptionSnapshot(
        source_order=("P0R1-T1", "P0R1-T2"),
        source_snapshots=(t1_snapshot, t2_snapshot),
        overwritten_paths=tuple(sorted(overwritten_paths)),
        file_checks=tuple(checks),
        code_delta_status="MATCH" if ready else "MISMATCH",
        adoption_status=(
            "READY_FOR_REQUIRED_TEST_REVALIDATION"
            if ready
            else "BLOCKED_BY_FILE_DELTA"
        ),
        ready_for_test_revalidation=ready,
        execution_eligible=False,
        legacy_gate_eligible=False,
        p0r1_evidence_directly_counted=False,
        inventory_disposition="INITIAL_PROVISIONAL_ONLY",
        entry_policy_final_gate_eligible=False,
        known_total_tokens=t1_snapshot.known_total_tokens,
        unknown_usage_count=t1_snapshot.unknown_usage_count,
        usage_owner_source_id="P0R1-T1",
        count_in_target_total=False,
    )
