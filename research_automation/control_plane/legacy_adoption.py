"""Exact-byte classification for immutable P0R1 TaskReport evidence.

Legacy reports never enter the TaskReport V2 validator and never grant phase,
ticket, lease, gate, or execution authority.
"""

from __future__ import annotations

import hashlib
import hmac
import json
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
