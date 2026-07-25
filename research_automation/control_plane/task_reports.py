"""Fail-closed integrity validation for TaskReport V2 documents.

This module validates evidence documents only. It never creates or grants an
authorization, ticket, lease, or phase transition.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Mapping

from .contracts import Phase, canonical_json


TASK_REPORT_V2 = "control_plane.task_report.v2"
_REPORT_HASH_DOMAIN = b"control_plane.task_report.v2\0"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY_BINDING_FIELDS = frozenset(
    {"plan_hash", "scope_hash", "instruction_policy_hash"}
)
_TASK_OUTCOMES = frozenset({"PASS", "FAIL", "BLOCKED", "IN_DOUBT"})
_PHASES = frozenset(phase.value for phase in Phase)
_TASK_REPORT_V2_FIELDS = frozenset(
    {
        "schema_version",
        "plan_version",
        "phase",
        "task_id",
        "attempt_id",
        "authorization_ref",
        "identity_binding",
        "objective",
        "dependencies",
        "idempotency_key",
        "allowed_files",
        "forbidden_files",
        "baseline_ref",
        "baseline_sha256",
        "input_evidence_refs",
        "test_receipts",
        "review_findings",
        "changed_files",
        "unexpected_changes",
        "external_invocations",
        "side_effect_summary",
        "outcome",
        "started_at",
        "completed_at",
        "report_payload_sha256",
    }
)


class TaskReportValidationError(ValueError):
    """Raised when a TaskReport V2 document fails closed validation."""


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not _SHA256_PATTERN.fullmatch(value):
        raise TaskReportValidationError(
            f"{field_name} must be a lowercase SHA-256 digest"
        )
    return value


def task_report_v2_payload_sha256(report: Mapping[str, object]) -> str:
    """Return the domain-separated hash of all fields except the hash field."""
    if not isinstance(report, Mapping):
        raise TaskReportValidationError("task report must be a mapping")
    payload = dict(report)
    payload.pop("report_payload_sha256", None)
    canonical_payload = canonical_json(payload).encode("utf-8")
    return hashlib.sha256(_REPORT_HASH_DOMAIN + canonical_payload).hexdigest()


def validate_task_report_v2(report: Mapping[str, object]) -> None:
    """Validate TaskReport V2 identity and payload integrity without side effects."""
    if not isinstance(report, Mapping):
        raise TaskReportValidationError("task report must be a mapping")
    if report.get("schema_version") != TASK_REPORT_V2:
        raise TaskReportValidationError("schema_version must be control_plane.task_report.v2")
    missing_fields = _TASK_REPORT_V2_FIELDS - set(report)
    if missing_fields:
        names = ", ".join(sorted(missing_fields))
        raise TaskReportValidationError(f"task report is missing fields: {names}")
    unknown_fields = set(report) - _TASK_REPORT_V2_FIELDS
    if unknown_fields:
        names = ", ".join(sorted(unknown_fields))
        raise TaskReportValidationError(f"task report contains unknown fields: {names}")
    if not isinstance(report["changed_files"], list):
        raise TaskReportValidationError("changed_files must be a list")
    identity_binding = report["identity_binding"]
    if not isinstance(identity_binding, Mapping):
        raise TaskReportValidationError("identity_binding must be a mapping")
    if set(identity_binding) != _IDENTITY_BINDING_FIELDS:
        raise TaskReportValidationError(
            "identity_binding must contain exactly plan_hash, scope_hash, and "
            "instruction_policy_hash"
        )
    for field_name in sorted(_IDENTITY_BINDING_FIELDS):
        _require_sha256(
            identity_binding[field_name],
            f"identity_binding.{field_name}",
        )
    if report["phase"] not in _PHASES:
        raise TaskReportValidationError("phase must be P0 through P8")
    if report["outcome"] not in _TASK_OUTCOMES:
        raise TaskReportValidationError(
            "outcome must be PASS, FAIL, BLOCKED, or IN_DOUBT"
        )

    claimed_hash = _require_sha256(
        report.get("report_payload_sha256"),
        "report_payload_sha256",
    )
    expected_hash = task_report_v2_payload_sha256(report)
    if not hmac.compare_digest(claimed_hash, expected_hash):
        raise TaskReportValidationError("report_payload_sha256 mismatch")
