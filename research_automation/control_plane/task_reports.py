"""Fail-closed integrity validation for TaskReport V2 documents.

This module validates evidence documents only. It never creates or grants an
authorization, ticket, lease, or phase transition.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Mapping

from .contracts import canonical_json


TASK_REPORT_V2 = "control_plane.task_report.v2"
_REPORT_HASH_DOMAIN = b"control_plane.task_report.v2\0"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class TaskReportValidationError(ValueError):
    """Raised when a TaskReport V2 document fails closed validation."""


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

    claimed_hash = report.get("report_payload_sha256")
    if not isinstance(claimed_hash, str) or not _SHA256_PATTERN.fullmatch(claimed_hash):
        raise TaskReportValidationError(
            "report_payload_sha256 must be a lowercase SHA-256 digest"
        )
    expected_hash = task_report_v2_payload_sha256(report)
    if not hmac.compare_digest(claimed_hash, expected_hash):
        raise TaskReportValidationError("report_payload_sha256 mismatch")
