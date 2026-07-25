"""Fail-closed building and validation for TaskReport V2 documents.

This module builds and validates evidence documents only. It never creates or
grants an authorization, ticket, lease, or phase transition.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping

from .contracts import Phase, canonical_json


TASK_REPORT_V2 = "control_plane.task_report.v2"
MAX_TASK_REPORT_V2_BYTES = 64 * 1024
MAX_TASK_REPORT_V2_DEPTH = 64
_REPORT_HASH_DOMAIN = b"control_plane.task_report.v2\0"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY_BINDING_FIELDS = frozenset(
    {"plan_hash", "scope_hash", "instruction_policy_hash"}
)
_TASK_OUTCOMES = frozenset({"PASS", "FAIL", "BLOCKED", "IN_DOUBT"})
_PHASES = frozenset(phase.value for phase in Phase)
_COMPUTED_FIELDS = frozenset(
    {"schema_version", "outcome", "reason_codes", "report_payload_sha256"}
)
_REQUIREMENT_FIELDS = frozenset(
    {
        "required_test_receipt_ids",
        "required_review_receipt_ids",
        "required_evidence_ids",
    }
)
_SIDE_EFFECT_SUMMARY_FIELDS = frozenset({"observed", "unauthorized"})
_TEST_RECEIPT_FIELDS = frozenset(
    {"receipt_id", "command", "exit_code", "result"}
)
_REVIEW_RECEIPT_FIELDS = frozenset(
    {"receipt_id", "reviewer_id", "exit_code", "result"}
)
_EVIDENCE_REF_FIELDS = frozenset(
    {"evidence_id", "evidence_ref", "evidence_sha256", "status"}
)
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
        "task_spec_ref",
        "task_spec_sha256",
        "requirements",
        "allowed_files",
        "forbidden_files",
        "baseline_ref",
        "baseline_sha256",
        "input_evidence_refs",
        "test_receipts",
        "review_receipts",
        "review_findings",
        "changed_files",
        "unexpected_changes",
        "external_invocations",
        "side_effect_summary",
        "ticket_state",
        "outcome",
        "reason_codes",
        "started_at",
        "completed_at",
        "report_payload_sha256",
    }
)


class TaskReportValidationError(ValueError):
    """Raised when a TaskReport V2 document fails closed validation."""


class TaskReportBuildError(TaskReportValidationError):
    """Raised when an untrusted draft cannot produce a TaskReport V2."""


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not _SHA256_PATTERN.fullmatch(value):
        raise TaskReportValidationError(
            f"{field_name} must be a lowercase SHA-256 digest"
        )
    return value


def _require_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise TaskReportValidationError(
            f"{field_name} must be a non-empty string"
        )
    return value


def _require_exact_int(value: object, field_name: str) -> int:
    if type(value) is not int:
        raise TaskReportValidationError(
            f"{field_name} must be an exact integer"
        )
    return value


def _require_closed_mapping(
    value: object,
    field_name: str,
    required_fields: frozenset[str],
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TaskReportValidationError(f"{field_name} must be a mapping")
    missing_fields = required_fields - set(value)
    if missing_fields:
        names = ", ".join(sorted(missing_fields))
        raise TaskReportValidationError(
            f"{field_name} is missing fields: {names}"
        )
    unknown_fields = set(value) - required_fields
    if unknown_fields:
        names = ", ".join(sorted(unknown_fields))
        raise TaskReportValidationError(
            f"{field_name} contains unknown fields: {names}"
        )
    return value


def _validate_receipt_common(
    receipt: Mapping[str, object],
    field_name: str,
) -> None:
    _require_non_empty_string(receipt["receipt_id"], f"{field_name}.receipt_id")
    _require_exact_int(receipt["exit_code"], f"{field_name}.exit_code")
    if receipt["result"] not in ("PASS", "FAIL"):
        raise TaskReportValidationError(
            f"{field_name}.result must be PASS or FAIL"
        )
    if receipt["result"] == "PASS" and receipt["exit_code"] != 0:
        raise TaskReportValidationError(
            f"{field_name} PASS requires exit_code 0"
        )


def task_report_v2_payload_sha256(report: Mapping[str, object]) -> str:
    """Return the domain-separated hash of all fields except the hash field."""
    if not isinstance(report, Mapping):
        raise TaskReportValidationError("task report must be a mapping")
    payload = dict(report)
    payload.pop("report_payload_sha256", None)
    canonical_payload = canonical_json(payload).encode("utf-8")
    return hashlib.sha256(_REPORT_HASH_DOMAIN + canonical_payload).hexdigest()


def _require_unique_string_list(value: object, field_name: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise TaskReportValidationError(
            f"{field_name} must be a list of non-empty strings"
        )
    if len(value) != len(set(value)):
        raise TaskReportValidationError(f"{field_name} must not contain duplicates")
    return value


def _receipt_results(
    receipts: object,
    field_name: str,
) -> dict[str, tuple[object, object]]:
    if not isinstance(receipts, list):
        raise TaskReportValidationError(f"{field_name} must be a list")
    results: dict[str, tuple[object, object]] = {}
    for index, receipt in enumerate(receipts):
        if not isinstance(receipt, Mapping):
            raise TaskReportValidationError(
                f"{field_name}[{index}] must be a mapping"
            )
        if field_name == "test_receipts":
            _require_closed_mapping(
                receipt,
                f"{field_name}[{index}]",
                _TEST_RECEIPT_FIELDS,
            )
            _validate_receipt_common(receipt, f"{field_name}[{index}]")
            _require_non_empty_string(
                receipt["command"],
                f"{field_name}[{index}].command",
            )
        elif field_name == "review_receipts":
            _require_closed_mapping(
                receipt,
                f"{field_name}[{index}]",
                _REVIEW_RECEIPT_FIELDS,
            )
            _validate_receipt_common(receipt, f"{field_name}[{index}]")
            _require_non_empty_string(
                receipt["reviewer_id"],
                f"{field_name}[{index}].reviewer_id",
            )
        receipt_id = receipt.get("receipt_id")
        if isinstance(receipt_id, str) and receipt_id:
            if receipt_id in results:
                raise TaskReportValidationError(
                    f"{field_name} must not contain duplicate receipt_id values"
                )
            results[receipt_id] = (
                receipt.get("result"),
                receipt.get("exit_code"),
            )
    return results


def _evidence_statuses(evidence_refs: object) -> dict[str, object]:
    if not isinstance(evidence_refs, list):
        return {}
    statuses: dict[str, object] = {}
    for evidence in evidence_refs:
        if not isinstance(evidence, Mapping):
            continue
        evidence_id = evidence.get("evidence_id")
        if isinstance(evidence_id, str) and evidence_id:
            statuses[evidence_id] = evidence.get("status")
    return statuses


def _validate_evidence_refs(evidence_refs: object) -> None:
    if not isinstance(evidence_refs, list):
        raise TaskReportValidationError("input_evidence_refs must be a list")
    evidence_ids: set[str] = set()
    for index, evidence in enumerate(evidence_refs):
        if not isinstance(evidence, Mapping):
            raise TaskReportValidationError(
                f"input_evidence_refs[{index}] must be a mapping"
            )
        _require_closed_mapping(
            evidence,
            f"input_evidence_refs[{index}]",
            _EVIDENCE_REF_FIELDS,
        )
        _require_sha256(
            evidence["evidence_sha256"],
            f"input_evidence_refs[{index}].evidence_sha256",
        )
        evidence_id = _require_non_empty_string(
            evidence["evidence_id"],
            f"input_evidence_refs[{index}].evidence_id",
        )
        if evidence_id in evidence_ids:
            raise TaskReportValidationError(
                "input_evidence_refs must not contain duplicate evidence_id values"
            )
        evidence_ids.add(evidence_id)
        _require_non_empty_string(
            evidence["evidence_ref"],
            f"input_evidence_refs[{index}].evidence_ref",
        )
        if evidence["status"] not in ("VERIFIED", "INVALID", "IN_DOUBT"):
            raise TaskReportValidationError(
                f"input_evidence_refs[{index}].status must be VERIFIED, "
                "INVALID, or IN_DOUBT"
            )


def _validate_nested_contracts(report: Mapping[str, object]) -> None:
    _receipt_results(report.get("test_receipts"), "test_receipts")
    _receipt_results(report.get("review_receipts"), "review_receipts")
    _validate_evidence_refs(report.get("input_evidence_refs"))


def _derive_outcome(report: Mapping[str, object]) -> tuple[str, list[str]]:
    requirements = report.get("requirements")
    if not isinstance(requirements, Mapping):
        raise TaskReportValidationError("requirements must be a mapping")
    if set(requirements) != _REQUIREMENT_FIELDS:
        raise TaskReportValidationError(
            "requirements must contain exactly required_test_receipt_ids, "
            "required_review_receipt_ids, and required_evidence_ids"
        )
    required_test_ids = _require_unique_string_list(
        requirements["required_test_receipt_ids"],
        "required_test_receipt_ids",
    )
    required_review_ids = _require_unique_string_list(
        requirements["required_review_receipt_ids"],
        "required_review_receipt_ids",
    )
    required_evidence_ids = _require_unique_string_list(
        requirements["required_evidence_ids"],
        "required_evidence_ids",
    )
    evidence_statuses = _evidence_statuses(report.get("input_evidence_refs"))

    ticket_state = report.get("ticket_state")
    if ticket_state == "IN_DOUBT":
        return "IN_DOUBT", ["TICKET_IN_DOUBT"]
    in_doubt_evidence = [
        evidence_id
        for evidence_id in sorted(set(required_evidence_ids) & set(evidence_statuses))
        if evidence_statuses[evidence_id] == "IN_DOUBT"
    ]
    if in_doubt_evidence:
        return "IN_DOUBT", [
            f"REQUIRED_EVIDENCE_IN_DOUBT:{evidence_id}"
            for evidence_id in in_doubt_evidence
        ]
    if ticket_state == "FAILED":
        return "FAIL", ["TICKET_FAILED"]
    if ticket_state != "SUCCEEDED":
        return "BLOCKED", ["TICKET_NOT_SUCCEEDED"]

    test_results = _receipt_results(report.get("test_receipts"), "test_receipts")
    review_results = _receipt_results(
        report.get("review_receipts"),
        "review_receipts",
    )
    unexpected_changes = _require_unique_string_list(
        report.get("unexpected_changes"),
        "unexpected_changes",
    )
    side_effect_summary = report.get("side_effect_summary")
    if not isinstance(side_effect_summary, Mapping):
        raise TaskReportValidationError("side_effect_summary must be a mapping")
    if set(side_effect_summary) != _SIDE_EFFECT_SUMMARY_FIELDS:
        raise TaskReportValidationError(
            "side_effect_summary must contain exactly observed and unauthorized"
        )
    _require_unique_string_list(
        side_effect_summary["observed"],
        "side_effect_summary.observed",
    )
    unauthorized_effects = _require_unique_string_list(
        side_effect_summary["unauthorized"],
        "side_effect_summary.unauthorized",
    )
    failed_reasons = [
        f"UNEXPECTED_CHANGE:{path}" for path in sorted(unexpected_changes)
    ]
    failed_reasons.extend(
        f"UNAUTHORIZED_SIDE_EFFECT:{effect}"
        for effect in sorted(unauthorized_effects)
    )
    failed_reasons.extend(
        f"REQUIRED_TEST_FAILED:{receipt_id}"
        for receipt_id in sorted(set(required_test_ids) & set(test_results))
        if test_results[receipt_id] != ("PASS", 0)
    )
    failed_reasons.extend(
        f"REQUIRED_REVIEW_FAILED:{receipt_id}"
        for receipt_id in sorted(set(required_review_ids) & set(review_results))
        if review_results[receipt_id] != ("PASS", 0)
    )
    failed_reasons.extend(
        f"REQUIRED_EVIDENCE_INVALID:{evidence_id}"
        for evidence_id in sorted(set(required_evidence_ids) & set(evidence_statuses))
        if evidence_statuses[evidence_id] == "INVALID"
    )
    missing_reasons = [
        f"MISSING_REQUIRED_TEST_RECEIPT:{receipt_id}"
        for receipt_id in sorted(set(required_test_ids) - set(test_results))
    ]
    missing_reasons.extend(
        f"MISSING_REQUIRED_REVIEW_RECEIPT:{receipt_id}"
        for receipt_id in sorted(set(required_review_ids) - set(review_results))
    )
    missing_reasons.extend(
        f"MISSING_REQUIRED_EVIDENCE:{evidence_id}"
        for evidence_id in sorted(
            set(required_evidence_ids) - set(evidence_statuses)
        )
    )
    if failed_reasons:
        return "FAIL", failed_reasons + missing_reasons
    if missing_reasons:
        return "BLOCKED", missing_reasons
    return "PASS", []


def build_task_report_v2(draft: Mapping[str, object]) -> dict[str, object]:
    """Build a report while keeping computed fields out of caller control."""
    if not isinstance(draft, Mapping):
        raise TaskReportBuildError("task report draft must be a mapping")
    supplied_computed_fields = set(draft) & _COMPUTED_FIELDS
    if supplied_computed_fields:
        names = ", ".join(sorted(supplied_computed_fields))
        raise TaskReportBuildError(
            f"task report draft contains computed fields: {names}"
        )

    report = dict(draft)
    report["schema_version"] = TASK_REPORT_V2
    try:
        outcome, reason_codes = _derive_outcome(report)
    except TaskReportValidationError as error:
        raise TaskReportBuildError(str(error)) from error
    report["outcome"] = outcome
    report["reason_codes"] = reason_codes
    report["report_payload_sha256"] = task_report_v2_payload_sha256(report)
    validate_task_report_v2(report)
    return report


def _reject_duplicate_json_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise TaskReportValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _require_bounded_json_depth(value: object) -> None:
    stack: list[tuple[object, int]] = [(value, 1)]
    while stack:
        current, depth = stack.pop()
        if depth > MAX_TASK_REPORT_V2_DEPTH:
            raise TaskReportValidationError(
                f"task report exceeds {MAX_TASK_REPORT_V2_DEPTH} level nesting limit"
            )
        if isinstance(current, Mapping):
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)


def parse_task_report_v2_bytes(raw: bytes) -> dict[str, object]:
    """Parse exact UTF-8 JSON bytes without last-write-wins duplicate keys."""
    if not isinstance(raw, bytes):
        raise TaskReportValidationError("task report input must be bytes")
    if len(raw) > MAX_TASK_REPORT_V2_BYTES:
        raise TaskReportValidationError(
            f"task report exceeds {MAX_TASK_REPORT_V2_BYTES} byte limit"
        )
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise TaskReportValidationError("task report must be strict UTF-8") from error
    try:
        payload = json.loads(text, object_pairs_hook=_reject_duplicate_json_keys)
    except TaskReportValidationError:
        raise
    except RecursionError as error:
        raise TaskReportValidationError(
            f"task report exceeds {MAX_TASK_REPORT_V2_DEPTH} level nesting limit"
        ) from error
    except json.JSONDecodeError as error:
        raise TaskReportValidationError("task report must be valid JSON") from error
    _require_bounded_json_depth(payload)
    if not isinstance(payload, dict):
        raise TaskReportValidationError("task report JSON must be an object")
    validate_task_report_v2(payload)
    return payload


def validate_task_report_v2(report: Mapping[str, object]) -> None:
    """Validate TaskReport V2 identity and payload integrity without side effects."""
    if not isinstance(report, Mapping):
        raise TaskReportValidationError("task report must be a mapping")
    if report.get("schema_version") != TASK_REPORT_V2:
        raise TaskReportValidationError(
            "schema_version must be control_plane.task_report.v2"
        )
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
    if not isinstance(report["review_receipts"], list):
        raise TaskReportValidationError("review_receipts must be a list")
    _require_sha256(report["task_spec_sha256"], "task_spec_sha256")
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
    _validate_nested_contracts(report)
    derived_outcome, derived_reasons = _derive_outcome(report)
    if report["outcome"] != derived_outcome:
        raise TaskReportValidationError("outcome does not match mechanical derivation")
    if report["reason_codes"] != derived_reasons:
        raise TaskReportValidationError(
            "reason_codes do not match mechanical derivation"
        )

    claimed_hash = _require_sha256(
        report.get("report_payload_sha256"),
        "report_payload_sha256",
    )
    expected_hash = task_report_v2_payload_sha256(report)
    if not hmac.compare_digest(claimed_hash, expected_hash):
        raise TaskReportValidationError("report_payload_sha256 mismatch")
