"""Mechanical, fail-closed phase-gate reports for P0 through P8."""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Callable

from .contracts import Phase, canonical_json
from .stores import AuthorityReader


GATE_REPORT_V1 = "control_plane.gate_report.v1"
_GATE_HASH_DOMAIN = b"control_plane.gate_report.v1\0"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_DRAFT_FIELDS = frozenset(
    {
        "plan_version",
        "phase",
        "attempt_id",
        "identity_binding",
        "task_reports",
        "implementation_baseline",
        "code_freeze_manifest",
        "final_inventory",
        "reviewed_entry_policy",
        "scheduler_inventory",
        "test_receipts",
        "authority_snapshot",
        "side_effect_summary",
        "file_delta_summary",
        "unresolved_risks",
    }
)


_COMPUTED_FIELDS = frozenset(
    {
        "schema_version",
        "verdict",
        "reason_codes",
        "auto_advance",
        "created_at",
        "gate_report_sha256",
    }
)


class GateError(RuntimeError):
    """Base error for generic phase-gate operations."""


class GateBuildError(GateError):
    """Raised when an untrusted gate draft cannot be built safely."""


class GateValidationError(GateError):
    """Raised when a GateReport is malformed or internally inconsistent."""


class GateAuthorityMismatchError(GateError):
    """Raised when a gate report disagrees with current trusted authority."""


def _require_nonempty(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise GateValidationError(f"{field_name} must be a canonical string")
    return value


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise GateValidationError(f"{field_name} must be a lowercase SHA-256")
    return value


def _require_closed_mapping(
    value: object,
    field_name: str,
    fields: frozenset[str],
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise GateValidationError(f"{field_name} has an invalid field contract")
    return value


def _require_string_array(value: object, field_name: str) -> list[str]:
    if (
        not isinstance(value, list)
        or any(
            not isinstance(item, str) or not item or item != item.strip()
            for item in value
        )
        or len(value) != len(set(value))
    ):
        raise GateValidationError(
            f"{field_name} must contain unique canonical strings"
        )
    return value


def _require_repo_ref(value: object, field_name: str) -> str:
    reference = _require_nonempty(value, field_name)
    if (
        reference.startswith("/")
        or "\\" in reference
        or any(part in ("", ".", "..") for part in reference.split("/"))
    ):
        raise GateValidationError(f"{field_name} must be repository-relative")
    return reference


def _validate_artifact(value: object, field_name: str) -> None:
    artifact = _require_closed_mapping(
        value,
        field_name,
        frozenset({"ref", "sha256"}),
    )
    _require_repo_ref(artifact["ref"], f"{field_name}.ref")
    _require_sha256(artifact["sha256"], f"{field_name}.sha256")


def _utc_text(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise GateBuildError("gate clock must return a timezone-aware datetime")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def gate_report_sha256(report: Mapping[str, object]) -> str:
    if not isinstance(report, Mapping):
        raise GateValidationError("gate report must be a mapping")
    payload = dict(report)
    payload.pop("gate_report_sha256", None)
    return hashlib.sha256(
        _GATE_HASH_DOMAIN + canonical_json(payload).encode("utf-8")
    ).hexdigest()


def _derive_gate_verdict(
    report: Mapping[str, object],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    task_reports = report["task_reports"]
    if not isinstance(task_reports, list):
        raise GateValidationError("task_reports must be a list")
    if not task_reports:
        reasons.append("MISSING_TASK_REPORTS")
    for task_report in task_reports:
        if not isinstance(task_report, Mapping):
            raise GateValidationError("task report reference must be a mapping")
        if task_report["outcome"] != "PASS":
            reasons.append(
                f"TASK_REPORT_NOT_PASS:{task_report['ticket_id']}"
            )

    scheduler = report["scheduler_inventory"]
    if not isinstance(scheduler, Mapping):
        raise GateValidationError("scheduler_inventory must be a mapping")
    if scheduler["status"] != "VERIFIED":
        reasons.append(f"SCHEDULER_{scheduler['status']}")

    receipts = report["test_receipts"]
    if not isinstance(receipts, list):
        raise GateValidationError("test_receipts must be a list")
    if not receipts:
        reasons.append("MISSING_GATE_TEST_RECEIPTS")
    for receipt in receipts:
        if not isinstance(receipt, Mapping):
            raise GateValidationError("gate test receipt must be a mapping")
        if receipt["result"] != "PASS" or receipt["exit_code"] != 0:
            reasons.append(f"GATE_TEST_FAILED:{receipt['receipt_id']}")

    authority = report["authority_snapshot"]
    if not isinstance(authority, Mapping):
        raise GateValidationError("authority_snapshot must be a mapping")
    active_grant_ids = authority["active_grant_ids"]
    if not isinstance(active_grant_ids, list):
        raise GateValidationError("authority_snapshot.active_grant_ids invalid")
    if len(active_grant_ids) != 1:
        reasons.append(f"ACTIVE_GRANT_COUNT:{len(active_grant_ids)}")
    succeeded_ticket_ids = authority["succeeded_ticket_ids"]
    if not isinstance(succeeded_ticket_ids, list):
        raise GateValidationError(
            "authority_snapshot.succeeded_ticket_ids invalid"
        )
    known_ticket_ids = set(succeeded_ticket_ids)
    for field_name, reason_prefix in (
        ("open_ticket_ids", "OPEN_TICKET"),
        ("failed_ticket_ids", "FAILED_TICKET"),
        ("in_doubt_ticket_ids", "IN_DOUBT_TICKET"),
    ):
        values = authority[field_name]
        if not isinstance(values, list):
            raise GateValidationError(f"authority_snapshot.{field_name} invalid")
        known_ticket_ids.update(values)
        reasons.extend(f"{reason_prefix}:{value}" for value in sorted(values))
    reported_ticket_ids = {
        str(task_report["ticket_id"]) for task_report in task_reports
    }
    reasons.extend(
        f"MISSING_TASK_REPORT:{ticket_id}"
        for ticket_id in sorted(set(succeeded_ticket_ids) - reported_ticket_ids)
    )
    reasons.extend(
        f"UNKNOWN_TASK_REPORT:{ticket_id}"
        for ticket_id in sorted(reported_ticket_ids - known_ticket_ids)
    )
    pending_count = authority["pending_outbox_count"]
    if type(pending_count) is not int or pending_count < 0:
        raise GateValidationError("pending_outbox_count must be non-negative")
    if pending_count:
        reasons.append(f"PENDING_OUTBOX:{pending_count}")

    side_effects = report["side_effect_summary"]
    if not isinstance(side_effects, Mapping):
        raise GateValidationError("side_effect_summary must be a mapping")
    reasons.extend(
        f"UNAUTHORIZED_SIDE_EFFECT:{effect}"
        for effect in sorted(side_effects["unauthorized"])
    )
    file_delta = report["file_delta_summary"]
    if not isinstance(file_delta, Mapping):
        raise GateValidationError("file_delta_summary must be a mapping")
    reasons.extend(
        f"UNEXPECTED_CHANGE:{path}"
        for path in sorted(file_delta["unexpected_changes"])
    )
    reasons.extend(
        f"UNRESOLVED_RISK:{risk}"
        for risk in sorted(report["unresolved_risks"])
    )
    return ("PASS", []) if not reasons else ("FAIL", reasons)


def _validate_nested_contracts(report: Mapping[str, object]) -> None:
    identity = _require_closed_mapping(
        report["identity_binding"],
        "identity_binding",
        frozenset({"plan_hash", "scope_hash", "instruction_policy_hash"}),
    )
    for field_name in identity:
        _require_sha256(identity[field_name], f"identity_binding.{field_name}")

    task_reports = report["task_reports"]
    if not isinstance(task_reports, list):
        raise GateValidationError("task_reports must be a list")
    seen_tickets: set[str] = set()
    seen_refs: set[str] = set()
    for index, item in enumerate(task_reports):
        task = _require_closed_mapping(
            item,
            f"task_reports[{index}]",
            frozenset({"report_ref", "report_sha256", "ticket_id", "outcome"}),
        )
        report_ref = _require_repo_ref(
            task["report_ref"],
            f"task_reports[{index}].report_ref",
        )
        ticket_id = _require_nonempty(
            task["ticket_id"],
            f"task_reports[{index}].ticket_id",
        )
        _require_sha256(
            task["report_sha256"],
            f"task_reports[{index}].report_sha256",
        )
        if task["outcome"] not in {"PASS", "FAIL", "BLOCKED", "IN_DOUBT"}:
            raise GateValidationError("task report outcome is invalid")
        if ticket_id in seen_tickets or report_ref in seen_refs:
            raise GateValidationError("task report references must be unique")
        seen_tickets.add(ticket_id)
        seen_refs.add(report_ref)

    for field_name in (
        "implementation_baseline",
        "code_freeze_manifest",
        "final_inventory",
        "reviewed_entry_policy",
    ):
        _validate_artifact(report[field_name], field_name)
    scheduler = _require_closed_mapping(
        report["scheduler_inventory"],
        "scheduler_inventory",
        frozenset({"ref", "sha256", "status"}),
    )
    _require_repo_ref(scheduler["ref"], "scheduler_inventory.ref")
    _require_sha256(scheduler["sha256"], "scheduler_inventory.sha256")
    if scheduler["status"] not in {"VERIFIED", "UNKNOWN", "INVALID"}:
        raise GateValidationError("scheduler_inventory.status is invalid")

    receipts = report["test_receipts"]
    if not isinstance(receipts, list):
        raise GateValidationError("test_receipts must be a list")
    receipt_ids: set[str] = set()
    for index, item in enumerate(receipts):
        receipt = _require_closed_mapping(
            item,
            f"test_receipts[{index}]",
            frozenset({"receipt_id", "command", "exit_code", "result"}),
        )
        receipt_id = _require_nonempty(
            receipt["receipt_id"],
            f"test_receipts[{index}].receipt_id",
        )
        _require_nonempty(receipt["command"], f"test_receipts[{index}].command")
        if type(receipt["exit_code"]) is not int:
            raise GateValidationError("gate test exit_code must be an integer")
        if receipt["result"] not in {"PASS", "FAIL"}:
            raise GateValidationError("gate test result is invalid")
        if receipt["result"] == "PASS" and receipt["exit_code"] != 0:
            raise GateValidationError("PASS gate test requires exit_code zero")
        if receipt_id in receipt_ids:
            raise GateValidationError("gate test receipt ids must be unique")
        receipt_ids.add(receipt_id)

    authority = _require_closed_mapping(
        report["authority_snapshot"],
        "authority_snapshot",
        frozenset(
            {
                "active_grant_ids",
                "open_ticket_ids",
                "succeeded_ticket_ids",
                "failed_ticket_ids",
                "in_doubt_ticket_ids",
                "pending_outbox_count",
            }
        ),
    )
    _require_string_array(
        authority["active_grant_ids"],
        "authority_snapshot.active_grant_ids",
    )
    all_ticket_ids: list[str] = []
    for field_name in (
        "open_ticket_ids",
        "succeeded_ticket_ids",
        "failed_ticket_ids",
        "in_doubt_ticket_ids",
    ):
        values = _require_string_array(
            authority[field_name],
            f"authority_snapshot.{field_name}",
        )
        all_ticket_ids.extend(values)
    if len(all_ticket_ids) != len(set(all_ticket_ids)):
        raise GateValidationError("authority ticket state sets overlap")
    if (
        type(authority["pending_outbox_count"]) is not int
        or authority["pending_outbox_count"] < 0
    ):
        raise GateValidationError("pending_outbox_count must be non-negative")

    side_effects = _require_closed_mapping(
        report["side_effect_summary"],
        "side_effect_summary",
        frozenset({"observed", "unauthorized"}),
    )
    observed = _require_string_array(
        side_effects["observed"],
        "side_effect_summary.observed",
    )
    unauthorized = _require_string_array(
        side_effects["unauthorized"],
        "side_effect_summary.unauthorized",
    )
    if not set(unauthorized).issubset(observed):
        raise GateValidationError("unauthorized side effects must be observed")
    file_delta = _require_closed_mapping(
        report["file_delta_summary"],
        "file_delta_summary",
        frozenset({"changed_files", "unexpected_changes"}),
    )
    changed_files = _require_string_array(
        file_delta["changed_files"],
        "file_delta_summary.changed_files",
    )
    unexpected = _require_string_array(
        file_delta["unexpected_changes"],
        "file_delta_summary.unexpected_changes",
    )
    if not set(unexpected).issubset(changed_files):
        raise GateValidationError("unexpected changes must be changed files")
    _require_string_array(report["unresolved_risks"], "unresolved_risks")


def validate_gate_report(report: Mapping[str, object]) -> None:
    if not isinstance(report, Mapping):
        raise GateValidationError("gate report must be a mapping")
    expected_fields = _DRAFT_FIELDS | _COMPUTED_FIELDS
    if set(report) != expected_fields:
        raise GateValidationError("gate report has an invalid top-level contract")
    if report["schema_version"] != GATE_REPORT_V1:
        raise GateValidationError("unsupported gate report schema")
    _require_nonempty(report["plan_version"], "plan_version")
    if report["phase"] not in {phase.value for phase in Phase}:
        raise GateValidationError("phase must be P0 through P8")
    _require_nonempty(report["attempt_id"], "attempt_id")
    _validate_nested_contracts(report)
    if report["auto_advance"] is not False:
        raise GateValidationError("auto_advance must be false")
    created_at = _require_nonempty(report["created_at"], "created_at")
    try:
        parsed = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise GateValidationError("created_at is invalid") from error
    if parsed.tzinfo is None or _utc_text(parsed) != created_at:
        raise GateValidationError("created_at must be canonical UTC")
    verdict, reasons = _derive_gate_verdict(report)
    if report["verdict"] != verdict or report["reason_codes"] != reasons:
        raise GateValidationError("gate verdict does not match derivation")
    claimed_hash = _require_sha256(
        report["gate_report_sha256"],
        "gate_report_sha256",
    )
    expected_hash = gate_report_sha256(report)
    if not hmac.compare_digest(claimed_hash, expected_hash):
        raise GateValidationError("gate_report_sha256 mismatch")


class PhaseGateBuilder:
    """Build a gate candidate while keeping verdict fields controller-owned."""

    __slots__ = ("_clock",)

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def build(self, draft: Mapping[str, object]) -> dict[str, object]:
        if not isinstance(draft, Mapping):
            raise GateBuildError("gate draft must be a mapping")
        supplied_computed_fields = set(draft) & _COMPUTED_FIELDS
        if supplied_computed_fields:
            names = ", ".join(sorted(supplied_computed_fields))
            raise GateBuildError(
                f"gate draft contains computed fields: {names}"
            )
        if set(draft) != _DRAFT_FIELDS:
            raise GateBuildError("gate draft has an invalid field contract")
        report = dict(draft)
        report["schema_version"] = GATE_REPORT_V1
        report["auto_advance"] = False
        report["created_at"] = _utc_text(self._clock())
        try:
            _validate_nested_contracts(report)
            verdict, reason_codes = _derive_gate_verdict(report)
            report["verdict"] = verdict
            report["reason_codes"] = reason_codes
            report["gate_report_sha256"] = gate_report_sha256(report)
            validate_gate_report(report)
        except (GateValidationError, TypeError, ValueError) as error:
            raise GateBuildError(str(error)) from error
        return report


class PhaseGateVerifier:
    """Verify report integrity against a fresh consistent authority snapshot."""

    __slots__ = ("_authority_reader",)

    def __init__(
        self,
        *,
        authority_reader: AuthorityReader | None = None,
    ) -> None:
        self._authority_reader = authority_reader or AuthorityReader()

    def verify(self, report: Mapping[str, object]) -> None:
        validate_gate_report(report)
        actual = self._authority_reader.phase_gate_snapshot(
            Phase(str(report["phase"])),
            str(report["attempt_id"]),
        )
        if report["authority_snapshot"] != actual.to_report_dict():
            raise GateAuthorityMismatchError(
                "gate authority snapshot does not match current authority"
            )


__all__ = [
    "GateAuthorityMismatchError",
    "GateBuildError",
    "GateError",
    "GateValidationError",
    "PhaseGateBuilder",
    "PhaseGateVerifier",
    "gate_report_sha256",
    "validate_gate_report",
]
