"""Mechanical, fail-closed phase-gate reports for P0 through P8."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .contracts import Phase, canonical_json
from .git_evidence import GitEvidenceError, GitBlobReader
from .artifact_semantics import (
    ArtifactBindingError,
    ArtifactSemanticError,
    validate_code_freeze_manifest,
    validate_final_inventory,
    validate_implementation_baseline,
    validate_reviewed_entry_policy,
    validate_scheduler_inventory,
)
from .inventory import (
    UnstableInventoryError,
    verify_current_git_inventory,
)
from .stores import (
    AuthorityIdentity,
    AuthorityReader,
    PhaseGateAuthoritySnapshot,
    PhaseGateClosure,
    PhaseGateClosureConflictError,
    TaskReportAuthorityError,
    _AuthorityStore,
)
from .task_reports import (
    MAX_TASK_REPORT_V2_BYTES,
    TaskReportValidationError,
    parse_task_report_v2_bytes,
)


GATE_REPORT_V1 = "control_plane.gate_report.v1"
_GATE_HASH_DOMAIN = b"control_plane.gate_report.v1\0"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_MAX_GATE_REPORT_BYTES = 256 * 1024
_MAX_GATE_REPORT_DEPTH = 32
_MAX_GATE_ARTIFACT_BYTES = 4 * 1024 * 1024
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


class GateEvidenceError(GateError):
    """Raised when referenced gate evidence is missing or corrupt."""


def _require_nonempty(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise GateValidationError(f"{field_name} must be a canonical string")
    return value


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise GateValidationError(f"{field_name} must be a lowercase SHA-256")
    return value


def _require_enum(
    value: object,
    field_name: str,
    allowed: frozenset[str],
) -> str:
    """Require a hashable, canonical string before membership checks.

    JSON arrays/objects are unhashable.  Checking the type explicitly keeps
    malformed untrusted reports on the controlled validation-error path
    instead of leaking a ``TypeError`` from set membership.
    """
    if not isinstance(value, str) or value not in allowed:
        raise GateValidationError(f"{field_name} is invalid")
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
        or ":" in reference
        or any(character in '<>"|?*' for character in reference)
        or any(part in ("", ".", "..") for part in reference.split("/"))
        or any(
            ord(character) < 32 or ord(character) == 127
            for character in reference
        )
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


def _reject_duplicate_json_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise GateValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _require_bounded_gate_depth(value: object) -> None:
    stack: list[tuple[object, int]] = [(value, 1)]
    while stack:
        current, depth = stack.pop()
        if depth > _MAX_GATE_REPORT_DEPTH:
            raise GateValidationError(
                f"gate report exceeds {_MAX_GATE_REPORT_DEPTH} level "
                "nesting limit"
            )
        if isinstance(current, Mapping):
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)


def parse_gate_report_v1_bytes(raw: bytes) -> dict[str, object]:
    """Parse exact GateReport V1 bytes without last-write-wins keys."""
    if not isinstance(raw, bytes):
        raise GateValidationError("gate report input must be bytes")
    if len(raw) > _MAX_GATE_REPORT_BYTES:
        raise GateValidationError("gate report exceeds its byte limit")
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise GateValidationError("gate report must be strict UTF-8") from error
    try:
        payload = json.loads(text, object_pairs_hook=_reject_duplicate_json_keys)
    except GateValidationError:
        raise
    except RecursionError as error:
        raise GateValidationError(
            "gate report exceeds its nesting limit"
        ) from error
    except json.JSONDecodeError as error:
        raise GateValidationError("gate report must be valid JSON") from error
    _require_bounded_gate_depth(payload)
    if not isinstance(payload, dict):
        raise GateValidationError("gate report JSON must be an object")
    validate_gate_report(payload)
    return payload


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
            reasons.append(
                "GATE_TEST_FAILED:"
                f"{receipt['ticket_id']}:{receipt['receipt_id']}"
            )

    authority = report["authority_snapshot"]
    if not isinstance(authority, Mapping):
        raise GateValidationError("authority_snapshot must be a mapping")
    active_policy_sha256 = authority["active_entry_policy_sha256"]
    reviewed_policy = report["reviewed_entry_policy"]
    if not isinstance(reviewed_policy, Mapping):
        raise GateValidationError("reviewed_entry_policy must be a mapping")
    if active_policy_sha256 is None:
        reasons.append("MISSING_ACTIVE_ENTRY_POLICY")
    elif active_policy_sha256 != reviewed_policy["sha256"]:
        reasons.append("ACTIVE_ENTRY_POLICY_MISMATCH")
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


def _project_task_report_evidence(
    task_reports: list[dict[str, object]],
) -> dict[str, object]:
    test_receipts: list[dict[str, object]] = []
    observed_effects: set[str] = set()
    unauthorized_effects: set[str] = set()
    changed_files: set[str] = set()
    unexpected_changes: set[str] = set()
    unresolved_risks: set[str] = set()
    for report in sorted(task_reports, key=lambda item: str(item["ticket_id"])):
        ticket_id = str(report["ticket_id"])
        for receipt in report["test_receipts"]:
            if not isinstance(receipt, Mapping):
                raise GateEvidenceError("TaskReport test receipt is invalid")
            test_receipts.append(
                {
                    "ticket_id": ticket_id,
                    "receipt_id": str(receipt["receipt_id"]),
                    "command": str(receipt["command"]),
                    "exit_code": receipt["exit_code"],
                    "result": str(receipt["result"]),
                }
            )
        side_effects = report["side_effect_summary"]
        if not isinstance(side_effects, Mapping):
            raise GateEvidenceError("TaskReport side-effect summary is invalid")
        observed_effects.update(str(item) for item in side_effects["observed"])
        unauthorized_effects.update(
            str(item) for item in side_effects["unauthorized"]
        )
        for changed_file in report["changed_files"]:
            if not isinstance(changed_file, Mapping):
                raise GateEvidenceError("TaskReport changed file is invalid")
            changed_files.add(str(changed_file["path"]))
        unexpected_changes.update(
            str(item) for item in report["unexpected_changes"]
        )
        for finding in report["review_findings"]:
            if not isinstance(finding, Mapping):
                raise GateEvidenceError("TaskReport review finding is invalid")
            if finding["status"] == "OPEN":
                unresolved_risks.add(
                    f"{ticket_id}:{finding['finding_id']}"
                )
    test_receipts.sort(
        key=lambda item: (str(item["ticket_id"]), str(item["receipt_id"]))
    )
    return {
        "test_receipts": test_receipts,
        "side_effect_summary": {
            "observed": sorted(observed_effects),
            "unauthorized": sorted(unauthorized_effects),
        },
        "file_delta_summary": {
            "changed_files": sorted(changed_files),
            "unexpected_changes": sorted(unexpected_changes),
        },
        "unresolved_risks": sorted(unresolved_risks),
    }


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
        _require_enum(
            task["outcome"],
            f"task_reports[{index}].outcome",
            frozenset({"PASS", "FAIL", "BLOCKED", "IN_DOUBT"}),
        )
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
    _require_enum(
        scheduler["status"],
        "scheduler_inventory.status",
        frozenset({"VERIFIED", "UNKNOWN", "INVALID"}),
    )

    receipts = report["test_receipts"]
    if not isinstance(receipts, list):
        raise GateValidationError("test_receipts must be a list")
    receipt_ids: set[tuple[str, str]] = set()
    for index, item in enumerate(receipts):
        receipt = _require_closed_mapping(
            item,
            f"test_receipts[{index}]",
            frozenset(
                {
                    "ticket_id",
                    "receipt_id",
                    "command",
                    "exit_code",
                    "result",
                }
            ),
        )
        ticket_id = _require_nonempty(
            receipt["ticket_id"],
            f"test_receipts[{index}].ticket_id",
        )
        receipt_id = _require_nonempty(
            receipt["receipt_id"],
            f"test_receipts[{index}].receipt_id",
        )
        _require_nonempty(receipt["command"], f"test_receipts[{index}].command")
        if type(receipt["exit_code"]) is not int:
            raise GateValidationError("gate test exit_code must be an integer")
        _require_enum(
            receipt["result"],
            f"test_receipts[{index}].result",
            frozenset({"PASS", "FAIL"}),
        )
        if receipt["result"] == "PASS" and receipt["exit_code"] != 0:
            raise GateValidationError("PASS gate test requires exit_code zero")
        receipt_key = (ticket_id, receipt_id)
        if receipt_key in receipt_ids:
            raise GateValidationError(
                "gate test receipt ticket/id pairs must be unique"
            )
        receipt_ids.add(receipt_key)

    authority = _require_closed_mapping(
        report["authority_snapshot"],
        "authority_snapshot",
        frozenset(
            {
                "active_entry_policy_sha256",
                "active_grant_ids",
                "open_ticket_ids",
                "succeeded_ticket_ids",
                "failed_ticket_ids",
                "in_doubt_ticket_ids",
                "pending_outbox_count",
            }
        ),
    )
    active_policy_sha256 = authority["active_entry_policy_sha256"]
    if active_policy_sha256 is not None:
        _require_sha256(
            active_policy_sha256,
            "authority_snapshot.active_entry_policy_sha256",
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
    _require_enum(
        report["phase"],
        "phase",
        frozenset(phase.value for phase in Phase),
    )
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
            serialized = canonical_json(report).encode("utf-8")
            if len(serialized) > _MAX_GATE_REPORT_BYTES:
                raise GateBuildError("gate report exceeds its byte limit")
            validate_gate_report(report)
        except (GateValidationError, TypeError, ValueError) as error:
            raise GateBuildError(str(error)) from error
        return report


class PhaseGateVerifier:
    """Verify report integrity against a fresh consistent authority snapshot."""

    __slots__ = ("_authority_reader", "_repository_root")

    def __init__(
        self,
        *,
        authority_reader: AuthorityReader | None = None,
        repository_root: str | Path | None = None,
    ) -> None:
        self._authority_reader = authority_reader or AuthorityReader()
        try:
            root = Path(
                repository_root
                if repository_root is not None
                else Path(__file__).resolve().parents[2]
            ).resolve(strict=True)
        except OSError as error:
            raise GateEvidenceError(
                "gate repository root is unavailable"
            ) from error
        if not root.is_dir():
            raise GateEvidenceError("gate repository root is not a directory")
        self._repository_root = root

    def _read_repository_bytes(
        self,
        reference: str,
        *,
        max_bytes: int,
        evidence_name: str,
    ) -> bytes:
        """Read canonical evidence from committed regular Git blobs only.

        A dirty, uncommitted, symlinked or traversing reference fails closed.
        """
        try:
            return GitBlobReader(self._repository_root).read(
                reference,
                max_bytes=max_bytes,
                evidence_name=evidence_name,
            ).raw
        except GitEvidenceError as error:
            raise GateEvidenceError(str(error)) from error

    def _verify_task_report_files(
        self,
        report: Mapping[str, object],
    ) -> list[dict[str, object]]:
        task_reports = report["task_reports"]
        if not isinstance(task_reports, list):
            raise GateEvidenceError("gate TaskReport references are invalid")
        parsed_reports: list[dict[str, object]] = []
        for task_report in task_reports:
            if not isinstance(task_report, Mapping):
                raise GateEvidenceError("gate TaskReport reference is invalid")
            raw = self._read_repository_bytes(
                str(task_report["report_ref"]),
                max_bytes=MAX_TASK_REPORT_V2_BYTES,
                evidence_name="TaskReport",
            )
            actual_sha256 = hashlib.sha256(raw).hexdigest()
            if not hmac.compare_digest(
                str(task_report["report_sha256"]),
                actual_sha256,
            ):
                raise GateEvidenceError("TaskReport evidence SHA-256 mismatch")
            try:
                parsed = parse_task_report_v2_bytes(raw)
            except TaskReportValidationError as error:
                raise GateEvidenceError(
                    "TaskReport evidence is not valid TaskReport V2"
                ) from error
            if (
                parsed["ticket_id"] != task_report["ticket_id"]
                or parsed["outcome"] != task_report["outcome"]
            ):
                raise GateEvidenceError(
                    "TaskReport reference does not match its contents"
                )
            if (
                parsed["phase"] != report["phase"]
                or parsed["attempt_id"] != report["attempt_id"]
                or parsed["identity_binding"] != report["identity_binding"]
            ):
                raise GateAuthorityMismatchError(
                    "TaskReport does not match the gate identity"
                )
            try:
                self._authority_reader.verify_task_report_binding(parsed)
            except TaskReportAuthorityError as error:
                raise GateAuthorityMismatchError(
                    "TaskReport does not match trusted authority"
                ) from error
            parsed_reports.append(parsed)
        return parsed_reports

    def _verify_gate_artifact_files(
        self,
        report: Mapping[str, object],
    ) -> dict[str, dict[str, object]]:
        artifacts: dict[str, dict[str, object]] = {}
        raw_artifacts: dict[str, bytes] = {}
        references: set[str] = set()
        task_report_refs = {
            str(item["report_ref"])
            for item in report["task_reports"]
            if isinstance(item, Mapping)
        }
        for field_name in (
            "implementation_baseline",
            "code_freeze_manifest",
            "final_inventory",
            "reviewed_entry_policy",
            "scheduler_inventory",
        ):
            artifact = report[field_name]
            if not isinstance(artifact, Mapping):
                raise GateEvidenceError(f"{field_name} reference is invalid")
            reference = str(artifact["ref"])
            if reference in references or reference in task_report_refs:
                raise GateEvidenceError(
                    "gate evidence artifact references must be distinct"
                )
            references.add(reference)
            raw = self._read_repository_bytes(
                reference,
                max_bytes=_MAX_GATE_ARTIFACT_BYTES,
                evidence_name=field_name,
            )
            actual_sha256 = hashlib.sha256(raw).hexdigest()
            if not hmac.compare_digest(
                str(artifact["sha256"]),
                actual_sha256,
            ):
                raise GateEvidenceError(f"{field_name} SHA-256 mismatch")
            raw_artifacts[field_name] = raw

        policy_reference = report["reviewed_entry_policy"]
        if not isinstance(policy_reference, Mapping):
            raise GateEvidenceError("reviewed entry policy reference is invalid")
        expected_policy_ref = (
            "research_state/control_plane/policies/"
            f"{policy_reference['sha256']}.json"
        )
        if policy_reference["ref"] != expected_policy_ref:
            raise GateEvidenceError(
                "reviewed entry policy is outside the immutable policy namespace"
            )

        identity = report["identity_binding"]
        if not isinstance(identity, Mapping):
            raise GateAuthorityMismatchError("gate identity binding is invalid")
        expected_identity = {
            "plan_hash": str(identity["plan_hash"]),
            "scope_hash": str(identity["scope_hash"]),
            "instruction_policy_hash": str(identity["instruction_policy_hash"]),
        }
        try:
            artifacts["implementation_baseline"] = validate_implementation_baseline(
                raw_artifacts["implementation_baseline"],
                expected_plan_version=str(report["plan_version"]),
                expected_phase=str(report["phase"]),
                expected_attempt_id=str(report["attempt_id"]),
                repository_root=self._repository_root,
            )
            artifacts["code_freeze_manifest"] = validate_code_freeze_manifest(
                raw_artifacts["code_freeze_manifest"],
                expected_plan_version=str(report["plan_version"]),
                expected_phase=str(report["phase"]),
                expected_attempt_id=str(report["attempt_id"]),
                expected_identity=expected_identity,
                repository_root=self._repository_root,
            )
            freeze_schema = artifacts["code_freeze_manifest"]["schema_version"]
            if freeze_schema != "control_plane.code_freeze_manifest.v2":
                raise ArtifactSemanticError(
                    "new phase gates require Git source identity evidence"
                )
            if (
                str(report["implementation_baseline"]["ref"])
                == str(report["code_freeze_manifest"]["ref"])
            ):
                raise ArtifactSemanticError(
                    "implementation baseline and code-freeze artifact must be distinct"
                )
            artifacts["final_inventory"] = validate_final_inventory(
                raw_artifacts["final_inventory"],
                expected_plan_version=str(report["plan_version"]),
                expected_phase=str(report["phase"]),
                expected_attempt_id=str(report["attempt_id"]),
                expected_identity=expected_identity,
                freeze_manifest=artifacts["code_freeze_manifest"],
            )
            try:
                verify_current_git_inventory(
                    self._repository_root,
                    freeze_manifest=artifacts["code_freeze_manifest"],
                    final_inventory=artifacts["final_inventory"],
                )
            except UnstableInventoryError as error:
                raise ArtifactSemanticError(
                    f"current executable surface cannot be verified: {error}"
                ) from error
            artifacts["reviewed_entry_policy"] = validate_reviewed_entry_policy(
                raw_artifacts["reviewed_entry_policy"],
                expected_plan_version=str(report["plan_version"]),
                expected_phase=str(report["phase"]),
                expected_attempt_id=str(report["attempt_id"]),
                expected_identity=expected_identity,
                final_inventory=artifacts["final_inventory"],
            )
            _, scheduler_status = validate_scheduler_inventory(
                raw_artifacts["scheduler_inventory"],
                expected_phase=str(report["phase"]),
                final_inventory=artifacts["final_inventory"],
            )
            scheduler_record = report["scheduler_inventory"]
            if scheduler_record["status"] != scheduler_status:
                raise ArtifactSemanticError(
                    "GateReport scheduler status is not derived from evidence"
                )
            artifacts["scheduler_inventory"] = {
                "status": scheduler_status,
            }
        except ArtifactBindingError as error:
            raise GateAuthorityMismatchError(str(error)) from error
        except ArtifactSemanticError as error:
            raise GateEvidenceError(str(error)) from error
        return artifacts

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
        artifacts = self._verify_evidence(report)
        if report["verdict"] == "PASS":
            self._verify_active_entry_policy_binding(
                report,
                artifacts=artifacts,
                authority_snapshot=actual,
            )

    def _verify_active_entry_policy_binding(
        self,
        report: Mapping[str, object],
        *,
        artifacts: Mapping[str, Mapping[str, object]],
        authority_snapshot: PhaseGateAuthoritySnapshot,
    ) -> None:
        active = self._authority_reader.active_entry_policy()
        active_snapshot_digest = authority_snapshot.active_entry_policy_sha256
        if active is None or active.policy_sha256 != active_snapshot_digest:
            raise GateAuthorityMismatchError(
                "active entry policy changed during gate verification"
            )
        policy_reference = report["reviewed_entry_policy"]
        policy = artifacts["reviewed_entry_policy"]
        inventory = artifacts["final_inventory"]
        identity_binding = report["identity_binding"]
        if not isinstance(policy_reference, Mapping) or not isinstance(
            identity_binding,
            Mapping,
        ):
            raise GateAuthorityMismatchError(
                "active entry policy gate binding is invalid"
            )
        expected_identity = AuthorityIdentity(
            plan_hash=str(identity_binding["plan_hash"]),
            scope_hash=str(identity_binding["scope_hash"]),
            instruction_policy_hash=str(
                identity_binding["instruction_policy_hash"]
            ),
        )
        expected_binding = (
            str(policy_reference["sha256"]),
            str(policy["policy_payload_sha256"]),
            str(inventory["inventory_payload_sha256"]),
            str(policy["review_receipt_sha256"]),
            str(policy["reviewer_id"]),
            Phase(str(report["phase"])),
            str(report["attempt_id"]),
            expected_identity,
        )
        actual_binding = (
            active.policy_sha256,
            active.policy_payload_sha256,
            active.inventory_payload_sha256,
            active.review_receipt_sha256,
            active.reviewer.actor_id,
            active.phase,
            active.attempt_id,
            active.identity,
        )
        if actual_binding != expected_binding:
            raise GateAuthorityMismatchError(
                "active entry policy binding does not match gate evidence"
            )
        if active.ticket_id not in authority_snapshot.succeeded_ticket_ids:
            raise GateAuthorityMismatchError(
                "active entry policy ticket is not succeeded"
            )

    def _verify_evidence(
        self,
        report: Mapping[str, object],
    ) -> dict[str, dict[str, object]]:
        artifacts = self._verify_gate_artifact_files(report)
        parsed_task_reports = self._verify_task_report_files(report)
        baseline_ref = report["implementation_baseline"]
        if not isinstance(baseline_ref, Mapping):
            raise GateEvidenceError("implementation baseline reference is invalid")
        for task_report in parsed_task_reports:
            if (
                task_report["baseline_ref"] != baseline_ref["ref"]
                or task_report["baseline_sha256"] != baseline_ref["sha256"]
            ):
                raise GateAuthorityMismatchError(
                    "TaskReport baseline does not match the gate baseline"
                )
        projected = _project_task_report_evidence(parsed_task_reports)
        if any(
            report[field_name] != projected[field_name]
            for field_name in (
                "test_receipts",
                "side_effect_summary",
                "file_delta_summary",
                "unresolved_risks",
            )
        ):
            raise GateEvidenceError(
                "gate fields do not match projected TaskReport evidence"
            )
        return artifacts

    def verify_evidence(self, report: Mapping[str, object]) -> None:
        validate_gate_report(report)
        self._verify_evidence(report)

    def verify_bytes(self, raw: bytes) -> None:
        self.verify(parse_gate_report_v1_bytes(raw))


class PhaseGateCloser:
    """Re-verify and atomically seal one immutable phase-gate result."""

    __slots__ = ("_authority_reader", "_authority_store", "_verifier")

    def __init__(
        self,
        *,
        root_secret: str,
        authority_reader: AuthorityReader | None = None,
        repository_root: str | Path | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        reader = authority_reader or AuthorityReader()
        self._authority_reader = reader
        self._verifier = PhaseGateVerifier(
            authority_reader=reader,
            repository_root=repository_root,
        )
        self._authority_store = _AuthorityStore(
            root_secret=root_secret,
            clock=clock,
        )

    def close(self, report: Mapping[str, object]) -> PhaseGateClosure:
        validate_gate_report(report)
        phase = Phase(str(report["phase"]))
        attempt_id = str(report["attempt_id"])
        identity_binding = report["identity_binding"]
        if not isinstance(identity_binding, Mapping):
            raise GateAuthorityMismatchError("gate identity is invalid")
        identity = AuthorityIdentity(
            plan_hash=str(identity_binding["plan_hash"]),
            scope_hash=str(identity_binding["scope_hash"]),
            instruction_policy_hash=str(
                identity_binding["instruction_policy_hash"]
            ),
        )
        gate_report_digest = str(report["gate_report_sha256"])
        verdict = str(report["verdict"])
        existing = self._authority_reader.phase_gate_closure(
            phase,
            attempt_id,
        )
        if existing is not None:
            self._verifier.verify_evidence(report)
            if (
                existing.gate_report_sha256 == gate_report_digest
                and existing.verdict == verdict
                and existing.identity == identity
            ):
                return existing
            raise PhaseGateClosureConflictError(
                "phase attempt already has a different immutable closure"
            )

        self._verifier.verify(report)
        authority_snapshot = report["authority_snapshot"]
        if not isinstance(authority_snapshot, Mapping):
            raise GateAuthorityMismatchError(
                "gate authority snapshot is invalid"
            )
        return self._authority_store._close_phase_gate(
            phase=phase,
            attempt_id=attempt_id,
            identity=identity,
            authority_snapshot=authority_snapshot,
            gate_report_sha256=gate_report_digest,
            verdict=verdict,
        )

    def close_bytes(self, raw: bytes) -> PhaseGateClosure:
        return self.close(parse_gate_report_v1_bytes(raw))


__all__ = [
    "GateAuthorityMismatchError",
    "GateBuildError",
    "GateEvidenceError",
    "GateError",
    "GateValidationError",
    "PhaseGateBuilder",
    "PhaseGateCloser",
    "PhaseGateVerifier",
    "gate_report_sha256",
    "parse_gate_report_v1_bytes",
    "validate_gate_report",
]
