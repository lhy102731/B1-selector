"""Deterministic evidence semantics and learning-commit primitives.

This module is deliberately control-plane-only: it accepts already materialized
runner metadata and never opens market data, KBase content, or raw logs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import time
from typing import Mapping

from .contracts import SideEffect
from .stores import AuthorityReader, TaskReportAuthorityError


_ZERO_SHA256 = "0" * 64
_LEGACY_EVENT_DOMAIN = b"control_plane.learning_commit_event.v1\0"
_EVENT_DOMAIN = b"control_plane.learning_commit_event.v2\0"
_LEARNING_TASK_ID = "P4-LEARNING-COMMIT"
_LEARNING_INPUT_IDS = frozenset(
    {
        "approved-claim",
        "approved-protocol",
        "learning-decision",
        "runner-adapter",
        "runner-artifact",
    }
)
_LEARNING_ALLOWED_FILES = frozenset(
    {
        "research_state/control_plane/learning_commit.sqlite3",
        "research_state/control_plane/learning_packets/",
    }
)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _event_sha256(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(_EVENT_DOMAIN + _canonical_bytes(payload)).hexdigest()


def _legacy_event_sha256(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(
        _LEGACY_EVENT_DOMAIN + _canonical_bytes(payload)
    ).hexdigest()


def _durable_create_only(path: Path, raw: bytes) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if os.name == "nt":
            import ctypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            move = kernel32.MoveFileExW
            move.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint]
            move.restype = ctypes.c_int
            if not move(str(temporary), str(path), 0x8):
                error = ctypes.get_last_error()
                if error not in {80, 183}:
                    raise OSError(error, "durable create-only publication failed")
        else:
            try:
                os.link(temporary, path)
            except FileExistsError:
                pass
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        deadline = time.monotonic() + 1.0
        while True:
            try:
                published_raw = path.read_bytes()
                break
            except (FileNotFoundError, PermissionError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        if published_raw != raw:
            raise ValueError("content-addressed packet conflict")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class EvidenceResult:
    verdict: str
    protocol_conformance: str
    audit_grade: str
    scientific_outcome: str
    promotion_eligible: bool
    evidence_refs: tuple[dict[str, str], ...]
    access_event_ids: tuple[str, ...]
    taint_refs: tuple[str, ...]
    invalidation_codes: tuple[str, ...]


class EvidenceAdapter:
    """Evaluate bounded runner metadata independently of runner booleans."""

    __slots__ = (
        "_known_runners",
        "_approved_protocol",
        "_approved_claim",
    )

    def __init__(
        self,
        *,
        known_runners: Mapping[str, str] | tuple[str, ...] = (),
        approved_protocol: Mapping[str, object] | None = None,
        approved_claim: Mapping[str, object] | None = None,
    ) -> None:
        known_runners_payload = (
            dict(known_runners)
            if isinstance(known_runners, Mapping)
            else {runner: "" for runner in known_runners}
        )
        self._known_runners = self._canonical_mapping_copy(
            known_runners_payload,
            "known_runners",
        )
        self._approved_protocol = None
        if approved_protocol is not None:
            self._approved_protocol = self._canonical_mapping_copy(
                dict(approved_protocol),
                "approved_protocol",
            )
        self._approved_claim = None
        if approved_claim is not None:
            self._approved_claim = self._canonical_mapping_copy(
                dict(approved_claim),
                "approved_claim",
            )

    @staticmethod
    def _canonical_mapping_copy(
        value: Mapping[str, object],
        name: str,
    ) -> dict[str, object]:
        try:
            canonical = json.loads(_canonical_bytes(dict(value)))
        except (TypeError, ValueError, UnicodeError) as error:
            raise ValueError(f"{name} must be canonical JSON") from error
        if not isinstance(canonical, dict):
            raise ValueError(f"{name} must be a mapping")
        return canonical

    def binding_payload(self) -> dict[str, object]:
        """Return the canonical configuration identity for durable receipts."""

        payload = {
            "schema_version": "control_plane.evidence_adapter_binding.v1",
            "adapter_id": "EvidenceAdapter.v1",
            "known_runners": dict(self._known_runners),
            "approved_protocol": self._approved_protocol,
            "approved_claim": self._approved_claim,
        }
        try:
            canonical = json.loads(_canonical_bytes(payload))
        except (TypeError, ValueError, UnicodeError) as error:
            raise ValueError(
                "EvidenceAdapter configuration must be canonical JSON"
            ) from error
        if not isinstance(canonical, dict):
            raise ValueError("EvidenceAdapter configuration is invalid")
        return canonical

    def evaluate(self, artifact: object) -> EvidenceResult:
        if not isinstance(artifact, Mapping):
            return EvidenceResult(
                "EVIDENCE_INVALID",
                "UNKNOWN",
                "INVALID",
                "UNKNOWN",
                False,
                (),
                (),
                (),
                ("INVALID_ARTIFACT_TYPE",),
            )
        required = {"schema_version", "runner", "status", "claim", "protocol_conformance", "artifact_refs", "access_event_ids", "taint_refs"}
        missing = sorted(required - set(artifact))
        if missing:
            return EvidenceResult("EVIDENCE_INVALID", "UNKNOWN", "INVALID", "UNKNOWN", False, (), (), (), tuple(f"MISSING:{name}" for name in missing))
        if artifact["schema_version"] != "runner.artifact.v1":
            return EvidenceResult("EVIDENCE_INVALID", "UNKNOWN", "INVALID", "UNKNOWN", False, (), (), (), ("UNKNOWN_RUNNER_SCHEMA",))
        if not isinstance(artifact["runner"], str) or not artifact["runner"]:
            return EvidenceResult("EVIDENCE_INVALID", "UNKNOWN", "INVALID", "UNKNOWN", False, (), (), (), ("INVALID_RUNNER_ID",))
        if artifact["runner"] not in self._known_runners:
            return EvidenceResult("EVIDENCE_INVALID", "UNKNOWN", "INVALID", "UNKNOWN", False, (), (), (), ("UNKNOWN_RUNNER",))
        expected_runner_version = self._known_runners[artifact["runner"]]
        if expected_runner_version and artifact.get("runner_version") != expected_runner_version:
            return EvidenceResult("EVIDENCE_INVALID", "UNKNOWN", "INVALID", "UNKNOWN", False, (), (), (), ("RUNNER_VERSION_MISMATCH",))
        if self._approved_protocol is not None:
            executed = artifact.get("executed_protocol")
            if not isinstance(executed, Mapping) or dict(executed) != self._approved_protocol:
                return EvidenceResult("EVIDENCE_INVALID", "NONCONFORMING", "INVALID", "UNKNOWN", False, (), (), (), ("EXECUTED_PROTOCOL_MISMATCH",))
        if artifact["status"] != "COMPLETED":
            return EvidenceResult("EVIDENCE_INVALID", "NONCONFORMING", "INVALID", "UNKNOWN", False, (), (), (), ("RUNNER_NOT_COMPLETED",))
        if artifact["protocol_conformance"] != "CONFORMING":
            return EvidenceResult("EVIDENCE_INVALID", "NONCONFORMING", "INVALID", "UNKNOWN", False, (), (), (), ("PROTOCOL_NONCONFORMING",))
        collections = (
            artifact["artifact_refs"],
            artifact["access_event_ids"],
            artifact["taint_refs"],
        )
        if any(not isinstance(value, (list, tuple)) for value in collections):
            return EvidenceResult("EVIDENCE_INVALID", "CONFORMING", "INVALID", "UNKNOWN", False, (), (), (), ("INVALID_ARTIFACT_COLLECTION",))
        if any(
            not isinstance(value, str) or not value
            for value in (*artifact["access_event_ids"], *artifact["taint_refs"])
        ):
            return EvidenceResult("EVIDENCE_INVALID", "CONFORMING", "INVALID", "UNKNOWN", False, (), (), (), ("INVALID_ARTIFACT_COLLECTION",))
        taint_refs = tuple(artifact["taint_refs"])
        if taint_refs:
            return EvidenceResult("EVIDENCE_INVALID", "CONFORMING", "INVALID", "UNKNOWN", False, (), tuple(artifact["access_event_ids"]), taint_refs, ("TAINTED_EVIDENCE",))
        claim = artifact["claim"]
        if claim is None:
            return EvidenceResult("NO_MATERIAL_FINDING", "CONFORMING", "PASS", "NO_MATERIAL_FINDING", False, (), tuple(artifact["access_event_ids"]), tuple(artifact["taint_refs"]), ())
        refs = tuple(artifact["artifact_refs"])
        if any(
            not isinstance(reference, Mapping)
            or set(reference) != {"ref", "sha256"}
            or not isinstance(reference["ref"], str)
            or not reference["ref"]
            or not isinstance(reference["sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", reference["sha256"]) is None
            for reference in refs
        ):
            return EvidenceResult("EVIDENCE_INVALID", "CONFORMING", "INVALID", "UNKNOWN", False, (), tuple(artifact["access_event_ids"]), (), ("INVALID_ARTIFACT_REFERENCE",))
        access_event_ids = tuple(artifact["access_event_ids"])
        if self._approved_claim is not None and isinstance(claim, Mapping):
            if dict(claim) == self._approved_claim and access_event_ids:
                return EvidenceResult("VALID", "CONFORMING", "PASS", str(claim.get("kind", "CLAIM")), True, tuple(dict(reference) for reference in refs), access_event_ids, (), ())
            return EvidenceResult("MATERIAL_UNAPPROVED", "CONFORMING", "PASS", "CLAIM_PRESENT", False, tuple(dict(reference) for reference in refs), access_event_ids, (), ("MATERIAL_UNAPPROVED",))
        return EvidenceResult("RESEARCH_ONLY", "CONFORMING", "PASS", "CLAIM_PRESENT", False, tuple(artifact["artifact_refs"]), tuple(artifact["access_event_ids"]), tuple(artifact["taint_refs"]), ("PROMOTION_REQUIRES_SEMANTIC_REVIEW",))


class LearningCommitAuthorizationError(RuntimeError):
    """Raised when terminal Authority facts do not authorize a projection."""


def _require_claim(claim: object) -> dict[str, object]:
    if not isinstance(claim, dict) or not claim:
        raise ValueError("claim must be a non-empty mapping")
    allowed_fields = {
        "kind",
        "summary",
        "scope",
        "parent_lineage",
        "reopen_predicate",
        "future_usage_guidance",
    }
    if not set(claim).issubset(allowed_fields):
        raise ValueError("claim uses fields outside the closed schema")
    if claim.get("kind") not in {
        "POSITIVE", "NEGATIVE", "PARTIAL", "ANTI_FACTOR", "FAILED_USAGE"
    }:
        raise ValueError("claim kind is invalid")
    for field_name in (
        "summary", "scope", "reopen_predicate", "future_usage_guidance"
    ):
        value = claim.get(field_name)
        if value is not None and (not isinstance(value, str) or len(value) > 4096):
            raise ValueError("claim text field is invalid")
    lineage = claim.get("parent_lineage", ())
    if not isinstance(lineage, (list, tuple)) or any(
        not isinstance(item, str) or not item or len(item) > 256
        for item in lineage
    ):
        raise ValueError("claim parent lineage is invalid")
    return dict(claim)


def _result_payload(evidence: EvidenceResult) -> dict[str, object]:
    return {
        "verdict": evidence.verdict,
        "protocol_conformance": evidence.protocol_conformance,
        "audit_grade": evidence.audit_grade,
        "scientific_outcome": evidence.scientific_outcome,
        "promotion_eligible": evidence.promotion_eligible,
        "evidence_refs": list(evidence.evidence_refs),
        "access_event_ids": list(evidence.access_event_ids),
        "taint_refs": list(evidence.taint_refs),
        "invalidation_codes": list(evidence.invalidation_codes),
    }


def _bound_file(root: Path, reference: Mapping[str, object]) -> bytes:
    ref = reference["evidence_ref"]
    digest = reference["evidence_sha256"]
    if (
        not isinstance(ref, str)
        or not ref.startswith("research_state/control_plane/p4/")
        or "\\" in ref
        or ":" in ref
        or any(part in {"", ".", ".."} for part in ref.split("/"))
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
    ):
        raise LearningCommitAuthorizationError("P4 evidence binding is invalid")
    path = (root / ref).resolve()
    try:
        path.relative_to(root)
        raw = path.read_bytes()
    except (OSError, ValueError) as error:
        raise LearningCommitAuthorizationError(
            "P4 evidence binding is unavailable"
        ) from error
    if hashlib.sha256(raw).hexdigest() != digest:
        raise LearningCommitAuthorizationError("P4 evidence binding changed")
    return raw


def _json_object(raw: bytes, name: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{name} must be canonical JSON") from error
    if not isinstance(value, dict) or _canonical_bytes(value) != raw:
        raise ValueError(f"{name} must be a canonical JSON object")
    return value


def _authority_order_key(completed_at: object, ticket_id: str) -> str:
    if not isinstance(completed_at, str):
        raise LearningCommitAuthorizationError(
            "Authority completion timestamp is invalid"
        )
    try:
        instant = datetime.fromisoformat(
            completed_at.replace("Z", "+00:00")
        )
    except ValueError as error:
        raise LearningCommitAuthorizationError(
            "Authority completion timestamp is invalid"
        ) from error
    if instant.tzinfo is None:
        raise LearningCommitAuthorizationError(
            "Authority completion timestamp is invalid"
        )
    canonical = instant.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")
    return f"{canonical}|{ticket_id}"


def _authority_projection(
    root: Path,
    task_report: Mapping[str, object],
):
    try:
        frozen_report = _json_object(
            _canonical_bytes(dict(task_report)),
            "terminal P4 TaskReport",
        )
    except (TypeError, ValueError) as error:
        raise LearningCommitAuthorizationError(
            "terminal P4 TaskReport is not a stable canonical mapping"
        ) from error
    try:
        binding = AuthorityReader().verify_task_report_binding(frozen_report)
    except (TaskReportAuthorityError, OSError, TypeError, ValueError) as error:
        raise LearningCommitAuthorizationError(
            "terminal P4 TaskReport authority is unavailable"
        ) from error
    if (
        frozen_report.get("phase") != "P4"
        or frozen_report.get("task_id") != _LEARNING_TASK_ID
        or binding.ticket_state != "SUCCEEDED"
        or frozenset(binding.allowed_side_effects)
        != {SideEffect.WRITE_CONTROL_PLANE}
        or frozenset(frozen_report.get("allowed_files", ()))
        != _LEARNING_ALLOWED_FILES
    ):
        raise LearningCommitAuthorizationError(
            "terminal P4 TaskReport does not authorize Learning Commit"
        )
    input_refs = frozen_report.get("input_evidence_refs")
    if not isinstance(input_refs, list):
        raise LearningCommitAuthorizationError("P4 evidence set is invalid")
    refs = {
        reference.get("evidence_id"): reference
        for reference in input_refs
        if isinstance(reference, Mapping) and reference.get("status") == "VERIFIED"
    }
    if set(refs) != _LEARNING_INPUT_IDS or len(input_refs) != len(refs):
        raise LearningCommitAuthorizationError("P4 evidence set is not exact")
    if (
        binding.terminal_evidence_ref
        != refs["learning-decision"]["evidence_ref"]
    ):
        raise LearningCommitAuthorizationError(
            "terminal evidence does not name the Learning decision"
        )
    baseline_ref = frozen_report.get("baseline_ref")
    baseline_sha256 = frozen_report.get("baseline_sha256")
    if not isinstance(baseline_ref, str) or not baseline_ref.startswith(
        "research_state/control_plane/"
    ):
        raise LearningCommitAuthorizationError("repository baseline is invalid")
    baseline_path = (root / baseline_ref).resolve()
    try:
        baseline_path.relative_to(root)
        baseline_raw = baseline_path.read_bytes()
    except (OSError, ValueError) as error:
        raise LearningCommitAuthorizationError("repository baseline is unavailable") from error
    if hashlib.sha256(baseline_raw).hexdigest() != baseline_sha256:
        raise LearningCommitAuthorizationError("repository baseline changed")
    baseline = _json_object(baseline_raw, "repository baseline")
    if baseline.get("repository_root") != str(root):
        raise LearningCommitAuthorizationError("repository root binding mismatch")

    raw = {name: _bound_file(root, refs[name]) for name in sorted(refs)}
    claim = _require_claim(_json_object(raw["approved-claim"], "approved claim"))
    protocol = _json_object(raw["approved-protocol"], "approved protocol")
    artifact = _json_object(raw["runner-artifact"], "runner artifact")
    adapter = _json_object(raw["runner-adapter"], "runner adapter")
    if set(adapter) != {"schema_version", "adapter_id", "source_ref", "source_sha256", "runners"}:
        raise ValueError("runner adapter binding has an invalid field contract")
    if (
        adapter["schema_version"] != "control_plane.runner_adapter.v1"
        or adapter["adapter_id"] != "EvidenceAdapter.v1"
        or adapter["source_ref"] != "research_automation/control_plane/evidence_learning.py"
        or not isinstance(adapter["runners"], dict)
        or not adapter["runners"]
        or any(
            not isinstance(name, str)
            or not name
            or not isinstance(version, str)
            or not version
            for name, version in adapter["runners"].items()
        )
    ):
        raise ValueError("runner adapter binding is invalid")
    source_path = (root / str(adapter["source_ref"])).resolve()
    try:
        source_path.relative_to(root)
        source_digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
    except (OSError, ValueError) as error:
        raise LearningCommitAuthorizationError("runner adapter source is unavailable") from error
    if source_digest != adapter["source_sha256"]:
        raise LearningCommitAuthorizationError("runner adapter source changed")
    evidence = EvidenceAdapter(
        known_runners=adapter["runners"],
        approved_protocol=protocol,
        approved_claim=claim,
    ).evaluate(artifact)
    if evidence.verdict != "VALID" or not evidence.promotion_eligible:
        raise ValueError("Authority-bound evidence is not commit eligible")
    expected_decision = {
        "schema_version": "control_plane.evidence_decision.v1",
        "bindings": {
            name: refs[name]["evidence_sha256"]
            for name in (
                "approved-claim",
                "approved-protocol",
                "runner-adapter",
                "runner-artifact",
            )
        },
        "claim": claim,
        "evidence": _result_payload(evidence),
    }
    decision = _json_object(raw["learning-decision"], "learning decision")
    if decision != expected_decision:
        raise ValueError("learning decision differs from Authority-bound evidence")
    return binding, evidence, claim, frozen_report, artifact


def _validated_learning_packet(root: Path, raw: bytes):
    packet = _json_object(raw, "learning packet")
    report = packet.get("authority_task_report")
    if not isinstance(report, Mapping):
        raise ValueError("learning packet Authority anchor is invalid")
    try:
        authority, evidence, claim, frozen_report, _ = _authority_projection(
            root,
            report,
        )
    except (LearningCommitAuthorizationError, ValueError) as error:
        raise ValueError("learning packet Authority anchor is invalid") from error
    expected_packet = {
        "schema_version": "control_plane.learning_packet.v2",
        "authority_task_report": frozen_report,
        "claim": claim,
        "evidence_refs": list(evidence.evidence_refs),
        "access_event_refs": list(evidence.access_event_ids),
        "taint_refs": list(evidence.taint_refs),
        "audit_grade": evidence.audit_grade,
        "invalidation_codes": list(evidence.invalidation_codes),
    }
    if packet != expected_packet:
        raise ValueError("learning packet Authority anchor mismatch")
    order_key = _authority_order_key(
        frozen_report["completed_at"],
        authority.ticket_id,
    )
    return authority, order_key


def _validate_legacy_learning_packet(raw: bytes) -> None:
    packet = _json_object(raw, "legacy learning packet")
    if set(packet) != {
        "schema_version",
        "claim",
        "evidence_refs",
        "access_event_refs",
        "taint_refs",
        "audit_grade",
        "invalidation_codes",
    } or packet["schema_version"] != "control_plane.learning_packet.v1":
        raise ValueError("legacy learning packet contract is invalid")
    _require_claim(packet["claim"])
    references = packet["evidence_refs"]
    if not isinstance(references, list) or any(
        not isinstance(reference, Mapping)
        or set(reference) != {"ref", "sha256"}
        or not isinstance(reference["ref"], str)
        or not reference["ref"]
        or not isinstance(reference["sha256"], str)
        or re.fullmatch(r"[0-9a-f]{64}", reference["sha256"]) is None
        for reference in references
    ):
        raise ValueError("legacy learning packet evidence refs are invalid")
    access_refs = packet["access_event_refs"]
    if not isinstance(access_refs, list) or any(
        not isinstance(reference, str) or not reference
        for reference in access_refs
    ):
        raise ValueError("legacy learning packet access refs are invalid")
    if (
        packet["audit_grade"] != "PASS"
        or packet["taint_refs"] != []
        or packet["invalidation_codes"] != []
    ):
        raise ValueError("legacy learning packet is not clean")


class LearningCommitService:
    """Project one Authority-valid terminal P4 decision into Learning."""

    def __init__(self, *, repository_root: str | Path):
        self._root = Path(repository_root).resolve()

    def commit(
        self,
        task_report: Mapping[str, object],
        *,
        expected_artifact: Mapping[str, object] | None = None,
        expected_evidence: EvidenceResult | None = None,
    ) -> str:
        binding, evidence, claim, frozen_report, artifact = _authority_projection(
            self._root,
            task_report,
        )
        if expected_artifact is not None and dict(expected_artifact) != artifact:
            raise ValueError("runner artifact differs from Authority-bound artifact")
        if expected_evidence is not None and expected_evidence != evidence:
            raise ValueError("runner evidence differs from Authority-bound evidence")
        for reference in evidence.evidence_refs:
            if not isinstance(reference, Mapping) or set(reference) != {"ref", "sha256"}:
                raise ValueError("evidence refs must use the compact closed schema")
            ref = reference["ref"]
            digest = reference["sha256"]
            if (
                not isinstance(ref, str)
                or not ref
                or ref.startswith("/")
                or "\\" in ref
                or ":" in ref
                or any(part in {"", ".", ".."} for part in ref.split("/"))
                or not isinstance(digest, str)
                or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            ):
                raise ValueError("evidence ref is invalid")
        packet = {
            "schema_version": "control_plane.learning_packet.v2",
            "authority_task_report": frozen_report,
            "claim": claim,
            "evidence_refs": list(evidence.evidence_refs),
            "access_event_refs": list(evidence.access_event_ids),
            "taint_refs": list(evidence.taint_refs),
            "audit_grade": evidence.audit_grade,
            "invalidation_codes": list(evidence.invalidation_codes),
        }
        raw = _canonical_bytes(packet)
        digest = hashlib.sha256(raw).hexdigest()
        authority_order_key = _authority_order_key(
            frozen_report["completed_at"],
            binding.ticket_id,
        )
        directory = self._root / "research_state/control_plane/learning_packets"
        journal = directory.parent / "learning_commit.sqlite3"
        if journal.exists() and journal.stat().st_size:
            self.rebuild_ledger()
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{digest}.json"
        _durable_create_only(path, raw)
        connection = sqlite3.connect(journal, timeout=30)
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS learning_commit_events ("
                "sequence INTEGER PRIMARY KEY, packet_hash TEXT NOT NULL UNIQUE, "
                "actor_id TEXT NOT NULL, previous_event_sha256 TEXT NOT NULL, "
                "event_sha256 TEXT NOT NULL UNIQUE)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS learning_commit_head ("
                "singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1), "
                "head_sequence INTEGER NOT NULL, head_event_sha256 TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT OR IGNORE INTO learning_commit_head"
                "(singleton_id, head_sequence, head_event_sha256) VALUES (1, 0, ?)",
                (_ZERO_SHA256,),
            )
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT sequence FROM learning_commit_events WHERE packet_hash = ?",
                (digest,),
            ).fetchone()
            if existing is None:
                head_sequence, previous_sha256 = connection.execute(
                    "SELECT head_sequence, head_event_sha256 FROM learning_commit_head WHERE singleton_id = 1"
                ).fetchone()
                next_sequence = int(head_sequence) + 1
                previous_order = None
                trusted_seen = False
                for committed_row in connection.execute(
                    "SELECT packet_hash FROM learning_commit_events ORDER BY sequence"
                ):
                    committed_path = directory / f"{committed_row[0]}.json"
                    committed_raw = committed_path.read_bytes()
                    committed_packet = _json_object(
                        committed_raw,
                        "learning packet",
                    )
                    if (
                        committed_packet.get("schema_version")
                        == "control_plane.learning_packet.v1"
                    ):
                        if trusted_seen:
                            raise ValueError(
                                "legacy Learning event follows trusted Learning"
                            )
                        _validate_legacy_learning_packet(committed_raw)
                        continue
                    trusted_seen = True
                    committed_authority, committed_order = (
                        _validated_learning_packet(
                            self._root,
                            committed_raw,
                        )
                    )
                    if committed_authority.ticket_id == binding.ticket_id:
                        raise ValueError(
                            "Authority ticket already projected different Learning bytes"
                        )
                    previous_order = committed_order
                if (
                    previous_order is not None
                    and previous_order >= authority_order_key
                ):
                    raise ValueError(
                        "Learning Commit Authority order is not append-only"
                    )
                event_payload = {
                    "schema_version": "control_plane.learning_commit_event.v2",
                    "sequence": next_sequence,
                    "packet_hash": digest,
                    "ticket_id": binding.ticket_id,
                    "report_payload_sha256": binding.report_payload_sha256,
                    "authority_order_key": authority_order_key,
                    "actor_id": binding.actor_id,
                    "previous_event_sha256": str(previous_sha256),
                }
                event_sha256 = _event_sha256(event_payload)
                connection.execute(
                    "INSERT INTO learning_commit_events"
                    "(sequence, packet_hash, actor_id, previous_event_sha256, event_sha256) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        next_sequence,
                        digest,
                        event_payload["actor_id"],
                        previous_sha256,
                        event_sha256,
                    ),
                )
                connection.execute(
                    "UPDATE learning_commit_head SET head_sequence = ?, head_event_sha256 = ? "
                    "WHERE singleton_id = 1 AND head_sequence = ? AND head_event_sha256 = ?",
                    (next_sequence, event_sha256, head_sequence, previous_sha256),
                )
            connection.commit()
        finally:
            connection.close()
        return digest

    def rebuild_ledger(self) -> dict[str, object]:
        directory = self._root / "research_state/control_plane/learning_packets"
        journal = directory.parent / "learning_commit.sqlite3"
        hashes: list[str] = []
        legacy_hashes: list[str] = []
        sequences: list[int] = []
        authority_order_keys: list[str] = []
        if journal.exists():
            connection = sqlite3.connect(f"file:{journal.as_posix()}?mode=ro", uri=True)
            try:
                rows = connection.execute(
                    "SELECT sequence, packet_hash, actor_id, "
                    "previous_event_sha256, event_sha256 "
                    "FROM learning_commit_events ORDER BY sequence"
                ).fetchall()
                head = connection.execute(
                    "SELECT head_sequence, head_event_sha256 FROM learning_commit_head WHERE singleton_id = 1"
                ).fetchone()
            except sqlite3.Error as error:
                raise ValueError("learning commit journal is invalid") from error
            finally:
                connection.close()
            previous_sha256 = _ZERO_SHA256
            trusted_seen = False
            for (
                sequence,
                digest,
                actor_id,
                recorded_previous,
                recorded_event,
            ) in rows:
                path = directory / f"{digest}.json"
                if not path.is_file():
                    raise ValueError("committed learning packet is missing or tampered")
                raw = path.read_bytes()
                if hashlib.sha256(raw).hexdigest() != digest:
                    raise ValueError("committed learning packet is missing or tampered")
                packet = _json_object(raw, "learning packet")
                if (
                    packet.get("schema_version")
                    == "control_plane.learning_packet.v1"
                ):
                    if trusted_seen:
                        raise ValueError(
                            "legacy Learning event follows trusted Learning"
                        )
                    _validate_legacy_learning_packet(raw)
                    event_payload = {
                        "schema_version": "control_plane.learning_commit_event.v1",
                        "sequence": int(sequence),
                        "packet_hash": str(digest),
                        "actor_id": str(actor_id),
                        "previous_event_sha256": str(recorded_previous),
                    }
                    expected_event = _legacy_event_sha256(event_payload)
                    legacy_hashes.append(str(digest))
                else:
                    trusted_seen = True
                    authority, authority_order_key = _validated_learning_packet(
                        self._root,
                        raw,
                    )
                    if actor_id != authority.actor_id:
                        raise ValueError("learning commit Authority anchor mismatch")
                    event_payload = {
                        "schema_version": "control_plane.learning_commit_event.v2",
                        "sequence": int(sequence),
                        "packet_hash": str(digest),
                        "ticket_id": authority.ticket_id,
                        "report_payload_sha256": authority.report_payload_sha256,
                        "authority_order_key": str(authority_order_key),
                        "actor_id": str(actor_id),
                        "previous_event_sha256": str(recorded_previous),
                    }
                    expected_event = _event_sha256(event_payload)
                    hashes.append(str(digest))
                    authority_order_keys.append(str(authority_order_key))
                if recorded_previous != previous_sha256 or recorded_event != expected_event:
                    raise ValueError("learning commit hash chain is invalid")
                sequences.append(int(sequence))
                previous_sha256 = str(recorded_event)
            if sequences != list(range(1, len(sequences) + 1)):
                raise ValueError("learning commit sequence is not contiguous")
            if authority_order_keys != sorted(authority_order_keys) or len(
                authority_order_keys
            ) != len(set(authority_order_keys)):
                raise ValueError("learning commit Authority order is invalid")
            if head != (len(sequences), previous_sha256):
                raise ValueError("learning commit head does not match the event chain")
        orphan_hashes: list[str] = []
        if directory.exists():
            committed = set(hashes) | set(legacy_hashes)
            for packet_path in sorted(directory.glob("*.json")):
                digest = packet_path.stem
                if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                    raise ValueError("learning packet filename is invalid")
                if hashlib.sha256(packet_path.read_bytes()).hexdigest() != digest:
                    raise ValueError("learning packet is tampered")
                if digest not in committed:
                    orphan_hashes.append(digest)
        return {
            "schema_version": "control_plane.learning_ledger.v2",
            "event_count": len(sequences),
            "packet_count": len(hashes),
            "packet_hashes": hashes,
            "legacy_unaudited_packet_count": len(legacy_hashes),
            "legacy_unaudited_packet_hashes": legacy_hashes,
            "sequences": sequences,
            "orphan_packet_hashes": orphan_hashes,
        }


__all__ = [
    "EvidenceAdapter",
    "EvidenceResult",
    "LearningCommitAuthorizationError",
    "LearningCommitService",
]
