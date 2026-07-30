"""Deterministic evidence semantics and learning-commit primitives.

This module is deliberately control-plane-only: it accepts already materialized
runner metadata and never opens market data, KBase content, or raw logs.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Mapping


_ZERO_SHA256 = "0" * 64
_EVENT_DOMAIN = b"control_plane.learning_commit_event.v1\0"


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
        if path.read_bytes() != raw:
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

    def __init__(
        self,
        *,
        known_runners: Mapping[str, str] | tuple[str, ...] = (),
        approved_protocol: Mapping[str, object] | None = None,
        approved_claim: Mapping[str, object] | None = None,
    ) -> None:
        self._known_runners = (
            dict(known_runners)
            if isinstance(known_runners, Mapping)
            else {runner: "" for runner in known_runners}
        )
        self._approved_protocol = (
            None if approved_protocol is None else dict(approved_protocol)
        )
        self._approved_claim = None if approved_claim is None else dict(approved_claim)

    def evaluate(self, artifact: Mapping[str, object]) -> EvidenceResult:
        if not isinstance(artifact, Mapping):
            raise TypeError("runner artifact must be a mapping")
        required = {"schema_version", "runner", "status", "claim", "protocol_conformance", "artifact_refs", "access_event_ids", "taint_refs"}
        missing = sorted(required - set(artifact))
        if missing:
            return EvidenceResult("EVIDENCE_INVALID", "UNKNOWN", "INVALID", "UNKNOWN", False, (), (), (), tuple(f"MISSING:{name}" for name in missing))
        if artifact["schema_version"] != "runner.artifact.v1":
            return EvidenceResult("EVIDENCE_INVALID", "UNKNOWN", "INVALID", "UNKNOWN", False, (), (), (), ("UNKNOWN_RUNNER_SCHEMA",))
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
            or re.fullmatch(r"[0-9a-f]{64}", reference["sha256"] or "") is None
            for reference in refs
        ):
            return EvidenceResult("EVIDENCE_INVALID", "CONFORMING", "INVALID", "UNKNOWN", False, (), tuple(artifact["access_event_ids"]), (), ("INVALID_ARTIFACT_REFERENCE",))
        access_event_ids = tuple(artifact["access_event_ids"])
        if self._approved_claim is not None and isinstance(claim, Mapping):
            if dict(claim) == self._approved_claim and access_event_ids:
                return EvidenceResult("VALID", "CONFORMING", "PASS", str(claim.get("kind", "CLAIM")), True, tuple(dict(reference) for reference in refs), access_event_ids, (), ())
            return EvidenceResult("MATERIAL_UNAPPROVED", "CONFORMING", "PASS", "CLAIM_PRESENT", False, tuple(dict(reference) for reference in refs), access_event_ids, (), ("MATERIAL_UNAPPROVED",))
        return EvidenceResult("RESEARCH_ONLY", "CONFORMING", "PASS", "CLAIM_PRESENT", False, tuple(artifact["artifact_refs"]), tuple(artifact["access_event_ids"]), tuple(artifact["taint_refs"]), ("PROMOTION_REQUIRES_SEMANTIC_REVIEW",))


class LearningCommitService:
    """Create-only learning packet writer for already-valid evidence."""

    def __init__(self, *, repository_root: str | Path):
        self._root = Path(repository_root).resolve()

    def commit(self, evidence: EvidenceResult, claim: dict, actor: object) -> str:
        if (
            evidence.verdict != "VALID"
            or not evidence.promotion_eligible
            or evidence.protocol_conformance != "CONFORMING"
            or evidence.audit_grade != "PASS"
            or evidence.taint_refs
            or evidence.invalidation_codes
        ):
            raise ValueError("only valid promotion-eligible evidence can commit")
        if not isinstance(claim, dict) or not claim:
            raise ValueError("claim must be a non-empty mapping")
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
        allowed_claim_fields = {
            "kind",
            "summary",
            "scope",
            "parent_lineage",
            "reopen_predicate",
            "future_usage_guidance",
        }
        if not set(claim).issubset(allowed_claim_fields):
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
        packet = {
            "schema_version": "control_plane.learning_packet.v1",
            "claim": claim,
            "evidence_refs": list(evidence.evidence_refs),
            "access_event_refs": list(evidence.access_event_ids),
            "taint_refs": list(evidence.taint_refs),
            "audit_grade": evidence.audit_grade,
            "invalidation_codes": list(evidence.invalidation_codes),
        }
        raw = _canonical_bytes(packet)
        digest = hashlib.sha256(raw).hexdigest()
        directory = self._root / "research_state/control_plane/learning_packets"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{digest}.json"
        _durable_create_only(path, raw)
        journal = directory.parent / "learning_commit.sqlite3"
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
                event_payload = {
                    "schema_version": "control_plane.learning_commit_event.v1",
                    "sequence": next_sequence,
                    "packet_hash": digest,
                    "actor_id": getattr(actor, "actor_id", "<unknown>"),
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
        sequences: list[int] = []
        if journal.exists():
            connection = sqlite3.connect(f"file:{journal.as_posix()}?mode=ro", uri=True)
            try:
                rows = connection.execute(
                    "SELECT sequence, packet_hash, actor_id, previous_event_sha256, event_sha256 "
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
            for sequence, digest, actor_id, recorded_previous, recorded_event in rows:
                path = directory / f"{digest}.json"
                if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                    raise ValueError("committed learning packet is missing or tampered")
                event_payload = {
                    "schema_version": "control_plane.learning_commit_event.v1",
                    "sequence": int(sequence),
                    "packet_hash": str(digest),
                    "actor_id": str(actor_id),
                    "previous_event_sha256": str(recorded_previous),
                }
                expected_event = _event_sha256(event_payload)
                if recorded_previous != previous_sha256 or recorded_event != expected_event:
                    raise ValueError("learning commit hash chain is invalid")
                sequences.append(int(sequence))
                hashes.append(str(digest))
                previous_sha256 = str(recorded_event)
            if sequences != list(range(1, len(sequences) + 1)):
                raise ValueError("learning commit sequence is not contiguous")
            if head != (len(sequences), previous_sha256):
                raise ValueError("learning commit head does not match the event chain")
        orphan_hashes: list[str] = []
        if directory.exists():
            committed = set(hashes)
            for packet_path in sorted(directory.glob("*.json")):
                digest = packet_path.stem
                if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                    raise ValueError("learning packet filename is invalid")
                if hashlib.sha256(packet_path.read_bytes()).hexdigest() != digest:
                    raise ValueError("learning packet is tampered")
                if digest not in committed:
                    orphan_hashes.append(digest)
        return {
            "schema_version": "control_plane.learning_ledger.v1",
            "event_count": len(hashes),
            "packet_count": len(hashes),
            "packet_hashes": hashes,
            "sequences": sequences,
            "orphan_packet_hashes": orphan_hashes,
        }


__all__ = ["EvidenceAdapter", "EvidenceResult", "LearningCommitService"]
