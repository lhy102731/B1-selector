"""Deterministic evidence semantics and learning-commit primitives.

This module is deliberately control-plane-only: it accepts already materialized
runner metadata and never opens market data, KBase content, or raw logs.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Mapping


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

    def evaluate(self, artifact: Mapping[str, object]) -> EvidenceResult:
        if not isinstance(artifact, Mapping):
            raise TypeError("runner artifact must be a mapping")
        required = {"schema_version", "runner", "status", "claim", "protocol_conformance", "artifact_refs", "access_event_ids", "taint_refs"}
        missing = sorted(required - set(artifact))
        if missing:
            return EvidenceResult("EVIDENCE_INVALID", "UNKNOWN", "INVALID", "UNKNOWN", False, (), (), (), tuple(f"MISSING:{name}" for name in missing))
        if artifact["schema_version"] != "runner.artifact.v1":
            return EvidenceResult("EVIDENCE_INVALID", "UNKNOWN", "INVALID", "UNKNOWN", False, (), (), (), ("UNKNOWN_RUNNER_SCHEMA",))
        if artifact["status"] != "COMPLETED":
            return EvidenceResult("EVIDENCE_INVALID", "NONCONFORMING", "INVALID", "UNKNOWN", False, (), (), (), ("RUNNER_NOT_COMPLETED",))
        if artifact["protocol_conformance"] != "CONFORMING":
            return EvidenceResult("EVIDENCE_INVALID", "NONCONFORMING", "INVALID", "UNKNOWN", False, (), (), (), ("PROTOCOL_NONCONFORMING",))
        claim = artifact["claim"]
        if claim is None:
            return EvidenceResult("NO_MATERIAL_FINDING", "CONFORMING", "PASS", "NO_MATERIAL_FINDING", False, (), tuple(artifact["access_event_ids"]), tuple(artifact["taint_refs"]), ())
        return EvidenceResult("RESEARCH_ONLY", "CONFORMING", "PASS", "CLAIM_PRESENT", False, tuple(artifact["artifact_refs"]), tuple(artifact["access_event_ids"]), tuple(artifact["taint_refs"]), ("PROMOTION_REQUIRES_SEMANTIC_REVIEW",))


class LearningCommitService:
    """Create-only learning packet writer for already-valid evidence."""

    def __init__(self, *, repository_root: str | Path):
        self._root = Path(repository_root).resolve()

    def commit(self, evidence: EvidenceResult, claim: dict, actor: object) -> str:
        if evidence.verdict != "VALID" or not evidence.promotion_eligible:
            raise ValueError("only valid promotion-eligible evidence can commit")
        if not isinstance(claim, dict) or not claim:
            raise ValueError("claim must be a non-empty mapping")
        packet = {
            "schema_version": "control_plane.learning_packet.v1",
            "claim": claim,
            "evidence_refs": list(evidence.evidence_refs),
            "access_event_refs": list(evidence.access_event_ids),
            "taint_refs": list(evidence.taint_refs),
            "audit_grade": evidence.audit_grade,
            "invalidation_codes": list(evidence.invalidation_codes),
        }
        raw = json.dumps(packet, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(raw).hexdigest()
        directory = self._root / "research_state/control_plane/learning_packets"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{digest}.json"
        if path.exists() and path.read_bytes() != raw:
            raise ValueError("content-addressed packet conflict")
        if not path.exists():
            path.write_bytes(raw)
        return digest


__all__ = ["EvidenceAdapter", "EvidenceResult", "LearningCommitService"]
