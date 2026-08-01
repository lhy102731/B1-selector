"""Fail-closed P4 runner-artifact finalization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol, runtime_checkable

from .evidence_learning import (
    EvidenceAdapter,
    EvidenceResult,
    LearningCommitAuthorizationError,
)


class RunAuthorizationError(RuntimeError):
    """Raised before an evidence-driven durable commit without live authority."""


@runtime_checkable
class LearningCommitSink(Protocol):
    """Minimal Learning projection boundary consumed by P4 finalization."""

    def commit(
        self,
        task_report: Mapping[str, object],
        *,
        expected_artifact: Mapping[str, object] | None = None,
        expected_evidence: EvidenceResult | None = None,
    ) -> str: ...


@dataclass(frozen=True, slots=True)
class RunFinalization:
    evidence: EvidenceResult
    packet_hash: str | None


class P4RunController:
    """Independently evaluate one artifact before any Learning Commit."""

    def __init__(
        self,
        *,
        evidence_adapter: EvidenceAdapter,
        learning_commit_service: LearningCommitSink,
    ) -> None:
        if not isinstance(evidence_adapter, EvidenceAdapter):
            raise TypeError("evidence_adapter must be an EvidenceAdapter")
        if not isinstance(learning_commit_service, LearningCommitSink):
            raise TypeError("learning_commit_service must be a LearningCommitSink")
        self._evidence_adapter = evidence_adapter
        self._learning_commit_service = learning_commit_service

    def finalize(
        self,
        *,
        artifact: Mapping[str, object],
        authority_task_report: Mapping[str, object] | None = None,
    ) -> RunFinalization:
        evidence = self._evidence_adapter.evaluate(artifact)
        if evidence.verdict != "VALID":
            return RunFinalization(evidence=evidence, packet_hash=None)
        try:
            packet_hash = self._learning_commit_service.commit(
                authority_task_report,
                expected_artifact=artifact,
                expected_evidence=evidence,
            )
        except (LearningCommitAuthorizationError, TypeError) as error:
            raise RunAuthorizationError("P4 commit authority is unavailable") from error
        return RunFinalization(evidence=evidence, packet_hash=packet_hash)


__all__ = [
    "LearningCommitSink",
    "P4RunController",
    "RunAuthorizationError",
    "RunFinalization",
]
