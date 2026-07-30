"""Fail-closed P4 runner-artifact finalization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .evidence_learning import (
    EvidenceAdapter,
    EvidenceResult,
    LearningCommitAuthorizationError,
    LearningCommitService,
)


class RunAuthorizationError(RuntimeError):
    """Raised before an evidence-driven durable commit without live authority."""


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
        learning_commit_service: LearningCommitService,
    ) -> None:
        if not isinstance(evidence_adapter, EvidenceAdapter):
            raise TypeError("evidence_adapter must be an EvidenceAdapter")
        if not isinstance(learning_commit_service, LearningCommitService):
            raise TypeError(
                "learning_commit_service must be a LearningCommitService"
            )
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


__all__ = ["P4RunController", "RunAuthorizationError", "RunFinalization"]
