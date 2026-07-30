"""Fail-closed P4 runner-artifact finalization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .evidence_learning import EvidenceAdapter, EvidenceResult, LearningCommitService
from .contracts import Phase, SideEffect
from .stores import AuthorityReader, TaskExecutionLease, TaskTicketError


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
        claim: dict[str, object],
        actor: object,
        authority_lease: TaskExecutionLease | None = None,
    ) -> RunFinalization:
        evidence = self._evidence_adapter.evaluate(artifact)
        if evidence.verdict != "VALID":
            return RunFinalization(evidence=evidence, packet_hash=None)
        if not isinstance(authority_lease, TaskExecutionLease):
            raise RunAuthorizationError("live P4 authority lease is required")
        try:
            binding = AuthorityReader().execution_lease_binding(authority_lease)
        except (TaskTicketError, OSError, ValueError) as error:
            raise RunAuthorizationError("P4 commit authority is unavailable") from error
        if (
            binding.phase is not Phase.P4
            or SideEffect.WRITE_CONTROL_PLANE not in binding.allowed_side_effects
            or actor != binding.actor
        ):
            raise RunAuthorizationError("P4 commit authority binding is invalid")
        packet_hash = self._learning_commit_service.commit(
            evidence,
            claim,
            binding.actor,
        )
        return RunFinalization(evidence=evidence, packet_hash=packet_hash)


__all__ = ["P4RunController", "RunAuthorizationError", "RunFinalization"]
