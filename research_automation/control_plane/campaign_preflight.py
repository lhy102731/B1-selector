"""Shared read-only protocol and scoped-Learning Campaign preflight."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from research_automation.foundations.protocols import (
    ExecutionSpec,
    MaterialProtocolChangeError,
    require_protocol_conformant,
)

from .memory import (
    ClaimScope,
    LearningGate,
    learning_execution_identity,
    learning_semantic_identity,
)


def run_campaign_preflight(
    *,
    execution_spec: ExecutionSpec,
    proposal: Mapping[str, object],
    committed_claims: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Report whether the same formal inputs would clear Campaign preflight."""
    if not isinstance(proposal, Mapping):
        raise ValueError("proposal must be a mapping")
    hypothesis = proposal.get("hypothesis")
    if not isinstance(hypothesis, str) or not hypothesis.strip():
        raise ValueError("proposal.hypothesis must be a non-empty string")
    normalized_hypothesis = " ".join(hypothesis.split()).casefold()
    normalized_scope = ClaimScope.from_mapping(
        proposal.get("scope")
    ).to_mapping()

    rejection_codes: list[str] = []
    try:
        require_protocol_conformant(execution_spec)
    except MaterialProtocolChangeError:
        rejection_codes.append("MATERIAL_PROTOCOL_UNAPPROVED")

    proposal_identity = {
        "execution_identity": learning_execution_identity(
            normalized_hypothesis,
            normalized_scope,
        ),
        "semantic_identity": learning_semantic_identity(normalized_hypothesis),
        "scope": normalized_scope,
    }
    learning_verdict = LearningGate().classify(
        proposal_identity,
        committed_claims,
        universal_required_scope=normalized_scope,
    )
    enforcement = learning_verdict["enforcement"]
    if enforcement == "HARD_BLOCK":
        rejection_codes.append("LEARNING_HARD_BLOCK")
    elif enforcement == "SCOPED_BLOCK":
        rejection_codes.append("LEARNING_SCOPED_BLOCK")
    elif enforcement != "ALLOW":
        raise ValueError("Learning preflight returned an unknown enforcement")

    return {
        "schema_version": "control_plane.campaign_preflight.v1",
        "verdict": "WOULD_REJECT" if rejection_codes else "WOULD_ACCEPT",
        "execution_spec_id": execution_spec.execution_spec_id,
        "protocol_conformance": execution_spec.conformance,
        "proposal_identity": proposal_identity,
        "learning_verdict": learning_verdict,
        "rejection_codes": rejection_codes,
    }


__all__ = ["run_campaign_preflight"]
