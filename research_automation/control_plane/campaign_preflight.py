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


LEGACY_SURFACE_WITHOUT_CAMPAIGN_CONTEXT = "LEGACY_SURFACE_WITHOUT_CAMPAIGN_CONTEXT"
BLOCKED_PENDING_C1_ADAPTER = "BLOCKED_PENDING_C1_ADAPTER"

# Fixed P6R3 command disposition (Step 5.5 of the corrective recovery plan).
# Read-only commands are always allowed; the campaign command requires a
# programmatic control-plane context; execute-handoff --dry-run stays read-only;
# every other legacy network/research command is blocked pending the C1 adapter.
READ_ONLY_COMMANDS = frozenset({"list", "status", "audit", "doctor", "export"})
PROGRAMMATIC_CONTEXT_ONLY_COMMANDS = frozenset({"campaign"})
BLOCKED_PENDING_C1_ADAPTER_COMMANDS = frozenset(
    {
        "brainstorm",
        "discover",
        "resume-discover",
        "full-cycle",
        "review",
        "chat",
        "roundtable",
        "interactive",
        "repair-handoff-runner",
    }
)
DRY_RUN_READ_ONLY_EXCEPTION_COMMANDS = frozenset({"execute-handoff"})


def command_disposition(command: str) -> dict[str, object]:
    """Return the fixed P6R3 disposition for one legacy CLI command."""
    if command in READ_ONLY_COMMANDS:
        disposition = "READ_ONLY_ALLOWED"
    elif command in PROGRAMMATIC_CONTEXT_ONLY_COMMANDS:
        disposition = "PROGRAMMATIC_CONTEXT_ONLY"
    elif command in BLOCKED_PENDING_C1_ADAPTER_COMMANDS:
        disposition = "BLOCKED_PENDING_C1_ADAPTER"
    elif command in DRY_RUN_READ_ONLY_EXCEPTION_COMMANDS:
        disposition = "DRY_RUN_READ_ONLY_EXCEPTION"
    else:
        disposition = "BLOCKED_PENDING_C1_ADAPTER"
    return {
        "schema_version": "control_plane.command_disposition.v1",
        "command": command,
        "disposition": disposition,
    }


class CampaignBoundaryError(RuntimeError):
    """Raised when a legacy surface crosses the fail-closed Campaign boundary."""

    def __init__(
        self,
        *,
        surface: str,
        rejection_codes: Sequence[str],
    ) -> None:
        message = (
            "campaign boundary rejected for "
            + surface
            + ": "
            + (",".join(rejection_codes) or "NO_REJECTION_CODES")
        )
        super().__init__(message)
        self.surface = surface
        self.rejection_codes = tuple(rejection_codes)


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


def require_campaign_boundary(
    *,
    surface: str,
    execution_spec: ExecutionSpec | None = None,
    proposal: Mapping[str, object] | None = None,
    committed_claims: Sequence[Mapping[str, object]] = (),
) -> dict[str, object]:
    """Fail closed unless a formal Campaign preflight would accept the surface.

    Legacy CLI and automation surfaces do not carry a formal Campaign
    execution context; until a later control-plane slice attaches one they
    are rejected with LEGACY_SURFACE_WITHOUT_CAMPAIGN_CONTEXT. Surfaces that
    do provide formal inputs are evaluated by run_campaign_preflight and
    must return a WOULD_ACCEPT verdict.
    """
    if execution_spec is None or proposal is None:
        result: dict[str, object] = {
            "schema_version": "control_plane.campaign_boundary.v1",
            "surface": surface,
            "verdict": "WOULD_REJECT",
            "rejection_codes": [
                LEGACY_SURFACE_WITHOUT_CAMPAIGN_CONTEXT,
            ],
        }
    else:
        result = run_campaign_preflight(
            execution_spec=execution_spec,
            proposal=proposal,
            committed_claims=committed_claims,
        )
    if result.get("verdict") != "WOULD_ACCEPT":
        rejection_codes = result.get("rejection_codes")
        if not isinstance(rejection_codes, list):
            rejection_codes = []
        raise CampaignBoundaryError(
            surface=surface,
            rejection_codes=rejection_codes,
        )
    return result


__all__ = [
    "BLOCKED_PENDING_C1_ADAPTER",
    "BLOCKED_PENDING_C1_ADAPTER_COMMANDS",
    "CampaignBoundaryError",
    "DRY_RUN_READ_ONLY_EXCEPTION_COMMANDS",
    "LEGACY_SURFACE_WITHOUT_CAMPAIGN_CONTEXT",
    "PROGRAMMATIC_CONTEXT_ONLY_COMMANDS",
    "READ_ONLY_COMMANDS",
    "command_disposition",
    "require_campaign_boundary",
    "run_campaign_preflight",
]
