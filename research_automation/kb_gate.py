"""Knowledge-base hard gate for research_automation/.

This module wraps ag2_research.knowledge_base.proposal_validator so that
research_automation modules (autonomous_runner.py / patch_executor.py /
proposer.py) can call a single function to enforce KB rules without
importing ag2_research themselves.

Usage in autonomous_runner._is_supported_code_change (or equivalent):

    from research_automation.kb_gate import gate_proposal_kb

    verdict = gate_proposal_kb(subject="b1_v3", proposal=task.proposal)
    if verdict["verdict"] == "reject":
        log_rejection(verdict)
        continue   # skip this proposal entirely
    if verdict["verdict"] == "needs_evidence":
        attach_evidence_request(task, verdict)
        # let it through to a follow-up round, but tagged

Usage in patch_executor (last-mile gate before file modification):

    verdict = gate_proposal_kb("b1_v3", proposal_dict)
    if verdict["verdict"] != "allow":
        raise PatchRejected(verdict)

This file is intentionally tiny — all logic lives in
ag2_research/knowledge_base/proposal_validator.py. This module exists so
research_automation/ doesn't have a hard dependency on ag2_research/ at
import time; we lazy-import.
"""
from __future__ import annotations

from typing import Any


def _unavailable_verdict(
    subject: str,
    warning: str,
    reason: str,
    *,
    proposal_only: bool,
) -> dict[str, Any]:
    return {
        "verdict": "needs_evidence" if proposal_only else "reject",
        "violations": [] if proposal_only else [warning],
        "warnings": [warning],
        "needs_evidence": [warning] if proposal_only else [],
        "reasons": [reason],
        "kb_version": None,
        "subject": subject,
    }


def gate_proposal_kb(
    subject: str,
    proposal: dict[str, Any],
    *,
    proposal_only: bool = False,
) -> dict[str, Any]:
    """Validate `proposal` against the KB for `subject`.

    Returns the same dict shape as
    ag2_research.knowledge_base.validate_proposal:

        {
          "verdict": "allow" | "reject" | "needs_evidence",
          "violations": [...],
          "warnings": [...],
          "needs_evidence": [...],
          "reasons": [...],
          "kb_version": "...",
          "subject": "...",
        }

    Automatic execution is fail-closed when validation is unavailable.
    Explicit proposal-only callers receive ``needs_evidence`` so they may
    archive a warning without executing or committing the proposal.
    """
    try:
        from ag2_research.knowledge_base import validate_proposal
    except Exception as e:
        return _unavailable_verdict(
            subject,
            "KB_INTEGRATION_DISABLED",
            f"ag2_research.knowledge_base unavailable: {e}",
            proposal_only=proposal_only,
        )

    try:
        return validate_proposal(subject, proposal)
    except FileNotFoundError as e:
        return _unavailable_verdict(
            subject,
            "KB_SUBJECT_NOT_REGISTERED",
            f"No KB for subject '{subject}': {e}",
            proposal_only=proposal_only,
        )
    except Exception as e:
        return _unavailable_verdict(
            subject,
            "KB_VALIDATION_ERROR",
            f"KB validation raised: {e}",
            proposal_only=proposal_only,
        )
