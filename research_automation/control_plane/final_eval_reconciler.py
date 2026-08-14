"""Bounded final evaluation reconciler (P8 CR-009 Gate D).

The reconciler observes durable bindings and closes only those that are
recoverable (RESULT_STAGED or CLOSED with a fixed result claim).  It never
reopens holdout bytes, never recomputes a staged result, never reissues the
original lease and never advances an intermediate state.  A binding stuck
in an intermediate state (REQUEST_FROZEN/AUTHORIZED/CONSUMED/EVALUATING)
is reported as UNRESOLVED -- the orchestrator's idempotent CAS replay is
the only path forward, and only with the original trusted inputs.

The terminal outcome is DERIVED from the committed claim document (the
worker result staged by the orchestrator), never caller-supplied: a staged
FAILED/TIMEOUT/CRASHED result must be recovered as FAILED, not flipped to
SUCCEEDED.
"""

from __future__ import annotations

import hmac
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from .git_evidence import GitBlobReader
from .stores import (
    FinalEvalBindingSnapshot,
    FinalEvalRecoveryError,
    TaskExecutionLease,
    TaskTicketError,
    _AuthorityStore,
)

RECOVERABLE_STATES = frozenset({"RESULT_STAGED", "CLOSED"})
INTERMEDIATE_STATES = frozenset(
    {"REQUEST_FROZEN", "AUTHORIZED", "CONSUMED", "EVALUATING"}
)


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    recovered: tuple[str, ...]
    unresolved: tuple[str, ...]
    skipped: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "recovered": sorted(self.recovered),
            "unresolved": sorted(self.unresolved),
            "skipped": sorted(self.skipped),
        }


class FinalEvalReconcilerError(RuntimeError):
    """Base error for the final evaluation reconciler."""


def _derive_terminal_state(
    authority: _AuthorityStore,
    binding: FinalEvalBindingSnapshot,
    *,
    repository_root: str | Path,
) -> str:
    """Derive the terminal outcome from the committed staged claim.

    Reads the committed claim blob (the worker result document the
    orchestrator staged), verifies its bytes hash against the binding's
    result_claim_sha256, and maps its declared outcome to the authority
    terminal state.  A missing/mismatched claim fails closed (raised), so a
    binding is never recovered with an outcome it did not stage.
    """

    claim_ref = binding.result_claim_ref
    claim_sha256 = binding.result_claim_sha256
    if not claim_ref or not claim_sha256:
        raise FinalEvalRecoveryError("binding has no staged claim to derive from")
    try:
        claim = GitBlobReader(repository_root).read(
            claim_ref,
            max_bytes=4 * 1024 * 1024,
            evidence_name="final-eval result claim",
        )
    except Exception as error:
        raise FinalEvalRecoveryError(
            "staged claim blob is unavailable: " + claim_ref
        ) from error
    if not hmac.compare_digest(
        claim.sha256, claim_sha256
    ):
        raise FinalEvalRecoveryError(
            "staged claim hash does not match the binding"
        )
    import json as _json

    try:
        document = _json.loads(claim.raw.decode("utf-8"))
    except (UnicodeDecodeError, _json.JSONDecodeError) as error:
        raise FinalEvalRecoveryError(
            "staged claim is not valid JSON"
        ) from error
    if not isinstance(document, Mapping):
        raise FinalEvalRecoveryError("staged claim is not an object")
    outcome = str(document.get("outcome", ""))
    if outcome == "SUCCEEDED":
        return "SUCCEEDED"
    if outcome in ("FAILED", "TIMEOUT", "CRASHED"):
        return "FAILED"
    raise FinalEvalRecoveryError(
        "staged claim has no derivable outcome: " + outcome
    )


def reconcile(
    authority: _AuthorityStore,
    maintenance_lease: TaskExecutionLease,
    *,
    evidence_ref_for: Mapping[str, str] | None = None,
    repository_root: str | Path | None = None,
    crash_hook: Callable[[str], None] | None = None,
) -> ReconciliationReport:
    """Close every recoverable binding with a bounded recovery lease.

    Invariants (GPT F-03 requirements):
    - no reopen: the reconciler never opens holdout bytes (no path access);
    - no recompute: staged results are closed as-is, never recomputed;
    - no reissue: the original lease is never restored; a fresh recovery
      lease is issued per binding and only for RESULT_STAGED/CLOSED;
    - outcome integrity: the terminal state is derived from the committed
      staged claim, so a failed evaluation cannot be flipped to SUCCEEDED;
    - intermediate states stay untouched (UNRESOLVED).
    """

    if not isinstance(authority, _AuthorityStore):
        raise FinalEvalReconcilerError("an _AuthorityStore is required")
    if not isinstance(maintenance_lease, TaskExecutionLease):
        raise FinalEvalReconcilerError(
            "a maintenance TaskExecutionLease is required"
        )
    evidence_refs = dict(evidence_ref_for or {})
    root = Path(
        repository_root
        if repository_root is not None
        else Path(__file__).resolve().parents[2]
    ).resolve(strict=True)

    recovered: list[str] = []
    unresolved: list[str] = []
    skipped: list[str] = []

    bindings = authority._scan_final_eval_bindings()
    for binding in bindings:
        if binding.saga_state in ("AUTHORITY_TERMINAL",):
            skipped.append(binding.ticket_id)
            continue
        if binding.saga_state in INTERMEDIATE_STATES:
            unresolved.append(binding.ticket_id)
            continue
        if binding.saga_state not in RECOVERABLE_STATES:
            skipped.append(binding.ticket_id)
            continue
        if binding.result_claim_ref is None:
            unresolved.append(binding.ticket_id)
            continue
        evidence_ref = evidence_refs.get(
            binding.ticket_id,
            binding.result_claim_ref,
        )
        try:
            terminal_state = _derive_terminal_state(
                authority,
                binding,
                repository_root=root,
            )
            recovery_lease = authority._issue_final_eval_recovery_lease(
                maintenance_lease,
                binding_id=binding.ticket_id,
                evidence_ref=evidence_ref,
            )
            # Crash boundary: the recovery lease is committed but the
            # binding is untouched; a fresh reconciler pass must re-derive
            # and recover the same binding (never reopened).
            if crash_hook is not None:
                crash_hook("CRASH_AFTER.RECOVERY_LEASE")
            # CR010-R03: RESULT_STAGED -> CLOSED is ONE committed
            # transaction; a crash here leaves the binding durably CLOSED
            # (claim fixed, terminal empty) for a fresh process to finalize.
            authority._close_final_eval_binding(recovery_lease)
            if crash_hook is not None:
                crash_hook("CRASH_AFTER.CLOSED")
            authority._finalize_final_eval_binding(
                recovery_lease,
                terminal_state=terminal_state,
                evidence_ref=evidence_ref,
            )
            # Crash boundary: the terminal transition committed; a fresh
            # pass must observe AUTHORITY_TERMINAL and skip it.
            if crash_hook is not None:
                crash_hook("CRASH_AFTER.AUTHORITY_TERMINAL")
            recovered.append(binding.ticket_id)
        except (FinalEvalRecoveryError, ValueError, TaskTicketError):
            unresolved.append(binding.ticket_id)

    return ReconciliationReport(
        recovered=tuple(recovered),
        unresolved=tuple(unresolved),
        skipped=tuple(skipped),
    )
