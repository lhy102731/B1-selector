"""Bounded final evaluation reconciler (P8 CR-009 Gate D).

The reconciler observes durable bindings and closes only those that are
recoverable (RESULT_STAGED or CLOSED with a fixed result claim).  It never
reopens holdout bytes, never recomputes a staged result, never reissues the
original lease and never advances an intermediate state.  A binding stuck
in an intermediate state (REQUEST_FROZEN/AUTHORIZED/CONSUMED/EVALUATING)
is reported as UNRESOLVED -- the orchestrator's idempotent CAS replay is
the only path forward, and only with the original trusted inputs.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .stores import (
    FinalEvalBindingSnapshot,
    FinalEvalRecoveryError,
    TaskExecutionLease,
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


def reconcile(
    authority: _AuthorityStore,
    maintenance_lease: TaskExecutionLease,
    *,
    evidence_ref_for: Mapping[str, str] | None = None,
) -> ReconciliationReport:
    """Close every recoverable binding with a bounded recovery lease.

    Invariants (GPT F-03 requirements):
    - no reopen: the reconciler never opens holdout bytes (no path access);
    - no recompute: staged results are closed as-is, never recomputed;
    - no reissue: the original lease is never restored; a fresh recovery
      lease is issued per binding and only for RESULT_STAGED/CLOSED;
    - intermediate states stay untouched (UNRESOLVED).
    """

    if not isinstance(authority, _AuthorityStore):
        raise FinalEvalReconcilerError("an _AuthorityStore is required")
    if not isinstance(maintenance_lease, TaskExecutionLease):
        raise FinalEvalReconcilerError(
            "a maintenance TaskExecutionLease is required"
        )
    evidence_refs = dict(evidence_ref_for or {})

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
            recovery_lease = authority._issue_final_eval_recovery_lease(
                maintenance_lease,
                binding_id=binding.ticket_id,
                evidence_ref=evidence_ref,
            )
            authority._recover_final_eval_binding(
                recovery_lease,
                terminal_state="SUCCEEDED",
                evidence_ref=evidence_ref,
            )
            recovered.append(binding.ticket_id)
        except (FinalEvalRecoveryError, ValueError):
            unresolved.append(binding.ticket_id)

    return ReconciliationReport(
        recovered=tuple(recovered),
        unresolved=tuple(unresolved),
        skipped=tuple(skipped),
    )
