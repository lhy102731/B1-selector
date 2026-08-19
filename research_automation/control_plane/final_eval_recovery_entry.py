"""Recovery-only final evaluation entry (CR-010 A3).

The ONLY entry a fresh process may use to recover a crashed final
evaluation.  The immutable ``RecoveryContext`` carries ONLY the redacted
root/authority capability, the ``binding_id`` and the controlled store
locator (the repository root that holds the disposable Authority store) --
never a nonce, never a grant bearer, never an evaluation request, never a
material resolver, evaluator, holdout backend, worker launcher or result
sink.  The entry reads durable binding/claim/receipt state, verifies the
exact lease, advances only the allowed transition and returns the already
existing result: it never opens the Holdout, never runs a worker and never
resolves materials.

State handling (durable binding state decides the exact lease, exactly one
of recovery or failure -- never both):

- ``AUTHORITY_TERMINAL`` -> idempotent terminal replay (read-only).
- ``RESULT_STAGED``/``CLOSED`` -> a fresh P0 READ maintenance lease is
  issued from the durable grant lineage and the reconciler closes the
  binding (never recompute, never reopen).
- ``CONSUMED``/``EVALUATING`` -> a typed one-shot ``FinalEvalFailureLease``
  terminalizes the binding as an unreplayable FAILED tombstone (never
  rebuild the evaluation context).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .final_eval_authority import FinalEvalRequestV2  # noqa: F401  (type ref)
from .stores import (
    Actor,
    AuthorityIdentity,
    FinalEvalFailureLease,
    Phase,
    SideEffect,
    TaskExecutionLease,
    _AuthorityStore,
)


class RecoveryEntryError(RuntimeError):
    """Base error for the recovery-only entry."""


class RecoveryContextRejected(RecoveryEntryError):
    """The recovery context is forged or carries evaluation secrets."""


@dataclass(frozen=True, slots=True)
class RecoveryContext:
    """The immutable recovery-only context (CR-010 A3).

    Deliberately NOT accepted here: nonce, grant, worker launcher, material
    resolver, evaluator, holdout backend and result sink.  The authority
    capability and the binding id are repr-redacted/opaque; the raw nonce
    and the root secret never appear in repr, errors or logs.
    """

    authority_capability: str = field(repr=False)
    binding_id: str
    repository_root: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.authority_capability, str)
            or len(self.authority_capability) < 32
        ):
            raise RecoveryContextRejected(
                "authority capability must be a strong in-memory secret"
            )
        if not isinstance(self.binding_id, str) or not self.binding_id.strip():
            raise RecoveryContextRejected("binding_id must be non-empty")
        if not isinstance(self.repository_root, str) or not Path(
            self.repository_root
        ).is_dir():
            raise RecoveryContextRejected("repository root is unavailable")


def _store_pair(repository_root: str):
    from . import stores as _stores_module

    root = Path(repository_root).resolve()
    return _stores_module.store_path_override(
        authority=root / "authority.sqlite3",
        operational=root / "operational.sqlite3",
    )


def _grant_lineage(authority: _AuthorityStore, binding_id: str):
    """Reconstruct the durable actor/identity from the binding ticket's
    grant row (never from evaluation context)."""

    def read(connection):
        row = connection.execute(
            """
            SELECT grant.actor_id, grant.actor_type, grant.invocation_id,
                   grant.plan_hash, grant.scope_hash,
                   grant.instruction_policy_hash, grant.attempt_id
            FROM task_tickets_v2 AS ticket
            JOIN phase_grants_v2 AS grant
              ON grant.grant_id = ticket.grant_id
            WHERE ticket.ticket_id = ?
            """,
            (binding_id,),
        ).fetchone()
        if row is None:
            raise RecoveryEntryError(
                "binding ticket has no grant lineage"
            )
        return row

    from .sqlite_uow import _SqliteUnitOfWork
    from .stores import _authority_spec

    row = _SqliteUnitOfWork(_authority_spec())._read(read)
    actor = Actor(
        str(row["actor_id"]),
        str(row["actor_type"]),
        str(row["invocation_id"]),
    )
    identity = AuthorityIdentity(
        str(row["plan_hash"]),
        str(row["scope_hash"]),
        str(row["instruction_policy_hash"]),
    )
    attempt_id = str(row["attempt_id"])
    return actor, identity, attempt_id


def _maintenance_lease(
    authority: _AuthorityStore,
    *,
    actor: Actor,
    identity: AuthorityIdentity,
) -> TaskExecutionLease:
    """A fresh P0 READ maintenance lease from the DURABLE grant lineage
    (the reconciler's bounded recovery lease is issued against it)."""
    import secrets as _secrets
    from datetime import datetime, timezone

    unique = _secrets.token_hex(8)
    maintenance_actor = Actor(
        "final-eval-recovery-maint",
        "automation",
        f"final-eval-recovery-inv-{unique}",
    )
    attempt_id = f"final-eval-recovery-maint-{unique}"
    envelope = authority._provision_authorization(
        phase=Phase.P0,
        attempt_id=attempt_id,
        actor=maintenance_actor,
        identity=identity,
        expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
        allowed_side_effects=(SideEffect.READ,),
    )
    grant = authority.claim_authorization(
        envelope,
        expected_phase=Phase.P0,
        expected_attempt_id=attempt_id,
        actor=maintenance_actor,
        identity=identity,
    )
    ticket = authority._issue_task_ticket(
        grant,
        {
            "task_id": "P8-RUNTIME-RECONCILER-MAINT",
            "objective": "recovery-only reconciler maintenance",
            "dependencies": [],
            "idempotency_key": "final-eval-recovery-maint-" + unique,
            "task_spec_ref": "manifest.json",
            "task_spec_sha256": "1" * 64,
            "requirements": {
                "required_test_receipt_ids": [],
                "required_review_receipt_ids": [],
                "required_evidence_ids": [],
            },
            "allowed_files": ["research_automation/control_plane/"],
            "forbidden_files": ["data/"],
            "baseline_ref": "manifest.json",
            "baseline_sha256": "1" * 64,
            "input_evidence_refs": [],
        },
        allowed_side_effects=(SideEffect.READ,),
    )
    return authority._begin_task(ticket)


def run_recovery(
    *,
    authority_capability: str,
    binding_id: str,
    repository_root: str,
) -> dict[str, object]:
    """Recover one binding from durable state only (CR-010 A3).

    Returns the durable outcome: ``saga_state`` is always
    ``AUTHORITY_TERMINAL``; ``terminal_binding`` is the existing terminal
    outcome (SUCCEEDED/FAILED) or the new unreplayable FAILED tombstone for
    intermediate crash states.
    """
    context = RecoveryContext(
        authority_capability=authority_capability,
        binding_id=binding_id,
        repository_root=repository_root,
    )
    with _store_pair(context.repository_root):
        from . import stores as _stores_module

        _stores_module._expected_schema_sha256.cache_clear()
        try:
            authority = _AuthorityStore(
                root_secret=context.authority_capability
            )
            binding = authority.final_eval_binding_snapshot(
                context.binding_id
            )
            if binding.saga_state == "AUTHORITY_TERMINAL":
                # idempotent terminal replay: read-only
                return {
                    "saga_state": binding.saga_state,
                    "terminal_binding": binding.terminal_binding or "IN_DOUBT",
                    "result_claim_ref": binding.result_claim_ref,
                    "recovery": "TERMINAL_REPLAY",
                }
            actor, identity, _attempt = _grant_lineage(
                authority, context.binding_id
            )
            if binding.saga_state in ("RESULT_STAGED", "CLOSED"):
                from .final_eval_reconciler import reconcile

                maintenance_lease = _maintenance_lease(
                    authority,
                    actor=actor,
                    identity=identity,
                )
                report = reconcile(
                    authority,
                    maintenance_lease,
                    evidence_ref_for={
                        context.binding_id: binding.result_claim_ref or ""
                    },
                    repository_root=context.repository_root,
                )
                authority._finish_task(
                    maintenance_lease,
                    outcome=(
                        "SUCCEEDED"
                        if context.binding_id
                        in set(report.recovered) | set(report.skipped)
                        else "FAILED"
                    ),
                    evidence_ref=str(binding.result_claim_ref or ""),
                )
                final = authority.final_eval_binding_snapshot(
                    context.binding_id
                )
                return {
                    "saga_state": final.saga_state,
                    "terminal_binding": final.terminal_binding or "IN_DOUBT",
                    "result_claim_ref": final.result_claim_ref,
                    "recovery": "RECONCILER_CLOSE",
                }
            if binding.saga_state in ("CONSUMED", "EVALUATING"):
                failure_lease = FinalEvalFailureLease.issue(
                    authority=authority,
                    binding_id=context.binding_id,
                    expected_saga_version=binding.saga_version,
                    identity=identity,
                    actor=actor,
                )
                failed = authority._fail_final_eval_binding(
                    failure_lease,
                    binding_id=context.binding_id,
                    expected_version=binding.saga_version,
                    failure_reason=(
                        "recovery entry: durable binding was left in "
                        + binding.saga_state
                        + " by a crashed process; the evaluation context "
                        "is never rebuilt"
                    ),
                )
                return {
                    "saga_state": failed.saga_state,
                    "terminal_binding": failed.terminal_binding or "FAILED",
                    "result_claim_ref": failed.result_claim_ref,
                    "recovery": "FAILED_MAINTENANCE",
                }
            raise RecoveryEntryError(
                "binding state has no allowed recovery transition: "
                + binding.saga_state
            )
        finally:
            _stores_module._expected_schema_sha256.cache_clear()


__all__ = [
    "RecoveryContext",
    "RecoveryContextRejected",
    "RecoveryEntryError",
    "run_recovery",
]
