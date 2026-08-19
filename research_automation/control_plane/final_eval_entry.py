"""The ONLY controlled host entry for the FinalEval runtime (Phase B).

``final_eval_entry.run(context)`` accepts an already verified in-memory
``AuthorizedFinalEvalContext``, calls the authorized composition root and
returns binding/outcome/state/evidence.  Missing or forged contexts fail
BEFORE evaluator/store construction; neither argv, environment nor a
database lookup can construct authorization.  ``dry-run`` is a zero-write
wiring preview and never an acceptance result.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .final_eval_composition import (
    AuthorizedFinalEvalContext,
    FinalEvalCompositionRejected,
    compose_final_eval_runtime,
    compose_holdout_store,
    compose_staging_backend,
)
from .final_evaluator import (
    AuthorityBroker,
    TrustedEvaluator,
    TrustedEvaluatorAdapter,
)
from .stores import Phase

if TYPE_CHECKING:  # pragma: no cover -- runtime imports stay lazy
    pass


def _compose_evaluator(context: AuthorizedFinalEvalContext) -> TrustedEvaluator:
    """Assemble the OPEN_HOLDOUT evaluator from the composition-owned
    HoldoutStore + synthetic staging backend (CR-010 F-01).

    The production entry NEVER accepts a caller-selected evaluator or a
    TrustedEvaluator subclass override: the evaluator is assembled HERE
    from the Authority-backed store and the bounded staging backend, so a
    deleted or tampered Holdout can never be papered over by injecting a
    forged evaluator.
    """
    return TrustedEvaluator(
        broker=AuthorityBroker(store=compose_holdout_store(context)),
        adapter=TrustedEvaluatorAdapter(
            backend=compose_staging_backend()
        ),
    )


def run(context: AuthorizedFinalEvalContext) -> dict[str, object]:
    """The ONLY host entry (CR-010 C0, Phase B).

    Accepts an already verified in-memory context and drives the durable
    saga through the composition root.  Missing or forged context fails
    before evaluator/store construction; an attempt id alone -- including
    one that exists in the database -- is never an authorization.  The
    evaluator is ALWAYS the composition-assembled OPEN_HOLDOUT evaluator;
    no caller-selected evaluator, evaluator request or material resolver
    override exists on the context (CR-010 F-01).
    """
    if not isinstance(context, AuthorizedFinalEvalContext):
        raise FinalEvalCompositionRejected(
            "the host entry requires an AuthorizedFinalEvalContext"
        )
    grant_phase = getattr(context.grant, "phase", None)
    if grant_phase is not Phase.P8:
        raise FinalEvalCompositionRejected(
            "the host entry requires a P8 grant"
        )
    runtime = compose_final_eval_runtime(context)
    evaluator = _compose_evaluator(context)
    return runtime.run(
        request=context.request,
        grant=context.grant,
        nonce=context.nonce,
        actor=context.actor,
        identity=context.identity,
        idempotency_key=context.idempotency_key,
        task_spec_ref=context.task_spec_ref,
        task_spec_sha256=context.task_spec_sha256,
        evaluator=evaluator,
        data_root=context.data_root,
    )


__all__ = ["run"]
