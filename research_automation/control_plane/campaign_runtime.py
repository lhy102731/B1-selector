"""Authorized single-process Campaign runtime (P6R3 Task 7).

This module assembles the only authorized execution path for fake/injected
Campaign runs in the corrective recovery.  It owns the fixed phase order
(preflight -> prepare -> start -> invoke required roster -> complete model ->
evidence -> commit/no-learning -> settle -> information gain -> next-cycle
decision -> observers -> complete/next cycle) and never fabricates evidence,
outcomes, receipts or usage.  It never opens network connections, never
constructs real providers, never reads root secrets and never writes to the
Authority/Operational stores directly; every durable write goes through the
injected ``OperationalCampaignController``.

The runtime is deliberately small: it only calls public controller steps,
uses durable replay for recovery, and exposes a safe result contract that
never leaks raw prompts, provider responses, secrets, URLs, nonces or data
bytes.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
import time

from research_automation.foundations.protocols import (
    ExecutionSpec,
    require_protocol_conformant,
)
from research_automation.control_plane.campaign import ProviderResponse
from research_automation.control_plane.campaign_controller import (
    CampaignBudgetLimits,
    CycleReservationLimits,
    OperationalCampaignController,
    OperationalModelCallLimits,
)
from research_automation.control_plane.campaign_roster import RosterMember
from research_automation.control_plane.evidence_learning import EvidenceAdapter
from research_automation.control_plane.campaign_store import (
    CampaignExecutionMode,
    CampaignLearningCommitSink,
    DryRunIsolationError,
)
from research_automation.task_queue import ExperimentTask

# Fixed phase chain (plan Step 7.2).  Each entry must execute exactly once per
# cycle in this order; skipping or repeating a phase fails closed.
PHASE_CHAIN = (
    "preflight",
    "prepare",
    "start",
    "invoke_required_roster",
    "complete_model",
    "evidence",
    "commit_or_no_learning",
    "settle",
    "information_gain",
    "next_cycle_decision",
    "observers",
)


class CampaignRuntimeError(RuntimeError):
    """Base error for invalid runtime configuration or phase violations."""


class CampaignRuntimePhaseError(CampaignRuntimeError):
    """Raised when a phase is skipped, repeated or out of order."""


class CampaignRuntimeSafetyError(CampaignRuntimeError):
    """Raised when an unsafe field would enter the safe result."""


@dataclass(frozen=True, slots=True)
class CycleSummary:
    """Safe per-cycle summary; contains only hashes, refs and counts."""

    cycle_id: str
    cycle_number: int
    status: str
    decision: str
    reason_code: str | None
    evidence_refs: tuple[str, ...] = ()
    event_refs: tuple[str, ...] = ()
    model_call_count: int = 0
    usage_status: str | None = None
    cost: str | None = None
    currency: str | None = None


@dataclass(frozen=True, slots=True)
class CampaignRuntimeResult:
    """Safe result contract (plan Step 7.6).

    Only status, canonical hashes, event/evidence refs and cycle/budget
    summaries are allowed.  Raw prompts, provider responses, root secrets,
    URLs, nonces and holdout/data bytes are never included.
    """

    campaign_id: str
    namespace: str
    mode: str
    status: str
    cycles_completed: int
    decision: str
    reason_code: str | None
    campaign_snapshot: Mapping[str, object]
    budget_summary: Mapping[str, object]
    cycle_summaries: tuple[CycleSummary, ...] = ()
    diagnostics: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": "control_plane.campaign_runtime_result.v1",
            "campaign_id": self.campaign_id,
            "namespace": self.namespace,
            "mode": self.mode,
            "status": self.status,
            "cycles_completed": self.cycles_completed,
            "decision": self.decision,
            "reason_code": self.reason_code,
            "campaign_snapshot": dict(self.campaign_snapshot),
            "budget_summary": dict(self.budget_summary),
            "cycle_summaries": [
                {
                    "cycle_id": summary.cycle_id,
                    "cycle_number": summary.cycle_number,
                    "status": summary.status,
                    "decision": summary.decision,
                    "reason_code": summary.reason_code,
                    "evidence_refs": list(summary.evidence_refs),
                    "event_refs": list(summary.event_refs),
                    "model_call_count": summary.model_call_count,
                    "usage_status": summary.usage_status,
                    "cost": summary.cost,
                    "currency": summary.currency,
                }
                for summary in self.cycle_summaries
            ],
            "diagnostics": list(self.diagnostics),
        }


class CampaignRuntimeObserver:
    """Observer hooks invoked around the durable cycle boundary.

    ``after_cycle_settled`` fires once the current cycle is terminal;
    ``before_next_cycle`` fires before the next cycle is prepared.  A raised
    observer exception requests a durable pause: the runtime stops without
    fabricating completion.
    """

    def after_cycle_settled(self, summary: CycleSummary) -> None:
        """Invoked after settlement; may raise to request a durable pause."""

    def before_next_cycle(self, decision: Mapping[str, object]) -> None:
        """Invoked before the next cycle; may raise to request a pause."""


class CampaignCommandContext:
    """Authorized runtime inputs (plan Step 7.1).

    Constructed in-process only; never deserialized from mapping/JSON/argv/
    env.  Holds the authorized controller plus frozen execution inputs and
    provider/evidence/learning sinks.  Never prints or exposes secrets.
    """

    def __init__(
        self,
        *,
        controller: OperationalCampaignController,
        task: ExperimentTask,
        execution_spec: ExecutionSpec,
        roster_members: Sequence[RosterMember],
        reservation_limits: CycleReservationLimits,
        call_limits: OperationalModelCallLimits,
        provider: object,
        prompt: object,
        evidence_adapter: EvidenceAdapter,
        learning_commit_sink: CampaignLearningCommitSink,
        campaign_id: str,
        namespace: str,
        mode: str,
        observers: Sequence[CampaignRuntimeObserver] = (),
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        authority_task_report: Mapping[str, object] | None = None,
    ) -> None:
        if not isinstance(controller, OperationalCampaignController):
            raise TypeError("controller must be an OperationalCampaignController")
        if not isinstance(task, ExperimentTask):
            raise TypeError("task must be an ExperimentTask")
        if not isinstance(execution_spec, ExecutionSpec):
            raise TypeError("execution_spec must be an ExecutionSpec")
        if not isinstance(reservation_limits, CycleReservationLimits):
            raise TypeError("reservation_limits must be CycleReservationLimits")
        if not isinstance(call_limits, OperationalModelCallLimits):
            raise TypeError("call_limits must be OperationalModelCallLimits")
        if not isinstance(evidence_adapter, EvidenceAdapter):
            raise TypeError("evidence_adapter must be an EvidenceAdapter")
        if not isinstance(learning_commit_sink, CampaignLearningCommitSink):
            raise TypeError(
                "learning_commit_sink must be a CampaignLearningCommitSink"
            )
        if not isinstance(mode, str):
            raise CampaignRuntimeError("mode must be formal or dry-run")
        normalized_mode = mode.strip().lower()
        if normalized_mode not in {"formal", "dry-run", "dry_run"}:
            raise CampaignRuntimeError("mode must be formal or dry-run")
        if normalized_mode == "dry_run":
            normalized_mode = "dry-run"
        members = tuple(roster_members)
        if not members or not all(
            isinstance(member, RosterMember) for member in members
        ):
            raise TypeError("roster_members must be a non-empty tuple of RosterMember")
        for observer in observers:
            if not isinstance(observer, CampaignRuntimeObserver):
                raise TypeError("observers must be CampaignRuntimeObserver instances")
        # Roster consistency with the frozen execution spec (plan Step 7.2).
        spec_roster = tuple(execution_spec.protocol.roster)
        roster_triples = {
            (member.role, member.profile, member.model) for member in members
        }
        spec_triples = {
            (item.role, item.provider_profile_id, item.model_id)
            for item in spec_roster
        }
        if roster_triples != spec_triples:
            raise CampaignRuntimeError(
                "roster members conflict with the execution spec roster"
            )
        self._controller = controller
        self._task = task
        self._execution_spec = execution_spec
        self._roster_members = members
        self._reservation_limits = reservation_limits
        self._call_limits = call_limits
        self._provider = provider
        self._prompt = prompt
        self._evidence_adapter = evidence_adapter
        self._learning_commit_sink = learning_commit_sink
        self._campaign_id = campaign_id
        self._namespace = namespace
        self._mode = normalized_mode
        self._observers = tuple(observers)
        self._monotonic_ns = monotonic_ns
        if authority_task_report is not None and not isinstance(
            authority_task_report,
            Mapping,
        ):
            raise TypeError("authority_task_report must be a mapping or None")
        self._authority_task_report = authority_task_report

    @property
    def controller(self) -> OperationalCampaignController:
        return self._controller

    @property
    def campaign_id(self) -> str:
        return self._campaign_id

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def mode(self) -> str:
        return self._mode


class CampaignRuntime:
    """Single-process authorized Campaign runner (plan Step 7.5)."""

    def __init__(self, context: CampaignCommandContext) -> None:
        if not isinstance(context, CampaignCommandContext):
            raise TypeError("context must be a CampaignCommandContext")
        self._context = context
        self._controller = context.controller
        self._phase = 0
        self._diagnostics: list[str] = []

    def _require_next_phase(self, expected: str) -> None:
        if self._phase >= len(PHASE_CHAIN) or PHASE_CHAIN[self._phase] != expected:
            raise CampaignRuntimePhaseError(
                f"expected phase {expected!r} but the chain is at "
                f"{PHASE_CHAIN[self._phase] if self._phase < len(PHASE_CHAIN) else 'end'}"
            )
        self._phase += 1

    def run(self, *, max_cycles: int | None = None) -> CampaignRuntimeResult:
        """Run the fixed phase chain until STOP or the cycle budget is met."""
        if max_cycles is not None and (
            type(max_cycles) is not int or max_cycles < 1
        ):
            raise CampaignRuntimeError("max_cycles must be a positive integer")
        controller = self._controller
        task = self._context._task
        members = self._context._roster_members
        cycle_number = 1
        summaries: list[CycleSummary] = []
        final_decision = "STOP"
        final_reason: str | None = None
        status = "COMPLETED"

        while True:
            if max_cycles is not None and cycle_number > max_cycles:
                final_decision = "STOP"
                final_reason = "MAX_CYCLES_LIMIT"
                break
            self._require_next_phase("preflight")
            require_protocol_conformant(self._context._execution_spec)
            self._require_next_phase("prepare")
            prepared = controller.prepare_cycle(
                task=task,
                cycle_number=cycle_number,
                execution_spec=self._context._execution_spec,
                roster_members=members,
                reservation_limits=self._context._reservation_limits,
            )
            self._require_next_phase("start")
            execution = controller.start_execution(
                cycle_id=prepared.cycle_id,
                acquisition_id=f"acquire-{prepared.cycle_id}",
            )
            self._require_next_phase("invoke_required_roster")
            model_calls = 0
            for member in members:
                controller.invoke_member_json(
                    execution=execution,
                    member_id=member.member_id,
                    provider=self._context._provider,
                    prompt=self._context._prompt,
                    limits=self._context._call_limits,
                )
                model_calls += 1
            self._require_next_phase("complete_model")
            usage = controller.complete_model_execution(execution=execution)
            self._require_next_phase("evidence")
            evidence = controller.record_model_evidence(
                execution=execution,
                member_id=members[0].member_id,
                evidence_adapter=self._context._evidence_adapter,
            )
            self._require_next_phase("commit_or_no_learning")
            learning_committed = False
            if self._context._authority_task_report is not None:
                try:
                    learning = controller.commit_learning(
                        execution=execution,
                        evidence_receipt=evidence,
                        authority_task_report=self._context._authority_task_report,
                        learning_commit_sink=self._context._learning_commit_sink,
                    )
                    learning_committed = True
                except DryRunIsolationError:
                    self._diagnostics.append(
                        "dry-run isolation: formal learning skipped"
                    )
            else:
                self._diagnostics.append(
                    "no authority task report: no-learning settlement path"
                )
            self._require_next_phase("settle")
            if learning_committed:
                settlement = controller.settle_cycle(
                    execution=execution,
                    execution_usage=usage,
                    learning_commit_receipt=learning,
                )
            else:
                settlement = controller.settle_cycle_without_learning(
                    execution=execution,
                    execution_usage=usage,
                    evidence_receipt=evidence,
                )
            self._require_next_phase("information_gain")
            information_gain = controller.record_information_gain(
                execution=execution,
                settlement_receipt=settlement,
            )
            self._require_next_phase("next_cycle_decision")
            decision = controller.decide_next_cycle(
                execution=execution,
                information_gain_receipt=information_gain,
            )
            self._require_next_phase("observers")
            summary = CycleSummary(
                cycle_id=prepared.cycle_id,
                cycle_number=cycle_number,
                status="COMPLETED",
                decision=str(decision.decision),
                reason_code=decision.reason_code,
                evidence_refs=(evidence.manifest_sha256,),
                event_refs=tuple(sorted({evidence.event_id, settlement.event_id})),
                model_call_count=model_calls,
                usage_status=(
                    usage.usage_status.value
                    if usage.usage_status is not None
                    else None
                ),
                cost=usage.cost,
                currency=usage.currency,
            )
            summaries.append(summary)
            for observer in self._context._observers:
                try:
                    observer.after_cycle_settled(summary)
                except Exception as error:  # noqa: BLE001 - durable pause request
                    self._diagnostics.append(
                        f"observer after_cycle_settled requested pause: {error}"
                    )
                    status = "PAUSED_BY_OBSERVER"
                    final_decision = str(decision.decision)
                    final_reason = decision.reason_code
                    break
            if status == "PAUSED_BY_OBSERVER":
                break
            if decision.decision == "STOP":
                final_decision = "STOP"
                final_reason = decision.reason_code
                break
            for observer in self._context._observers:
                try:
                    observer.before_next_cycle(
                        {
                            "decision": decision.decision,
                            "reason_code": decision.reason_code,
                            "next_cycle_number": decision.next_cycle_number,
                        }
                    )
                except Exception as error:  # noqa: BLE001 - durable pause request
                    self._diagnostics.append(
                        f"observer before_next_cycle requested pause: {error}"
                    )
                    status = "PAUSED_BY_OBSERVER"
                    final_decision = str(decision.decision)
                    final_reason = decision.reason_code
                    break
            if status == "PAUSED_BY_OBSERVER":
                break
            cycle_number += 1

        try:
            controller.complete_campaign()
        except Exception as error:  # noqa: BLE001 - surfaced in diagnostics
            self._diagnostics.append(f"complete_campaign: {error}")
            status = "INCOMPLETE"
        campaign_snapshot = controller.campaign_snapshot()
        budget = controller.budget_snapshot()
        budget_summary = {
            "currency": budget.currency,
            "reserved_cost": str(budget.reserved_cost),
            "spent_cost": str(budget.spent_cost),
            "reserved_input_tokens": budget.reserved_input_tokens,
            "reserved_output_tokens": budget.reserved_output_tokens,
            "spent_input_tokens": budget.spent_input_tokens,
            "spent_output_tokens": budget.spent_output_tokens,
        }
        return CampaignRuntimeResult(
            campaign_id=self._context.campaign_id,
            namespace=self._context.namespace,
            mode=self._context.mode,
            status=status,
            cycles_completed=len(summaries),
            decision=final_decision,
            reason_code=final_reason,
            campaign_snapshot={
                "campaign_id": campaign_snapshot.campaign_id,
                "status": campaign_snapshot.status.value,
                "sequence": campaign_snapshot.sequence,
            },
            budget_summary=budget_summary,
            cycle_summaries=tuple(summaries),
            diagnostics=tuple(self._diagnostics),
        )


__all__ = [
    "CampaignCommandContext",
    "CampaignRuntime",
    "CampaignRuntimeError",
    "CampaignRuntimeObserver",
    "CampaignRuntimePhaseError",
    "CampaignRuntimeResult",
    "CampaignRuntimeSafetyError",
    "CycleSummary",
    "PHASE_CHAIN",
]
