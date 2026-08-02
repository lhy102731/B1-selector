"""Public P6 Campaign composition over the durable domain journals."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from research_automation.foundations.protocols import (
    ExecutionSpec,
    require_protocol_conformant,
)
from research_automation.task_queue import ExperimentTask

from . import stores
from .budget import (
    BudgetConflictError,
    BudgetLedger,
    BudgetExceededError,
    BudgetReservation,
    BudgetSnapshot,
    _cost as _bounded_cost,
    _cost_text,
)
from .campaign import (
    InvalidModelResponseError,
    InvocationOutcome,
    ModelInvocation,
    ModelInvocationProviderError,
    ModelInvocationTimeoutError,
    RetryingModelInvocation,
    StreamingDisabledError,
    UsageEnvelope,
    UsageStatus,
)
from .campaign_context import (
    CycleContextReceipt,
    OperationalCycleContextJournal,
    canonical_campaign_proposal,
)
from .evidence_learning import (
    EvidenceAdapter,
    EvidenceResult,
    LearningCommitService,
)
from .campaign_freeze import FrozenCycleInputs, OperationalCycleFreezeJournal
from .campaign_lease import (
    CycleLease,
    CycleLeaseConflictError,
    OperationalCycleLeaseJournal,
    ProcessIdentityProvider,
    _verified_current_owner,
)
from .campaign_lifecycle import (
    CampaignStateConflictError,
    CampaignSnapshot,
    CampaignStatus,
    CycleSnapshot,
    CycleStatus,
    OperationalCampaignLifecycle,
    _CYCLE_TRANSITIONED,
)
from .campaign_roster import (
    OperationalRosterJournal,
    RosterCompletion,
    RosterDriftError,
    RosterManifest,
    RosterMember,
    VerifiedRosterResponse,
    _roster_manifest,
)
from .campaign_store import (
    CampaignLearningCommitSink,
    CampaignJournalError,
    CycleBudgetSnapshot,
    OperationalBudgetJournal,
    OperationalCampaignJournal,
    OperationalCycleBudgetJournal,
    OperationalUsageJournal,
    RecordedModelAttempt,
    _BUDGET_RESERVED,
    _BUDGET_SETTLED,
    _attempt_id,
    _event_domain_payload,
    _identifier,
)
from .sqlite_uow import _SqliteUnitOfWork


_WORK_ITEM_AGGREGATE_TYPE = "CAMPAIGN_WORK_ITEM"
_WORK_ITEM_ADOPTED = "CAMPAIGN_WORK_ITEM_ADOPTED"
_PREPARATION_AGGREGATE_TYPE = "CAMPAIGN_CYCLE_PREPARATION"
_CYCLE_PREPARED = "CAMPAIGN_CYCLE_PREPARED"
_MODEL_CALL_AGGREGATE_TYPE = "OPERATIONAL_MODEL_CALL"
_MODEL_CALL_STARTED = "OPERATIONAL_MODEL_CALL_STARTED"
_MODEL_CALL_COMPLETED = "OPERATIONAL_MODEL_CALL_COMPLETED"
_EXECUTION_USAGE_AGGREGATE_TYPE = "OPERATIONAL_EXECUTION_USAGE"
_EXECUTION_USAGE_FROZEN = "OPERATIONAL_EXECUTION_USAGE_FROZEN"
_MODEL_EVIDENCE_AGGREGATE_TYPE = "OPERATIONAL_MODEL_EVIDENCE"
_MODEL_EVIDENCE_RECORDED = "OPERATIONAL_MODEL_EVIDENCE_RECORDED"
_LEARNING_COMMIT_INTENT_AGGREGATE_TYPE = "OPERATIONAL_LEARNING_COMMIT_INTENT"
_LEARNING_COMMIT_INTENT_RECORDED = "OPERATIONAL_LEARNING_COMMIT_INTENT_RECORDED"
_LEARNING_COMMIT_AGGREGATE_TYPE = "OPERATIONAL_LEARNING_COMMIT"
_LEARNING_COMMIT_RECORDED = "OPERATIONAL_LEARNING_COMMIT_RECORDED"
_NO_LEARNING_DISPOSITION_AGGREGATE_TYPE = (
    "OPERATIONAL_NO_LEARNING_DISPOSITION"
)
_NO_LEARNING_DISPOSITION_RECORDED = (
    "OPERATIONAL_NO_LEARNING_DISPOSITION_RECORDED"
)
_CYCLE_SETTLEMENT_AGGREGATE_TYPE = "OPERATIONAL_CYCLE_SETTLEMENT"
_CYCLE_SETTLEMENT_RECORDED = "OPERATIONAL_CYCLE_SETTLEMENT_RECORDED"
_INFORMATION_GAIN_AGGREGATE_TYPE = "OPERATIONAL_INFORMATION_GAIN"
_INFORMATION_GAIN_RECORDED = "OPERATIONAL_INFORMATION_GAIN_RECORDED"
_NEXT_CYCLE_DECISION_AGGREGATE_TYPE = "OPERATIONAL_NEXT_CYCLE_DECISION"
_NEXT_CYCLE_DECISION_RECORDED = "OPERATIONAL_NEXT_CYCLE_DECISION_RECORDED"
_MAX_OPERATIONAL_PROMPT_BYTES = 48 * 1024
_MAX_OPERATIONAL_REQUEST_BYTES = 128 * 1024
_MAX_OPERATIONAL_OUTPUT_BYTES = 48 * 1024
_MODEL_CALL_IN_DOUBT_RESULT = object()
_MODEL_CALL_BUDGET_EXCEEDED_RESULT = object()


def _bounded_limits(
    *,
    max_input_tokens: int,
    max_output_tokens: int,
    max_cost: str | int | Decimal,
    max_wall_time_ms: int,
    max_tool_attempts: int,
    max_data_exposures: int,
    max_disk_growth_bytes: int,
) -> None:
    BudgetLedger(
        max_input_tokens=max_input_tokens,
        max_output_tokens=max_output_tokens,
        max_cost=max_cost,
        max_wall_time_ms=max_wall_time_ms,
        max_tool_attempts=max_tool_attempts,
        max_data_exposures=max_data_exposures,
        max_disk_growth_bytes=max_disk_growth_bytes,
    )


@dataclass(frozen=True, slots=True)
class CampaignBudgetLimits:
    max_cycles: int
    max_input_tokens: int
    max_output_tokens: int
    max_cost: str | int | Decimal
    max_wall_time_ms: int = 0
    max_tool_attempts: int = 0
    max_data_exposures: int = 0
    max_disk_growth_bytes: int = 0

    def __post_init__(self) -> None:
        if type(self.max_cycles) is not int or self.max_cycles < 0:
            raise ValueError("max_cycles must be a non-negative integer")
        _bounded_limits(
            max_input_tokens=self.max_input_tokens,
            max_output_tokens=self.max_output_tokens,
            max_cost=self.max_cost,
            max_wall_time_ms=self.max_wall_time_ms,
            max_tool_attempts=self.max_tool_attempts,
            max_data_exposures=self.max_data_exposures,
            max_disk_growth_bytes=self.max_disk_growth_bytes,
        )


@dataclass(frozen=True, slots=True)
class CycleReservationLimits:
    max_input_tokens: int
    max_output_tokens: int
    max_cost: str | int | Decimal
    max_wall_time_ms: int = 0
    max_tool_attempts: int = 0
    max_data_exposures: int = 0
    max_disk_growth_bytes: int = 0

    def __post_init__(self) -> None:
        _bounded_limits(
            max_input_tokens=self.max_input_tokens,
            max_output_tokens=self.max_output_tokens,
            max_cost=self.max_cost,
            max_wall_time_ms=self.max_wall_time_ms,
            max_tool_attempts=self.max_tool_attempts,
            max_data_exposures=self.max_data_exposures,
            max_disk_growth_bytes=self.max_disk_growth_bytes,
        )


@dataclass(frozen=True, slots=True)
class OperationalModelCallLimits:
    max_input_tokens: int
    max_output_tokens: int
    max_cost: str | int | Decimal
    max_wall_time_ms: int
    max_attempts: int

    def __post_init__(self) -> None:
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 100:
            raise ValueError(
                "max_attempts must be an integer from 1 through 100"
            )
        _bounded_limits(
            max_input_tokens=self.max_input_tokens,
            max_output_tokens=self.max_output_tokens,
            max_cost=self.max_cost,
            max_wall_time_ms=self.max_wall_time_ms,
            max_tool_attempts=self.max_attempts,
            max_data_exposures=0,
            max_disk_growth_bytes=0,
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "max_input_tokens": self.max_input_tokens,
            "max_output_tokens": self.max_output_tokens,
            "max_cost": _cost_text(_bounded_cost(self.max_cost)),
            "max_wall_time_ms": self.max_wall_time_ms,
            "max_attempts": self.max_attempts,
        }


@dataclass(frozen=True, slots=True)
class PreparedOperationalCycle:
    cycle_id: str
    reservation: BudgetReservation
    context: CycleContextReceipt
    roster: RosterManifest
    frozen: FrozenCycleInputs
    preparation_manifest_sha256: str

    @property
    def context_manifest_sha256(self) -> str:
        return self.context.manifest_sha256

    @property
    def roster_manifest_sha256(self) -> str:
        return self.roster.manifest_sha256


@dataclass(frozen=True, slots=True)
class ExecutingOperationalCycle:
    cycle: CycleSnapshot
    lease: CycleLease


@dataclass(frozen=True, slots=True)
class ExecutedOperationalModelCall:
    cycle_id: str
    call_id: str
    member_id: str
    output_json: str
    attempt_id: str
    attempt_count: int
    wall_time_ms: int | None
    usage_attempts: tuple[RecordedModelAttempt, ...]
    verified_response: VerifiedRosterResponse
    manifest_sha256: str
    event_id: str

    @property
    def output(self) -> object:
        return json.loads(self.output_json)


@dataclass(frozen=True, slots=True)
class OperationalExecutionUsage:
    cycle_id: str
    usage_status: UsageStatus
    input_tokens: int | None
    output_tokens: int | None
    cost: str | None
    currency: str | None
    wall_time_ms: int | None
    tool_attempts: int
    data_exposures: int
    disk_growth_bytes: int
    model_calls: tuple[ExecutedOperationalModelCall, ...]
    roster_completion: RosterCompletion
    manifest_sha256: str
    event_id: str


@dataclass(frozen=True, slots=True)
class OperationalEvidenceReceipt:
    cycle_id: str
    member_id: str
    preparation_manifest_sha256: str
    execution_usage_manifest_sha256: str
    model_call_manifest_sha256: str
    artifact_sha256: str
    adapter_manifest_sha256: str
    evidence: EvidenceResult
    manifest_sha256: str
    event_id: str


@dataclass(frozen=True, slots=True)
class OperationalLearningCommitReceipt:
    cycle_id: str
    member_id: str
    evidence_manifest_sha256: str
    authority_task_report_sha256: str
    packet_hash: str
    manifest_sha256: str
    event_id: str


@dataclass(frozen=True, slots=True)
class OperationalCycleSettlementReceipt:
    cycle_id: str
    reservation_id: str
    settlement_state: str
    execution_usage_manifest_sha256: str
    learning_commit_manifest_sha256: str
    budget_settlement_event_id: str
    manifest_sha256: str
    event_id: str


@dataclass(frozen=True, slots=True)
class OperationalNoLearningSettlementReceipt:
    cycle_id: str
    reservation_id: str
    disposition_reason: str
    evidence_manifest_sha256: str
    execution_usage_manifest_sha256: str
    settlement_state: str
    disposition_event_id: str
    budget_settlement_event_id: str
    manifest_sha256: str
    event_id: str


@dataclass(frozen=True, slots=True)
class OperationalInformationGainReceipt:
    cycle_id: str
    information_gain_status: str
    continuation_eligible: bool
    settlement_manifest_sha256: str
    learning_packet_hash: str | None
    disposition_reason: str | None
    manifest_sha256: str
    event_id: str


@dataclass(frozen=True, slots=True)
class OperationalNextCycleDecisionReceipt:
    cycle_id: str
    decision: str
    continuation_allowed: bool
    reason_code: str
    next_cycle_number: int | None
    information_gain_manifest_sha256: str
    cycle_budget_id: str
    reserved_cycle_count: int
    max_cycles: int
    manifest_sha256: str
    event_id: str


class _FencedOperationalUsageJournal:
    __slots__ = ("_controller", "_execution", "_delegate")

    def __init__(
        self,
        *,
        controller: "OperationalCampaignController",
        execution: ExecutingOperationalCycle,
        delegate: OperationalUsageJournal,
    ) -> None:
        self._controller = controller
        self._execution = execution
        self._delegate = delegate

    def begin(self, envelope: UsageEnvelope) -> None:
        if not isinstance(envelope, UsageEnvelope):
            raise TypeError("envelope must be a UsageEnvelope")
        self._delegate._journal._authorize()

        def begin(connection) -> None:
            self._controller._require_active_execution_in_transaction(
                connection,
                self._execution,
            )
            self._delegate._begin_in_transaction(
                connection,
                envelope=envelope,
            )

        _SqliteUnitOfWork(stores._operational_spec())._write(begin)

    def finish(
        self,
        *,
        call_id: str,
        attempt_id: str,
        outcome: InvocationOutcome,
    ) -> None:
        if not isinstance(outcome, InvocationOutcome):
            raise TypeError("outcome must be an InvocationOutcome")
        if outcome is InvocationOutcome.RESPONSE_RECEIVED:
            raise ValueError("outcome must be a final outcome")
        self._delegate._journal._authorize()

        def finish(connection) -> None:
            self._controller._require_active_execution_in_transaction(
                connection,
                self._execution,
            )
            self._delegate._finish_in_transaction(
                connection,
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=outcome,
            )

        _SqliteUnitOfWork(stores._operational_spec())._write(finish)


def _stable_id(domain: bytes, *parts: str) -> str:
    return hashlib.sha256(
        domain + b"\0" + "\0".join(parts).encode("ascii")
    ).hexdigest()


def _canonical_json_text(value: object, name: str) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, UnicodeError) as error:
        raise ValueError(f"{name} must be canonical JSON") from error


def _controller_sha256(domain: bytes, value: object, name: str) -> str:
    return hashlib.sha256(
        domain + b"\0" + _canonical_json_text(value, name).encode("utf-8")
    ).hexdigest()


def operational_prompt_sha256(prompt: object) -> str:
    """Return the bounded canonical prompt identity frozen into a roster."""

    prompt_text = _canonical_json_text(prompt, "operational prompt")
    if len(prompt_text.encode("utf-8")) > _MAX_OPERATIONAL_PROMPT_BYTES:
        raise ValueError("operational prompt exceeds the bounded size")
    return hashlib.sha256(
        b"control_plane.operational_role_prompt.v1\0"
        + prompt_text.encode("utf-8")
    ).hexdigest()


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _stored_sha256(value: object, name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise CampaignJournalError(f"{name} is not a SHA-256 digest")
    return value


def _evidence_result_payload(evidence: EvidenceResult) -> dict[str, object]:
    if not isinstance(evidence, EvidenceResult):
        raise TypeError("evidence must be an EvidenceResult")
    return {
        "verdict": evidence.verdict,
        "protocol_conformance": evidence.protocol_conformance,
        "audit_grade": evidence.audit_grade,
        "scientific_outcome": evidence.scientific_outcome,
        "promotion_eligible": evidence.promotion_eligible,
        "evidence_refs": [dict(reference) for reference in evidence.evidence_refs],
        "access_event_ids": list(evidence.access_event_ids),
        "taint_refs": list(evidence.taint_refs),
        "invalidation_codes": list(evidence.invalidation_codes),
    }


def _evidence_result_from_payload(payload: object) -> EvidenceResult:
    if not isinstance(payload, dict) or set(payload) != {
        "verdict",
        "protocol_conformance",
        "audit_grade",
        "scientific_outcome",
        "promotion_eligible",
        "evidence_refs",
        "access_event_ids",
        "taint_refs",
        "invalidation_codes",
    }:
        raise CampaignJournalError("stored operational evidence is invalid")
    try:
        evidence = EvidenceResult(
            verdict=payload["verdict"],
            protocol_conformance=payload["protocol_conformance"],
            audit_grade=payload["audit_grade"],
            scientific_outcome=payload["scientific_outcome"],
            promotion_eligible=payload["promotion_eligible"],
            evidence_refs=tuple(
                dict(reference) for reference in payload["evidence_refs"]
            ),
            access_event_ids=tuple(payload["access_event_ids"]),
            taint_refs=tuple(payload["taint_refs"]),
            invalidation_codes=tuple(payload["invalidation_codes"]),
        )
        if _canonical_json_text(
            _evidence_result_payload(evidence),
            "replayed operational evidence",
        ) != _canonical_json_text(payload, "stored operational evidence"):
            raise CampaignJournalError(
                "stored operational evidence is not canonical"
            )
    except (TypeError, ValueError, UnicodeError) as error:
        raise CampaignJournalError(
            "stored operational evidence is invalid"
        ) from error
    return evidence


def _canonical_task(
    task: ExperimentTask,
    *,
    cycle_number: int,
) -> tuple[str, dict[str, object]]:
    if not isinstance(task, ExperimentTask):
        raise TypeError("task must be an ExperimentTask input DTO")
    task_id = _identifier(task.task_id, "task.task_id")
    strategy = _identifier(task.strategy, "task.strategy")
    source = _identifier(task.source, "task.source")
    if type(task.priority) is not int or not 0 <= task.priority <= 1_000_000:
        raise ValueError("task.priority must be from 0 through 1000000")
    if type(cycle_number) is not int or not 1 <= cycle_number <= 1_000_000:
        raise ValueError("cycle_number must be from 1 through 1000000")
    proposal_json = _canonical_json_text(task.proposal, "task.proposal")
    try:
        proposal = json.loads(proposal_json)
    except json.JSONDecodeError as error:
        raise ValueError("task.proposal must be canonical JSON") from error
    if not isinstance(proposal, dict):
        raise ValueError("task.proposal must be a mapping")
    payload = {
        "schema_version": "control_plane.campaign_work_item.v1",
        "task_id": task_id,
        "cycle_number": cycle_number,
        "strategy": strategy,
        "proposal": proposal,
        "source": source,
        "priority": task.priority,
    }
    return task_id, payload


class OperationalCampaignController:
    """Compose one authoritative pre-execution path for Campaign Cycles."""

    __slots__ = (
        "_journal",
        "_lifecycle",
        "_cycle_budget",
        "_budget",
        "_context",
        "_roster",
        "_freeze",
        "_leases",
        "_monotonic_ns",
    )

    def __init__(
        self,
        *,
        journal: OperationalCampaignJournal,
        repository_root: str | Path,
        budget_limits: CampaignBudgetLimits,
        identity_provider: ProcessIdentityProvider,
        monotonic_ns: Callable[[], int],
        tokenizer_kind: str | None = None,
        tokenizer_name: str | None = None,
    ) -> None:
        if not isinstance(journal, OperationalCampaignJournal):
            raise TypeError("journal must be an OperationalCampaignJournal")
        if not isinstance(budget_limits, CampaignBudgetLimits):
            raise TypeError("budget_limits must be CampaignBudgetLimits")
        journal._authorize()
        lifecycle = OperationalCampaignLifecycle(journal=journal)
        budget_identity = (
            journal.namespace,
            journal.campaign_id,
        )
        cycle_budget = OperationalCycleBudgetJournal(
            journal=journal,
            budget_id=_stable_id(
                b"control_plane.controller_cycle_budget.v1",
                *budget_identity,
            ),
            max_cycles=budget_limits.max_cycles,
        )
        budget = OperationalBudgetJournal(
            journal=journal,
            budget_id=_stable_id(
                b"control_plane.controller_resource_budget.v1",
                *budget_identity,
            ),
            max_input_tokens=budget_limits.max_input_tokens,
            max_output_tokens=budget_limits.max_output_tokens,
            max_cost=budget_limits.max_cost,
            max_wall_time_ms=budget_limits.max_wall_time_ms,
            max_tool_attempts=budget_limits.max_tool_attempts,
            max_data_exposures=budget_limits.max_data_exposures,
            max_disk_growth_bytes=budget_limits.max_disk_growth_bytes,
        )
        context = OperationalCycleContextJournal(
            journal=journal,
            lifecycle=lifecycle,
            repository_root=repository_root,
            tokenizer_kind=tokenizer_kind,
            tokenizer_name=tokenizer_name,
        )
        roster = OperationalRosterJournal(
            journal=journal,
            lifecycle=lifecycle,
        )
        freeze = OperationalCycleFreezeJournal(
            journal=journal,
            lifecycle=lifecycle,
            roster=roster,
            context=context,
        )
        leases = OperationalCycleLeaseJournal(
            journal=journal,
            lifecycle=lifecycle,
            identity_provider=identity_provider,
            monotonic_ns=monotonic_ns,
        )
        self._journal = journal
        self._lifecycle = lifecycle
        self._cycle_budget = cycle_budget
        self._budget = budget
        self._context = context
        self._roster = roster
        self._freeze = freeze
        self._leases = leases
        self._monotonic_ns = monotonic_ns

    def prepare_cycle(
        self,
        *,
        task: ExperimentTask,
        cycle_number: int,
        execution_spec: ExecutionSpec,
        roster_members: tuple[RosterMember, ...],
        reservation_limits: CycleReservationLimits,
    ) -> PreparedOperationalCycle:
        self._journal._authorize()
        if not isinstance(reservation_limits, CycleReservationLimits):
            raise TypeError("reservation_limits must be CycleReservationLimits")
        cycle_id, work_item = _canonical_task(
            task,
            cycle_number=cycle_number,
        )
        work_item["proposal"] = canonical_campaign_proposal(
            work_item["proposal"]
        )
        if not isinstance(execution_spec, ExecutionSpec):
            raise TypeError("execution_spec must be an ExecutionSpec")
        require_protocol_conformant(execution_spec)
        canonical_roster = _roster_manifest(cycle_id, roster_members)
        protocol_roster = tuple(
            sorted(
                (
                    member.role,
                    member.provider_profile_id,
                    member.model_id,
                )
                for member in execution_spec.protocol.roster
            )
        )
        operational_roster = tuple(
            sorted(
                (member.role, member.profile, member.model)
                for member in canonical_roster.members
            )
        )
        if operational_roster != protocol_roster:
            raise ValueError("ExecutionSpec roster conflicts with roster_members")
        if cycle_number > 1:
            _SqliteUnitOfWork(stores._operational_spec())._read(
                lambda connection: (
                    self._require_prior_cycle_continuation_in_transaction(
                        connection,
                        cycle_number=cycle_number,
                    )
                )
            )
        self._lifecycle.activate()
        reservation_id = self._reservation_id(cycle_id)

        def reserve_and_open(connection):
            self._require_prior_cycle_continuation_in_transaction(
                connection,
                cycle_number=cycle_number,
            )
            self._adopt_work_item_in_transaction(
                connection,
                cycle_id=cycle_id,
                payload=work_item,
            )
            self._cycle_budget._reserve_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            reservation = self._budget._reserve_in_transaction(
                connection,
                reservation_id=reservation_id,
                call_id=cycle_id,
                max_input_tokens=reservation_limits.max_input_tokens,
                max_output_tokens=reservation_limits.max_output_tokens,
                max_cost=reservation_limits.max_cost,
                max_wall_time_ms=reservation_limits.max_wall_time_ms,
                max_tool_attempts=reservation_limits.max_tool_attempts,
                max_data_exposures=reservation_limits.max_data_exposures,
                max_disk_growth_bytes=reservation_limits.max_disk_growth_bytes,
            )
            cycle = self._lifecycle._open_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                cycle_number=cycle_number,
            )
            if cycle.status in {
                CycleStatus.CREATED,
                CycleStatus.BUDGET_RESERVED,
            }:
                cycle = self._lifecycle._advance_cycle_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    expected_status=CycleStatus.CREATED,
                    next_status=CycleStatus.BUDGET_RESERVED,
                )
            return cycle, reservation

        cycle, reservation = _SqliteUnitOfWork(
            stores._operational_spec()
        )._write(reserve_and_open)
        context = self._context.prepare(
            cycle_id=cycle_id,
            proposal=work_item["proposal"],
            roles=tuple(member.role for member in canonical_roster.members),
        )
        roster = self._roster.freeze(
            cycle_id=cycle_id,
            members=canonical_roster.members,
        )
        frozen = self._freeze.freeze(
            cycle_id=cycle_id,
            proposal=work_item["proposal"],
            execution_spec=execution_spec,
            expected_roster=roster,
        )
        preparation_manifest_sha256 = self._record_cycle_preparation(
            cycle_id=cycle_id,
            expected_work_item=work_item,
            expected_reservation=reservation,
            expected_context=context,
            expected_roster=roster,
            expected_frozen=frozen,
        )
        return PreparedOperationalCycle(
            cycle_id=cycle_id,
            reservation=reservation,
            context=context,
            roster=roster,
            frozen=frozen,
            preparation_manifest_sha256=preparation_manifest_sha256,
        )

    def campaign_snapshot(self) -> CampaignSnapshot:
        return self._lifecycle.snapshot()

    def complete_campaign(self) -> CampaignSnapshot:
        """Complete a controller-managed Campaign after a durable STOP."""

        self._journal._authorize()

        def complete(connection) -> CampaignSnapshot:
            opened = self._lifecycle._opened_cycles(connection)
            cycles = tuple(
                sorted(
                    (
                        self._lifecycle._replay_cycle(
                            self._lifecycle._cycle_events(
                                connection,
                                opened_cycle.cycle_id,
                            )
                        )
                        for opened_cycle in opened
                    ),
                    key=lambda cycle: cycle.cycle_number,
                )
            )
            if not cycles or any(
                cycle.status is not CycleStatus.COMPLETED for cycle in cycles
            ):
                raise CampaignStateConflictError(
                    "Campaign has an incomplete Cycle"
                )
            if tuple(cycle.cycle_number for cycle in cycles) != tuple(
                range(1, len(cycles) + 1)
            ):
                raise CampaignStateConflictError(
                    "Campaign Cycle continuation chain is invalid"
                )
            decisions: list[OperationalNextCycleDecisionReceipt] = []
            for cycle in cycles:
                information_gain = (
                    self._stored_information_gain_receipt_in_transaction(
                        connection,
                        cycle_id=cycle.cycle_id,
                    )
                )
                decisions.append(
                    self._stored_next_cycle_decision_receipt_in_transaction(
                        connection,
                        cycle_id=cycle.cycle_id,
                        information_gain_receipt=information_gain,
                    )
                )
            for index, decision in enumerate(decisions[:-1]):
                successor = cycles[index + 1]
                if (
                    decision.decision != "CONTINUE"
                    or not decision.continuation_allowed
                    or decision.next_cycle_number
                    != successor.cycle_number
                ):
                    raise CampaignStateConflictError(
                        "Campaign Cycle continuation chain is invalid"
                    )
            final_decision = decisions[-1]
            if (
                final_decision.decision != "STOP"
                or final_decision.continuation_allowed
                or final_decision.next_cycle_number is not None
            ):
                raise CampaignStateConflictError(
                    "Campaign has an unconsumed continuation decision"
                )
            return self._lifecycle._complete_in_transaction(connection)

        return _SqliteUnitOfWork(stores._operational_spec())._write(complete)

    def start_execution(
        self,
        *,
        cycle_id: str,
        acquisition_id: str,
    ) -> ExecutingOperationalCycle:
        self._journal._authorize()
        frozen = self._freeze.snapshot(cycle_id=cycle_id)
        self._preparation_snapshot(cycle_id=cycle_id, frozen=frozen)
        lease = self._leases.acquire(
            cycle_id=cycle_id,
            acquisition_id=acquisition_id,
        )
        cycle = self._leases.advance_cycle(
            lease=lease,
            expected_status=CycleStatus.FROZEN,
            next_status=CycleStatus.EXECUTING,
        )
        return ExecutingOperationalCycle(cycle=cycle, lease=lease)

    def invoke_member_json(
        self,
        *,
        execution: ExecutingOperationalCycle,
        member_id: str,
        provider: object,
        prompt: object,
        limits: OperationalModelCallLimits,
    ) -> ExecutedOperationalModelCall:
        self._journal._authorize()
        if not isinstance(limits, OperationalModelCallLimits):
            raise TypeError("limits must be OperationalModelCallLimits")
        cycle_id = self._require_active_execution(execution)
        member_id = _identifier(member_id, "member_id")
        frozen = self._freeze.snapshot(cycle_id=cycle_id)
        preparation_manifest_sha256 = self._preparation_snapshot(
            cycle_id=cycle_id,
            frozen=frozen,
        )
        context = self._context.snapshot(cycle_id=cycle_id)
        manifest = _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._roster._replay(
                self._roster._events(connection, cycle_id)
            )
        )
        if (
            context.manifest_sha256 != frozen.context_manifest_sha256
            or manifest.manifest_sha256 != frozen.roster_manifest_sha256
        ):
            raise CampaignJournalError(
                "execution inputs conflict with the frozen preparation"
            )
        member = next(
            (
                candidate
                for candidate in manifest.members
                if candidate.member_id == member_id
            ),
            None,
        )
        if member is None:
            raise ValueError("member_id is not present in the frozen roster")
        if not callable(getattr(provider, "invoke", None)):
            raise TypeError("provider must expose a callable invoke method")
        provider_identity = tuple(
            getattr(provider, field_name, None)
            for field_name in (
                "provider_name",
                "profile",
                "model",
                "config_sha256",
                "capability_sha256",
            )
        )
        if any(type(value) is not str for value in provider_identity):
            raise TypeError("provider binding identity is invalid")
        if provider_identity != (
            member.provider,
            member.profile,
            member.model,
            member.config_sha256,
            member.capability_sha256,
        ):
            raise ValueError("provider binding conflicts with the frozen roster")
        prompt_text = _canonical_json_text(prompt, "operational prompt")
        if len(prompt_text.encode("utf-8")) > _MAX_OPERATIONAL_PROMPT_BYTES:
            raise ValueError("operational prompt exceeds the bounded size")
        if operational_prompt_sha256(prompt) != member.prompt_sha256:
            raise ValueError("prompt conflicts with the frozen roster")
        call_id = self._member_call_id(cycle_id, member_id)
        request = {
            "schema_version": "control_plane.operational_model_request.v1",
            "cycle_id": cycle_id,
            "call_id": call_id,
            "member_id": member_id,
            "role": member.role,
            "prompt": json.loads(prompt_text),
            "context_manifest_sha256": context.manifest_sha256,
            "messages": context.messages_for(member.role),
        }
        request_text = _canonical_json_text(
            request,
            "operational model request",
        )
        if len(request_text.encode("utf-8")) > _MAX_OPERATIONAL_REQUEST_BYTES:
            raise ValueError("operational model request exceeds the bounded size")
        frozen_request = json.loads(request_text)
        usage = OperationalUsageJournal(
            journal=self._journal,
            cycle_id=cycle_id,
        )
        fixed_identity = {
            "schema_version": "control_plane.operational_model_call.v1",
            "cycle_id": cycle_id,
            "call_id": call_id,
            "member_id": member_id,
            "role": member.role,
            "provider": member.provider,
            "profile": member.profile,
            "request_model": member.model,
            "prompt_sha256": member.prompt_sha256,
            "config_sha256": member.config_sha256,
            "capability_sha256": member.capability_sha256,
            "context_manifest_sha256": context.manifest_sha256,
            "roster_manifest_sha256": manifest.manifest_sha256,
            "preparation_manifest_sha256": preparation_manifest_sha256,
            "request_sha256": _controller_sha256(
                b"control_plane.operational_model_request.v1",
                frozen_request,
                "operational model request",
            ),
            "call_limits": limits.to_payload(),
        }
        replay = _SqliteUnitOfWork(stores._operational_spec())._write(
            lambda connection: self._begin_model_call_in_transaction(
                connection,
                execution=execution,
                cycle_id=cycle_id,
                call_id=call_id,
                fixed_identity=fixed_identity,
                usage=usage,
            )
        )
        if replay is _MODEL_CALL_IN_DOUBT_RESULT:
            raise CampaignJournalError(
                "operational model call is incomplete and in doubt"
            )
        if replay is _MODEL_CALL_BUDGET_EXCEEDED_RESULT:
            raise BudgetExceededError(
                "known usage exceeds its call limits"
            )
        if replay is not None:
            return replay
        started_monotonic_ns = self._safe_monotonic_ns()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=_FencedOperationalUsageJournal(
                    controller=self,
                    execution=execution,
                    delegate=usage,
                ),
                provider_name=member.provider,
                profile=member.profile,
                request_model=member.model,
            ),
            max_attempts=limits.max_attempts,
        )
        try:
            result = invocation.invoke_json_with_receipt(
                frozen_request,
                call_id=call_id,
            )
        except (
            InvalidModelResponseError,
            ModelInvocationProviderError,
            ModelInvocationTimeoutError,
            StreamingDisabledError,
        ) as error:
            failed_attempts = usage.list_attempts(call_id=call_id)
            if failed_attempts:
                try:
                    self._roster.verify_response(
                        cycle_id=cycle_id,
                        member_id=member_id,
                        usage_journal=usage,
                        call_id=call_id,
                        attempt_id=failed_attempts[-1].envelope.attempt_id,
                        _transaction_guard=lambda connection: (
                            self._require_active_execution_in_transaction(
                                connection,
                                execution,
                            )
                        ),
                    )
                except RosterDriftError as drift:
                    raise drift from error
            raise
        finished_monotonic_ns = self._safe_monotonic_ns()
        wall_time_ms = self._elapsed_wall_time_ms(
            started_monotonic_ns,
            finished_monotonic_ns,
        )
        output_json = _canonical_json_text(
            result.output,
            "operational model output",
        )
        if len(output_json.encode("utf-8")) > _MAX_OPERATIONAL_OUTPUT_BYTES:
            raise ValueError("operational model output exceeds the bounded size")
        attempts = usage.list_attempts(call_id=call_id)
        if (
            len(attempts) != result.attempt_count
            or attempts[-1].envelope.attempt_id != result.attempt_id
        ):
            raise CampaignJournalError(
                "logical invocation receipt conflicts with persisted usage"
            )
        try:
            self._require_known_model_call_usage_within_limits(
                usage_attempts=attempts,
                wall_time_ms=wall_time_ms,
                attempt_count=result.attempt_count,
                limits=limits,
            )
        except BudgetExceededError:
            self._block_model_call_budget_exceeded(
                execution=execution,
                cycle_id=cycle_id,
                call_id=call_id,
            )
            raise
        verified = self._roster.verify_response(
            cycle_id=cycle_id,
            member_id=member_id,
            usage_journal=usage,
            call_id=call_id,
            attempt_id=result.attempt_id,
            _transaction_guard=lambda connection: (
                self._require_active_execution_in_transaction(
                    connection,
                    execution,
                )
            ),
        )
        return self._record_model_call(
            execution=execution,
            cycle_id=cycle_id,
            call_id=call_id,
            fixed_identity=fixed_identity,
            output_json=output_json,
            attempt_id=result.attempt_id,
            attempt_count=result.attempt_count,
            wall_time_ms=wall_time_ms,
            usage=usage,
            expected_attempts=attempts,
            expected_verified=verified,
        )

    def complete_model_execution(
        self,
        *,
        execution: ExecutingOperationalCycle,
    ) -> OperationalExecutionUsage:
        self._journal._authorize()
        cycle_id = self._require_active_execution(execution)
        roster_snapshot = self._roster.snapshot(cycle_id=cycle_id)
        if roster_snapshot.member_ids != roster_snapshot.verified_member_ids:
            raise CampaignJournalError(
                "required roster responses are incomplete"
            )
        roster_completion = self._roster.complete_responses(
            cycle_id=cycle_id,
            _transaction_guard=lambda connection: (
                self._require_active_execution_in_transaction(
                    connection,
                    execution,
                )
            ),
        )
        frozen = self._freeze.snapshot(cycle_id=cycle_id)
        preparation_manifest_sha256 = self._preparation_snapshot(
            cycle_id=cycle_id,
            frozen=frozen,
        )
        context = self._context.snapshot(cycle_id=cycle_id)
        manifest = _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._roster._replay(
                self._roster._events(connection, cycle_id)
            )
        )
        return self._record_execution_usage(
            execution=execution,
            cycle_id=cycle_id,
            preparation_manifest_sha256=preparation_manifest_sha256,
            context=context,
            roster=manifest,
            roster_completion=roster_completion,
        )

    def record_model_evidence(
        self,
        *,
        execution: ExecutingOperationalCycle,
        member_id: str,
        evidence_adapter: EvidenceAdapter,
    ) -> OperationalEvidenceReceipt:
        """Evaluate one frozen model artifact and durably advance its Cycle."""

        self._journal._authorize()
        member_id = _identifier(member_id, "member_id")
        if type(evidence_adapter) is not EvidenceAdapter:
            raise TypeError("evidence_adapter must be an EvidenceAdapter")

        def record(connection) -> OperationalEvidenceReceipt:
            cycle_id, current_cycle = (
                self._require_evidence_execution_generation_in_transaction(
                    connection,
                    execution,
                )
            )
            (
                preparation_manifest_sha256,
                context,
                roster,
            ) = self._evidence_preparation_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            member = next(
                (
                    candidate
                    for candidate in roster.members
                    if candidate.member_id == member_id
                ),
                None,
            )
            if member is None:
                raise ValueError("member_id is not present in the frozen roster")
            usage = OperationalUsageJournal(
                journal=self._journal,
                cycle_id=cycle_id,
            )
            model_calls = tuple(
                self._model_call_for_member_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    member=candidate,
                    preparation_manifest_sha256=(
                        preparation_manifest_sha256
                    ),
                    context_manifest_sha256=context.manifest_sha256,
                    roster_manifest_sha256=roster.manifest_sha256,
                    usage=usage,
                )
                for candidate in roster.members
            )
            selected_call = next(
                model_call
                for model_call in model_calls
                if model_call.member_id == member_id
            )
            usage_event, usage_payload = (
                self._execution_usage_binding_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    preparation_manifest_sha256=(
                        preparation_manifest_sha256
                    ),
                    context=context,
                    roster=roster,
                    model_calls=model_calls,
                )
            )
            artifact = selected_call.output
            adapter_binding = evidence_adapter.binding_payload()
            adapter_manifest_sha256 = _controller_sha256(
                b"control_plane.operational_evidence_adapter.v1",
                adapter_binding,
                "operational evidence adapter",
            )
            evidence = evidence_adapter.evaluate(artifact)
            evidence_payload = _evidence_result_payload(evidence)
            artifact_sha256 = _controller_sha256(
                b"control_plane.operational_evidence_artifact.v1",
                artifact,
                "operational evidence artifact",
            )
            identity = {
                "schema_version": "control_plane.operational_model_evidence.v1",
                "cycle_id": cycle_id,
                "member_id": member_id,
                "preparation_manifest_sha256": (
                    preparation_manifest_sha256
                ),
                "execution_usage_manifest_sha256": usage_payload[
                    "manifest_sha256"
                ],
                "model_call_manifest_sha256": (
                    selected_call.manifest_sha256
                ),
                "artifact_sha256": artifact_sha256,
                "adapter_manifest_sha256": adapter_manifest_sha256,
                "evidence": evidence_payload,
            }
            manifest_sha256 = _controller_sha256(
                b"control_plane.operational_model_evidence.v1",
                identity,
                "operational model evidence",
            )
            payload = {**identity, "manifest_sha256": manifest_sha256}
            receipt = OperationalEvidenceReceipt(
                cycle_id=cycle_id,
                member_id=member_id,
                preparation_manifest_sha256=(
                    preparation_manifest_sha256
                ),
                execution_usage_manifest_sha256=usage_payload[
                    "manifest_sha256"
                ],
                model_call_manifest_sha256=(
                    selected_call.manifest_sha256
                ),
                artifact_sha256=artifact_sha256,
                adapter_manifest_sha256=adapter_manifest_sha256,
                evidence=evidence,
                manifest_sha256=manifest_sha256,
                event_id=self._model_evidence_event_id(cycle_id),
            )
            events = self._model_evidence_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            if events:
                if (
                    len(events) != 1
                    or events[0].event_id != receipt.event_id
                    or events[0].event_type != _MODEL_EVIDENCE_RECORDED
                    or _canonical_json_text(
                        _event_domain_payload(events[0]),
                        "stored operational model evidence",
                    )
                    != _canonical_json_text(
                        payload,
                        "expected operational model evidence",
                    )
                    or events[0].sequence <= usage_event.sequence
                    or current_cycle.status is not CycleStatus.EVIDENCE_READY
                    or current_cycle.sequence <= events[0].sequence
                ):
                    raise CampaignJournalError(
                        "operational model evidence conflicts"
                    )
                return receipt
            if current_cycle.status is not CycleStatus.EXECUTING:
                raise CampaignJournalError(
                    "operational model evidence is missing"
                )
            event = self._journal._append_in_transaction(
                connection,
                event_id=receipt.event_id,
                cycle_id=cycle_id,
                aggregate_type=_MODEL_EVIDENCE_AGGREGATE_TYPE,
                aggregate_id=cycle_id,
                event_type=_MODEL_EVIDENCE_RECORDED,
                payload=payload,
            )
            if event.sequence <= usage_event.sequence:
                raise CampaignJournalError(
                    "operational evidence must follow its frozen usage"
                )
            advanced = self._lifecycle._advance_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                expected_status=CycleStatus.EXECUTING,
                next_status=CycleStatus.EVIDENCE_READY,
            )
            if advanced.sequence <= event.sequence:
                raise CampaignJournalError(
                    "EVIDENCE_READY must follow operational evidence"
                )
            if event.event_id != receipt.event_id:
                raise CampaignJournalError(
                    "operational model evidence event identity conflicts"
                )
            return receipt

        return _SqliteUnitOfWork(stores._operational_spec())._write(record)

    def commit_learning(
        self,
        *,
        execution: ExecutingOperationalCycle,
        evidence_receipt: OperationalEvidenceReceipt,
        authority_task_report: Mapping[str, object],
        learning_commit_sink: CampaignLearningCommitSink,
    ) -> OperationalLearningCommitReceipt:
        """Project eligible evidence and advance one fenced Cycle."""

        self._journal._authorize()
        if type(evidence_receipt) is not OperationalEvidenceReceipt:
            raise TypeError(
                "evidence_receipt must be an OperationalEvidenceReceipt"
            )
        if not isinstance(authority_task_report, Mapping):
            raise TypeError("authority_task_report must be a mapping")
        if type(learning_commit_sink) is not CampaignLearningCommitSink:
            raise TypeError(
                "learning_commit_sink must be a CampaignLearningCommitSink"
            )
        if learning_commit_sink._journal is not self._journal:
            raise ValueError(
                "learning_commit_sink must use the same Campaign journal"
            )
        learning_service = learning_commit_sink._service
        if type(learning_service) is not LearningCommitService:
            raise TypeError(
                "learning_commit_sink must use the formal LearningCommitService"
            )
        if learning_service._root != self._context._repository_root:
            raise ValueError(
                "learning_commit_sink must use the same repository root"
            )
        self._journal.require_formal_learning_sink()
        report_text = _canonical_json_text(
            dict(authority_task_report),
            "Authority TaskReport",
        )
        if len(report_text.encode("utf-8")) > _MAX_OPERATIONAL_REQUEST_BYTES:
            raise ValueError("Authority TaskReport exceeds the bounded size")
        frozen_report = json.loads(report_text)
        authority_task_report_sha256 = _controller_sha256(
            b"control_plane.operational_learning_task_report.v1",
            frozen_report,
            "Authority TaskReport",
        )

        def prepare_intent(connection) -> str:
            cycle_id, current_cycle, frozen_artifact, _ = (
                self._learning_commit_state_in_transaction(
                    connection,
                    execution=execution,
                    evidence_receipt=evidence_receipt,
                )
            )
            intent_events = (
                self._learning_commit_intent_events_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                )
            )
            if len(intent_events) > 1:
                raise CampaignJournalError(
                    "operational Learning Commit intent conflicts"
                )
            if not intent_events and self._journal._event_in_transaction(
                connection,
                self._learning_commit_intent_event_id(cycle_id),
            ) is not None:
                raise CampaignJournalError(
                    "operational Learning Commit intent conflicts"
                )
            expected_packet_hash = _stored_sha256(
                LearningCommitService.expected_packet_hash(
                    learning_service,
                    frozen_report,
                    expected_artifact=frozen_artifact,
                    expected_evidence=evidence_receipt.evidence,
                ),
                "expected Learning packet hash",
            )
            intent_payload = self._learning_commit_intent_payload(
                cycle_id=cycle_id,
                evidence_receipt=evidence_receipt,
                authority_task_report_sha256=(
                    authority_task_report_sha256
                ),
                packet_hash=expected_packet_hash,
            )
            if intent_events:
                if (
                    len(intent_events) != 1
                    or intent_events[0].event_id
                    != self._learning_commit_intent_event_id(cycle_id)
                    or intent_events[0].event_type
                    != _LEARNING_COMMIT_INTENT_RECORDED
                    or _canonical_json_text(
                        _event_domain_payload(intent_events[0]),
                        "stored operational Learning Commit intent",
                    )
                    != _canonical_json_text(
                        intent_payload,
                        "expected operational Learning Commit intent",
                    )
                ):
                    raise CampaignJournalError(
                        "operational Learning Commit intent conflicts"
                    )
                intent_event = intent_events[0]
            else:
                if current_cycle.status is CycleStatus.LEARNING_COMMITTED:
                    raise CampaignJournalError(
                        "operational Learning Commit intent conflicts"
                    )
                intent_event = self._journal._append_in_transaction(
                    connection,
                    event_id=self._learning_commit_intent_event_id(cycle_id),
                    cycle_id=cycle_id,
                    aggregate_type=(
                        _LEARNING_COMMIT_INTENT_AGGREGATE_TYPE
                    ),
                    aggregate_id=cycle_id,
                    event_type=_LEARNING_COMMIT_INTENT_RECORDED,
                    payload=intent_payload,
                )
            evidence_events = self._model_evidence_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            if (
                len(evidence_events) != 1
                or intent_event.sequence <= evidence_events[0].sequence
                or (
                    current_cycle.status is CycleStatus.EVIDENCE_READY
                    and intent_event.sequence <= current_cycle.sequence
                )
            ):
                raise CampaignJournalError(
                    "Learning Commit intent must follow operational evidence"
                )
            return expected_packet_hash

        expected_packet_hash = _SqliteUnitOfWork(
            stores._operational_spec()
        )._write(prepare_intent)

        def record(connection) -> OperationalLearningCommitReceipt:
            cycle_id, current_cycle, frozen_artifact, events = (
                self._learning_commit_state_in_transaction(
                    connection,
                    execution=execution,
                    evidence_receipt=evidence_receipt,
                )
            )
            if current_cycle.status is CycleStatus.LEARNING_COMMITTED:
                stored_payload = _event_domain_payload(events[0])
                stored_identity = {
                    key: value
                    for key, value in stored_payload.items()
                    if key != "manifest_sha256"
                }
                if (
                    set(stored_payload)
                    != {
                        "schema_version",
                        "cycle_id",
                        "member_id",
                        "evidence_manifest_sha256",
                        "authority_task_report_sha256",
                        "packet_hash",
                        "manifest_sha256",
                    }
                    or stored_payload["schema_version"]
                    != "control_plane.operational_learning_commit.v1"
                    or stored_payload["cycle_id"] != cycle_id
                    or stored_payload["member_id"]
                    != evidence_receipt.member_id
                    or stored_payload["evidence_manifest_sha256"]
                    != evidence_receipt.manifest_sha256
                    or stored_payload["authority_task_report_sha256"]
                    != authority_task_report_sha256
                    or stored_payload["manifest_sha256"]
                    != _controller_sha256(
                        b"control_plane.operational_learning_commit.v1",
                        stored_identity,
                        "stored operational Learning Commit",
                    )
                ):
                    raise CampaignJournalError(
                        "operational Learning Commit conflicts"
                    )
                _stored_sha256(
                    stored_payload["packet_hash"],
                    "stored Learning packet hash",
                )
            intent_payload = self._learning_commit_intent_payload(
                cycle_id=cycle_id,
                evidence_receipt=evidence_receipt,
                authority_task_report_sha256=(
                    authority_task_report_sha256
                ),
                packet_hash=expected_packet_hash,
            )
            intent_events = (
                self._learning_commit_intent_events_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                )
            )
            if (
                len(intent_events) != 1
                or intent_events[0].event_id
                != self._learning_commit_intent_event_id(cycle_id)
                or intent_events[0].event_type
                != _LEARNING_COMMIT_INTENT_RECORDED
                or _canonical_json_text(
                    _event_domain_payload(intent_events[0]),
                    "stored operational Learning Commit intent",
                )
                != _canonical_json_text(
                    intent_payload,
                    "expected operational Learning Commit intent",
                )
            ):
                raise CampaignJournalError(
                    "operational Learning Commit intent conflicts"
                )
            intent_event = intent_events[0]
            packet_hash = _stored_sha256(
                LearningCommitService.commit(
                    learning_service,
                    frozen_report,
                    expected_artifact=frozen_artifact,
                    expected_evidence=evidence_receipt.evidence,
                ),
                "Learning packet hash",
            )
            if packet_hash != expected_packet_hash:
                raise CampaignJournalError(
                    "Learning packet hash differs from its durable intent"
                )
            identity = {
                "schema_version": "control_plane.operational_learning_commit.v1",
                "cycle_id": cycle_id,
                "member_id": evidence_receipt.member_id,
                "evidence_manifest_sha256": (
                    evidence_receipt.manifest_sha256
                ),
                "authority_task_report_sha256": (
                    authority_task_report_sha256
                ),
                "packet_hash": packet_hash,
            }
            manifest_sha256 = _controller_sha256(
                b"control_plane.operational_learning_commit.v1",
                identity,
                "operational Learning Commit",
            )
            payload = {**identity, "manifest_sha256": manifest_sha256}
            receipt = OperationalLearningCommitReceipt(
                cycle_id=cycle_id,
                member_id=evidence_receipt.member_id,
                evidence_manifest_sha256=evidence_receipt.manifest_sha256,
                authority_task_report_sha256=(
                    authority_task_report_sha256
                ),
                packet_hash=packet_hash,
                manifest_sha256=manifest_sha256,
                event_id=self._learning_commit_event_id(cycle_id),
            )
            if events:
                if (
                    len(events) != 1
                    or events[0].event_id != receipt.event_id
                    or events[0].event_type != _LEARNING_COMMIT_RECORDED
                    or _canonical_json_text(
                        _event_domain_payload(events[0]),
                        "stored operational Learning Commit",
                    )
                    != _canonical_json_text(
                        payload,
                        "expected operational Learning Commit",
                    )
                ):
                    raise CampaignJournalError(
                        "operational Learning Commit conflicts"
                    )
                event = events[0]
            else:
                event = self._journal._append_in_transaction(
                    connection,
                    event_id=receipt.event_id,
                    cycle_id=cycle_id,
                    aggregate_type=_LEARNING_COMMIT_AGGREGATE_TYPE,
                    aggregate_id=cycle_id,
                    event_type=_LEARNING_COMMIT_RECORDED,
                    payload=payload,
                )
            evidence_events = self._model_evidence_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            if (
                len(evidence_events) != 1
                or event.sequence
                <= max(
                    evidence_events[0].sequence,
                    intent_event.sequence,
                )
            ):
                raise CampaignJournalError(
                    "Learning Commit must follow its intent and evidence"
                )
            advanced = self._lifecycle._advance_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                expected_status=CycleStatus.EVIDENCE_READY,
                next_status=CycleStatus.LEARNING_COMMITTED,
            )
            if advanced.sequence <= event.sequence:
                raise CampaignJournalError(
                    "LEARNING_COMMITTED must follow its receipt"
                )
            return receipt

        return _SqliteUnitOfWork(stores._operational_spec())._write(record)

    def settle_cycle(
        self,
        *,
        execution: ExecutingOperationalCycle,
        execution_usage: OperationalExecutionUsage,
        learning_commit_receipt: OperationalLearningCommitReceipt,
    ) -> OperationalCycleSettlementReceipt:
        """Settle one learned Cycle against its frozen execution usage."""

        self._journal._authorize()
        if type(execution_usage) is not OperationalExecutionUsage:
            raise TypeError(
                "execution_usage must be an OperationalExecutionUsage"
            )
        if (
            type(learning_commit_receipt)
            is not OperationalLearningCommitReceipt
        ):
            raise TypeError(
                "learning_commit_receipt must be an "
                "OperationalLearningCommitReceipt"
            )

        def settle(connection) -> OperationalCycleSettlementReceipt:
            cycle_id, current_cycle = (
                self._require_evidence_execution_generation_in_transaction(
                    connection,
                    execution,
                )
            )
            if current_cycle.status not in {
                CycleStatus.LEARNING_COMMITTED,
                CycleStatus.SETTLED,
            }:
                raise CampaignJournalError(
                    "Cycle settlement requires LEARNING_COMMITTED"
                )
            usage_event, replayed_usage = (
                self._replay_execution_usage_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    receipt=execution_usage,
                    allow_settled_reservation=(
                        current_cycle.status is CycleStatus.SETTLED
                    ),
                )
            )
            learning_event = (
                self._replay_learning_commit_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    receipt=learning_commit_receipt,
                )
            )
            learning_transitions = tuple(
                event
                for event in self._lifecycle._cycle_events(
                    connection,
                    cycle_id,
                )
                if event.event_type == _CYCLE_TRANSITIONED
                and _event_domain_payload(event).get("to_status")
                == CycleStatus.LEARNING_COMMITTED.value
            )
            if len(learning_transitions) != 1:
                raise CampaignJournalError(
                    "LEARNING_COMMITTED transition is missing or ambiguous"
                )
            learning_transition = learning_transitions[0]
            settlement_events = (
                self._cycle_settlement_events_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                )
            )
            if (
                current_cycle.status is CycleStatus.LEARNING_COMMITTED
                and settlement_events
            ) or (
                current_cycle.status is CycleStatus.SETTLED
                and (
                    len(settlement_events) != 1
                    or settlement_events[0].event_id
                    != self._cycle_settlement_event_id(cycle_id)
                    or settlement_events[0].event_type
                    != _CYCLE_SETTLEMENT_RECORDED
                )
            ):
                raise CampaignJournalError(
                    "operational Cycle settlement conflicts"
                )
            reservation_id = self._reservation_id(cycle_id)
            settlement = self._budget._settle_in_transaction(
                connection,
                reservation_id=reservation_id,
                input_tokens=replayed_usage.input_tokens,
                output_tokens=replayed_usage.output_tokens,
                cost=replayed_usage.cost,
                wall_time_ms=replayed_usage.wall_time_ms,
                tool_attempts=replayed_usage.tool_attempts,
                data_exposures=replayed_usage.data_exposures,
                disk_growth_bytes=replayed_usage.disk_growth_bytes,
            )
            budget_event_id = self._budget._event_id(
                "settle",
                reservation_id=reservation_id,
            )
            budget_event = next(
                (
                    event
                    for event in self._budget._events_in_transaction(
                        connection
                    )
                    if event.event_id == budget_event_id
                ),
                None,
            )
            if budget_event is None or budget_event.event_type != _BUDGET_SETTLED:
                raise CampaignJournalError(
                    "Cycle budget settlement event is missing"
                )
            identity = {
                "schema_version": "control_plane.operational_cycle_settlement.v1",
                "cycle_id": cycle_id,
                "reservation_id": reservation_id,
                "settlement_state": settlement.state,
                "execution_usage_manifest_sha256": (
                    replayed_usage.manifest_sha256
                ),
                "learning_commit_manifest_sha256": (
                    learning_commit_receipt.manifest_sha256
                ),
                "budget_settlement_event_id": budget_event.event_id,
            }
            manifest_sha256 = _controller_sha256(
                b"control_plane.operational_cycle_settlement.v1",
                identity,
                "operational Cycle settlement",
            )
            payload = {**identity, "manifest_sha256": manifest_sha256}
            receipt = OperationalCycleSettlementReceipt(
                cycle_id=cycle_id,
                reservation_id=reservation_id,
                settlement_state=settlement.state,
                execution_usage_manifest_sha256=(
                    replayed_usage.manifest_sha256
                ),
                learning_commit_manifest_sha256=(
                    learning_commit_receipt.manifest_sha256
                ),
                budget_settlement_event_id=budget_event.event_id,
                manifest_sha256=manifest_sha256,
                event_id=self._cycle_settlement_event_id(cycle_id),
            )
            if settlement_events:
                if (
                    len(settlement_events) != 1
                    or settlement_events[0].event_id != receipt.event_id
                    or settlement_events[0].event_type
                    != _CYCLE_SETTLEMENT_RECORDED
                    or _canonical_json_text(
                        _event_domain_payload(settlement_events[0]),
                        "stored operational Cycle settlement",
                    )
                    != _canonical_json_text(
                        payload,
                        "expected operational Cycle settlement",
                    )
                ):
                    raise CampaignJournalError(
                        "operational Cycle settlement conflicts"
                    )
                event = settlement_events[0]
            else:
                event = self._journal._append_in_transaction(
                    connection,
                    event_id=receipt.event_id,
                    cycle_id=cycle_id,
                    aggregate_type=_CYCLE_SETTLEMENT_AGGREGATE_TYPE,
                    aggregate_id=cycle_id,
                    event_type=_CYCLE_SETTLEMENT_RECORDED,
                    payload=payload,
                )
            if (
                usage_event.sequence >= learning_event.sequence
                or learning_transition.sequence <= learning_event.sequence
                or budget_event.sequence <= learning_transition.sequence
                or event.sequence <= budget_event.sequence
            ):
                raise CampaignJournalError(
                    "Cycle settlement event order conflicts"
                )
            advanced = self._lifecycle._advance_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                expected_status=CycleStatus.LEARNING_COMMITTED,
                next_status=CycleStatus.SETTLED,
            )
            if advanced.sequence <= event.sequence:
                raise CampaignJournalError(
                    "SETTLED must follow its Cycle settlement receipt"
                )
            return receipt

        return _SqliteUnitOfWork(stores._operational_spec())._write(settle)

    def settle_cycle_without_learning(
        self,
        *,
        execution: ExecutingOperationalCycle,
        execution_usage: OperationalExecutionUsage,
        evidence_receipt: OperationalEvidenceReceipt,
    ) -> OperationalNoLearningSettlementReceipt:
        """Settle one ineligible Cycle without fabricating Learning."""

        self._journal._authorize()
        if type(execution_usage) is not OperationalExecutionUsage:
            raise TypeError(
                "execution_usage must be an OperationalExecutionUsage"
            )
        if type(evidence_receipt) is not OperationalEvidenceReceipt:
            raise TypeError(
                "evidence_receipt must be an OperationalEvidenceReceipt"
            )

        def settle(connection) -> OperationalNoLearningSettlementReceipt:
            cycle_id, current_cycle = (
                self._require_evidence_execution_generation_in_transaction(
                    connection,
                    execution,
                )
            )
            if current_cycle.status not in {
                CycleStatus.EVIDENCE_READY,
                CycleStatus.SETTLED,
            }:
                raise CampaignJournalError(
                    "no-Learning settlement requires EVIDENCE_READY"
                )
            usage_event, replayed_usage = (
                self._replay_execution_usage_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    receipt=execution_usage,
                    allow_settled_reservation=(
                        current_cycle.status is CycleStatus.SETTLED
                    ),
                )
            )
            self._replay_evidence_receipt_in_transaction(
                connection,
                cycle_id=cycle_id,
                receipt=evidence_receipt,
                current_cycle=current_cycle,
                allow_settled_reservation=(
                    current_cycle.status is CycleStatus.SETTLED
                ),
            )
            evidence_event = self._model_evidence_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )[0]
            disposition_reason = self._no_learning_disposition_reason(
                evidence_receipt.evidence
            )
            if (
                self._learning_commit_intent_events_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                )
                or self._learning_commit_events_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                )
            ):
                raise CampaignJournalError(
                    "no-Learning disposition conflicts with Learning state"
                )

            disposition_identity = {
                "schema_version": (
                    "control_plane.operational_no_learning_disposition.v1"
                ),
                "cycle_id": cycle_id,
                "member_id": evidence_receipt.member_id,
                "evidence_manifest_sha256": evidence_receipt.manifest_sha256,
                "evidence_verdict": evidence_receipt.evidence.verdict,
                "scientific_outcome": (
                    evidence_receipt.evidence.scientific_outcome
                ),
                "disposition_reason": disposition_reason,
            }
            disposition_manifest_sha256 = _controller_sha256(
                b"control_plane.operational_no_learning_disposition.v1",
                disposition_identity,
                "operational no-Learning disposition",
            )
            disposition_payload = {
                **disposition_identity,
                "manifest_sha256": disposition_manifest_sha256,
            }
            disposition_event_id = self._no_learning_disposition_event_id(
                cycle_id
            )
            disposition_events = (
                self._no_learning_disposition_events_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                )
            )
            settlement_events = (
                self._cycle_settlement_events_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                )
            )
            if current_cycle.status is CycleStatus.EVIDENCE_READY:
                required_event_ids = (
                    disposition_event_id,
                    self._lifecycle._cycle_event_id(
                        cycle_id,
                        CycleStatus.LEARNING_SKIPPED.value,
                    ),
                    self._budget._event_id(
                        "settle",
                        reservation_id=self._reservation_id(cycle_id),
                    ),
                    self._cycle_settlement_event_id(cycle_id),
                    self._lifecycle._cycle_event_id(
                        cycle_id,
                        CycleStatus.SETTLED.value,
                    ),
                )
                if (
                    disposition_events
                    or settlement_events
                    or any(
                        self._journal._event_in_transaction(
                            connection,
                            event_id,
                        )
                        is not None
                        for event_id in required_event_ids
                    )
                ):
                    raise CampaignJournalError(
                        "operational no-Learning settlement conflicts"
                    )
                disposition_event = self._journal._append_in_transaction(
                    connection,
                    event_id=disposition_event_id,
                    cycle_id=cycle_id,
                    aggregate_type=(
                        _NO_LEARNING_DISPOSITION_AGGREGATE_TYPE
                    ),
                    aggregate_id=cycle_id,
                    event_type=_NO_LEARNING_DISPOSITION_RECORDED,
                    payload=disposition_payload,
                )
                skipped = self._lifecycle._advance_cycle_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    expected_status=CycleStatus.EVIDENCE_READY,
                    next_status=CycleStatus.LEARNING_SKIPPED,
                )
                skipped_sequence = skipped.sequence
            else:
                if (
                    len(disposition_events) != 1
                    or disposition_events[0].event_id
                    != disposition_event_id
                    or disposition_events[0].event_type
                    != _NO_LEARNING_DISPOSITION_RECORDED
                    or _canonical_json_text(
                        _event_domain_payload(disposition_events[0]),
                        "stored no-Learning disposition",
                    )
                    != _canonical_json_text(
                        disposition_payload,
                        "expected no-Learning disposition",
                    )
                ):
                    raise CampaignJournalError(
                        "operational no-Learning disposition conflicts"
                    )
                disposition_event = disposition_events[0]
                skipped_transitions = tuple(
                    event
                    for event in self._lifecycle._cycle_events(
                        connection,
                        cycle_id,
                    )
                    if event.event_type == _CYCLE_TRANSITIONED
                    and _event_domain_payload(event).get("to_status")
                    == CycleStatus.LEARNING_SKIPPED.value
                )
                if len(skipped_transitions) != 1:
                    raise CampaignJournalError(
                        "LEARNING_SKIPPED transition is missing or ambiguous"
                    )
                skipped_sequence = skipped_transitions[0].sequence

            reservation_id = self._reservation_id(cycle_id)
            settlement = self._budget._settle_in_transaction(
                connection,
                reservation_id=reservation_id,
                input_tokens=replayed_usage.input_tokens,
                output_tokens=replayed_usage.output_tokens,
                cost=replayed_usage.cost,
                wall_time_ms=replayed_usage.wall_time_ms,
                tool_attempts=replayed_usage.tool_attempts,
                data_exposures=replayed_usage.data_exposures,
                disk_growth_bytes=replayed_usage.disk_growth_bytes,
            )
            budget_event_id = self._budget._event_id(
                "settle",
                reservation_id=reservation_id,
            )
            budget_event = next(
                (
                    event
                    for event in self._budget._events_in_transaction(
                        connection
                    )
                    if event.event_id == budget_event_id
                ),
                None,
            )
            if budget_event is None or budget_event.event_type != _BUDGET_SETTLED:
                raise CampaignJournalError(
                    "Cycle budget settlement event is missing"
                )
            settlement_identity = {
                "schema_version": (
                    "control_plane.operational_no_learning_settlement.v1"
                ),
                "cycle_id": cycle_id,
                "reservation_id": reservation_id,
                "disposition_reason": disposition_reason,
                "evidence_manifest_sha256": evidence_receipt.manifest_sha256,
                "execution_usage_manifest_sha256": (
                    replayed_usage.manifest_sha256
                ),
                "settlement_state": settlement.state,
                "disposition_event_id": disposition_event.event_id,
                "budget_settlement_event_id": budget_event.event_id,
            }
            manifest_sha256 = _controller_sha256(
                b"control_plane.operational_no_learning_settlement.v1",
                settlement_identity,
                "operational no-Learning settlement",
            )
            settlement_payload = {
                **settlement_identity,
                "manifest_sha256": manifest_sha256,
            }
            receipt = OperationalNoLearningSettlementReceipt(
                cycle_id=cycle_id,
                reservation_id=reservation_id,
                disposition_reason=disposition_reason,
                evidence_manifest_sha256=evidence_receipt.manifest_sha256,
                execution_usage_manifest_sha256=(
                    replayed_usage.manifest_sha256
                ),
                settlement_state=settlement.state,
                disposition_event_id=disposition_event.event_id,
                budget_settlement_event_id=budget_event.event_id,
                manifest_sha256=manifest_sha256,
                event_id=self._cycle_settlement_event_id(cycle_id),
            )
            if settlement_events:
                if (
                    len(settlement_events) != 1
                    or settlement_events[0].event_id != receipt.event_id
                    or settlement_events[0].event_type
                    != _CYCLE_SETTLEMENT_RECORDED
                    or _canonical_json_text(
                        _event_domain_payload(settlement_events[0]),
                        "stored no-Learning Cycle settlement",
                    )
                    != _canonical_json_text(
                        settlement_payload,
                        "expected no-Learning Cycle settlement",
                    )
                ):
                    raise CampaignJournalError(
                        "operational no-Learning settlement conflicts"
                    )
                settlement_event = settlement_events[0]
            else:
                settlement_event = self._journal._append_in_transaction(
                    connection,
                    event_id=receipt.event_id,
                    cycle_id=cycle_id,
                    aggregate_type=_CYCLE_SETTLEMENT_AGGREGATE_TYPE,
                    aggregate_id=cycle_id,
                    event_type=_CYCLE_SETTLEMENT_RECORDED,
                    payload=settlement_payload,
                )

            if current_cycle.status is CycleStatus.EVIDENCE_READY:
                advanced = self._lifecycle._advance_cycle_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    expected_status=CycleStatus.LEARNING_SKIPPED,
                    next_status=CycleStatus.SETTLED,
                )
                settled_sequence = advanced.sequence
            else:
                settled_transitions = tuple(
                    event
                    for event in self._lifecycle._cycle_events(
                        connection,
                        cycle_id,
                    )
                    if event.event_type == _CYCLE_TRANSITIONED
                    and _event_domain_payload(event).get("to_status")
                    == CycleStatus.SETTLED.value
                )
                if len(settled_transitions) != 1:
                    raise CampaignJournalError(
                        "SETTLED transition is missing or ambiguous"
                    )
                settled_sequence = settled_transitions[0].sequence
            if (
                usage_event.sequence >= evidence_event.sequence
                or disposition_event.sequence <= evidence_event.sequence
                or skipped_sequence <= disposition_event.sequence
                or budget_event.sequence <= skipped_sequence
                or settlement_event.sequence <= budget_event.sequence
                or settled_sequence <= settlement_event.sequence
            ):
                raise CampaignJournalError(
                    "no-Learning Cycle settlement event order conflicts"
                )
            return receipt

        return _SqliteUnitOfWork(stores._operational_spec())._write(settle)

    def record_information_gain(
        self,
        *,
        execution: ExecutingOperationalCycle,
        settlement_receipt: (
            OperationalCycleSettlementReceipt
            | OperationalNoLearningSettlementReceipt
            | None
        ) = None,
    ) -> OperationalInformationGainReceipt:
        """Record controller-derived information gain for one settled Cycle."""

        self._journal._authorize()
        if settlement_receipt is not None and type(settlement_receipt) not in {
            OperationalCycleSettlementReceipt,
            OperationalNoLearningSettlementReceipt,
        }:
            raise TypeError(
                "settlement_receipt must be a formal operational settlement"
            )

        def record(connection) -> OperationalInformationGainReceipt:
            cycle_id, current_cycle = (
                self._require_evidence_execution_generation_in_transaction(
                    connection,
                    execution,
                    allow_information_gain_recorded=True,
                )
            )
            if current_cycle.status not in {
                CycleStatus.SETTLED,
                CycleStatus.INFORMATION_GAIN_RECORDED,
            }:
                raise CampaignJournalError(
                    "information gain requires SETTLED"
                )
            resolved_settlement = (
                settlement_receipt
                if settlement_receipt is not None
                else self._stored_settlement_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                )
            )
            if type(resolved_settlement) is OperationalCycleSettlementReceipt:
                packet_hash, settled_sequence = (
                    self._replay_learned_settlement_receipt_in_transaction(
                        connection,
                        cycle_id=cycle_id,
                        receipt=resolved_settlement,
                    )
                )
                information_gain_status = "ELIGIBLE_LEARNING_COMMITTED"
                continuation_eligible = True
                disposition_reason = None
            else:
                disposition_reason, settled_sequence = (
                    self._replay_no_learning_settlement_receipt_in_transaction(
                        connection,
                        cycle_id=cycle_id,
                        receipt=resolved_settlement,
                    )
                )
                packet_hash = None
                if disposition_reason in {
                    "NO_MATERIAL_FINDING",
                    "MATERIAL_UNAPPROVED",
                    "RESEARCH_ONLY",
                }:
                    information_gain_status = disposition_reason
                else:
                    information_gain_status = "INELIGIBLE_EVIDENCE"
                continuation_eligible = False
            identity = {
                "schema_version": (
                    "control_plane.operational_information_gain.v1"
                ),
                "cycle_id": cycle_id,
                "information_gain_status": information_gain_status,
                "continuation_eligible": continuation_eligible,
                "settlement_manifest_sha256": (
                    resolved_settlement.manifest_sha256
                ),
                "learning_packet_hash": packet_hash,
                "disposition_reason": disposition_reason,
            }
            manifest_sha256 = _controller_sha256(
                b"control_plane.operational_information_gain.v1",
                identity,
                "operational information gain",
            )
            payload = {**identity, "manifest_sha256": manifest_sha256}
            receipt = OperationalInformationGainReceipt(
                cycle_id=cycle_id,
                information_gain_status=information_gain_status,
                continuation_eligible=continuation_eligible,
                settlement_manifest_sha256=(
                    resolved_settlement.manifest_sha256
                ),
                learning_packet_hash=packet_hash,
                disposition_reason=disposition_reason,
                manifest_sha256=manifest_sha256,
                event_id=self._information_gain_event_id(cycle_id),
            )
            events = self._information_gain_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            if current_cycle.status is CycleStatus.SETTLED:
                if (
                    events
                    or self._journal._event_in_transaction(
                        connection,
                        receipt.event_id,
                    )
                    is not None
                    or self._journal._event_in_transaction(
                        connection,
                        self._lifecycle._cycle_event_id(
                            cycle_id,
                            CycleStatus.INFORMATION_GAIN_RECORDED.value,
                        ),
                    )
                    is not None
                ):
                    raise CampaignJournalError(
                        "operational information gain conflicts"
                    )
                event = self._journal._append_in_transaction(
                    connection,
                    event_id=receipt.event_id,
                    cycle_id=cycle_id,
                    aggregate_type=_INFORMATION_GAIN_AGGREGATE_TYPE,
                    aggregate_id=cycle_id,
                    event_type=_INFORMATION_GAIN_RECORDED,
                    payload=payload,
                )
            else:
                if (
                    len(events) != 1
                    or events[0].event_id != receipt.event_id
                    or events[0].event_type != _INFORMATION_GAIN_RECORDED
                    or _canonical_json_text(
                        _event_domain_payload(events[0]),
                        "stored operational information gain",
                    )
                    != _canonical_json_text(
                        payload,
                        "expected operational information gain",
                    )
                ):
                    raise CampaignJournalError(
                        "operational information gain conflicts"
                    )
                event = events[0]
            if event.sequence <= settled_sequence:
                raise CampaignJournalError(
                    "information gain must follow SETTLED"
                )
            advanced = self._lifecycle._advance_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                expected_status=CycleStatus.SETTLED,
                next_status=CycleStatus.INFORMATION_GAIN_RECORDED,
            )
            if advanced.sequence <= event.sequence:
                raise CampaignJournalError(
                    "INFORMATION_GAIN_RECORDED must follow its receipt"
                )
            return receipt

        return _SqliteUnitOfWork(stores._operational_spec())._write(record)

    def decide_next_cycle(
        self,
        *,
        execution: ExecutingOperationalCycle,
        information_gain_receipt: OperationalInformationGainReceipt | None = None,
    ) -> OperationalNextCycleDecisionReceipt:
        """Record the mechanical continuation decision and complete a Cycle."""

        self._journal._authorize()
        if (
            information_gain_receipt is not None
            and type(information_gain_receipt)
            is not OperationalInformationGainReceipt
        ):
            raise TypeError(
                "information_gain_receipt must be a formal operational "
                "information-gain receipt"
            )

        def decide(connection) -> OperationalNextCycleDecisionReceipt:
            cycle_id, current_cycle = (
                self._require_evidence_execution_generation_in_transaction(
                    connection,
                    execution,
                    allow_cycle_completed=True,
                )
            )
            if current_cycle.status not in {
                CycleStatus.INFORMATION_GAIN_RECORDED,
                CycleStatus.COMPLETED,
            }:
                raise CampaignJournalError(
                    "next-Cycle decision requires INFORMATION_GAIN_RECORDED"
                )
            resolved_information_gain = (
                information_gain_receipt
                if information_gain_receipt is not None
                else self._stored_information_gain_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                )
            )
            information_gain_sequence = (
                self._replay_information_gain_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    receipt=resolved_information_gain,
                )
            )
            events = self._next_cycle_decision_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            if current_cycle.status is CycleStatus.COMPLETED:
                return self._stored_next_cycle_decision_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    information_gain_receipt=resolved_information_gain,
                )
            cycle_budget = self._cycle_budget._snapshot_in_transaction(
                connection
            )
            self._require_cycle_budget_prefix(
                cycle_budget=cycle_budget,
                cycle_id=cycle_id,
                cycle_number=current_cycle.cycle_number,
            )
            reserved_cycle_count = len(cycle_budget.reserved_cycle_ids)
            (
                decision,
                continuation_allowed,
                reason_code,
                next_cycle_number,
            ) = self._derive_next_cycle_decision(
                information_gain_receipt=resolved_information_gain,
                cycle_number=current_cycle.cycle_number,
                reserved_cycle_count=reserved_cycle_count,
                max_cycles=cycle_budget.max_cycles,
            )
            identity = {
                "schema_version": (
                    "control_plane.operational_next_cycle_decision.v1"
                ),
                "cycle_id": cycle_id,
                "decision": decision,
                "continuation_allowed": continuation_allowed,
                "reason_code": reason_code,
                "next_cycle_number": next_cycle_number,
                "information_gain_manifest_sha256": (
                    resolved_information_gain.manifest_sha256
                ),
                "cycle_budget_id": cycle_budget.budget_id,
                "reserved_cycle_count": reserved_cycle_count,
                "max_cycles": cycle_budget.max_cycles,
            }
            manifest_sha256 = _controller_sha256(
                b"control_plane.operational_next_cycle_decision.v1",
                identity,
                "operational next-Cycle decision",
            )
            payload = {**identity, "manifest_sha256": manifest_sha256}
            receipt = OperationalNextCycleDecisionReceipt(
                cycle_id=cycle_id,
                decision=decision,
                continuation_allowed=continuation_allowed,
                reason_code=reason_code,
                next_cycle_number=next_cycle_number,
                information_gain_manifest_sha256=(
                    resolved_information_gain.manifest_sha256
                ),
                cycle_budget_id=cycle_budget.budget_id,
                reserved_cycle_count=reserved_cycle_count,
                max_cycles=cycle_budget.max_cycles,
                manifest_sha256=manifest_sha256,
                event_id=self._next_cycle_decision_event_id(cycle_id),
            )
            if (
                events
                or self._journal._event_in_transaction(
                    connection,
                    receipt.event_id,
                )
                is not None
                or self._journal._event_in_transaction(
                    connection,
                    self._lifecycle._cycle_event_id(
                        cycle_id,
                        CycleStatus.NEXT_CYCLE_DECIDED.value,
                    ),
                )
                is not None
                or self._journal._event_in_transaction(
                    connection,
                    self._lifecycle._cycle_event_id(
                        cycle_id,
                        CycleStatus.COMPLETED.value,
                    ),
                )
                is not None
            ):
                raise CampaignJournalError(
                    "operational next-Cycle decision conflicts"
                )
            event = self._journal._append_in_transaction(
                connection,
                event_id=receipt.event_id,
                cycle_id=cycle_id,
                aggregate_type=_NEXT_CYCLE_DECISION_AGGREGATE_TYPE,
                aggregate_id=cycle_id,
                event_type=_NEXT_CYCLE_DECISION_RECORDED,
                payload=payload,
            )
            decided = self._lifecycle._advance_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                expected_status=CycleStatus.INFORMATION_GAIN_RECORDED,
                next_status=CycleStatus.NEXT_CYCLE_DECIDED,
            )
            completed = self._lifecycle._advance_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                expected_status=CycleStatus.NEXT_CYCLE_DECIDED,
                next_status=CycleStatus.COMPLETED,
            )
            if (
                event.sequence <= information_gain_sequence
                or decided.sequence <= event.sequence
                or completed.sequence <= decided.sequence
            ):
                raise CampaignJournalError(
                    "next-Cycle decision event order conflicts"
                )
            return receipt

        return _SqliteUnitOfWork(stores._operational_spec())._write(decide)

    @staticmethod
    def _derive_next_cycle_decision(
        *,
        information_gain_receipt: OperationalInformationGainReceipt,
        cycle_number: int,
        reserved_cycle_count: int,
        max_cycles: int,
    ) -> tuple[str, bool, str, int | None]:
        continuation_allowed = (
            information_gain_receipt.continuation_eligible
            and reserved_cycle_count < max_cycles
            and cycle_number < 1_000_000
        )
        if continuation_allowed:
            return (
                "CONTINUE",
                True,
                "CONTINUATION_ELIGIBLE",
                cycle_number + 1,
            )
        if not information_gain_receipt.continuation_eligible:
            reason_code = "INFORMATION_GAIN_INELIGIBLE"
        elif reserved_cycle_count >= max_cycles:
            reason_code = "CYCLE_BUDGET_EXHAUSTED"
        else:
            reason_code = "CYCLE_NUMBER_EXHAUSTED"
        return "STOP", False, reason_code, None

    @staticmethod
    def _require_cycle_budget_prefix(
        *,
        cycle_budget: CycleBudgetSnapshot,
        cycle_id: str,
        cycle_number: int,
    ) -> None:
        if (
            len(cycle_budget.reserved_cycle_ids) != cycle_number
            or not cycle_budget.reserved_cycle_ids
            or cycle_budget.reserved_cycle_ids[-1] != cycle_id
        ):
            raise CampaignJournalError("Cycle budget prefix conflicts")

    def _require_prior_cycle_continuation_in_transaction(
        self,
        connection,
        *,
        cycle_number: int,
    ) -> None:
        if cycle_number == 1:
            return
        previous_cycles = tuple(
            opened
            for opened in self._lifecycle._opened_cycles(connection)
            if opened.cycle_number == cycle_number - 1
        )
        if len(previous_cycles) != 1:
            raise CampaignJournalError(
                "previous Cycle did not authorize continuation"
            )
        previous_cycle = self._lifecycle._replay_cycle(
            self._lifecycle._cycle_events(
                connection,
                previous_cycles[0].cycle_id,
            )
        )
        if previous_cycle.status is not CycleStatus.COMPLETED:
            raise CampaignJournalError(
                "previous Cycle did not authorize continuation"
            )
        information_gain = (
            self._stored_information_gain_receipt_in_transaction(
                connection,
                cycle_id=previous_cycle.cycle_id,
            )
        )
        decision = self._stored_next_cycle_decision_receipt_in_transaction(
            connection,
            cycle_id=previous_cycle.cycle_id,
            information_gain_receipt=information_gain,
        )
        if (
            decision.decision != "CONTINUE"
            or not decision.continuation_allowed
            or decision.next_cycle_number != cycle_number
        ):
            raise CampaignJournalError(
                "previous Cycle did not authorize continuation"
            )

    def cycle_snapshot(self, cycle_id: str) -> CycleSnapshot:
        return self._lifecycle.cycle_snapshot(cycle_id)

    def cycle_budget_snapshot(self) -> CycleBudgetSnapshot:
        return self._cycle_budget.snapshot()

    def budget_snapshot(self) -> BudgetSnapshot:
        return self._budget.snapshot()

    def _record_execution_usage(
        self,
        *,
        execution: ExecutingOperationalCycle,
        cycle_id: str,
        preparation_manifest_sha256: str,
        context: CycleContextReceipt,
        roster: RosterManifest,
        roster_completion: RosterCompletion,
    ) -> OperationalExecutionUsage:
        def record(connection) -> OperationalExecutionUsage:
            self._require_active_execution_in_transaction(
                connection,
                execution,
            )
            roster_events = self._roster._events(connection, cycle_id)
            roster_history = self._roster._replay_history(
                connection,
                roster_events,
            )
            completion_event = next(
                (
                    event
                    for event in roster_events
                    if event.event_id == roster_completion.event_id
                ),
                None,
            )
            if (
                roster_history.manifest != roster
                or roster_history.terminal_event_id != roster_completion.event_id
                or completion_event is None
            ):
                raise CampaignJournalError(
                    "execution usage roster completion is invalid"
                )
            persisted_context = self._context._replay_context(
                self._context._context_events(connection, cycle_id)
            )
            if persisted_context != context:
                raise CampaignJournalError(
                    "execution usage context binding is invalid"
                )
            expected_call_ids = tuple(
                self._member_call_id(cycle_id, member.member_id)
                for member in roster.members
            )
            stored_call_ids = tuple(
                str(row["aggregate_id"])
                for row in connection.execute(
                    "SELECT DISTINCT aggregate_id FROM campaign_events "
                    "WHERE namespace = ? AND campaign_id = ? "
                    "AND cycle_id = ? AND aggregate_type = ? "
                    "ORDER BY aggregate_id",
                    (
                        self._journal.namespace,
                        self._journal.campaign_id,
                        cycle_id,
                        _MODEL_CALL_AGGREGATE_TYPE,
                    ),
                )
            )
            if tuple(sorted(expected_call_ids)) != stored_call_ids:
                raise CampaignJournalError(
                    "execution usage model call inventory conflicts"
                )
            usage = OperationalUsageJournal(
                journal=self._journal,
                cycle_id=cycle_id,
            )
            model_calls = tuple(
                self._model_call_for_member_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    member=member,
                    preparation_manifest_sha256=(
                        preparation_manifest_sha256
                    ),
                    context_manifest_sha256=context.manifest_sha256,
                    roster_manifest_sha256=roster.manifest_sha256,
                    usage=usage,
                )
                for member in roster.members
            )
            all_attempts = usage._list_attempts_in_transaction(
                connection,
                call_id=None,
            )
            bound_attempts = tuple(
                attempt
                for model_call in model_calls
                for attempt in model_call.usage_attempts
            )
            attempt_keys = {
                (
                    attempt.envelope.call_id,
                    attempt.envelope.attempt_id,
                )
                for attempt in all_attempts
            }
            bound_keys = {
                (
                    attempt.envelope.call_id,
                    attempt.envelope.attempt_id,
                )
                for attempt in bound_attempts
            }
            if (
                len(all_attempts) != len(bound_attempts)
                or attempt_keys != bound_keys
            ):
                raise CampaignJournalError(
                    "execution usage attempt inventory conflicts"
                )

            identity = self._execution_usage_identity(
                cycle_id=cycle_id,
                preparation_manifest_sha256=(
                    preparation_manifest_sha256
                ),
                context_manifest_sha256=context.manifest_sha256,
                roster_manifest_sha256=roster.manifest_sha256,
                roster_completion_event_id=roster_completion.event_id,
                model_calls=model_calls,
                all_attempts=all_attempts,
            )
            usage_status = UsageStatus(str(identity["usage_status"]))
            input_tokens = identity["input_tokens"]
            output_tokens = identity["output_tokens"]
            cost = identity["cost"]
            currency = identity["currency"]
            wall_time_ms = identity["wall_time_ms"]
            manifest_sha256 = _controller_sha256(
                b"control_plane.operational_execution_usage.v1",
                identity,
                "operational execution usage",
            )
            payload = {**identity, "manifest_sha256": manifest_sha256}
            events = self._execution_usage_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            if events:
                if (
                    len(events) != 1
                    or events[0].event_id
                    != self._execution_usage_event_id(cycle_id)
                    or events[0].event_type != _EXECUTION_USAGE_FROZEN
                    or _canonical_json_text(
                        _event_domain_payload(events[0]),
                        "stored execution usage",
                    )
                    != _canonical_json_text(
                        payload,
                        "expected execution usage",
                    )
                ):
                    raise CampaignJournalError(
                        "operational execution usage conflicts"
                    )
                event = events[0]
            else:
                event = self._journal._append_in_transaction(
                    connection,
                    event_id=self._execution_usage_event_id(cycle_id),
                    cycle_id=cycle_id,
                    aggregate_type=_EXECUTION_USAGE_AGGREGATE_TYPE,
                    aggregate_id=cycle_id,
                    event_type=_EXECUTION_USAGE_FROZEN,
                    payload=payload,
                )
            call_events = tuple(
                self._model_call_events_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    call_id=model_call.call_id,
                )[-1]
                for model_call in model_calls
            )
            if event.sequence <= max(
                completion_event.sequence,
                *(call_event.sequence for call_event in call_events),
            ):
                raise CampaignJournalError(
                    "execution usage must follow its model calls and roster"
                )
            return OperationalExecutionUsage(
                cycle_id=cycle_id,
                usage_status=usage_status,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost=cost,
                currency=currency,
                wall_time_ms=wall_time_ms,
                tool_attempts=len(all_attempts),
                data_exposures=0,
                disk_growth_bytes=0,
                model_calls=model_calls,
                roster_completion=roster_completion,
                manifest_sha256=manifest_sha256,
                event_id=event.event_id,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(record)

    @staticmethod
    def _execution_usage_identity(
        *,
        cycle_id: str,
        preparation_manifest_sha256: str,
        context_manifest_sha256: str,
        roster_manifest_sha256: str,
        roster_completion_event_id: str,
        model_calls: tuple[ExecutedOperationalModelCall, ...],
        all_attempts: tuple[RecordedModelAttempt, ...],
    ) -> dict[str, object]:
        def sum_tokens(field_name: str) -> int | None:
            values = [
                getattr(attempt.envelope, field_name)
                for attempt in all_attempts
            ]
            if any(value is None for value in values):
                return None
            return sum(int(value) for value in values)

        input_tokens = sum_tokens("input_tokens")
        output_tokens = sum_tokens("output_tokens")
        reported_costs = [
            attempt.envelope.reported_cost for attempt in all_attempts
        ]
        currencies = {attempt.envelope.currency for attempt in all_attempts}
        if (
            any(value is None for value in reported_costs)
            or None in currencies
            or len(currencies) > 1
        ):
            cost = None
            currency = None
        else:
            cost = _decimal_text(
                sum(
                    (
                        Decimal(str(value))
                        for value in reported_costs
                    ),
                    Decimal("0"),
                )
            )
            currency = next(iter(currencies), None)
        wall_times = [model_call.wall_time_ms for model_call in model_calls]
        wall_time_ms = (
            None
            if any(value is None for value in wall_times)
            else sum(int(value) for value in wall_times)
        )
        attempt_statuses = {
            attempt.envelope.usage_status for attempt in all_attempts
        }
        if (
            UsageStatus.UNKNOWN in attempt_statuses
            or input_tokens is None
            or output_tokens is None
            or cost is None
            or wall_time_ms is None
        ):
            usage_status = UsageStatus.UNKNOWN
        elif UsageStatus.ESTIMATED in attempt_statuses:
            usage_status = UsageStatus.ESTIMATED
        else:
            usage_status = UsageStatus.REPORTED
        return {
            "schema_version": "control_plane.operational_execution_usage.v1",
            "cycle_id": cycle_id,
            "preparation_manifest_sha256": preparation_manifest_sha256,
            "context_manifest_sha256": context_manifest_sha256,
            "roster_manifest_sha256": roster_manifest_sha256,
            "roster_completion_event_id": roster_completion_event_id,
            "model_call_manifest_sha256s": [
                model_call.manifest_sha256 for model_call in model_calls
            ],
            "usage_status": usage_status.value,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": cost,
            "currency": currency,
            "wall_time_ms": wall_time_ms,
            "tool_attempts": len(all_attempts),
            "data_exposures": 0,
            "disk_growth_bytes": 0,
        }

    def _model_call_for_member_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        member: RosterMember,
        preparation_manifest_sha256: str,
        context_manifest_sha256: str,
        roster_manifest_sha256: str,
        usage: OperationalUsageJournal,
    ) -> ExecutedOperationalModelCall:
        call_id = self._member_call_id(cycle_id, member.member_id)
        events = self._model_call_events_in_transaction(
            connection,
            cycle_id=cycle_id,
            call_id=call_id,
        )
        if len(events) != 2:
            raise CampaignJournalError(
                "required operational model call is missing"
            )
        payload = _event_domain_payload(events[-1])
        request_sha256 = _stored_sha256(
            payload.get("request_sha256"),
            "stored request_sha256",
        )
        call_limits = self._model_call_limits_from_payload(
            payload.get("call_limits")
        ).to_payload()
        expected_identity = {
            "schema_version": "control_plane.operational_model_call.v1",
            "cycle_id": cycle_id,
            "call_id": call_id,
            "member_id": member.member_id,
            "role": member.role,
            "provider": member.provider,
            "profile": member.profile,
            "request_model": member.model,
            "prompt_sha256": member.prompt_sha256,
            "config_sha256": member.config_sha256,
            "capability_sha256": member.capability_sha256,
            "context_manifest_sha256": context_manifest_sha256,
            "roster_manifest_sha256": roster_manifest_sha256,
            "preparation_manifest_sha256": preparation_manifest_sha256,
            "request_sha256": request_sha256,
            "call_limits": call_limits,
        }
        model_call = self._replay_model_call_in_transaction(
            connection,
            cycle_id=cycle_id,
            call_id=call_id,
            expected_identity=expected_identity,
            usage=usage,
        )
        self._require_known_model_call_usage_within_limits(
            usage_attempts=model_call.usage_attempts,
            wall_time_ms=model_call.wall_time_ms,
            attempt_count=model_call.attempt_count,
            limits=self._model_call_limits_from_payload(call_limits),
        )
        return model_call

    @staticmethod
    def _require_known_model_call_usage_within_limits(
        *,
        usage_attempts: tuple[RecordedModelAttempt, ...],
        wall_time_ms: int | None,
        attempt_count: int,
        limits: OperationalModelCallLimits,
    ) -> None:
        def known_token_lower_bound(field_name: str) -> int:
            values = tuple(
                getattr(attempt.envelope, field_name)
                for attempt in usage_attempts
            )
            return sum(int(value) for value in values if value is not None)

        reported_costs = tuple(
            attempt.envelope.reported_cost
            for attempt in usage_attempts
        )
        known_cost_lower_bound = sum(
            (
                _bounded_cost(str(value))
                for value in reported_costs
                if value is not None
            ),
            Decimal("0"),
        )
        known_input_tokens = known_token_lower_bound("input_tokens")
        known_output_tokens = known_token_lower_bound("output_tokens")
        if (
            known_input_tokens > limits.max_input_tokens
            or known_output_tokens > limits.max_output_tokens
            or known_cost_lower_bound > _bounded_cost(limits.max_cost)
            or (
                wall_time_ms is not None
                and wall_time_ms > limits.max_wall_time_ms
            )
            or attempt_count > limits.max_attempts
        ):
            raise BudgetExceededError(
                "known usage exceeds its call limits"
            )

    def _block_model_call_budget_exceeded(
        self,
        *,
        execution: ExecutingOperationalCycle,
        cycle_id: str,
        call_id: str,
    ) -> None:
        def block(connection) -> None:
            self._require_active_execution_in_transaction(
                connection,
                execution,
            )
            events = self._model_call_events_in_transaction(
                connection,
                cycle_id=cycle_id,
                call_id=call_id,
            )
            if len(events) != 1 or events[0].event_type != _MODEL_CALL_STARTED:
                raise CampaignJournalError(
                    "model call budget violation has no active start intent"
                )
            self._lifecycle._block_in_transaction(
                connection,
                reason_code="MODEL_CALL_BUDGET_EXCEEDED",
                source_ref=events[0].event_id,
            )

        _SqliteUnitOfWork(stores._operational_spec())._write(block)

    def _execution_usage_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_execution_usage.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _execution_usage_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        rows = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _EXECUTION_USAGE_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_id"] != cycle_id
            for row in rows
        ):
            raise CampaignJournalError(
                "operational execution usage stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_EXECUTION_USAGE_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _evidence_preparation_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        allow_settled_reservation: bool = False,
    ) -> tuple[str, CycleContextReceipt, RosterManifest]:
        policy = self._freeze._replay_policy(
            self._freeze._policy_events(connection)
        )
        frozen = self._freeze._replay_freeze(
            self._freeze._freeze_events(connection, cycle_id)
        )
        self._freeze._require_complete_freeze_order(
            connection,
            policy,
            frozen,
        )
        identity, _, _, minimum_sequence = (
            self._controller_artifact_identity_in_transaction(
                connection,
                cycle_id=cycle_id,
                frozen=frozen,
                allow_settled_reservation=allow_settled_reservation,
            )
        )
        preparation_manifest_sha256 = _controller_sha256(
            b"control_plane.campaign_cycle_preparation.v1",
            identity,
            "Cycle preparation identity",
        )
        self._replay_preparation(
            cycle_id=cycle_id,
            events=self._preparation_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            ),
            expected_payload={
                **identity,
                "manifest_sha256": preparation_manifest_sha256,
            },
            minimum_sequence=minimum_sequence,
        )
        context = self._context._replay_context(
            self._context._context_events(connection, cycle_id)
        )
        roster = self._roster._replay(
            self._roster._events(connection, cycle_id)
        )
        if (
            context.manifest_sha256 != frozen.context_manifest_sha256
            or roster.manifest_sha256 != frozen.roster_manifest_sha256
        ):
            raise CampaignJournalError(
                "evidence inputs conflict with the frozen preparation"
            )
        return preparation_manifest_sha256, context, roster

    def _execution_usage_binding_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        preparation_manifest_sha256: str,
        context: CycleContextReceipt,
        roster: RosterManifest,
        model_calls: tuple[ExecutedOperationalModelCall, ...],
    ):
        events = self._execution_usage_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        if len(events) != 1:
            raise CampaignJournalError(
                "frozen operational execution usage is missing or ambiguous"
            )
        event = events[0]
        payload = _event_domain_payload(event)
        roster_events = self._roster._events(connection, cycle_id)
        roster_history = self._roster._replay_history(
            connection,
            roster_events,
        )
        stored_call_ids = tuple(
            str(row["aggregate_id"])
            for row in connection.execute(
                "SELECT DISTINCT aggregate_id FROM campaign_events "
                "WHERE namespace = ? AND campaign_id = ? "
                "AND cycle_id = ? AND aggregate_type = ? "
                "ORDER BY aggregate_id",
                (
                    self._journal.namespace,
                    self._journal.campaign_id,
                    cycle_id,
                    _MODEL_CALL_AGGREGATE_TYPE,
                ),
            )
        )
        expected_call_ids = tuple(
            sorted(model_call.call_id for model_call in model_calls)
        )
        if stored_call_ids != expected_call_ids:
            raise CampaignJournalError(
                "execution usage model call inventory conflicts"
            )
        all_attempts = OperationalUsageJournal(
            journal=self._journal,
            cycle_id=cycle_id,
        )._list_attempts_in_transaction(
            connection,
            call_id=None,
        )
        bound_attempts = tuple(
            attempt
            for model_call in model_calls
            for attempt in model_call.usage_attempts
        )
        if (
            len(all_attempts) != len(bound_attempts)
            or {
                (attempt.envelope.call_id, attempt.envelope.attempt_id)
                for attempt in all_attempts
            }
            != {
                (attempt.envelope.call_id, attempt.envelope.attempt_id)
                for attempt in bound_attempts
            }
        ):
            raise CampaignJournalError(
                "execution usage attempt inventory conflicts"
            )
        completion_event = next(
            (
                roster_event
                for roster_event in roster_events
                if roster_event.event_id
                == roster_history.terminal_event_id
            ),
            None,
        )
        call_events = tuple(
            self._model_call_events_in_transaction(
                connection,
                cycle_id=cycle_id,
                call_id=model_call.call_id,
            )[-1]
            for model_call in model_calls
        )
        if (
            completion_event is None
            or roster_history.terminal_event_id is None
            or roster_history.terminal_event_type
            != "ROSTER_RESPONSES_COMPLETED"
            or roster_history.verified_member_ids
            != frozenset(member.member_id for member in roster.members)
        ):
            raise CampaignJournalError(
                "frozen operational execution usage conflicts"
            )
        expected_identity = self._execution_usage_identity(
            cycle_id=cycle_id,
            preparation_manifest_sha256=preparation_manifest_sha256,
            context_manifest_sha256=context.manifest_sha256,
            roster_manifest_sha256=roster.manifest_sha256,
            roster_completion_event_id=roster_history.terminal_event_id,
            model_calls=model_calls,
            all_attempts=all_attempts,
        )
        expected_payload = {
            **expected_identity,
            "manifest_sha256": _controller_sha256(
                b"control_plane.operational_execution_usage.v1",
                expected_identity,
                "replayed operational execution usage",
            ),
        }
        if (
            event.event_id != self._execution_usage_event_id(cycle_id)
            or event.event_type != _EXECUTION_USAGE_FROZEN
            or _canonical_json_text(
                payload,
                "stored operational execution usage",
            )
            != _canonical_json_text(
                expected_payload,
                "replayed operational execution usage",
            )
            or event.sequence
            <= max(
                completion_event.sequence,
                *(call_event.sequence for call_event in call_events),
            )
        ):
            raise CampaignJournalError(
                "frozen operational execution usage conflicts"
            )
        return event, payload

    def _replay_evidence_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        receipt: OperationalEvidenceReceipt,
        current_cycle: CycleSnapshot,
        allow_settled_reservation: bool = False,
    ) -> object:
        (
            preparation_manifest_sha256,
            context,
            roster,
        ) = self._evidence_preparation_in_transaction(
            connection,
            cycle_id=cycle_id,
            allow_settled_reservation=allow_settled_reservation,
        )
        member = next(
            (
                candidate
                for candidate in roster.members
                if candidate.member_id == receipt.member_id
            ),
            None,
        )
        if member is None:
            raise CampaignJournalError(
                "evidence member is absent from the frozen roster"
            )
        usage = OperationalUsageJournal(
            journal=self._journal,
            cycle_id=cycle_id,
        )
        model_calls = tuple(
            self._model_call_for_member_in_transaction(
                connection,
                cycle_id=cycle_id,
                member=candidate,
                preparation_manifest_sha256=preparation_manifest_sha256,
                context_manifest_sha256=context.manifest_sha256,
                roster_manifest_sha256=roster.manifest_sha256,
                usage=usage,
            )
            for candidate in roster.members
        )
        selected_call = next(
            model_call
            for model_call in model_calls
            if model_call.member_id == receipt.member_id
        )
        usage_event, usage_payload = (
            self._execution_usage_binding_in_transaction(
                connection,
                cycle_id=cycle_id,
                preparation_manifest_sha256=preparation_manifest_sha256,
                context=context,
                roster=roster,
                model_calls=model_calls,
            )
        )
        artifact = selected_call.output
        expected_identity = {
            "schema_version": "control_plane.operational_model_evidence.v1",
            "cycle_id": cycle_id,
            "member_id": receipt.member_id,
            "preparation_manifest_sha256": preparation_manifest_sha256,
            "execution_usage_manifest_sha256": usage_payload[
                "manifest_sha256"
            ],
            "model_call_manifest_sha256": selected_call.manifest_sha256,
            "artifact_sha256": _controller_sha256(
                b"control_plane.operational_evidence_artifact.v1",
                artifact,
                "replayed operational evidence artifact",
            ),
            "adapter_manifest_sha256": receipt.adapter_manifest_sha256,
            "evidence": _evidence_result_payload(receipt.evidence),
        }
        manifest_sha256 = _controller_sha256(
            b"control_plane.operational_model_evidence.v1",
            expected_identity,
            "replayed operational model evidence",
        )
        expected_payload = {
            **expected_identity,
            "manifest_sha256": manifest_sha256,
        }
        events = self._model_evidence_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        if (
            receipt.cycle_id != cycle_id
            or receipt.preparation_manifest_sha256
            != preparation_manifest_sha256
            or receipt.execution_usage_manifest_sha256
            != usage_payload["manifest_sha256"]
            or receipt.model_call_manifest_sha256
            != selected_call.manifest_sha256
            or receipt.artifact_sha256 != expected_identity["artifact_sha256"]
            or receipt.manifest_sha256 != manifest_sha256
            or receipt.event_id != self._model_evidence_event_id(cycle_id)
            or len(events) != 1
            or events[0].event_id != receipt.event_id
            or events[0].event_type != _MODEL_EVIDENCE_RECORDED
            or _canonical_json_text(
                _event_domain_payload(events[0]),
                "stored operational model evidence",
            )
            != _canonical_json_text(
                expected_payload,
                "replayed operational model evidence",
            )
            or events[0].sequence <= usage_event.sequence
            or current_cycle.sequence <= events[0].sequence
        ):
            raise CampaignJournalError(
                "operational evidence receipt conflicts"
            )
        return artifact

    def _replay_stored_evidence_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        expected_manifest_sha256: str,
    ) -> tuple[object, OperationalEvidenceReceipt]:
        _stored_sha256(expected_manifest_sha256, "evidence manifest")
        events = self._model_evidence_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        payload = (
            _event_domain_payload(events[0])
            if len(events) == 1
            else {}
        )
        if set(payload) != {
            "schema_version",
            "cycle_id",
            "member_id",
            "preparation_manifest_sha256",
            "execution_usage_manifest_sha256",
            "model_call_manifest_sha256",
            "artifact_sha256",
            "adapter_manifest_sha256",
            "evidence",
            "manifest_sha256",
        }:
            raise CampaignJournalError(
                "operational evidence receipt conflicts"
            )
        try:
            receipt = OperationalEvidenceReceipt(
                cycle_id=payload["cycle_id"],
                member_id=payload["member_id"],
                preparation_manifest_sha256=(
                    payload["preparation_manifest_sha256"]
                ),
                execution_usage_manifest_sha256=(
                    payload["execution_usage_manifest_sha256"]
                ),
                model_call_manifest_sha256=(
                    payload["model_call_manifest_sha256"]
                ),
                artifact_sha256=payload["artifact_sha256"],
                adapter_manifest_sha256=(
                    payload["adapter_manifest_sha256"]
                ),
                evidence=_evidence_result_from_payload(payload["evidence"]),
                manifest_sha256=payload["manifest_sha256"],
                event_id=events[0].event_id,
            )
            current_cycle = self._lifecycle._replay_cycle(
                self._lifecycle._cycle_events(connection, cycle_id)
            )
            self._replay_evidence_receipt_in_transaction(
                connection,
                cycle_id=cycle_id,
                receipt=receipt,
                current_cycle=current_cycle,
                allow_settled_reservation=True,
            )
        except (CampaignJournalError, TypeError, ValueError) as error:
            raise CampaignJournalError(
                "operational evidence receipt conflicts"
            ) from error
        if receipt.manifest_sha256 != expected_manifest_sha256:
            raise CampaignJournalError(
                "operational evidence receipt conflicts"
            )
        return events[0], receipt

    def _learning_commit_state_in_transaction(
        self,
        connection,
        *,
        execution: ExecutingOperationalCycle,
        evidence_receipt: OperationalEvidenceReceipt,
    ) -> tuple[str, CycleSnapshot, dict[str, object], tuple]:
        cycle_id, current_cycle = (
            self._require_evidence_execution_generation_in_transaction(
                connection,
                execution,
            )
        )
        if current_cycle.status not in {
            CycleStatus.EVIDENCE_READY,
            CycleStatus.LEARNING_COMMITTED,
        }:
            raise CampaignJournalError(
                "Learning Commit requires EVIDENCE_READY"
            )
        artifact = self._replay_evidence_receipt_in_transaction(
            connection,
            cycle_id=cycle_id,
            receipt=evidence_receipt,
            current_cycle=current_cycle,
        )
        if (
            evidence_receipt.evidence.verdict != "VALID"
            or not evidence_receipt.evidence.promotion_eligible
            or evidence_receipt.evidence.taint_refs
            or evidence_receipt.evidence.invalidation_codes
            or not isinstance(artifact, Mapping)
        ):
            raise CampaignJournalError(
                "operational evidence is not Learning eligible"
            )
        events = self._learning_commit_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        if current_cycle.status is CycleStatus.EVIDENCE_READY:
            required_event_ids = (
                self._learning_commit_event_id(cycle_id),
                self._lifecycle._cycle_event_id(
                    cycle_id,
                    CycleStatus.LEARNING_COMMITTED.value,
                ),
            )
            if any(
                self._journal._event_in_transaction(
                    connection,
                    event_id,
                )
                is not None
                for event_id in required_event_ids
            ):
                raise CampaignJournalError(
                    "operational Learning Commit event identity conflicts"
                )
            if events:
                raise CampaignJournalError(
                    "operational Learning Commit conflicts"
                )
        elif (
            len(events) != 1
            or events[0].event_id
            != self._learning_commit_event_id(cycle_id)
            or events[0].event_type != _LEARNING_COMMIT_RECORDED
        ):
            raise CampaignJournalError(
                "operational Learning Commit conflicts"
            )
        return cycle_id, current_cycle, dict(artifact), events

    def _replay_execution_usage_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        receipt: OperationalExecutionUsage,
        allow_settled_reservation: bool = False,
    ):
        (
            preparation_manifest_sha256,
            context,
            roster,
        ) = self._evidence_preparation_in_transaction(
            connection,
            cycle_id=cycle_id,
            allow_settled_reservation=allow_settled_reservation,
        )
        usage = OperationalUsageJournal(
            journal=self._journal,
            cycle_id=cycle_id,
        )
        model_calls = tuple(
            self._model_call_for_member_in_transaction(
                connection,
                cycle_id=cycle_id,
                member=member,
                preparation_manifest_sha256=preparation_manifest_sha256,
                context_manifest_sha256=context.manifest_sha256,
                roster_manifest_sha256=roster.manifest_sha256,
                usage=usage,
            )
            for member in roster.members
        )
        usage_event, payload = self._execution_usage_binding_in_transaction(
            connection,
            cycle_id=cycle_id,
            preparation_manifest_sha256=preparation_manifest_sha256,
            context=context,
            roster=roster,
            model_calls=model_calls,
        )
        roster_history = self._roster._replay_history(
            connection,
            self._roster._events(connection, cycle_id),
        )
        roster_completion = RosterCompletion(
            cycle_id=cycle_id,
            member_ids=tuple(sorted(roster_history.verified_member_ids)),
            event_id=roster_history.terminal_event_id,
        )
        replayed = OperationalExecutionUsage(
            cycle_id=cycle_id,
            usage_status=UsageStatus(str(payload["usage_status"])),
            input_tokens=payload["input_tokens"],
            output_tokens=payload["output_tokens"],
            cost=payload["cost"],
            currency=payload["currency"],
            wall_time_ms=payload["wall_time_ms"],
            tool_attempts=payload["tool_attempts"],
            data_exposures=payload["data_exposures"],
            disk_growth_bytes=payload["disk_growth_bytes"],
            model_calls=model_calls,
            roster_completion=roster_completion,
            manifest_sha256=payload["manifest_sha256"],
            event_id=usage_event.event_id,
        )
        if receipt != replayed:
            raise CampaignJournalError(
                "operational execution usage receipt conflicts"
            )
        return usage_event, replayed

    def _model_evidence_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_model_evidence.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _model_evidence_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        rows = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _MODEL_EVIDENCE_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_id"] != cycle_id
            for row in rows
        ):
            raise CampaignJournalError(
                "operational model evidence stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_MODEL_EVIDENCE_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _learning_commit_intent_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_learning_commit_intent.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    @staticmethod
    def _learning_commit_intent_payload(
        *,
        cycle_id: str,
        evidence_receipt: OperationalEvidenceReceipt,
        authority_task_report_sha256: str,
        packet_hash: str,
    ) -> dict[str, object]:
        identity = {
            "schema_version": (
                "control_plane.operational_learning_commit_intent.v1"
            ),
            "cycle_id": cycle_id,
            "member_id": evidence_receipt.member_id,
            "evidence_manifest_sha256": evidence_receipt.manifest_sha256,
            "authority_task_report_sha256": authority_task_report_sha256,
            "packet_hash": packet_hash,
        }
        return {
            **identity,
            "manifest_sha256": _controller_sha256(
                b"control_plane.operational_learning_commit_intent.v1",
                identity,
                "operational Learning Commit intent",
            ),
        }

    def _learning_commit_intent_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        rows = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _LEARNING_COMMIT_INTENT_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_id"] != cycle_id
            for row in rows
        ):
            raise CampaignJournalError(
                "operational Learning Commit intent stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_LEARNING_COMMIT_INTENT_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _learning_commit_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_learning_commit.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _learning_commit_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        rows = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _LEARNING_COMMIT_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_id"] != cycle_id
            for row in rows
        ):
            raise CampaignJournalError(
                "operational Learning Commit stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_LEARNING_COMMIT_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _replay_learning_commit_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        receipt: OperationalLearningCommitReceipt,
    ):
        for value, name in (
            (receipt.evidence_manifest_sha256, "evidence manifest"),
            (receipt.authority_task_report_sha256, "Authority TaskReport"),
            (receipt.packet_hash, "Learning packet"),
            (receipt.manifest_sha256, "Learning Commit manifest"),
        ):
            _stored_sha256(value, name)
        identity = {
            "schema_version": "control_plane.operational_learning_commit.v1",
            "cycle_id": cycle_id,
            "member_id": receipt.member_id,
            "evidence_manifest_sha256": receipt.evidence_manifest_sha256,
            "authority_task_report_sha256": (
                receipt.authority_task_report_sha256
            ),
            "packet_hash": receipt.packet_hash,
        }
        payload = {
            **identity,
            "manifest_sha256": _controller_sha256(
                b"control_plane.operational_learning_commit.v1",
                identity,
                "replayed operational Learning Commit",
            ),
        }
        events = self._learning_commit_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        intent_events = self._learning_commit_intent_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        intent_identity = {
            **identity,
            "schema_version": (
                "control_plane.operational_learning_commit_intent.v1"
            ),
        }
        intent_payload = {
            **intent_identity,
            "manifest_sha256": _controller_sha256(
                b"control_plane.operational_learning_commit_intent.v1",
                intent_identity,
                "replayed operational Learning Commit intent",
            ),
        }
        evidence_events = self._model_evidence_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        if (
            receipt.cycle_id != cycle_id
            or receipt.event_id != self._learning_commit_event_id(cycle_id)
            or receipt.manifest_sha256 != payload["manifest_sha256"]
            or len(events) != 1
            or events[0].event_id != receipt.event_id
            or events[0].event_type != _LEARNING_COMMIT_RECORDED
            or _canonical_json_text(
                _event_domain_payload(events[0]),
                "stored operational Learning Commit",
            )
            != _canonical_json_text(
                payload,
                "replayed operational Learning Commit",
            )
            or len(intent_events) != 1
            or intent_events[0].event_id
            != self._learning_commit_intent_event_id(cycle_id)
            or intent_events[0].event_type
            != _LEARNING_COMMIT_INTENT_RECORDED
            or _canonical_json_text(
                _event_domain_payload(intent_events[0]),
                "stored operational Learning Commit intent",
            )
            != _canonical_json_text(
                intent_payload,
                "replayed operational Learning Commit intent",
            )
            or len(evidence_events) != 1
            or _event_domain_payload(evidence_events[0]).get(
                "manifest_sha256"
            )
            != receipt.evidence_manifest_sha256
            or events[0].sequence
            <= max(
                intent_events[0].sequence,
                evidence_events[0].sequence,
            )
        ):
            raise CampaignJournalError(
                "operational Learning Commit receipt conflicts"
            )
        return events[0]

    @staticmethod
    def _no_learning_disposition_reason(evidence: EvidenceResult) -> str:
        if type(evidence) is not EvidenceResult:
            raise TypeError("evidence must be an EvidenceResult")
        if evidence.verdict == "NO_MATERIAL_FINDING":
            if (
                evidence.promotion_eligible
                or evidence.scientific_outcome != "NO_MATERIAL_FINDING"
                or evidence.taint_refs
                or evidence.invalidation_codes
            ):
                raise CampaignJournalError(
                    "NO_MATERIAL_FINDING evidence is inconsistent"
                )
            return "NO_MATERIAL_FINDING"
        if evidence.promotion_eligible or evidence.verdict == "VALID":
            raise CampaignJournalError(
                "Learning-eligible evidence requires Learning Commit"
            )
        if evidence.taint_refs:
            return "TAINTED_EVIDENCE"
        if evidence.verdict in {
            "EVIDENCE_INVALID",
            "MATERIAL_UNAPPROVED",
            "RESEARCH_ONLY",
        }:
            return evidence.verdict
        raise CampaignJournalError(
            "evidence has no supported no-Learning disposition"
        )

    def _no_learning_disposition_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_no_learning_disposition.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _no_learning_disposition_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        rows = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _NO_LEARNING_DISPOSITION_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_id"] != cycle_id
            for row in rows
        ):
            raise CampaignJournalError(
                "operational no-Learning disposition stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_NO_LEARNING_DISPOSITION_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _cycle_settlement_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_cycle_settlement.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _cycle_settlement_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        rows = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _CYCLE_SETTLEMENT_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_id"] != cycle_id
            for row in rows
        ):
            raise CampaignJournalError(
                "operational Cycle settlement stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_CYCLE_SETTLEMENT_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _stored_settlement_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ) -> (
        OperationalCycleSettlementReceipt
        | OperationalNoLearningSettlementReceipt
    ):
        events = self._cycle_settlement_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        if (
            len(events) != 1
            or events[0].event_id != self._cycle_settlement_event_id(cycle_id)
            or events[0].event_type != _CYCLE_SETTLEMENT_RECORDED
        ):
            raise CampaignJournalError(
                "operational Cycle settlement receipt conflicts"
            )
        payload = _event_domain_payload(events[0])
        schema_version = payload.get("schema_version")
        if (
            schema_version == "control_plane.operational_cycle_settlement.v1"
            and set(payload)
            == {
                "schema_version",
                "cycle_id",
                "reservation_id",
                "settlement_state",
                "execution_usage_manifest_sha256",
                "learning_commit_manifest_sha256",
                "budget_settlement_event_id",
                "manifest_sha256",
            }
        ):
            return OperationalCycleSettlementReceipt(
                cycle_id=payload["cycle_id"],
                reservation_id=payload["reservation_id"],
                settlement_state=payload["settlement_state"],
                execution_usage_manifest_sha256=(
                    payload["execution_usage_manifest_sha256"]
                ),
                learning_commit_manifest_sha256=(
                    payload["learning_commit_manifest_sha256"]
                ),
                budget_settlement_event_id=(
                    payload["budget_settlement_event_id"]
                ),
                manifest_sha256=payload["manifest_sha256"],
                event_id=events[0].event_id,
            )
        if (
            schema_version
            == "control_plane.operational_no_learning_settlement.v1"
            and set(payload)
            == {
                "schema_version",
                "cycle_id",
                "reservation_id",
                "disposition_reason",
                "evidence_manifest_sha256",
                "execution_usage_manifest_sha256",
                "settlement_state",
                "disposition_event_id",
                "budget_settlement_event_id",
                "manifest_sha256",
            }
        ):
            return OperationalNoLearningSettlementReceipt(
                cycle_id=payload["cycle_id"],
                reservation_id=payload["reservation_id"],
                disposition_reason=payload["disposition_reason"],
                evidence_manifest_sha256=(
                    payload["evidence_manifest_sha256"]
                ),
                execution_usage_manifest_sha256=(
                    payload["execution_usage_manifest_sha256"]
                ),
                settlement_state=payload["settlement_state"],
                disposition_event_id=payload["disposition_event_id"],
                budget_settlement_event_id=(
                    payload["budget_settlement_event_id"]
                ),
                manifest_sha256=payload["manifest_sha256"],
                event_id=events[0].event_id,
            )
        raise CampaignJournalError(
            "operational Cycle settlement receipt conflicts"
        )

    def _replay_learned_settlement_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        receipt: OperationalCycleSettlementReceipt,
    ) -> tuple[str, int]:
        for value, name in (
            (receipt.execution_usage_manifest_sha256, "execution usage"),
            (receipt.learning_commit_manifest_sha256, "Learning Commit"),
            (receipt.manifest_sha256, "Cycle settlement"),
        ):
            _stored_sha256(value, name)
        reservation_id = self._reservation_id(cycle_id)
        expected_identity = {
            "schema_version": "control_plane.operational_cycle_settlement.v1",
            "cycle_id": cycle_id,
            "reservation_id": reservation_id,
            "settlement_state": receipt.settlement_state,
            "execution_usage_manifest_sha256": (
                receipt.execution_usage_manifest_sha256
            ),
            "learning_commit_manifest_sha256": (
                receipt.learning_commit_manifest_sha256
            ),
            "budget_settlement_event_id": receipt.budget_settlement_event_id,
        }
        expected_payload = {
            **expected_identity,
            "manifest_sha256": _controller_sha256(
                b"control_plane.operational_cycle_settlement.v1",
                expected_identity,
                "replayed operational Cycle settlement",
            ),
        }
        settlement_events = self._cycle_settlement_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        usage_events = self._execution_usage_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        learning_events = self._learning_commit_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        budget_events = self._budget._events_in_transaction(connection)
        self._budget._replay(budget_events)
        budget_event = next(
            (
                event
                for event in budget_events
                if event.event_id == receipt.budget_settlement_event_id
            ),
            None,
        )
        budget_payload = (
            _event_domain_payload(budget_event)
            if budget_event is not None
            else {}
        )
        usage_payload = (
            _event_domain_payload(usage_events[0])
            if len(usage_events) == 1
            else {}
        )
        lifecycle_events = self._lifecycle._cycle_events(
            connection,
            cycle_id,
        )
        learning_transitions = tuple(
            event
            for event in lifecycle_events
            if event.event_type == _CYCLE_TRANSITIONED
            and _event_domain_payload(event).get("to_status")
            == CycleStatus.LEARNING_COMMITTED.value
        )
        settled_transitions = tuple(
            event
            for event in lifecycle_events
            if event.event_type == _CYCLE_TRANSITIONED
            and _event_domain_payload(event).get("to_status")
            == CycleStatus.SETTLED.value
        )
        learning_payload = (
            _event_domain_payload(learning_events[0])
            if len(learning_events) == 1
            else {}
        )
        if set(learning_payload) != {
            "schema_version",
            "cycle_id",
            "member_id",
            "evidence_manifest_sha256",
            "authority_task_report_sha256",
            "packet_hash",
            "manifest_sha256",
        }:
            raise CampaignJournalError(
                "operational Cycle settlement receipt conflicts"
            )
        try:
            replayed_learning = OperationalLearningCommitReceipt(
                cycle_id=learning_payload["cycle_id"],
                member_id=learning_payload["member_id"],
                evidence_manifest_sha256=(
                    learning_payload["evidence_manifest_sha256"]
                ),
                authority_task_report_sha256=(
                    learning_payload["authority_task_report_sha256"]
                ),
                packet_hash=learning_payload["packet_hash"],
                manifest_sha256=learning_payload["manifest_sha256"],
                event_id=learning_events[0].event_id,
            )
            learning_event = (
                self._replay_learning_commit_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    receipt=replayed_learning,
                )
            )
            _, replayed_evidence = (
                self._replay_stored_evidence_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    expected_manifest_sha256=(
                        replayed_learning.evidence_manifest_sha256
                    ),
                )
            )
        except (CampaignJournalError, TypeError, ValueError) as error:
            raise CampaignJournalError(
                "operational Cycle settlement receipt conflicts"
            ) from error
        learning_identity = {
            key: value
            for key, value in learning_payload.items()
            if key != "manifest_sha256"
        }
        packet_hash = replayed_learning.packet_hash
        if (
            receipt.cycle_id != cycle_id
            or receipt.reservation_id != reservation_id
            or receipt.event_id != self._cycle_settlement_event_id(cycle_id)
            or receipt.manifest_sha256 != expected_payload["manifest_sha256"]
            or len(settlement_events) != 1
            or settlement_events[0].event_id != receipt.event_id
            or settlement_events[0].event_type != _CYCLE_SETTLEMENT_RECORDED
            or _canonical_json_text(
                _event_domain_payload(settlement_events[0]),
                "stored operational Cycle settlement",
            )
            != _canonical_json_text(
                expected_payload,
                "replayed operational Cycle settlement",
            )
            or len(usage_events) != 1
            or usage_events[0].event_type != _EXECUTION_USAGE_FROZEN
            or _event_domain_payload(usage_events[0]).get("manifest_sha256")
            != receipt.execution_usage_manifest_sha256
            or replayed_evidence.execution_usage_manifest_sha256
            != receipt.execution_usage_manifest_sha256
            or replayed_evidence.member_id != replayed_learning.member_id
            or learning_payload.get("manifest_sha256")
            != receipt.learning_commit_manifest_sha256
            or learning_payload.get("manifest_sha256")
            != _controller_sha256(
                b"control_plane.operational_learning_commit.v1",
                learning_identity,
                "replayed operational Learning Commit",
            )
            or budget_event is None
            or budget_event.event_type != _BUDGET_SETTLED
            or budget_event.event_id
            != self._budget._event_id(
                "settle",
                reservation_id=reservation_id,
            )
            or budget_payload.get("reservation_id") != reservation_id
            or budget_payload.get("state") != receipt.settlement_state
            or any(
                budget_payload.get(field) != usage_payload.get(field)
                for field in (
                    "input_tokens",
                    "output_tokens",
                    "cost",
                    "wall_time_ms",
                    "tool_attempts",
                    "data_exposures",
                    "disk_growth_bytes",
                )
            )
            or len(learning_transitions) != 1
            or len(settled_transitions) != 1
            or usage_events[0].sequence >= learning_event.sequence
            or learning_transitions[0].sequence
            <= learning_event.sequence
            or budget_event.sequence <= learning_transitions[0].sequence
            or settlement_events[0].sequence <= budget_event.sequence
            or settled_transitions[0].sequence
            <= settlement_events[0].sequence
        ):
            raise CampaignJournalError(
                "operational Cycle settlement receipt conflicts"
            )
        _stored_sha256(packet_hash, "Learning packet")
        return packet_hash, settled_transitions[0].sequence

    def _replay_no_learning_settlement_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        receipt: OperationalNoLearningSettlementReceipt,
    ) -> tuple[str, int]:
        for value, name in (
            (receipt.evidence_manifest_sha256, "evidence"),
            (receipt.execution_usage_manifest_sha256, "execution usage"),
            (receipt.manifest_sha256, "no-Learning settlement"),
        ):
            _stored_sha256(value, name)
        reservation_id = self._reservation_id(cycle_id)
        expected_identity = {
            "schema_version": (
                "control_plane.operational_no_learning_settlement.v1"
            ),
            "cycle_id": cycle_id,
            "reservation_id": reservation_id,
            "disposition_reason": receipt.disposition_reason,
            "evidence_manifest_sha256": receipt.evidence_manifest_sha256,
            "execution_usage_manifest_sha256": (
                receipt.execution_usage_manifest_sha256
            ),
            "settlement_state": receipt.settlement_state,
            "disposition_event_id": receipt.disposition_event_id,
            "budget_settlement_event_id": receipt.budget_settlement_event_id,
        }
        expected_payload = {
            **expected_identity,
            "manifest_sha256": _controller_sha256(
                b"control_plane.operational_no_learning_settlement.v1",
                expected_identity,
                "replayed no-Learning settlement",
            ),
        }
        settlement_events = self._cycle_settlement_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        disposition_events = (
            self._no_learning_disposition_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
        )
        evidence_events = self._model_evidence_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        usage_events = self._execution_usage_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        budget_events = self._budget._events_in_transaction(connection)
        self._budget._replay(budget_events)
        budget_event = next(
            (
                event
                for event in budget_events
                if event.event_id == receipt.budget_settlement_event_id
            ),
            None,
        )
        budget_payload = (
            _event_domain_payload(budget_event)
            if budget_event is not None
            else {}
        )
        usage_payload = (
            _event_domain_payload(usage_events[0])
            if len(usage_events) == 1
            else {}
        )
        lifecycle_events = self._lifecycle._cycle_events(
            connection,
            cycle_id,
        )
        skipped_transitions = tuple(
            event
            for event in lifecycle_events
            if event.event_type == _CYCLE_TRANSITIONED
            and _event_domain_payload(event).get("to_status")
            == CycleStatus.LEARNING_SKIPPED.value
        )
        settled_transitions = tuple(
            event
            for event in lifecycle_events
            if event.event_type == _CYCLE_TRANSITIONED
            and _event_domain_payload(event).get("to_status")
            == CycleStatus.SETTLED.value
        )
        disposition_payload = (
            _event_domain_payload(disposition_events[0])
            if len(disposition_events) == 1
            else {}
        )
        disposition_identity = {
            key: value
            for key, value in disposition_payload.items()
            if key != "manifest_sha256"
        }
        try:
            evidence_event, replayed_evidence = (
                self._replay_stored_evidence_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    expected_manifest_sha256=(
                        receipt.evidence_manifest_sha256
                    ),
                )
            )
            replayed_disposition_reason = (
                self._no_learning_disposition_reason(
                    replayed_evidence.evidence
                )
            )
        except (CampaignJournalError, TypeError, ValueError) as error:
            raise CampaignJournalError(
                "operational no-Learning settlement receipt conflicts"
            ) from error
        if (
            receipt.cycle_id != cycle_id
            or receipt.reservation_id != reservation_id
            or receipt.event_id != self._cycle_settlement_event_id(cycle_id)
            or receipt.disposition_event_id
            != self._no_learning_disposition_event_id(cycle_id)
            or receipt.manifest_sha256 != expected_payload["manifest_sha256"]
            or len(settlement_events) != 1
            or settlement_events[0].event_id != receipt.event_id
            or settlement_events[0].event_type != _CYCLE_SETTLEMENT_RECORDED
            or _canonical_json_text(
                _event_domain_payload(settlement_events[0]),
                "stored no-Learning settlement",
            )
            != _canonical_json_text(
                expected_payload,
                "replayed no-Learning settlement",
            )
            or len(disposition_events) != 1
            or disposition_events[0].event_id != receipt.disposition_event_id
            or disposition_events[0].event_type
            != _NO_LEARNING_DISPOSITION_RECORDED
            or set(disposition_payload)
            != {
                "schema_version",
                "cycle_id",
                "member_id",
                "evidence_manifest_sha256",
                "evidence_verdict",
                "scientific_outcome",
                "disposition_reason",
                "manifest_sha256",
            }
            or disposition_payload.get("schema_version")
            != "control_plane.operational_no_learning_disposition.v1"
            or disposition_payload.get("cycle_id") != cycle_id
            or disposition_payload.get("evidence_manifest_sha256")
            != receipt.evidence_manifest_sha256
            or disposition_payload.get("disposition_reason")
            != receipt.disposition_reason
            or disposition_payload.get("disposition_reason")
            != replayed_disposition_reason
            or disposition_payload.get("member_id")
            != replayed_evidence.member_id
            or disposition_payload.get("evidence_verdict")
            != replayed_evidence.evidence.verdict
            or disposition_payload.get("scientific_outcome")
            != replayed_evidence.evidence.scientific_outcome
            or disposition_payload.get("manifest_sha256")
            != _controller_sha256(
                b"control_plane.operational_no_learning_disposition.v1",
                disposition_identity,
                "replayed no-Learning disposition",
            )
            or len(evidence_events) != 1
            or evidence_events[0].event_type != _MODEL_EVIDENCE_RECORDED
            or evidence_events[0].event_id != evidence_event.event_id
            or _event_domain_payload(evidence_events[0]).get(
                "manifest_sha256"
            )
            != receipt.evidence_manifest_sha256
            or len(usage_events) != 1
            or usage_events[0].event_type != _EXECUTION_USAGE_FROZEN
            or _event_domain_payload(usage_events[0]).get("manifest_sha256")
            != receipt.execution_usage_manifest_sha256
            or replayed_evidence.execution_usage_manifest_sha256
            != receipt.execution_usage_manifest_sha256
            or budget_event is None
            or budget_event.event_type != _BUDGET_SETTLED
            or budget_event.event_id
            != self._budget._event_id(
                "settle",
                reservation_id=reservation_id,
            )
            or budget_payload.get("reservation_id") != reservation_id
            or budget_payload.get("state") != receipt.settlement_state
            or any(
                budget_payload.get(field) != usage_payload.get(field)
                for field in (
                    "input_tokens",
                    "output_tokens",
                    "cost",
                    "wall_time_ms",
                    "tool_attempts",
                    "data_exposures",
                    "disk_growth_bytes",
                )
            )
            or len(skipped_transitions) != 1
            or len(settled_transitions) != 1
            or self._learning_commit_intent_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            or self._learning_commit_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            or usage_events[0].sequence >= evidence_events[0].sequence
            or disposition_events[0].sequence <= evidence_events[0].sequence
            or skipped_transitions[0].sequence
            <= disposition_events[0].sequence
            or budget_event.sequence <= skipped_transitions[0].sequence
            or settlement_events[0].sequence <= budget_event.sequence
            or settled_transitions[0].sequence
            <= settlement_events[0].sequence
        ):
            raise CampaignJournalError(
                "operational no-Learning settlement receipt conflicts"
            )
        if receipt.disposition_reason not in {
            "NO_MATERIAL_FINDING",
            "TAINTED_EVIDENCE",
            "EVIDENCE_INVALID",
            "MATERIAL_UNAPPROVED",
            "RESEARCH_ONLY",
        }:
            raise CampaignJournalError(
                "operational no-Learning disposition is invalid"
            )
        return receipt.disposition_reason, settled_transitions[0].sequence

    def _information_gain_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_information_gain.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _information_gain_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        rows = connection.execute(
            "SELECT cycle_id, aggregate_type, aggregate_id, event_type "
            "FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND ((aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)) "
            "OR (event_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)))",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _INFORMATION_GAIN_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
                _INFORMATION_GAIN_RECORDED,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_type"] != _INFORMATION_GAIN_AGGREGATE_TYPE
            or row["aggregate_id"] != cycle_id
            or row["event_type"] != _INFORMATION_GAIN_RECORDED
            for row in rows
        ):
            raise CampaignJournalError(
                "operational information gain stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_INFORMATION_GAIN_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _replay_information_gain_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        receipt: OperationalInformationGainReceipt,
    ) -> int:
        if (
            type(receipt) is not OperationalInformationGainReceipt
            or receipt.cycle_id != cycle_id
        ):
            raise CampaignJournalError("information-gain receipt conflicts")
        events = self._information_gain_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        transitions = tuple(
            event
            for event in self._lifecycle._cycle_events(connection, cycle_id)
            if event.event_type == _CYCLE_TRANSITIONED
            and _event_domain_payload(event).get("to_status")
            == CycleStatus.INFORMATION_GAIN_RECORDED.value
        )
        payload = {
            "schema_version": "control_plane.operational_information_gain.v1",
            "cycle_id": receipt.cycle_id,
            "information_gain_status": receipt.information_gain_status,
            "continuation_eligible": receipt.continuation_eligible,
            "settlement_manifest_sha256": (
                receipt.settlement_manifest_sha256
            ),
            "learning_packet_hash": receipt.learning_packet_hash,
            "disposition_reason": receipt.disposition_reason,
            "manifest_sha256": receipt.manifest_sha256,
        }
        identity = {
            key: value for key, value in payload.items() if key != "manifest_sha256"
        }
        settlement = self._stored_settlement_receipt_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        if type(settlement) is OperationalCycleSettlementReceipt:
            packet_hash, settled_sequence = (
                self._replay_learned_settlement_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    receipt=settlement,
                )
            )
            expected_status = "ELIGIBLE_LEARNING_COMMITTED"
            expected_continuation = True
            expected_disposition = None
        else:
            expected_disposition, settled_sequence = (
                self._replay_no_learning_settlement_receipt_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    receipt=settlement,
                )
            )
            packet_hash = None
            if expected_disposition in {
                "NO_MATERIAL_FINDING",
                "MATERIAL_UNAPPROVED",
                "RESEARCH_ONLY",
            }:
                expected_status = expected_disposition
            else:
                expected_status = "INELIGIBLE_EVIDENCE"
            expected_continuation = False
        if (
            len(events) != 1
            or len(transitions) != 1
            or receipt.event_id != self._information_gain_event_id(cycle_id)
            or receipt.manifest_sha256
            != _controller_sha256(
                b"control_plane.operational_information_gain.v1",
                identity,
                "operational information gain",
            )
            or receipt.information_gain_status != expected_status
            or receipt.continuation_eligible is not expected_continuation
            or receipt.settlement_manifest_sha256
            != settlement.manifest_sha256
            or receipt.learning_packet_hash != packet_hash
            or receipt.disposition_reason != expected_disposition
            or events[0].event_id != receipt.event_id
            or _canonical_json_text(
                _event_domain_payload(events[0]),
                "stored operational information gain",
            )
            != _canonical_json_text(payload, "expected operational information gain")
            or events[0].sequence <= settled_sequence
            or transitions[0].sequence <= events[0].sequence
        ):
            raise CampaignJournalError("information-gain receipt conflicts")
        return transitions[0].sequence

    def _stored_information_gain_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ) -> OperationalInformationGainReceipt:
        events = self._information_gain_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        if len(events) != 1:
            raise CampaignJournalError(
                "operational information gain is missing or ambiguous"
            )
        event = events[0]
        payload = _event_domain_payload(event)
        if set(payload) != {
            "schema_version",
            "cycle_id",
            "information_gain_status",
            "continuation_eligible",
            "settlement_manifest_sha256",
            "learning_packet_hash",
            "disposition_reason",
            "manifest_sha256",
        }:
            raise CampaignJournalError(
                "operational information gain payload is invalid"
            )
        try:
            stored_cycle_id = _identifier(
                payload["cycle_id"],
                "stored information-gain cycle_id",
            )
            information_gain_status = _identifier(
                payload["information_gain_status"],
                "stored information_gain_status",
            )
            continuation_eligible = payload["continuation_eligible"]
            if type(continuation_eligible) is not bool:
                raise ValueError("stored continuation_eligible is invalid")
            settlement_manifest_sha256 = _stored_sha256(
                payload["settlement_manifest_sha256"],
                "stored settlement_manifest_sha256",
            )
            learning_packet_hash = payload["learning_packet_hash"]
            if learning_packet_hash is not None:
                learning_packet_hash = _stored_sha256(
                    learning_packet_hash,
                    "stored learning_packet_hash",
                )
            disposition_reason = payload["disposition_reason"]
            if disposition_reason is not None:
                disposition_reason = _identifier(
                    disposition_reason,
                    "stored disposition_reason",
                )
            manifest_sha256 = _stored_sha256(
                payload["manifest_sha256"],
                "stored information-gain manifest_sha256",
            )
        except (TypeError, ValueError) as error:
            raise CampaignJournalError(
                "operational information gain payload is invalid"
            ) from error
        if (
            payload["schema_version"]
            != "control_plane.operational_information_gain.v1"
            or stored_cycle_id != cycle_id
            or event.event_id != self._information_gain_event_id(cycle_id)
        ):
            raise CampaignJournalError(
                "operational information gain payload is invalid"
            )
        receipt = OperationalInformationGainReceipt(
            cycle_id=stored_cycle_id,
            information_gain_status=information_gain_status,
            continuation_eligible=continuation_eligible,
            settlement_manifest_sha256=settlement_manifest_sha256,
            learning_packet_hash=learning_packet_hash,
            disposition_reason=disposition_reason,
            manifest_sha256=manifest_sha256,
            event_id=event.event_id,
        )
        self._replay_information_gain_receipt_in_transaction(
            connection,
            cycle_id=cycle_id,
            receipt=receipt,
        )
        return receipt

    def _next_cycle_decision_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_next_cycle_decision.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _next_cycle_decision_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        rows = connection.execute(
            "SELECT cycle_id, aggregate_type, aggregate_id, event_type "
            "FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND ((aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)) "
            "OR (event_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)))",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _NEXT_CYCLE_DECISION_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
                _NEXT_CYCLE_DECISION_RECORDED,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_type"] != _NEXT_CYCLE_DECISION_AGGREGATE_TYPE
            or row["aggregate_id"] != cycle_id
            or row["event_type"] != _NEXT_CYCLE_DECISION_RECORDED
            for row in rows
        ):
            raise CampaignJournalError(
                "operational next-Cycle decision stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_NEXT_CYCLE_DECISION_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _stored_next_cycle_decision_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        information_gain_receipt: OperationalInformationGainReceipt,
    ) -> OperationalNextCycleDecisionReceipt:
        events = self._next_cycle_decision_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        if len(events) != 1:
            raise CampaignJournalError(
                "operational next-Cycle decision is missing or ambiguous"
            )
        event = events[0]
        payload = _event_domain_payload(event)
        if set(payload) != {
            "schema_version",
            "cycle_id",
            "decision",
            "continuation_allowed",
            "reason_code",
            "next_cycle_number",
            "information_gain_manifest_sha256",
            "cycle_budget_id",
            "reserved_cycle_count",
            "max_cycles",
            "manifest_sha256",
        }:
            raise CampaignJournalError(
                "operational next-Cycle decision payload is invalid"
            )
        try:
            stored_cycle_id = _identifier(
                payload["cycle_id"],
                "stored next-Cycle decision cycle_id",
            )
            decision = _identifier(
                payload["decision"],
                "stored next-Cycle decision",
            )
            continuation_allowed = payload["continuation_allowed"]
            if type(continuation_allowed) is not bool:
                raise ValueError("stored continuation_allowed is invalid")
            reason_code = _identifier(
                payload["reason_code"],
                "stored next-Cycle reason_code",
            )
            next_cycle_number = payload["next_cycle_number"]
            if next_cycle_number is not None and (
                type(next_cycle_number) is not int
                or not 1 <= next_cycle_number <= 1_000_000
            ):
                raise ValueError("stored next_cycle_number is invalid")
            information_gain_manifest_sha256 = _stored_sha256(
                payload["information_gain_manifest_sha256"],
                "stored information_gain_manifest_sha256",
            )
            cycle_budget_id = _identifier(
                payload["cycle_budget_id"],
                "stored cycle_budget_id",
            )
            reserved_cycle_count = payload["reserved_cycle_count"]
            max_cycles = payload["max_cycles"]
            if (
                type(reserved_cycle_count) is not int
                or reserved_cycle_count < 0
                or type(max_cycles) is not int
                or max_cycles < 0
            ):
                raise ValueError("stored Cycle budget snapshot is invalid")
            manifest_sha256 = _stored_sha256(
                payload["manifest_sha256"],
                "stored next-Cycle decision manifest_sha256",
            )
        except (TypeError, ValueError) as error:
            raise CampaignJournalError(
                "operational next-Cycle decision payload is invalid"
            ) from error
        receipt = OperationalNextCycleDecisionReceipt(
            cycle_id=stored_cycle_id,
            decision=decision,
            continuation_allowed=continuation_allowed,
            reason_code=reason_code,
            next_cycle_number=next_cycle_number,
            information_gain_manifest_sha256=(
                information_gain_manifest_sha256
            ),
            cycle_budget_id=cycle_budget_id,
            reserved_cycle_count=reserved_cycle_count,
            max_cycles=max_cycles,
            manifest_sha256=manifest_sha256,
            event_id=event.event_id,
        )
        self._replay_next_cycle_decision_receipt_in_transaction(
            connection,
            cycle_id=cycle_id,
            receipt=receipt,
            information_gain_receipt=information_gain_receipt,
        )
        return receipt

    def _replay_next_cycle_decision_receipt_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        receipt: OperationalNextCycleDecisionReceipt,
        information_gain_receipt: OperationalInformationGainReceipt,
    ) -> int:
        if (
            type(receipt) is not OperationalNextCycleDecisionReceipt
            or receipt.cycle_id != cycle_id
        ):
            raise CampaignJournalError(
                "operational next-Cycle decision receipt conflicts"
            )
        information_gain_sequence = (
            self._replay_information_gain_receipt_in_transaction(
                connection,
                cycle_id=cycle_id,
                receipt=information_gain_receipt,
            )
        )
        events = self._next_cycle_decision_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        transitions = tuple(
            event
            for event in self._lifecycle._cycle_events(connection, cycle_id)
            if event.event_type == _CYCLE_TRANSITIONED
            and _event_domain_payload(event).get("to_status")
            in {
                CycleStatus.NEXT_CYCLE_DECIDED.value,
                CycleStatus.COMPLETED.value,
            }
        )
        if len(events) != 1 or len(transitions) != 2:
            raise CampaignJournalError(
                "operational next-Cycle decision receipt conflicts"
            )
        transition_by_status = {
            _event_domain_payload(event).get("to_status"): event
            for event in transitions
        }
        if set(transition_by_status) != {
            CycleStatus.NEXT_CYCLE_DECIDED.value,
            CycleStatus.COMPLETED.value,
        }:
            raise CampaignJournalError(
                "operational next-Cycle decision receipt conflicts"
            )
        event = events[0]
        historical_budget_events = tuple(
            budget_event
            for budget_event in self._cycle_budget._events_in_transaction(
                connection
            )
            if budget_event.sequence < event.sequence
        )
        try:
            historical_budget = self._cycle_budget._replay(
                historical_budget_events
            )
        except (CampaignJournalError, BudgetConflictError) as error:
            raise CampaignJournalError(
                "operational next-Cycle budget binding conflicts"
            ) from error
        cycle = self._lifecycle._replay_cycle(
            self._lifecycle._cycle_events(connection, cycle_id)
        )
        self._require_cycle_budget_prefix(
            cycle_budget=historical_budget,
            cycle_id=cycle_id,
            cycle_number=cycle.cycle_number,
        )
        reserved_cycle_count = len(historical_budget.reserved_cycle_ids)
        (
            expected_decision,
            expected_allowed,
            expected_reason,
            expected_next_cycle_number,
        ) = self._derive_next_cycle_decision(
            information_gain_receipt=information_gain_receipt,
            cycle_number=cycle.cycle_number,
            reserved_cycle_count=reserved_cycle_count,
            max_cycles=historical_budget.max_cycles,
        )
        identity = {
            "schema_version": (
                "control_plane.operational_next_cycle_decision.v1"
            ),
            "cycle_id": cycle_id,
            "decision": expected_decision,
            "continuation_allowed": expected_allowed,
            "reason_code": expected_reason,
            "next_cycle_number": expected_next_cycle_number,
            "information_gain_manifest_sha256": (
                information_gain_receipt.manifest_sha256
            ),
            "cycle_budget_id": historical_budget.budget_id,
            "reserved_cycle_count": reserved_cycle_count,
            "max_cycles": historical_budget.max_cycles,
        }
        manifest_sha256 = _controller_sha256(
            b"control_plane.operational_next_cycle_decision.v1",
            identity,
            "operational next-Cycle decision",
        )
        expected_payload = {**identity, "manifest_sha256": manifest_sha256}
        decided = transition_by_status[CycleStatus.NEXT_CYCLE_DECIDED.value]
        completed = transition_by_status[CycleStatus.COMPLETED.value]
        if (
            cycle.status is not CycleStatus.COMPLETED
            or cycle_id not in historical_budget.reserved_cycle_ids
            or receipt
            != OperationalNextCycleDecisionReceipt(
                cycle_id=cycle_id,
                decision=expected_decision,
                continuation_allowed=expected_allowed,
                reason_code=expected_reason,
                next_cycle_number=expected_next_cycle_number,
                information_gain_manifest_sha256=(
                    information_gain_receipt.manifest_sha256
                ),
                cycle_budget_id=historical_budget.budget_id,
                reserved_cycle_count=reserved_cycle_count,
                max_cycles=historical_budget.max_cycles,
                manifest_sha256=manifest_sha256,
                event_id=self._next_cycle_decision_event_id(cycle_id),
            )
            or event.event_id != receipt.event_id
            or _canonical_json_text(
                _event_domain_payload(event),
                "stored operational next-Cycle decision",
            )
            != _canonical_json_text(
                expected_payload,
                "expected operational next-Cycle decision",
            )
            or event.sequence <= information_gain_sequence
            or decided.sequence <= event.sequence
            or completed.sequence <= decided.sequence
        ):
            raise CampaignJournalError(
                "operational next-Cycle decision receipt conflicts"
            )
        return completed.sequence

    def _record_model_call(
        self,
        *,
        execution: ExecutingOperationalCycle,
        cycle_id: str,
        call_id: str,
        fixed_identity: dict[str, object],
        output_json: str,
        attempt_id: str,
        attempt_count: int,
        wall_time_ms: int | None,
        usage: OperationalUsageJournal,
        expected_attempts: tuple[RecordedModelAttempt, ...],
        expected_verified: VerifiedRosterResponse,
    ) -> ExecutedOperationalModelCall:
        output = json.loads(output_json)
        dynamic_identity = {
            "attempt_id": attempt_id,
            "attempt_count": attempt_count,
            "wall_time_ms": wall_time_ms,
            "output": output,
            "output_sha256": _controller_sha256(
                b"control_plane.operational_model_output.v1",
                output,
                "operational model output",
            ),
            "verified_response_event_id": expected_verified.event_id,
        }
        receipt_identity = {**fixed_identity, **dynamic_identity}
        payload = {
            **receipt_identity,
            "manifest_sha256": _controller_sha256(
                b"control_plane.operational_model_call_receipt.v1",
                receipt_identity,
                "operational model call receipt",
            ),
        }

        def record(connection) -> ExecutedOperationalModelCall:
            self._require_active_execution_in_transaction(
                connection,
                execution,
            )
            events = self._model_call_events_in_transaction(
                connection,
                cycle_id=cycle_id,
                call_id=call_id,
            )
            if len(events) == 1:
                self._journal._append_in_transaction(
                    connection,
                    event_id=self._model_call_event_id(
                        cycle_id,
                        call_id,
                        "complete",
                    ),
                    cycle_id=cycle_id,
                    aggregate_type=_MODEL_CALL_AGGREGATE_TYPE,
                    aggregate_id=call_id,
                    event_type=_MODEL_CALL_COMPLETED,
                    payload=payload,
                )
            elif len(events) != 2:
                raise CampaignJournalError(
                    "operational model call journal is incomplete or ambiguous"
                )
            replay = self._replay_model_call_in_transaction(
                connection,
                cycle_id=cycle_id,
                call_id=call_id,
                expected_identity=fixed_identity,
                usage=usage,
            )
            if (
                replay.output_json != output_json
                or replay.attempt_id != attempt_id
                or replay.attempt_count != attempt_count
                or replay.wall_time_ms != wall_time_ms
                or replay.usage_attempts != expected_attempts
                or replay.verified_response != expected_verified
            ):
                raise CampaignJournalError(
                    "operational model call result conflicts"
                )
            return replay

        return _SqliteUnitOfWork(stores._operational_spec())._write(record)

    @staticmethod
    def _model_call_limits_from_payload(
        payload: object,
    ) -> OperationalModelCallLimits:
        if not isinstance(payload, dict) or set(payload) != {
            "max_input_tokens",
            "max_output_tokens",
            "max_cost",
            "max_wall_time_ms",
            "max_attempts",
        }:
            raise CampaignJournalError("model call limits are invalid")
        try:
            limits = OperationalModelCallLimits(**payload)
        except (TypeError, ValueError) as error:
            raise CampaignJournalError("model call limits are invalid") from error
        if limits.to_payload() != payload:
            raise CampaignJournalError("model call limits are not canonical")
        return limits

    def _require_model_call_allocation_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        call_id: str,
        call_limits: object,
    ) -> None:
        requested = self._model_call_limits_from_payload(call_limits)
        roster = self._roster._replay(
            self._roster._events(connection, cycle_id)
        )
        expected_call_ids = {
            self._member_call_id(cycle_id, member.member_id)
            for member in roster.members
        }
        if call_id not in expected_call_ids:
            raise CampaignJournalError(
                "model call allocation is outside the frozen roster"
            )
        budget_events = self._budget._events_in_transaction(connection)
        self._budget._replay(budget_events)
        reservation_id = self._reservation_id(cycle_id)
        reservation_event = next(
            (
                event
                for event in budget_events
                if event.event_id
                == self._budget._event_id(
                    "reserve",
                    reservation_id=reservation_id,
                )
            ),
            None,
        )
        if reservation_event is None:
            raise CampaignJournalError(
                "Cycle resource reservation is missing"
            )
        reservation = _event_domain_payload(reservation_event)
        allocations: list[OperationalModelCallLimits] = []
        rows = connection.execute(
            "SELECT * FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? AND cycle_id = ? "
            "AND aggregate_type = ? AND event_type = ? "
            "ORDER BY sequence",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                cycle_id,
                _MODEL_CALL_AGGREGATE_TYPE,
                _MODEL_CALL_STARTED,
            ),
        ).fetchall()
        start_events = tuple(
            self._journal._event_in_transaction(
                connection,
                str(row["event_id"]),
            )
            for row in rows
        )
        for event in start_events:
            if event is None or event.aggregate_id not in expected_call_ids:
                raise CampaignJournalError(
                    "model call allocation inventory conflicts"
                )
            payload = _event_domain_payload(event)
            allocations.append(
                self._model_call_limits_from_payload(
                    payload.get("call_limits")
                )
            )
        allocations.append(requested)
        allocated_input = sum(item.max_input_tokens for item in allocations)
        allocated_output = sum(item.max_output_tokens for item in allocations)
        allocated_cost = sum(
            (_bounded_cost(item.max_cost) for item in allocations),
            Decimal("0"),
        )
        allocated_wall_time = sum(
            item.max_wall_time_ms for item in allocations
        )
        allocated_attempts = sum(item.max_attempts for item in allocations)
        if (
            allocated_input > reservation["max_input_tokens"]
            or allocated_output > reservation["max_output_tokens"]
            or allocated_cost > _bounded_cost(reservation["max_cost"])
            or allocated_wall_time > reservation["max_wall_time_ms"]
            or allocated_attempts > reservation["max_tool_attempts"]
        ):
            raise BudgetExceededError(
                "model call allocations exceed the Cycle reservation"
            )

    def _begin_model_call_in_transaction(
        self,
        connection,
        *,
        execution: ExecutingOperationalCycle,
        cycle_id: str,
        call_id: str,
        fixed_identity: dict[str, object],
        usage: OperationalUsageJournal,
    ) -> ExecutedOperationalModelCall | object | None:
        self._require_active_execution_in_transaction(connection, execution)
        self._require_model_attempt_inventory_in_transaction(
            connection,
            cycle_id=cycle_id,
            usage=usage,
        )
        other_in_doubt = self._other_in_doubt_model_call_in_transaction(
            connection,
            cycle_id=cycle_id,
            requested_call_id=call_id,
            requested_identity=fixed_identity,
            usage=usage,
        )
        if other_in_doubt is not None:
            self._lifecycle._block_in_transaction(
                connection,
                reason_code="MODEL_CALL_IN_DOUBT",
                source_ref=other_in_doubt.event_id,
            )
            return _MODEL_CALL_IN_DOUBT_RESULT
        events = self._model_call_events_in_transaction(
            connection,
            cycle_id=cycle_id,
            call_id=call_id,
        )
        if len(events) == 2:
            replay = self._replay_model_call_in_transaction(
                connection,
                cycle_id=cycle_id,
                call_id=call_id,
                expected_identity=fixed_identity,
                usage=usage,
            )
            try:
                self._require_known_model_call_usage_within_limits(
                    usage_attempts=replay.usage_attempts,
                    wall_time_ms=replay.wall_time_ms,
                    attempt_count=replay.attempt_count,
                    limits=self._model_call_limits_from_payload(
                        fixed_identity["call_limits"]
                    ),
                )
            except BudgetExceededError:
                self._lifecycle._block_in_transaction(
                    connection,
                    reason_code="MODEL_CALL_BUDGET_EXCEEDED",
                    source_ref=events[-1].event_id,
                )
                return _MODEL_CALL_BUDGET_EXCEEDED_RESULT
            return replay
        start_manifest_sha256 = _controller_sha256(
            b"control_plane.operational_model_call_start.v1",
            fixed_identity,
            "operational model call start",
        )
        start_payload = {
            **fixed_identity,
            "manifest_sha256": start_manifest_sha256,
        }
        if events:
            if (
                len(events) != 1
                or events[0].event_id
                != self._model_call_event_id(cycle_id, call_id, "start")
                or events[0].event_type != _MODEL_CALL_STARTED
                or _canonical_json_text(
                    _event_domain_payload(events[0]),
                    "stored operational model call start",
                )
                != _canonical_json_text(
                    start_payload,
                    "expected operational model call start",
                )
            ):
                raise CampaignJournalError(
                    "operational model call start conflicts"
                )
            self._lifecycle._block_in_transaction(
                connection,
                reason_code="MODEL_CALL_IN_DOUBT",
                source_ref=events[0].event_id,
            )
            return _MODEL_CALL_IN_DOUBT_RESULT
        self._require_model_call_allocation_in_transaction(
            connection,
            cycle_id=cycle_id,
            call_id=call_id,
            call_limits=fixed_identity["call_limits"],
        )
        self._journal._append_in_transaction(
            connection,
            event_id=self._model_call_event_id(cycle_id, call_id, "start"),
            cycle_id=cycle_id,
            aggregate_type=_MODEL_CALL_AGGREGATE_TYPE,
            aggregate_id=call_id,
            event_type=_MODEL_CALL_STARTED,
            payload=start_payload,
        )
        return None

    def _require_model_attempt_inventory_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        usage: OperationalUsageJournal,
    ) -> None:
        roster = self._roster._replay(
            self._roster._events(connection, cycle_id)
        )
        members_by_call_id = {
            self._member_call_id(cycle_id, member.member_id): member
            for member in roster.members
        }
        attempts_by_call_id: dict[str, list[RecordedModelAttempt]] = {}
        for attempt in usage._list_attempts_in_transaction(
            connection,
            call_id=None,
        ):
            attempts_by_call_id.setdefault(
                attempt.envelope.call_id,
                [],
            ).append(attempt)
        for stored_call_id, attempts in attempts_by_call_id.items():
            member = members_by_call_id.get(stored_call_id)
            events = self._model_call_events_in_transaction(
                connection,
                cycle_id=cycle_id,
                call_id=stored_call_id,
            )
            if member is None or not events:
                raise CampaignJournalError(
                    "model attempt inventory conflicts with the frozen roster"
                )
            start_event = events[0]
            start_payload = _event_domain_payload(start_event)
            if (
                start_event.event_id
                != self._model_call_event_id(
                    cycle_id,
                    stored_call_id,
                    "start",
                )
                or start_event.event_type != _MODEL_CALL_STARTED
                or start_payload.get("cycle_id") != cycle_id
                or start_payload.get("call_id") != stored_call_id
                or start_payload.get("member_id") != member.member_id
            ):
                raise CampaignJournalError(
                    "model attempt inventory has an invalid call start"
                )
            limits = self._model_call_limits_from_payload(
                start_payload.get("call_limits")
            )
            expected_attempt_ids = tuple(
                f"{stored_call_id}-attempt-{index:03d}"
                for index in range(1, len(attempts) + 1)
            )
            if (
                len(attempts) > limits.max_attempts
                or tuple(
                    attempt.envelope.attempt_id for attempt in attempts
                )
                != expected_attempt_ids
            ):
                raise CampaignJournalError(
                    "model attempt inventory exceeds its frozen call limits"
                )

    def _other_in_doubt_model_call_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        requested_call_id: str,
        requested_identity: dict[str, object],
        usage: OperationalUsageJournal,
    ):
        roster = self._roster._replay(
            self._roster._events(connection, cycle_id)
        )
        members_by_call_id = {
            self._member_call_id(cycle_id, member.member_id): member
            for member in roster.members
        }
        rows = connection.execute(
            "SELECT DISTINCT aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? AND cycle_id = ? "
            "AND aggregate_type = ? ORDER BY aggregate_id",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                cycle_id,
                _MODEL_CALL_AGGREGATE_TYPE,
            ),
        ).fetchall()
        for row in rows:
            existing_call_id = str(row["aggregate_id"])
            if existing_call_id == requested_call_id:
                continue
            member = members_by_call_id.get(existing_call_id)
            if member is None:
                raise CampaignJournalError(
                    "model call inventory conflicts with the frozen roster"
                )
            events = self._model_call_events_in_transaction(
                connection,
                cycle_id=cycle_id,
                call_id=existing_call_id,
            )
            if len(events) not in {1, 2}:
                raise CampaignJournalError(
                    "operational model call journal is incomplete or ambiguous"
                )
            start_event = events[0]
            start_payload = _event_domain_payload(start_event)
            if (
                start_event.event_id
                != self._model_call_event_id(
                    cycle_id,
                    existing_call_id,
                    "start",
                )
                or start_event.event_type != _MODEL_CALL_STARTED
                or start_payload.get("cycle_id") != cycle_id
                or start_payload.get("call_id") != existing_call_id
                or start_payload.get("member_id") != member.member_id
            ):
                raise CampaignJournalError(
                    "operational model call start conflicts"
                )
            self._model_call_limits_from_payload(
                start_payload.get("call_limits")
            )
            if len(events) == 1:
                return start_event
            try:
                self._model_call_for_member_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    member=member,
                    preparation_manifest_sha256=str(
                        requested_identity["preparation_manifest_sha256"]
                    ),
                    context_manifest_sha256=str(
                        requested_identity["context_manifest_sha256"]
                    ),
                    roster_manifest_sha256=str(
                        requested_identity["roster_manifest_sha256"]
                    ),
                    usage=usage,
                )
            except (BudgetExceededError, CampaignJournalError):
                return start_event
        return None

    def _replay_model_call_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        call_id: str,
        expected_identity: dict[str, object],
        usage: OperationalUsageJournal,
    ) -> ExecutedOperationalModelCall:
        call_limits = self._model_call_limits_from_payload(
            expected_identity.get("call_limits")
        )
        events = self._model_call_events_in_transaction(
            connection,
            cycle_id=cycle_id,
            call_id=call_id,
        )
        if len(events) != 2:
            raise CampaignJournalError(
                "operational model call result is missing or ambiguous"
            )
        start_event, event = events
        start_payload = {
            **expected_identity,
            "manifest_sha256": _controller_sha256(
                b"control_plane.operational_model_call_start.v1",
                expected_identity,
                "expected operational model call start",
            ),
        }
        if (
            start_event.event_id
            != self._model_call_event_id(cycle_id, call_id, "start")
            or start_event.event_type != _MODEL_CALL_STARTED
            or _canonical_json_text(
                _event_domain_payload(start_event),
                "stored operational model call start",
            )
            != _canonical_json_text(
                start_payload,
                "expected operational model call start",
            )
        ):
            raise CampaignJournalError(
                "operational model call start conflicts"
            )
        payload = _event_domain_payload(event)
        dynamic_fields = {
            "attempt_id",
            "attempt_count",
            "wall_time_ms",
            "output",
            "output_sha256",
            "verified_response_event_id",
            "manifest_sha256",
        }
        if (
            event.event_id
            != self._model_call_event_id(cycle_id, call_id, "complete")
            or event.event_type != _MODEL_CALL_COMPLETED
            or event.sequence <= start_event.sequence
            or set(payload) != set(expected_identity) | dynamic_fields
            or _canonical_json_text(
                {key: payload.get(key) for key in expected_identity},
                "stored operational model call identity",
            )
            != _canonical_json_text(
                expected_identity,
                "expected operational model call identity",
            )
        ):
            raise CampaignJournalError(
                "operational model call identity conflicts"
            )
        attempt_count = payload["attempt_count"]
        attempt_id = payload["attempt_id"]
        wall_time_ms = payload["wall_time_ms"]
        output = payload["output"]
        try:
            output_json = _canonical_json_text(
                output,
                "stored operational model output",
            )
        except ValueError as error:
            raise CampaignJournalError(
                "operational model call output is invalid"
            ) from error
        if len(output_json.encode("utf-8")) > _MAX_OPERATIONAL_OUTPUT_BYTES:
            raise CampaignJournalError(
                "operational model call output exceeds the bounded size"
            )
        receipt_identity = {
            key: payload[key] for key in payload if key != "manifest_sha256"
        }
        if (
            type(attempt_count) is not int
            or not 1 <= attempt_count <= call_limits.max_attempts
            or attempt_id != f"{call_id}-attempt-{attempt_count:03d}"
            or (
                wall_time_ms is not None
                and (type(wall_time_ms) is not int or wall_time_ms < 0)
            )
            or payload["output_sha256"]
            != _controller_sha256(
                b"control_plane.operational_model_output.v1",
                output,
                "stored operational model output",
            )
            or payload["manifest_sha256"]
            != _controller_sha256(
                b"control_plane.operational_model_call_receipt.v1",
                receipt_identity,
                "stored operational model call receipt",
            )
        ):
            raise CampaignJournalError(
                "operational model call receipt is invalid"
            )
        attempts = usage._list_attempts_in_transaction(
            connection,
            call_id=call_id,
        )
        expected_attempt_ids = tuple(
            f"{call_id}-attempt-{index:03d}"
            for index in range(1, attempt_count + 1)
        )
        retryable_failures = {
            InvocationOutcome.EMPTY_OUTPUT,
            InvocationOutcome.INVALID_JSON,
            InvocationOutcome.TIMEOUT,
            InvocationOutcome.EXCEPTION,
        }
        if (
            len(attempts) != attempt_count
            or tuple(
                attempt.envelope.attempt_id for attempt in attempts
            )
            != expected_attempt_ids
            or any(
                attempt.final_outcome not in retryable_failures
                for attempt in attempts[:-1]
            )
            or attempts[-1].envelope.attempt_id != attempt_id
            or attempts[-1].final_outcome is not InvocationOutcome.SUCCESS
        ):
            raise CampaignJournalError(
                "operational model call usage binding is invalid"
            )
        roster_events = self._roster._events(connection, cycle_id)
        roster_history = self._roster._replay_history(
            connection,
            roster_events,
        )
        member_id = str(expected_identity["member_id"])
        verified_event_id = self._roster._event_id(
            cycle_id,
            f"verified:{member_id}",
        )
        verified_event = next(
            (
                roster_event
                for roster_event in roster_events
                if roster_event.event_id == verified_event_id
            ),
            None,
        )
        first_attempt_events = usage._events_in_transaction(
            connection,
            _attempt_id(
                cycle_id,
                call_id,
                attempts[0].envelope.attempt_id,
            ),
        )
        last_attempt_events = usage._events_in_transaction(
            connection,
            _attempt_id(
                cycle_id,
                call_id,
                attempts[-1].envelope.attempt_id,
            ),
        )
        if (
            member_id not in roster_history.verified_member_ids
            or payload["verified_response_event_id"] != verified_event_id
            or verified_event is None
            or not first_attempt_events
            or not last_attempt_events
            or start_event.sequence >= first_attempt_events[0].sequence
            or last_attempt_events[-1].sequence >= verified_event.sequence
            or verified_event.sequence >= event.sequence
            or attempts[-1].envelope.response_model is None
        ):
            raise CampaignJournalError(
                "operational model call roster binding is invalid"
            )
        return ExecutedOperationalModelCall(
            cycle_id=cycle_id,
            call_id=call_id,
            member_id=member_id,
            output_json=output_json,
            attempt_id=attempt_id,
            attempt_count=attempt_count,
            wall_time_ms=wall_time_ms,
            usage_attempts=attempts,
            verified_response=VerifiedRosterResponse(
                member_id=member_id,
                response_model=attempts[-1].envelope.response_model,
                event_id=verified_event_id,
            ),
            manifest_sha256=payload["manifest_sha256"],
            event_id=event.event_id,
        )

    def _model_call_event_id(
        self,
        cycle_id: str,
        call_id: str,
        role: str,
    ) -> str:
        return _stable_id(
            b"control_plane.controller_model_call_result.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
            call_id,
            role,
        )

    def _model_call_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        call_id: str,
    ):
        rows = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND aggregate_id = ?",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _MODEL_CALL_AGGREGATE_TYPE,
                call_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id or row["aggregate_id"] != call_id
            for row in rows
        ):
            raise CampaignJournalError(
                "operational model call stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_MODEL_CALL_AGGREGATE_TYPE,
            aggregate_id=call_id,
        )

    def _safe_monotonic_ns(self) -> int | None:
        try:
            value = self._monotonic_ns()
        except Exception:
            return None
        if type(value) is not int or value < 0:
            return None
        return value

    @staticmethod
    def _elapsed_wall_time_ms(
        started_monotonic_ns: int | None,
        finished_monotonic_ns: int | None,
    ) -> int | None:
        if (
            started_monotonic_ns is None
            or finished_monotonic_ns is None
            or finished_monotonic_ns < started_monotonic_ns
        ):
            return None
        elapsed_ns = finished_monotonic_ns - started_monotonic_ns
        return (elapsed_ns + 999_999) // 1_000_000

    def _require_active_execution(
        self,
        execution: ExecutingOperationalCycle,
    ) -> str:
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._require_active_execution_in_transaction(
                connection,
                execution,
            )
        )

    def _require_active_execution_in_transaction(
        self,
        connection,
        execution: ExecutingOperationalCycle,
    ) -> str:
        if not isinstance(execution, ExecutingOperationalCycle):
            raise TypeError("execution must be an ExecutingOperationalCycle")
        cycle_id = _identifier(execution.cycle.cycle_id, "execution.cycle_id")
        if (
            execution.lease.cycle_id != cycle_id
            or execution.cycle.status is not CycleStatus.EXECUTING
        ):
            raise CampaignJournalError("execution receipt is invalid")
        try:
            current_owner = _verified_current_owner(
                self._leases._identity_provider
            )
        except CycleLeaseConflictError as error:
            raise CampaignJournalError("execution receipt is stale") from error
        if (
            current_owner != self._leases._owner
            or execution.lease.owner != current_owner
        ):
            raise CampaignJournalError("execution receipt is stale")
        cycle = self._lifecycle._replay_cycle(
            self._lifecycle._cycle_events(connection, cycle_id)
        )
        campaign = self._lifecycle._replay_campaign(
            self._lifecycle._campaign_events(connection)
        )
        lease_history = self._leases._replay(
            self._leases._events(connection, cycle_id)
        )
        if (
            campaign.status is not CampaignStatus.ACTIVE
            or cycle != execution.cycle
            or not self._same_fencing_generation(
                lease_history.active,
                execution.lease,
            )
        ):
            raise CampaignJournalError("execution receipt is stale")
        return cycle_id

    def _require_evidence_execution_generation_in_transaction(
        self,
        connection,
        execution: ExecutingOperationalCycle,
        *,
        allow_information_gain_recorded: bool = False,
        allow_cycle_completed: bool = False,
    ) -> tuple[str, CycleSnapshot]:
        if not isinstance(execution, ExecutingOperationalCycle):
            raise TypeError("execution must be an ExecutingOperationalCycle")
        cycle_id = _identifier(execution.cycle.cycle_id, "execution.cycle_id")
        allowed_execution_statuses = {
            CycleStatus.EXECUTING,
            CycleStatus.EVIDENCE_READY,
            CycleStatus.LEARNING_COMMITTED,
            CycleStatus.SETTLED,
        }
        if allow_information_gain_recorded:
            allowed_execution_statuses.add(
                CycleStatus.INFORMATION_GAIN_RECORDED
            )
        if allow_cycle_completed:
            allowed_execution_statuses.update(
                {
                    CycleStatus.INFORMATION_GAIN_RECORDED,
                    CycleStatus.NEXT_CYCLE_DECIDED,
                    CycleStatus.COMPLETED,
                }
            )
        if (
            execution.lease.cycle_id != cycle_id
            or execution.cycle.status not in allowed_execution_statuses
        ):
            raise CampaignJournalError("execution receipt is invalid")
        try:
            current_owner = _verified_current_owner(
                self._leases._identity_provider
            )
        except CycleLeaseConflictError as error:
            raise CampaignJournalError("execution receipt is stale") from error
        if (
            current_owner != self._leases._owner
            or execution.lease.owner != current_owner
        ):
            raise CampaignJournalError("execution receipt is stale")
        cycle_events = self._lifecycle._cycle_events(connection, cycle_id)
        current_cycle = self._lifecycle._replay_cycle(cycle_events)
        original_events = tuple(
            event
            for event in cycle_events
            if event.sequence <= execution.cycle.sequence
        )
        campaign = self._lifecycle._replay_campaign(
            self._lifecycle._campaign_events(connection)
        )
        lease_history = self._leases._replay(
            self._leases._events(connection, cycle_id)
        )
        allowed_current_statuses = {
            CycleStatus.EXECUTING,
            CycleStatus.EVIDENCE_READY,
            CycleStatus.LEARNING_COMMITTED,
            CycleStatus.SETTLED,
        }
        if allow_information_gain_recorded:
            allowed_current_statuses.add(
                CycleStatus.INFORMATION_GAIN_RECORDED
            )
        if allow_cycle_completed:
            allowed_current_statuses.update(
                {
                    CycleStatus.INFORMATION_GAIN_RECORDED,
                    CycleStatus.NEXT_CYCLE_DECIDED,
                    CycleStatus.COMPLETED,
                }
            )
        campaign_status_allowed = (
            campaign.status is CampaignStatus.ACTIVE
            or (
                allow_cycle_completed
                and current_cycle.status is CycleStatus.COMPLETED
                and campaign.status is CampaignStatus.COMPLETED
            )
        )
        if (
            not campaign_status_allowed
            or current_cycle.status not in allowed_current_statuses
            or not original_events
            or self._lifecycle._replay_cycle(original_events)
            != execution.cycle
            or not self._same_fencing_generation(
                lease_history.active,
                execution.lease,
            )
        ):
            raise CampaignJournalError("execution receipt is stale")
        return cycle_id, current_cycle

    @staticmethod
    def _same_fencing_generation(
        active: CycleLease,
        expected: CycleLease,
    ) -> bool:
        return (
            active.cycle_id == expected.cycle_id
            and active.acquisition_id == expected.acquisition_id
            and active.lease_id == expected.lease_id
            and active.fencing_token == expected.fencing_token
            and active.owner == expected.owner
        )

    def _member_call_id(self, cycle_id: str, member_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_member_call.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
            member_id,
        )

    @staticmethod
    def _reservation_payload(
        reservation: BudgetReservation,
    ) -> dict[str, object]:
        return {
            "reservation_id": reservation.reservation_id,
            "call_id": reservation.call_id,
            "max_input_tokens": reservation.max_input_tokens,
            "max_output_tokens": reservation.max_output_tokens,
            "max_cost": reservation.max_cost,
            "max_wall_time_ms": reservation.max_wall_time_ms,
            "max_tool_attempts": reservation.max_tool_attempts,
            "max_data_exposures": reservation.max_data_exposures,
            "max_disk_growth_bytes": reservation.max_disk_growth_bytes,
        }

    def _controller_artifact_identity_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        frozen: FrozenCycleInputs,
        allow_settled_reservation: bool = False,
    ) -> tuple[
        dict[str, object],
        dict[str, object],
        dict[str, object],
        int,
    ]:
        work_event, work_item = self._replay_work_item(
            cycle_id=cycle_id,
            events=self._work_item_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            ),
        )
        cycle_budget = self._cycle_budget._snapshot_in_transaction(connection)
        if cycle_id not in cycle_budget.reserved_cycle_ids:
            raise CampaignJournalError(
                "controller preparation is incomplete: Cycle slot is missing"
            )
        budget_events = self._budget._events_in_transaction(connection)
        self._budget._replay(budget_events)
        reservation_id = self._reservation_id(cycle_id)
        reservation_events = tuple(
            event
            for event in budget_events
            if event.event_id
            == self._budget._event_id(
                "reserve",
                reservation_id=reservation_id,
            )
        )
        if len(reservation_events) != 1:
            raise CampaignJournalError(
                "controller preparation is incomplete: resource reservation is missing"
            )
        reservation_event = reservation_events[0]
        reservation_payload = _event_domain_payload(reservation_event)
        if (
            reservation_event.event_type != _BUDGET_RESERVED
            or reservation_payload.get("reservation_id") != reservation_id
            or reservation_payload.get("call_id") != cycle_id
            or (
                not allow_settled_reservation
                and any(
                    event.event_type == _BUDGET_SETTLED
                    and _event_domain_payload(event).get("reservation_id")
                    == reservation_id
                    for event in budget_events
                )
            )
        ):
            raise CampaignJournalError(
                "controller preparation is incomplete: resource reservation is invalid"
            )
        cycle_events = self._lifecycle._cycle_events(connection, cycle_id)
        cycle = self._lifecycle._replay_cycle(cycle_events)
        opened = cycle_events[0]
        frozen_transitions = tuple(
            event
            for event in cycle_events
            if event.event_type == _CYCLE_TRANSITIONED
            and _event_domain_payload(event).get("to_status")
            == CycleStatus.FROZEN.value
        )
        if (
            work_item["cycle_number"] != cycle.cycle_number
            or work_event.sequence >= opened.sequence
            or reservation_event.sequence >= opened.sequence
            or frozen.cycle_id != cycle_id
            or len(frozen_transitions) != 1
            or frozen_transitions[0].sequence <= frozen.sequence
            or _controller_sha256(
                b"control_plane.campaign_proposal.v1",
                work_item["proposal"],
                "stored Campaign proposal",
            )
            != frozen.proposal_sha256
        ):
            raise CampaignJournalError(
                "controller preparation is incomplete: artifact binding is invalid"
            )
        identity = {
            "schema_version": "control_plane.campaign_cycle_preparation.v1",
            "cycle_id": cycle_id,
            "cycle_number": cycle.cycle_number,
            "cycle_budget_id": cycle_budget.budget_id,
            "resource_budget_id": self._budget._budget_id,
            "work_item_event_id": work_event.event_id,
            "work_item_sha256": _controller_sha256(
                b"control_plane.controller_work_item_payload.v1",
                work_item,
                "stored Campaign work item",
            ),
            "reservation_event_id": reservation_event.event_id,
            "reservation_id": reservation_id,
            "reservation_sha256": _controller_sha256(
                b"control_plane.controller_cycle_reservation_bounds.v1",
                reservation_payload,
                "stored Cycle reservation",
            ),
            "context_manifest_sha256": frozen.context_manifest_sha256,
            "roster_manifest_sha256": frozen.roster_manifest_sha256,
            "freeze_event_id": frozen.event_id,
            "freeze_manifest_sha256": frozen.manifest_sha256,
            "frozen_transition_event_id": frozen_transitions[0].event_id,
        }
        return (
            identity,
            work_item,
            reservation_payload,
            frozen_transitions[0].sequence,
        )

    def _record_cycle_preparation(
        self,
        *,
        cycle_id: str,
        expected_work_item: dict[str, object],
        expected_reservation: BudgetReservation,
        expected_context: CycleContextReceipt,
        expected_roster: RosterManifest,
        expected_frozen: FrozenCycleInputs,
    ) -> str:
        def record(connection) -> str:
            identity, work_item, reservation_payload, minimum_sequence = (
                self._controller_artifact_identity_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    frozen=expected_frozen,
                )
            )
            if (
                _canonical_json_text(work_item, "stored Campaign work item")
                != _canonical_json_text(
                    expected_work_item,
                    "expected Campaign work item",
                )
                or _canonical_json_text(
                    reservation_payload,
                    "stored Cycle reservation",
                )
                != _canonical_json_text(
                    self._reservation_payload(expected_reservation),
                    "expected Cycle reservation",
                )
                or identity["context_manifest_sha256"]
                != expected_context.manifest_sha256
                or identity["roster_manifest_sha256"]
                != expected_roster.manifest_sha256
                or identity["freeze_manifest_sha256"]
                != expected_frozen.manifest_sha256
            ):
                raise CampaignJournalError(
                    "controller preparation artifacts conflict"
                )
            manifest_sha256 = _controller_sha256(
                b"control_plane.campaign_cycle_preparation.v1",
                identity,
                "Cycle preparation identity",
            )
            payload = {**identity, "manifest_sha256": manifest_sha256}
            events = self._preparation_events_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
            if events:
                self._replay_preparation(
                    cycle_id=cycle_id,
                    events=events,
                    expected_payload=payload,
                    minimum_sequence=minimum_sequence,
                )
                return manifest_sha256
            event = self._journal._append_in_transaction(
                connection,
                event_id=self._preparation_event_id(cycle_id),
                cycle_id=cycle_id,
                aggregate_type=_PREPARATION_AGGREGATE_TYPE,
                aggregate_id=cycle_id,
                event_type=_CYCLE_PREPARED,
                payload=payload,
            )
            if event.sequence <= minimum_sequence:
                raise CampaignJournalError(
                    "Cycle preparation must follow the frozen inputs"
                )
            return manifest_sha256

        return _SqliteUnitOfWork(stores._operational_spec())._write(record)

    def _preparation_snapshot(
        self,
        *,
        cycle_id: str,
        frozen: FrozenCycleInputs,
    ) -> str:
        def snapshot(connection) -> str:
            identity, _, _, minimum_sequence = (
                self._controller_artifact_identity_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                    frozen=frozen,
                )
            )
            manifest_sha256 = _controller_sha256(
                b"control_plane.campaign_cycle_preparation.v1",
                identity,
                "Cycle preparation identity",
            )
            payload = {**identity, "manifest_sha256": manifest_sha256}
            self._replay_preparation(
                cycle_id=cycle_id,
                events=self._preparation_events_in_transaction(
                    connection,
                    cycle_id=cycle_id,
                ),
                expected_payload=payload,
                minimum_sequence=minimum_sequence,
            )
            return manifest_sha256

        return _SqliteUnitOfWork(stores._operational_spec())._read(snapshot)

    def _preparation_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_cycle_preparation_event.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _preparation_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        related_streams = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _PREPARATION_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_id"] != cycle_id
            for row in related_streams
        ):
            raise CampaignJournalError(
                "Campaign Cycle preparation stream conflicts"
            )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_PREPARATION_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _replay_preparation(
        self,
        *,
        cycle_id: str,
        events,
        expected_payload: dict[str, object],
        minimum_sequence: int,
    ) -> None:
        if not events:
            raise CampaignJournalError(
                "controller preparation is incomplete: receipt is missing"
            )
        if len(events) != 1:
            raise CampaignJournalError(
                "Campaign Cycle preparation receipt conflicts"
            )
        event = events[0]
        payload = _event_domain_payload(event)
        if (
            event.event_id != self._preparation_event_id(cycle_id)
            or event.event_type != _CYCLE_PREPARED
            or event.sequence <= minimum_sequence
            or _canonical_json_text(
                payload,
                "stored Cycle preparation receipt",
            )
            != _canonical_json_text(
                expected_payload,
                "expected Cycle preparation receipt",
            )
        ):
            raise CampaignJournalError(
                "Campaign Cycle preparation receipt conflicts"
            )

    def _reservation_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_cycle_reservation.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _work_item_event_id(self, cycle_id: str) -> str:
        return _stable_id(
            b"control_plane.controller_work_item.v1",
            self._journal.namespace,
            self._journal.campaign_id,
            cycle_id,
        )

    def _work_item_events_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ):
        related_streams = connection.execute(
            "SELECT cycle_id, aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ? "
            "AND (cycle_id = ? OR aggregate_id = ?)",
            (
                self._journal.namespace,
                self._journal.campaign_id,
                _WORK_ITEM_AGGREGATE_TYPE,
                cycle_id,
                cycle_id,
            ),
        ).fetchall()
        if any(
            row["cycle_id"] != cycle_id
            or row["aggregate_id"] != cycle_id
            for row in related_streams
        ):
            raise CampaignJournalError("Campaign work item stream conflicts")
        return self._journal._list_in_transaction(
            connection,
            cycle_id=cycle_id,
            aggregate_type=_WORK_ITEM_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
        )

    def _replay_work_item(
        self,
        *,
        cycle_id: str,
        events,
    ):
        if not events:
            raise CampaignJournalError(
                "controller preparation is incomplete: work item is missing"
            )
        if (
            len(events) != 1
            or events[0].event_id != self._work_item_event_id(cycle_id)
            or events[0].event_type != _WORK_ITEM_ADOPTED
        ):
            raise CampaignJournalError("Campaign work item conflicts")
        payload = _event_domain_payload(events[0])
        if set(payload) != {
            "schema_version",
            "task_id",
            "cycle_number",
            "strategy",
            "proposal",
            "source",
            "priority",
        }:
            raise CampaignJournalError("Campaign work item conflicts")
        try:
            task = ExperimentTask(
                task_id=payload["task_id"],
                strategy=payload["strategy"],
                proposal=payload["proposal"],
                source=payload["source"],
                priority=payload["priority"],
            )
            replayed_cycle_id, canonical = _canonical_task(
                task,
                cycle_number=payload["cycle_number"],
            )
        except (TypeError, ValueError) as error:
            raise CampaignJournalError("Campaign work item conflicts") from error
        if (
            payload["schema_version"]
            != "control_plane.campaign_work_item.v1"
            or replayed_cycle_id != cycle_id
            or _canonical_json_text(payload, "stored Campaign work item")
            != _canonical_json_text(canonical, "canonical Campaign work item")
        ):
            raise CampaignJournalError("Campaign work item conflicts")
        return events[0], payload

    def _adopt_work_item_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
        payload: dict[str, object],
    ) -> None:
        events = self._work_item_events_in_transaction(
            connection,
            cycle_id=cycle_id,
        )
        if events:
            _, stored_payload = self._replay_work_item(
                cycle_id=cycle_id,
                events=events,
            )
            if _canonical_json_text(
                stored_payload,
                "stored Campaign work item",
            ) != _canonical_json_text(payload, "requested Campaign work item"):
                raise CampaignJournalError("Campaign work item conflicts")
            return
        self._journal._append_in_transaction(
            connection,
            event_id=self._work_item_event_id(cycle_id),
            cycle_id=cycle_id,
            aggregate_type=_WORK_ITEM_AGGREGATE_TYPE,
            aggregate_id=cycle_id,
            event_type=_WORK_ITEM_ADOPTED,
            payload=payload,
        )


__all__ = [
    "CampaignBudgetLimits",
    "CycleReservationLimits",
    "ExecutedOperationalModelCall",
    "ExecutingOperationalCycle",
    "OperationalCycleSettlementReceipt",
    "OperationalEvidenceReceipt",
    "OperationalExecutionUsage",
    "OperationalInformationGainReceipt",
    "OperationalLearningCommitReceipt",
    "OperationalNextCycleDecisionReceipt",
    "OperationalNoLearningSettlementReceipt",
    "OperationalCampaignController",
    "OperationalModelCallLimits",
    "PreparedOperationalCycle",
    "operational_prompt_sha256",
]
