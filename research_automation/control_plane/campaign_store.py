"""Grant-bound append-only OperationalJournal APIs for P6 campaign events."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Callable

from . import stores
from .budget import (
    BudgetConflictError,
    BudgetError,
    BudgetExceededError,
    BudgetLedger,
    BudgetReservation,
    BudgetSettlement,
    BudgetSnapshot,
    _canonical_currency,
    _cost,
    _cost_text,
)
from .campaign import InvocationOutcome, UsageEnvelope, UsageStatus
from .contracts import Phase, SideEffect
from .sqlite_uow import _SqliteUnitOfWork

if TYPE_CHECKING:
    from .campaign_lifecycle import CycleSnapshot, OperationalCampaignLifecycle
    from .evidence_learning import EvidenceResult, LearningCommitService


_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_MAX_EVENT_PAYLOAD_BYTES = 64 * 1024


class CampaignJournalError(RuntimeError):
    """Base error for P6 campaign journal operations."""


class CampaignEventConflictError(CampaignJournalError):
    """Raised when an event ID is replayed with different content."""


class DryRunIsolationError(CampaignJournalError):
    """Raised before a dry-run context can reach a formal state sink."""


class CampaignExecutionMode(str, Enum):
    FORMAL = "FORMAL"
    DRY_RUN = "DRY_RUN"


@dataclass(frozen=True)
class CampaignEvent:
    event_id: str
    namespace: str
    campaign_id: str
    cycle_id: str | None
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload_json: str
    payload_sha256: str
    occurred_at: datetime
    sequence: int

    def payload(self) -> dict[str, object]:
        value = json.loads(self.payload_json)
        if not isinstance(value, dict):
            raise CampaignJournalError("stored event payload is not an object")
        return value


@dataclass(frozen=True)
class RecordedModelAttempt:
    envelope: UsageEnvelope
    final_outcome: InvocationOutcome


def _identifier(value: str, name: str) -> str:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a bounded control-plane identifier")
    return value


def dry_run_namespace(dry_run_id: str) -> str:
    """Return the durable OperationalJournal namespace for one preview run."""
    identifier = _identifier(dry_run_id, "dry_run_id")
    return _identifier(f"dry-run:{identifier}", "namespace")


def campaign_execution_mode(namespace: str) -> CampaignExecutionMode:
    """Classify the closed P6 namespace contract without consulting filenames."""
    namespace = _identifier(namespace, "namespace")
    if namespace == "formal":
        return CampaignExecutionMode.FORMAL
    if namespace.startswith("dry-run:") and namespace != "dry-run:":
        return CampaignExecutionMode.DRY_RUN
    raise ValueError("namespace is outside the formal/dry-run campaign contract")


def _utc(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()


def _payload(value: Mapping[str, object]) -> tuple[str, str]:
    if not isinstance(value, Mapping):
        raise TypeError("payload must be a mapping")
    try:
        payload_json = json.dumps(
            dict(value),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, UnicodeError) as error:
        raise ValueError("campaign event payload is not canonical JSON") from error
    encoded = payload_json.encode("utf-8")
    if len(encoded) > _MAX_EVENT_PAYLOAD_BYTES:
        raise ValueError("campaign event payload exceeds the bounded size")
    return payload_json, hashlib.sha256(encoded).hexdigest()


def _event_integrity_sha256(
    *,
    event_id: str,
    namespace: str,
    campaign_id: str,
    cycle_id: str | None,
    aggregate_type: str,
    aggregate_id: str,
    event_type: str,
    payload_json: str,
    occurred_at: str,
    sequence: int,
) -> str:
    envelope = json.dumps(
        {
            "domain": "control_plane.campaign_event.v1",
            "event_id": event_id,
            "namespace": namespace,
            "campaign_id": campaign_id,
            "cycle_id": cycle_id,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "event_type": event_type,
            "payload_json": payload_json,
            "occurred_at": occurred_at,
            "sequence": sequence,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(envelope).hexdigest()


def campaign_scope_sha256(*, namespace: str, campaign_id: str) -> str:
    payload = json.dumps(
        {
            "campaign_id": _identifier(campaign_id, "campaign_id"),
            "namespace": _identifier(namespace, "namespace"),
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(b"control_plane.campaign_scope.v1\0" + payload).hexdigest()


def _require_p6_grant(
    grant: object,
    *,
    namespace: str | None = None,
    campaign_id: str | None = None,
) -> stores.AuthorityGrant:
    if not isinstance(grant, stores.AuthorityGrant) or grant.phase is not Phase.P6:
        raise PermissionError("an active P6 AuthorityGrant is required")
    required = {SideEffect.READ, SideEffect.WRITE_CONTROL_PLANE}
    if not required.issubset(grant.allowed_side_effects):
        raise PermissionError("P6 journal access requires READ and WRITE_CONTROL_PLANE")
    try:
        _SqliteUnitOfWork(stores._authority_spec())._read(
            lambda connection: stores._AuthorityStore._require_active_grant(
                connection,
                grant,
            )
        )
    except stores.AuthorizationRejectedError as error:
        raise PermissionError("the P6 AuthorityGrant is invalid or inactive") from error
    if namespace is not None or campaign_id is not None:
        if namespace is None or campaign_id is None:
            raise ValueError("namespace and campaign_id must be bound together")
        expected_scope = campaign_scope_sha256(
            namespace=namespace,
            campaign_id=campaign_id,
        )
        if not hmac.compare_digest(grant.identity.scope_hash, expected_scope):
            raise PermissionError("P6 AuthorityGrant does not match campaign scope")
    return grant


def _event_from_row(row: Mapping[str, object]) -> CampaignEvent:
    payload_json = str(row["payload_json"])
    payload_sha256 = str(row["payload_sha256"])
    observed_sha256 = _event_integrity_sha256(
        event_id=str(row["event_id"]),
        namespace=str(row["namespace"]),
        campaign_id=str(row["campaign_id"]),
        cycle_id=None if row["cycle_id"] is None else str(row["cycle_id"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=str(row["aggregate_id"]),
        event_type=str(row["event_type"]),
        payload_json=payload_json,
        occurred_at=str(row["occurred_at"]),
        sequence=int(row["sequence"]),
    )
    if not hmac.compare_digest(observed_sha256, payload_sha256):
        raise CampaignJournalError("stored event payload integrity mismatch")
    return CampaignEvent(
        event_id=str(row["event_id"]),
        namespace=str(row["namespace"]),
        campaign_id=str(row["campaign_id"]),
        cycle_id=None if row["cycle_id"] is None else str(row["cycle_id"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=str(row["aggregate_id"]),
        event_type=str(row["event_type"]),
        payload_json=payload_json,
        payload_sha256=payload_sha256,
        occurred_at=datetime.fromisoformat(str(row["occurred_at"])),
        sequence=int(row["sequence"]),
    )


class OperationalCampaignJournal:
    """Append and replay bounded P6 events in the fixed OperationalJournal.

    Event IDs are globally unique idempotency keys. Domain adapters derive them
    from namespace, campaign, cycle, aggregate, and event role so identities do
    not alias across Campaign or Cycle boundaries.

    Grant validation is the authorization linearization point. Revocation blocks
    operations that start after that check; an already-started local journal
    transaction is allowed to finish and remains attributable to its grant.
    Runtime cycle invalidation is handled separately by P6 fencing leases.
    """

    __slots__ = ("_clock", "_grant", "_namespace", "_campaign_id")

    def __init__(
        self,
        *,
        root_secret: str,
        grant: object,
        namespace: str,
        campaign_id: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._namespace = _identifier(namespace, "namespace")
        self._campaign_id = _identifier(campaign_id, "campaign_id")
        self._grant = _require_p6_grant(
            grant,
            namespace=self._namespace,
            campaign_id=self._campaign_id,
        )
        campaign_execution_mode(self._namespace)
        stores._migrate_operational_journal_v3(root_secret=root_secret)
        stores._require_store_root(stores._operational_spec(), root_secret)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    @property
    def execution_mode(self) -> CampaignExecutionMode:
        return campaign_execution_mode(self._namespace)

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def campaign_id(self) -> str:
        return self._campaign_id

    def require_formal_learning_sink(self) -> None:
        """Authorize this journal and fail before a preview reaches Learning."""
        self._authorize()
        if self.execution_mode is not CampaignExecutionMode.FORMAL:
            raise DryRunIsolationError(
                "dry-run Campaigns cannot write formal Learning state"
            )

    def append(
        self,
        *,
        event_id: str,
        cycle_id: str | None,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: Mapping[str, object],
    ) -> CampaignEvent:
        self._authorize()
        return _SqliteUnitOfWork(stores._operational_spec())._write(
            lambda connection: self._append_in_transaction(
                connection,
                event_id=event_id,
                cycle_id=cycle_id,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                event_type=event_type,
                payload=payload,
            )
        )

    def _authorize(self) -> None:
        _require_p6_grant(
            self._grant,
            namespace=self._namespace,
            campaign_id=self._campaign_id,
        )

    def _append_in_transaction(
        self,
        connection,
        *,
        event_id: str,
        cycle_id: str | None,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: Mapping[str, object],
    ) -> CampaignEvent:
        event_id = _identifier(event_id, "event_id")
        namespace = self._namespace
        campaign_id = self._campaign_id
        if cycle_id is not None:
            cycle_id = _identifier(cycle_id, "cycle_id")
        aggregate_type = _identifier(aggregate_type, "aggregate_type")
        aggregate_id = _identifier(aggregate_id, "aggregate_id")
        event_type = _identifier(event_type, "event_type")
        if "_authority_grant_id" in payload:
            raise ValueError("payload cannot override the AuthorityGrant binding")
        bound_payload = dict(payload)
        bound_payload["_authority_grant_id"] = self._grant.grant_id
        payload_json, _ = _payload(bound_payload)
        occurred_at = self._clock()
        occurred_text = _utc(occurred_at)
        event = self._event_in_transaction(connection, event_id)
        if event is not None:
            expected = (
                namespace,
                campaign_id,
                cycle_id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload_json,
            )
            observed = (
                event.namespace,
                event.campaign_id,
                event.cycle_id,
                event.aggregate_type,
                event.aggregate_id,
                event.event_type,
                event.payload_json,
            )
            if observed != expected:
                raise CampaignEventConflictError(
                    "event_id was replayed with different content"
                )
            return event
        connection.execute(
            """
            INSERT INTO campaign_events
            (event_id, namespace, campaign_id, cycle_id, aggregate_type,
             aggregate_id, event_type, payload_json, payload_sha256, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                namespace,
                campaign_id,
                cycle_id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload_json,
                "0" * 64,
                occurred_text,
            ),
        )
        sequence = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        integrity_sha256 = _event_integrity_sha256(
            event_id=event_id,
            namespace=namespace,
            campaign_id=campaign_id,
            cycle_id=cycle_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload_json=payload_json,
            occurred_at=occurred_text,
            sequence=sequence,
        )
        connection.execute(
            "UPDATE campaign_events SET payload_sha256 = ? WHERE sequence = ?",
            (integrity_sha256, sequence),
        )
        return CampaignEvent(
            event_id=event_id,
            namespace=namespace,
            campaign_id=campaign_id,
            cycle_id=cycle_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload_json=payload_json,
            payload_sha256=integrity_sha256,
            occurred_at=occurred_at,
            sequence=sequence,
        )

    @staticmethod
    def _event_in_transaction(
        connection,
        event_id: str,
    ) -> CampaignEvent | None:
        event_id = _identifier(event_id, "event_id")
        row = connection.execute(
            "SELECT * FROM campaign_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        return None if row is None else _event_from_row(row)

    def list_events(
        self,
        *,
        cycle_id: str | None,
        aggregate_type: str,
        aggregate_id: str,
    ) -> tuple[CampaignEvent, ...]:
        self._authorize()
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._list_in_transaction(
                connection,
                cycle_id=cycle_id,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            )
        )

    def _list_in_transaction(
        self,
        connection,
        *,
        cycle_id: str | None,
        aggregate_type: str,
        aggregate_id: str,
    ) -> tuple[CampaignEvent, ...]:
        values = tuple(
            _identifier(value, name)
            for value, name in (
                (self._namespace, "namespace"),
                (self._campaign_id, "campaign_id"),
                (aggregate_type, "aggregate_type"),
                (aggregate_id, "aggregate_id"),
            )
        )
        if cycle_id is not None:
            cycle_id = _identifier(cycle_id, "cycle_id")
            query = """
                SELECT * FROM campaign_events
                WHERE namespace = ? AND campaign_id = ? AND cycle_id = ?
                  AND aggregate_type = ? AND aggregate_id = ?
                ORDER BY sequence
            """
            query_values = (*values[:2], cycle_id, *values[2:])
        else:
            query = """
                SELECT * FROM campaign_events
                WHERE namespace = ? AND campaign_id = ? AND cycle_id IS NULL
                  AND aggregate_type = ? AND aggregate_id = ?
                ORDER BY sequence
            """
            query_values = values
        rows = connection.execute(query, query_values).fetchall()
        return tuple(_event_from_row(row) for row in rows)


class CampaignLearningCommitSink:
    """Bind the formal P4 Learning service to one P6 Campaign namespace."""

    __slots__ = ("_journal", "_service")

    def __init__(
        self,
        *,
        journal: OperationalCampaignJournal,
        service: LearningCommitService,
    ) -> None:
        from .evidence_learning import LearningCommitService

        if not isinstance(journal, OperationalCampaignJournal):
            raise TypeError("journal must be an OperationalCampaignJournal")
        if type(service) is not LearningCommitService:
            raise TypeError("service must be a LearningCommitService")
        journal._authorize()
        object.__setattr__(self, "_journal", journal)
        object.__setattr__(self, "_service", service)

    def __setattr__(self, _name: str, _value: object) -> None:
        raise AttributeError("CampaignLearningCommitSink is immutable")

    def __delattr__(self, _name: str) -> None:
        raise AttributeError("CampaignLearningCommitSink is immutable")

    def commit(
        self,
        task_report: Mapping[str, object],
        *,
        expected_artifact: Mapping[str, object] | None = None,
        expected_evidence: EvidenceResult | None = None,
    ) -> str:
        self._journal.require_formal_learning_sink()
        return self._service.commit(
            task_report,
            expected_artifact=expected_artifact,
            expected_evidence=expected_evidence,
        )


_BUDGET_AGGREGATE_TYPE = "CAMPAIGN_BUDGET"
_BUDGET_OPENED = "BUDGET_OPENED"
_BUDGET_RESERVED = "BUDGET_RESERVED"
_BUDGET_SETTLED = "BUDGET_SETTLED"


def _require_single_campaign_aggregate_id(
    connection,
    *,
    journal: OperationalCampaignJournal,
    aggregate_type: str,
    aggregate_id: str,
    conflict_message: str,
) -> None:
    aggregate_ids = {
        str(row["aggregate_id"])
        for row in connection.execute(
            "SELECT DISTINCT aggregate_id FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND aggregate_type = ?",
            (
                journal._namespace,
                journal._campaign_id,
                aggregate_type,
            ),
        )
    }
    if any(observed != aggregate_id for observed in aggregate_ids):
        raise BudgetConflictError(conflict_message)


def _budget_event_id(
    *,
    namespace: str,
    campaign_id: str,
    budget_id: str,
    role: str,
    reservation_id: str | None = None,
) -> str:
    parts = [namespace, campaign_id, budget_id, role]
    if reservation_id is not None:
        parts.append(reservation_id)
    return hashlib.sha256(
        b"control_plane.campaign_budget_event.v1\0"
        + "\0".join(parts).encode("ascii")
    ).hexdigest()


def _event_domain_payload(event: CampaignEvent) -> dict[str, object]:
    payload = event.payload()
    grant_id = payload.pop("_authority_grant_id", None)
    try:
        _identifier(grant_id, "stored AuthorityGrant id")
    except (TypeError, ValueError) as error:
        raise CampaignJournalError(
            "campaign event is missing its AuthorityGrant binding"
        ) from error
    return payload


class OperationalBudgetJournal:
    """Persistent Campaign budget with cross-process atomic reservations."""

    __slots__ = (
        "_journal",
        "_budget_id",
        "_currency",
        "_max_input_tokens",
        "_max_output_tokens",
        "_max_cost",
        "_max_wall_time_ms",
        "_max_tool_attempts",
        "_max_data_exposures",
        "_max_disk_growth_bytes",
    )

    def __init__(
        self,
        *,
        journal: OperationalCampaignJournal,
        budget_id: str,
        currency: str,
        max_input_tokens: int,
        max_output_tokens: int,
        max_cost: str | int | Decimal,
        max_wall_time_ms: int = 0,
        max_tool_attempts: int = 0,
        max_data_exposures: int = 0,
        max_disk_growth_bytes: int = 0,
    ) -> None:
        if not isinstance(journal, OperationalCampaignJournal):
            raise TypeError("journal must be an OperationalCampaignJournal")
        journal._authorize()
        self._journal = journal
        self._budget_id = _identifier(budget_id, "budget_id")
        self._currency = _canonical_currency(currency)
        BudgetLedger(
            currency=self._currency,
            max_input_tokens=max_input_tokens,
            max_output_tokens=max_output_tokens,
            max_cost=max_cost,
            max_wall_time_ms=max_wall_time_ms,
            max_tool_attempts=max_tool_attempts,
            max_data_exposures=max_data_exposures,
            max_disk_growth_bytes=max_disk_growth_bytes,
        )
        self._max_input_tokens = max_input_tokens
        self._max_output_tokens = max_output_tokens
        self._max_cost = _cost_text(_cost(max_cost))
        self._max_wall_time_ms = max_wall_time_ms
        self._max_tool_attempts = max_tool_attempts
        self._max_data_exposures = max_data_exposures
        self._max_disk_growth_bytes = max_disk_growth_bytes

        def open_budget(connection) -> None:
            events = self._events_in_transaction(connection)
            if events:
                self._replay(events)
                return
            self._journal._append_in_transaction(
                connection,
                event_id=self._event_id("open"),
                cycle_id=None,
                aggregate_type=_BUDGET_AGGREGATE_TYPE,
                aggregate_id=self._budget_id,
                event_type=_BUDGET_OPENED,
                payload=self._limits_payload(),
            )

        _SqliteUnitOfWork(stores._operational_spec())._write(open_budget)

    def reserve(
        self,
        *,
        reservation_id: str,
        call_id: str,
        currency: str,
        max_input_tokens: int,
        max_output_tokens: int,
        max_cost: str | int | Decimal,
        max_wall_time_ms: int = 0,
        max_tool_attempts: int = 0,
        max_data_exposures: int = 0,
        max_disk_growth_bytes: int = 0,
    ) -> BudgetReservation:
        self._journal._authorize()
        currency = _canonical_currency(currency)
        if currency != self._currency:
            raise BudgetConflictError("reservation currency conflicts with budget")
        reservation_id = _identifier(reservation_id, "reservation_id")
        call_id = _identifier(call_id, "call_id")
        return _SqliteUnitOfWork(stores._operational_spec())._write(
            lambda connection: self._reserve_in_transaction(
                connection,
                reservation_id=reservation_id,
                call_id=call_id,
                currency=currency,
                max_input_tokens=max_input_tokens,
                max_output_tokens=max_output_tokens,
                max_cost=max_cost,
                max_wall_time_ms=max_wall_time_ms,
                max_tool_attempts=max_tool_attempts,
                max_data_exposures=max_data_exposures,
                max_disk_growth_bytes=max_disk_growth_bytes,
            )
        )

    def _reserve_in_transaction(
        self,
        connection,
        *,
        reservation_id: str,
        call_id: str,
        currency: str,
        max_input_tokens: int,
        max_output_tokens: int,
        max_cost: str | int | Decimal,
        max_wall_time_ms: int = 0,
        max_tool_attempts: int = 0,
        max_data_exposures: int = 0,
        max_disk_growth_bytes: int = 0,
    ) -> BudgetReservation:
        currency = _canonical_currency(currency)
        if currency != self._currency:
            raise BudgetConflictError("reservation currency conflicts with budget")
        events = self._events_in_transaction(connection)
        ledger = self._replay(events)
        reservation = ledger.reserve(
            reservation_id=reservation_id,
            call_id=call_id,
            currency=currency,
            max_input_tokens=max_input_tokens,
            max_output_tokens=max_output_tokens,
            max_cost=max_cost,
            max_wall_time_ms=max_wall_time_ms,
            max_tool_attempts=max_tool_attempts,
            max_data_exposures=max_data_exposures,
            max_disk_growth_bytes=max_disk_growth_bytes,
        )
        event_id = self._event_id(
            "reserve",
            reservation_id=reservation_id,
        )
        if any(event.event_id == event_id for event in events):
            return reservation
        self._journal._append_in_transaction(
            connection,
            event_id=event_id,
            cycle_id=None,
            aggregate_type=_BUDGET_AGGREGATE_TYPE,
            aggregate_id=self._budget_id,
            event_type=_BUDGET_RESERVED,
            payload={
                "reservation_id": reservation.reservation_id,
                "call_id": reservation.call_id,
                "currency": reservation.currency,
                "max_input_tokens": reservation.max_input_tokens,
                "max_output_tokens": reservation.max_output_tokens,
                "max_cost": reservation.max_cost,
                "max_wall_time_ms": reservation.max_wall_time_ms,
                "max_tool_attempts": reservation.max_tool_attempts,
                "max_data_exposures": reservation.max_data_exposures,
                "max_disk_growth_bytes": reservation.max_disk_growth_bytes,
            },
        )
        return reservation

    def settle(
        self,
        reservation_id: str,
        *,
        currency: str,
        input_tokens: int | None,
        output_tokens: int | None,
        cost: str | int | Decimal | None,
        wall_time_ms: int | None = None,
        tool_attempts: int | None = None,
        data_exposures: int | None = None,
        disk_growth_bytes: int | None = None,
    ) -> BudgetSettlement:
        self._journal._authorize()
        currency = _canonical_currency(currency)
        if currency != self._currency:
            raise BudgetConflictError("settlement currency conflicts with budget")
        reservation_id = _identifier(reservation_id, "reservation_id")
        return _SqliteUnitOfWork(stores._operational_spec())._write(
            lambda connection: self._settle_in_transaction(
                connection,
                reservation_id=reservation_id,
                currency=currency,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost=cost,
                wall_time_ms=wall_time_ms,
                tool_attempts=tool_attempts,
                data_exposures=data_exposures,
                disk_growth_bytes=disk_growth_bytes,
            )
        )

    def _settle_in_transaction(
        self,
        connection,
        *,
        reservation_id: str,
        currency: str,
        input_tokens: int | None,
        output_tokens: int | None,
        cost: str | int | Decimal | None,
        wall_time_ms: int | None = None,
        tool_attempts: int | None = None,
        data_exposures: int | None = None,
        disk_growth_bytes: int | None = None,
    ) -> BudgetSettlement:
        currency = _canonical_currency(currency)
        if currency != self._currency:
            raise BudgetConflictError("settlement currency conflicts with budget")
        events = self._events_in_transaction(connection)
        ledger = self._replay(events)
        settlement = ledger.settle(
            reservation_id,
            currency=currency,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=cost,
            wall_time_ms=wall_time_ms,
            tool_attempts=tool_attempts,
            data_exposures=data_exposures,
            disk_growth_bytes=disk_growth_bytes,
        )
        event_id = self._event_id(
            "settle",
            reservation_id=reservation_id,
        )
        if any(event.event_id == event_id for event in events):
            return settlement
        self._journal._append_in_transaction(
            connection,
            event_id=event_id,
            cycle_id=None,
            aggregate_type=_BUDGET_AGGREGATE_TYPE,
            aggregate_id=self._budget_id,
            event_type=_BUDGET_SETTLED,
            payload={
                "reservation_id": reservation_id,
                "currency": settlement.currency,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost": None if cost is None else _cost_text(_cost(cost)),
                "wall_time_ms": wall_time_ms,
                "tool_attempts": tool_attempts,
                "data_exposures": data_exposures,
                "disk_growth_bytes": disk_growth_bytes,
                "state": settlement.state,
            },
        )
        return settlement

    def snapshot(self) -> BudgetSnapshot:
        self._journal._authorize()
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._replay(
                self._events_in_transaction(connection)
            ).snapshot()
        )

    def _limits_payload(self) -> dict[str, object]:
        return {
            "budget_id": self._budget_id,
            "currency": self._currency,
            "max_input_tokens": self._max_input_tokens,
            "max_output_tokens": self._max_output_tokens,
            "max_cost": self._max_cost,
            "max_wall_time_ms": self._max_wall_time_ms,
            "max_tool_attempts": self._max_tool_attempts,
            "max_data_exposures": self._max_data_exposures,
            "max_disk_growth_bytes": self._max_disk_growth_bytes,
        }

    def _event_id(
        self,
        role: str,
        *,
        reservation_id: str | None = None,
    ) -> str:
        return _budget_event_id(
            namespace=self._journal._namespace,
            campaign_id=self._journal._campaign_id,
            budget_id=self._budget_id,
            role=role,
            reservation_id=reservation_id,
        )

    def _events_in_transaction(self, connection) -> tuple[CampaignEvent, ...]:
        _require_single_campaign_aggregate_id(
            connection,
            journal=self._journal,
            aggregate_type=_BUDGET_AGGREGATE_TYPE,
            aggregate_id=self._budget_id,
            conflict_message="Campaign has more than one usage budget",
        )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=None,
            aggregate_type=_BUDGET_AGGREGATE_TYPE,
            aggregate_id=self._budget_id,
        )

    def _replay(self, events: tuple[CampaignEvent, ...]) -> BudgetLedger:
        if not events:
            raise CampaignJournalError("campaign budget has not been opened")
        opened = events[0]
        opened_payload = _event_domain_payload(opened)
        if (
            opened.event_id != self._event_id("open")
            or opened.event_type != _BUDGET_OPENED
            or set(opened_payload)
            != {
                "budget_id",
                "currency",
                "max_input_tokens",
                "max_output_tokens",
                "max_cost",
                "max_wall_time_ms",
                "max_tool_attempts",
                "max_data_exposures",
                "max_disk_growth_bytes",
            }
            or opened_payload["budget_id"] != self._budget_id
            or opened_payload["currency"] != self._currency
            or type(opened_payload["max_input_tokens"]) is not int
            or opened_payload["max_input_tokens"] != self._max_input_tokens
            or type(opened_payload["max_output_tokens"]) is not int
            or opened_payload["max_output_tokens"] != self._max_output_tokens
            or type(opened_payload["max_cost"]) is not str
            or opened_payload["max_cost"] != self._max_cost
            or type(opened_payload["max_wall_time_ms"]) is not int
            or opened_payload["max_wall_time_ms"] != self._max_wall_time_ms
            or type(opened_payload["max_tool_attempts"]) is not int
            or opened_payload["max_tool_attempts"] != self._max_tool_attempts
            or type(opened_payload["max_data_exposures"]) is not int
            or opened_payload["max_data_exposures"] != self._max_data_exposures
            or type(opened_payload["max_disk_growth_bytes"]) is not int
            or opened_payload["max_disk_growth_bytes"]
            != self._max_disk_growth_bytes
        ):
            raise BudgetConflictError("campaign budget configuration conflicts")
        ledger = BudgetLedger(
            currency=self._currency,
            max_input_tokens=self._max_input_tokens,
            max_output_tokens=self._max_output_tokens,
            max_cost=self._max_cost,
            max_wall_time_ms=self._max_wall_time_ms,
            max_tool_attempts=self._max_tool_attempts,
            max_data_exposures=self._max_data_exposures,
            max_disk_growth_bytes=self._max_disk_growth_bytes,
        )
        for event in events[1:]:
            payload = _event_domain_payload(event)
            if event.event_type == _BUDGET_RESERVED:
                expected_fields = {
                    "reservation_id",
                    "call_id",
                    "currency",
                    "max_input_tokens",
                    "max_output_tokens",
                    "max_cost",
                    "max_wall_time_ms",
                    "max_tool_attempts",
                    "max_data_exposures",
                    "max_disk_growth_bytes",
                }
                if set(payload) != expected_fields:
                    raise CampaignJournalError(
                        "campaign budget reservation is invalid"
                    )
                try:
                    reservation_id = _identifier(
                        payload["reservation_id"],
                        "stored reservation_id",
                    )
                    call_id = _identifier(payload["call_id"], "stored call_id")
                except ValueError as error:
                    raise CampaignJournalError(
                        "campaign budget identifiers are invalid"
                    ) from error
                if event.event_id != self._event_id(
                    "reserve",
                    reservation_id=reservation_id,
                ):
                    raise CampaignJournalError(
                        "campaign budget event identity is invalid"
                    )
                try:
                    reservation = ledger.reserve(
                        reservation_id=reservation_id,
                        call_id=call_id,
                        currency=payload["currency"],
                        max_input_tokens=payload["max_input_tokens"],
                        max_output_tokens=payload["max_output_tokens"],
                        max_cost=payload["max_cost"],
                        max_wall_time_ms=payload["max_wall_time_ms"],
                        max_tool_attempts=payload["max_tool_attempts"],
                        max_data_exposures=payload["max_data_exposures"],
                        max_disk_growth_bytes=payload["max_disk_growth_bytes"],
                    )
                    canonical_payload = {
                        "reservation_id": reservation.reservation_id,
                        "call_id": reservation.call_id,
                        "currency": reservation.currency,
                        "max_input_tokens": reservation.max_input_tokens,
                        "max_output_tokens": reservation.max_output_tokens,
                        "max_cost": reservation.max_cost,
                        "max_wall_time_ms": reservation.max_wall_time_ms,
                        "max_tool_attempts": reservation.max_tool_attempts,
                        "max_data_exposures": reservation.max_data_exposures,
                        "max_disk_growth_bytes": reservation.max_disk_growth_bytes,
                    }
                except (BudgetError, TypeError, ValueError, UnicodeError) as error:
                    raise CampaignJournalError(
                        "campaign budget reservation replay failed"
                    ) from error
                if canonical_payload != payload:
                    raise CampaignJournalError("campaign budget reservation is not canonical")
            elif event.event_type == _BUDGET_SETTLED:
                expected_fields = {
                    "reservation_id",
                    "currency",
                    "input_tokens",
                    "output_tokens",
                    "cost",
                    "wall_time_ms",
                    "tool_attempts",
                    "data_exposures",
                    "disk_growth_bytes",
                    "state",
                }
                if set(payload) != expected_fields:
                    raise CampaignJournalError(
                        "campaign budget settlement is invalid"
                    )
                try:
                    reservation_id = _identifier(
                        payload["reservation_id"],
                        "stored reservation_id",
                    )
                except ValueError as error:
                    raise CampaignJournalError(
                        "campaign budget identifiers are invalid"
                    ) from error
                if event.event_id != self._event_id(
                    "settle",
                    reservation_id=reservation_id,
                ):
                    raise CampaignJournalError(
                        "campaign budget event identity is invalid"
                    )
                try:
                    settlement = ledger.settle(
                        reservation_id,
                        currency=payload["currency"],
                        input_tokens=payload["input_tokens"],
                        output_tokens=payload["output_tokens"],
                        cost=payload["cost"],
                        wall_time_ms=payload["wall_time_ms"],
                        tool_attempts=payload["tool_attempts"],
                        data_exposures=payload["data_exposures"],
                        disk_growth_bytes=payload["disk_growth_bytes"],
                    )
                    canonical_payload = {
                        "reservation_id": reservation_id,
                        "currency": settlement.currency,
                        "input_tokens": payload["input_tokens"],
                        "output_tokens": payload["output_tokens"],
                        "cost": (
                            None
                            if payload["cost"] is None
                            else _cost_text(_cost(payload["cost"]))
                        ),
                        "wall_time_ms": payload["wall_time_ms"],
                        "tool_attempts": payload["tool_attempts"],
                        "data_exposures": payload["data_exposures"],
                        "disk_growth_bytes": payload["disk_growth_bytes"],
                        "state": settlement.state,
                    }
                except (BudgetError, TypeError, ValueError, UnicodeError) as error:
                    raise CampaignJournalError(
                        "campaign budget settlement replay failed"
                    ) from error
                if canonical_payload != payload:
                    raise CampaignJournalError("campaign budget settlement is not canonical")
            else:
                raise CampaignJournalError("campaign budget event type is invalid")
        return ledger


_CYCLE_BUDGET_AGGREGATE_TYPE = "CAMPAIGN_CYCLE_BUDGET"
_CYCLE_BUDGET_OPENED = "CYCLE_BUDGET_OPENED"
_CYCLE_SLOT_RESERVED = "CYCLE_SLOT_RESERVED"


@dataclass(frozen=True, slots=True)
class CycleBudgetSnapshot:
    budget_id: str
    max_cycles: int
    reserved_cycle_ids: tuple[str, ...]


def _cycle_budget_event_id(
    *,
    namespace: str,
    campaign_id: str,
    budget_id: str,
    role: str,
    cycle_id: str | None = None,
) -> str:
    components = [namespace, campaign_id, budget_id, role]
    if cycle_id is not None:
        components.append(cycle_id)
    return hashlib.sha256(
        b"control_plane.campaign_cycle_budget_event.v1\0"
        + "\0".join(components).encode("ascii")
    ).hexdigest()


class OperationalCycleBudgetJournal:
    """Persist the cumulative Cycle-count budget for one Campaign."""

    __slots__ = ("_journal", "_budget_id", "_max_cycles")

    def __init__(
        self,
        *,
        journal: OperationalCampaignJournal,
        budget_id: str,
        max_cycles: int,
    ) -> None:
        if not isinstance(journal, OperationalCampaignJournal):
            raise TypeError("journal must be an OperationalCampaignJournal")
        journal._authorize()
        self._journal = journal
        self._budget_id = _identifier(budget_id, "budget_id")
        if type(max_cycles) is not int or max_cycles < 0:
            raise ValueError("max_cycles must be a non-negative integer")
        self._max_cycles = max_cycles

        def open_budget(connection) -> None:
            from .campaign_lifecycle import (
                _CYCLE_AGGREGATE_TYPE,
                _CYCLE_OPENED,
            )

            events = self._events_in_transaction(connection)
            if events:
                self._snapshot_in_transaction(connection, events=events)
                return
            existing_budget = connection.execute(
                "SELECT aggregate_id FROM campaign_events "
                "WHERE namespace = ? AND campaign_id = ? "
                "AND aggregate_type = ? LIMIT 1",
                (
                    self._journal._namespace,
                    self._journal._campaign_id,
                    _CYCLE_BUDGET_AGGREGATE_TYPE,
                ),
            ).fetchone()
            if existing_budget is not None:
                raise BudgetConflictError(
                    "Campaign already has another Cycle budget"
                )
            existing_cycle = connection.execute(
                "SELECT 1 FROM campaign_events "
                "WHERE namespace = ? AND campaign_id = ? "
                "AND aggregate_type = ? AND event_type = ? LIMIT 1",
                (
                    self._journal._namespace,
                    self._journal._campaign_id,
                    _CYCLE_AGGREGATE_TYPE,
                    _CYCLE_OPENED,
                ),
            ).fetchone()
            if existing_cycle is not None:
                raise BudgetConflictError(
                    "Cycle budget cannot adopt unbudgeted Cycle history"
                )
            self._journal._append_in_transaction(
                connection,
                event_id=self._event_id("open"),
                cycle_id=None,
                aggregate_type=_CYCLE_BUDGET_AGGREGATE_TYPE,
                aggregate_id=self._budget_id,
                event_type=_CYCLE_BUDGET_OPENED,
                payload={
                    "budget_id": self._budget_id,
                    "max_cycles": self._max_cycles,
                },
            )

        _SqliteUnitOfWork(stores._operational_spec())._write(open_budget)

    def reserve(self, *, cycle_id: str) -> CycleBudgetSnapshot:
        self._journal._authorize()
        cycle_id = _identifier(cycle_id, "cycle_id")
        return _SqliteUnitOfWork(stores._operational_spec())._write(
            lambda connection: self._reserve_in_transaction(
                connection,
                cycle_id=cycle_id,
            )
        )

    def open_cycle(
        self,
        *,
        lifecycle: OperationalCampaignLifecycle,
        cycle_id: str,
        cycle_number: int,
    ) -> CycleSnapshot:
        from .campaign_lifecycle import OperationalCampaignLifecycle

        self._journal._authorize()
        if not isinstance(lifecycle, OperationalCampaignLifecycle):
            raise TypeError("lifecycle must be an OperationalCampaignLifecycle")
        if lifecycle._journal is not self._journal:
            raise ValueError("lifecycle must use the same Campaign journal")
        cycle_id = _identifier(cycle_id, "cycle_id")

        def reserve_and_open(connection):
            self._reserve_in_transaction(connection, cycle_id=cycle_id)
            return lifecycle._open_cycle_in_transaction(
                connection,
                cycle_id=cycle_id,
                cycle_number=cycle_number,
            )

        return _SqliteUnitOfWork(stores._operational_spec())._write(
            reserve_and_open
        )

    def _reserve_in_transaction(
        self,
        connection,
        *,
        cycle_id: str,
    ) -> CycleBudgetSnapshot:
        events = self._events_in_transaction(connection)
        snapshot = self._snapshot_in_transaction(connection, events=events)
        if cycle_id in snapshot.reserved_cycle_ids:
            return snapshot
        if len(snapshot.reserved_cycle_ids) >= self._max_cycles:
            raise BudgetExceededError(
                "cycle reservation exceeds configured limit"
            )
        self._journal._append_in_transaction(
            connection,
            event_id=self._event_id("reserve", cycle_id=cycle_id),
            cycle_id=None,
            aggregate_type=_CYCLE_BUDGET_AGGREGATE_TYPE,
            aggregate_id=self._budget_id,
            event_type=_CYCLE_SLOT_RESERVED,
            payload={"budget_id": self._budget_id, "cycle_id": cycle_id},
        )
        return CycleBudgetSnapshot(
            self._budget_id,
            self._max_cycles,
            (*snapshot.reserved_cycle_ids, cycle_id),
        )

    def snapshot(self) -> CycleBudgetSnapshot:
        self._journal._authorize()
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            self._snapshot_in_transaction
        )

    def _event_id(self, role: str, *, cycle_id: str | None = None) -> str:
        return _cycle_budget_event_id(
            namespace=self._journal._namespace,
            campaign_id=self._journal._campaign_id,
            budget_id=self._budget_id,
            role=role,
            cycle_id=cycle_id,
        )

    def _events_in_transaction(self, connection) -> tuple[CampaignEvent, ...]:
        _require_single_campaign_aggregate_id(
            connection,
            journal=self._journal,
            aggregate_type=_CYCLE_BUDGET_AGGREGATE_TYPE,
            aggregate_id=self._budget_id,
            conflict_message="Campaign has more than one Cycle budget",
        )
        return self._journal._list_in_transaction(
            connection,
            cycle_id=None,
            aggregate_type=_CYCLE_BUDGET_AGGREGATE_TYPE,
            aggregate_id=self._budget_id,
        )

    def _snapshot_in_transaction(
        self,
        connection,
        *,
        events: tuple[CampaignEvent, ...] | None = None,
    ) -> CycleBudgetSnapshot:
        budget_events = (
            self._events_in_transaction(connection)
            if events is None
            else events
        )
        snapshot = self._replay(budget_events)
        self._require_prior_cycle_reservations(connection, budget_events)
        return snapshot

    def _require_prior_cycle_reservations(
        self,
        connection,
        budget_events: tuple[CampaignEvent, ...],
    ) -> None:
        from .campaign_lifecycle import (
            CycleStatus,
            _CYCLE_AGGREGATE_TYPE,
            _CYCLE_OPENED,
            _state_event_id,
        )

        reserved_at = {
            str(_event_domain_payload(event)["cycle_id"]): event.sequence
            for event in budget_events
            if event.event_type == _CYCLE_SLOT_RESERVED
        }
        rows = connection.execute(
            "SELECT * FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? "
            "AND cycle_id IS NOT NULL AND aggregate_type = ? "
            "AND event_type = ? ORDER BY sequence",
            (
                self._journal._namespace,
                self._journal._campaign_id,
                _CYCLE_AGGREGATE_TYPE,
                _CYCLE_OPENED,
            ),
        ).fetchall()
        for row in rows:
            event = _event_from_row(row)
            payload = _event_domain_payload(event)
            try:
                cycle_id = _identifier(payload.get("cycle_id"), "stored cycle_id")
            except (TypeError, ValueError) as error:
                raise CampaignJournalError(
                    "Cycle open event identity is invalid"
                ) from error
            if (
                set(payload) != {"cycle_id", "cycle_number", "status"}
                or type(payload["cycle_number"]) is not int
                or not 1 <= payload["cycle_number"] <= 1_000_000
                or payload["status"] != CycleStatus.CREATED.value
                or event.cycle_id != cycle_id
                or event.aggregate_id != cycle_id
                or event.event_id
                != _state_event_id(
                    namespace=self._journal._namespace,
                    campaign_id=self._journal._campaign_id,
                    aggregate_type=_CYCLE_AGGREGATE_TYPE,
                    aggregate_id=cycle_id,
                    role=CycleStatus.CREATED.value,
                )
            ):
                raise CampaignJournalError("Cycle open event is invalid")
            reservation_sequence = reserved_at.get(cycle_id)
            if (
                reservation_sequence is None
                or reservation_sequence >= event.sequence
            ):
                raise BudgetConflictError(
                    "Cycle was opened before its budget reservation"
                )

    def _replay(self, events: tuple[CampaignEvent, ...]) -> CycleBudgetSnapshot:
        if not events:
            raise CampaignJournalError("cycle budget has not been opened")
        opened = events[0]
        opened_payload = _event_domain_payload(opened)
        if (
            opened.event_id != self._event_id("open")
            or opened.event_type != _CYCLE_BUDGET_OPENED
            or set(opened_payload) != {"budget_id", "max_cycles"}
            or opened_payload["budget_id"] != self._budget_id
            or type(opened_payload["max_cycles"]) is not int
            or opened_payload["max_cycles"] != self._max_cycles
        ):
            raise BudgetConflictError("cycle budget configuration conflicts")
        reserved: list[str] = []
        for event in events[1:]:
            payload = _event_domain_payload(event)
            if (
                event.event_type != _CYCLE_SLOT_RESERVED
                or set(payload) != {"budget_id", "cycle_id"}
                or payload["budget_id"] != self._budget_id
            ):
                raise CampaignJournalError("cycle budget event is invalid")
            try:
                cycle_id = _identifier(
                    payload["cycle_id"],
                    "stored cycle_id",
                )
            except (TypeError, ValueError) as error:
                raise CampaignJournalError(
                    "cycle budget event identity is invalid"
                ) from error
            if (
                event.cycle_id is not None
                or event.event_id
                != self._event_id("reserve", cycle_id=cycle_id)
                or cycle_id in reserved
            ):
                raise CampaignJournalError("cycle budget event identity is invalid")
            reserved.append(cycle_id)
        if len(reserved) > self._max_cycles:
            raise CampaignJournalError("cycle budget exceeds configured limit")
        return CycleBudgetSnapshot(
            self._budget_id,
            self._max_cycles,
            tuple(reserved),
        )


def _attempt_id(cycle_id: str, call_id: str, attempt_id: str) -> str:
    cycle_id = _identifier(cycle_id, "cycle_id")
    call_id = _identifier(call_id, "call_id")
    attempt_id = _identifier(attempt_id, "attempt_id")
    return hashlib.sha256(
        f"{cycle_id}\0{call_id}\0{attempt_id}".encode("ascii")
    ).hexdigest()


def _event_id(
    namespace: str,
    campaign_id: str,
    cycle_id: str,
    aggregate_id: str,
    suffix: str,
) -> str:
    source = "\0".join(
        (namespace, campaign_id, cycle_id, aggregate_id, suffix)
    ).encode("ascii")
    return hashlib.sha256(source).hexdigest()


def _envelope_payload(envelope: UsageEnvelope) -> dict[str, object]:
    return {
        "provider": envelope.provider,
        "profile": envelope.profile,
        "request_model": envelope.request_model,
        "response_model": envelope.response_model,
        "call_id": envelope.call_id,
        "attempt_id": envelope.attempt_id,
        "usage_status": envelope.usage_status.value,
        "input_tokens": envelope.input_tokens,
        "output_tokens": envelope.output_tokens,
        "total_tokens": envelope.total_tokens,
        "cache_read_tokens": envelope.cache_read_tokens,
        "cache_write_tokens": envelope.cache_write_tokens,
        "reasoning_tokens": envelope.reasoning_tokens,
        "reported_cost": envelope.reported_cost,
        "currency": envelope.currency,
        "fallback": envelope.fallback,
        "streamed": envelope.streamed,
        "outcome": envelope.outcome.value,
        "raw_usage_sha256": envelope.raw_usage_sha256,
    }


def _parse_envelope(payload: Mapping[str, object]) -> UsageEnvelope:
    token_fields = (
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "reasoning_tokens",
    )
    tokens: dict[str, int | None] = {}
    for field_name in token_fields:
        value = payload[field_name]
        if value is not None and (
            type(value) is not int
            or value < 0
            or value.bit_length() > 512
        ):
            raise ValueError(f"{field_name} is invalid")
        tokens[field_name] = value
    known_component_tokens = sum(
        int(tokens[field_name])
        for field_name in ("input_tokens", "output_tokens")
        if tokens[field_name] is not None
    )
    if (
        tokens["total_tokens"] is not None
        and tokens["total_tokens"] < known_component_tokens
    ):
        raise ValueError("total_tokens is below known token components")
    provider = _identifier(payload["provider"], "stored provider")
    profile = _identifier(payload["profile"], "stored profile")
    request_model = _identifier(
        payload["request_model"],
        "stored request_model",
    )
    response_model_value = payload["response_model"]
    response_model = (
        None
        if response_model_value is None
        else _identifier(response_model_value, "stored response_model")
    )
    call_id = _identifier(payload["call_id"], "stored call_id")
    attempt_id = _identifier(payload["attempt_id"], "stored attempt_id")
    raw_status = payload["usage_status"]
    raw_outcome = payload["outcome"]
    if type(raw_status) is not str or type(raw_outcome) is not str:
        raise ValueError("usage status or outcome is invalid")
    usage_status = UsageStatus(raw_status)
    outcome = InvocationOutcome(raw_outcome)
    reported_cost_value = payload["reported_cost"]
    if reported_cost_value is None:
        reported_cost = None
    else:
        if type(reported_cost_value) is not str:
            raise ValueError("reported_cost is invalid")
        _cost(reported_cost_value)
        reported_cost = reported_cost_value
    currency_value = payload["currency"]
    if currency_value is None:
        currency = None
    elif (
        type(currency_value) is not str
        or not currency_value
        or currency_value != currency_value.strip()
        or len(currency_value) > 128
    ):
        raise ValueError("currency is invalid")
    else:
        currency = currency_value
    fallback = payload["fallback"]
    streamed = payload["streamed"]
    if type(fallback) is not bool or type(streamed) is not bool:
        raise ValueError("fallback or streamed is invalid")
    raw_usage_sha256 = payload["raw_usage_sha256"]
    if (
        type(raw_usage_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", raw_usage_sha256) is None
    ):
        raise ValueError("raw_usage_sha256 is invalid")
    if usage_status is UsageStatus.UNKNOWN and (
        any(value is not None for value in tokens.values())
        or reported_cost is not None
        or currency is not None
    ):
        raise ValueError("UNKNOWN usage must not contain reported values")
    if usage_status is not UsageStatus.UNKNOWN and (
        all(value is None for value in tokens.values())
        and reported_cost is None
    ):
        raise ValueError("known usage status requires a reported value")
    return UsageEnvelope(
        provider=provider,
        profile=profile,
        request_model=request_model,
        response_model=response_model,
        call_id=call_id,
        attempt_id=attempt_id,
        usage_status=usage_status,
        input_tokens=tokens["input_tokens"],
        output_tokens=tokens["output_tokens"],
        total_tokens=tokens["total_tokens"],
        cache_read_tokens=tokens["cache_read_tokens"],
        cache_write_tokens=tokens["cache_write_tokens"],
        reasoning_tokens=tokens["reasoning_tokens"],
        reported_cost=reported_cost,
        currency=currency,
        fallback=fallback,
        streamed=streamed,
        outcome=outcome,
        raw_usage_sha256=raw_usage_sha256,
    )


class OperationalUsageJournal:
    """Persist ModelInvocation usage begin/finish events and replay final state."""

    __slots__ = ("_journal", "_cycle_id")

    def __init__(
        self,
        *,
        journal: OperationalCampaignJournal,
        cycle_id: str,
    ) -> None:
        if not isinstance(journal, OperationalCampaignJournal):
            raise TypeError("journal must be an OperationalCampaignJournal")
        self._journal = journal
        self._cycle_id = _identifier(cycle_id, "cycle_id")

    def begin(self, envelope: UsageEnvelope) -> None:
        if not isinstance(envelope, UsageEnvelope):
            raise TypeError("envelope must be a UsageEnvelope")
        self._journal._authorize()
        _SqliteUnitOfWork(stores._operational_spec())._write(
            lambda connection: self._begin_in_transaction(
                connection,
                envelope=envelope,
            )
        )

    def _begin_in_transaction(
        self,
        connection,
        *,
        envelope: UsageEnvelope,
    ) -> None:
        try:
            payload = _envelope_payload(envelope)
            parsed = _parse_envelope(payload)
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise ValueError("envelope is invalid") from error
        if parsed != envelope:
            raise ValueError("envelope is not canonical")
        if parsed.outcome not in {
            InvocationOutcome.RESPONSE_RECEIVED,
            InvocationOutcome.TIMEOUT,
            InvocationOutcome.EXCEPTION,
        }:
            raise ValueError("initial outcome is invalid")
        aggregate_id = _attempt_id(
            self._cycle_id,
            envelope.call_id,
            envelope.attempt_id,
        )
        self._journal._append_in_transaction(
            connection,
            event_id=_event_id(
                self._journal._namespace,
                self._journal._campaign_id,
                self._cycle_id,
                aggregate_id,
                "usage",
            ),
            cycle_id=self._cycle_id,
            aggregate_type="MODEL_ATTEMPT",
            aggregate_id=aggregate_id,
            event_type="MODEL_USAGE_RECORDED",
            payload=payload,
        )

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
        self._journal._authorize()
        _SqliteUnitOfWork(stores._operational_spec())._write(
            lambda connection: self._finish_in_transaction(
                connection,
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=outcome,
            )
        )

    def _finish_in_transaction(
        self,
        connection,
        *,
        call_id: str,
        attempt_id: str,
        outcome: InvocationOutcome,
    ) -> None:
        aggregate_id = _attempt_id(self._cycle_id, call_id, attempt_id)
        events = self._events_in_transaction(connection, aggregate_id)
        if len(events) == 2:
            recorded = self._read_attempt_in_transaction(
                connection,
                call_id=call_id,
                attempt_id=attempt_id,
            )
            if recorded.final_outcome is not outcome:
                raise CampaignJournalError(
                    "model finish conflicts with the persisted final outcome"
                )
            return
        if len(events) != 1:
            raise CampaignJournalError(
                "model attempt journal is incomplete or ambiguous"
            )
        usage_events = [
            event for event in events if event.event_type == "MODEL_USAGE_RECORDED"
        ]
        if len(usage_events) != 1:
            raise CampaignJournalError("usage must be recorded before final outcome")
        envelope = _parse_envelope(usage_events[0].payload())
        if envelope.call_id != call_id or envelope.attempt_id != attempt_id:
            raise CampaignJournalError("recorded usage identity does not match attempt")
        if envelope.outcome is not InvocationOutcome.RESPONSE_RECEIVED:
            raise CampaignJournalError(
                "terminal usage cannot accept a later finish event"
            )
        if outcome not in {
            InvocationOutcome.SUCCESS,
            InvocationOutcome.EMPTY_OUTPUT,
            InvocationOutcome.INVALID_JSON,
            InvocationOutcome.STREAMING_DISABLED,
        }:
            raise CampaignJournalError(
                "model finish outcome cannot follow a provider response"
            )
        if envelope.streamed != (
            outcome is InvocationOutcome.STREAMING_DISABLED
        ):
            raise CampaignJournalError(
                "model streaming outcome does not match recorded usage"
            )
        self._journal._append_in_transaction(
            connection,
            event_id=_event_id(
                self._journal._namespace,
                self._journal._campaign_id,
                self._cycle_id,
                aggregate_id,
                "finish",
            ),
            cycle_id=self._cycle_id,
            aggregate_type="MODEL_ATTEMPT",
            aggregate_id=aggregate_id,
            event_type="MODEL_USAGE_FINISHED",
            payload={
                "call_id": call_id,
                "attempt_id": attempt_id,
                "outcome": outcome.value,
            },
        )

    def read_attempt(self, *, call_id: str, attempt_id: str) -> RecordedModelAttempt:
        self._journal._authorize()
        _attempt_id(self._cycle_id, call_id, attempt_id)
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._read_attempt_in_transaction(
                connection,
                call_id=call_id,
                attempt_id=attempt_id,
            )
        )

    def list_attempts(
        self,
        *,
        call_id: str | None = None,
    ) -> tuple[RecordedModelAttempt, ...]:
        self._journal._authorize()
        if call_id is not None:
            call_id = _identifier(call_id, "call_id")
        return _SqliteUnitOfWork(stores._operational_spec())._read(
            lambda connection: self._list_attempts_in_transaction(
                connection,
                call_id=call_id,
            )
        )

    def _list_attempts_in_transaction(
        self,
        connection,
        *,
        call_id: str | None,
    ) -> tuple[RecordedModelAttempt, ...]:
        rows = connection.execute(
            "SELECT aggregate_id, MIN(sequence) AS first_sequence "
            "FROM campaign_events "
            "WHERE namespace = ? AND campaign_id = ? AND cycle_id = ? "
            "AND aggregate_type = ? "
            "GROUP BY aggregate_id ORDER BY first_sequence",
            (
                self._journal._namespace,
                self._journal._campaign_id,
                self._cycle_id,
                "MODEL_ATTEMPT",
            ),
        ).fetchall()
        attempts: list[RecordedModelAttempt] = []
        for row in rows:
            aggregate_id = _identifier(
                row["aggregate_id"],
                "stored model attempt aggregate_id",
            )
            events = self._events_in_transaction(connection, aggregate_id)
            if not events:
                raise CampaignJournalError("model attempt stream is missing")
            payload = _event_domain_payload(events[0])
            try:
                stored_call_id = _identifier(
                    payload.get("call_id"),
                    "stored call_id",
                )
                stored_attempt_id = _identifier(
                    payload.get("attempt_id"),
                    "stored attempt_id",
                )
            except (TypeError, ValueError) as error:
                raise CampaignJournalError(
                    "model attempt identity is invalid"
                ) from error
            if aggregate_id != _attempt_id(
                self._cycle_id,
                stored_call_id,
                stored_attempt_id,
            ):
                raise CampaignJournalError(
                    "model attempt aggregate identity is invalid"
                )
            recorded = self._read_attempt_in_transaction(
                connection,
                call_id=stored_call_id,
                attempt_id=stored_attempt_id,
            )
            if call_id is None or stored_call_id == call_id:
                attempts.append(recorded)
        return tuple(attempts)

    def _read_attempt_in_transaction(
        self,
        connection,
        *,
        call_id: str,
        attempt_id: str,
    ) -> RecordedModelAttempt:
        aggregate_id = _attempt_id(self._cycle_id, call_id, attempt_id)
        events = self._events_in_transaction(connection, aggregate_id)
        if len(events) not in {1, 2}:
            raise CampaignJournalError(
                "model attempt journal is incomplete or ambiguous"
            )
        usage_event = events[0]
        expected_usage_envelope = (
            self._journal._namespace,
            self._journal._campaign_id,
            self._cycle_id,
            "MODEL_ATTEMPT",
            aggregate_id,
            "MODEL_USAGE_RECORDED",
            _event_id(
                self._journal._namespace,
                self._journal._campaign_id,
                self._cycle_id,
                aggregate_id,
                "usage",
            ),
        )
        observed_usage_envelope = (
            usage_event.namespace,
            usage_event.campaign_id,
            usage_event.cycle_id,
            usage_event.aggregate_type,
            usage_event.aggregate_id,
            usage_event.event_type,
            usage_event.event_id,
        )
        if observed_usage_envelope != expected_usage_envelope:
            raise CampaignJournalError("model usage event envelope is invalid")
        usage_payload = _event_domain_payload(usage_event)
        try:
            envelope = _parse_envelope(usage_payload)
            observed_usage_json, _ = _payload(usage_payload)
            expected_usage_json, _ = _payload(_envelope_payload(envelope))
        except (KeyError, TypeError, ValueError) as error:
            raise CampaignJournalError("model usage payload is invalid") from error
        if observed_usage_json != expected_usage_json:
            raise CampaignJournalError("model usage payload is not canonical")
        if envelope.call_id != call_id or envelope.attempt_id != attempt_id:
            raise CampaignJournalError("recorded usage identity does not match attempt")
        if len(events) == 2:
            if envelope.outcome is not InvocationOutcome.RESPONSE_RECEIVED:
                raise CampaignJournalError(
                    "model finish cannot follow a terminal usage outcome"
                )
            finish_event = events[1]
            expected_finish_envelope = (
                self._journal._namespace,
                self._journal._campaign_id,
                self._cycle_id,
                "MODEL_ATTEMPT",
                aggregate_id,
                "MODEL_USAGE_FINISHED",
                _event_id(
                    self._journal._namespace,
                    self._journal._campaign_id,
                    self._cycle_id,
                    aggregate_id,
                    "finish",
                ),
            )
            observed_finish_envelope = (
                finish_event.namespace,
                finish_event.campaign_id,
                finish_event.cycle_id,
                finish_event.aggregate_type,
                finish_event.aggregate_id,
                finish_event.event_type,
                finish_event.event_id,
            )
            if observed_finish_envelope != expected_finish_envelope:
                raise CampaignJournalError("model finish event envelope is invalid")
            finish_payload = _event_domain_payload(finish_event)
            if set(finish_payload) != {"call_id", "attempt_id", "outcome"}:
                raise CampaignJournalError("model finish payload is invalid")
            try:
                stored_call_id = _identifier(
                    finish_payload["call_id"],
                    "stored call_id",
                )
                stored_attempt_id = _identifier(
                    finish_payload["attempt_id"],
                    "stored attempt_id",
                )
                final_outcome = InvocationOutcome(finish_payload["outcome"])
            except (TypeError, ValueError) as error:
                raise CampaignJournalError("model finish payload is invalid") from error
            if stored_call_id != call_id or stored_attempt_id != attempt_id:
                raise CampaignJournalError(
                    "final outcome identity does not match attempt"
                )
            if final_outcome is InvocationOutcome.RESPONSE_RECEIVED:
                raise CampaignJournalError("model finish outcome is not terminal")
            if final_outcome not in {
                InvocationOutcome.SUCCESS,
                InvocationOutcome.EMPTY_OUTPUT,
                InvocationOutcome.INVALID_JSON,
                InvocationOutcome.STREAMING_DISABLED,
            }:
                raise CampaignJournalError(
                    "model finish outcome cannot follow a provider response"
                )
            if envelope.streamed != (
                final_outcome is InvocationOutcome.STREAMING_DISABLED
            ):
                raise CampaignJournalError(
                    "model streaming outcome does not match recorded usage"
                )
        elif envelope.outcome in {
            InvocationOutcome.TIMEOUT,
            InvocationOutcome.EXCEPTION,
        }:
            final_outcome = envelope.outcome
        else:
            raise CampaignJournalError(
                "model attempt does not follow the persisted invocation FSM"
            )
        return RecordedModelAttempt(envelope, final_outcome)

    def _events(self, aggregate_id: str) -> tuple[CampaignEvent, ...]:
        return self._journal.list_events(
            cycle_id=self._cycle_id,
            aggregate_type="MODEL_ATTEMPT",
            aggregate_id=aggregate_id,
        )

    def _events_in_transaction(
        self,
        connection,
        aggregate_id: str,
    ) -> tuple[CampaignEvent, ...]:
        return self._journal._list_in_transaction(
            connection,
            cycle_id=self._cycle_id,
            aggregate_type="MODEL_ATTEMPT",
            aggregate_id=aggregate_id,
        )


__all__ = [
    "CampaignLearningCommitSink",
    "CampaignExecutionMode",
    "CampaignEvent",
    "CampaignEventConflictError",
    "CampaignJournalError",
    "DryRunIsolationError",
    "CycleBudgetSnapshot",
    "OperationalBudgetJournal",
    "OperationalCampaignJournal",
    "OperationalCycleBudgetJournal",
    "OperationalUsageJournal",
    "RecordedModelAttempt",
    "campaign_execution_mode",
    "campaign_scope_sha256",
    "dry_run_namespace",
]
