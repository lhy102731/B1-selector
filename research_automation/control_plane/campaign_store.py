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
from typing import Callable

from . import stores
from .budget import (
    BudgetConflictError,
    BudgetError,
    BudgetLedger,
    BudgetReservation,
    BudgetSettlement,
    BudgetSnapshot,
    _cost,
    _cost_text,
)
from .campaign import InvocationOutcome, UsageEnvelope, UsageStatus
from .contracts import Phase, SideEffect
from .sqlite_uow import _SqliteUnitOfWork


_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_MAX_EVENT_PAYLOAD_BYTES = 64 * 1024


class CampaignJournalError(RuntimeError):
    """Base error for P6 campaign journal operations."""


class CampaignEventConflictError(CampaignJournalError):
    """Raised when an event ID is replayed with different content."""


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
        stores._migrate_operational_journal_v3(root_secret=root_secret)
        stores._require_store_root(stores._operational_spec(), root_secret)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

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
        existing = connection.execute(
            "SELECT * FROM campaign_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if existing is not None:
            event = _event_from_row(existing)
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


_BUDGET_AGGREGATE_TYPE = "CAMPAIGN_BUDGET"
_BUDGET_OPENED = "BUDGET_OPENED"
_BUDGET_RESERVED = "BUDGET_RESERVED"
_BUDGET_SETTLED = "BUDGET_SETTLED"


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
        "_max_input_tokens",
        "_max_output_tokens",
        "_max_cost",
    )

    def __init__(
        self,
        *,
        journal: OperationalCampaignJournal,
        budget_id: str,
        max_input_tokens: int,
        max_output_tokens: int,
        max_cost: str | int | Decimal,
    ) -> None:
        if not isinstance(journal, OperationalCampaignJournal):
            raise TypeError("journal must be an OperationalCampaignJournal")
        journal._authorize()
        self._journal = journal
        self._budget_id = _identifier(budget_id, "budget_id")
        BudgetLedger(
            max_input_tokens=max_input_tokens,
            max_output_tokens=max_output_tokens,
            max_cost=max_cost,
        )
        self._max_input_tokens = max_input_tokens
        self._max_output_tokens = max_output_tokens
        self._max_cost = _cost_text(_cost(max_cost))

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
        max_input_tokens: int,
        max_output_tokens: int,
        max_cost: str | int | Decimal,
    ) -> BudgetReservation:
        self._journal._authorize()
        reservation_id = _identifier(reservation_id, "reservation_id")
        call_id = _identifier(call_id, "call_id")

        def reserve_budget(connection) -> BudgetReservation:
            events = self._events_in_transaction(connection)
            ledger = self._replay(events)
            reservation = ledger.reserve(
                reservation_id=reservation_id,
                call_id=call_id,
                max_input_tokens=max_input_tokens,
                max_output_tokens=max_output_tokens,
                max_cost=max_cost,
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
                    "max_input_tokens": reservation.max_input_tokens,
                    "max_output_tokens": reservation.max_output_tokens,
                    "max_cost": reservation.max_cost,
                },
            )
            return reservation

        return _SqliteUnitOfWork(stores._operational_spec())._write(reserve_budget)

    def settle(
        self,
        reservation_id: str,
        *,
        input_tokens: int | None,
        output_tokens: int | None,
        cost: str | int | Decimal | None,
    ) -> BudgetSettlement:
        self._journal._authorize()
        reservation_id = _identifier(reservation_id, "reservation_id")

        def settle_budget(connection) -> BudgetSettlement:
            events = self._events_in_transaction(connection)
            ledger = self._replay(events)
            settlement = ledger.settle(
                reservation_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost=cost,
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
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost": None if cost is None else _cost_text(_cost(cost)),
                    "state": settlement.state,
                },
            )
            return settlement

        return _SqliteUnitOfWork(stores._operational_spec())._write(settle_budget)

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
            "max_input_tokens": self._max_input_tokens,
            "max_output_tokens": self._max_output_tokens,
            "max_cost": self._max_cost,
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
        if (
            opened.event_id != self._event_id("open")
            or opened.event_type != _BUDGET_OPENED
            or _event_domain_payload(opened) != self._limits_payload()
        ):
            raise BudgetConflictError("campaign budget configuration conflicts")
        ledger = BudgetLedger(
            max_input_tokens=self._max_input_tokens,
            max_output_tokens=self._max_output_tokens,
            max_cost=self._max_cost,
        )
        for event in events[1:]:
            payload = _event_domain_payload(event)
            if event.event_type == _BUDGET_RESERVED:
                expected_fields = {
                    "reservation_id",
                    "call_id",
                    "max_input_tokens",
                    "max_output_tokens",
                    "max_cost",
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
                        max_input_tokens=payload["max_input_tokens"],
                        max_output_tokens=payload["max_output_tokens"],
                        max_cost=payload["max_cost"],
                    )
                    canonical_payload = {
                        "reservation_id": reservation.reservation_id,
                        "call_id": reservation.call_id,
                        "max_input_tokens": reservation.max_input_tokens,
                        "max_output_tokens": reservation.max_output_tokens,
                        "max_cost": reservation.max_cost,
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
                    "input_tokens",
                    "output_tokens",
                    "cost",
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
                        input_tokens=payload["input_tokens"],
                        output_tokens=payload["output_tokens"],
                        cost=payload["cost"],
                    )
                    canonical_payload = {
                        "reservation_id": reservation_id,
                        "input_tokens": payload["input_tokens"],
                        "output_tokens": payload["output_tokens"],
                        "cost": (
                            None
                            if payload["cost"] is None
                            else _cost_text(_cost(payload["cost"]))
                        ),
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
    return UsageEnvelope(
        provider=str(payload["provider"]),
        profile=str(payload["profile"]),
        request_model=str(payload["request_model"]),
        response_model=(
            None if payload["response_model"] is None else str(payload["response_model"])
        ),
        call_id=str(payload["call_id"]),
        attempt_id=str(payload["attempt_id"]),
        usage_status=UsageStatus(str(payload["usage_status"])),
        input_tokens=payload["input_tokens"],
        output_tokens=payload["output_tokens"],
        total_tokens=payload["total_tokens"],
        cache_read_tokens=payload["cache_read_tokens"],
        cache_write_tokens=payload["cache_write_tokens"],
        reasoning_tokens=payload["reasoning_tokens"],
        reported_cost=(
            None if payload["reported_cost"] is None else str(payload["reported_cost"])
        ),
        currency=None if payload["currency"] is None else str(payload["currency"]),
        fallback=bool(payload["fallback"]),
        streamed=bool(payload["streamed"]),
        outcome=InvocationOutcome(str(payload["outcome"])),
        raw_usage_sha256=str(payload["raw_usage_sha256"]),
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
        aggregate_id = _attempt_id(
            self._cycle_id,
            envelope.call_id,
            envelope.attempt_id,
        )
        self._journal.append(
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
            payload=_envelope_payload(envelope),
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
        aggregate_id = _attempt_id(self._cycle_id, call_id, attempt_id)
        events = self._events(aggregate_id)
        usage_events = [
            event for event in events if event.event_type == "MODEL_USAGE_RECORDED"
        ]
        if len(usage_events) != 1:
            raise CampaignJournalError("usage must be recorded before final outcome")
        envelope = _parse_envelope(usage_events[0].payload())
        if envelope.call_id != call_id or envelope.attempt_id != attempt_id:
            raise CampaignJournalError("recorded usage identity does not match attempt")
        self._journal.append(
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
        aggregate_id = _attempt_id(self._cycle_id, call_id, attempt_id)
        events = self._events(aggregate_id)
        usage_events = [
            event for event in events if event.event_type == "MODEL_USAGE_RECORDED"
        ]
        finish_events = [
            event for event in events if event.event_type == "MODEL_USAGE_FINISHED"
        ]
        if len(usage_events) != 1 or len(finish_events) > 1:
            raise CampaignJournalError("model attempt journal is incomplete or ambiguous")
        envelope = _parse_envelope(usage_events[0].payload())
        if envelope.call_id != call_id or envelope.attempt_id != attempt_id:
            raise CampaignJournalError("recorded usage identity does not match attempt")
        if finish_events:
            finish_payload = finish_events[0].payload()
            if (
                finish_payload.get("call_id") != call_id
                or finish_payload.get("attempt_id") != attempt_id
            ):
                raise CampaignJournalError("final outcome identity does not match attempt")
            final_outcome = InvocationOutcome(str(finish_payload["outcome"]))
        elif envelope.outcome is not InvocationOutcome.RESPONSE_RECEIVED:
            final_outcome = envelope.outcome
        else:
            raise CampaignJournalError("model attempt has no final outcome")
        return RecordedModelAttempt(envelope, final_outcome)

    def _events(self, aggregate_id: str) -> tuple[CampaignEvent, ...]:
        return self._journal.list_events(
            cycle_id=self._cycle_id,
            aggregate_type="MODEL_ATTEMPT",
            aggregate_id=aggregate_id,
        )


__all__ = [
    "CampaignEvent",
    "CampaignEventConflictError",
    "CampaignJournalError",
    "OperationalBudgetJournal",
    "OperationalCampaignJournal",
    "OperationalUsageJournal",
    "RecordedModelAttempt",
    "campaign_scope_sha256",
]
