"""Atomic, offline budget reservation primitives for the P6 controller."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Context, Decimal, Inexact, InvalidOperation, Rounded, localcontext
import re
from threading import RLock


_MAX_COST_TEXT_CHARS = 160
_MAX_COST_DIGITS = 128
_MAX_COST_EXPONENT = 128
_MAX_COST_INT_BITS = 512
_COST_CONTEXT = Context(prec=512)
_COST_CONTEXT.traps[Inexact] = True
_COST_CONTEXT.traps[Rounded] = True
_CANONICAL_CURRENCY_RE = re.compile(r"[A-Z]{3}\Z", re.ASCII)


class BudgetError(RuntimeError):
    """Base class for budget ledger failures."""


class BudgetExceededError(BudgetError):
    """Raised when a reservation would exceed any configured budget dimension."""


class BudgetConflictError(BudgetError):
    """Raised when an idempotency key is reused with different bounds."""


def _canonical_currency(value: object) -> str:
    if type(value) is not str or _CANONICAL_CURRENCY_RE.fullmatch(value) is None:
        raise ValueError("currency must be an exact three-letter uppercase code")
    return value


@dataclass(frozen=True)
class BudgetReservation:
    reservation_id: str
    call_id: str
    currency: str
    max_input_tokens: int
    max_output_tokens: int
    max_cost: str
    max_wall_time_ms: int
    max_tool_attempts: int
    max_data_exposures: int
    max_disk_growth_bytes: int


@dataclass(frozen=True)
class BudgetSnapshot:
    currency: str
    reserved_input_tokens: int
    reserved_output_tokens: int
    reserved_cost: str
    reserved_wall_time_ms: int
    reserved_tool_attempts: int
    reserved_data_exposures: int
    reserved_disk_growth_bytes: int
    spent_input_tokens: int
    spent_output_tokens: int
    spent_cost: str
    spent_wall_time_ms: int
    spent_tool_attempts: int
    spent_data_exposures: int
    spent_disk_growth_bytes: int


@dataclass(frozen=True)
class BudgetSettlement:
    reservation_id: str
    currency: str
    state: str


@dataclass
class _ReservationRecord:
    reservation: BudgetReservation
    reserved_cost: Decimal
    state: str = "RESERVED"
    actual_input_tokens: int | None = None
    actual_output_tokens: int | None = None
    actual_cost: str | None = None
    actual_currency: str | None = None
    actual_wall_time_ms: int | None = None
    actual_tool_attempts: int | None = None
    actual_data_exposures: int | None = None
    actual_disk_growth_bytes: int | None = None


def _cost(value: str | int | Decimal) -> Decimal:
    if isinstance(value, str):
        if len(value) > _MAX_COST_TEXT_CHARS:
            raise ValueError("budget cost exceeds the bounded decimal length")
        source = value
    elif type(value) is int:
        if value.bit_length() > _MAX_COST_INT_BITS:
            raise ValueError("budget cost exceeds the bounded decimal length")
        source = str(value)
    elif isinstance(value, Decimal):
        source = value
    else:
        raise ValueError("budget cost must be a finite non-negative decimal")
    try:
        amount = source if isinstance(source, Decimal) else Decimal(source)
    except (InvalidOperation, ValueError, TypeError) as error:
        raise ValueError("budget cost must be a finite non-negative decimal") from error
    if not amount.is_finite() or amount < 0:
        raise ValueError("budget cost must be a finite non-negative decimal")
    parts = amount.as_tuple()
    if (
        len(parts.digits) > _MAX_COST_DIGITS
        or not isinstance(parts.exponent, int)
        or abs(parts.exponent) > _MAX_COST_EXPONENT
    ):
        raise ValueError("budget cost exceeds the bounded decimal length")
    return amount


def _cost_text(amount: Decimal) -> str:
    if amount == 0:
        return "0"
    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if len(text) <= _MAX_COST_DIGITS:
        return text
    parts = amount.as_tuple()
    digits = list(parts.digits)
    exponent = int(parts.exponent)
    while (
        len(digits) > 1
        and digits[-1] == 0
        and exponent < _MAX_COST_EXPONENT
    ):
        digits.pop()
        exponent += 1
    coefficient = "".join(str(digit) for digit in digits)
    sign = "-" if parts.sign else ""
    return f"{sign}{coefficient}e{exponent:+d}"


def _add_cost(*amounts: Decimal) -> Decimal:
    with localcontext(_COST_CONTEXT):
        total = Decimal("0")
        for amount in amounts:
            total += amount
        return total


def _subtract_cost(left: Decimal, right: Decimal) -> Decimal:
    with localcontext(_COST_CONTEXT):
        return left - right


def _bound(value: int, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


class BudgetLedger:
    """Thread-safe reservation projection with idempotent reservation IDs."""

    __slots__ = (
        "_lock",
        "_currency",
        "_max_input_tokens",
        "_max_output_tokens",
        "_max_cost",
        "_max_wall_time_ms",
        "_max_tool_attempts",
        "_max_data_exposures",
        "_max_disk_growth_bytes",
        "_reserved_input_tokens",
        "_reserved_output_tokens",
        "_reserved_cost",
        "_reserved_wall_time_ms",
        "_reserved_tool_attempts",
        "_reserved_data_exposures",
        "_reserved_disk_growth_bytes",
        "_spent_input_tokens",
        "_spent_output_tokens",
        "_spent_cost",
        "_spent_wall_time_ms",
        "_spent_tool_attempts",
        "_spent_data_exposures",
        "_spent_disk_growth_bytes",
        "_reservations",
    )

    def __init__(
        self,
        *,
        currency: str,
        max_input_tokens: int,
        max_output_tokens: int,
        max_cost: str | int | Decimal,
        max_wall_time_ms: int = 0,
        max_tool_attempts: int = 0,
        max_data_exposures: int = 0,
        max_disk_growth_bytes: int = 0,
    ) -> None:
        self._lock = RLock()
        self._currency = _canonical_currency(currency)
        self._max_input_tokens = _bound(max_input_tokens, "max_input_tokens")
        self._max_output_tokens = _bound(max_output_tokens, "max_output_tokens")
        self._max_cost = _cost(max_cost)
        self._max_wall_time_ms = _bound(max_wall_time_ms, "max_wall_time_ms")
        self._max_tool_attempts = _bound(max_tool_attempts, "max_tool_attempts")
        self._max_data_exposures = _bound(
            max_data_exposures,
            "max_data_exposures",
        )
        self._max_disk_growth_bytes = _bound(
            max_disk_growth_bytes,
            "max_disk_growth_bytes",
        )
        self._reserved_input_tokens = 0
        self._reserved_output_tokens = 0
        self._reserved_cost = Decimal("0")
        self._reserved_wall_time_ms = 0
        self._reserved_tool_attempts = 0
        self._reserved_data_exposures = 0
        self._reserved_disk_growth_bytes = 0
        self._spent_input_tokens = 0
        self._spent_output_tokens = 0
        self._spent_cost = Decimal("0")
        self._spent_wall_time_ms = 0
        self._spent_tool_attempts = 0
        self._spent_data_exposures = 0
        self._spent_disk_growth_bytes = 0
        self._reservations: dict[str, _ReservationRecord] = {}

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
        requested_currency = _canonical_currency(currency)
        if requested_currency != self._currency:
            raise BudgetConflictError("reservation currency conflicts with ledger")
        if not isinstance(reservation_id, str) or not reservation_id.strip():
            raise ValueError("reservation_id must be non-empty")
        if not isinstance(call_id, str) or not call_id.strip():
            raise ValueError("call_id must be non-empty")
        requested_cost = _cost(max_cost)
        reservation = BudgetReservation(
            reservation_id=reservation_id,
            call_id=call_id,
            currency=requested_currency,
            max_input_tokens=_bound(max_input_tokens, "max_input_tokens"),
            max_output_tokens=_bound(max_output_tokens, "max_output_tokens"),
            max_cost=_cost_text(requested_cost),
            max_wall_time_ms=_bound(max_wall_time_ms, "max_wall_time_ms"),
            max_tool_attempts=_bound(max_tool_attempts, "max_tool_attempts"),
            max_data_exposures=_bound(
                max_data_exposures,
                "max_data_exposures",
            ),
            max_disk_growth_bytes=_bound(
                max_disk_growth_bytes,
                "max_disk_growth_bytes",
            ),
        )
        with self._lock:
            existing = self._reservations.get(reservation_id)
            if existing is not None:
                if existing.reservation != reservation:
                    raise BudgetConflictError(
                        "reservation_id was reused with different bounds"
                    )
                return existing.reservation
            new_reserved_cost = _add_cost(self._reserved_cost, requested_cost)
            if (
                self._reserved_input_tokens
                + reservation.max_input_tokens
                + self._spent_input_tokens
                > self._max_input_tokens
                or self._reserved_output_tokens
                + reservation.max_output_tokens
                + self._spent_output_tokens
                > self._max_output_tokens
                or _add_cost(new_reserved_cost, self._spent_cost) > self._max_cost
                or self._reserved_wall_time_ms
                + reservation.max_wall_time_ms
                + self._spent_wall_time_ms
                > self._max_wall_time_ms
                or self._reserved_tool_attempts
                + reservation.max_tool_attempts
                + self._spent_tool_attempts
                > self._max_tool_attempts
                or self._reserved_data_exposures
                + reservation.max_data_exposures
                + self._spent_data_exposures
                > self._max_data_exposures
                or self._reserved_disk_growth_bytes
                + reservation.max_disk_growth_bytes
                + self._spent_disk_growth_bytes
                > self._max_disk_growth_bytes
            ):
                raise BudgetExceededError("budget reservation exceeds configured limit")
            self._reserved_input_tokens += reservation.max_input_tokens
            self._reserved_output_tokens += reservation.max_output_tokens
            self._reserved_cost = new_reserved_cost
            self._reserved_wall_time_ms += reservation.max_wall_time_ms
            self._reserved_tool_attempts += reservation.max_tool_attempts
            self._reserved_data_exposures += reservation.max_data_exposures
            self._reserved_disk_growth_bytes += reservation.max_disk_growth_bytes
            self._reservations[reservation_id] = _ReservationRecord(
                reservation,
                requested_cost,
            )
            return reservation

    def snapshot(self) -> BudgetSnapshot:
        with self._lock:
            return BudgetSnapshot(
                currency=self._currency,
                reserved_input_tokens=self._reserved_input_tokens,
                reserved_output_tokens=self._reserved_output_tokens,
                reserved_cost=_cost_text(self._reserved_cost),
                reserved_wall_time_ms=self._reserved_wall_time_ms,
                reserved_tool_attempts=self._reserved_tool_attempts,
                reserved_data_exposures=self._reserved_data_exposures,
                reserved_disk_growth_bytes=self._reserved_disk_growth_bytes,
                spent_input_tokens=self._spent_input_tokens,
                spent_output_tokens=self._spent_output_tokens,
                spent_cost=_cost_text(self._spent_cost),
                spent_wall_time_ms=self._spent_wall_time_ms,
                spent_tool_attempts=self._spent_tool_attempts,
                spent_data_exposures=self._spent_data_exposures,
                spent_disk_growth_bytes=self._spent_disk_growth_bytes,
            )

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
        actual_currency = _canonical_currency(currency)
        if actual_currency != self._currency:
            raise BudgetConflictError("settlement currency conflicts with ledger")
        if input_tokens is not None:
            _bound(input_tokens, "input_tokens")
        if output_tokens is not None:
            _bound(output_tokens, "output_tokens")
        if wall_time_ms is not None:
            _bound(wall_time_ms, "wall_time_ms")
        if tool_attempts is not None:
            _bound(tool_attempts, "tool_attempts")
        if data_exposures is not None:
            _bound(data_exposures, "data_exposures")
        if disk_growth_bytes is not None:
            _bound(disk_growth_bytes, "disk_growth_bytes")
        actual_cost_value = None if cost is None else _cost(cost)
        actual_cost = (
            None if actual_cost_value is None else _cost_text(actual_cost_value)
        )
        with self._lock:
            record = self._reservations.get(reservation_id)
            if record is None:
                raise BudgetError("reservation_id is unknown")
            requested = record.reservation
            normalized_resources = (
                0
                if wall_time_ms is None and requested.max_wall_time_ms == 0
                else wall_time_ms,
                0
                if tool_attempts is None and requested.max_tool_attempts == 0
                else tool_attempts,
                0
                if data_exposures is None and requested.max_data_exposures == 0
                else data_exposures,
                0
                if disk_growth_bytes is None
                and requested.max_disk_growth_bytes == 0
                else disk_growth_bytes,
            )
            actual_tuple = (
                input_tokens,
                output_tokens,
                actual_cost,
                actual_currency,
                *normalized_resources,
            )
            stored_tuple = (
                record.actual_input_tokens,
                record.actual_output_tokens,
                record.actual_cost,
                record.actual_currency,
                record.actual_wall_time_ms,
                record.actual_tool_attempts,
                record.actual_data_exposures,
                record.actual_disk_growth_bytes,
            )
            if record.state != "RESERVED":
                if stored_tuple != actual_tuple:
                    raise BudgetConflictError(
                        "settlement was replayed with different actual usage"
                    )
                return BudgetSettlement(
                    reservation_id=reservation_id,
                    currency=self._currency,
                    state=record.state,
                )
            if (
                input_tokens is None
                or output_tokens is None
                or actual_cost is None
                or any(value is None for value in normalized_resources)
            ):
                record.actual_input_tokens = input_tokens
                record.actual_output_tokens = output_tokens
                record.actual_cost = actual_cost
                record.actual_currency = actual_currency
                (
                    record.actual_wall_time_ms,
                    record.actual_tool_attempts,
                    record.actual_data_exposures,
                    record.actual_disk_growth_bytes,
                ) = normalized_resources
                record.state = "SETTLED_UNKNOWN"
                return BudgetSettlement(
                    reservation_id=reservation_id,
                    currency=self._currency,
                    state=record.state,
                )
            (
                actual_wall_time_ms,
                actual_tool_attempts,
                actual_data_exposures,
                actual_disk_growth_bytes,
            ) = normalized_resources
            if (
                input_tokens > requested.max_input_tokens
                or output_tokens > requested.max_output_tokens
                or actual_cost_value > record.reserved_cost
                or actual_wall_time_ms > requested.max_wall_time_ms
                or actual_tool_attempts > requested.max_tool_attempts
                or actual_data_exposures > requested.max_data_exposures
                or actual_disk_growth_bytes > requested.max_disk_growth_bytes
            ):
                raise BudgetConflictError("actual usage exceeds its reservation")
            new_reserved_input = (
                self._reserved_input_tokens - requested.max_input_tokens
            )
            new_reserved_output = (
                self._reserved_output_tokens - requested.max_output_tokens
            )
            new_reserved_cost = _subtract_cost(
                self._reserved_cost,
                record.reserved_cost,
            )
            new_spent_input = self._spent_input_tokens + input_tokens
            new_spent_output = self._spent_output_tokens + output_tokens
            new_spent_cost = _add_cost(self._spent_cost, actual_cost_value)
            new_reserved_wall_time_ms = (
                self._reserved_wall_time_ms - requested.max_wall_time_ms
            )
            new_reserved_tool_attempts = (
                self._reserved_tool_attempts - requested.max_tool_attempts
            )
            new_reserved_data_exposures = (
                self._reserved_data_exposures - requested.max_data_exposures
            )
            new_reserved_disk_growth_bytes = (
                self._reserved_disk_growth_bytes
                - requested.max_disk_growth_bytes
            )
            self._reserved_input_tokens = new_reserved_input
            self._reserved_output_tokens = new_reserved_output
            self._reserved_cost = new_reserved_cost
            self._reserved_wall_time_ms = new_reserved_wall_time_ms
            self._reserved_tool_attempts = new_reserved_tool_attempts
            self._reserved_data_exposures = new_reserved_data_exposures
            self._reserved_disk_growth_bytes = new_reserved_disk_growth_bytes
            self._spent_input_tokens = new_spent_input
            self._spent_output_tokens = new_spent_output
            self._spent_cost = new_spent_cost
            self._spent_wall_time_ms += actual_wall_time_ms
            self._spent_tool_attempts += actual_tool_attempts
            self._spent_data_exposures += actual_data_exposures
            self._spent_disk_growth_bytes += actual_disk_growth_bytes
            record.actual_input_tokens = input_tokens
            record.actual_output_tokens = output_tokens
            record.actual_cost = actual_cost
            record.actual_currency = actual_currency
            record.actual_wall_time_ms = actual_wall_time_ms
            record.actual_tool_attempts = actual_tool_attempts
            record.actual_data_exposures = actual_data_exposures
            record.actual_disk_growth_bytes = actual_disk_growth_bytes
            record.state = "SETTLED"
            return BudgetSettlement(
                reservation_id=reservation_id,
                currency=self._currency,
                state=record.state,
            )


__all__ = [
    "BudgetConflictError",
    "BudgetError",
    "BudgetExceededError",
    "BudgetLedger",
    "BudgetReservation",
    "BudgetSettlement",
    "BudgetSnapshot",
]
