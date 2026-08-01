"""Atomic, offline budget reservation primitives for the P6 controller."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Context, Decimal, Inexact, InvalidOperation, Rounded, localcontext
from threading import RLock


_MAX_COST_TEXT_CHARS = 160
_MAX_COST_DIGITS = 128
_MAX_COST_EXPONENT = 128
_MAX_COST_INT_BITS = 512
_COST_CONTEXT = Context(prec=512)
_COST_CONTEXT.traps[Inexact] = True
_COST_CONTEXT.traps[Rounded] = True


class BudgetError(RuntimeError):
    """Base class for budget ledger failures."""


class BudgetExceededError(BudgetError):
    """Raised when a reservation would exceed any configured budget dimension."""


class BudgetConflictError(BudgetError):
    """Raised when an idempotency key is reused with different bounds."""


@dataclass(frozen=True)
class BudgetReservation:
    reservation_id: str
    call_id: str
    max_input_tokens: int
    max_output_tokens: int
    max_cost: str


@dataclass(frozen=True)
class BudgetSnapshot:
    reserved_input_tokens: int
    reserved_output_tokens: int
    reserved_cost: str
    spent_input_tokens: int
    spent_output_tokens: int
    spent_cost: str


@dataclass(frozen=True)
class BudgetSettlement:
    reservation_id: str
    state: str


@dataclass
class _ReservationRecord:
    reservation: BudgetReservation
    reserved_cost: Decimal
    state: str = "RESERVED"
    actual_input_tokens: int | None = None
    actual_output_tokens: int | None = None
    actual_cost: str | None = None


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
        "_max_input_tokens",
        "_max_output_tokens",
        "_max_cost",
        "_reserved_input_tokens",
        "_reserved_output_tokens",
        "_reserved_cost",
        "_spent_input_tokens",
        "_spent_output_tokens",
        "_spent_cost",
        "_reservations",
    )

    def __init__(
        self,
        *,
        max_input_tokens: int,
        max_output_tokens: int,
        max_cost: str | int | Decimal,
    ) -> None:
        self._lock = RLock()
        self._max_input_tokens = _bound(max_input_tokens, "max_input_tokens")
        self._max_output_tokens = _bound(max_output_tokens, "max_output_tokens")
        self._max_cost = _cost(max_cost)
        self._reserved_input_tokens = 0
        self._reserved_output_tokens = 0
        self._reserved_cost = Decimal("0")
        self._spent_input_tokens = 0
        self._spent_output_tokens = 0
        self._spent_cost = Decimal("0")
        self._reservations: dict[str, _ReservationRecord] = {}

    def reserve(
        self,
        *,
        reservation_id: str,
        call_id: str,
        max_input_tokens: int,
        max_output_tokens: int,
        max_cost: str | int | Decimal,
    ) -> BudgetReservation:
        if not isinstance(reservation_id, str) or not reservation_id.strip():
            raise ValueError("reservation_id must be non-empty")
        if not isinstance(call_id, str) or not call_id.strip():
            raise ValueError("call_id must be non-empty")
        requested_cost = _cost(max_cost)
        reservation = BudgetReservation(
            reservation_id=reservation_id,
            call_id=call_id,
            max_input_tokens=_bound(max_input_tokens, "max_input_tokens"),
            max_output_tokens=_bound(max_output_tokens, "max_output_tokens"),
            max_cost=_cost_text(requested_cost),
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
            ):
                raise BudgetExceededError("budget reservation exceeds configured limit")
            self._reserved_input_tokens += reservation.max_input_tokens
            self._reserved_output_tokens += reservation.max_output_tokens
            self._reserved_cost = new_reserved_cost
            self._reservations[reservation_id] = _ReservationRecord(
                reservation,
                requested_cost,
            )
            return reservation

    def snapshot(self) -> BudgetSnapshot:
        with self._lock:
            return BudgetSnapshot(
                reserved_input_tokens=self._reserved_input_tokens,
                reserved_output_tokens=self._reserved_output_tokens,
                reserved_cost=_cost_text(self._reserved_cost),
                spent_input_tokens=self._spent_input_tokens,
                spent_output_tokens=self._spent_output_tokens,
                spent_cost=_cost_text(self._spent_cost),
            )

    def settle(
        self,
        reservation_id: str,
        *,
        input_tokens: int | None,
        output_tokens: int | None,
        cost: str | int | Decimal | None,
    ) -> BudgetSettlement:
        if input_tokens is not None:
            _bound(input_tokens, "input_tokens")
        if output_tokens is not None:
            _bound(output_tokens, "output_tokens")
        actual_cost_value = None if cost is None else _cost(cost)
        actual_cost = (
            None if actual_cost_value is None else _cost_text(actual_cost_value)
        )
        with self._lock:
            record = self._reservations.get(reservation_id)
            if record is None:
                raise BudgetError("reservation_id is unknown")
            actual_tuple = (input_tokens, output_tokens, actual_cost)
            stored_tuple = (
                record.actual_input_tokens,
                record.actual_output_tokens,
                record.actual_cost,
            )
            if record.state != "RESERVED":
                if stored_tuple != actual_tuple:
                    raise BudgetConflictError(
                        "settlement was replayed with different actual usage"
                    )
                return BudgetSettlement(reservation_id, record.state)
            if input_tokens is None or output_tokens is None or actual_cost is None:
                record.actual_input_tokens = input_tokens
                record.actual_output_tokens = output_tokens
                record.actual_cost = actual_cost
                record.state = "SETTLED_UNKNOWN"
                return BudgetSettlement(reservation_id, record.state)
            requested = record.reservation
            if (
                input_tokens > requested.max_input_tokens
                or output_tokens > requested.max_output_tokens
                or actual_cost_value > record.reserved_cost
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
            self._reserved_input_tokens = new_reserved_input
            self._reserved_output_tokens = new_reserved_output
            self._reserved_cost = new_reserved_cost
            self._spent_input_tokens = new_spent_input
            self._spent_output_tokens = new_spent_output
            self._spent_cost = new_spent_cost
            record.actual_input_tokens = input_tokens
            record.actual_output_tokens = output_tokens
            record.actual_cost = actual_cost
            record.state = "SETTLED"
            return BudgetSettlement(reservation_id, record.state)


__all__ = [
    "BudgetConflictError",
    "BudgetError",
    "BudgetExceededError",
    "BudgetLedger",
    "BudgetReservation",
    "BudgetSettlement",
    "BudgetSnapshot",
]
