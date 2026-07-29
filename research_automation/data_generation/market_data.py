"""Fail-closed semantics for missing market bars and portfolio valuation."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from types import MappingProxyType


class BarAvailability(str, Enum):
    """Mutually exclusive outcomes for one instrument and trading session."""

    PRESENT = "PRESENT"
    NO_BAR_CONFIRMED = "NO_BAR_CONFIRMED"
    UNKNOWN_NO_BAR = "UNKNOWN_NO_BAR"
    FETCH_FAILED = "FETCH_FAILED"


class BarUse(str, Enum):
    """Consumers that require a real bar for the same trading session."""

    FEATURE = "FEATURE"
    SIGNAL = "SIGNAL"
    ENTRY = "ENTRY"
    EXIT = "EXIT"


class MarketDataUseError(ValueError):
    """Raised when unavailable or stale data reaches a forbidden consumer."""


class ValuationState(str, Enum):
    """Freshness state for one portfolio valuation amount."""

    CURRENT = "CURRENT"
    STALE_VALUATION = "STALE_VALUATION"


@dataclass(frozen=True, slots=True)
class MarketSessionKey:
    """A-share instrument and trading-session identity."""

    instrument_id: str
    session_date: date

    def __post_init__(self) -> None:
        if not isinstance(self.instrument_id, str) or re.fullmatch(
            r"[0-9]{6}",
            self.instrument_id,
        ) is None:
            raise ValueError("instrument_id must be a six-digit stock code")
        if type(self.session_date) is not date:
            raise ValueError("session_date must be a date")


@dataclass(frozen=True, slots=True)
class NoBarConfirmation:
    """Typed authoritative evidence that one session has no bar."""

    key: MarketSessionKey
    source_id: str
    evidence_ref: str

    def __post_init__(self) -> None:
        if not isinstance(self.key, MarketSessionKey):
            raise ValueError("confirmation requires a MarketSessionKey")
        _canonical_ref(self.source_id, "source_id")
        _canonical_ref(self.evidence_ref, "evidence_ref")


@dataclass(frozen=True, slots=True)
class FetchFailure:
    """Typed fetch failure evidence for one instrument and session."""

    key: MarketSessionKey
    source_id: str
    error_ref: str

    def __post_init__(self) -> None:
        if not isinstance(self.key, MarketSessionKey):
            raise ValueError("fetch failure requires a MarketSessionKey")
        _canonical_ref(self.source_id, "source_id")
        _canonical_ref(self.error_ref, "error_ref")


@dataclass(frozen=True, slots=True, init=False)
class BarObservation:
    """One classified bar outcome with explicit source evidence."""

    availability: BarAvailability
    key: MarketSessionKey
    no_bar_confirmation: NoBarConfirmation | None = None
    fetch_failure: FetchFailure | None = None
    _bar_payload: Mapping[str, object] | None = field(
        default=None,
        repr=False,
    )

    @classmethod
    def _create(
        cls,
        *,
        availability: BarAvailability,
        key: MarketSessionKey,
        no_bar_confirmation: NoBarConfirmation | None = None,
        fetch_failure: FetchFailure | None = None,
        bar_payload: Mapping[str, object] | None = None,
    ) -> BarObservation:
        observation = object.__new__(cls)
        object.__setattr__(observation, "availability", availability)
        object.__setattr__(observation, "key", key)
        object.__setattr__(
            observation,
            "no_bar_confirmation",
            no_bar_confirmation,
        )
        object.__setattr__(observation, "fetch_failure", fetch_failure)
        object.__setattr__(observation, "_bar_payload", bar_payload)
        observation.__post_init__()
        return observation

    def __post_init__(self) -> None:
        if not isinstance(self.availability, BarAvailability):
            raise ValueError("availability must be a BarAvailability")
        if not isinstance(self.key, MarketSessionKey):
            raise ValueError("bar observation requires a MarketSessionKey")
        confirmation = self.no_bar_confirmation
        if confirmation is not None:
            if not isinstance(confirmation, NoBarConfirmation):
                raise ValueError("no_bar_confirmation must be typed evidence")
            if confirmation.key != self.key:
                raise ValueError("no-bar confirmation key does not match the session")
        failure = self.fetch_failure
        if failure is not None:
            if not isinstance(failure, FetchFailure):
                raise ValueError("fetch_failure must be typed evidence")
            if failure.key != self.key:
                raise ValueError("fetch failure key does not match the session")
        if (
            self.availability is BarAvailability.NO_BAR_CONFIRMED
            and (confirmation is None or failure is not None)
        ):
            raise ValueError("confirmed no-bar requires source evidence")
        if self.availability is BarAvailability.FETCH_FAILED and (
            failure is None or confirmation is not None
        ):
            raise ValueError("fetch failure requires failure evidence")
        if self.availability in {
            BarAvailability.PRESENT,
            BarAvailability.UNKNOWN_NO_BAR,
        } and (confirmation is not None or failure is not None):
            raise ValueError("bar availability conflicts with its evidence")
        if self.availability is BarAvailability.PRESENT:
            if not isinstance(self._bar_payload, Mapping) or not self._bar_payload:
                raise ValueError("present observation requires a bound bar payload")
        elif self._bar_payload is not None:
            raise ValueError("non-present observation cannot carry a bar payload")

    @property
    def is_suspended(self) -> bool:
        """Only source-confirmed absence may be interpreted as suspension."""

        return self.availability is BarAvailability.NO_BAR_CONFIRMED

    def require_usable_for(
        self,
        use: BarUse,
    ) -> Mapping[str, object]:
        """Release the bound bar payload only when the session is present."""

        if not isinstance(use, BarUse):
            raise ValueError("use must be a BarUse")
        if self.availability is not BarAvailability.PRESENT:
            raise MarketDataUseError(
                f"{self.availability.value} cannot feed {use.value}"
            )
        if self._bar_payload is None:
            raise MarketDataUseError("present observation has no bound payload")
        return self._bar_payload


@dataclass(frozen=True, slots=True, init=False)
class PortfolioValuation:
    """An amount that may be consumed by NAV but not silently by a model."""

    _value: float = field(repr=False)
    state: ValuationState
    source_availability: BarAvailability
    key: MarketSessionKey

    @classmethod
    def _create(
        cls,
        *,
        value: float | int,
        state: ValuationState,
        source_availability: BarAvailability,
        key: MarketSessionKey,
    ) -> PortfolioValuation:
        valuation = object.__new__(cls)
        object.__setattr__(valuation, "_value", value)
        object.__setattr__(valuation, "state", state)
        object.__setattr__(
            valuation,
            "source_availability",
            source_availability,
        )
        object.__setattr__(valuation, "key", key)
        valuation.__post_init__()
        return valuation

    def __post_init__(self) -> None:
        value = _finite_nonnegative_value(self._value, "value")
        object.__setattr__(self, "_value", value)
        if not isinstance(self.state, ValuationState):
            raise ValueError("state must be a ValuationState")
        if not isinstance(self.source_availability, BarAvailability):
            raise ValueError("source_availability must be a BarAvailability")
        if not isinstance(self.key, MarketSessionKey):
            raise ValueError("valuation requires a MarketSessionKey")
        if (
            self.state is ValuationState.CURRENT
            and self.source_availability is not BarAvailability.PRESENT
        ):
            raise ValueError("current valuation requires a present bar")
        if (
            self.state is ValuationState.STALE_VALUATION
            and self.source_availability is BarAvailability.PRESENT
        ):
            raise ValueError("stale valuation requires a non-present bar")

    @property
    def is_stale(self) -> bool:
        return self.state is ValuationState.STALE_VALUATION

    def for_portfolio_nav(self) -> NavValuation:
        """Return a branded NAV value that preserves the freshness marker."""

        return NavValuation(
            amount=self._value,
            state=self.state,
            source_availability=self.source_availability,
            key=self.key,
        )

    def for_model_feature(self) -> float:
        """Reject stale valuation before it can enter a model feature vector."""

        if self.is_stale:
            raise MarketDataUseError("STALE_VALUATION cannot feed a model")
        return self._value


@dataclass(frozen=True, slots=True)
class NavValuation:
    """A portfolio-only amount whose freshness remains explicit."""

    amount: float
    state: ValuationState
    source_availability: BarAvailability
    key: MarketSessionKey

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "amount",
            _finite_nonnegative_value(self.amount, "amount"),
        )
        if not isinstance(self.state, ValuationState):
            raise ValueError("state must be a ValuationState")
        if not isinstance(self.source_availability, BarAvailability):
            raise ValueError("source_availability must be a BarAvailability")
        if not isinstance(self.key, MarketSessionKey):
            raise ValueError("NAV valuation requires a MarketSessionKey")
        if (
            self.state is ValuationState.CURRENT
            and self.source_availability is not BarAvailability.PRESENT
        ):
            raise ValueError("current NAV valuation requires a present bar")
        if (
            self.state is ValuationState.STALE_VALUATION
            and self.source_availability is BarAvailability.PRESENT
        ):
            raise ValueError("stale NAV valuation requires a non-present bar")


def classify_bar_availability(
    *,
    key: MarketSessionKey,
    bar_payload: Mapping[str, object] | None = None,
    no_bar_confirmation: NoBarConfirmation | None = None,
    fetch_failure: FetchFailure | None = None,
) -> BarObservation:
    """Classify one session without treating an empty response as suspension."""

    if not isinstance(key, MarketSessionKey):
        raise ValueError("key must be a MarketSessionKey")
    bar_present = bar_payload is not None
    if bar_payload is not None:
        if not isinstance(bar_payload, Mapping) or not bar_payload:
            raise ValueError("bar_payload must be a non-empty mapping")
    if no_bar_confirmation is not None:
        if not isinstance(no_bar_confirmation, NoBarConfirmation):
            raise ValueError("no_bar_confirmation must be typed evidence")
        if no_bar_confirmation.key != key:
            raise ValueError("no-bar confirmation key does not match the session")
    if fetch_failure is not None:
        if not isinstance(fetch_failure, FetchFailure):
            raise ValueError("fetch_failure must be typed evidence")
        if fetch_failure.key != key:
            raise ValueError("fetch failure key does not match the session")
    if bar_present and (
        no_bar_confirmation is not None or fetch_failure is not None
    ):
        raise ValueError("a present bar cannot carry missing-bar evidence")
    if no_bar_confirmation is not None and fetch_failure is not None:
        raise ValueError("confirmed absence and fetch failure conflict")
    if bar_present:
        payload = _freeze_payload_mapping(bar_payload)
        return BarObservation._create(
            availability=BarAvailability.PRESENT,
            key=key,
            bar_payload=payload,
        )
    if no_bar_confirmation is not None:
        return BarObservation._create(
            availability=BarAvailability.NO_BAR_CONFIRMED,
            key=key,
            no_bar_confirmation=no_bar_confirmation,
        )
    if fetch_failure is not None:
        return BarObservation._create(
            availability=BarAvailability.FETCH_FAILED,
            fetch_failure=fetch_failure,
            key=key,
        )
    return BarObservation._create(
        availability=BarAvailability.UNKNOWN_NO_BAR,
        key=key,
    )


def value_portfolio_position(
    observation: BarObservation,
    *,
    current_value: float | int | None,
    last_known_value: float | int | None,
) -> PortfolioValuation:
    """Use current value when present, otherwise mark the explicit fallback stale."""

    if not isinstance(observation, BarObservation):
        raise ValueError("observation must be a BarObservation")
    if observation.availability is BarAvailability.PRESENT:
        if current_value is None:
            raise ValueError("present bar requires current_value")
        return PortfolioValuation._create(
            value=_finite_nonnegative_value(current_value, "current_value"),
            state=ValuationState.CURRENT,
            source_availability=observation.availability,
            key=observation.key,
        )
    if current_value is not None:
        raise ValueError("a non-present bar cannot carry current_value")
    if last_known_value is None:
        raise ValueError("stale valuation requires last_known_value")
    return PortfolioValuation._create(
        value=_finite_nonnegative_value(last_known_value, "last_known_value"),
        state=ValuationState.STALE_VALUATION,
        source_availability=observation.availability,
        key=observation.key,
    )


def _canonical_optional_ref(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be canonical non-empty text")
    return value


def _canonical_ref(value: str, field_name: str) -> str:
    normalized = _canonical_optional_ref(value, field_name)
    if normalized is None:
        raise ValueError(f"{field_name} must be canonical non-empty text")
    return normalized


def _finite_nonnegative_value(value: float | int, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite non-negative number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be a finite non-negative number")
    return normalized


def _freeze_payload_mapping(
    value: Mapping[str, object] | None,
) -> Mapping[str, object]:
    if value is None:
        raise ValueError("present observation requires bar_payload")
    frozen = _freeze_payload_value(value, active_ids=set(), depth=0)
    if not isinstance(frozen, Mapping):
        raise ValueError("bar_payload must be a mapping")
    return frozen


def _freeze_payload_value(
    value: object,
    *,
    active_ids: set[int],
    depth: int,
) -> object:
    if depth > 32:
        raise ValueError("bar_payload exceeds the nesting limit")
    if value is None or isinstance(value, (str, bytes, bool, int, float, date)):
        return value
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active_ids:
            raise ValueError("bar_payload contains a cycle")
        active_ids.add(identity)
        try:
            frozen: dict[str, object] = {}
            for key, item in value.items():
                canonical_key = _canonical_ref(key, "bar_payload key")
                frozen[canonical_key] = _freeze_payload_value(
                    item,
                    active_ids=active_ids,
                    depth=depth + 1,
                )
            return MappingProxyType(frozen)
        finally:
            active_ids.remove(identity)
    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in active_ids:
            raise ValueError("bar_payload contains a cycle")
        active_ids.add(identity)
        try:
            return tuple(
                _freeze_payload_value(
                    item,
                    active_ids=active_ids,
                    depth=depth + 1,
                )
                for item in value
            )
        finally:
            active_ids.remove(identity)
    raise ValueError(
        f"bar_payload contains unsupported mutable value: {type(value).__name__}"
    )


__all__ = [
    "BarAvailability",
    "BarObservation",
    "BarUse",
    "FetchFailure",
    "MarketDataUseError",
    "MarketSessionKey",
    "NavValuation",
    "NoBarConfirmation",
    "PortfolioValuation",
    "ValuationState",
    "classify_bar_availability",
    "value_portfolio_position",
]
