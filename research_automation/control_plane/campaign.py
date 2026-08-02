"""Offline-safe model invocation contracts for the P6 campaign controller."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_none
from typing import Protocol

from .budget import _cost as _bounded_cost


_MAX_RAW_USAGE_DEPTH = 16
_MAX_RAW_USAGE_NODES = 1024
_MAX_RAW_USAGE_COLLECTION_ITEMS = 256
_MAX_RAW_USAGE_PREFIX_UNITS = 4096
_MAX_RAW_USAGE_INT_BITS = 512
_MAX_MODEL_OUTPUT_BYTES = 48 * 1024
_MAX_MODEL_OUTPUT_DEPTH = 32
_MAX_MODEL_OUTPUT_NODES = 4096
_MAX_REPORTED_TEXT_CHARS = 128
_CONTROL_PLANE_IDENTIFIER_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z"
)


class UsageStatus(str, Enum):
    REPORTED = "REPORTED"
    ESTIMATED = "ESTIMATED"
    UNKNOWN = "UNKNOWN"


class InvocationOutcome(str, Enum):
    RESPONSE_RECEIVED = "RESPONSE_RECEIVED"
    SUCCESS = "SUCCESS"
    EMPTY_OUTPUT = "EMPTY_OUTPUT"
    INVALID_JSON = "INVALID_JSON"
    TIMEOUT = "TIMEOUT"
    EXCEPTION = "EXCEPTION"
    STREAMING_DISABLED = "STREAMING_DISABLED"


class InvalidModelResponseError(ValueError):
    """Raised when a provider response cannot satisfy the invocation contract."""


class ModelInvocationTimeoutError(TimeoutError):
    """Raised after a timed-out provider attempt has been accounted for."""


class ModelInvocationProviderError(RuntimeError):
    """Raised after a provider exception has been accounted for."""


class StreamingDisabledError(RuntimeError):
    """Raised when a provider returns an unsupported streamed response."""


class _ModelOutputBoundsError(ValueError):
    """Raised when parsed model JSON exceeds a bounded output contract."""


@dataclass(frozen=True)
class ProviderResponse:
    output_text: str | None
    request_model: str
    response_model: str
    raw_usage: object
    usage_status: UsageStatus | None = None
    fallback: bool = False
    streamed: bool = False


@dataclass(frozen=True)
class UsageEnvelope:
    provider: str
    profile: str
    request_model: str
    response_model: str | None
    call_id: str
    attempt_id: str
    usage_status: UsageStatus
    input_tokens: int | None
    output_tokens: int | None
    total_tokens: int | None
    cache_read_tokens: int | None
    cache_write_tokens: int | None
    reasoning_tokens: int | None
    reported_cost: str | None
    currency: str | None
    fallback: bool
    streamed: bool
    outcome: InvocationOutcome
    raw_usage_sha256: str


@dataclass(frozen=True, slots=True)
class LogicalInvocationResult:
    output: object
    call_id: str
    attempt_id: str
    attempt_count: int


class ModelProvider(Protocol):
    def invoke(self, request: object) -> ProviderResponse: ...


class UsageJournal(Protocol):
    def begin(self, envelope: UsageEnvelope) -> None: ...

    def finish(
        self,
        *,
        call_id: str,
        attempt_id: str,
        outcome: InvocationOutcome,
    ) -> None: ...


def _usage_get(raw_usage: Mapping[str, object], field: str) -> object:
    try:
        return raw_usage.get(field)
    except Exception:
        return None


def _reported_token(raw_usage: Mapping[str, object], field: str) -> int | None:
    value = _usage_get(raw_usage, field)
    if value is None:
        return None
    if (
        type(value) is not int
        or value < 0
        or value.bit_length() > _MAX_RAW_USAGE_INT_BITS
    ):
        return None
    return value


def _type_name(value: object) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _safe_raw_usage_value(
    value: object,
    *,
    seen: set[int],
    remaining_nodes: list[int],
    depth: int,
) -> object:
    if remaining_nodes[0] <= 0:
        return {"$usage_marker": "node_limit"}
    remaining_nodes[0] -= 1
    if depth > _MAX_RAW_USAGE_DEPTH:
        return {"$usage_marker": "depth_limit"}
    if value is None or type(value) is bool:
        return value
    if type(value) is int:
        if value.bit_length() <= _MAX_RAW_USAGE_INT_BITS:
            return value
        return {
            "$usage_marker": "oversized_int",
            "bits": value.bit_length(),
            "negative": value < 0,
        }
    if isinstance(value, str):
        if len(value) <= _MAX_RAW_USAGE_PREFIX_UNITS:
            return value
        prefix = value[:_MAX_RAW_USAGE_PREFIX_UNITS].encode(
            "utf-8",
            errors="replace",
        )
        return {
            "$usage_marker": "oversized_text",
            "characters": len(value),
            "prefix_characters": _MAX_RAW_USAGE_PREFIX_UNITS,
            "prefix_sha256": hashlib.sha256(prefix).hexdigest(),
        }
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return {"$usage_marker": "nonfinite_float", "value": str(value)}
    if isinstance(value, Decimal):
        try:
            finite = value.is_finite()
            adjusted = value.adjusted() if finite else None
            signed = value.is_signed()
        except Exception as error:
            return {
                "$usage_marker": "decimal_error",
                "error_type": _type_name(error),
            }
        return {
            "$usage_marker": "decimal",
            "finite": finite,
            "adjusted": adjusted,
            "signed": signed,
        }
    if isinstance(value, bytes):
        prefix = value[:_MAX_RAW_USAGE_PREFIX_UNITS]
        return {
            "$usage_marker": "bytes",
            "bytes": len(value),
            "prefix_bytes": len(prefix),
            "prefix_sha256": hashlib.sha256(prefix).hexdigest(),
        }
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in seen:
            return {"$usage_marker": "cycle", "type": _type_name(value)}
        seen.add(identity)
        pairs: list[list[object]] = []
        truncated = False
        error_type: str | None = None
        try:
            for index, (key, item) in enumerate(value.items()):
                if index >= _MAX_RAW_USAGE_COLLECTION_ITEMS:
                    truncated = True
                    break
                pairs.append(
                    [
                        _safe_raw_usage_value(
                            key,
                            seen=seen,
                            remaining_nodes=remaining_nodes,
                            depth=depth + 1,
                        ),
                        _safe_raw_usage_value(
                            item,
                            seen=seen,
                            remaining_nodes=remaining_nodes,
                            depth=depth + 1,
                        ),
                    ]
                )
        except Exception as error:
            error_type = _type_name(error)
        finally:
            seen.remove(identity)
        pairs.sort(
            key=lambda pair: json.dumps(
                pair[0],
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        normalized: dict[str, object] = {
            "$usage_marker": "mapping",
            "items": pairs,
        }
        if truncated:
            normalized["truncated"] = True
        if error_type is not None:
            normalized["iteration_error_type"] = error_type
        return normalized
    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in seen:
            return {"$usage_marker": "cycle", "type": _type_name(value)}
        seen.add(identity)
        items: list[object] = []
        truncated = False
        error_type: str | None = None
        try:
            for index, item in enumerate(value):
                if index >= _MAX_RAW_USAGE_COLLECTION_ITEMS:
                    truncated = True
                    break
                items.append(
                    _safe_raw_usage_value(
                        item,
                        seen=seen,
                        remaining_nodes=remaining_nodes,
                        depth=depth + 1,
                    )
                )
        except Exception as error:
            error_type = _type_name(error)
        finally:
            seen.remove(identity)
        normalized_sequence: dict[str, object] = {
            "$usage_marker": "sequence",
            "type": _type_name(value),
            "items": items,
            "truncated": truncated,
        }
        if error_type is not None:
            normalized_sequence["iteration_error_type"] = error_type
        return normalized_sequence
    return {"$usage_marker": "unsupported_type", "type": _type_name(value)}


def _raw_usage_sha256(raw_usage: object) -> str:
    try:
        normalized = _safe_raw_usage_value(
            raw_usage,
            seen=set(),
            remaining_nodes=[_MAX_RAW_USAGE_NODES],
            depth=0,
        )
        payload = json.dumps(
            normalized,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except Exception as error:
        payload = json.dumps(
            {
                "$usage_marker": "normalization_error",
                "raw_type": _type_name(raw_usage),
                "error_type": _type_name(error),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _require_bounded_model_output(value: object) -> None:
    stack: list[tuple[object, int]] = [(value, 1)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > _MAX_MODEL_OUTPUT_NODES:
            raise _ModelOutputBoundsError(
                f"provider response exceeds {_MAX_MODEL_OUTPUT_NODES} node limit"
            )
        if depth > _MAX_MODEL_OUTPUT_DEPTH:
            raise _ModelOutputBoundsError(
                f"provider response exceeds {_MAX_MODEL_OUTPUT_DEPTH} level "
                "nesting limit"
            )
        if isinstance(current, Mapping):
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)


def _parse_bounded_model_output(
    output_text: str,
    *,
    max_output_bytes: int,
) -> object:
    raw_output = output_text.encode("utf-8", errors="strict")
    if len(raw_output) > max_output_bytes:
        raise _ModelOutputBoundsError(
            "provider response exceeds the bounded size"
        )
    parsed = json.loads(output_text)
    _require_bounded_model_output(parsed)
    return parsed


def _reported_text(raw_usage: Mapping[str, object], field: str) -> str | None:
    value = _usage_get(raw_usage, field)
    if value is None:
        return None
    if (
        type(value) is not str
        or len(value) > _MAX_REPORTED_TEXT_CHARS
        or not value.strip()
    ):
        return None
    return value.strip()


def _reported_cost(raw_usage: Mapping[str, object]) -> str | None:
    """Normalize provider cost scalars before applying the ledger bound.

    Provider SDKs commonly expose cost as a native float or Decimal; both are
    converted to decimal text, then validated by the same bounded parser used
    by BudgetLedger. The stored envelope always carries text, never a float.
    """
    value = _usage_get(raw_usage, "reported_cost")
    if type(value) is str:
        try:
            _bounded_cost(value)
        except ValueError:
            return None
        candidate = value.strip()
    elif type(value) is int:
        if value.bit_length() > _MAX_RAW_USAGE_INT_BITS:
            return None
        try:
            candidate = str(value)
        except (OverflowError, ValueError):
            return None
    elif type(value) is float:
        candidate = str(value)
    elif type(value) is Decimal:
        candidate = str(value)
    else:
        return None
    if not candidate:
        return None
    try:
        _bounded_cost(candidate)
    except ValueError:
        return None
    return candidate


class ModelInvocation:
    """Invoke one provider attempt and account for it before parsing output."""

    __slots__ = (
        "_provider",
        "_usage_journal",
        "_provider_name",
        "_profile",
        "_request_model",
        "_max_output_bytes",
    )

    def __init__(
        self,
        *,
        provider: ModelProvider,
        usage_journal: UsageJournal,
        provider_name: str,
        profile: str,
        request_model: str,
        max_output_bytes: int = _MAX_MODEL_OUTPUT_BYTES,
    ) -> None:
        if type(max_output_bytes) is not int or max_output_bytes < 1:
            raise ValueError("max_output_bytes must be a positive integer")
        self._provider = provider
        self._usage_journal = usage_journal
        self._provider_name = provider_name
        self._profile = profile
        self._request_model = request_model
        self._max_output_bytes = max_output_bytes

    def invoke_json(
        self,
        request: object,
        *,
        call_id: str,
        attempt_id: str,
    ) -> object:
        try:
            response = self._provider.invoke(request)
        except TimeoutError as error:
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.TIMEOUT,
            )
            raise ModelInvocationTimeoutError("provider invocation timed out") from error
        except Exception as error:
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.EXCEPTION,
            )
            raise ModelInvocationProviderError("provider invocation failed") from error
        if not isinstance(response, ProviderResponse):
            error = TypeError("provider returned an invalid response object")
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.EXCEPTION,
            )
            raise ModelInvocationProviderError("provider invocation failed") from error
        if (
            type(response.fallback) is not bool
            or type(response.streamed) is not bool
        ):
            error = TypeError("provider response flags are invalid")
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.EXCEPTION,
            )
            raise ModelInvocationProviderError("provider invocation failed") from error
        if (
            type(response.response_model) is not str
            or _CONTROL_PLANE_IDENTIFIER_RE.fullmatch(response.response_model)
            is None
        ):
            error = TypeError("provider response model is invalid")
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.EXCEPTION,
            )
            raise ModelInvocationProviderError("provider invocation failed") from error
        if response.output_text is not None and type(response.output_text) is not str:
            error = TypeError("provider response output is invalid")
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.EXCEPTION,
            )
            raise ModelInvocationProviderError("provider invocation failed") from error
        raw_usage_source = response.raw_usage
        raw_usage = raw_usage_source if isinstance(raw_usage_source, Mapping) else {}
        values = {
            field: _reported_token(raw_usage, field)
            for field in (
                "input_tokens",
                "output_tokens",
                "total_tokens",
                "cache_read_tokens",
                "cache_write_tokens",
                "reasoning_tokens",
            )
        }
        known_component_tokens = sum(
            int(values[field])
            for field in ("input_tokens", "output_tokens")
            if values[field] is not None
        )
        if (
            values["total_tokens"] is not None
            and values["total_tokens"] < known_component_tokens
        ):
            values["total_tokens"] = known_component_tokens
        reported_cost = _reported_cost(raw_usage)
        currency = _reported_text(raw_usage, "currency")
        status_hint = response.usage_status
        if status_hint is not None and not isinstance(status_hint, UsageStatus):
            status_hint = None
        has_known_usage = (
            any(value is not None for value in values.values())
            or reported_cost is not None
        )
        if not has_known_usage:
            status = UsageStatus.UNKNOWN
            values = {field: None for field in values}
            reported_cost = None
            currency = None
        elif status_hint is UsageStatus.ESTIMATED:
            status = UsageStatus.ESTIMATED
        else:
            status = UsageStatus.REPORTED
        self._usage_journal.begin(
            UsageEnvelope(
                provider=self._provider_name,
                profile=self._profile,
                request_model=self._request_model,
                response_model=response.response_model,
                call_id=call_id,
                attempt_id=attempt_id,
                usage_status=status,
                input_tokens=values["input_tokens"],
                output_tokens=values["output_tokens"],
                total_tokens=values["total_tokens"],
                cache_read_tokens=values["cache_read_tokens"],
                cache_write_tokens=values["cache_write_tokens"],
                reasoning_tokens=values["reasoning_tokens"],
                reported_cost=reported_cost,
                currency=currency,
                fallback=response.fallback,
                streamed=response.streamed,
                outcome=InvocationOutcome.RESPONSE_RECEIVED,
                raw_usage_sha256=_raw_usage_sha256(raw_usage_source),
            )
        )
        if response.streamed:
            self._usage_journal.finish(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.STREAMING_DISABLED,
            )
            raise StreamingDisabledError("streaming usage accounting is not enabled")
        if response.output_text is None or not response.output_text.strip():
            self._usage_journal.finish(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.EMPTY_OUTPUT,
            )
            raise InvalidModelResponseError("provider response is empty")
        try:
            parsed = _parse_bounded_model_output(
                response.output_text,
                max_output_bytes=self._max_output_bytes,
            )
        except (
            UnicodeError,
            RecursionError,
            json.JSONDecodeError,
            _ModelOutputBoundsError,
        ) as error:
            self._usage_journal.finish(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.INVALID_JSON,
            )
            raise InvalidModelResponseError("provider response is not valid JSON") from error
        self._usage_journal.finish(
            call_id=call_id,
            attempt_id=attempt_id,
            outcome=InvocationOutcome.SUCCESS,
        )
        return parsed

    def _record_unknown_outcome(
        self,
        *,
        call_id: str,
        attempt_id: str,
        outcome: InvocationOutcome,
    ) -> None:
        self._usage_journal.begin(
            UsageEnvelope(
                provider=self._provider_name,
                profile=self._profile,
                request_model=self._request_model,
                response_model=None,
                call_id=call_id,
                attempt_id=attempt_id,
                usage_status=UsageStatus.UNKNOWN,
                input_tokens=None,
                output_tokens=None,
                total_tokens=None,
                cache_read_tokens=None,
                cache_write_tokens=None,
                reasoning_tokens=None,
                reported_cost=None,
                currency=None,
                fallback=False,
                streamed=False,
                outcome=outcome,
                raw_usage_sha256=_raw_usage_sha256({}),
            )
        )


class RetryingModelInvocation:
    """Run bounded logical attempts; provider adapters remain single-call only."""

    __slots__ = ("_attempt", "_max_attempts")

    def __init__(self, *, attempt: ModelInvocation, max_attempts: int) -> None:
        if not isinstance(attempt, ModelInvocation):
            raise TypeError("attempt must be a ModelInvocation")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be an integer from 1 through 100")
        self._attempt = attempt
        self._max_attempts = max_attempts

    def invoke_json(self, request: object, *, call_id: str) -> object:
        return self.invoke_json_with_receipt(
            request,
            call_id=call_id,
        ).output

    def invoke_json_with_receipt(
        self,
        request: object,
        *,
        call_id: str,
    ) -> LogicalInvocationResult:
        retrying = Retrying(
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_none(),
            retry=retry_if_exception_type(
                (
                    InvalidModelResponseError,
                    ModelInvocationProviderError,
                    ModelInvocationTimeoutError,
                )
            ),
            reraise=True,
        )
        for logical_attempt in retrying:
            with logical_attempt:
                attempt_number = logical_attempt.retry_state.attempt_number
                attempt_id = f"{call_id}-attempt-{attempt_number:03d}"
                output = self._attempt.invoke_json(
                    request,
                    call_id=call_id,
                    attempt_id=attempt_id,
                )
                return LogicalInvocationResult(
                    output=output,
                    call_id=call_id,
                    attempt_id=attempt_id,
                    attempt_count=attempt_number,
                )
        raise RuntimeError("logical retry loop terminated without an outcome")


__all__ = [
    "InvalidModelResponseError",
    "InvocationOutcome",
    "LogicalInvocationResult",
    "ModelInvocation",
    "ModelInvocationProviderError",
    "ModelInvocationTimeoutError",
    "ProviderResponse",
    "RetryingModelInvocation",
    "StreamingDisabledError",
    "UsageEnvelope",
    "UsageJournal",
    "UsageStatus",
]
