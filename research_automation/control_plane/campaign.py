"""Offline-safe model invocation contracts for the P6 campaign controller."""

from __future__ import annotations

import hashlib
import io
import json
import math
import multiprocessing
import pickle
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum
from multiprocessing.reduction import ForkingPickler
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_none
from typing import Protocol

from .budget import _cost as _bounded_cost


_MAX_RAW_USAGE_DEPTH = 16
_MAX_RAW_USAGE_NODES = 1024
_MAX_RAW_USAGE_COLLECTION_ITEMS = 256
_MAX_RAW_USAGE_PREFIX_UNITS = 4096
_MAX_RAW_USAGE_INT_BITS = 512
_MAX_MODEL_OUTPUT_BYTES = 48 * 1024
_MAX_MODEL_OUTPUT_INT_BITS = _MAX_RAW_USAGE_INT_BITS
_MAX_MODEL_OUTPUT_INT_DECIMAL_DIGITS = 155
_MAX_MODEL_OUTPUT_DEPTH = 32
_MAX_MODEL_OUTPUT_NODES = 4096
_MAX_REPORTED_TEXT_CHARS = 128
_MAX_PROVIDER_REQUEST_BYTES = 256 * 1024
_MAX_PROVIDER_REQUEST_DEPTH = 32
_MAX_PROVIDER_REQUEST_NODES = 8192
_MAX_PROVIDER_COLLECTION_ITEMS = 4096
_MAX_PROVIDER_PICKLE_BYTES = 256 * 1024
_MAX_PROVIDER_FRAME_BYTES = 160 * 1024
_WORKER_REAP_JOIN_SECONDS = 0.5
_PROVIDER_PROTOCOL_VERSION = 1
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


class _ModelInvocationDeadlineExceeded(ModelInvocationTimeoutError):
    """Private terminal signal for an exhausted logical-call deadline."""


class ModelInvocationProviderError(RuntimeError):
    """Raised after a provider exception has been accounted for."""


class _ModelInvocationExecutorProtocolError(ModelInvocationProviderError):
    """Private terminal signal for deterministic executor protocol failures."""


class StreamingDisabledError(RuntimeError):
    """Raised when a provider returns an unsupported streamed response."""


_USAGE_JOURNAL_ERROR_MARKER = "_control_plane_usage_journal_origin"


def _is_usage_journal_error(error: BaseException) -> bool:
    try:
        return object.__getattribute__(error, _USAGE_JOURNAL_ERROR_MARKER) is True
    except AttributeError:
        return False


def _call_usage_journal(operation, /, *args: object, **kwargs: object) -> object:
    try:
        return operation(*args, **kwargs)
    except Exception as error:
        try:
            object.__setattr__(error, _USAGE_JOURNAL_ERROR_MARKER, True)
        except Exception:
            pass
        raise


def _is_retryable_model_invocation_error(error: BaseException) -> bool:
    if _is_usage_journal_error(error):
        return False
    return isinstance(
        error,
        (
            InvalidModelResponseError,
            ModelInvocationProviderError,
            ModelInvocationTimeoutError,
        ),
    ) and not isinstance(
        error,
        (
            _ModelInvocationDeadlineExceeded,
            _ModelInvocationExecutorProtocolError,
        ),
    )


class _ModelOutputBoundsError(ValueError):
    """Raised when parsed model JSON exceeds a bounded output contract."""


class _ProviderExecutorTimeout(TimeoutError):
    """Internal signal that a provider reported its own fast timeout."""


class _ProviderExecutorDeadlineExceeded(TimeoutError):
    """Internal signal that the parent-enforced absolute deadline elapsed."""


class _ProviderExecutorConfigurationError(ValueError):
    """Internal signal for deterministic executor/preflight configuration."""


class _ProviderExecutorError(RuntimeError):
    """Base class for failures occurring after a provider attempt starts."""


class _ProviderExecutorProtocolError(_ProviderExecutorError):
    """Internal signal for a malformed or failed worker protocol."""


class _ProviderExecutorProviderError(_ProviderExecutorError):
    """Internal signal for an exception raised by the provider attempt."""


class _ProviderFrameOverflowError(ValueError):
    """Internal signal that a valid worker frame exceeded its byte bound."""


@dataclass(frozen=True)
class ProviderResponse:
    output_text: str | None
    request_model: str
    response_model: str
    raw_usage: object
    usage_status: UsageStatus | None = None
    fallback: bool = False
    streamed: bool = False


@dataclass(frozen=True, slots=True)
class _ProviderResponseSnapshot:
    output_text: str | None
    output_overflow: bool
    request_model: str
    response_model: str
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
    raw_usage_sha256: str


@dataclass(frozen=True, slots=True)
class _CanonicalJsonRequest:
    value: object
    payload: bytes


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
        deadline: float | None = None,
    ) -> InvocationOutcome: ...


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


def _parse_bounded_model_integer(value: str) -> int:
    digits = value[1:] if value.startswith("-") else value
    if len(digits) > _MAX_MODEL_OUTPUT_INT_DECIMAL_DIGITS:
        raise _ModelOutputBoundsError(
            "provider response integer exceeds the bounded size"
        )
    parsed = int(value)
    if parsed.bit_length() > _MAX_MODEL_OUTPUT_INT_BITS:
        raise _ModelOutputBoundsError(
            "provider response integer exceeds the bounded size"
        )
    return parsed


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
    if len(output_text) > max_output_bytes:
        raise _ModelOutputBoundsError(
            "provider response exceeds the bounded size"
        )
    raw_output = output_text.encode("utf-8", errors="strict")
    if len(raw_output) > max_output_bytes:
        raise _ModelOutputBoundsError(
            "provider response exceeds the bounded size"
        )
    parsed = json.loads(output_text, parse_int=_parse_bounded_model_integer)
    _require_bounded_model_output(parsed)
    canonical_output = json.dumps(
        parsed,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    canonical_output_bytes = canonical_output.encode("utf-8")
    if len(canonical_output_bytes) > max_output_bytes:
        raise _ModelOutputBoundsError(
            "provider response exceeds the bounded size"
        )
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


def _snapshot_provider_response(
    response: object,
    *,
    max_output_bytes: int,
) -> _ProviderResponseSnapshot:
    if not isinstance(response, ProviderResponse):
        raise TypeError("provider returned an invalid response object")
    if type(response.fallback) is not bool or type(response.streamed) is not bool:
        raise TypeError("provider response flags are invalid")
    if (
        type(response.response_model) is not str
        or _CONTROL_PLANE_IDENTIFIER_RE.fullmatch(response.response_model) is None
    ):
        raise TypeError("provider response model is invalid")
    if (
        type(response.request_model) is not str
        or _CONTROL_PLANE_IDENTIFIER_RE.fullmatch(response.request_model) is None
    ):
        raise TypeError("provider request model is invalid")
    if response.output_text is not None and type(response.output_text) is not str:
        raise TypeError("provider response output is invalid")

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

    output_text = response.output_text
    output_overflow = False
    if output_text is not None:
        if len(output_text) > max_output_bytes:
            output_overflow = True
        else:
            try:
                encoded_output = output_text.encode("utf-8", errors="strict")
                output_overflow = len(encoded_output) > max_output_bytes
            except UnicodeError:
                pass
    if output_overflow:
        output_text = None

    return _ProviderResponseSnapshot(
        output_text=output_text,
        output_overflow=output_overflow,
        request_model=response.request_model,
        response_model=response.response_model,
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
        raw_usage_sha256=_raw_usage_sha256(raw_usage_source),
    )


class _InlineProviderExecutor:
    """Default direct execution for test-only, cooperative adapters.

    Inline execution fences at entry and on the return side, but cannot preempt
    a provider while its synchronous ``invoke`` call is running.
    """

    __slots__ = ()

    def execute(
        self,
        provider: ModelProvider,
        request: _CanonicalJsonRequest,
        *,
        deadline: float | None,
        max_output_bytes: int,
    ) -> _ProviderResponseSnapshot:
        if deadline is not None:
            _raise_if_provider_deadline_expired(deadline)
        try:
            response = provider.invoke(request.value)
        except TimeoutError as error:
            _raise_provider_deadline_if_expired_after_failure(deadline, error)
            raise _ProviderExecutorTimeout(
                "provider invocation timed out"
            ) from error
        except Exception as error:
            _raise_provider_deadline_if_expired_after_failure(deadline, error)
            raise _ProviderExecutorProviderError(
                "provider invocation failed"
            ) from error
        if deadline is not None:
            _raise_if_provider_deadline_expired(deadline)
        try:
            snapshot = _snapshot_provider_response(
                response,
                max_output_bytes=max_output_bytes,
            )
        except Exception as error:
            _raise_provider_deadline_if_expired_after_failure(deadline, error)
            raise _ProviderExecutorProtocolError(
                "provider returned an invalid response"
            ) from error
        if deadline is not None:
            _raise_if_provider_deadline_expired(deadline)
        return snapshot


def _validate_json_request(
    value: object,
    *,
    depth: int,
    active: set[int],
    remaining_nodes: list[int],
) -> None:
    if remaining_nodes[0] <= 0:
        raise ValueError("provider request exceeds the node bound")
    remaining_nodes[0] -= 1
    if depth > _MAX_PROVIDER_REQUEST_DEPTH:
        raise ValueError("provider request exceeds the depth bound")
    if value is None or type(value) is bool:
        return
    if type(value) is int:
        if value.bit_length() > _MAX_RAW_USAGE_INT_BITS:
            raise ValueError("provider request integer exceeds the size bound")
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("provider request contains a non-finite float")
        return
    if type(value) is str:
        if len(value) > _MAX_PROVIDER_REQUEST_BYTES:
            raise ValueError("provider request string exceeds the size bound")
        return
    if type(value) not in (dict, list):
        raise TypeError("provider request must contain only strict JSON values")
    if len(value) > _MAX_PROVIDER_COLLECTION_ITEMS:
        raise ValueError("provider request collection exceeds the item bound")
    identity = id(value)
    if identity in active:
        raise ValueError("provider request contains a cycle")
    active.add(identity)
    try:
        if type(value) is dict:
            for key, item in value.items():
                if type(key) is not str:
                    raise TypeError("provider request object keys must be strings")
                _validate_json_request(
                    key,
                    depth=depth + 1,
                    active=active,
                    remaining_nodes=remaining_nodes,
                )
                _validate_json_request(
                    item,
                    depth=depth + 1,
                    active=active,
                    remaining_nodes=remaining_nodes,
                )
        else:
            for item in value:
                _validate_json_request(
                    item,
                    depth=depth + 1,
                    active=active,
                    remaining_nodes=remaining_nodes,
                )
    finally:
        active.remove(identity)


def _canonical_request_bytes(request: object) -> bytes:
    _validate_json_request(
        request,
        depth=1,
        active=set(),
        remaining_nodes=[_MAX_PROVIDER_REQUEST_NODES],
    )
    try:
        payload = json.dumps(
            request,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8", errors="strict")
    except (TypeError, ValueError, UnicodeError, RecursionError) as error:
        raise ValueError("provider request is not strict JSON") from error
    if len(payload) > _MAX_PROVIDER_REQUEST_BYTES:
        raise ValueError("provider request exceeds the byte bound")
    return payload


def _canonical_json_request(request: object) -> _CanonicalJsonRequest:
    payload = _canonical_request_bytes(request)
    return _CanonicalJsonRequest(value=json.loads(payload), payload=payload)


class _BoundedPickleBuffer(io.BytesIO):
    def write(self, data: bytes) -> int:
        if self.tell() + len(data) > _MAX_PROVIDER_PICKLE_BYTES:
            raise ValueError("provider spawn pickle exceeds the byte bound")
        return super().write(data)


def _bounded_provider_pickle(provider: ModelProvider) -> bytes:
    buffer = _BoundedPickleBuffer()
    try:
        ForkingPickler(buffer, pickle.HIGHEST_PROTOCOL).dump(provider)
    except Exception as error:
        raise _ProviderExecutorConfigurationError(
            "provider is not spawn-picklable within bounds"
        ) from error
    payload = buffer.getvalue()
    if len(payload) > _MAX_PROVIDER_PICKLE_BYTES:
        raise _ProviderExecutorConfigurationError(
            "provider spawn pickle exceeds the byte bound"
        )
    return payload


def _snapshot_payload(snapshot: _ProviderResponseSnapshot) -> dict[str, object]:
    return {
        "output_text": snapshot.output_text,
        "output_overflow": snapshot.output_overflow,
        "request_model": snapshot.request_model,
        "response_model": snapshot.response_model,
        "usage_status": snapshot.usage_status.value,
        "input_tokens": snapshot.input_tokens,
        "output_tokens": snapshot.output_tokens,
        "total_tokens": snapshot.total_tokens,
        "cache_read_tokens": snapshot.cache_read_tokens,
        "cache_write_tokens": snapshot.cache_write_tokens,
        "reasoning_tokens": snapshot.reasoning_tokens,
        "reported_cost": snapshot.reported_cost,
        "currency": snapshot.currency,
        "fallback": snapshot.fallback,
        "streamed": snapshot.streamed,
        "raw_usage_sha256": snapshot.raw_usage_sha256,
    }


def _provider_frame(tag: str, snapshot: _ProviderResponseSnapshot | None = None) -> bytes:
    frame: dict[str, object] = {"v": _PROVIDER_PROTOCOL_VERSION, "tag": tag}
    if snapshot is not None:
        frame["snapshot"] = _snapshot_payload(snapshot)
    payload = json.dumps(
        frame,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("ascii")
    if len(payload) > _MAX_PROVIDER_FRAME_BYTES:
        raise _ProviderFrameOverflowError(
            "provider worker frame exceeds the byte bound"
        )
    return payload


def _bounded_response_frame(snapshot: _ProviderResponseSnapshot) -> bytes:
    try:
        return _provider_frame("response", snapshot)
    except _ProviderFrameOverflowError:
        if snapshot.output_text is None:
            raise
        return _provider_frame(
            "response",
            replace(snapshot, output_text=None, output_overflow=True),
        )


def _spawned_provider_worker(
    provider_pickle: bytes,
    request_bytes: bytes,
    send_connection: object,
    max_output_bytes: int,
) -> None:
    """Module-level spawn target; only bounded tagged bytes leave the child."""

    try:
        try:
            provider = pickle.loads(provider_pickle)
            request = json.loads(request_bytes)
        except Exception:
            send_connection.send_bytes(_provider_frame("protocol_error"))
            return
        try:
            response = provider.invoke(request)
        except TimeoutError:
            send_connection.send_bytes(_provider_frame("provider_timeout"))
            return
        except Exception:
            send_connection.send_bytes(_provider_frame("provider_exception"))
            return
        try:
            snapshot = _snapshot_provider_response(
                response,
                max_output_bytes=max_output_bytes,
            )
            frame = _bounded_response_frame(snapshot)
        except Exception:
            frame = _provider_frame("invalid_response")
        send_connection.send_bytes(frame)
    except Exception:
        try:
            send_connection.send_bytes(_provider_frame("protocol_error"))
        except Exception:
            pass
    finally:
        try:
            send_connection.close()
        except Exception:
            pass


def _bounded_token_snapshot(value: object) -> int | None:
    if value is None:
        return None
    if (
        type(value) is not int
        or value < 0
        or value.bit_length() > _MAX_RAW_USAGE_INT_BITS
    ):
        raise ValueError("invalid token field in provider worker frame")
    return value


def _decode_provider_frame(frame: bytes) -> _ProviderResponseSnapshot:
    try:
        decoded = json.loads(frame.decode("ascii", errors="strict"))
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise _ProviderExecutorProtocolError(
            "provider worker protocol failed"
        ) from error
    if type(decoded) is not dict or decoded.get("v") != _PROVIDER_PROTOCOL_VERSION:
        raise _ProviderExecutorProtocolError("provider worker protocol failed")
    tag = decoded.get("tag")
    if tag == "provider_timeout":
        raise _ProviderExecutorTimeout("provider invocation timed out")
    if tag == "provider_exception":
        raise _ProviderExecutorProviderError("provider invocation failed")
    if tag in {"invalid_response", "protocol_error"}:
        raise _ProviderExecutorProtocolError("provider worker protocol failed")
    if tag != "response" or type(decoded.get("snapshot")) is not dict:
        raise _ProviderExecutorProtocolError("provider worker protocol failed")
    snapshot = decoded["snapshot"]
    try:
        output_text = snapshot["output_text"]
        output_overflow = snapshot["output_overflow"]
        request_model = snapshot["request_model"]
        response_model = snapshot["response_model"]
        fallback = snapshot["fallback"]
        streamed = snapshot["streamed"]
        raw_usage_sha256 = snapshot["raw_usage_sha256"]
        if output_text is not None and type(output_text) is not str:
            raise ValueError("invalid output")
        if type(output_overflow) is not bool:
            raise ValueError("invalid overflow marker")
        if (
            type(request_model) is not str
            or _CONTROL_PLANE_IDENTIFIER_RE.fullmatch(request_model) is None
            or type(response_model) is not str
            or _CONTROL_PLANE_IDENTIFIER_RE.fullmatch(response_model) is None
        ):
            raise ValueError("invalid model identifier")
        if type(fallback) is not bool or type(streamed) is not bool:
            raise ValueError("invalid flags")
        if (
            type(raw_usage_sha256) is not str
            or re.fullmatch(r"[0-9a-f]{64}", raw_usage_sha256) is None
        ):
            raise ValueError("invalid usage hash")
        usage_status = UsageStatus(snapshot["usage_status"])
        reported_cost = snapshot["reported_cost"]
        currency = snapshot["currency"]
        if reported_cost is not None:
            normalized_cost = _reported_cost({"reported_cost": reported_cost})
            if type(reported_cost) is not str or normalized_cost is None:
                raise ValueError("invalid reported cost")
        if currency is not None and (
            type(currency) is not str
            or not currency
            or len(currency) > _MAX_REPORTED_TEXT_CHARS
        ):
            raise ValueError("invalid currency")
        return _ProviderResponseSnapshot(
            output_text=output_text,
            output_overflow=output_overflow,
            request_model=request_model,
            response_model=response_model,
            usage_status=usage_status,
            input_tokens=_bounded_token_snapshot(snapshot["input_tokens"]),
            output_tokens=_bounded_token_snapshot(snapshot["output_tokens"]),
            total_tokens=_bounded_token_snapshot(snapshot["total_tokens"]),
            cache_read_tokens=_bounded_token_snapshot(snapshot["cache_read_tokens"]),
            cache_write_tokens=_bounded_token_snapshot(snapshot["cache_write_tokens"]),
            reasoning_tokens=_bounded_token_snapshot(snapshot["reasoning_tokens"]),
            reported_cost=reported_cost,
            currency=currency,
            fallback=fallback,
            streamed=streamed,
            raw_usage_sha256=raw_usage_sha256,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise _ProviderExecutorProtocolError(
            "provider worker protocol failed"
        ) from error


def _terminate_and_reap_worker(process: object, *, join_timeout: float) -> None:
    first_error: Exception | None = None
    try:
        process.terminate()
    except Exception as error:
        first_error = error
    try:
        process.join(join_timeout)
    except Exception as error:
        if first_error is None:
            first_error = error
    try:
        worker_alive = process.is_alive()
    except Exception as error:
        if first_error is None:
            first_error = error
        worker_alive = True
    if worker_alive:
        try:
            process.kill()
        except Exception as error:
            if first_error is None:
                first_error = error
        try:
            process.join(join_timeout)
        except Exception as error:
            if first_error is None:
                first_error = error
    try:
        worker_alive = process.is_alive()
    except Exception as error:
        if first_error is None:
            first_error = error
        worker_alive = True
    if first_error is not None:
        raise first_error
    if worker_alive:
        raise RuntimeError("spawned provider worker survived kill escalation")


def _best_effort_close(resource: object) -> Exception | None:
    try:
        resource.close()
    except Exception as error:
        return error
    return None


def _raise_if_provider_deadline_expired(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise _ProviderExecutorDeadlineExceeded(
            "provider invocation deadline expired"
        )


def _raise_provider_deadline_if_expired_after_failure(
    deadline: float | None,
    failure: Exception,
) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        raise _ProviderExecutorDeadlineExceeded(
            "provider invocation deadline expired"
        ) from failure


class SpawnedProviderExecutor:
    """Run a bound trusted fake provider in a directly reaped spawn process.

    P6 providers must be trusted, spawn-picklable fakes and must not create
    descendant processes.  Bounded serialization is a one-time preflight, not
    an isolation boundary.  Cleanup guarantees apply only to the direct worker.
    """

    __slots__ = ("_provider", "_provider_pickle")

    def __init__(self, provider: ModelProvider) -> None:
        self._provider = provider
        self._provider_pickle = _bounded_provider_pickle(provider)

    def execute(
        self,
        provider: ModelProvider,
        request: _CanonicalJsonRequest,
        *,
        deadline: float | None,
        max_output_bytes: int,
    ) -> _ProviderResponseSnapshot:
        if provider is not self._provider:
            raise _ProviderExecutorConfigurationError(
                "SpawnedProviderExecutor requires its bound provider identity"
            )
        if type(request) is not _CanonicalJsonRequest:
            raise _ProviderExecutorConfigurationError(
                "SpawnedProviderExecutor requires a canonical JSON request"
            )
        if (
            type(max_output_bytes) is not int
            or not 1 <= max_output_bytes <= _MAX_MODEL_OUTPUT_BYTES
        ):
            raise _ProviderExecutorConfigurationError(
                "SpawnedProviderExecutor requires the universal output bound"
            )
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise _ProviderExecutorConfigurationError(
                "SpawnedProviderExecutor requires a finite deadline"
            )
        _raise_if_provider_deadline_expired(deadline)
        try:
            context = multiprocessing.get_context("spawn")
        except Exception as error:
            if time.monotonic() >= deadline:
                raise _ProviderExecutorDeadlineExceeded(
                    "provider invocation deadline expired"
                ) from error
            raise _ProviderExecutorProtocolError(
                "provider worker context failed to construct"
            ) from error
        try:
            _raise_if_provider_deadline_expired(deadline)
            receive_connection, send_connection = context.Pipe(duplex=False)
        except Exception as error:
            if isinstance(error, _ProviderExecutorDeadlineExceeded):
                raise
            if time.monotonic() >= deadline:
                raise _ProviderExecutorDeadlineExceeded(
                    "provider invocation deadline expired"
                ) from error
            raise _ProviderExecutorProtocolError(
                "provider worker pipe failed to construct"
            ) from error
        try:
            _raise_if_provider_deadline_expired(deadline)
        except _ProviderExecutorDeadlineExceeded as error:
            cleanup_error: Exception | None = None
            for connection in (receive_connection, send_connection):
                close_error = _best_effort_close(connection)
                if cleanup_error is None and close_error is not None:
                    cleanup_error = close_error
            if cleanup_error is not None:
                raise error from cleanup_error
            raise
        try:
            worker = context.Process(
                target=_spawned_provider_worker,
                args=(
                    self._provider_pickle,
                    request.payload,
                    send_connection,
                    max_output_bytes,
                ),
            )
        except BaseException as error:
            cleanup_error: Exception | None = None
            for connection in (receive_connection, send_connection):
                close_error = _best_effort_close(connection)
                if cleanup_error is None and close_error is not None:
                    cleanup_error = close_error
            if not isinstance(error, Exception):
                raise
            if time.monotonic() >= deadline:
                raise _ProviderExecutorDeadlineExceeded(
                    "provider invocation deadline expired"
                ) from error
            raise _ProviderExecutorProtocolError(
                "provider worker failed to construct"
            ) from (error if cleanup_error is None else cleanup_error)

        worker_started = False
        worker_reaped = False
        send_connection_closed = False
        result: _ProviderResponseSnapshot | None = None
        failure: BaseException | None = None
        failure_traceback = None
        try:
            _raise_if_provider_deadline_expired(deadline)
            try:
                worker.start()
                worker_started = True
            except Exception as error:
                worker_started = getattr(worker, "pid", None) is not None
                if time.monotonic() >= deadline:
                    raise _ProviderExecutorDeadlineExceeded(
                        "provider invocation deadline expired"
                    ) from error
                raise _ProviderExecutorProtocolError(
                    "provider worker failed to start"
                ) from error
            _raise_if_provider_deadline_expired(deadline)

            try:
                send_connection.close()
                send_connection_closed = True
            except Exception as error:
                if time.monotonic() >= deadline:
                    raise _ProviderExecutorDeadlineExceeded(
                        "provider invocation deadline expired"
                    ) from error
                raise _ProviderExecutorProtocolError(
                    "provider worker protocol failed"
                ) from error
            _raise_if_provider_deadline_expired(deadline)

            remaining = max(0.0, deadline - time.monotonic())
            try:
                ready = receive_connection.poll(remaining)
            except Exception as error:
                if time.monotonic() >= deadline:
                    raise _ProviderExecutorDeadlineExceeded(
                        "provider invocation deadline expired"
                    ) from error
                raise _ProviderExecutorProtocolError(
                    "provider worker protocol failed"
                ) from error
            _raise_if_provider_deadline_expired(deadline)
            if not ready:
                raise _ProviderExecutorDeadlineExceeded(
                    "provider invocation deadline expired"
                )

            try:
                frame = receive_connection.recv_bytes(
                    maxlength=_MAX_PROVIDER_FRAME_BYTES
                )
            except (EOFError, OSError) as error:
                if time.monotonic() >= deadline:
                    raise _ProviderExecutorDeadlineExceeded(
                        "provider invocation deadline expired"
                    ) from error
                raise _ProviderExecutorProtocolError(
                    "provider worker protocol failed"
                ) from error
            _raise_if_provider_deadline_expired(deadline)

            try:
                worker.join(max(0.0, deadline - time.monotonic()))
            except Exception as error:
                if time.monotonic() >= deadline:
                    raise _ProviderExecutorDeadlineExceeded(
                        "provider invocation deadline expired"
                    ) from error
                raise _ProviderExecutorProtocolError(
                    "provider worker protocol failed"
                ) from error
            _raise_if_provider_deadline_expired(deadline)
            try:
                worker_alive = worker.is_alive()
            except Exception as error:
                if time.monotonic() >= deadline:
                    raise _ProviderExecutorDeadlineExceeded(
                        "provider invocation deadline expired"
                    ) from error
                raise _ProviderExecutorProtocolError(
                    "provider worker protocol failed"
                ) from error
            _raise_if_provider_deadline_expired(deadline)
            if worker_alive:
                raise _ProviderExecutorDeadlineExceeded(
                    "provider invocation deadline expired"
                )
            worker_reaped = True

            try:
                result = _decode_provider_frame(frame)
            except Exception as error:
                if time.monotonic() >= deadline:
                    raise _ProviderExecutorDeadlineExceeded(
                        "provider invocation deadline expired"
                    ) from error
                raise
            _raise_if_provider_deadline_expired(deadline)
        except BaseException as error:
            failure = error
            failure_traceback = error.__traceback__

        cleanup_error: Exception | None = None
        worker_may_have_started = (
            worker_started or getattr(worker, "pid", None) is not None
        )
        if worker_may_have_started and not worker_reaped:
            try:
                _terminate_and_reap_worker(
                    worker,
                    join_timeout=_WORKER_REAP_JOIN_SECONDS,
                )
            except Exception as error:
                cleanup_error = error
        for resource in (worker, receive_connection):
            close_error = _best_effort_close(resource)
            if cleanup_error is None and close_error is not None:
                cleanup_error = close_error
        if not send_connection_closed:
            close_error = _best_effort_close(send_connection)
            if cleanup_error is None and close_error is not None:
                cleanup_error = close_error

        # Control-flow interrupts win after cleanup, including if cleanup
        # crosses the deadline.  They are never provider retry signals.
        if failure is not None and not isinstance(failure, Exception):
            raise failure.with_traceback(failure_traceback)
        if time.monotonic() >= deadline:
            deadline_error = _ProviderExecutorDeadlineExceeded(
                "provider invocation deadline expired"
            )
            if cleanup_error is not None:
                raise deadline_error from cleanup_error
            if failure is not None:
                raise deadline_error from failure
            raise deadline_error
        if cleanup_error is not None:
            raise _ProviderExecutorProtocolError(
                "provider worker cleanup failed"
            ) from cleanup_error
        if failure is not None:
            raise failure.with_traceback(failure_traceback)
        if result is None:
            raise _ProviderExecutorProtocolError(
                "provider worker protocol failed"
            )
        return result


class ModelInvocation:
    """Invoke one provider attempt and account for it before parsing output.

    A public ``invoke_json`` override must preserve the attempt's atomic
    contract: before returning or raising an ordinary invocation error, it must
    complete exactly one terminal usage outcome.  Tail latency after that
    outcome is linearized must not rewrite it.
    """

    __slots__ = (
        "_provider",
        "_provider_executor",
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
        provider_executor: SpawnedProviderExecutor | None = None,
        usage_journal: UsageJournal,
        provider_name: str,
        profile: str,
        request_model: str,
        max_output_bytes: int = _MAX_MODEL_OUTPUT_BYTES,
    ) -> None:
        if type(max_output_bytes) is not int or max_output_bytes < 1:
            raise ValueError("max_output_bytes must be a positive integer")
        if max_output_bytes > _MAX_MODEL_OUTPUT_BYTES:
            raise ValueError(
                "max_output_bytes exceeds the universal 48 KiB ceiling"
            )
        if (
            provider_executor is not None
            and type(provider_executor) is not SpawnedProviderExecutor
        ):
            raise TypeError(
                "provider_executor must be an exact SpawnedProviderExecutor"
            )
        if (
            provider_executor is not None
            and provider_executor._provider is not provider
        ):
            raise ValueError(
                "SpawnedProviderExecutor must be bound to the exact provider"
            )
        self._provider = provider
        self._provider_executor = (
            _InlineProviderExecutor()
            if provider_executor is None
            else provider_executor
        )
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
        deadline: float | None = None,
    ) -> object:
        canonical_request = _canonical_json_request(request)
        try:
            response = self._provider_executor.execute(
                self._provider,
                canonical_request,
                deadline=deadline,
                max_output_bytes=self._max_output_bytes,
            )
            if type(response) is not _ProviderResponseSnapshot:
                raise _ProviderExecutorProtocolError(
                    "provider executor returned an invalid snapshot"
                )
        except _ProviderExecutorConfigurationError:
            raise
        except _ProviderExecutorDeadlineExceeded as error:
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.TIMEOUT,
            )
            raise _ModelInvocationDeadlineExceeded(
                "logical invocation deadline expired"
            ) from error
        except _ProviderExecutorTimeout as error:
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.TIMEOUT,
            )
            raise ModelInvocationTimeoutError("provider invocation timed out") from error
        except _ProviderExecutorProviderError as error:
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.EXCEPTION,
            )
            raise ModelInvocationProviderError("provider invocation failed") from error
        except _ProviderExecutorProtocolError as error:
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.EXCEPTION,
            )
            raise _ModelInvocationExecutorProtocolError(
                "provider executor protocol failed"
            ) from error
        except Exception as error:
            self._record_unknown_outcome(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.EXCEPTION,
            )
            raise _ModelInvocationExecutorProtocolError(
                "provider executor protocol failed"
            ) from error
        _call_usage_journal(
            self._usage_journal.begin,
            UsageEnvelope(
                provider=self._provider_name,
                profile=self._profile,
                request_model=response.request_model,
                response_model=response.response_model,
                call_id=call_id,
                attempt_id=attempt_id,
                usage_status=response.usage_status,
                input_tokens=response.input_tokens,
                output_tokens=response.output_tokens,
                total_tokens=response.total_tokens,
                cache_read_tokens=response.cache_read_tokens,
                cache_write_tokens=response.cache_write_tokens,
                reasoning_tokens=response.reasoning_tokens,
                reported_cost=response.reported_cost,
                currency=response.currency,
                fallback=response.fallback,
                streamed=response.streamed,
                outcome=InvocationOutcome.RESPONSE_RECEIVED,
                raw_usage_sha256=response.raw_usage_sha256,
            )
        )
        if response.streamed:
            self._commit_response_terminal(
                call_id=call_id,
                attempt_id=attempt_id,
                candidate=InvocationOutcome.STREAMING_DISABLED,
                deadline=deadline,
            )
            raise StreamingDisabledError("streaming usage accounting is not enabled")
        if response.output_overflow:
            self._commit_response_terminal(
                call_id=call_id,
                attempt_id=attempt_id,
                candidate=InvocationOutcome.INVALID_JSON,
                deadline=deadline,
            )
            raise InvalidModelResponseError("provider response is not valid JSON")
        if (
            response.output_text is None
            or not response.output_text
            or response.output_text.isspace()
        ):
            self._commit_response_terminal(
                call_id=call_id,
                attempt_id=attempt_id,
                candidate=InvocationOutcome.EMPTY_OUTPUT,
                deadline=deadline,
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
            ValueError,
            _ModelOutputBoundsError,
        ) as error:
            try:
                self._commit_response_terminal(
                    call_id=call_id,
                    attempt_id=attempt_id,
                    candidate=InvocationOutcome.INVALID_JSON,
                    deadline=deadline,
                )
            except _ModelInvocationDeadlineExceeded as deadline_error:
                raise deadline_error from error
            raise InvalidModelResponseError("provider response is not valid JSON") from error
        self._commit_response_terminal(
            call_id=call_id,
            attempt_id=attempt_id,
            candidate=InvocationOutcome.SUCCESS,
            deadline=deadline,
        )
        return parsed

    def _commit_response_terminal(
        self,
        *,
        call_id: str,
        attempt_id: str,
        candidate: InvocationOutcome,
        deadline: float | None,
    ) -> InvocationOutcome:
        actual = _call_usage_journal(
            self._usage_journal.finish,
            call_id=call_id,
            attempt_id=attempt_id,
            outcome=candidate,
            deadline=deadline,
        )
        if actual is candidate:
            return actual
        if actual is InvocationOutcome.TIMEOUT:
            raise _ModelInvocationDeadlineExceeded(
                "logical invocation deadline expired"
            )
        raise TypeError("usage journal returned an invalid terminal outcome")

    def _record_unknown_outcome(
        self,
        *,
        call_id: str,
        attempt_id: str,
        outcome: InvocationOutcome,
    ) -> None:
        _call_usage_journal(
            self._usage_journal.begin,
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

    __slots__ = ("_attempt", "_max_attempts", "_max_wall_time_ms")

    def __init__(
        self,
        *,
        attempt: ModelInvocation,
        max_attempts: int,
        max_wall_time_ms: int | None = None,
    ) -> None:
        if not isinstance(attempt, ModelInvocation):
            raise TypeError("attempt must be a ModelInvocation")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be an integer from 1 through 100")
        if max_wall_time_ms is not None and (
            type(max_wall_time_ms) is not int or max_wall_time_ms <= 0
        ):
            raise ValueError("max_wall_time_ms must be a positive integer")
        if (
            type(attempt._provider_executor) is SpawnedProviderExecutor
            and max_wall_time_ms is None
        ):
            raise ValueError(
                "spawned provider retries require max_wall_time_ms"
            )
        self._attempt = attempt
        self._max_attempts = max_attempts
        self._max_wall_time_ms = max_wall_time_ms

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
        absolute_deadline = (
            None
            if self._max_wall_time_ms is None
            else time.monotonic() + (self._max_wall_time_ms / 1000.0)
        )
        retrying = Retrying(
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_none(),
            retry=retry_if_exception(_is_retryable_model_invocation_error),
            reraise=True,
        )
        for logical_attempt in retrying:
            with logical_attempt:
                if (
                    absolute_deadline is not None
                    and time.monotonic() >= absolute_deadline
                ):
                    raise _ModelInvocationDeadlineExceeded(
                        "logical invocation deadline expired"
                    )
                attempt_number = logical_attempt.retry_state.attempt_number
                attempt_id = f"{call_id}-attempt-{attempt_number:03d}"
                output = self._attempt.invoke_json(
                    request,
                    call_id=call_id,
                    attempt_id=attempt_id,
                    deadline=absolute_deadline,
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
    "SpawnedProviderExecutor",
    "UsageEnvelope",
    "UsageJournal",
    "UsageStatus",
]
