"""Fake/injected provider adapters with normalized usage and retry ownership.

P6 adapter boundary for campaign provider calls. This module contains only
injected/fake adapters and pure usage normalization helpers. It never opens
network connections, never imports provider SDKs, and never changes the
behavior of existing production invocation code. Logical retry ownership
remains exclusively in ``RetryingModelInvocation``; every adapter here is a
single-call boundary and fails closed when an injected client advertises
SDK-level internal retries.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import math
import re
from typing import Protocol

from .campaign import (
    ModelInvocation,
    ProviderResponse,
    RetryingModelInvocation,
    UsageStatus,
)


_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_MAX_RAW_USAGE_INT_BITS = 512
_MAX_REPORTED_TEXT_CHARS = 256
_MAX_COST_TEXT_CHARS = 128
_USAGE_TOKEN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
)


class ProviderAdapterError(ValueError):
    """Base error for invalid adapter input or configuration."""


class RetryOwnershipError(ProviderAdapterError):
    """Raised when an injected client has SDK-level retries enabled."""


@dataclass(frozen=True, slots=True)
class NormalizedUsage:
    """Canonical usage fields used to build a ``ProviderResponse``."""

    usage_status: UsageStatus
    input_tokens: int | None
    output_tokens: int | None
    total_tokens: int | None
    cache_read_tokens: int | None
    cache_write_tokens: int | None
    reasoning_tokens: int | None
    reported_cost: str | None
    currency: str | None


def _usage_get(raw_usage: Mapping[str, object], field: str) -> object:
    try:
        return raw_usage.get(field)
    except Exception:
        return None


def _normalized_token(
    raw_usage: Mapping[str, object],
    field: str,
) -> int | None:
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


def _normalized_text(
    raw_usage: Mapping[str, object],
    field: str,
) -> str | None:
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


def _normalized_cost(raw_usage: Mapping[str, object]) -> str | None:
    value = _usage_get(raw_usage, "reported_cost")
    if value is None:
        return None
    if type(value) is str:
        candidate = value.strip()
    elif type(value) is int:
        if value.bit_length() > _MAX_RAW_USAGE_INT_BITS:
            return None
        candidate = str(value)
    elif type(value) is float:
        if not math.isfinite(value):
            return None
        candidate = repr(value)
    elif isinstance(value, Decimal):
        try:
            if not value.is_finite():
                return None
            candidate = str(value)
        except Exception:
            return None
    else:
        return None
    if (
        not candidate
        or len(candidate) > _MAX_COST_TEXT_CHARS
    ):
        return None
    try:
        parsed = Decimal(candidate)
    except InvalidOperation:
        return None
    if not parsed.is_finite():
        return None
    return candidate
def normalize_usage(
    raw_usage: object,
    *,
    usage_status_hint: UsageStatus | None = None,
) -> NormalizedUsage:
    """Normalize raw provider usage into canonical token/cost fields.

    Missing or malformed values become ``None`` and are never replaced with a
    zero or a fixed estimate. When no usage is known the status is UNKNOWN and
    every value is null. An explicit ESTIMATED hint or a truthy ``estimated``
    flag in the raw usage selects ESTIMATED; otherwise REPORTED.
    """
    source = raw_usage if isinstance(raw_usage, Mapping) else {}
    values = {
        field: _normalized_token(source, field) for field in _USAGE_TOKEN_FIELDS
    }
    reported_cost = _normalized_cost(source)
    currency = _normalized_text(source, "currency")
    estimated_flag = _usage_get(source, "estimated")
    has_known_usage = (
        any(value is not None for value in values.values())
        or reported_cost is not None
    )
    if not has_known_usage:
        return NormalizedUsage(
            usage_status=UsageStatus.UNKNOWN,
            input_tokens=None,
            output_tokens=None,
            total_tokens=None,
            cache_read_tokens=None,
            cache_write_tokens=None,
            reasoning_tokens=None,
            reported_cost=None,
            currency=None,
        )
    if usage_status_hint is UsageStatus.ESTIMATED or estimated_flag is True:
        status = UsageStatus.ESTIMATED
    else:
        status = UsageStatus.REPORTED
    total_tokens = values["total_tokens"]
    known_components = (values["input_tokens"] or 0) + (
        values["output_tokens"] or 0
    )
    if total_tokens is not None and total_tokens < known_components:
        total_tokens = known_components
    return NormalizedUsage(
        usage_status=status,
        input_tokens=values["input_tokens"],
        output_tokens=values["output_tokens"],
        total_tokens=total_tokens,
        cache_read_tokens=values["cache_read_tokens"],
        cache_write_tokens=values["cache_write_tokens"],
        reasoning_tokens=values["reasoning_tokens"],
        reported_cost=reported_cost,
        currency=currency,
    )


class ProviderResponseNormalizer:
    """Normalize mapping or ``ProviderResponse`` payloads to the campaign contract."""

    def normalize(
        self,
        raw: object,
        *,
        request_model: str,
        response_model: str,
        fallback: bool = False,
        streamed: bool = False,
    ) -> ProviderResponse:
        if not _IDENTIFIER_RE.fullmatch(request_model):
            raise ProviderAdapterError("request model is not a valid identifier")
        if not _IDENTIFIER_RE.fullmatch(response_model):
            raise ProviderAdapterError("response model is not a valid identifier")
        if type(fallback) is not bool or type(streamed) is not bool:
            raise ProviderAdapterError("adapter flags must be booleans")
        if isinstance(raw, ProviderResponse):
            return self._normalize_provider_response(
                raw,
                request_model=request_model,
                response_model=response_model,
                fallback=fallback,
                streamed=streamed,
            )
        if isinstance(raw, Mapping):
            return self._normalize_mapping(
                raw,
                request_model=request_model,
                response_model=response_model,
                fallback=fallback,
                streamed=streamed,
            )
        raise ProviderAdapterError(
            "adapter response must be a mapping or ProviderResponse"
        )

    def _normalize_mapping(
        self,
        raw: Mapping[str, object],
        *,
        request_model: str,
        response_model: str,
        fallback: bool,
        streamed: bool,
    ) -> ProviderResponse:
        output_text = raw.get("output_text", raw.get("output"))
        if output_text is not None and type(output_text) is not str:
            raise ProviderAdapterError("adapter output must be a string or null")
        observed_request = raw.get("request_model", request_model)
        observed_response = raw.get("response_model", response_model)
        if observed_request != request_model or observed_response != response_model:
            raise ProviderAdapterError("adapter model identifiers disagree")
        if "fallback" in raw:
            fallback = raw["fallback"]
        if "streamed" in raw:
            streamed = raw["streamed"]
        if type(fallback) is not bool or type(streamed) is not bool:
            raise ProviderAdapterError("adapter flags must be booleans")
        raw_usage = raw.get("raw_usage", raw.get("usage", {}))
        hint = raw.get("usage_status")
        usage_status_hint = hint if isinstance(hint, UsageStatus) else None
        normalized = normalize_usage(
            raw_usage,
            usage_status_hint=usage_status_hint,
        )
        return ProviderResponse(
            output_text=output_text,
            request_model=request_model,
            response_model=response_model,
            raw_usage=raw_usage,
            usage_status=normalized.usage_status,
            fallback=fallback,
            streamed=streamed,
        )

    def _normalize_provider_response(
        self,
        response: ProviderResponse,
        *,
        request_model: str,
        response_model: str,
        fallback: bool,
        streamed: bool,
    ) -> ProviderResponse:
        if (
            response.request_model != request_model
            or response.response_model != response_model
        ):
            raise ProviderAdapterError("adapter model identifiers disagree")
        if type(response.fallback) is not bool or type(response.streamed) is not bool:
            raise ProviderAdapterError("adapter flags must be booleans")
        if response.output_text is not None and type(response.output_text) is not str:
            raise ProviderAdapterError("adapter output must be a string or null")
        normalized = normalize_usage(
            response.raw_usage,
            usage_status_hint=response.usage_status,
        )
        return ProviderResponse(
            output_text=response.output_text,
            request_model=request_model,
            response_model=response_model,
            raw_usage=response.raw_usage,
            usage_status=normalized.usage_status,
            fallback=response.fallback,
            streamed=response.streamed,
        )
class CallableProviderAdapter:
    """Adapt an injected single-call callable into the campaign provider contract."""

    def __init__(
        self,
        handler: Callable[[object], object],
        *,
        request_model: str,
        response_model: str,
        normalizer: ProviderResponseNormalizer | None = None,
    ) -> None:
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._handler = handler
        self._request_model = request_model
        self._response_model = response_model
        self._normalizer = normalizer or ProviderResponseNormalizer()
        self._calls = 0
        self._last_request: object = None

    @property
    def calls(self) -> int:
        return self._calls

    @property
    def last_request(self) -> object:
        return self._last_request

    def invoke(self, request: object) -> ProviderResponse:
        self._calls += 1
        self._last_request = request
        raw = self._handler(request)
        return self._normalizer.normalize(
            raw,
            request_model=self._request_model,
            response_model=self._response_model,
        )


class RetryDisabledClientAdapter:
    """Adapt an injected client whose SDK-level retries are provably disabled.

    The client must expose ``max_retries`` equal to zero (or omit the
    attribute entirely for non-SDK fakes) and a callable ``invoke`` method.
    A positive retry count fails closed with ``RetryOwnershipError`` so that
    logical retry ownership stays exclusively in ``RetryingModelInvocation``.
    """

    def __init__(
        self,
        client: object,
        *,
        request_model: str,
        response_model: str,
        normalizer: ProviderResponseNormalizer | None = None,
    ) -> None:
        retries = getattr(client, "max_retries", None)
        if retries is not None and retries != 0:
            raise RetryOwnershipError(
                "injected client must have SDK-level retries disabled"
            )
        invoker = getattr(client, "invoke", None)
        if not callable(invoker):
            raise TypeError("injected client must expose a callable invoke method")
        self._client = client
        self._invoker = invoker
        self._request_model = request_model
        self._response_model = response_model
        self._normalizer = normalizer or ProviderResponseNormalizer()
        self._calls = 0

    @property
    def calls(self) -> int:
        return self._calls

    def invoke(self, request: object) -> ProviderResponse:
        self._calls += 1
        raw = self._invoker(request)
        return self._normalizer.normalize(
            raw,
            request_model=self._request_model,
            response_model=self._response_model,
        )


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded logical retry policy owned by ``RetryingModelInvocation``."""

    max_attempts: int
    max_wall_time_ms: int | None = None

    def __post_init__(self) -> None:
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 100:
            raise ProviderAdapterError(
                "max_attempts must be an integer from 1 through 100"
            )
        if self.max_wall_time_ms is not None and (
            type(self.max_wall_time_ms) is not int or self.max_wall_time_ms <= 0
        ):
            raise ProviderAdapterError(
                "max_wall_time_ms must be a positive integer"
            )


def build_retrying_invocation(
    *,
    attempt: ModelInvocation,
    policy: RetryPolicy,
) -> RetryingModelInvocation:
    """Construct the logical retry owner for one adapter-backed attempt."""
    if not isinstance(attempt, ModelInvocation):
        raise TypeError("attempt must be a ModelInvocation")
    if not isinstance(policy, RetryPolicy):
        raise TypeError("policy must be a RetryPolicy")
    return RetryingModelInvocation(
        attempt=attempt,
        max_attempts=policy.max_attempts,
        max_wall_time_ms=policy.max_wall_time_ms,
    )


class AG2InjectedSeamAdapter:
    """Adapt an injected AG2-style single-call callable (no AG2 import).

    The handler must return an AG2-style mapping with ``output_text``/``output``
    and optional ``usage``/``raw_usage``.  This seam never imports or starts
    real AG2, never reads profile secrets, and owns no retry logic.
    """

    def __init__(
        self,
        handler: Callable[[object], object],
        *,
        request_model: str,
        response_model: str,
        normalizer: ProviderResponseNormalizer | None = None,
    ) -> None:
        if not callable(handler):
            raise TypeError("AG2 handler must be callable")
        self._handler = handler
        self._request_model = request_model
        self._response_model = response_model
        self._normalizer = normalizer or ProviderResponseNormalizer()
        self._calls = 0
        self._last_request: object = None

    @property
    def calls(self) -> int:
        return self._calls

    @property
    def last_request(self) -> object:
        return self._last_request

    def invoke(self, request: object) -> ProviderResponse:
        self._calls += 1
        self._last_request = request
        raw = self._handler(request)
        if not isinstance(raw, Mapping):
            raise ProviderAdapterError("AG2 response must be a mapping")
        return self._normalizer.normalize(
            raw,
            request_model=self._request_model,
            response_model=self._response_model,
        )


class OpenAICompatibleInjectedSeamAdapter:
    """Adapt an injected OpenAI-compatible callable with SDK retries disabled.

    The injected callable must already have SDK-level retries disabled
    (``max_retries == 0`` or absent for fakes).  Response/usage are
    standardized through the normalizer; missing values become ``None`` +
    ``UsageStatus.UNKNOWN``, never fabricated zeros.
    """

    def __init__(
        self,
        handler: Callable[[object], object],
        *,
        request_model: str,
        response_model: str,
        max_retries: int | None = 0,
        normalizer: ProviderResponseNormalizer | None = None,
    ) -> None:
        if not callable(handler):
            raise TypeError("OpenAI-compatible handler must be callable")
        if max_retries is not None and max_retries != 0:
            raise RetryOwnershipError(
                "injected OpenAI-compatible client must have SDK retries disabled"
            )
        self._handler = handler
        self._request_model = request_model
        self._response_model = response_model
        self._normalizer = normalizer or ProviderResponseNormalizer()
        self._calls = 0
        self._last_request: object = None

    @property
    def calls(self) -> int:
        return self._calls

    @property
    def last_request(self) -> object:
        return self._last_request

    def invoke(self, request: object) -> ProviderResponse:
        self._calls += 1
        self._last_request = request
        raw = self._handler(request)
        if not isinstance(raw, Mapping):
            raise ProviderAdapterError("OpenAI-compatible response must be a mapping")
        return self._normalizer.normalize(
            raw,
            request_model=self._request_model,
            response_model=self._response_model,
        )


class CliInjectedSeamAdapter:
    """Adapt an injected subprocess runner into the provider contract.

    Only accepts an argv vector and an injected runner callable; shell strings,
    out-of-bounds stdin/stdout, non-zero exits, timeouts and malformed JSON are
    rejected or surfaced as explicit outcomes.  stderr/raw output never leaks
    into the safe result.
    """

    def __init__(
        self,
        runner: Callable[[list[str]], tuple[int, str]],
        *,
        request_model: str,
        response_model: str,
        normalizer: ProviderResponseNormalizer | None = None,
        max_argv_chars: int = 4096,
        max_stdout_chars: int = 1 << 20,
    ) -> None:
        if not callable(runner):
            raise TypeError("CLI runner must be callable")
        if type(max_argv_chars) is not int or max_argv_chars <= 0:
            raise ProviderAdapterError("max_argv_chars must be a positive integer")
        if type(max_stdout_chars) is not int or max_stdout_chars <= 0:
            raise ProviderAdapterError("max_stdout_chars must be a positive integer")
        self._runner = runner
        self._request_model = request_model
        self._response_model = response_model
        self._normalizer = normalizer or ProviderResponseNormalizer()
        self._max_argv_chars = max_argv_chars
        self._max_stdout_chars = max_stdout_chars
        self._calls = 0
        self._last_request: object = None

    @property
    def calls(self) -> int:
        return self._calls

    @property
    def last_request(self) -> object:
        return self._last_request

    def invoke(self, request: object) -> ProviderResponse:
        self._calls += 1
        self._last_request = request
        if not isinstance(request, list) or not all(
            isinstance(part, str) for part in request
        ):
            raise ProviderAdapterError("CLI seam request must be an argv vector")
        argv = list(request)
        if sum(len(part) for part in argv) > self._max_argv_chars:
            raise ProviderAdapterError("CLI argv exceeds the bounded size")
        exit_code, stdout = self._runner(argv)
        if exit_code != 0:
            raise ProviderAdapterError(
                f"CLI provider exited with code {exit_code}"
            )
        if len(stdout) > self._max_stdout_chars:
            raise ProviderAdapterError("CLI stdout exceeds the bounded size")
        try:
            import json as _json

            raw = _json.loads(stdout)
        except ValueError as error:
            raise ProviderAdapterError("CLI stdout is not valid JSON") from error
        if not isinstance(raw, Mapping):
            raise ProviderAdapterError("CLI stdout must be a JSON object")
        return self._normalizer.normalize(
            raw,
            request_model=self._request_model,
            response_model=self._response_model,
        )


__all__ = [
    "AG2InjectedSeamAdapter",
    "CallableProviderAdapter",
    "CliInjectedSeamAdapter",
    "NormalizedUsage",
    "OpenAICompatibleInjectedSeamAdapter",
    "ProviderAdapterError",
    "ProviderResponseNormalizer",
    "RetryDisabledClientAdapter",
    "RetryOwnershipError",
    "RetryPolicy",
    "build_retrying_invocation",
    "normalize_usage",
]
