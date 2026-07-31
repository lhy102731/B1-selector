"""Offline-safe model invocation contracts for the P6 campaign controller."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Protocol


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


class InvalidModelResponseError(ValueError):
    """Raised when a provider response cannot satisfy the invocation contract."""


class ModelInvocationTimeoutError(TimeoutError):
    """Raised after a timed-out provider attempt has been accounted for."""


class ModelInvocationProviderError(RuntimeError):
    """Raised after a provider exception has been accounted for."""


@dataclass(frozen=True)
class ProviderResponse:
    output_text: str | None
    request_model: str
    response_model: str
    raw_usage: Mapping[str, object]


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
    outcome: InvocationOutcome
    raw_usage_sha256: str


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


def _reported_token(raw_usage: Mapping[str, object], field: str) -> int | None:
    value = raw_usage.get(field)
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise ValueError(f"raw usage {field} must be a non-negative integer or null")
    return value


def _raw_usage_sha256(raw_usage: Mapping[str, object]) -> str:
    payload = json.dumps(
        dict(raw_usage),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class ModelInvocation:
    """Invoke one provider attempt and account for it before parsing output."""

    __slots__ = (
        "_provider",
        "_usage_journal",
        "_provider_name",
        "_profile",
        "_request_model",
    )

    def __init__(
        self,
        *,
        provider: ModelProvider,
        usage_journal: UsageJournal,
        provider_name: str,
        profile: str,
        request_model: str,
    ) -> None:
        self._provider = provider
        self._usage_journal = usage_journal
        self._provider_name = provider_name
        self._profile = profile
        self._request_model = request_model

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
        raw_usage = response.raw_usage
        values = {
            field: _reported_token(raw_usage, field)
            for field in ("input_tokens", "output_tokens", "total_tokens")
        }
        status = (
            UsageStatus.UNKNOWN
            if all(value is None for value in values.values())
            else UsageStatus.REPORTED
        )
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
                outcome=InvocationOutcome.RESPONSE_RECEIVED,
                raw_usage_sha256=_raw_usage_sha256(raw_usage),
            )
        )
        if response.output_text is None or not response.output_text.strip():
            self._usage_journal.finish(
                call_id=call_id,
                attempt_id=attempt_id,
                outcome=InvocationOutcome.EMPTY_OUTPUT,
            )
            raise InvalidModelResponseError("provider response is empty")
        try:
            parsed = json.loads(response.output_text)
        except (TypeError, json.JSONDecodeError) as error:
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
                outcome=outcome,
                raw_usage_sha256=_raw_usage_sha256({}),
            )
        )


__all__ = [
    "InvalidModelResponseError",
    "InvocationOutcome",
    "ModelInvocation",
    "ModelInvocationProviderError",
    "ModelInvocationTimeoutError",
    "ProviderResponse",
    "UsageEnvelope",
    "UsageJournal",
    "UsageStatus",
]
