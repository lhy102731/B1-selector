"""Production-owned deterministic offline provider for fake Campaign runs.

P6R3 corrective recovery (Task 6.8a): this provider is the single
production-owned fake entry point for P6 campaign executions and the shared
provider for the C0 rollout.  It exposes a fixed identity that matches the
roster binding convention (``fake-provider`` / ``offline-local`` /
``deterministic-reviewer``), emits strict JSON responses with reported or
unknown usage, and supports an explicit deterministic fault schedule
(timeout / invalid JSON / exception).  The constructor rejects URLs, API
keys and clients; the module never imports a network stack and never starts
a real subprocess.  It contains no Authority/protocol/test fixture builders.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
import tempfile
from pathlib import Path

from .campaign import ProviderResponse, UsageStatus


class OfflineProviderError(ValueError):
    """Base error for invalid offline provider configuration."""


class OfflineProviderFaultScheduleError(OfflineProviderError):
    """Raised when a scheduled fault is invalid."""


class OfflineProviderTimeout(TimeoutError):
    """Synthetic provider timeout raised by the deterministic schedule."""


class OfflineProviderInvalidResponse(OfflineProviderError):
    """Synthetic malformed JSON raised by the deterministic schedule."""


class OfflineProviderException(RuntimeError):
    """Synthetic generic exception raised by the deterministic schedule."""


class CampaignOfflineProvider:
    """Deterministic fake provider matching the roster member binding."""

    provider_name = "fake-provider"
    profile = "offline-local"
    model = "deterministic-reviewer"
    config_sha256 = "2" * 64
    capability_sha256 = "3" * 64

    def __init__(
        self,
        artifact: Mapping[str, object],
        *,
        usage: Mapping[str, object] | None = None,
        schedule: Mapping[str, object] | None = None,
        counter_path: str | None = None,
    ) -> None:
        """Build a deterministic fake provider.

        ``artifact`` is returned as strict JSON output text on every call.
        ``usage`` overrides the default reported usage (``input_tokens=7``,
        ``output_tokens=3``, ``total_tokens=10``, ``reported_cost="0.02"``,
        ``currency="USD"``); use ``{"usage_status": "unknown"}`` for UNKNOWN
        usage.  ``schedule`` maps an occurrence number (1-based) to a fault
        kind: ``"timeout"``, ``"invalid_json"`` or ``"exception"``.
        ``counter_path`` persists the call counter for subprocess reuse.
        """
        if not isinstance(artifact, Mapping):
            raise OfflineProviderError("artifact must be a mapping")
        self._artifact = {key: value for key, value in artifact.items()}
        self._usage = dict(usage) if usage is not None else None
        if schedule is not None:
            if not isinstance(schedule, Mapping):
                raise OfflineProviderFaultScheduleError("schedule must be a mapping")
            parsed: dict[int, str] = {}
            for key, value in schedule.items():
                try:
                    occurrence = int(key)
                except (TypeError, ValueError) as error:
                    raise OfflineProviderFaultScheduleError(
                        "schedule keys must be occurrence numbers"
                    ) from error
                if occurrence < 1:
                    raise OfflineProviderFaultScheduleError(
                        "schedule occurrences are 1-based"
                    )
                if value not in ("timeout", "invalid_json", "exception"):
                    raise OfflineProviderFaultScheduleError(
                        "unknown scheduled fault kind"
                    )
                parsed[occurrence] = str(value)
            self._schedule = parsed
        else:
            self._schedule = {}
        if counter_path is None:
            handle = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                delete=False,
                prefix="campaign-offline-provider-counter-",
            )
            handle.write("0")
            handle.close()
            counter_path = handle.name
            Path(counter_path).write_text("0", encoding="utf-8")
        elif not Path(counter_path).exists():
            Path(counter_path).write_text("0", encoding="utf-8")
        self._counter_path = str(counter_path)

    @property
    def call_count(self) -> int:
        try:
            raw = Path(self._counter_path).read_text(encoding="utf-8").strip()
            return int(raw or "0")
        except FileNotFoundError:
            return 0

    def _next_call_number(self) -> int:
        counter_path = Path(self._counter_path)
        with counter_path.open("r+", encoding="utf-8") as stream:
            raw = stream.read().strip() or "0"
            value = int(raw) + 1
            stream.seek(0)
            stream.write(str(value))
            stream.truncate()
        return value

    def _usage_payload(self) -> dict[str, object]:
        if self._usage is None:
            return {
                "input_tokens": 7,
                "output_tokens": 3,
                "total_tokens": 10,
                "reported_cost": "0.02",
                "currency": "USD",
            }
        return dict(self._usage)

    def invoke(self, request: object) -> ProviderResponse:
        number = self._next_call_number()
        fault = self._schedule.get(number)
        if fault == "timeout":
            raise OfflineProviderTimeout("synthetic provider timeout")
        if fault == "invalid_json":
            return ProviderResponse(
                output_text="{invalid-json",
                request_model=self.model,
                response_model=self.model,
                raw_usage=self._usage_payload(),
            )
        if fault == "exception":
            raise OfflineProviderException("synthetic provider exception")
        usage = self._usage_payload()
        usage_status = (
            UsageStatus.UNKNOWN
            if usage.get("usage_status") == "unknown"
            else UsageStatus.REPORTED
        )
        return ProviderResponse(
            output_text=json.dumps(
                self._artifact,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
            request_model=self.model,
            response_model=self.model,
            raw_usage=usage,
            usage_status=usage_status,
        )


__all__ = [
    "CampaignOfflineProvider",
    "OfflineProviderError",
    "OfflineProviderException",
    "OfflineProviderFaultScheduleError",
    "OfflineProviderInvalidResponse",
    "OfflineProviderTimeout",
]
