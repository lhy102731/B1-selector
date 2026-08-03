from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from collections.abc import Iterator, Mapping
from dataclasses import replace
from decimal import Decimal

from research_automation.control_plane import campaign as campaign_module
from research_automation.control_plane.campaign import (
    InvalidModelResponseError,
    InvocationOutcome,
    ModelInvocationProviderError,
    ModelInvocationTimeoutError,
    ModelInvocation,
    ProviderResponse,
    RetryingModelInvocation,
    SpawnedProviderExecutor,
    StreamingDisabledError,
    UsageEnvelope,
    UsageStatus,
)


class _FakeProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text="{not-json",
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage={
                "input_tokens": 17,
                "output_tokens": 5,
                "total_tokens": 22,
            },
        )


class _ChildPidProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text=json.dumps({"pid": os.getpid(), "request": request}),
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage={
                "input_tokens": 7,
                "output_tokens": 3,
                "total_tokens": 10,
                "opaque": lambda: None,
            },
        )


class _CountingReduceProvider:
    reduce_calls = 0

    def __reduce_ex__(self, protocol: int) -> object:
        del protocol
        type(self).reduce_calls += 1
        return (type(self), ())

    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text=json.dumps({"request": request}),
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage={},
        )


class _InvocationMarkerProvider:
    def __init__(self, marker_path: str) -> None:
        self._marker_path = marker_path

    def invoke(self, request: object) -> ProviderResponse:
        Path(self._marker_path).write_text("invoked", encoding="ascii")
        return ProviderResponse(
            output_text=json.dumps({"request": request}),
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage={},
        )


class _RequestSemanticsProvider:
    def invoke(self, request: object) -> ProviderResponse:
        assert type(request) is dict
        request_object = request
        return ProviderResponse(
            output_text=json.dumps(
                {
                    "request": request_object,
                    "shared_identity": request_object["left"]
                    is request_object["right"],
                }
            ),
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage={},
        )


class _HangingProvider:
    def __init__(self, marker_path: str) -> None:
        self._marker_path = marker_path

    def invoke(self, request: object) -> ProviderResponse:
        Path(self._marker_path).write_text(str(os.getpid()), encoding="ascii")
        while True:
            time.sleep(0.05)


class _SpawnExceptionProvider:
    def invoke(self, request: object) -> ProviderResponse:
        raise RuntimeError("provider detail must not cross the process boundary")


class _AbruptExitProvider:
    def invoke(self, request: object) -> ProviderResponse:
        raise SystemExit(7)


class _OversizedSpawnOutputProvider:
    def __init__(self, marker_path: str) -> None:
        self._marker_path = marker_path

    def invoke(self, request: object) -> ProviderResponse:
        Path(self._marker_path).write_text(str(os.getpid()), encoding="ascii")
        return ProviderResponse(
            output_text=json.dumps({"payload": "x" * (64 * 1024)}),
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage={"input_tokens": 2, "output_tokens": 20},
        )


_ESCAPED_FRAME_OVERFLOW_USAGE = {
    "input_tokens": 101,
    "output_tokens": 202,
    "total_tokens": 303,
    "cache_read_tokens": 11,
    "cache_write_tokens": 12,
    "reasoning_tokens": 13,
    "reported_cost": "0.0125",
    "currency": "USD",
}


class _EscapedFrameOverflowProvider:
    def __init__(self, marker_path: str) -> None:
        self._marker_path = marker_path

    def invoke(self, request: object) -> ProviderResponse:
        Path(self._marker_path).write_text(str(os.getpid()), encoding="ascii")
        return ProviderResponse(
            output_text="\x01" * 30_000,
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage=_ESCAPED_FRAME_OVERFLOW_USAGE,
            fallback=True,
        )


class _OversizedPickleProvider:
    def __init__(self) -> None:
        self.payload = b"x" * (512 * 1024)

    def invoke(self, request: object) -> ProviderResponse:
        raise AssertionError("oversized provider must not be started")


class _NoProcessContext:
    def __init__(self) -> None:
        self.process_calls = 0

    def Process(self, *args: object, **kwargs: object) -> object:
        self.process_calls += 1
        raise AssertionError("request/provider preflight must happen before Process")


class _PipeFailureContext:
    def __init__(
        self,
        *,
        clock: "_ControlledClock | None" = None,
        consume_seconds: float = 0.0,
    ) -> None:
        self._clock = clock
        self._consume_seconds = consume_seconds
        self.pipe_calls = 0
        self.process_calls = 0

    def Pipe(self, *, duplex: bool) -> tuple[object, object]:
        if duplex:
            raise AssertionError("spawn boundary pipe must be one-way")
        self.pipe_calls += 1
        if self._clock is not None:
            self._clock.now += self._consume_seconds
        raise OSError("synthetic pipe construction failure")

    def Process(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        self.process_calls += 1
        raise AssertionError("Process must not run after Pipe failure")


class _ProcessConstructionFailureContext:
    def __init__(
        self,
        *,
        clock: "_ControlledClock | None" = None,
        consume_seconds: float = 0.0,
    ) -> None:
        self._clock = clock
        self._consume_seconds = consume_seconds
        self.pipe_calls = 0
        self.process_calls = 0
        self.receive = _ClosablePipeEnd()
        self.send = _ClosablePipeEnd()

    def Pipe(self, *, duplex: bool) -> tuple[object, object]:
        if duplex:
            raise AssertionError("spawn boundary pipe must be one-way")
        self.pipe_calls += 1
        return self.receive, self.send

    def Process(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        self.process_calls += 1
        if self._clock is not None:
            self._clock.now += self._consume_seconds
        raise OSError("synthetic process construction failure")


class _EscalationProcess:
    def __init__(self) -> None:
        self.actions: list[object] = []
        self._killed = False

    def terminate(self) -> None:
        self.actions.append("terminate")

    def join(self, timeout: float | None = None) -> None:
        self.actions.append(("join", timeout))

    def is_alive(self) -> bool:
        self.actions.append("is_alive")
        return not self._killed

    def kill(self) -> None:
        self.actions.append("kill")
        self._killed = True


class _ClosablePipeEnd:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _ControlledClock:
    def __init__(self, now: float) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now


class _DeadlineFenceProcess:
    pid = None

    def __init__(self) -> None:
        self.start_calls = 0
        self.terminate_calls = 0
        self.kill_calls = 0
        self.closed = False
        self._alive = False

    def start(self) -> None:
        self.start_calls += 1
        self.pid = 12345
        self._alive = True

    def terminate(self) -> None:
        self.terminate_calls += 1
        self._alive = False

    def join(self, timeout: float | None = None) -> None:
        del timeout

    def is_alive(self) -> bool:
        return self._alive

    def kill(self) -> None:
        self.kill_calls += 1
        self._alive = False

    def close(self) -> None:
        self.closed = True


class _DeadlineFenceReceive(_ClosablePipeEnd):
    def poll(self, timeout: float) -> bool:
        del timeout
        return False


class _DeadlineFenceContext:
    def __init__(self, clock: _ControlledClock, deadline: float) -> None:
        self._clock = clock
        self._deadline = deadline
        self.process_calls = 0
        self.receive = _DeadlineFenceReceive()
        self.send = _ClosablePipeEnd()
        self.process = _DeadlineFenceProcess()

    def Pipe(self, *, duplex: bool) -> tuple[object, object]:
        if duplex:
            raise AssertionError("spawn boundary pipe must be one-way")
        return self.receive, self.send

    def Process(self, *args: object, **kwargs: object) -> _DeadlineFenceProcess:
        del args, kwargs
        self.process_calls += 1
        self._clock.now = self._deadline
        return self.process


class _StartFailureProcess:
    pid = None

    def __init__(self) -> None:
        self.closed = False

    def start(self) -> None:
        raise OSError("synthetic spawn failure")

    def close(self) -> None:
        self.closed = True


class _StartFailureContext:
    def __init__(self) -> None:
        self.receive = _ClosablePipeEnd()
        self.send = _ClosablePipeEnd()
        self.process = _StartFailureProcess()

    def Pipe(self, *, duplex: bool) -> tuple[_ClosablePipeEnd, _ClosablePipeEnd]:
        if duplex:
            raise AssertionError("spawn boundary pipe must be one-way")
        return self.receive, self.send

    def Process(self, *args: object, **kwargs: object) -> _StartFailureProcess:
        return self.process


class _PollFailurePipeEnd:
    def __init__(self, actions: list[object], name: str) -> None:
        self._actions = actions
        self._name = name
        self.closed = False

    def poll(self, timeout: float) -> bool:
        self._actions.append(("receive.poll", timeout))
        raise OSError("synthetic poll failure")

    def close(self) -> None:
        self._actions.append(f"{self._name}.close")
        self.closed = True


class _PollFailureProcess:
    pid = 12345

    def __init__(self, actions: list[object]) -> None:
        self._actions = actions
        self._alive = False
        self.closed = False

    def start(self) -> None:
        self._actions.append("process.start")
        self._alive = True

    def terminate(self) -> None:
        self._actions.append("process.terminate")

    def join(self, timeout: float | None = None) -> None:
        self._actions.append(("process.join", timeout))

    def is_alive(self) -> bool:
        self._actions.append("process.is_alive")
        return self._alive

    def kill(self) -> None:
        self._actions.append("process.kill")
        self._alive = False

    def close(self) -> None:
        self._actions.append("process.close")
        self.closed = True


class _PollFailureContext:
    def __init__(self) -> None:
        self.actions: list[object] = []
        self.receive = _PollFailurePipeEnd(self.actions, "receive")
        self.send = _PollFailurePipeEnd(self.actions, "send")
        self.process = _PollFailureProcess(self.actions)

    def Pipe(
        self, *, duplex: bool
    ) -> tuple[_PollFailurePipeEnd, _PollFailurePipeEnd]:
        if duplex:
            raise AssertionError("spawn boundary pipe must be one-way")
        return self.receive, self.send

    def Process(self, *args: object, **kwargs: object) -> _PollFailureProcess:
        return self.process


class _OutputTextProvider:
    def __init__(self, output_text: str) -> None:
        self._output_text = output_text

    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text=self._output_text,
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage={},
        )


class _EmptyProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text="   ",
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage={},
        )


class _NullOutputProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text=None,
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage={},
        )


class _TimeoutProvider:
    def invoke(self, request: object) -> ProviderResponse:
        raise TimeoutError("fake provider timeout")


class _ExceptionProvider:
    def invoke(self, request: object) -> ProviderResponse:
        raise RuntimeError("fake provider failure")


class _MalformedResponseProvider:
    def invoke(self, request: object) -> object:
        return {"output_text": '{"status":"not-a-response"}'}


class _MalformedFieldsProvider:
    def __init__(self, **changes: object) -> None:
        self._changes = changes

    def invoke(self, request: object) -> ProviderResponse:
        response = ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-request-model",
            response_model="fake-response-model",
            raw_usage={"input_tokens": 1},
        )
        return replace(response, **self._changes)


class _FallbackProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="provider-misattributed-model",
            response_model="fake-fallback-model",
            raw_usage={
                "input_tokens": 20,
                "output_tokens": 8,
                "total_tokens": 28,
                "cache_read_tokens": 6,
                "cache_write_tokens": 2,
                "reasoning_tokens": 3,
                "reported_cost": 0.00125,
                "currency": "USD" * 1000,
            },
            fallback=True,
            streamed=False,
        )


class _EstimatedUsageProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-estimated-model",
            response_model="fake-estimated-model",
            raw_usage={
                "input_tokens": 12,
                "output_tokens": 4,
                "total_tokens": 16,
                "reported_cost": "0.01",
                "currency": "USD",
            },
            usage_status=UsageStatus.ESTIMATED,
        )


class _StreamingProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"partial"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={"input_tokens": 4, "output_tokens": 2, "total_tokens": 6},
            streamed=True,
        )


class _MalformedUsageProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={
                "input_tokens": 10**5000,
                "output_tokens": "five",
                "total_tokens": True,
            },
        )


class _NonFiniteCostProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={"reported_cost": float("nan")},
        )


class _StringNonFiniteCostProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={"reported_cost": "NaN"},
        )


class _CyclicUsageProvider:
    def invoke(self, request: object) -> ProviderResponse:
        raw_usage: dict[str, object] = {}
        raw_usage["cycle"] = raw_usage
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage=raw_usage,
        )


class _NonMappingUsageProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage=b"provider-usage",
        )


class _RaisingGetMapping(Mapping[str, object]):
    def __getitem__(self, key: str) -> object:
        raise RuntimeError("provider mapping lookup failed")

    def __iter__(self) -> Iterator[str]:
        return iter(("input_tokens",))

    def __len__(self) -> int:
        return 1


class _RaisingGetUsageProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage=_RaisingGetMapping(),
        )


class _OversizedBytesUsageProvider:
    def __init__(self, tail: bytes) -> None:
        self._tail = tail

    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage=(b"x" * 5000) + self._tail,
        )


class _OversizedCostProvider:
    def __init__(self, value: object) -> None:
        self._value = value

    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={"reported_cost": self._value},
        )


class _RaisingCurrencyProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={"currency": _RaisingString("USD")},
        )


class _RaisingSequence(list[object]):
    def __getitem__(self, key: object) -> object:
        if isinstance(key, slice):
            raise RuntimeError("sequence slicing failed")
        return super().__getitem__(key)  # type: ignore[index]


class _RaisingSequenceUsageProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage=_RaisingSequence(["input_tokens", 1]),
        )


class _OversizedDecimalUsageProvider:
    def __init__(self, tail: str) -> None:
        self._tail = tail

    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage=Decimal(("1" * 1000) + self._tail),
        )


class _MalformedUnicodeUsageProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={"currency": "\ud800"},
        )


class _RaisingString(str):
    def __len__(self) -> int:
        raise RuntimeError("string length failed")


class _RaisingStringUsageProvider:
    def invoke(self, request: object) -> ProviderResponse:
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage=_RaisingString("provider-usage"),
        )


class _TimeoutThenSuccessProvider:
    def __init__(self) -> None:
        self.call_count = 0

    def invoke(self, request: object) -> ProviderResponse:
        self.call_count += 1
        if self.call_count == 1:
            raise TimeoutError("first fake attempt timed out")
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={"input_tokens": 3, "output_tokens": 2, "total_tokens": 5},
        )


class _SpawnTimeoutThenSuccessProvider:
    def __init__(self, first_attempt_marker: str) -> None:
        self.first_attempt_marker = first_attempt_marker

    def invoke(self, request: object) -> ProviderResponse:
        del request
        marker = Path(self.first_attempt_marker)
        if not marker.exists():
            marker.write_text(str(os.getpid()), encoding="ascii")
            raise TimeoutError("first spawned provider attempt timed out")
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={"input_tokens": 3, "output_tokens": 2, "total_tokens": 5},
        )


class _ExceptionThenSuccessProvider:
    def __init__(self) -> None:
        self.call_count = 0

    def invoke(self, request: object) -> ProviderResponse:
        del request
        self.call_count += 1
        if self.call_count == 1:
            raise RuntimeError("first provider attempt failed")
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={},
        )


class _InvalidJsonThenSuccessProvider:
    def __init__(self) -> None:
        self.call_count = 0

    def invoke(self, request: object) -> ProviderResponse:
        del request
        self.call_count += 1
        return ProviderResponse(
            output_text=("{not-json" if self.call_count == 1 else '{"status":"ok"}'),
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={},
        )


class _SlowTimeoutProvider:
    def __init__(self, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds
        self.call_count = 0

    def invoke(self, request: object) -> ProviderResponse:
        del request
        self.call_count += 1
        time.sleep(self.delay_seconds)
        raise TimeoutError("provider timed out after consuming the budget")


class _StreamingThenSuccessProvider:
    def __init__(self) -> None:
        self.call_count = 0

    def invoke(self, request: object) -> ProviderResponse:
        self.call_count += 1
        if self.call_count == 1:
            return ProviderResponse(
                output_text='{"status":"partial"}',
                request_model="fake-primary-model",
                response_model="fake-primary-model",
                raw_usage={"input_tokens": 2, "output_tokens": 1},
                streamed=True,
            )
        return ProviderResponse(
            output_text='{"status":"ok"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={"input_tokens": 2, "output_tokens": 1},
        )


class _RecordingUsageJournal:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def begin(self, envelope: UsageEnvelope) -> None:
        self.events.append(("begin", envelope))

    def finish(
        self,
        *,
        call_id: str,
        attempt_id: str,
        outcome: InvocationOutcome,
    ) -> None:
        self.events.append(("finish", (call_id, attempt_id, outcome)))


def _spawned_invocation(
    provider: object,
    journal: _RecordingUsageJournal,
) -> ModelInvocation:
    return ModelInvocation(
        provider=provider,
        provider_executor=SpawnedProviderExecutor(provider),
        usage_journal=journal,
        provider_name="fake",
        profile="offline",
        request_model="fake-request-model",
    )


def _active_child_pids() -> set[int]:
    return {
        child.pid
        for child in multiprocessing.active_children()
        if child.pid is not None
    }


class ModelInvocationTests(unittest.TestCase):
    def test_model_invocation_rejects_arbitrary_provider_executor(self) -> None:
        class ArbitraryExecutor:
            def execute(self, *args: object, **kwargs: object) -> object:
                raise AssertionError("arbitrary executor must never be called")

        with self.assertRaisesRegex(
            TypeError,
            "provider_executor must be an exact SpawnedProviderExecutor",
        ):
            ModelInvocation(
                provider=_ChildPidProvider(),
                provider_executor=ArbitraryExecutor(),
                usage_journal=_RecordingUsageJournal(),
                provider_name="fake",
                profile="offline",
                request_model="fake-request-model",
            )

        self.assertFalse(hasattr(campaign_module, "ProviderExecutor"))
        self.assertFalse(hasattr(campaign_module, "InlineProviderExecutor"))
        self.assertNotIn("ProviderExecutor", campaign_module.__all__)
        self.assertNotIn("InlineProviderExecutor", campaign_module.__all__)

        bound_provider = _ChildPidProvider()
        with self.assertRaisesRegex(ValueError, "exact provider"):
            ModelInvocation(
                provider=_ChildPidProvider(),
                provider_executor=SpawnedProviderExecutor(bound_provider),
                usage_journal=_RecordingUsageJournal(),
                provider_name="fake",
                profile="offline",
                request_model="fake-request-model",
            )

    def test_spawned_executor_binds_and_serializes_provider_once_at_construction(
        self,
    ) -> None:
        baseline_pids = _active_child_pids()
        provider = _CountingReduceProvider()
        _CountingReduceProvider.reduce_calls = 0
        executor = SpawnedProviderExecutor(provider)

        self.assertEqual(_CountingReduceProvider.reduce_calls, 1)
        self.assertEqual(_active_child_pids() - baseline_pids, set())
        invocation = ModelInvocation(
            provider=provider,
            provider_executor=executor,
            usage_journal=_RecordingUsageJournal(),
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        for index in range(2):
            self.assertEqual(
                invocation.invoke_json(
                    {"index": index},
                    call_id=f"call-cached-provider-{index}",
                    attempt_id="attempt-001",
                    deadline=time.monotonic() + 10.0,
                ),
                {"request": {"index": index}},
            )

        self.assertEqual(_CountingReduceProvider.reduce_calls, 1)

    def test_model_invocation_rejects_output_limit_above_universal_ceiling(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "universal 48 KiB ceiling"):
            ModelInvocation(
                provider=_ChildPidProvider(),
                usage_journal=_RecordingUsageJournal(),
                provider_name="fake",
                profile="offline",
                request_model="fake-request-model",
                max_output_bytes=(48 * 1024) + 1,
            )

    def test_inline_and_spawn_reject_non_strict_json_before_attempt_or_usage(
        self,
    ) -> None:
        cyclic: list[object] = []
        cyclic.append(cyclic)
        invalid_requests = (
            ("tuple", {"value": (1, 2)}),
            ("custom", {"value": object()}),
            ("nonfinite", {"value": float("nan")}),
            ("cyclic", cyclic),
            ("oversized", {"value": "x" * (256 * 1024 + 1)}),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            for executor_kind in ("inline", "spawn"):
                for name, request in invalid_requests:
                    with self.subTest(executor=executor_kind, request=name):
                        marker = Path(temp_dir) / f"{executor_kind}-{name}.marker"
                        provider = _InvocationMarkerProvider(str(marker))
                        journal = _RecordingUsageJournal()
                        executor = (
                            None
                            if executor_kind == "inline"
                            else SpawnedProviderExecutor(provider)
                        )
                        invocation = ModelInvocation(
                            provider=provider,
                            provider_executor=executor,
                            usage_journal=journal,
                            provider_name="fake",
                            profile="offline",
                            request_model="fake-request-model",
                        )

                        with self.assertRaises((TypeError, ValueError)):
                            invocation.invoke_json(
                                request,
                                call_id=f"call-{executor_kind}-{name}",
                                attempt_id="attempt-001",
                                deadline=time.monotonic() + 10.0,
                            )

                        self.assertFalse(marker.exists())
                        self.assertEqual(journal.events, [])

    def test_inline_and_spawn_receive_the_same_json_round_tripped_value(self) -> None:
        baseline_pids = _active_child_pids()
        shared_list = ["same-input-object"]
        request = {"left": shared_list, "right": shared_list}
        results: list[object] = []

        for executor_kind in ("inline", "spawn"):
            with self.subTest(executor=executor_kind):
                provider = _RequestSemanticsProvider()
                journal = _RecordingUsageJournal()
                invocation = (
                    ModelInvocation(
                        provider=provider,
                        usage_journal=journal,
                        provider_name="fake",
                        profile="offline",
                        request_model="fake-request-model",
                    )
                    if executor_kind == "inline"
                    else _spawned_invocation(provider, journal)
                )
                results.append(
                    invocation.invoke_json(
                        request,
                        call_id=f"call-json-round-trip-{executor_kind}",
                        attempt_id="attempt-001",
                        deadline=time.monotonic() + 10.0,
                    )
                )

        self.assertEqual(results[0], results[1])
        self.assertEqual(
            results[0],
            {
                "request": {
                    "left": ["same-input-object"],
                    "right": ["same-input-object"],
                },
                "shared_identity": False,
            },
        )
        self.assertEqual(_active_child_pids() - baseline_pids, set())

    def test_invalid_executor_snapshot_is_accounted_and_raised_as_provider_error(
        self,
    ) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_ChildPidProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with patch.object(
            campaign_module._InlineProviderExecutor,
            "execute",
            return_value=object(),
        ):
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-invalid-executor-snapshot",
                    attempt_id="attempt-001",
                )

        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_spawned_executor_runs_provider_in_child_and_reaps_it(self) -> None:
        baseline_pids = _active_child_pids()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)

        result = invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-spawn-success",
            attempt_id="attempt-001",
            deadline=time.monotonic() + 10.0,
        )

        self.assertNotEqual(result["pid"], os.getpid())
        self.assertEqual(result["request"], {"prompt": "offline-only"})
        self.assertFalse(
            any(child.pid == result["pid"] for child in multiprocessing.active_children())
        )
        self.assertEqual(_active_child_pids() - baseline_pids, set())
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.REPORTED)
        self.assertEqual(envelope.total_tokens, 10)

    def test_spawned_executor_hard_deadline_reaps_hanging_child_and_accounts_timeout(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            baseline_pids = _active_child_pids()
            marker = Path(temp_dir) / "hanging-child.pid"
            journal = _RecordingUsageJournal()
            invocation = _spawned_invocation(
                _HangingProvider(str(marker)), journal
            )
            started = time.monotonic()

            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "hang"},
                    call_id="call-spawn-timeout",
                    attempt_id="attempt-001",
                    deadline=started + 5.0,
                )

            self.assertLess(time.monotonic() - started, 8.0)
            child_pid = int(marker.read_text(encoding="ascii"))
            self.assertNotEqual(child_pid, os.getpid())
            self.assertFalse(
                any(child.pid == child_pid for child in multiprocessing.active_children())
            )
            self.assertEqual(_active_child_pids() - baseline_pids, set())
            envelope = journal.events[0][1]
            self.assertIsInstance(envelope, UsageEnvelope)
            self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
            self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_spawned_provider_exception_is_reaped_and_accounted(self) -> None:
        baseline_pids = _active_child_pids()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_SpawnExceptionProvider(), journal)

        with self.assertRaises(ModelInvocationProviderError) as raised:
            invocation.invoke_json(
                {"prompt": "explode"},
                call_id="call-spawn-exception",
                attempt_id="attempt-001",
                deadline=time.monotonic() + 10.0,
            )

        self.assertNotIn("provider detail", str(raised.exception))
        self.assertEqual(_active_child_pids() - baseline_pids, set())
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_spawned_provider_timeout_tag_is_reaped_and_accounted(self) -> None:
        baseline_pids = _active_child_pids()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_TimeoutProvider(), journal)

        with self.assertRaises(ModelInvocationTimeoutError):
            invocation.invoke_json(
                {"prompt": "timeout"},
                call_id="call-spawn-provider-timeout",
                attempt_id="attempt-001",
                deadline=time.monotonic() + 10.0,
            )

        self.assertEqual(_active_child_pids() - baseline_pids, set())
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_spawned_invalid_response_is_reaped_and_accounted(self) -> None:
        baseline_pids = _active_child_pids()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_MalformedResponseProvider(), journal)

        with self.assertRaises(ModelInvocationProviderError):
            invocation.invoke_json(
                {"prompt": "invalid-response"},
                call_id="call-spawn-invalid-response",
                attempt_id="attempt-001",
                deadline=time.monotonic() + 10.0,
            )

        self.assertEqual(_active_child_pids() - baseline_pids, set())
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_spawned_worker_eof_is_reaped_and_accounted(self) -> None:
        baseline_pids = _active_child_pids()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_AbruptExitProvider(), journal)

        with self.assertRaises(ModelInvocationProviderError):
            invocation.invoke_json(
                {"prompt": "abrupt-exit"},
                call_id="call-spawn-eof",
                attempt_id="attempt-001",
                deadline=time.monotonic() + 10.0,
            )

        self.assertEqual(_active_child_pids() - baseline_pids, set())
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_spawned_oversized_output_is_accounted_then_fails_without_live_child(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            baseline_pids = _active_child_pids()
            marker = Path(temp_dir) / "oversized-child.pid"
            journal = _RecordingUsageJournal()
            invocation = _spawned_invocation(
                _OversizedSpawnOutputProvider(str(marker)), journal
            )

            with self.assertRaises(InvalidModelResponseError):
                invocation.invoke_json(
                    {"prompt": "oversized"},
                    call_id="call-spawn-overflow",
                    attempt_id="attempt-001",
                    deadline=time.monotonic() + 10.0,
                )

            child_pid = int(marker.read_text(encoding="ascii"))
            self.assertFalse(
                any(child.pid == child_pid for child in multiprocessing.active_children())
            )
            self.assertEqual(_active_child_pids() - baseline_pids, set())
            envelope = journal.events[0][1]
            self.assertIsInstance(envelope, UsageEnvelope)
            self.assertEqual(envelope.usage_status, UsageStatus.REPORTED)
            self.assertEqual(envelope.input_tokens, 2)
            self.assertEqual(
                journal.events[1][1],
                (
                    "call-spawn-overflow",
                    "attempt-001",
                    InvocationOutcome.INVALID_JSON,
                ),
            )

    def test_spawned_escaped_frame_overflow_preserves_reported_usage(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            baseline_pids = _active_child_pids()
            marker = Path(temp_dir) / "escaped-frame-overflow-child.pid"
            journal = _RecordingUsageJournal()
            invocation = _spawned_invocation(
                _EscapedFrameOverflowProvider(str(marker)), journal
            )

            with self.assertRaises(InvalidModelResponseError):
                invocation.invoke_json(
                    {"prompt": "escaped-frame-overflow"},
                    call_id="call-spawn-escaped-frame-overflow",
                    attempt_id="attempt-001",
                    deadline=time.monotonic() + 10.0,
                )

            child_pid = int(marker.read_text(encoding="ascii"))
            self.assertFalse(
                any(child.pid == child_pid for child in multiprocessing.active_children())
            )
            self.assertEqual(_active_child_pids() - baseline_pids, set())
            envelope = journal.events[0][1]
            self.assertIsInstance(envelope, UsageEnvelope)
            self.assertEqual(envelope.usage_status, UsageStatus.REPORTED)
            self.assertEqual(envelope.request_model, "fake-request-model")
            self.assertEqual(envelope.response_model, "fake-response-model")
            self.assertEqual(envelope.input_tokens, 101)
            self.assertEqual(envelope.output_tokens, 202)
            self.assertEqual(envelope.total_tokens, 303)
            self.assertEqual(envelope.cache_read_tokens, 11)
            self.assertEqual(envelope.cache_write_tokens, 12)
            self.assertEqual(envelope.reasoning_tokens, 13)
            self.assertEqual(envelope.reported_cost, "0.0125")
            self.assertEqual(envelope.currency, "USD")
            self.assertTrue(envelope.fallback)
            self.assertFalse(envelope.streamed)
            self.assertEqual(
                envelope.raw_usage_sha256,
                campaign_module._raw_usage_sha256(_ESCAPED_FRAME_OVERFLOW_USAGE),
            )
            self.assertEqual(
                journal.events[1][1],
                (
                    "call-spawn-escaped-frame-overflow",
                    "attempt-001",
                    InvocationOutcome.INVALID_JSON,
                ),
            )

    def test_spawned_executor_rejects_non_json_request_without_process_start(
        self,
    ) -> None:
        context = _NoProcessContext()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)

        with patch.object(campaign_module.multiprocessing, "get_context", return_value=context):
            with self.assertRaises((TypeError, ValueError)):
                invocation.invoke_json(
                    {"not-json": object()},
                    call_id="call-spawn-request-reject",
                    attempt_id="attempt-001",
                    deadline=time.monotonic() + 10.0,
                )

        self.assertEqual(context.process_calls, 0)
        self.assertEqual(journal.events, [])

    def test_spawned_executor_rejects_oversized_provider_pickle_without_start(
        self,
    ) -> None:
        context = _NoProcessContext()
        journal = _RecordingUsageJournal()
        provider = _OversizedPickleProvider()

        with patch.object(
            campaign_module.multiprocessing, "get_context", return_value=context
        ):
            with self.assertRaises(ValueError):
                ModelInvocation(
                    provider=provider,
                    provider_executor=SpawnedProviderExecutor(provider),
                    usage_journal=journal,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-request-model",
                )

        self.assertEqual(context.process_calls, 0)
        self.assertEqual(journal.events, [])

    def test_spawned_executor_rejects_unbounded_json_without_process_start(
        self,
    ) -> None:
        context = _NoProcessContext()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)

        with patch.object(campaign_module.multiprocessing, "get_context", return_value=context):
            with self.assertRaises((TypeError, ValueError)):
                invocation.invoke_json(
                    {"prompt": "x" * (256 * 1024 + 1)},
                    call_id="call-spawn-request-overflow",
                    attempt_id="attempt-001",
                    deadline=time.monotonic() + 10.0,
                )

        self.assertEqual(context.process_calls, 0)
        self.assertEqual(journal.events, [])

    def test_deadline_cleanup_escalates_terminate_join_kill_join(self) -> None:
        process = _EscalationProcess()

        campaign_module._terminate_and_reap_worker(process, join_timeout=0.25)

        self.assertEqual(
            process.actions,
            [
                "terminate",
                ("join", 0.25),
                "is_alive",
                "kill",
                ("join", 0.25),
                "is_alive",
            ],
        )

    def test_spawn_failure_closes_both_pipe_ends_and_process_handle(self) -> None:
        context = _StartFailureContext()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)

        with patch.object(campaign_module.multiprocessing, "get_context", return_value=context):
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-spawn-start-failure",
                    attempt_id="attempt-001",
                    deadline=time.monotonic() + 10.0,
                )

        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertTrue(context.process.closed)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_spawned_poll_failure_reaps_and_closes_every_worker_resource(self) -> None:
        context = _PollFailureContext()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)

        with patch.object(campaign_module.multiprocessing, "get_context", return_value=context):
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-spawn-poll-failure",
                    attempt_id="attempt-001",
                    deadline=time.monotonic() + 10.0,
                )

        poll_timeout = context.actions[2][1]
        self.assertGreater(poll_timeout, 0.0)
        self.assertEqual(
            context.actions,
            [
                "process.start",
                "send.close",
                ("receive.poll", poll_timeout),
                "process.terminate",
                ("process.join", 0.5),
                "process.is_alive",
                "process.kill",
                ("process.join", 0.5),
                "process.is_alive",
                "process.close",
                "receive.close",
            ],
        )
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_raw_utf8_output_exact_limit_succeeds_and_next_byte_rejects(self) -> None:
        escaped_as = r"\u0061" * 1_000
        cjk_character = chr(0x4E2D)
        filler = "a" * (48 * 1024 - 17 - len(escaped_as))
        exact_output = '{"payload":"' + escaped_as + cjk_character + filler + '"}'
        next_output = '{"payload":"' + escaped_as + cjk_character + filler + "a" + '"}'

        exact_canonical = json.dumps(
            json.loads(exact_output),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        self.assertLess(len(exact_output), 48 * 1024)
        self.assertEqual(len(exact_output.encode("utf-8")), 48 * 1024)
        self.assertLess(len(exact_canonical.encode("utf-8")), 48 * 1024)
        self.assertEqual(len(next_output.encode("utf-8")), 48 * 1024 + 1)

        exact_journal = _RecordingUsageJournal()
        exact_invocation = ModelInvocation(
            provider=_OutputTextProvider(exact_output),
            usage_journal=exact_journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )
        self.assertEqual(
            exact_invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-raw-utf8-exact",
                attempt_id="attempt-001",
            )["payload"],
            "a" * 1_000 + cjk_character + filler,
        )
        self.assertEqual(exact_journal.events[-1][1][-1], InvocationOutcome.SUCCESS)

        next_journal = _RecordingUsageJournal()
        next_invocation = ModelInvocation(
            provider=_OutputTextProvider(next_output),
            usage_journal=next_journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )
        with self.assertRaises(InvalidModelResponseError):
            next_invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-raw-utf8-next",
                attempt_id="attempt-001",
            )
        self.assertEqual(
            next_journal.events[-1][1],
            (
                "call-output-raw-utf8-next",
                "attempt-001",
                InvocationOutcome.INVALID_JSON,
            ),
        )

    def test_canonical_output_exact_limit_succeeds_and_next_byte_rejects(self) -> None:
        cjk_characters = chr(0x4E2D) * 1_000
        filler = "a" * (48 * 1024 - 14 - 6 * len(cjk_characters))
        exact_output = '{"payload":"' + cjk_characters + filler + '"}'
        next_output = '{"payload":"' + cjk_characters + filler + "a" + '"}'

        exact_canonical = json.dumps(
            json.loads(exact_output),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        next_canonical = json.dumps(
            json.loads(next_output),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        self.assertLess(len(exact_output.encode("utf-8")), 48 * 1024)
        self.assertEqual(len(exact_canonical.encode("utf-8")), 48 * 1024)
        self.assertEqual(len(next_canonical.encode("utf-8")), 48 * 1024 + 1)

        exact_journal = _RecordingUsageJournal()
        exact_invocation = ModelInvocation(
            provider=_OutputTextProvider(exact_output),
            usage_journal=exact_journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )
        self.assertEqual(
            exact_invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-canonical-exact",
                attempt_id="attempt-001",
            )["payload"],
            cjk_characters + filler,
        )
        self.assertEqual(exact_journal.events[-1][1][-1], InvocationOutcome.SUCCESS)

        next_journal = _RecordingUsageJournal()
        next_invocation = ModelInvocation(
            provider=_OutputTextProvider(next_output),
            usage_journal=next_journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )
        with self.assertRaises(InvalidModelResponseError):
            next_invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-canonical-next",
                attempt_id="attempt-001",
            )
        self.assertEqual(
            next_journal.events[-1][1],
            (
                "call-output-canonical-next",
                "attempt-001",
                InvocationOutcome.INVALID_JSON,
            ),
        )

    def test_output_nesting_exact_limit_succeeds_and_next_level_rejects(self) -> None:
        exact_output = "[" * 31 + "0" + "]" * 31
        next_output = "[" * 32 + "0" + "]" * 32

        exact_journal = _RecordingUsageJournal()
        exact_invocation = ModelInvocation(
            provider=_OutputTextProvider(exact_output),
            usage_journal=exact_journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )
        self.assertEqual(
            exact_invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-depth-exact",
                attempt_id="attempt-001",
            ),
            json.loads(exact_output),
        )
        self.assertEqual(exact_journal.events[-1][1][-1], InvocationOutcome.SUCCESS)

        next_journal = _RecordingUsageJournal()
        next_invocation = ModelInvocation(
            provider=_OutputTextProvider(next_output),
            usage_journal=next_journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )
        with self.assertRaises(InvalidModelResponseError):
            next_invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-depth-next",
                attempt_id="attempt-001",
            )
        self.assertEqual(
            next_journal.events[-1][1],
            (
                "call-output-depth-next",
                "attempt-001",
                InvocationOutcome.INVALID_JSON,
            ),
        )

    def test_output_nodes_exact_limit_succeeds_and_next_node_rejects(self) -> None:
        exact_output = "[" + ",".join("0" for _ in range(4_095)) + "]"
        next_output = "[" + ",".join("0" for _ in range(4_096)) + "]"

        exact_journal = _RecordingUsageJournal()
        exact_invocation = ModelInvocation(
            provider=_OutputTextProvider(exact_output),
            usage_journal=exact_journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )
        self.assertEqual(
            exact_invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-nodes-exact",
                attempt_id="attempt-001",
            ),
            [0] * 4_095,
        )
        self.assertEqual(exact_journal.events[-1][1][-1], InvocationOutcome.SUCCESS)

        next_journal = _RecordingUsageJournal()
        next_invocation = ModelInvocation(
            provider=_OutputTextProvider(next_output),
            usage_journal=next_journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )
        with self.assertRaises(InvalidModelResponseError):
            next_invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-nodes-next",
                attempt_id="attempt-001",
            )
        self.assertEqual(
            next_journal.events[-1][1],
            (
                "call-output-nodes-next",
                "attempt-001",
                InvocationOutcome.INVALID_JSON,
            ),
        )

    def test_canonical_ascii_expansion_is_terminal_invalid_json(self) -> None:
        journal = _RecordingUsageJournal()
        output_text = '{"payload":"' + "\u4e2d" * 10_000 + '"}'
        self.assertLess(len(output_text.encode("utf-8")), 48 * 1024)
        canonical_text = json.dumps(
            json.loads(output_text),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        self.assertGreater(len(canonical_text), 48 * 1024)
        invocation = ModelInvocation(
            provider=_OutputTextProvider(output_text),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(InvalidModelResponseError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-canonical-bytes",
                attempt_id="attempt-001",
            )

        self.assertEqual(
            journal.events,
            [
                ("begin", journal.events[0][1]),
                (
                    "finish",
                    (
                        "call-output-canonical-bytes",
                        "attempt-001",
                        InvocationOutcome.INVALID_JSON,
                    ),
                ),
            ],
        )

    def test_nonfinite_json_numbers_are_terminal_invalid_json(self) -> None:
        for label, output_text in (("nan", "NaN"), ("overflow", "1e10000")):
            with self.subTest(label=label):
                journal = _RecordingUsageJournal()
                invocation = ModelInvocation(
                    provider=_OutputTextProvider(output_text),
                    usage_journal=journal,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-request-model",
                )

                with self.assertRaises(InvalidModelResponseError):
                    invocation.invoke_json(
                        {"prompt": "offline-only"},
                        call_id=f"call-output-{label}",
                        attempt_id="attempt-001",
                    )

                self.assertEqual(
                    journal.events,
                    [
                        ("begin", journal.events[0][1]),
                        (
                            "finish",
                            (
                                f"call-output-{label}",
                                "attempt-001",
                                InvocationOutcome.INVALID_JSON,
                            ),
                        ),
                    ],
                )

    def test_character_count_above_output_limit_is_terminal_invalid_json(
        self,
    ) -> None:
        journal = _RecordingUsageJournal()
        output_text = "0" * (48 * 1024 + 1)
        invocation = ModelInvocation(
            provider=_OutputTextProvider(output_text),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(InvalidModelResponseError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-character-count",
                attempt_id="attempt-001",
            )

        self.assertEqual(
            journal.events,
            [
                ("begin", journal.events[0][1]),
                (
                    "finish",
                    (
                        "call-output-character-count",
                        "attempt-001",
                        InvocationOutcome.INVALID_JSON,
                    ),
                ),
            ],
        )

    def test_oversized_utf8_output_is_terminal_invalid_json(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_OutputTextProvider('{"payload":"' + "中" * 20_000 + '"}'),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(InvalidModelResponseError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-bytes",
                attempt_id="attempt-001",
            )

        self.assertEqual(
            journal.events,
            [
                ("begin", journal.events[0][1]),
                (
                    "finish",
                    (
                        "call-output-bytes",
                        "attempt-001",
                        InvocationOutcome.INVALID_JSON,
                    ),
                ),
            ],
        )

    def test_excessive_json_integer_digits_are_terminal_invalid_json(
        self,
    ) -> None:
        if not (
            hasattr(sys, "get_int_max_str_digits")
            and hasattr(sys, "set_int_max_str_digits")
        ):
            self.skipTest("CPython integer digit-limit API is unavailable")
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_OutputTextProvider("9" * 5_000),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        original_limit = sys.get_int_max_str_digits()
        try:
            sys.set_int_max_str_digits(0)
            with self.assertRaises(InvalidModelResponseError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-output-integer-digits",
                    attempt_id="attempt-001",
                )
        finally:
            sys.set_int_max_str_digits(original_limit)

        self.assertEqual(
            journal.events,
            [
                ("begin", journal.events[0][1]),
                (
                    "finish",
                    (
                        "call-output-integer-digits",
                        "attempt-001",
                        InvocationOutcome.INVALID_JSON,
                    ),
                ),
            ],
        )

    def test_json_integer_bounds_allow_512_bits_and_reject_larger_values(
        self,
    ) -> None:
        for label, output_text, expected in (
            ("one", "1", 1),
            ("512-bit-power", str(1 << 511), 1 << 511),
            ("512-bit-maximum", str((1 << 512) - 1), (1 << 512) - 1),
        ):
            with self.subTest(label=label):
                journal = _RecordingUsageJournal()
                invocation = ModelInvocation(
                    provider=_OutputTextProvider(output_text),
                    usage_journal=journal,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-request-model",
                )

                self.assertEqual(
                    invocation.invoke_json(
                        {"prompt": "offline-only"},
                        call_id=f"call-output-integer-{label}",
                        attempt_id="attempt-001",
                    ),
                    expected,
                )
                self.assertEqual(journal.events[-1][1][-1], InvocationOutcome.SUCCESS)

        for label, output_text in (
            ("positive-513-bit", str(1 << 512)),
            ("negative-513-bit", str(-(1 << 512))),
        ):
            with self.subTest(label=label):
                journal = _RecordingUsageJournal()
                invocation = ModelInvocation(
                    provider=_OutputTextProvider(output_text),
                    usage_journal=journal,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-request-model",
                )

                with self.assertRaises(InvalidModelResponseError):
                    invocation.invoke_json(
                        {"prompt": "offline-only"},
                        call_id=f"call-output-integer-{label}",
                        attempt_id="attempt-001",
                    )
                self.assertEqual(
                    journal.events[-1][1][-1], InvocationOutcome.INVALID_JSON
                )

    def test_excessive_output_nesting_is_terminal_invalid_json(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_OutputTextProvider("[" * 2_000 + "0" + "]" * 2_000),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(InvalidModelResponseError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-depth",
                attempt_id="attempt-001",
            )

        self.assertEqual(
            journal.events,
            [
                ("begin", journal.events[0][1]),
                (
                    "finish",
                    (
                        "call-output-depth",
                        "attempt-001",
                        InvocationOutcome.INVALID_JSON,
                    ),
                ),
            ],
        )

    def test_wide_output_exceeding_node_limit_is_terminal_invalid_json(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_OutputTextProvider("[" + ",".join("0" for _ in range(4_097)) + "]"),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(InvalidModelResponseError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-output-nodes",
                attempt_id="attempt-001",
            )

        self.assertEqual(
            journal.events,
            [
                ("begin", journal.events[0][1]),
                (
                    "finish",
                    (
                        "call-output-nodes",
                        "attempt-001",
                        InvocationOutcome.INVALID_JSON,
                    ),
                ),
            ],
        )

    def test_invalid_json_keeps_reported_usage_before_failure_is_exposed(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_FakeProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(InvalidModelResponseError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-001",
                attempt_id="attempt-001",
            )

        self.assertEqual([event[0] for event in journal.events], ["begin", "finish"])
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.REPORTED)
        self.assertEqual(envelope.input_tokens, 17)
        self.assertEqual(envelope.output_tokens, 5)
        self.assertEqual(envelope.total_tokens, 22)
        self.assertEqual(envelope.request_model, "fake-request-model")
        self.assertEqual(envelope.response_model, "fake-response-model")
        self.assertEqual(
            journal.events[1][1],
            ("call-001", "attempt-001", InvocationOutcome.INVALID_JSON),
        )

    def test_empty_output_keeps_unknown_usage_without_inventing_zeroes(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_EmptyProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(InvalidModelResponseError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-002",
                attempt_id="attempt-001",
            )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertIsNone(envelope.input_tokens)
        self.assertIsNone(envelope.output_tokens)
        self.assertIsNone(envelope.total_tokens)
        self.assertEqual(
            journal.events[1][1],
            ("call-002", "attempt-001", InvocationOutcome.EMPTY_OUTPUT),
        )

    def test_null_output_is_accounted_as_empty_output(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_NullOutputProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(InvalidModelResponseError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-003",
                attempt_id="attempt-001",
            )

        self.assertEqual(
            journal.events[1][1],
            ("call-003", "attempt-001", InvocationOutcome.EMPTY_OUTPUT),
        )

    def test_timeout_records_unknown_usage_before_error_is_exposed(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_TimeoutProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(ModelInvocationTimeoutError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-004",
                attempt_id="attempt-001",
            )

        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.request_model, "fake-request-model")
        self.assertIsNone(envelope.response_model)
        self.assertIsNone(envelope.input_tokens)
        self.assertIsNone(envelope.output_tokens)
        self.assertIsNone(envelope.total_tokens)

    def test_provider_exception_records_unknown_usage_before_wrapping(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_ExceptionProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(ModelInvocationProviderError) as raised:
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-005",
                attempt_id="attempt-001",
            )

        self.assertIsInstance(raised.exception.__cause__, RuntimeError)
        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.request_model, "fake-request-model")
        self.assertIsNone(envelope.response_model)

    def test_malformed_provider_response_is_accounted_as_an_exception(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_MalformedResponseProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(ModelInvocationProviderError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-malformed-response",
                attempt_id="attempt-001",
            )

        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)

    def test_malformed_provider_fields_are_accounted_before_rejection(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_MalformedFieldsProvider(fallback=1),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(ModelInvocationProviderError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-malformed-fields",
                attempt_id="attempt-001",
            )

        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertFalse(envelope.fallback)

    def test_malformed_response_model_is_accounted_before_rejection(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_MalformedFieldsProvider(response_model=""),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(ModelInvocationProviderError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-malformed-response-model",
                attempt_id="attempt-001",
            )

        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)
        self.assertIsNone(envelope.response_model)

    def test_malformed_request_model_is_accounted_before_rejection(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_MalformedFieldsProvider(request_model=""),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(ModelInvocationProviderError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-malformed-request-model",
                attempt_id="attempt-001",
            )

        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.request_model, "fake-request-model")
        self.assertIsNone(envelope.response_model)

    def test_malformed_streamed_flag_is_accounted_before_rejection(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_MalformedFieldsProvider(streamed=1),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(ModelInvocationProviderError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-malformed-streamed",
                attempt_id="attempt-001",
            )

        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)
        self.assertFalse(envelope.streamed)

    def test_malformed_output_type_is_accounted_before_rejection(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_MalformedFieldsProvider(output_text=1),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        with self.assertRaises(ModelInvocationProviderError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-malformed-output",
                attempt_id="attempt-001",
            )

        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)

    def test_fallback_response_preserves_provider_attributed_model_identity(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_FallbackProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        result = invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-006",
            attempt_id="attempt-001",
        )

        self.assertEqual(result, {"status": "ok"})
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.request_model, "provider-misattributed-model")
        self.assertEqual(envelope.response_model, "fake-fallback-model")
        self.assertTrue(envelope.fallback)
        self.assertFalse(envelope.streamed)
        self.assertEqual(envelope.cache_read_tokens, 6)
        self.assertEqual(envelope.cache_write_tokens, 2)
        self.assertEqual(envelope.reasoning_tokens, 3)
        self.assertEqual(envelope.reported_cost, "0.00125")
        self.assertIsNone(envelope.currency)

    def test_estimated_provider_usage_preserves_its_status(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_EstimatedUsageProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-estimated-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-estimated",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.ESTIMATED)
        self.assertEqual(envelope.total_tokens, 16)
        self.assertEqual(envelope.currency, "USD")

    def test_unknown_status_hint_cannot_erase_known_usage(self) -> None:
        for index, status_hint in enumerate(
            (UsageStatus.UNKNOWN, "reported"),
            start=1,
        ):
            with self.subTest(status_hint=status_hint):
                journal = _RecordingUsageJournal()
                invocation = ModelInvocation(
                    provider=_MalformedFieldsProvider(
                        usage_status=status_hint,
                    ),
                    usage_journal=journal,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-request-model",
                )

                result = invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id=f"call-contradictory-status-{index}",
                    attempt_id="attempt-001",
                )

                self.assertEqual(result, {"status": "ok"})
                envelope = journal.events[0][1]
                self.assertIsInstance(envelope, UsageEnvelope)
                self.assertEqual(envelope.usage_status, UsageStatus.REPORTED)
                self.assertEqual(envelope.input_tokens, 1)

    def test_streamed_response_is_accounted_then_blocked(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_StreamingProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        with self.assertRaises(StreamingDisabledError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-007",
                attempt_id="attempt-001",
            )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertTrue(envelope.streamed)
        self.assertEqual(
            journal.events[1][1],
            ("call-007", "attempt-001", InvocationOutcome.STREAMING_DISABLED),
        )

    def test_malformed_token_usage_is_recorded_as_unknown(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_MalformedUsageProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        result = invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-008",
            attempt_id="attempt-001",
        )

        self.assertEqual(result, {"status": "ok"})
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertIsNone(envelope.input_tokens)
        self.assertIsNone(envelope.output_tokens)
        self.assertIsNone(envelope.total_tokens)
        self.assertEqual(len(envelope.raw_usage_sha256), 64)

    def test_reported_total_is_raised_to_known_token_components(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_MalformedFieldsProvider(
                raw_usage={
                    "input_tokens": 3,
                    "output_tokens": 2,
                    "total_tokens": 4,
                }
            ),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-request-model",
        )

        result = invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-inconsistent-total",
            attempt_id="attempt-001",
        )

        self.assertEqual(result, {"status": "ok"})
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.total_tokens, 5)

    def test_nonfinite_cost_cannot_bypass_unknown_usage_accounting(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_NonFiniteCostProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        result = invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-009",
            attempt_id="attempt-001",
        )

        self.assertEqual(result, {"status": "ok"})
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertIsNone(envelope.reported_cost)
        self.assertEqual(len(envelope.raw_usage_sha256), 64)

    def test_string_nonfinite_cost_is_not_reported(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_StringNonFiniteCostProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-010",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertIsNone(envelope.reported_cost)

    def test_cyclic_raw_usage_is_safely_hashed_and_accounted(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_CyclicUsageProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-011",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(len(envelope.raw_usage_sha256), 64)

    def test_nonmapping_raw_usage_is_hashed_without_bypassing_accounting(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_NonMappingUsageProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-012",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(len(envelope.raw_usage_sha256), 64)

    def test_mapping_lookup_failure_is_recorded_as_unknown_usage(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_RaisingGetUsageProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-013",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(len(envelope.raw_usage_sha256), 64)

    def test_oversized_bytes_hash_reads_only_a_bounded_prefix(self) -> None:
        hashes: list[str] = []
        for call_id, tail in (("call-014", b"a"), ("call-015", b"b")):
            journal = _RecordingUsageJournal()
            invocation = ModelInvocation(
                provider=_OversizedBytesUsageProvider(tail),
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            )
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id=call_id,
                attempt_id="attempt-001",
            )
            envelope = journal.events[0][1]
            self.assertIsInstance(envelope, UsageEnvelope)
            hashes.append(envelope.raw_usage_sha256)

        self.assertEqual(hashes[0], hashes[1])

    def test_oversized_cost_values_cannot_bypass_accounting(self) -> None:
        for index, value in enumerate(("1" * 5000, 10**5000), start=16):
            with self.subTest(value_type=type(value).__name__):
                journal = _RecordingUsageJournal()
                invocation = ModelInvocation(
                    provider=_OversizedCostProvider(value),
                    usage_journal=journal,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-primary-model",
                )
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id=f"call-{index:03d}",
                    attempt_id="attempt-001",
                )
                envelope = journal.events[0][1]
                self.assertIsInstance(envelope, UsageEnvelope)
                self.assertIsNone(envelope.reported_cost)

    def test_raising_reported_cost_text_cannot_bypass_accounting(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_OversizedCostProvider(_RaisingString("0.01")),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        result = invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-025",
            attempt_id="attempt-001",
        )

        self.assertEqual(result, {"status": "ok"})
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertIsNone(envelope.reported_cost)

    def test_raising_currency_text_cannot_bypass_accounting(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_RaisingCurrencyProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        result = invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-026",
            attempt_id="attempt-001",
        )

        self.assertEqual(result, {"status": "ok"})
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertIsNone(envelope.currency)

    def test_cost_outside_budget_decimal_boundary_is_unknown(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_OversizedCostProvider("1e999"),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-027",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertIsNone(envelope.reported_cost)

    def test_cost_at_budget_decimal_boundary_remains_reported(self) -> None:
        accepted_cost = ("1" * 128) + "e0"
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_OversizedCostProvider(accepted_cost),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-028",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.reported_cost, accepted_cost)

    def test_cost_text_and_exponent_boundaries_match_budget_validator(self) -> None:
        cases = (
            ("1e128", "1e128"),
            ("1e-128", "1e-128"),
            ((" " * 16) + ("1" * 128) + (" " * 16), "1" * 128),
        )
        rejected = ("1e129", "1e-129", (" " * 16) + ("1" * 128) + (" " * 17))

        for index, (raw_cost, expected_cost) in enumerate(cases, start=29):
            with self.subTest(raw_cost=raw_cost):
                journal = _RecordingUsageJournal()
                invocation = ModelInvocation(
                    provider=_OversizedCostProvider(raw_cost),
                    usage_journal=journal,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-primary-model",
                )
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id=f"call-{index:03d}",
                    attempt_id="attempt-001",
                )
                envelope = journal.events[0][1]
                self.assertIsInstance(envelope, UsageEnvelope)
                self.assertEqual(envelope.reported_cost, expected_cost)

        for index, raw_cost in enumerate(rejected, start=32):
            with self.subTest(raw_cost=raw_cost):
                journal = _RecordingUsageJournal()
                invocation = ModelInvocation(
                    provider=_OversizedCostProvider(raw_cost),
                    usage_journal=journal,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-primary-model",
                )
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id=f"call-{index:03d}",
                    attempt_id="attempt-001",
                )
                envelope = journal.events[0][1]
                self.assertIsInstance(envelope, UsageEnvelope)
                self.assertIsNone(envelope.reported_cost)

    def test_decimal_provider_cost_is_normalized_before_budget_validation(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_OversizedCostProvider(Decimal("0.00125")),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-035",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.reported_cost, "0.00125")

    def test_sequence_normalization_failure_still_records_unknown_usage(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_RaisingSequenceUsageProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-018",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(len(envelope.raw_usage_sha256), 64)

    def test_decimal_usage_hash_does_not_materialize_the_coefficient(self) -> None:
        hashes: list[str] = []
        for call_id, tail in (("call-019", "2"), ("call-020", "3")):
            journal = _RecordingUsageJournal()
            invocation = ModelInvocation(
                provider=_OversizedDecimalUsageProvider(tail),
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            )
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id=call_id,
                attempt_id="attempt-001",
            )
            envelope = journal.events[0][1]
            self.assertIsInstance(envelope, UsageEnvelope)
            hashes.append(envelope.raw_usage_sha256)

        self.assertEqual(hashes[0], hashes[1])

    def test_malformed_unicode_usage_is_still_accounted(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_MalformedUnicodeUsageProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-021",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(len(envelope.raw_usage_sha256), 64)

    def test_normalization_exception_uses_a_nonthrowing_hash_marker(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_RaisingStringUsageProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-022",
            attempt_id="attempt-001",
        )

        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(len(envelope.raw_usage_sha256), 64)

    def test_retrying_invocation_accepts_positive_wall_time_budget(self) -> None:
        journal = _RecordingUsageJournal()
        attempt = ModelInvocation(
            provider=_FakeProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        try:
            invocation = RetryingModelInvocation(
                attempt=attempt,
                max_attempts=1,
                max_wall_time_ms=100,
            )
        except TypeError as error:
            self.fail(f"positive max_wall_time_ms was rejected: {error}")

        self.assertIsInstance(invocation, RetryingModelInvocation)

    def test_retrying_invocation_rejects_non_positive_integer_wall_time(self) -> None:
        journal = _RecordingUsageJournal()
        attempt = ModelInvocation(
            provider=_FakeProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        for invalid in (0, -1, True, False, 1.5, "100"):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(
                    ValueError,
                    "max_wall_time_ms must be a positive integer",
                ):
                    RetryingModelInvocation(
                        attempt=attempt,
                        max_attempts=1,
                        max_wall_time_ms=invalid,  # type: ignore[arg-type]
                    )

    def test_spawned_retrying_invocation_requires_wall_time_budget(self) -> None:
        journal = _RecordingUsageJournal()

        with self.assertRaisesRegex(
            ValueError,
            "spawned provider retries require max_wall_time_ms",
        ):
            RetryingModelInvocation(
                attempt=_spawned_invocation(_ChildPidProvider(), journal),
                max_attempts=1,
            )

    def test_retries_reuse_one_absolute_monotonic_deadline(self) -> None:
        journal = _RecordingUsageJournal()
        deadlines: list[float | None] = []
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=_FakeProvider(),
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=2,
            max_wall_time_ms=100,
        )

        def invoke_attempt(
            _attempt: ModelInvocation,
            request: object,
            *,
            call_id: str,
            attempt_id: str,
            deadline: float | None = None,
        ) -> object:
            del request, call_id, attempt_id
            deadlines.append(deadline)
            if len(deadlines) == 1:
                raise ModelInvocationProviderError("retryable provider error")
            return {"status": "ok"}

        started = time.monotonic()
        with patch.object(
            ModelInvocation,
            "invoke_json",
            autospec=True,
            side_effect=invoke_attempt,
        ):
            result = invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-shared-deadline",
            )

        self.assertEqual(result, {"status": "ok"})
        self.assertEqual(len(deadlines), 2)
        self.assertIsNotNone(deadlines[0])
        self.assertEqual(deadlines[0], deadlines[1])
        self.assertGreaterEqual(deadlines[0], started + 0.09)  # type: ignore[operator]
        self.assertLess(deadlines[0], started + 0.5)  # type: ignore[operator]

    def test_hard_deadline_is_terminal_without_phantom_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            baseline_pids = _active_child_pids()
            marker = Path(temp_dir) / "logical-deadline-child.pid"
            journal = _RecordingUsageJournal()
            invocation = RetryingModelInvocation(
                attempt=_spawned_invocation(
                    _HangingProvider(str(marker)),
                    journal,
                ),
                max_attempts=3,
                max_wall_time_ms=1_000,
            )
            started = time.monotonic()

            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "hang"},
                    call_id="call-logical-deadline",
                )

            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 2.5)
            child_pid = int(marker.read_text(encoding="ascii"))
            self.assertFalse(
                any(child.pid == child_pid for child in multiprocessing.active_children())
            )
            self.assertEqual(_active_child_pids() - baseline_pids, set())
            begin_events = [event for event in journal.events if event[0] == "begin"]
            self.assertEqual(len(begin_events), 1)
            envelope = begin_events[0][1]
            self.assertIsInstance(envelope, UsageEnvelope)
            self.assertEqual(envelope.attempt_id, "call-logical-deadline-attempt-001")
            self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_expired_budget_after_fast_timeout_prevents_next_attempt(self) -> None:
        provider = _SlowTimeoutProvider(delay_seconds=0.05)
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=3,
            max_wall_time_ms=20,
        )

        with self.assertRaises(ModelInvocationTimeoutError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-expired-before-retry",
            )

        self.assertEqual(provider.call_count, 1)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        self.assertEqual(begin_events[0][1].outcome, InvocationOutcome.TIMEOUT)

    def test_spawned_protocol_failure_is_accounted_once_without_retry(self) -> None:
        baseline_pids = _active_child_pids()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_MalformedResponseProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=5_000,
        )

        with self.assertRaises(ModelInvocationProviderError):
            invocation.invoke_json(
                {"prompt": "malformed-response"},
                call_id="call-terminal-protocol",
            )

        self.assertEqual(_active_child_pids() - baseline_pids, set())
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        envelope = begin_events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_fast_pipe_failure_is_terminal_and_accounted_once(self) -> None:
        context = _PipeFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )

        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ):
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-fast-pipe-failure",
                )

        self.assertEqual(context.pipe_calls, 1)
        self.assertEqual(context.process_calls, 0)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        envelope = begin_events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_pipe_failure_after_deadline_is_terminal_timeout(self) -> None:
        clock = _ControlledClock(now=10.0)
        context = _PipeFailureContext(clock=clock, consume_seconds=1.0)
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )

        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module.time, "monotonic", side_effect=clock.monotonic):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-deadline-during-pipe-construction",
                )

        self.assertEqual(context.pipe_calls, 1)
        self.assertEqual(context.process_calls, 0)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        envelope = begin_events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_process_failure_after_deadline_closes_pipes_and_times_out(self) -> None:
        clock = _ControlledClock(now=20.0)
        context = _ProcessConstructionFailureContext(
            clock=clock,
            consume_seconds=1.0,
        )
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )

        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module.time, "monotonic", side_effect=clock.monotonic):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-deadline-during-process-construction",
                )

        self.assertEqual(context.pipe_calls, 1)
        self.assertEqual(context.process_calls, 1)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        envelope = begin_events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_fast_process_construction_failure_is_terminal_protocol_error(self) -> None:
        context = _ProcessConstructionFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )

        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ):
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-fast-process-construction-failure",
                )

        self.assertEqual(context.pipe_calls, 1)
        self.assertEqual(context.process_calls, 1)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        envelope = begin_events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_unexpected_executor_exception_is_terminal_and_accounted_once(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=_FakeProvider(),
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )

        with patch.object(
            campaign_module._InlineProviderExecutor,
            "execute",
            side_effect=RuntimeError("undeclared executor failure"),
        ) as execute:
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-terminal-unexpected-executor-failure",
                )

        self.assertEqual(execute.call_count, 1)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        envelope = begin_events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_executor_configuration_failure_is_not_retried_or_accounted(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=_FakeProvider(),
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )

        with patch.object(
            campaign_module._InlineProviderExecutor,
            "execute",
            side_effect=campaign_module._ProviderExecutorConfigurationError(
                "deterministic executor configuration failure"
            ),
        ) as execute:
            with self.assertRaises(ValueError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-terminal-configuration",
                )

        self.assertEqual(execute.call_count, 1)
        self.assertEqual(journal.events, [])

    def test_preexpired_spawn_deadline_does_not_start_child(self) -> None:
        context = _NoProcessContext()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)

        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-preexpired-spawn",
                    attempt_id="attempt-001",
                    deadline=time.monotonic() - 1.0,
                )

        self.assertEqual(context.process_calls, 0)
        self.assertEqual(len(journal.events), 1)
        self.assertEqual(journal.events[0][1].outcome, InvocationOutcome.TIMEOUT)

    def test_deadline_expiring_during_spawn_construction_does_not_start_child(
        self,
    ) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=deadline - 1.0)
        context = _DeadlineFenceContext(clock, deadline)
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)

        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module.time, "monotonic", side_effect=clock.monotonic):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-deadline-during-spawn-construction",
                    attempt_id="attempt-001",
                    deadline=deadline,
                )

        self.assertEqual(context.process_calls, 1)
        self.assertEqual(context.process.start_calls, 0)
        self.assertEqual(context.process.terminate_calls, 0)
        self.assertEqual(context.process.kill_calls, 0)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.call_id, "call-deadline-during-spawn-construction")
        self.assertEqual(envelope.attempt_id, "attempt-001")
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_spawned_provider_timeout_retries_with_remaining_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            baseline_pids = _active_child_pids()
            marker = Path(temp_dir) / "first-provider-timeout.pid"
            journal = _RecordingUsageJournal()
            invocation = RetryingModelInvocation(
                attempt=_spawned_invocation(
                    _SpawnTimeoutThenSuccessProvider(str(marker)),
                    journal,
                ),
                max_attempts=2,
                max_wall_time_ms=5_000,
            )

            result = invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-spawn-timeout-retry",
            )

            self.assertEqual(result, {"status": "ok"})
            self.assertTrue(marker.exists())
            self.assertEqual(_active_child_pids() - baseline_pids, set())
            begin_events = [event for event in journal.events if event[0] == "begin"]
            self.assertEqual(len(begin_events), 2)
            self.assertEqual(begin_events[0][1].outcome, InvocationOutcome.TIMEOUT)
            self.assertEqual(begin_events[1][1].outcome, InvocationOutcome.RESPONSE_RECEIVED)

    def test_provider_exception_retries_with_remaining_budget(self) -> None:
        provider = _ExceptionThenSuccessProvider()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=2,
            max_wall_time_ms=1_000,
        )

        result = invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-provider-exception-retry",
        )

        self.assertEqual(result, {"status": "ok"})
        self.assertEqual(provider.call_count, 2)

    def test_invalid_json_retries_with_remaining_budget(self) -> None:
        provider = _InvalidJsonThenSuccessProvider()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=2,
            max_wall_time_ms=1_000,
        )

        result = invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-invalid-json-retry",
        )

        self.assertEqual(result, {"status": "ok"})
        self.assertEqual(provider.call_count, 2)

    def test_tenacity_owns_two_accounted_logical_attempts(self) -> None:
        provider = _TimeoutThenSuccessProvider()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=2,
            max_wall_time_ms=1_000,
        )

        result = invocation.invoke_json(
            {"prompt": "offline-only"},
            call_id="call-023",
        )

        self.assertEqual(result, {"status": "ok"})
        self.assertEqual(provider.call_count, 2)
        self.assertEqual(
            [
                event[1].attempt_id
                for event in journal.events
                if event[0] == "begin"
            ],
            ["call-023-attempt-001", "call-023-attempt-002"],
        )
        self.assertEqual(
            journal.events[0][1].outcome,
            InvocationOutcome.TIMEOUT,
        )

    def test_retry_receipt_identifies_the_successful_attempt(self) -> None:
        provider = _TimeoutThenSuccessProvider()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=2,
        )

        receipt = invocation.invoke_json_with_receipt(
            {"prompt": "offline-only"},
            call_id="call-025",
        )

        self.assertEqual(receipt.output, {"status": "ok"})
        self.assertEqual(receipt.attempt_id, "call-025-attempt-002")
        self.assertEqual(receipt.attempt_count, 2)

    def test_nonretryable_streaming_failure_is_not_double_invoked(self) -> None:
        provider = _StreamingThenSuccessProvider()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )

        with self.assertRaises(StreamingDisabledError):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-024",
            )

        self.assertEqual(provider.call_count, 1)


if __name__ == "__main__":
    unittest.main()
