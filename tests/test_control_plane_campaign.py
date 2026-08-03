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


class _ProcessConstructionInterruptContext:
    def __init__(self, interrupt: BaseException) -> None:
        self._interrupt = interrupt
        self.process_calls = 0
        self.receive = _ClosablePipeEnd()
        self.send = _ClosablePipeEnd()

    def Pipe(self, *, duplex: bool) -> tuple[object, object]:
        if duplex:
            raise AssertionError("spawn boundary pipe must be one-way")
        return self.receive, self.send

    def Process(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        self.process_calls += 1
        raise self._interrupt


class _PipeDeadlineFenceContext:
    def __init__(self, *, clock: "_ControlledClock", deadline: float) -> None:
        self._clock = clock
        self._deadline = deadline
        self.pipe_calls = 0
        self.process_calls = 0
        self.receive = _ClosablePipeEnd()
        self.send = _ClosablePipeEnd()

    def Pipe(self, *, duplex: bool) -> tuple[object, object]:
        if duplex:
            raise AssertionError("spawn boundary pipe must be one-way")
        self.pipe_calls += 1
        self._clock.now = self._deadline
        return self.receive, self.send

    def Process(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        self.process_calls += 1
        raise AssertionError("Process must not run after Pipe crosses deadline")


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


class _LifecycleFenceConnection:
    def __init__(
        self,
        *,
        actions: list[object],
        name: str,
        clock: _ControlledClock,
        deadline: float,
        cross_deadline_at: str | None,
        frame: bytes | None = None,
        fail_first_close: bool = False,
    ) -> None:
        self._actions = actions
        self._name = name
        self._clock = clock
        self._deadline = deadline
        self._cross_deadline_at = cross_deadline_at
        self._frame = frame
        self._fail_first_close = fail_first_close
        self.process: _LifecycleFenceProcess | None = None
        self.close_calls = 0
        self.closed = False

    def _cross_deadline(self, phase: str) -> None:
        if self._cross_deadline_at == phase:
            self._clock.now = self._deadline

    def poll(self, timeout: float) -> bool:
        self._actions.append(("receive.poll", timeout))
        self._cross_deadline("poll")
        return True

    def recv_bytes(self, *, maxlength: int) -> bytes:
        self._actions.append(("receive.recv_bytes", maxlength))
        self._cross_deadline("recv")
        if self._frame is None:
            raise AssertionError("receive frame was not configured")
        if self.process is not None:
            self.process._alive = False
        return self._frame

    def close(self) -> None:
        self.close_calls += 1
        self._actions.append(f"{self._name}.close")
        if self._fail_first_close and self.close_calls == 1:
            raise OSError("synthetic parent send close failure")
        self._cross_deadline(f"{self._name}_close")
        self.closed = True


class _LifecycleFenceProcess:
    pid = None

    def __init__(
        self,
        *,
        actions: list[object],
        clock: _ControlledClock,
        deadline: float,
        cross_deadline_at: str | None,
    ) -> None:
        self._actions = actions
        self._clock = clock
        self._deadline = deadline
        self._cross_deadline_at = cross_deadline_at
        self._alive = False
        self.closed = False
        self.terminate_calls = 0
        self.kill_calls = 0

    def _cross_deadline(self, phase: str) -> None:
        if self._cross_deadline_at == phase:
            self._clock.now = self._deadline

    def start(self) -> None:
        self._actions.append("process.start")
        self.pid = 12345
        self._alive = True
        self._cross_deadline("start")

    def terminate(self) -> None:
        self._actions.append("process.terminate")
        self.terminate_calls += 1
        self._alive = False

    def join(self, timeout: float | None = None) -> None:
        self._actions.append(("process.join", timeout))
        self._cross_deadline("join")

    def is_alive(self) -> bool:
        self._actions.append("process.is_alive")
        self._cross_deadline("is_alive")
        return self._alive

    def kill(self) -> None:
        self._actions.append("process.kill")
        self.kill_calls += 1
        self._alive = False

    def close(self) -> None:
        self._actions.append("process.close")
        self.closed = True


class _LifecycleFenceContext:
    def __init__(
        self,
        *,
        clock: _ControlledClock,
        deadline: float,
        cross_deadline_at: str | None = None,
        fail_first_send_close: bool = False,
    ) -> None:
        self.actions: list[object] = []
        snapshot = campaign_module._snapshot_provider_response(
            _ChildPidProvider().invoke({"prompt": "late-response"}),
            max_output_bytes=campaign_module._MAX_MODEL_OUTPUT_BYTES,
        )
        self.receive = _LifecycleFenceConnection(
            actions=self.actions,
            name="receive",
            clock=clock,
            deadline=deadline,
            cross_deadline_at=cross_deadline_at,
            frame=campaign_module._bounded_response_frame(snapshot),
        )
        self.send = _LifecycleFenceConnection(
            actions=self.actions,
            name="send",
            clock=clock,
            deadline=deadline,
            cross_deadline_at=cross_deadline_at,
            fail_first_close=fail_first_send_close,
        )
        self.process = _LifecycleFenceProcess(
            actions=self.actions,
            clock=clock,
            deadline=deadline,
            cross_deadline_at=cross_deadline_at,
        )
        self.receive.process = self.process

    def Pipe(
        self, *, duplex: bool
    ) -> tuple[_LifecycleFenceConnection, _LifecycleFenceConnection]:
        if duplex:
            raise AssertionError("spawn boundary pipe must be one-way")
        return self.receive, self.send

    def Process(self, *args: object, **kwargs: object) -> _LifecycleFenceProcess:
        del args, kwargs
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


class _InMemoryTimeoutThenSuccessSpawnProvider:
    def __init__(self, observed_counts_path: str) -> None:
        self.call_count = 0
        self.observed_counts_path = observed_counts_path

    def invoke(self, request: object) -> ProviderResponse:
        del request
        self.call_count += 1
        with Path(self.observed_counts_path).open("a", encoding="ascii") as output:
            output.write(f"{self.call_count}\n")
        if self.call_count == 1:
            raise TimeoutError("fresh provider snapshot timed out")
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


class _ClockAdvancingTimeoutProvider:
    def __init__(self, clock: _ControlledClock, *, advance_seconds: float) -> None:
        self._clock = clock
        self._advance_seconds = advance_seconds
        self.call_count = 0

    def invoke(self, request: object) -> ProviderResponse:
        del request
        self.call_count += 1
        self._clock.now += self._advance_seconds
        raise TimeoutError("provider timed out after consuming the budget")


class _ClockAdvancingSuccessProvider:
    def __init__(self, clock: _ControlledClock, *, advance_seconds: float) -> None:
        self._clock = clock
        self._advance_seconds = advance_seconds
        self.call_count = 0

    def invoke(self, request: object) -> ProviderResponse:
        del request
        self.call_count += 1
        self._clock.now += self._advance_seconds
        return ProviderResponse(
            output_text='{"status":"late"}',
            request_model="fake-primary-model",
            response_model="fake-primary-model",
            raw_usage={"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
        )


class _ClockAdvancingExceptionProvider:
    def __init__(self, clock: _ControlledClock, *, advance_seconds: float) -> None:
        self._clock = clock
        self._advance_seconds = advance_seconds
        self.call_count = 0

    def invoke(self, request: object) -> ProviderResponse:
        del request
        self.call_count += 1
        self._clock.now += self._advance_seconds
        raise RuntimeError("provider failed after consuming the budget")


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
    def __init__(self, *, clock: _ControlledClock | None = None) -> None:
        self.events: list[tuple[str, object]] = []
        self.finish_calls: list[
            tuple[str, str, InvocationOutcome, float | None, InvocationOutcome]
        ] = []
        self._clock = clock

    def begin(self, envelope: UsageEnvelope) -> None:
        self.events.append(("begin", envelope))

    def finish(
        self,
        *,
        call_id: str,
        attempt_id: str,
        outcome: InvocationOutcome,
        deadline: float | None = None,
    ) -> InvocationOutcome:
        actual = (
            InvocationOutcome.TIMEOUT
            if deadline is not None
            and self._clock is not None
            and self._clock.monotonic() >= deadline
            else outcome
        )
        self.finish_calls.append(
            (call_id, attempt_id, outcome, deadline, actual)
        )
        self.events.append(("finish", (call_id, attempt_id, actual)))
        return actual


class _DeadlineAdvancingUsageJournal(_RecordingUsageJournal):
    def __init__(self, clock: _ControlledClock, *, deadline: float) -> None:
        super().__init__(clock=clock)
        self._clock = clock
        self._deadline = deadline

    def begin(self, envelope: UsageEnvelope) -> None:
        super().begin(envelope)
        self._clock.now = self._deadline


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

    def test_cleanup_escalation_continues_after_terminate_failure(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _PollFailureContext()
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)

        def fail_terminate() -> None:
            context.actions.append("process.terminate")
            raise OSError("synthetic terminate failure")

        context.process.terminate = fail_terminate  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-terminate-failure",
                    attempt_id="attempt-001",
                    deadline=deadline,
                )

        poll_timeout = context.actions[2][1]
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

    def test_cleanup_terminate_interrupt_is_deferred_until_everything_is_closed(
        self,
    ) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _PollFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )
        interrupt = KeyboardInterrupt("synthetic cleanup terminate interruption")

        def interrupt_terminate() -> None:
            context.actions.append("process.terminate")
            raise interrupt

        context.process.terminate = interrupt_terminate  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(KeyboardInterrupt) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-terminate-interrupt",
                )

        self.assertIs(caught.exception, interrupt)
        self.assertEqual(context.actions.count("process.start"), 1)
        self.assertIn("process.kill", context.actions)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_cleanup_initial_join_interrupt_is_deferred_until_worker_is_reaped(
        self,
    ) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _PollFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )
        interrupt = SystemExit(17)
        join_calls = 0

        def interrupt_initial_join(timeout: float | None = None) -> None:
            nonlocal join_calls
            context.actions.append(("process.join", timeout))
            join_calls += 1
            if join_calls == 1:
                raise interrupt

        context.process.join = interrupt_initial_join  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(SystemExit) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-initial-join-interrupt",
                )

        self.assertIs(caught.exception, interrupt)
        self.assertEqual(join_calls, 2)
        self.assertIn("process.kill", context.actions)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_cleanup_initial_liveness_interrupt_still_forces_kill(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _PollFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )
        interrupt = KeyboardInterrupt("synthetic initial liveness interruption")
        is_alive_calls = 0

        def interrupt_initial_is_alive() -> bool:
            nonlocal is_alive_calls
            context.actions.append("process.is_alive")
            is_alive_calls += 1
            if is_alive_calls == 1:
                raise interrupt
            return context.process._alive

        context.process.is_alive = (  # type: ignore[method-assign]
            interrupt_initial_is_alive
        )
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(KeyboardInterrupt) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-initial-liveness-interrupt",
                )

        self.assertIs(caught.exception, interrupt)
        self.assertEqual(is_alive_calls, 2)
        self.assertIn("process.kill", context.actions)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_cleanup_kill_interrupt_retries_kill_before_propagating(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _PollFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )
        interrupt = SystemExit(23)
        kill_calls = 0

        def interrupt_initial_kill() -> None:
            nonlocal kill_calls
            context.actions.append("process.kill")
            kill_calls += 1
            if kill_calls == 1:
                raise interrupt
            context.process._alive = False

        context.process.kill = interrupt_initial_kill  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(SystemExit) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-kill-interrupt",
                )

        self.assertIs(caught.exception, interrupt)
        self.assertEqual(kill_calls, 2)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_cleanup_post_kill_join_interrupt_still_checks_reaped_state(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _PollFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )
        interrupt = KeyboardInterrupt("synthetic post-kill join interruption")
        join_calls = 0

        def interrupt_post_kill_join(timeout: float | None = None) -> None:
            nonlocal join_calls
            context.actions.append(("process.join", timeout))
            join_calls += 1
            if join_calls == 2:
                raise interrupt

        context.process.join = (  # type: ignore[method-assign]
            interrupt_post_kill_join
        )
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(KeyboardInterrupt) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-post-kill-join-interrupt",
                )

        self.assertIs(caught.exception, interrupt)
        self.assertEqual(join_calls, 2)
        self.assertEqual(context.actions.count("process.is_alive"), 2)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_cleanup_final_liveness_interrupt_conservatively_retries_kill(
        self,
    ) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _PollFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )
        interrupt = SystemExit(29)
        kill_calls = 0
        is_alive_calls = 0

        def delayed_kill() -> None:
            nonlocal kill_calls
            context.actions.append("process.kill")
            kill_calls += 1
            if kill_calls == 2:
                context.process._alive = False

        def interrupt_final_is_alive() -> bool:
            nonlocal is_alive_calls
            context.actions.append("process.is_alive")
            is_alive_calls += 1
            if is_alive_calls == 2:
                raise interrupt
            return context.process._alive

        context.process.kill = delayed_kill  # type: ignore[method-assign]
        context.process.is_alive = (  # type: ignore[method-assign]
            interrupt_final_is_alive
        )
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(SystemExit) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-final-liveness-interrupt",
                )

        self.assertIs(caught.exception, interrupt)
        self.assertEqual(kill_calls, 2)
        self.assertEqual(is_alive_calls, 3)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_cleanup_worker_close_interrupt_does_not_skip_pipe_cleanup(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _PollFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )
        interrupt = KeyboardInterrupt("synthetic worker close interruption")
        process_close_calls = 0

        def interrupt_process_close() -> None:
            nonlocal process_close_calls
            context.actions.append("process.close")
            process_close_calls += 1
            if process_close_calls == 1:
                clock.now = deadline
                raise interrupt
            context.process.closed = True

        context.process.close = interrupt_process_close  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(KeyboardInterrupt) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-worker-close-interrupt",
                )

        self.assertIs(caught.exception, interrupt)
        self.assertEqual(process_close_calls, 2)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_cleanup_receive_close_interrupt_does_not_skip_send_retry(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _LifecycleFenceContext(
            clock=clock,
            deadline=deadline,
            fail_first_send_close=True,
        )
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )
        interrupt = SystemExit(31)
        receive_close_calls = 0

        def interrupt_receive_close() -> None:
            nonlocal receive_close_calls
            context.actions.append("receive.close")
            receive_close_calls += 1
            if receive_close_calls == 1:
                raise interrupt
            context.receive.closed = True

        context.receive.close = interrupt_receive_close  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(SystemExit) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-receive-close-interrupt",
                )

        self.assertIs(caught.exception, interrupt)
        self.assertEqual(receive_close_calls, 2)
        self.assertEqual(context.send.close_calls, 2)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_primary_interrupt_wins_over_send_cleanup_interrupt(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _LifecycleFenceContext(clock=clock, deadline=deadline)
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)
        primary_interrupt = KeyboardInterrupt("synthetic start interruption")
        cleanup_interrupt = SystemExit(37)
        send_close_calls = 0

        def interrupt_start_after_pid() -> None:
            context.actions.append("process.start")
            context.process.pid = 12345
            context.process._alive = True
            raise primary_interrupt

        def interrupt_send_close() -> None:
            nonlocal send_close_calls
            context.actions.append("send.close")
            send_close_calls += 1
            if send_close_calls == 1:
                raise cleanup_interrupt
            context.send.closed = True

        context.process.start = interrupt_start_after_pid  # type: ignore[method-assign]
        context.send.close = interrupt_send_close  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(KeyboardInterrupt) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-primary-wins-over-send-cleanup-interrupt",
                    attempt_id="attempt-001",
                    deadline=deadline,
                )

        self.assertIs(caught.exception, primary_interrupt)
        self.assertEqual(send_close_calls, 2)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_cleanup_pid_probe_interrupt_conservatively_reaps_worker(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _LifecycleFenceContext(clock=clock, deadline=deadline)
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)
        primary_interrupt = KeyboardInterrupt("synthetic partial start interruption")
        cleanup_interrupt = SystemExit(41)

        class PidProbeInterruptProcess(_LifecycleFenceProcess):
            @property
            def pid(self) -> int:
                raise cleanup_interrupt

            def start(self) -> None:
                self._actions.append("process.start")
                self._alive = True
                raise primary_interrupt

        context.process = PidProbeInterruptProcess(
            actions=context.actions,
            clock=clock,
            deadline=deadline,
            cross_deadline_at=None,
        )
        context.receive.process = context.process
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(KeyboardInterrupt) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-pid-probe-interrupt",
                    attempt_id="attempt-001",
                    deadline=deadline,
                )

        self.assertIs(caught.exception, primary_interrupt)
        self.assertGreaterEqual(context.process.terminate_calls, 1)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_pipe_deadline_cleanup_interrupt_wins_after_both_ends_are_tried(
        self,
    ) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _PipeDeadlineFenceContext(clock=clock, deadline=deadline)
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )
        interrupt = KeyboardInterrupt("synthetic pipe deadline cleanup interruption")
        receive_close_calls = 0

        def interrupt_receive_close() -> None:
            nonlocal receive_close_calls
            receive_close_calls += 1
            if receive_close_calls == 1:
                raise interrupt
            context.receive.closed = True

        context.receive.close = interrupt_receive_close  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(KeyboardInterrupt) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-pipe-deadline-cleanup-interrupt",
                )

        self.assertIs(caught.exception, interrupt)
        self.assertEqual(receive_close_calls, 2)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(context.process_calls, 0)
        self.assertEqual(journal.events, [])

    def test_process_construction_cleanup_interrupt_wins_after_both_pipe_closes(
        self,
    ) -> None:
        context = _ProcessConstructionFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )
        interrupt = SystemExit(43)
        receive_close_calls = 0

        def interrupt_receive_close() -> None:
            nonlocal receive_close_calls
            receive_close_calls += 1
            if receive_close_calls == 1:
                raise interrupt
            context.receive.closed = True

        context.receive.close = interrupt_receive_close  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ):
            with self.assertRaises(SystemExit) as caught:
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-process-construction-cleanup-interrupt",
                )

        self.assertIs(caught.exception, interrupt)
        self.assertEqual(context.process_calls, 1)
        self.assertEqual(receive_close_calls, 2)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

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

    def test_retrying_invocation_rejects_wall_time_that_overflows_deadline(
        self,
    ) -> None:
        journal = _RecordingUsageJournal()
        attempt = ModelInvocation(
            provider=_FakeProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-primary-model",
        )

        with self.assertRaisesRegex(
            ValueError,
            "max_wall_time_ms exceeds the finite deadline range",
        ):
            RetryingModelInvocation(
                attempt=attempt,
                max_attempts=1,
                max_wall_time_ms=10**400,
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

    def test_spawned_retries_use_fresh_provider_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            observed_counts = Path(temp_dir) / "observed-counts.txt"
            journal = _RecordingUsageJournal()
            invocation = RetryingModelInvocation(
                attempt=_spawned_invocation(
                    _InMemoryTimeoutThenSuccessSpawnProvider(
                        str(observed_counts)
                    ),
                    journal,
                ),
                max_attempts=2,
                max_wall_time_ms=5_000,
            )

            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-fresh-provider-snapshots",
                )

            self.assertEqual(
                observed_counts.read_text(encoding="ascii").splitlines(),
                ["1", "1"],
            )
            begin_events = [
                event for event in journal.events if event[0] == "begin"
            ]
            self.assertEqual(len(begin_events), 2)
            self.assertTrue(
                all(
                    event[1].outcome is InvocationOutcome.TIMEOUT
                    for event in begin_events
                )
            )

    def test_retrying_invocation_honors_public_attempt_override(self) -> None:
        clock = _ControlledClock(now=90.0)
        journal = _RecordingUsageJournal()

        class CountingProvider:
            def __init__(self) -> None:
                self.call_count = 0

            def invoke(self, request: object) -> ProviderResponse:
                del request
                self.call_count += 1
                return ProviderResponse(
                    output_text='{"source":"base"}',
                    request_model="fake-primary-model",
                    response_model="fake-primary-model",
                    raw_usage={},
                )

        class PublicOverrideInvocation(ModelInvocation):
            def __init__(self, provider: CountingProvider) -> None:
                super().__init__(
                    provider=provider,
                    usage_journal=journal,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-primary-model",
                )
                self.override_calls = 0
                self.deadlines: list[float | None] = []

            def invoke_json(
                self,
                request: object,
                *,
                call_id: str,
                attempt_id: str,
                deadline: float | None = None,
            ) -> object:
                self.override_calls += 1
                self.deadlines.append(deadline)
                return super().invoke_json(
                    request,
                    call_id=call_id,
                    attempt_id=attempt_id,
                    deadline=deadline,
                )

        provider = CountingProvider()
        attempt = PublicOverrideInvocation(provider)
        invocation = RetryingModelInvocation(
            attempt=attempt,
            max_attempts=1,
            max_wall_time_ms=100,
        )

        with patch.object(campaign_module, "time", clock):
            result = invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-public-override",
            )

        self.assertEqual(attempt.override_calls, 1)
        self.assertEqual(provider.call_count, 1)
        self.assertEqual(attempt.deadlines, [90.1])
        self.assertEqual(result, {"source": "base"})
        self.assertEqual(
            [event[0] for event in journal.events],
            ["begin", "finish"],
        )
        self.assertEqual(
            journal.events[0][1].outcome,
            InvocationOutcome.RESPONSE_RECEIVED,
        )
        self.assertEqual(
            journal.events[1][1][2],
            InvocationOutcome.SUCCESS,
        )
        self.assertEqual(journal.finish_calls[0][3], 90.1)

    def test_retries_reuse_one_absolute_monotonic_deadline(self) -> None:
        clock = _ControlledClock(now=100.0)
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

        with patch.object(campaign_module, "time", clock), patch.object(
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
        self.assertEqual(deadlines[0], 100.1)

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
        clock = _ControlledClock(now=200.0)
        provider = _ClockAdvancingTimeoutProvider(
            clock,
            advance_seconds=0.02,
        )
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

        with patch.object(campaign_module, "time", clock):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-expired-before-retry",
                )

        self.assertEqual(provider.call_count, 1)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        self.assertEqual(begin_events[0][1].outcome, InvocationOutcome.TIMEOUT)

    def test_deadline_expired_between_attempts_records_terminal_attempt(
        self,
    ) -> None:
        clock = _ControlledClock(now=225.0)
        deadline = 225.02
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
            max_attempts=3,
            max_wall_time_ms=20,
        )

        def cross_deadline(_retry_state: object) -> float:
            clock.now = deadline
            return 0.0

        with patch.object(campaign_module, "time", clock), patch.object(
            campaign_module,
            "wait_none",
            return_value=cross_deadline,
        ):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-deadline-between-attempts",
                )

        self.assertEqual(provider.call_count, 1)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 2)
        self.assertEqual(
            [event[1].attempt_id for event in begin_events],
            [
                "call-deadline-between-attempts-attempt-001",
                "call-deadline-between-attempts-attempt-002",
            ],
        )
        self.assertEqual(
            [event[1].outcome for event in begin_events],
            [InvocationOutcome.EXCEPTION, InvocationOutcome.TIMEOUT],
        )

    def test_inline_expired_before_provider_never_invokes_provider(self) -> None:
        deadline = 250.0
        original_canonicalize = campaign_module._canonical_json_request

        for scenario, start_time in (
            ("preexpired", deadline),
            ("canonicalization_crossed", deadline - 1.0),
        ):
            with self.subTest(scenario=scenario):
                clock = _ControlledClock(now=start_time)
                provider = _ClockAdvancingSuccessProvider(
                    clock,
                    advance_seconds=0.0,
                )
                journal = _RecordingUsageJournal()
                invocation = ModelInvocation(
                    provider=provider,
                    usage_journal=journal,
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-primary-model",
                )

                def canonicalize(request: object) -> object:
                    canonical_request = original_canonicalize(request)
                    if scenario == "canonicalization_crossed":
                        clock.now = deadline
                    return canonical_request

                with patch.object(campaign_module, "time", clock), patch.object(
                    campaign_module,
                    "_canonical_json_request",
                    side_effect=canonicalize,
                ):
                    with self.assertRaises(ModelInvocationTimeoutError):
                        invocation.invoke_json(
                            {"prompt": "offline-only"},
                            call_id=f"call-inline-{scenario}",
                            attempt_id="attempt-001",
                            deadline=deadline,
                        )

                self.assertEqual(provider.call_count, 0)
                self.assertEqual(len(journal.events), 1)
                event, envelope = journal.events[0]
                self.assertEqual(event, "begin")
                self.assertIsInstance(envelope, UsageEnvelope)
                self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
                self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_inline_late_success_is_rejected_once_at_the_shared_deadline(self) -> None:
        clock = _ControlledClock(now=300.0)
        provider = _ClockAdvancingSuccessProvider(
            clock,
            advance_seconds=0.1,
        )
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
            max_wall_time_ms=100,
        )

        with patch.object(campaign_module, "time", clock):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-inline-late-success",
                )

        self.assertEqual(provider.call_count, 1)
        self.assertEqual(len(journal.events), 1)
        event, envelope = journal.events[0]
        self.assertEqual(event, "begin")
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_inline_late_provider_exception_is_deadline_terminal(self) -> None:
        clock = _ControlledClock(now=350.0)
        provider = _ClockAdvancingExceptionProvider(
            clock,
            advance_seconds=0.1,
        )
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=1,
            max_wall_time_ms=100,
        )

        with patch.object(campaign_module, "time", clock):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-inline-late-provider-exception",
                )

        self.assertEqual(provider.call_count, 1)
        self.assertEqual(len(journal.events), 1)
        event, envelope = journal.events[0]
        self.assertEqual(event, "begin")
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_deadline_crossing_after_usage_begin_cannot_commit_success(self) -> None:
        clock = _ControlledClock(now=375.0)
        provider = _ClockAdvancingSuccessProvider(
            clock,
            advance_seconds=0.0,
        )
        journal = _DeadlineAdvancingUsageJournal(clock, deadline=375.1)
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=2,
            max_wall_time_ms=100,
        )

        with patch.object(campaign_module, "time", clock):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-deadline-during-usage-begin",
                )

        self.assertEqual(provider.call_count, 1)
        outcomes = [
            event[1][2]
            for event in journal.events
            if event[0] == "finish"
        ]
        self.assertEqual(outcomes, [InvocationOutcome.TIMEOUT])
        self.assertNotIn(InvocationOutcome.SUCCESS, outcomes)

    def test_usage_begin_deadline_crossing_selects_timeout_for_every_response(
        self,
    ) -> None:
        deadline = 500.1
        scenarios = (
            (
                "streamed",
                ProviderResponse(
                    output_text='{"status":"partial"}',
                    request_model="fake-primary-model",
                    response_model="fake-primary-model",
                    raw_usage={"input_tokens": 4, "output_tokens": 2},
                    streamed=True,
                ),
                48 * 1024,
                InvocationOutcome.STREAMING_DISABLED,
            ),
            (
                "overflow",
                ProviderResponse(
                    output_text="x" * 32,
                    request_model="fake-primary-model",
                    response_model="fake-primary-model",
                    raw_usage={"input_tokens": 4, "output_tokens": 2},
                ),
                8,
                InvocationOutcome.INVALID_JSON,
            ),
            (
                "empty",
                ProviderResponse(
                    output_text="   ",
                    request_model="fake-primary-model",
                    response_model="fake-primary-model",
                    raw_usage={"input_tokens": 4, "output_tokens": 2},
                ),
                48 * 1024,
                InvocationOutcome.EMPTY_OUTPUT,
            ),
            (
                "parse-error",
                ProviderResponse(
                    output_text="{not-json",
                    request_model="fake-primary-model",
                    response_model="fake-primary-model",
                    raw_usage={"input_tokens": 4, "output_tokens": 2},
                ),
                48 * 1024,
                InvocationOutcome.INVALID_JSON,
            ),
            (
                "success",
                ProviderResponse(
                    output_text='{"ok":1}',
                    request_model="fake-primary-model",
                    response_model="fake-primary-model",
                    raw_usage={"input_tokens": 4, "output_tokens": 2},
                ),
                48 * 1024,
                InvocationOutcome.SUCCESS,
            ),
        )

        for name, response, max_output_bytes, candidate in scenarios:
            with self.subTest(name=name):
                clock = _ControlledClock(now=500.0)
                journal = _DeadlineAdvancingUsageJournal(
                    clock,
                    deadline=deadline,
                )

                class CountingProvider:
                    def __init__(self) -> None:
                        self.call_count = 0

                    def invoke(self, request: object) -> ProviderResponse:
                        del request
                        self.call_count += 1
                        return response

                provider = CountingProvider()
                invocation = RetryingModelInvocation(
                    attempt=ModelInvocation(
                        provider=provider,
                        usage_journal=journal,
                        provider_name="fake",
                        profile="offline",
                        request_model="fake-primary-model",
                        max_output_bytes=max_output_bytes,
                    ),
                    max_attempts=3,
                    max_wall_time_ms=100,
                )

                with patch.object(campaign_module, "time", clock):
                    with self.assertRaises(ModelInvocationTimeoutError):
                        invocation.invoke_json(
                            {"prompt": "offline-only"},
                            call_id=f"call-begin-deadline-{name}",
                        )

                self.assertEqual(provider.call_count, 1)
                self.assertEqual(
                    [event[0] for event in journal.events],
                    ["begin", "finish"],
                )
                envelope = journal.events[0][1]
                self.assertIsInstance(envelope, UsageEnvelope)
                self.assertEqual(
                    envelope.outcome,
                    InvocationOutcome.RESPONSE_RECEIVED,
                )
                self.assertEqual(
                    journal.events[1][1][2],
                    InvocationOutcome.TIMEOUT,
                )
                self.assertEqual(
                    journal.finish_calls[0][2:],
                    (candidate, deadline, InvocationOutcome.TIMEOUT),
                )

    def test_parse_deadline_crossing_prefers_timeout_and_preserves_parse_cause(
        self,
    ) -> None:
        original_parse = campaign_module._parse_bounded_model_output
        deadline = 525.1

        for name, output_text, expected_candidate in (
            ("success", '{"ok":1}', InvocationOutcome.SUCCESS),
            ("parse-error", "{not-json", InvocationOutcome.INVALID_JSON),
        ):
            with self.subTest(name=name):
                clock = _ControlledClock(now=525.0)
                journal = _RecordingUsageJournal(clock=clock)

                class CountingProvider:
                    def __init__(self) -> None:
                        self.call_count = 0

                    def invoke(self, request: object) -> ProviderResponse:
                        del request
                        self.call_count += 1
                        return ProviderResponse(
                            output_text=output_text,
                            request_model="fake-primary-model",
                            response_model="fake-primary-model",
                            raw_usage={"input_tokens": 3, "output_tokens": 1},
                        )

                provider = CountingProvider()
                invocation = RetryingModelInvocation(
                    attempt=ModelInvocation(
                        provider=provider,
                        usage_journal=journal,
                        provider_name="fake",
                        profile="offline",
                        request_model="fake-primary-model",
                    ),
                    max_attempts=3,
                    max_wall_time_ms=100,
                )

                def parse_then_cross(
                    text: str,
                    *,
                    max_output_bytes: int,
                ) -> object:
                    try:
                        return original_parse(
                            text,
                            max_output_bytes=max_output_bytes,
                        )
                    finally:
                        clock.now = deadline

                with patch.object(campaign_module, "time", clock), patch.object(
                    campaign_module,
                    "_parse_bounded_model_output",
                    side_effect=parse_then_cross,
                ):
                    with self.assertRaises(ModelInvocationTimeoutError) as caught:
                        invocation.invoke_json(
                            {"prompt": "offline-only"},
                            call_id=f"call-parse-deadline-{name}",
                        )

                self.assertEqual(provider.call_count, 1)
                self.assertEqual(
                    [event[0] for event in journal.events],
                    ["begin", "finish"],
                )
                self.assertEqual(
                    journal.finish_calls[0][2:],
                    (
                        expected_candidate,
                        deadline,
                        InvocationOutcome.TIMEOUT,
                    ),
                )
                if name == "parse-error":
                    self.assertIsInstance(
                        caught.exception.__cause__,
                        json.JSONDecodeError,
                    )

    def test_valid_snapshot_keeps_known_usage_when_deadline_crosses_before_finish(
        self,
    ) -> None:
        deadline = 550.1
        original_execute = campaign_module._InlineProviderExecutor.execute

        for phase in ("executor_return", "usage_begin"):
            with self.subTest(phase=phase):
                clock = _ControlledClock(now=550.0)
                journal = (
                    _DeadlineAdvancingUsageJournal(clock, deadline=deadline)
                    if phase == "usage_begin"
                    else _RecordingUsageJournal(clock=clock)
                )

                class CountingProvider:
                    def __init__(self) -> None:
                        self.call_count = 0

                    def invoke(self, request: object) -> ProviderResponse:
                        del request
                        self.call_count += 1
                        return ProviderResponse(
                            output_text='{"ok":1}',
                            request_model="fake-primary-model",
                            response_model="fake-response-model",
                            raw_usage={
                                "input_tokens": 7,
                                "output_tokens": 2,
                                "total_tokens": 9,
                                "reported_cost": "0.01",
                                "currency": "USD",
                            },
                        )

                provider = CountingProvider()
                invocation = RetryingModelInvocation(
                    attempt=ModelInvocation(
                        provider=provider,
                        usage_journal=journal,
                        provider_name="fake",
                        profile="offline",
                        request_model="fake-primary-model",
                    ),
                    max_attempts=2,
                    max_wall_time_ms=100,
                )

                def execute_then_maybe_cross(
                    executor: object,
                    bound_provider: object,
                    request: object,
                    *,
                    deadline: float | None,
                    max_output_bytes: int,
                ) -> object:
                    snapshot = original_execute(
                        executor,
                        bound_provider,
                        request,
                        deadline=deadline,
                        max_output_bytes=max_output_bytes,
                    )
                    if phase == "executor_return":
                        clock.now = 550.1
                    return snapshot

                with patch.object(campaign_module, "time", clock), patch.object(
                    campaign_module._InlineProviderExecutor,
                    "execute",
                    new=execute_then_maybe_cross,
                ):
                    with self.assertRaises(ModelInvocationTimeoutError):
                        invocation.invoke_json(
                            {"prompt": "offline-only"},
                            call_id=f"call-known-usage-{phase}",
                        )

                self.assertEqual(provider.call_count, 1)
                self.assertEqual(
                    [event[0] for event in journal.events],
                    ["begin", "finish"],
                )
                envelope = journal.events[0][1]
                self.assertIsInstance(envelope, UsageEnvelope)
                self.assertEqual(envelope.usage_status, UsageStatus.REPORTED)
                self.assertEqual(envelope.response_model, "fake-response-model")
                self.assertEqual(envelope.total_tokens, 9)
                self.assertEqual(envelope.reported_cost, "0.01")
                self.assertEqual(
                    envelope.outcome,
                    InvocationOutcome.RESPONSE_RECEIVED,
                )
                self.assertEqual(
                    journal.events[1][1][2],
                    InvocationOutcome.TIMEOUT,
                )

    def test_usage_finish_actual_outcome_controls_public_result_without_retry(
        self,
    ) -> None:
        class InvalidActualJournal(_RecordingUsageJournal):
            def finish(
                self,
                *,
                call_id: str,
                attempt_id: str,
                outcome: InvocationOutcome,
                deadline: float | None = None,
            ) -> InvocationOutcome:
                super().finish(
                    call_id=call_id,
                    attempt_id=attempt_id,
                    outcome=outcome,
                    deadline=deadline,
                )
                return InvocationOutcome.EXCEPTION

        class CountingProvider:
            def __init__(self) -> None:
                self.call_count = 0

            def invoke(self, request: object) -> ProviderResponse:
                del request
                self.call_count += 1
                return ProviderResponse(
                    output_text='{"ok":1}',
                    request_model="fake-primary-model",
                    response_model="fake-primary-model",
                    raw_usage={},
                )

        provider = CountingProvider()
        journal = InvalidActualJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=3,
        )

        with self.assertRaisesRegex(TypeError, "usage journal.*outcome"):
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-invalid-journal-actual",
            )

        self.assertEqual(provider.call_count, 1)
        self.assertEqual(len(journal.finish_calls), 1)

    def test_usage_journal_errors_propagate_once_without_provider_retry(self) -> None:
        class JournalFailure(RuntimeError):
            pass

        for phase in ("begin", "finish"):
            with self.subTest(phase=phase):
                failure = JournalFailure(f"{phase} failed")

                class FailingJournal(_RecordingUsageJournal):
                    def begin(self, envelope: UsageEnvelope) -> None:
                        if phase == "begin":
                            raise failure
                        super().begin(envelope)

                    def finish(
                        self,
                        *,
                        call_id: str,
                        attempt_id: str,
                        outcome: InvocationOutcome,
                        deadline: float | None = None,
                    ) -> InvocationOutcome:
                        if phase == "finish":
                            self.finish_calls.append(
                                (
                                    call_id,
                                    attempt_id,
                                    outcome,
                                    deadline,
                                    outcome,
                                )
                            )
                            raise failure
                        return super().finish(
                            call_id=call_id,
                            attempt_id=attempt_id,
                            outcome=outcome,
                            deadline=deadline,
                        )

                class CountingProvider:
                    def __init__(self) -> None:
                        self.call_count = 0

                    def invoke(self, request: object) -> ProviderResponse:
                        del request
                        self.call_count += 1
                        return ProviderResponse(
                            output_text='{"ok":1}',
                            request_model="fake-primary-model",
                            response_model="fake-primary-model",
                            raw_usage={},
                        )

                provider = CountingProvider()
                journal = FailingJournal()
                invocation = RetryingModelInvocation(
                    attempt=ModelInvocation(
                        provider=provider,
                        usage_journal=journal,
                        provider_name="fake",
                        profile="offline",
                        request_model="fake-primary-model",
                    ),
                    max_attempts=3,
                )

                with self.assertRaises(JournalFailure) as caught:
                    invocation.invoke_json(
                        {"prompt": "offline-only"},
                        call_id=f"call-journal-{phase}-error",
                    )

                self.assertIs(caught.exception, failure)
                self.assertEqual(provider.call_count, 1)
                self.assertEqual(
                    len(journal.finish_calls),
                    0 if phase == "begin" else 1,
                )

    def test_retryable_typed_journal_errors_are_never_provider_retries(
        self,
    ) -> None:
        failure_types = (
            InvalidModelResponseError,
            ModelInvocationProviderError,
            ModelInvocationTimeoutError,
        )
        for phase in ("begin", "finish"):
            for failure_type in failure_types:
                with self.subTest(phase=phase, failure_type=failure_type.__name__):
                    failure = failure_type(f"journal {phase} failed")

                    class FailingJournal:
                        def __init__(self) -> None:
                            self.begin_count = 0
                            self.finish_count = 0

                        def begin(self, envelope: UsageEnvelope) -> None:
                            del envelope
                            self.begin_count += 1
                            if phase == "begin":
                                raise failure

                        def finish(
                            self,
                            *,
                            call_id: str,
                            attempt_id: str,
                            outcome: InvocationOutcome,
                            deadline: float | None = None,
                        ) -> InvocationOutcome:
                            del call_id, attempt_id, deadline
                            self.finish_count += 1
                            if phase == "finish":
                                raise failure
                            return outcome

                    class CountingProvider:
                        def __init__(self) -> None:
                            self.call_count = 0

                        def invoke(self, request: object) -> ProviderResponse:
                            del request
                            self.call_count += 1
                            return ProviderResponse(
                                output_text='{"ok":1}',
                                request_model="fake-primary-model",
                                response_model="fake-primary-model",
                                raw_usage={},
                            )

                    provider = CountingProvider()
                    journal = FailingJournal()
                    invocation = RetryingModelInvocation(
                        attempt=ModelInvocation(
                            provider=provider,
                            usage_journal=journal,
                            provider_name="fake",
                            profile="offline",
                            request_model="fake-primary-model",
                        ),
                        max_attempts=3,
                    )

                    with self.assertRaises(failure_type) as caught:
                        invocation.invoke_json(
                            {"prompt": "offline-only"},
                            call_id=(
                                f"call-typed-journal-{phase}-"
                                f"{failure_type.__name__}"
                            ),
                        )

                    self.assertIs(caught.exception, failure)
                    self.assertIsNone(caught.exception.__cause__)
                    self.assertEqual(provider.call_count, 1)
                    self.assertEqual(journal.begin_count, 1)
                    self.assertEqual(
                        journal.finish_count,
                        0 if phase == "begin" else 1,
                    )

    def test_class_only_journal_markers_do_not_suppress_provider_retries(
        self,
    ) -> None:
        class ClassMarkedProviderError(ModelInvocationProviderError):
            _control_plane_usage_journal_origin = True

        class InheritedMarker:
            _control_plane_usage_journal_origin = True

        class InheritedMarkedProviderError(
            InheritedMarker,
            ModelInvocationProviderError,
        ):
            pass

        class AlwaysFailingInvocation(ModelInvocation):
            def __init__(self, failure: ModelInvocationProviderError) -> None:
                super().__init__(
                    provider=_OutputTextProvider('{"ok":1}'),
                    usage_journal=_RecordingUsageJournal(),
                    provider_name="fake",
                    profile="offline",
                    request_model="fake-primary-model",
                )
                self.failure = failure
                self.call_count = 0

            def invoke_json(
                self,
                request: object,
                *,
                call_id: str,
                attempt_id: str,
                deadline: float | None = None,
            ) -> object:
                del request, call_id, attempt_id, deadline
                self.call_count += 1
                raise self.failure

        failures = (
            ClassMarkedProviderError("class marker is not provenance"),
            InheritedMarkedProviderError("inherited marker is not provenance"),
        )
        for failure in failures:
            with self.subTest(failure_type=type(failure).__name__):
                self.assertNotIn(
                    campaign_module._USAGE_JOURNAL_ERROR_MARKER,
                    failure.__dict__,
                )
                self.assertFalse(campaign_module._is_usage_journal_error(failure))
                attempt = AlwaysFailingInvocation(failure)
                invocation = RetryingModelInvocation(
                    attempt=attempt,
                    max_attempts=3,
                )

                with self.assertRaises(type(failure)) as caught:
                    invocation.invoke_json(
                        {"prompt": "offline-only"},
                        call_id=f"call-class-marker-{type(failure).__name__}",
                    )

                self.assertIs(caught.exception, failure)
                self.assertEqual(attempt.call_count, 3)

    def test_data_descriptors_cannot_block_journal_error_provenance(
        self,
    ) -> None:
        class ExplodingMarkerDescriptor:
            def __get__(self, instance: object, owner: type | None = None) -> object:
                del instance, owner
                raise RuntimeError("marker descriptor getter must not run")

            def __set__(self, instance: object, value: object) -> None:
                del instance, value
                raise RuntimeError("marker descriptor setter must not run")

        class DescriptorShieldedJournalError(ModelInvocationProviderError):
            _control_plane_usage_journal_origin = ExplodingMarkerDescriptor()

            @property
            def __dict__(self) -> dict[str, object]:
                raise RuntimeError("subclass __dict__ descriptor must not run")

        for phase in ("begin", "finish"):
            failure = DescriptorShieldedJournalError(f"journal {phase} failed")

            class FailingJournal:
                def __init__(self) -> None:
                    self.begin_count = 0
                    self.finish_count = 0

                def begin(self, envelope: UsageEnvelope) -> None:
                    del envelope
                    self.begin_count += 1
                    if phase == "begin":
                        raise failure

                def finish(
                    self,
                    *,
                    call_id: str,
                    attempt_id: str,
                    outcome: InvocationOutcome,
                    deadline: float | None = None,
                ) -> InvocationOutcome:
                    del call_id, attempt_id, deadline
                    self.finish_count += 1
                    if phase == "finish":
                        raise failure
                    return outcome

            class CountingProvider:
                def __init__(self) -> None:
                    self.call_count = 0

                def invoke(self, request: object) -> ProviderResponse:
                    del request
                    self.call_count += 1
                    return ProviderResponse(
                        output_text='{"ok":1}',
                        request_model="fake-primary-model",
                        response_model="fake-primary-model",
                        raw_usage={},
                    )

            with self.subTest(phase=phase):
                provider = CountingProvider()
                journal = FailingJournal()
                invocation = RetryingModelInvocation(
                    attempt=ModelInvocation(
                        provider=provider,
                        usage_journal=journal,
                        provider_name="fake",
                        profile="offline",
                        request_model="fake-primary-model",
                    ),
                    max_attempts=3,
                )

                with self.assertRaises(DescriptorShieldedJournalError) as caught:
                    invocation.invoke_json(
                        {"prompt": "offline-only"},
                        call_id=f"call-descriptor-journal-{phase}",
                    )

                self.assertIs(caught.exception, failure)
                self.assertIs(type(caught.exception), DescriptorShieldedJournalError)
                self.assertTrue(campaign_module._is_usage_journal_error(failure))
                self.assertEqual(provider.call_count, 1)
                self.assertEqual(journal.begin_count, 1)
                self.assertEqual(
                    journal.finish_count,
                    0 if phase == "begin" else 1,
                )

    def test_uncooperative_typed_journal_errors_preserve_identity_without_retry(
        self,
    ) -> None:
        class MarkerWriteRejectingTimeoutError(ModelInvocationTimeoutError):
            def __setattr__(self, name: str, value: object) -> None:
                if name == campaign_module._USAGE_JOURNAL_ERROR_MARKER:
                    raise AttributeError("ordinary marker writes are rejected")
                super().__setattr__(name, value)

        class MarkerReadRejectingInvalidResponseError(InvalidModelResponseError):
            def __getattribute__(self, name: str) -> object:
                if name == campaign_module._USAGE_JOURNAL_ERROR_MARKER:
                    raise AttributeError("ordinary marker reads are rejected")
                return super().__getattribute__(name)

        cases = (
            ("begin", MarkerWriteRejectingTimeoutError("journal begin failed")),
            (
                "finish",
                MarkerReadRejectingInvalidResponseError("journal finish failed"),
            ),
        )
        for phase, failure in cases:
            with self.subTest(phase=phase, failure_type=type(failure).__name__):
                class FailingJournal:
                    def __init__(self) -> None:
                        self.begin_count = 0
                        self.finish_count = 0

                    def begin(self, envelope: UsageEnvelope) -> None:
                        del envelope
                        self.begin_count += 1
                        if phase == "begin":
                            raise failure

                    def finish(
                        self,
                        *,
                        call_id: str,
                        attempt_id: str,
                        outcome: InvocationOutcome,
                        deadline: float | None = None,
                    ) -> InvocationOutcome:
                        del call_id, attempt_id, deadline
                        self.finish_count += 1
                        if phase == "finish":
                            raise failure
                        return outcome

                class CountingProvider:
                    def __init__(self) -> None:
                        self.call_count = 0

                    def invoke(self, request: object) -> ProviderResponse:
                        del request
                        self.call_count += 1
                        return ProviderResponse(
                            output_text='{"ok":1}',
                            request_model="fake-primary-model",
                            response_model="fake-primary-model",
                            raw_usage={},
                        )

                provider = CountingProvider()
                journal = FailingJournal()
                invocation = RetryingModelInvocation(
                    attempt=ModelInvocation(
                        provider=provider,
                        usage_journal=journal,
                        provider_name="fake",
                        profile="offline",
                        request_model="fake-primary-model",
                    ),
                    max_attempts=3,
                )

                with self.assertRaises(type(failure)) as caught:
                    invocation.invoke_json(
                        {"prompt": "offline-only"},
                        call_id=f"call-uncooperative-journal-{phase}",
                    )

                self.assertIs(caught.exception, failure)
                self.assertIs(type(caught.exception), type(failure))
                self.assertEqual(provider.call_count, 1)
                self.assertEqual(journal.begin_count, 1)
                self.assertEqual(
                    journal.finish_count,
                    0 if phase == "begin" else 1,
                )

        unmarked_provider_failure = MarkerReadRejectingInvalidResponseError(
            "unmarked provider failure"
        )
        self.assertFalse(
            campaign_module._is_usage_journal_error(unmarked_provider_failure)
        )

    def test_retryable_typed_unknown_outcome_begin_error_is_not_retried(
        self,
    ) -> None:
        failure = ModelInvocationProviderError(
            "unknown outcome journal begin failed"
        )

        class FailingJournal:
            def __init__(self) -> None:
                self.begin_count = 0
                self.finish_count = 0

            def begin(self, envelope: UsageEnvelope) -> None:
                del envelope
                self.begin_count += 1
                raise failure

            def finish(self, **kwargs: object) -> InvocationOutcome:
                del kwargs
                self.finish_count += 1
                raise AssertionError("unknown outcome must not call finish")

        class CountingTimeoutProvider:
            def __init__(self) -> None:
                self.call_count = 0

            def invoke(self, request: object) -> ProviderResponse:
                del request
                self.call_count += 1
                raise TimeoutError("provider timed out")

        provider = CountingTimeoutProvider()
        journal = FailingJournal()
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=3,
        )

        with self.assertRaises(ModelInvocationProviderError) as caught:
            invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-typed-journal-unknown-begin",
            )

        self.assertIs(caught.exception, failure)
        self.assertIsNone(caught.exception.__cause__)
        self.assertEqual(provider.call_count, 1)
        self.assertEqual(journal.begin_count, 1)
        self.assertEqual(journal.finish_count, 0)

    def test_journal_control_flow_interrupts_propagate_without_retry(self) -> None:
        for phase in ("begin", "finish"):
            for interrupt_type in (KeyboardInterrupt, SystemExit):
                with self.subTest(phase=phase, interrupt_type=interrupt_type.__name__):
                    interrupt = interrupt_type(f"journal {phase} interrupted")

                    class InterruptingJournal:
                        def __init__(self) -> None:
                            self.begin_count = 0
                            self.finish_count = 0

                        def begin(self, envelope: UsageEnvelope) -> None:
                            del envelope
                            self.begin_count += 1
                            if phase == "begin":
                                raise interrupt

                        def finish(
                            self,
                            *,
                            call_id: str,
                            attempt_id: str,
                            outcome: InvocationOutcome,
                            deadline: float | None = None,
                        ) -> InvocationOutcome:
                            del call_id, attempt_id, deadline
                            self.finish_count += 1
                            if phase == "finish":
                                raise interrupt
                            return outcome

                    class CountingProvider:
                        def __init__(self) -> None:
                            self.call_count = 0

                        def invoke(self, request: object) -> ProviderResponse:
                            del request
                            self.call_count += 1
                            return ProviderResponse(
                                output_text='{"ok":1}',
                                request_model="fake-primary-model",
                                response_model="fake-primary-model",
                                raw_usage={},
                            )

                    provider = CountingProvider()
                    journal = InterruptingJournal()
                    invocation = RetryingModelInvocation(
                        attempt=ModelInvocation(
                            provider=provider,
                            usage_journal=journal,
                            provider_name="fake",
                            profile="offline",
                            request_model="fake-primary-model",
                        ),
                        max_attempts=3,
                    )

                    with self.assertRaises(interrupt_type) as caught:
                        invocation.invoke_json(
                            {"prompt": "offline-only"},
                            call_id=(
                                f"call-journal-interrupt-{phase}-"
                                f"{interrupt_type.__name__}"
                            ),
                        )

                    self.assertIs(caught.exception, interrupt)
                    self.assertEqual(provider.call_count, 1)
                    self.assertEqual(journal.begin_count, 1)
                    self.assertEqual(
                        journal.finish_count,
                        0 if phase == "begin" else 1,
                    )

    def test_retrying_preserves_success_linearized_before_tail_deadline(self) -> None:
        deadline = 575.1
        clock = _ControlledClock(now=575.0)

        class TailCrossingJournal(_RecordingUsageJournal):
            def finish(
                self,
                *,
                call_id: str,
                attempt_id: str,
                outcome: InvocationOutcome,
                deadline: float | None = None,
            ) -> InvocationOutcome:
                actual = super().finish(
                    call_id=call_id,
                    attempt_id=attempt_id,
                    outcome=outcome,
                    deadline=deadline,
                )
                clock.now = 575.1
                return actual

        provider = _OutputTextProvider('{"ok":1}')
        journal = TailCrossingJournal(clock=clock)
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=2,
            max_wall_time_ms=100,
        )

        with patch.object(campaign_module, "time", clock):
            result = invocation.invoke_json(
                {"prompt": "offline-only"},
                call_id="call-success-before-tail-deadline",
            )

        self.assertEqual(result, {"ok": 1})
        self.assertEqual(
            journal.finish_calls[0][2:],
            (InvocationOutcome.SUCCESS, deadline, InvocationOutcome.SUCCESS),
        )

    def test_public_attempt_final_fence_rejects_late_result_without_retry(
        self,
    ) -> None:
        clock = _ControlledClock(now=400.0)
        journal = _RecordingUsageJournal(clock=clock)
        provider = _ClockAdvancingSuccessProvider(
            clock,
            advance_seconds=0.0,
        )
        invocation = RetryingModelInvocation(
            attempt=ModelInvocation(
                provider=provider,
                usage_journal=journal,
                provider_name="fake",
                profile="offline",
                request_model="fake-primary-model",
            ),
            max_attempts=2,
            max_wall_time_ms=100,
        )
        original_parse = campaign_module._parse_bounded_model_output

        def parse_after_deadline(
            output_text: str,
            *,
            max_output_bytes: int,
        ) -> object:
            parsed = original_parse(
                output_text,
                max_output_bytes=max_output_bytes,
            )
            clock.now = 400.1
            return parsed

        with patch.object(campaign_module, "time", clock), patch.object(
            campaign_module,
            "_parse_bounded_model_output",
            side_effect=parse_after_deadline,
        ) as parse_output:
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-public-final-fence",
                )

        self.assertEqual(provider.call_count, 1)
        self.assertEqual(parse_output.call_count, 1)
        self.assertEqual(len(journal.events), 2)
        event, envelope = journal.events[0]
        self.assertEqual(event, "begin")
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.outcome, InvocationOutcome.RESPONSE_RECEIVED)
        self.assertEqual(
            journal.events[1],
            (
                "finish",
                (
                    "call-public-final-fence",
                    "call-public-final-fence-attempt-001",
                    InvocationOutcome.TIMEOUT,
                ),
            ),
        )

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
        ), patch.object(
            campaign_module.time,
            "monotonic",
            side_effect=clock.monotonic,
        ):
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

    def test_process_construction_interrupt_closes_both_pipe_ends(self) -> None:
        for interrupt_type in (KeyboardInterrupt, SystemExit):
            with self.subTest(interrupt_type=interrupt_type.__name__):
                context = _ProcessConstructionInterruptContext(
                    interrupt_type("synthetic process construction interruption")
                )
                journal = _RecordingUsageJournal()
                invocation = _spawned_invocation(_ChildPidProvider(), journal)

                with patch.object(
                    campaign_module.multiprocessing,
                    "get_context",
                    return_value=context,
                ):
                    with self.assertRaises(interrupt_type):
                        invocation.invoke_json(
                            {"prompt": "offline-only"},
                            call_id="call-process-construction-interrupt",
                            attempt_id="attempt-001",
                            deadline=time.monotonic() + 10.0,
                        )

                self.assertEqual(context.process_calls, 1)
                self.assertTrue(context.receive.closed)
                self.assertTrue(context.send.closed)
                self.assertEqual(journal.events, [])

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

    def test_raw_executor_timeout_is_terminal_protocol_error_without_retry(self) -> None:
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
            side_effect=TimeoutError("undeclared executor timeout"),
        ) as execute:
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-terminal-raw-executor-timeout",
                )

        self.assertEqual(execute.call_count, 1)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        envelope = begin_events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_parent_send_close_failure_reaps_worker_and_closes_all_resources(
        self,
    ) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _LifecycleFenceContext(
            clock=clock,
            deadline=deadline,
            fail_first_send_close=True,
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
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-parent-send-close-failure",
                )

        self.assertGreaterEqual(context.process.terminate_calls, 1)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertGreaterEqual(context.send.close_calls, 2)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        envelope = begin_events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_persistent_parent_send_close_failure_has_one_bounded_retry(
        self,
    ) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _LifecycleFenceContext(clock=clock, deadline=deadline)
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )

        def fail_send_close() -> None:
            context.send.close_calls += 1
            context.actions.append("send.close")
            raise OSError("synthetic persistent parent send close failure")

        context.send.close = fail_send_close  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-persistent-parent-send-close-failure",
                )

        self.assertEqual(context.send.close_calls, 2)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        self.assertEqual(begin_events[0][1].outcome, InvocationOutcome.EXCEPTION)

    def test_partial_start_failure_with_pid_reaps_and_closes_worker(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _LifecycleFenceContext(clock=clock, deadline=deadline)
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )

        def fail_start_after_pid() -> None:
            context.actions.append("process.start")
            context.process.pid = 12345
            context.process._alive = True
            raise OSError("synthetic partial start failure")

        context.process.start = fail_start_after_pid  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(
            campaign_module.time,
            "monotonic",
            side_effect=clock.monotonic,
        ):
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-partial-start-failure",
                )

        self.assertGreaterEqual(context.process.terminate_calls, 1)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        self.assertEqual(begin_events[0][1].outcome, InvocationOutcome.EXCEPTION)

    def test_partial_start_interrupt_with_pid_still_reaps_worker(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _LifecycleFenceContext(clock=clock, deadline=deadline)
        journal = _RecordingUsageJournal()
        invocation = _spawned_invocation(_ChildPidProvider(), journal)

        def interrupt_start_after_pid() -> None:
            context.actions.append("process.start")
            context.process.pid = 12345
            context.process._alive = True
            raise KeyboardInterrupt("synthetic partial start interruption")

        context.process.start = interrupt_start_after_pid  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(
            campaign_module.time,
            "monotonic",
            side_effect=clock.monotonic,
        ):
            with self.assertRaises(KeyboardInterrupt):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-partial-start-interrupt",
                    attempt_id="attempt-001",
                    deadline=deadline,
                )

        self.assertGreaterEqual(context.process.terminate_calls, 1)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(journal.events, [])

    def test_control_flow_interrupt_wins_after_cleanup_even_at_deadline(self) -> None:
        for interrupt_type in (KeyboardInterrupt, SystemExit):
            with self.subTest(interrupt_type=interrupt_type.__name__):
                deadline = 10.0
                clock = _ControlledClock(now=9.0)
                context = _LifecycleFenceContext(clock=clock, deadline=deadline)
                journal = _RecordingUsageJournal()
                invocation = _spawned_invocation(_ChildPidProvider(), journal)

                def interrupt_start_after_pid() -> None:
                    context.actions.append("process.start")
                    context.process.pid = 12345
                    context.process._alive = True
                    clock.now = deadline
                    raise interrupt_type("synthetic start interruption at deadline")

                context.process.start = (  # type: ignore[method-assign]
                    interrupt_start_after_pid
                )
                with patch.object(
                    campaign_module.multiprocessing,
                    "get_context",
                    return_value=context,
                ), patch.object(campaign_module, "time", clock):
                    with self.assertRaises(interrupt_type):
                        invocation.invoke_json(
                            {"prompt": "offline-only"},
                            call_id="call-interrupt-at-deadline",
                            attempt_id="attempt-001",
                            deadline=deadline,
                        )

                self.assertGreaterEqual(context.process.terminate_calls, 1)
                self.assertFalse(context.process._alive)
                self.assertTrue(context.process.closed)
                self.assertTrue(context.receive.closed)
                self.assertTrue(context.send.closed)
                self.assertEqual(journal.events, [])

    def test_final_process_close_error_does_not_skip_remaining_cleanup(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _LifecycleFenceContext(clock=clock, deadline=deadline)
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=3,
            max_wall_time_ms=1_000,
        )

        def fail_process_close() -> None:
            context.actions.append("process.close")
            raise OSError("synthetic final process close failure")

        context.process.close = fail_process_close  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(
            campaign_module.time,
            "monotonic",
            side_effect=clock.monotonic,
        ):
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-final-process-close-failure",
                )

        self.assertIn("process.close", context.actions)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertFalse(context.process._alive)
        begin_events = [event for event in journal.events if event[0] == "begin"]
        self.assertEqual(len(begin_events), 1)
        envelope = begin_events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.EXCEPTION)

    def test_cleanup_error_overrides_retryable_primary_failure(self) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _LifecycleFenceContext(clock=clock, deadline=deadline)
        context.receive._frame = campaign_module._provider_frame("provider_timeout")
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=2,
            max_wall_time_ms=1_000,
        )

        def fail_process_close() -> None:
            context.actions.append("process.close")
            raise OSError("synthetic final process close failure")

        context.process.close = fail_process_close  # type: ignore[method-assign]
        with patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ), patch.object(campaign_module, "time", clock):
            with self.assertRaises(ModelInvocationProviderError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-cleanup-overrides-provider-timeout",
                )

        self.assertEqual(context.actions.count("process.start"), 1)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
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

    def test_get_context_failure_after_deadline_is_terminal_timeout(self) -> None:
        clock = _ControlledClock(now=20.0)
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=2,
            max_wall_time_ms=1_000,
        )

        def fail_get_context_after_deadline(method: str) -> object:
            self.assertEqual(method, "spawn")
            clock.now = 21.0
            raise OSError("synthetic context construction failure")

        with patch.object(campaign_module, "time", clock), patch.object(
            campaign_module.multiprocessing,
            "get_context",
            side_effect=fail_get_context_after_deadline,
        ):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-deadline-during-get-context-failure",
                )

        self.assertEqual(len(journal.events), 1)
        self.assertEqual(journal.events[0][1].outcome, InvocationOutcome.TIMEOUT)

    def test_get_context_return_after_deadline_does_not_construct_pipe(self) -> None:
        clock = _ControlledClock(now=30.0)
        context = _PipeFailureContext()
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=2,
            max_wall_time_ms=1_000,
        )

        def return_get_context_after_deadline(method: str) -> object:
            self.assertEqual(method, "spawn")
            clock.now = 31.0
            return context

        with patch.object(campaign_module, "time", clock), patch.object(
            campaign_module.multiprocessing,
            "get_context",
            side_effect=return_get_context_after_deadline,
        ):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-deadline-after-get-context-return",
                )

        self.assertEqual(context.pipe_calls, 0)
        self.assertEqual(context.process_calls, 0)
        self.assertEqual(len(journal.events), 1)
        self.assertEqual(journal.events[0][1].outcome, InvocationOutcome.TIMEOUT)

    def test_pipe_return_after_deadline_closes_ends_without_process(self) -> None:
        deadline = 41.0
        clock = _ControlledClock(now=40.0)
        context = _PipeDeadlineFenceContext(clock=clock, deadline=deadline)
        journal = _RecordingUsageJournal()
        invocation = RetryingModelInvocation(
            attempt=_spawned_invocation(_ChildPidProvider(), journal),
            max_attempts=2,
            max_wall_time_ms=1_000,
        )

        with patch.object(campaign_module, "time", clock), patch.object(
            campaign_module.multiprocessing,
            "get_context",
            return_value=context,
        ):
            with self.assertRaises(ModelInvocationTimeoutError):
                invocation.invoke_json(
                    {"prompt": "offline-only"},
                    call_id="call-deadline-after-pipe-return",
                )

        self.assertEqual(context.pipe_calls, 1)
        self.assertEqual(context.process_calls, 0)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
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

    def test_deadline_crossed_during_start_reaps_without_polling_or_receiving(
        self,
    ) -> None:
        deadline = 10.0
        clock = _ControlledClock(now=9.0)
        context = _LifecycleFenceContext(
            clock=clock,
            deadline=deadline,
            cross_deadline_at="start",
        )
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
                    call_id="call-deadline-during-start",
                    attempt_id="attempt-001",
                    deadline=deadline,
                )

        action_names = [
            action[0] if type(action) is tuple else action
            for action in context.actions
        ]
        self.assertNotIn("receive.poll", action_names)
        self.assertNotIn(
            "receive.recv_bytes",
            action_names,
        )
        self.assertGreaterEqual(context.process.terminate_calls, 1)
        self.assertFalse(context.process._alive)
        self.assertTrue(context.process.closed)
        self.assertTrue(context.receive.closed)
        self.assertTrue(context.send.closed)
        self.assertEqual(len(journal.events), 1)
        envelope = journal.events[0][1]
        self.assertIsInstance(envelope, UsageEnvelope)
        self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_late_ready_response_is_rejected_after_each_blocking_phase(self) -> None:
        for phase in ("send_close", "poll", "recv", "join", "is_alive"):
            with self.subTest(phase=phase):
                deadline = 10.0
                clock = _ControlledClock(now=9.0)
                context = _LifecycleFenceContext(
                    clock=clock,
                    deadline=deadline,
                    cross_deadline_at=phase,
                )
                journal = _RecordingUsageJournal()
                invocation = _spawned_invocation(_ChildPidProvider(), journal)

                with patch.object(
                    campaign_module.multiprocessing,
                    "get_context",
                    return_value=context,
                ), patch.object(
                    campaign_module.time,
                    "monotonic",
                    side_effect=clock.monotonic,
                ):
                    with self.assertRaises(ModelInvocationTimeoutError):
                        invocation.invoke_json(
                            {"prompt": "offline-only"},
                            call_id=f"call-deadline-during-{phase}",
                            attempt_id="attempt-001",
                            deadline=deadline,
                        )

                self.assertFalse(context.process._alive)
                self.assertTrue(context.process.closed)
                self.assertTrue(context.receive.closed)
                self.assertTrue(context.send.closed)
                self.assertEqual(len(journal.events), 1)
                envelope = journal.events[0][1]
                self.assertIsInstance(envelope, UsageEnvelope)
                self.assertEqual(envelope.usage_status, UsageStatus.UNKNOWN)
                self.assertEqual(envelope.outcome, InvocationOutcome.TIMEOUT)

    def test_late_response_is_rejected_when_decode_crosses_deadline(self) -> None:
        for decode_outcome in ("response", "protocol_error"):
            with self.subTest(decode_outcome=decode_outcome):
                deadline = 10.0
                clock = _ControlledClock(now=9.0)
                context = _LifecycleFenceContext(clock=clock, deadline=deadline)
                journal = _RecordingUsageJournal()
                invocation = _spawned_invocation(_ChildPidProvider(), journal)
                decode_provider_frame = campaign_module._decode_provider_frame

                def decode_after_deadline(frame: bytes) -> object:
                    clock.now = deadline
                    if decode_outcome == "protocol_error":
                        raise campaign_module._ProviderExecutorProtocolError(
                            "synthetic late decode failure"
                        )
                    return decode_provider_frame(frame)

                with patch.object(
                    campaign_module.multiprocessing,
                    "get_context",
                    return_value=context,
                ), patch.object(
                    campaign_module.time,
                    "monotonic",
                    side_effect=clock.monotonic,
                ), patch.object(
                    campaign_module,
                    "_decode_provider_frame",
                    side_effect=decode_after_deadline,
                ):
                    with self.assertRaises(ModelInvocationTimeoutError):
                        invocation.invoke_json(
                            {"prompt": "offline-only"},
                            call_id="call-deadline-during-decode",
                            attempt_id="attempt-001",
                            deadline=deadline,
                        )

                self.assertFalse(context.process._alive)
                self.assertTrue(context.process.closed)
                self.assertTrue(context.receive.closed)
                self.assertTrue(context.send.closed)
                self.assertEqual(len(journal.events), 1)
                envelope = journal.events[0][1]
                self.assertIsInstance(envelope, UsageEnvelope)
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
