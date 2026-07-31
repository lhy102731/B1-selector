from __future__ import annotations

import unittest

from research_automation.control_plane.campaign import (
    InvalidModelResponseError,
    InvocationOutcome,
    ModelInvocation,
    ProviderResponse,
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


class ModelInvocationTests(unittest.TestCase):
    def test_invalid_json_keeps_reported_usage_before_failure_is_exposed(self) -> None:
        journal = _RecordingUsageJournal()
        invocation = ModelInvocation(
            provider=_FakeProvider(),
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
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


if __name__ == "__main__":
    unittest.main()
