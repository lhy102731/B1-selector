from __future__ import annotations

from decimal import Decimal
import unittest

from research_automation.control_plane.campaign import (
    InvocationOutcome,
    ModelInvocation,
    ModelInvocationProviderError,
    ProviderResponse,
    UsageEnvelope,
    UsageStatus,
)
from research_automation.control_plane.campaign_adapters import (
    AG2InjectedSeamAdapter,
    CliInjectedSeamAdapter,
    OpenAICompatibleInjectedSeamAdapter,

    CallableProviderAdapter,
    NormalizedUsage,
    ProviderAdapterError,
    ProviderResponseNormalizer,
    RetryDisabledClientAdapter,
    RetryOwnershipError,
    RetryPolicy,
    build_retrying_invocation,
    normalize_usage,
)


class _RecordingUsageJournal:
    def __init__(self) -> None:
        self.envelopes: list[UsageEnvelope] = []
        self.finishes: list[tuple[str, str, InvocationOutcome]] = []

    def begin(self, envelope: UsageEnvelope) -> None:
        self.envelopes.append(envelope)

    def finish(
        self,
        *,
        call_id: str,
        attempt_id: str,
        outcome: InvocationOutcome,
        deadline: float | None = None,
    ) -> InvocationOutcome:
        self.finishes.append((call_id, attempt_id, outcome))
        return outcome


class NormalizeUsageTests(unittest.TestCase):
    def test_report_status_from_tokens(self) -> None:
        result = normalize_usage(
            {
                "input_tokens": 7,
                "output_tokens": 2,
                "total_tokens": 9,
                "currency": "USD",
                "reported_cost": "0.01",
            }
        )
        self.assertIsInstance(result, NormalizedUsage)
        self.assertEqual(result.usage_status, UsageStatus.REPORTED)
        self.assertEqual(result.input_tokens, 7)
        self.assertEqual(result.output_tokens, 2)
        self.assertEqual(result.total_tokens, 9)
        self.assertEqual(result.currency, "USD")
        self.assertEqual(result.reported_cost, "0.01")

    def test_estimated_flag_selects_estimated(self) -> None:
        result = normalize_usage(
            {"input_tokens": 10, "output_tokens": 5, "estimated": True}
        )
        self.assertEqual(result.usage_status, UsageStatus.ESTIMATED)
        self.assertEqual(result.total_tokens, None)

    def test_explicit_estimated_hint_selects_estimated(self) -> None:
        result = normalize_usage(
            {"input_tokens": 1},
            usage_status_hint=UsageStatus.ESTIMATED,
        )
        self.assertEqual(result.usage_status, UsageStatus.ESTIMATED)

    def test_unknown_when_no_usage(self) -> None:
        result = normalize_usage({})
        self.assertEqual(result.usage_status, UsageStatus.UNKNOWN)
        self.assertEqual(result.input_tokens, None)
        self.assertEqual(result.output_tokens, None)
        self.assertEqual(result.total_tokens, None)
        self.assertEqual(result.reported_cost, None)
        self.assertEqual(result.currency, None)

    def test_malformed_tokens_become_null(self) -> None:
        result = normalize_usage(
            {
                "input_tokens": "7",
                "output_tokens": -1,
                "total_tokens": 2 ** 600,
                "cache_read_tokens": 3,
            }
        )
        self.assertEqual(result.input_tokens, None)
        self.assertEqual(result.output_tokens, None)
        self.assertEqual(result.total_tokens, None)
        self.assertEqual(result.cache_read_tokens, 3)
        self.assertEqual(result.usage_status, UsageStatus.REPORTED)

    def test_total_tokens_never_below_component_sum(self) -> None:
        result = normalize_usage({"input_tokens": 7, "output_tokens": 2, "total_tokens": 5})
        self.assertEqual(result.total_tokens, 9)

    def test_cost_accepts_numeric_scalars(self) -> None:
        self.assertEqual(normalize_usage({"reported_cost": 3}).reported_cost, "3")
        self.assertEqual(normalize_usage({"reported_cost": 0.5}).reported_cost, "0.5")
        self.assertEqual(
            normalize_usage({"reported_cost": Decimal("1.25")}).reported_cost,
            "1.25",
        )
        self.assertEqual(
            normalize_usage({"reported_cost": "2.50"}).reported_cost,
            "2.50",
        )

    def test_cost_rejects_non_finite_values(self) -> None:
        self.assertEqual(normalize_usage({"reported_cost": float("nan")}).reported_cost, None)
        self.assertEqual(
            normalize_usage({"reported_cost": Decimal("Infinity")}).reported_cost,
            None,
        )
        self.assertEqual(normalize_usage({"reported_cost": "not-a-cost"}).reported_cost, None)

    def test_non_mapping_raw_usage_is_unknown(self) -> None:
        result = normalize_usage(b"raw")
        self.assertEqual(result.usage_status, UsageStatus.UNKNOWN)


class ProviderResponseNormalizerTests(unittest.TestCase):
    def test_mapping_output_alias_and_flags(self) -> None:
        normalizer = ProviderResponseNormalizer()
        response = normalizer.normalize(
            {
                "output": '{"ok": true}',
                "raw_usage": {"input_tokens": 3, "output_tokens": 1},
                "fallback": True,
            },
            request_model="fake-model",
            response_model="fake-model",
        )
        self.assertEqual(response.output_text, '{"ok": true}')
        self.assertEqual(response.request_model, "fake-model")
        self.assertEqual(response.response_model, "fake-model")
        self.assertEqual(response.usage_status, UsageStatus.REPORTED)
        self.assertTrue(response.fallback)
        self.assertFalse(response.streamed)

    def test_mapping_usage_alias_key(self) -> None:
        normalizer = ProviderResponseNormalizer()
        response = normalizer.normalize(
            {"output_text": "ok", "usage": {"input_tokens": 1}},
            request_model="m1",
            response_model="m1",
        )
        self.assertEqual(response.usage_status, UsageStatus.REPORTED)
        self.assertEqual(response.input_tokens if hasattr(response, "input_tokens") else None, None)
        self.assertEqual(response.raw_usage["input_tokens"], 1)

    def test_rejects_invalid_model_identifiers(self) -> None:
        normalizer = ProviderResponseNormalizer()
        with self.assertRaises(ProviderAdapterError):
            normalizer.normalize({}, request_model="bad model", response_model="m")
        with self.assertRaises(ProviderAdapterError):
            normalizer.normalize({}, request_model="m", response_model="")

    def test_rejects_unknown_output_type(self) -> None:
        normalizer = ProviderResponseNormalizer()
        with self.assertRaises(ProviderAdapterError):
            normalizer.normalize(
                {"output_text": 123},
                request_model="m",
                response_model="m",
            )

    def test_rejects_model_disagreement(self) -> None:
        normalizer = ProviderResponseNormalizer()
        with self.assertRaises(ProviderAdapterError):
            normalizer.normalize(
                {"response_model": "other"},
                request_model="m",
                response_model="m",
            )

    def test_rejects_non_boolean_flags(self) -> None:
        normalizer = ProviderResponseNormalizer()
        with self.assertRaises(ProviderAdapterError):
            normalizer.normalize(
                {"output_text": "ok", "streamed": "yes"},
                request_model="m",
                response_model="m",
            )

    def test_normalizes_provider_response(self) -> None:
        normalizer = ProviderResponseNormalizer()
        raw = ProviderResponse(
            output_text="ok",
            request_model="m",
            response_model="m",
            raw_usage={"input_tokens": 1, "output_tokens": 2},
        )
        response = normalizer.normalize(
            raw,
            request_model="m",
            response_model="m",
        )
        self.assertEqual(response.usage_status, UsageStatus.REPORTED)
        self.assertEqual(response.output_text, "ok")

    def test_normalized_provider_response_preserves_original_flags(self) -> None:
        normalizer = ProviderResponseNormalizer()
        raw = ProviderResponse(
            output_text="ok",
            request_model="m",
            response_model="m",
            raw_usage={"input_tokens": 1},
            fallback=True,
            streamed=True,
        )
        response = normalizer.normalize(
            raw,
            request_model="m",
            response_model="m",
        )
        self.assertTrue(response.fallback)
        self.assertTrue(response.streamed)

    def test_rejects_non_mapping_non_response(self) -> None:
        normalizer = ProviderResponseNormalizer()
        with self.assertRaises(ProviderAdapterError):
            normalizer.normalize("raw", request_model="m", response_model="m")


class _SuccessfulHandler:
    def __call__(self, request: object) -> object:
        return {
            "output_text": '{"ok": true}',
            "raw_usage": {"input_tokens": 2, "output_tokens": 1, "total_tokens": 3},
        }


class _OnceFailingHandler:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, request: object) -> object:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("transient provider failure")
        return {
            "output_text": '{"ok": true}',
            "raw_usage": {"input_tokens": 2, "output_tokens": 1, "total_tokens": 3},
        }


class CallableProviderAdapterTests(unittest.TestCase):
    def test_single_call_and_counter(self) -> None:
        handler = _SuccessfulHandler()
        adapter = CallableProviderAdapter(
            handler,
            request_model="fake-model",
            response_model="fake-model",
        )
        response = adapter.invoke({"prompt": "hello"})
        self.assertEqual(adapter.calls, 1)
        self.assertEqual(adapter.last_request, {"prompt": "hello"})
        self.assertEqual(response.output_text, '{"ok": true}')
        self.assertEqual(response.usage_status, UsageStatus.REPORTED)

    def test_propagates_handler_exception(self) -> None:
        def handler(request: object) -> object:
            raise RuntimeError("boom")

        adapter = CallableProviderAdapter(
            handler,
            request_model="m",
            response_model="m",
        )
        with self.assertRaises(RuntimeError):
            adapter.invoke({"prompt": "x"})


class _FakeRetryDisabledClient:
    def __init__(self, max_retries: int | None = 0) -> None:
        self.max_retries = max_retries
        self.calls = 0

    def invoke(self, request: object) -> object:
        self.calls += 1
        return {"output_text": "ok", "raw_usage": {"input_tokens": 1}}


class RetryDisabledClientAdapterTests(unittest.TestCase):
    def test_accepts_zero_retries(self) -> None:
        client = _FakeRetryDisabledClient(max_retries=0)
        adapter = RetryDisabledClientAdapter(
            client,
            request_model="m",
            response_model="m",
        )
        response = adapter.invoke({"prompt": "x"})
        self.assertEqual(adapter.calls, 1)
        self.assertEqual(client.calls, 1)
        self.assertEqual(response.output_text, "ok")

    def test_accepts_missing_retry_attribute_for_fakes(self) -> None:
        client = _FakeRetryDisabledClient()
        del client.max_retries
        adapter = RetryDisabledClientAdapter(
            client,
            request_model="m",
            response_model="m",
        )
        self.assertEqual(adapter.invoke({"prompt": "x"}).output_text, "ok")

    def test_rejects_positive_retry_config(self) -> None:
        client = _FakeRetryDisabledClient(max_retries=3)
        with self.assertRaises(RetryOwnershipError):
            RetryDisabledClientAdapter(
                client,
                request_model="m",
                response_model="m",
            )

    def test_rejects_missing_invoke(self) -> None:
        with self.assertRaises(TypeError):
            RetryDisabledClientAdapter(
                object(),
                request_model="m",
                response_model="m",
            )


class RetryPolicyTests(unittest.TestCase):
    def test_valid_policy(self) -> None:
        policy = RetryPolicy(max_attempts=3, max_wall_time_ms=1000)
        self.assertEqual(policy.max_attempts, 3)
        self.assertEqual(policy.max_wall_time_ms, 1000)

    def test_rejects_invalid_attempts(self) -> None:
        with self.assertRaises(ProviderAdapterError):
            RetryPolicy(max_attempts=0)
        with self.assertRaises(ProviderAdapterError):
            RetryPolicy(max_attempts=101)

    def test_rejects_invalid_wall_time(self) -> None:
        with self.assertRaises(ProviderAdapterError):
            RetryPolicy(max_attempts=2, max_wall_time_ms=0)

    def test_build_rejects_wrong_types(self) -> None:
        with self.assertRaises(TypeError):
            build_retrying_invocation(attempt=object(), policy=RetryPolicy(max_attempts=1))
        with self.assertRaises(TypeError):
            build_retrying_invocation(
                attempt=object(),  # type: ignore[arg-type]
                policy=object(),  # type: ignore[arg-type]
            )


class RetryOwnershipIntegrationTests(unittest.TestCase):
    def test_logical_retries_are_owned_by_retrying_invocation(self) -> None:
        handler = _OnceFailingHandler()
        adapter = CallableProviderAdapter(
            handler,
            request_model="fake-model",
            response_model="fake-model",
        )
        journal = _RecordingUsageJournal()
        attempt = ModelInvocation(
            provider=adapter,
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-model",
        )
        invocation = build_retrying_invocation(
            attempt=attempt,
            policy=RetryPolicy(max_attempts=3, max_wall_time_ms=1000),
        )
        result = invocation.invoke_json(
            {"prompt": "retry me"},
            call_id="call-001",
        )
        self.assertEqual(result, {"ok": True})
        self.assertEqual(adapter.calls, 2)
        self.assertEqual(handler.calls, 2)
        self.assertEqual(len(journal.envelopes), 2)
        self.assertEqual(len(journal.finishes), 1)
        self.assertEqual(journal.finishes[0][2], InvocationOutcome.SUCCESS)
        self.assertEqual(
            [envelope.outcome for envelope in journal.envelopes],
            [InvocationOutcome.EXCEPTION, InvocationOutcome.RESPONSE_RECEIVED],
        )

    def test_retries_exhaust_after_max_attempts(self) -> None:
        def handler(request: object) -> object:
            raise RuntimeError("always failing")

        adapter = CallableProviderAdapter(
            handler,
            request_model="fake-model",
            response_model="fake-model",
        )
        journal = _RecordingUsageJournal()
        attempt = ModelInvocation(
            provider=adapter,
            usage_journal=journal,
            provider_name="fake",
            profile="offline",
            request_model="fake-model",
        )
        invocation = build_retrying_invocation(
            attempt=attempt,
            policy=RetryPolicy(max_attempts=2, max_wall_time_ms=1000),
        )
        with self.assertRaises(ModelInvocationProviderError):
            invocation.invoke_json({"prompt": "x"}, call_id="call-002")
        self.assertEqual(adapter.calls, 2)
        self.assertEqual(len(journal.envelopes), 2)


class AG2InjectedSeamTests(unittest.TestCase):
    def test_ag2_seam_normalizes_injected_response(self) -> None:
        adapter = AG2InjectedSeamAdapter(
            lambda request: {
                "output_text": "deterministic",
                "raw_usage": {
                    "input_tokens": 5,
                    "output_tokens": 2,
                    "total_tokens": 7,
                    "reported_cost": "0.01",
                    "currency": "USD",
                },
            },
            request_model="deterministic-reviewer",
            response_model="deterministic-reviewer",
        )
        response = adapter.invoke({"prompt": "x"})
        self.assertEqual(response.output_text, "deterministic")
        self.assertEqual(response.usage_status, UsageStatus.REPORTED)
        self.assertEqual(adapter.calls, 1)
        self.assertEqual(adapter.last_request, {"prompt": "x"})

    def test_ag2_seam_rejects_non_mapping_response(self) -> None:
        adapter = AG2InjectedSeamAdapter(
            lambda request: "not-a-mapping",
            request_model="deterministic-reviewer",
            response_model="deterministic-reviewer",
        )
        with self.assertRaises(ProviderAdapterError):
            adapter.invoke("x")

    def test_ag2_seam_rejects_non_callable_handler(self) -> None:
        with self.assertRaises(TypeError):
            AG2InjectedSeamAdapter(None, request_model="m", response_model="m")  # type: ignore[arg-type]

    def test_ag2_seam_model_drift_fails_closed(self) -> None:
        adapter = AG2InjectedSeamAdapter(
            lambda request: {"output_text": "x", "response_model": "other-model"},
            request_model="deterministic-reviewer",
            response_model="deterministic-reviewer",
        )
        with self.assertRaises(ProviderAdapterError):
            adapter.invoke("x")


class OpenAICompatibleSeamTests(unittest.TestCase):
    def test_openai_seam_accepts_disabled_retry_callable(self) -> None:
        adapter = OpenAICompatibleInjectedSeamAdapter(
            lambda request: {
                "output": "choices[0].message.content",
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 1,
                    "total_tokens": 4,
                },
            },
            request_model="deterministic-reviewer",
            response_model="deterministic-reviewer",
            max_retries=0,
        )
        response = adapter.invoke({"prompt": "x"})
        self.assertEqual(response.output_text, "choices[0].message.content")
        self.assertEqual(response.usage_status, UsageStatus.REPORTED)
        self.assertEqual(adapter.calls, 1)

    def test_openai_seam_rejects_sdk_retries(self) -> None:
        with self.assertRaises(RetryOwnershipError):
            OpenAICompatibleInjectedSeamAdapter(
                lambda request: {"output": "x"},
                request_model="m",
                response_model="m",
                max_retries=3,
            )

    def test_openai_seam_missing_usage_is_unknown_not_zero(self) -> None:
        adapter = OpenAICompatibleInjectedSeamAdapter(
            lambda request: {"output": "x"},
            request_model="deterministic-reviewer",
            response_model="deterministic-reviewer",
            max_retries=0,
        )
        response = adapter.invoke("x")
        self.assertEqual(response.output_text, "x")
        self.assertEqual(response.usage_status, UsageStatus.UNKNOWN)

    def test_openai_seam_rejects_non_mapping_response(self) -> None:
        adapter = OpenAICompatibleInjectedSeamAdapter(
            lambda request: "nope",
            request_model="m",
            response_model="m",
            max_retries=0,
        )
        with self.assertRaises(ProviderAdapterError):
            adapter.invoke("x")


class CliInjectedSeamTests(unittest.TestCase):
    def test_cli_seam_accepts_argv_vector_and_parses_json(self) -> None:
        def runner(argv):
            self.assertEqual(argv, ["provider-cli", "--prompt", "x"])
            return 0, '{"output_text": "cli result", "raw_usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2, "reported_cost": "0.01", "currency": "USD"}}'

        adapter = CliInjectedSeamAdapter(
            runner,
            request_model="deterministic-reviewer",
            response_model="deterministic-reviewer",
        )
        response = adapter.invoke(["provider-cli", "--prompt", "x"])
        self.assertEqual(response.output_text, "cli result")
        self.assertEqual(response.usage_status, UsageStatus.REPORTED)
        self.assertEqual(adapter.calls, 1)

    def test_cli_seam_rejects_shell_string_request(self) -> None:
        adapter = CliInjectedSeamAdapter(
            lambda argv: (0, "{}"),
            request_model="m",
            response_model="m",
        )
        with self.assertRaises(ProviderAdapterError):
            adapter.invoke("provider-cli --prompt x")

    def test_cli_seam_rejects_nonzero_exit(self) -> None:
        adapter = CliInjectedSeamAdapter(
            lambda argv: (1, "boom"),
            request_model="m",
            response_model="m",
        )
        with self.assertRaises(ProviderAdapterError):
            adapter.invoke(["provider-cli"])

    def test_cli_seam_rejects_malformed_json(self) -> None:
        adapter = CliInjectedSeamAdapter(
            lambda argv: (0, "{invalid-json"),
            request_model="m",
            response_model="m",
        )
        with self.assertRaises(ProviderAdapterError):
            adapter.invoke(["provider-cli"])

    def test_cli_seam_rejects_oversized_argv(self) -> None:
        adapter = CliInjectedSeamAdapter(
            lambda argv: (0, "{}"),
            request_model="m",
            response_model="m",
            max_argv_chars=8,
        )
        with self.assertRaises(ProviderAdapterError):
            adapter.invoke(["provider-cli", "--long-argument"])

    def test_cli_seam_rejects_oversized_stdout(self) -> None:
        adapter = CliInjectedSeamAdapter(
            lambda argv: (0, '{"output_text": "' + "x" * 200 + '"}'),
            request_model="m",
            response_model="m",
            max_stdout_chars=64,
        )
        with self.assertRaises(ProviderAdapterError):
            adapter.invoke(["provider-cli"])

    def test_cli_seam_stderr_never_leaks_into_safe_result(self) -> None:
        def runner(argv):
            return 0, '{"output_text": "safe"}'

        adapter = CliInjectedSeamAdapter(
            runner,
            request_model="deterministic-reviewer",
            response_model="deterministic-reviewer",
        )
        response = adapter.invoke(["provider-cli"])
        self.assertEqual(response.output_text, "safe")
        self.assertNotIn("stderr", response.raw_usage)


if __name__ == "__main__":
    unittest.main()
