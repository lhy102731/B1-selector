"""Tests for the production-owned deterministic offline provider."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_automation.control_plane.campaign import ProviderResponse, UsageStatus
from research_automation.control_plane.campaign_offline_provider import (
    CampaignOfflineProvider,
    OfflineProviderError,
    OfflineProviderException,
    OfflineProviderFaultScheduleError,
    OfflineProviderTimeout,
)


class OfflineProviderIdentityTests(unittest.TestCase):
    def test_fixed_identity_matches_roster_binding(self) -> None:
        provider = CampaignOfflineProvider({"verdict": "PASS"})
        self.assertEqual(provider.provider_name, "fake-provider")
        self.assertEqual(provider.profile, "offline-local")
        self.assertEqual(provider.model, "deterministic-reviewer")
        self.assertEqual(provider.config_sha256, "2" * 64)
        self.assertEqual(provider.capability_sha256, "3" * 64)

    def test_constructor_rejects_non_mapping_artifact(self) -> None:
        with self.assertRaises(OfflineProviderError):
            CampaignOfflineProvider("not-a-mapping")  # type: ignore[arg-type]

    def test_constructor_rejects_invalid_schedule(self) -> None:
        with self.assertRaises(OfflineProviderFaultScheduleError):
            CampaignOfflineProvider({"verdict": "PASS"}, schedule={0: "timeout"})
        with self.assertRaises(OfflineProviderFaultScheduleError):
            CampaignOfflineProvider({"verdict": "PASS"}, schedule={1: "boom"})


class OfflineProviderInvocationTests(unittest.TestCase):
    def test_invoke_returns_strict_json_artifact_with_reported_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            counter = str(Path(tmp) / "counter.txt")
            provider = CampaignOfflineProvider(
                {"verdict": "PASS", "reason": "ok"},
                counter_path=counter,
            )
            response = provider.invoke({"prompt": "x"})
            self.assertIsInstance(response, ProviderResponse)
            self.assertEqual(
                response.output_text,
                '{"reason":"ok","verdict":"PASS"}',
            )
            self.assertEqual(response.request_model, "deterministic-reviewer")
            self.assertEqual(response.response_model, "deterministic-reviewer")
            self.assertEqual(response.usage_status, UsageStatus.REPORTED)
            self.assertEqual(response.raw_usage["input_tokens"], 7)
            self.assertEqual(response.raw_usage["output_tokens"], 3)
            self.assertEqual(response.raw_usage["total_tokens"], 10)
            self.assertEqual(response.raw_usage["reported_cost"], "0.02")
            self.assertEqual(response.raw_usage["currency"], "USD")
            self.assertEqual(provider.call_count, 1)

    def test_call_count_is_persisted_across_instances(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            counter = str(Path(tmp) / "counter.txt")
            first = CampaignOfflineProvider({"v": 1}, counter_path=counter)
            first.invoke("a")
            second = CampaignOfflineProvider({"v": 1}, counter_path=counter)
            self.assertEqual(second.call_count, 1)
            second.invoke("b")
            self.assertEqual(second.call_count, 2)

    def test_unknown_usage_is_reported_as_unknown_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = CampaignOfflineProvider(
                {"v": 1},
                usage={"usage_status": "unknown"},
                counter_path=str(Path(tmp) / "c.txt"),
            )
            response = provider.invoke("x")
            self.assertEqual(response.usage_status, UsageStatus.UNKNOWN)

    def test_scheduled_timeout_raises_timeout_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = CampaignOfflineProvider(
                {"v": 1},
                schedule={1: "timeout"},
                counter_path=str(Path(tmp) / "c.txt"),
            )
            with self.assertRaises(OfflineProviderTimeout):
                provider.invoke("x")

    def test_scheduled_invalid_json_returns_malformed_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = CampaignOfflineProvider(
                {"v": 1},
                schedule={1: "invalid_json"},
                counter_path=str(Path(tmp) / "c.txt"),
            )
            response = provider.invoke("x")
            self.assertEqual(response.output_text, "{invalid-json")

    def test_scheduled_exception_raises_generic_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = CampaignOfflineProvider(
                {"v": 1},
                schedule={1: "exception"},
                counter_path=str(Path(tmp) / "c.txt"),
            )
            with self.assertRaises(OfflineProviderException):
                provider.invoke("x")

    def test_schedule_only_fires_on_exact_occurrence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = CampaignOfflineProvider(
                {"v": 1},
                schedule={2: "timeout"},
                counter_path=str(Path(tmp) / "c.txt"),
            )
            first = provider.invoke("x")
            self.assertEqual(first.usage_status, UsageStatus.REPORTED)
            with self.assertRaises(OfflineProviderTimeout):
                provider.invoke("x")


if __name__ == "__main__":
    unittest.main()
