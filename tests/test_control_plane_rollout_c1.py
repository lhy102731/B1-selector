"""C1 integration contract (RED at skeleton; GREEN after all slices land)."""

from __future__ import annotations

import unittest

from research_automation.control_plane import rollout_c1
from research_automation.control_plane.rollout_c1_usage import DryRunBudget
from research_automation.control_plane.rollout_providers import ProviderCallResult


def _fake_provider(model: str, prompt: str) -> ProviderCallResult:
    assert "DRY_RUN" in prompt
    return ProviderCallResult(
        model=model,
        status="ok",
        text="DRY_RUN_OK",
        input_tokens=8,
        output_tokens=4,
        total_tokens=12,
        wall_time_ms=10,
        detail="fake",
    )


class C1IntegrationTests(unittest.TestCase):
    def test_full_dry_run_passes_with_fake_provider(self) -> None:
        outcome = rollout_c1.run_c1_dry_run(
            provider_override=_fake_provider,
            budget=DryRunBudget(),
        )
        payload = outcome.to_payload()
        self.assertTrue(payload["pass"])
        self.assertTrue(payload["roster_verified"])
        self.assertTrue(payload["usage_verified"])
        self.assertTrue(payload["context_verified"])
        self.assertTrue(payload["budget_verified"])
        self.assertTrue(payload["no_learning_commit"])
        self.assertTrue(payload["no_real_campaign_or_holdout"])
        self.assertEqual(len(payload["usage_records"]), 5)
        self.assertEqual(len(payload["failures"]), 0)

    def test_report_round_trips_and_digest_is_stable(self) -> None:
        first = rollout_c1.run_c1_dry_run(provider_override=_fake_provider)
        second = rollout_c1.run_c1_dry_run(provider_override=_fake_provider)
        self.assertEqual(first.to_payload()["final_state_digest"], second.to_payload()["final_state_digest"])
        serialized = rollout_c1.serialize_outcome(first)
        self.assertIn("C1_DRY_RUN_REPORT_V1", serialized)

    def test_failure_records_are_not_hidden(self) -> None:
        def failing(model: str, prompt: str) -> ProviderCallResult:
            return ProviderCallResult(model, "http_429", "", 0, 0, 0, 5, "quota")

        outcome = rollout_c1.run_c1_dry_run(provider_override=failing)
        self.assertFalse(outcome.to_payload()["pass"])
        self.assertEqual(len(outcome.to_payload()["failures"]), 5)


if __name__ == "__main__":
    unittest.main()
