"""Focused tests for research_automation.control_plane.rollout_c1_context.

RED-to-GREEN: must fail against the skeleton, then pass after implementation.
"""

from __future__ import annotations

import unittest

from research_automation.control_plane.rollout_c1_context import (
    DryRunContext,
    build_dry_run_context,
    estimate_tokens,
    is_data_free_prompt,
    verify_context,
)


class EstimateTokensTests(unittest.TestCase):
    def test_non_empty_text_token_estimate(self) -> None:
        self.assertEqual(estimate_tokens("abcd"), 1)
        self.assertEqual(estimate_tokens("abcde"), 2)
        self.assertEqual(estimate_tokens("abcdefgh"), 2)
        self.assertEqual(estimate_tokens("abcdefghi"), 3)

    def test_empty_text_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            estimate_tokens("")

    def test_single_character_returns_one(self) -> None:
        self.assertEqual(estimate_tokens("x"), 1)


class BuildDryRunContextTests(unittest.TestCase):
    def test_default_cycle_index_is_one(self) -> None:
        ctx = build_dry_run_context("kimi-k2.7-code")
        self.assertEqual(ctx.model, "kimi-k2.7-code")
        self.assertEqual(ctx.cycle_index, 1)
        self.assertIn("DRY_RUN_OK", ctx.prompt)
        self.assertIn("kimi-k2.7-code", ctx.prompt)

    def test_custom_cycle_index(self) -> None:
        ctx = build_dry_run_context("deepseek-chat", cycle_index=3)
        self.assertEqual(ctx.cycle_index, 3)
        self.assertIn("cycle 3", ctx.prompt)

    def test_prompt_fields_are_consistent(self) -> None:
        ctx = build_dry_run_context("doubao-seed-2.0-pro", cycle_index=2)
        expected_prompt = (
            "Control-plane C1 dry run cycle 2 for model "
            "doubao-seed-2.0-pro. Reply with exactly: DRY_RUN_OK"
        )
        self.assertEqual(ctx.prompt, expected_prompt)
        self.assertEqual(ctx.prompt_chars, len(expected_prompt))
        self.assertEqual(ctx.prompt_tokens_estimate, estimate_tokens(expected_prompt))


class IsDataFreePromptTests(unittest.TestCase):
    def test_valid_prompt_is_data_free(self) -> None:
        self.assertTrue(is_data_free_prompt("DRY_RUN_OK control plane only"))

    def test_rejects_kbase(self) -> None:
        self.assertFalse(is_data_free_prompt("Load from KBase DRY_RUN"))

    def test_rejects_csv(self) -> None:
        self.assertFalse(is_data_free_prompt("Read file.csv DRY_RUN"))

    def test_rejects_parquet(self) -> None:
        self.assertFalse(is_data_free_prompt("Read file.parquet DRY_RUN"))

    def test_rejects_strategy(self) -> None:
        self.assertFalse(is_data_free_prompt("Strategy signal DRY_RUN"))

    def test_rejects_stock(self) -> None:
        self.assertFalse(is_data_free_prompt("Stock 000001 DRY_RUN"))

    def test_rejects_data_slash(self) -> None:
        self.assertFalse(is_data_free_prompt("data/ohlc DRY_RUN"))

    def test_rejects_empty(self) -> None:
        self.assertFalse(is_data_free_prompt(""))

    def test_case_insensitive_rejection(self) -> None:
        self.assertFalse(is_data_free_prompt("DATA/CSV strategy KBASE"))


class VerifyContextTests(unittest.TestCase):
    def test_passes_for_valid_context(self) -> None:
        ctx = build_dry_run_context("glm-5.2")
        self.assertTrue(verify_context(ctx, expected_model="glm-5.2"))

    def test_fails_when_model_mismatch(self) -> None:
        ctx = build_dry_run_context("glm-5.2")
        self.assertFalse(verify_context(ctx, expected_model="minimax-m3"))

    def test_fails_when_cycle_mismatch(self) -> None:
        ctx = build_dry_run_context("minimax-m3", cycle_index=2)
        self.assertFalse(verify_context(ctx, expected_model="minimax-m3"))

    def test_fails_when_prompt_not_data_free(self) -> None:
        ctx = DryRunContext(
            model="kimi-k2.7-code",
            cycle_index=1,
            prompt="data/stock.csv strategy",
            prompt_chars=25,
            prompt_tokens_estimate=estimate_tokens("data/stock.csv strategy"),
        )
        self.assertFalse(verify_context(ctx, expected_model="kimi-k2.7-code"))

    def test_fails_when_token_estimate_mismatch(self) -> None:
        ctx = DryRunContext(
            model="deepseek-chat",
            cycle_index=1,
            prompt="DRY_RUN_OK",
            prompt_chars=11,
            prompt_tokens_estimate=999,
        )
        self.assertFalse(verify_context(ctx, expected_model="deepseek-chat"))


if __name__ == "__main__":
    unittest.main()
