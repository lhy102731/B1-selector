"""C1 dry-run driver focused tests (RED → GREEN for the driver slice).

These tests mock dependencies from other seats (context, usage, report, providers)
so they exercise ONLY the driver logic in rollout_c1.py.
"""

from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from research_automation.control_plane import rollout_c1
from research_automation.control_plane.rollout_c1_context import DryRunContext
from research_automation.control_plane.rollout_c1_usage import (
    BudgetVerdict,
    DryRunBudget,
    UsageRecord,
)
from research_automation.control_plane.rollout_providers import ProviderCallResult


# ---------------------------------------------------------------------------
# Fake helpers — stand in for other-seat modules that are still skeletons
# ---------------------------------------------------------------------------

class _FakeLedger:
    """Minimal UsageLedger fake so driver tests don't depend on usage slice."""

    def __init__(self, *, verify_passed: bool = True, verify_detail: str = "ok") -> None:
        self._records: list[UsageRecord] = []
        self._verify_passed = verify_passed
        self._verify_detail = verify_detail

    def record(self, record: UsageRecord) -> None:
        self._records.append(record)

    def records(self) -> list[UsageRecord]:
        return list(self._records)

    def total_tokens(self) -> int:
        return sum(r.total_tokens for r in self._records)

    def verify_budget(self, budget: DryRunBudget) -> BudgetVerdict:
        return BudgetVerdict(passed=self._verify_passed, detail=self._verify_detail)


def _fake_ctx(model: str, cycle_index: int = 1) -> DryRunContext:
    """Return a data-free DryRunContext fake."""
    return DryRunContext(
        model=model,
        cycle_index=cycle_index,
        prompt=f"DRY_RUN ping for {model} cycle {cycle_index}",
        prompt_chars=40,
        prompt_tokens_estimate=10,
    )


def _ok_provider(model: str, prompt: str) -> ProviderCallResult:
    """Fake provider that always succeeds."""
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class C1DriverTests(unittest.TestCase):
    """Focused tests for the C1 dry-run driver slice (rollout_c1.py)."""

    def setUp(self) -> None:
        self._now = datetime(2026, 8, 11, 12, 0, 0, tzinfo=timezone.utc)

    # ------------------------------------------------------------------
    # 1. full pass with fake provider
    # ------------------------------------------------------------------
    @patch("research_automation.control_plane.rollout_c1.serialize_report")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_report")
    @patch("research_automation.control_plane.rollout_c1.UsageLedger")
    @patch("research_automation.control_plane.rollout_c1.verify_context")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_context")
    def test_full_pass_with_fake_provider(
        self,
        mock_build_ctx: MagicMock,
        mock_verify: MagicMock,
        mock_ledger_cls: MagicMock,
        mock_build_report: MagicMock,
        mock_serialize: MagicMock,
    ) -> None:
        """All five models succeed → pass is True, all verification flags True."""
        mock_build_ctx.side_effect = _fake_ctx
        mock_verify.return_value = True
        mock_ledger_cls.side_effect = lambda: _FakeLedger()
        mock_build_report.return_value = {"schema": "C1_DRY_RUN_REPORT_V1", "pass": True}
        mock_serialize.return_value = '{"schema":"C1_DRY_RUN_REPORT_V1"}'

        outcome = rollout_c1.run_c1_dry_run(
            provider_override=_ok_provider,
            budget=DryRunBudget(),
            now=self._now,
        )
        payload = outcome.to_payload()

        # Structural assertions
        self.assertTrue(payload["pass"])
        self.assertTrue(payload["roster_verified"])
        self.assertTrue(payload["usage_verified"])
        self.assertTrue(payload["context_verified"])
        self.assertTrue(payload["budget_verified"])
        self.assertTrue(payload["no_learning_commit"])
        self.assertTrue(payload["no_real_campaign_or_holdout"])

        # Identity fields
        self.assertEqual(payload["attempt_id"], "c1-attempt-001")
        self.assertEqual(payload["plan_version"], "V3.4.2-P0R2")

        # Cardinality
        self.assertEqual(len(payload["models"]), 5)
        self.assertEqual(len(payload["usage_records"]), 5)
        self.assertEqual(len(payload["failures"]), 0)

        # Timestamps are ISO Z
        for key in ("started_at", "completed_at"):
            self.assertIn("2026-08-11", payload[key])

        # Usage record shape
        for rec in payload["usage_records"]:
            self.assertIn("model", rec)
            self.assertIn("status", rec)
            self.assertIn("input_tokens", rec)
            self.assertIn("output_tokens", rec)
            self.assertIn("total_tokens", rec)
            self.assertEqual(rec["status"], "ok")
            self.assertEqual(rec["total_tokens"], rec["input_tokens"] + rec["output_tokens"])

        # Digest is 64 hex chars
        digest = payload["final_state_digest"]
        self.assertEqual(len(digest), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in digest))

    # ------------------------------------------------------------------
    # 2. digest stability across two runs
    # ------------------------------------------------------------------
    @patch("research_automation.control_plane.rollout_c1.serialize_report")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_report")
    @patch("research_automation.control_plane.rollout_c1.UsageLedger")
    @patch("research_automation.control_plane.rollout_c1.verify_context")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_context")
    def test_digest_stable_across_two_runs(
        self,
        mock_build_ctx: MagicMock,
        mock_verify: MagicMock,
        mock_ledger_cls: MagicMock,
        mock_build_report: MagicMock,
        mock_serialize: MagicMock,
    ) -> None:
        """Two runs with same now produce identical final_state_digest."""
        mock_build_ctx.side_effect = _fake_ctx
        mock_verify.return_value = True
        mock_ledger_cls.side_effect = lambda: _FakeLedger()
        mock_build_report.return_value = {"schema": "C1_DRY_RUN_REPORT_V1"}
        mock_serialize.return_value = '{"schema":"C1_DRY_RUN_REPORT_V1"}'

        first = rollout_c1.run_c1_dry_run(provider_override=_ok_provider, now=self._now)
        second = rollout_c1.run_c1_dry_run(provider_override=_ok_provider, now=self._now)

        self.assertEqual(
            first.to_payload()["final_state_digest"],
            second.to_payload()["final_state_digest"],
            "Same inputs + same timestamp must produce identical digest",
        )

    # ------------------------------------------------------------------
    # 3. budget failure makes pass False
    # ------------------------------------------------------------------
    @patch("research_automation.control_plane.rollout_c1.serialize_report")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_report")
    @patch("research_automation.control_plane.rollout_c1.UsageLedger")
    @patch("research_automation.control_plane.rollout_c1.verify_context")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_context")
    def test_budget_failure_makes_pass_false(
        self,
        mock_build_ctx: MagicMock,
        mock_verify: MagicMock,
        mock_ledger_cls: MagicMock,
        mock_build_report: MagicMock,
        mock_serialize: MagicMock,
    ) -> None:
        """When the ledger verdict fails, pass must be False."""
        mock_build_ctx.side_effect = _fake_ctx
        mock_verify.return_value = True
        mock_ledger_cls.side_effect = lambda: _FakeLedger(
            verify_passed=False, verify_detail="token budget exceeded"
        )
        mock_build_report.return_value = {"schema": "C1_DRY_RUN_REPORT_V1"}
        mock_serialize.return_value = '{"schema":"C1_DRY_RUN_REPORT_V1"}'

        outcome = rollout_c1.run_c1_dry_run(provider_override=_ok_provider, now=self._now)
        payload = outcome.to_payload()

        self.assertFalse(payload["pass"])
        self.assertFalse(payload["budget_verified"])
        self.assertEqual(payload["budget_detail"], "token budget exceeded")

    # ------------------------------------------------------------------
    # 4. partial provider failure
    # ------------------------------------------------------------------
    @patch("research_automation.control_plane.rollout_c1.serialize_report")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_report")
    @patch("research_automation.control_plane.rollout_c1.UsageLedger")
    @patch("research_automation.control_plane.rollout_c1.verify_context")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_context")
    def test_partial_provider_failure(
        self,
        mock_build_ctx: MagicMock,
        mock_verify: MagicMock,
        mock_ledger_cls: MagicMock,
        mock_build_report: MagicMock,
        mock_serialize: MagicMock,
    ) -> None:
        """Only the failed model appears in failures; pass is False."""
        mock_build_ctx.side_effect = _fake_ctx
        mock_verify.return_value = True
        mock_ledger_cls.side_effect = lambda: _FakeLedger()
        mock_build_report.return_value = {"schema": "C1_DRY_RUN_REPORT_V1"}
        mock_serialize.return_value = '{"schema":"C1_DRY_RUN_REPORT_V1"}'

        call_count = [0]

        def mixed_provider(model: str, prompt: str) -> ProviderCallResult:
            call_count[0] += 1
            if call_count[0] == 1:
                # First model (doubao) fails with HTTP 429
                return ProviderCallResult(
                    model, "http_429", "", 0, 0, 0, 5, "quota exhausted"
                )
            return ProviderCallResult(
                model, "ok", "ok", 8, 4, 12, 10, "ok"
            )

        outcome = rollout_c1.run_c1_dry_run(provider_override=mixed_provider, now=self._now)
        payload = outcome.to_payload()

        self.assertFalse(payload["pass"])
        self.assertEqual(len(payload["failures"]), 1)
        self.assertIn("doubao-seed-2.0-pro", payload["failures"])
        self.assertFalse(payload["usage_verified"])

        # The other 4 models still have ok usage records
        ok_records = [r for r in payload["usage_records"] if r["status"] == "ok"]
        self.assertEqual(len(ok_records), 4)

    # ------------------------------------------------------------------
    # 5. roster mismatch
    # ------------------------------------------------------------------
    @patch("research_automation.control_plane.rollout_c1.serialize_report")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_report")
    @patch("research_automation.control_plane.rollout_c1.UsageLedger")
    @patch("research_automation.control_plane.rollout_c1.verify_context")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_context")
    def test_roster_mismatch_makes_roster_verified_false(
        self,
        mock_build_ctx: MagicMock,
        mock_verify: MagicMock,
        mock_ledger_cls: MagicMock,
        mock_build_report: MagicMock,
        mock_serialize: MagicMock,
    ) -> None:
        """Custom model list != DEFAULT_MODELS → roster_verified False."""
        mock_build_ctx.side_effect = _fake_ctx
        mock_verify.return_value = True
        mock_ledger_cls.side_effect = lambda: _FakeLedger()
        mock_build_report.return_value = {"schema": "C1_DRY_RUN_REPORT_V1"}
        mock_serialize.return_value = '{"schema":"C1_DRY_RUN_REPORT_V1"}'

        custom_models = ["doubao-seed-2.0-pro", "glm-5.2"]

        outcome = rollout_c1.run_c1_dry_run(
            models=custom_models,
            provider_override=_ok_provider,
            now=self._now,
        )
        payload = outcome.to_payload()

        self.assertFalse(payload["roster_verified"])
        self.assertFalse(payload["pass"])
        self.assertEqual(payload["models"], custom_models)

    # ------------------------------------------------------------------
    # 6. deduplication preserves order
    # ------------------------------------------------------------------
    @patch("research_automation.control_plane.rollout_c1.serialize_report")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_report")
    @patch("research_automation.control_plane.rollout_c1.UsageLedger")
    @patch("research_automation.control_plane.rollout_c1.verify_context")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_context")
    def test_duplicate_models_are_deduplicated_preserving_order(
        self,
        mock_build_ctx: MagicMock,
        mock_verify: MagicMock,
        mock_ledger_cls: MagicMock,
        mock_build_report: MagicMock,
        mock_serialize: MagicMock,
    ) -> None:
        """Duplicate models in the input list are collapsed preserving first-seen order."""
        mock_build_ctx.side_effect = _fake_ctx
        mock_verify.return_value = True
        mock_ledger_cls.side_effect = lambda: _FakeLedger()
        mock_build_report.return_value = {"schema": "C1_DRY_RUN_REPORT_V1"}
        mock_serialize.return_value = '{"schema":"C1_DRY_RUN_REPORT_V1"}'

        dup_models = ["glm-5.2", "doubao-seed-2.0-pro", "glm-5.2", "kimi-k2.7-code"]

        outcome = rollout_c1.run_c1_dry_run(
            models=dup_models,
            provider_override=_ok_provider,
            now=self._now,
        )
        payload = outcome.to_payload()

        # Order preserved, duplicates removed: glm, doubao, kimi
        self.assertEqual(
            payload["models"],
            ["glm-5.2", "doubao-seed-2.0-pro", "kimi-k2.7-code"],
        )
        self.assertEqual(len(payload["usage_records"]), 3)

    # ------------------------------------------------------------------
    # 7. cycles < 1 raises ValueError
    # ------------------------------------------------------------------
    def test_cycles_less_than_1_raises_value_error(self) -> None:
        """cycles=0 must raise ValueError immediately."""
        with self.assertRaises(ValueError):
            rollout_c1.run_c1_dry_run(cycles=0)

    # ------------------------------------------------------------------
    # 8. max_tokens <= 0 raises ValueError
    # ------------------------------------------------------------------
    def test_max_tokens_zero_or_negative_raises_value_error(self) -> None:
        """max_tokens=0 and max_tokens=-1 must raise ValueError."""
        with self.assertRaises(ValueError):
            rollout_c1.run_c1_dry_run(max_tokens=0)
        with self.assertRaises(ValueError):
            rollout_c1.run_c1_dry_run(max_tokens=-1)

    # ------------------------------------------------------------------
    # 9. serialize_outcome contains C1_DRY_RUN_REPORT_V1
    # ------------------------------------------------------------------
    @patch("research_automation.control_plane.rollout_c1.serialize_report")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_report")
    def test_serialize_outcome_contains_c1_report_v1(
        self,
        mock_build_report: MagicMock,
        mock_serialize: MagicMock,
    ) -> None:
        """serialize_outcome delegates to report builders and the result has the schema marker."""
        mock_build_report.return_value = {
            "schema": "C1_DRY_RUN_REPORT_V1",
            "data": "test",
        }
        mock_serialize.return_value = json.dumps(
            {"schema": "C1_DRY_RUN_REPORT_V1", "data": "test"},
            ensure_ascii=False,
            sort_keys=True,
        )

        outcome = rollout_c1.DryRunOutcome(
            attempt_id="c1-attempt-001",
            plan_version="V3.4.2-P0R2",
            models=("doubao-seed-2.0-pro", "glm-5.2"),
            started_at="2026-08-11T12:00:00Z",
            completed_at="2026-08-11T12:00:01Z",
            usage_records=(),
            roster_verified=True,
            usage_verified=True,
            context_verified=True,
            budget_verified=True,
            budget_detail="ok",
            no_learning_commit=True,
            no_real_campaign_or_holdout=True,
            failures=(),
            pass_=True,
            final_state_digest="abc123",
        )

        result = rollout_c1.serialize_outcome(outcome)

        self.assertIn("C1_DRY_RUN_REPORT_V1", result)
        mock_build_report.assert_called_once()
        mock_serialize.assert_called_once()

    # ------------------------------------------------------------------
    # 10. context_verified False when any verify_context returns False
    # ------------------------------------------------------------------
    @patch("research_automation.control_plane.rollout_c1.serialize_report")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_report")
    @patch("research_automation.control_plane.rollout_c1.UsageLedger")
    @patch("research_automation.control_plane.rollout_c1.verify_context")
    @patch("research_automation.control_plane.rollout_c1.build_dry_run_context")
    def test_context_verified_false_when_any_verify_fails(
        self,
        mock_build_ctx: MagicMock,
        mock_verify: MagicMock,
        mock_ledger_cls: MagicMock,
        mock_build_report: MagicMock,
        mock_serialize: MagicMock,
    ) -> None:
        """If any verify_context call returns False, context_verified must be False."""
        mock_build_ctx.side_effect = _fake_ctx
        # First call returns False, rest return True
        mock_verify.side_effect = [False] + [True] * 99
        mock_ledger_cls.side_effect = lambda: _FakeLedger()
        mock_build_report.return_value = {"schema": "C1_DRY_RUN_REPORT_V1"}
        mock_serialize.return_value = '{"schema":"C1_DRY_RUN_REPORT_V1"}'

        outcome = rollout_c1.run_c1_dry_run(provider_override=_ok_provider, now=self._now)
        payload = outcome.to_payload()

        self.assertFalse(payload["context_verified"])
        self.assertFalse(payload["pass"])


if __name__ == "__main__":
    unittest.main()
