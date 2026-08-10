"""C1 canonical dry-run report tests (RED at skeleton; GREEN after impl)."""

from __future__ import annotations

import json
import os
import sys
import unittest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.normpath(os.path.join(THIS_DIR, "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from research_automation.control_plane import rollout_c1_report  # noqa: E402


def _realistic_payload() -> dict:
    return {
        "attempt_id": "c1-attempt-001",
        "plan_version": "V3.4.2-P0R2",
        "models": [
            "doubao-seed-2.0-pro",
            "glm-5.2",
            "kimi-k2.7-code",
            "minimax-m3",
            "deepseek-chat",
        ],
        "started_at": "2026-08-11T10:00:00+00:00",
        "completed_at": "2026-08-11T10:01:00+00:00",
        "usage_records": [
            {
                "model": "doubao-seed-2.0-pro",
                "status": "ok",
                "input_tokens": 8,
                "output_tokens": 4,
                "total_tokens": 12,
            },
            {
                "model": "glm-5.2",
                "status": "ok",
                "input_tokens": 8,
                "output_tokens": 4,
                "total_tokens": 12,
            },
            {
                "model": "kimi-k2.7-code",
                "status": "ok",
                "input_tokens": 8,
                "output_tokens": 4,
                "total_tokens": 12,
            },
            {
                "model": "minimax-m3",
                "status": "ok",
                "input_tokens": 8,
                "output_tokens": 4,
                "total_tokens": 12,
            },
            {
                "model": "deepseek-chat",
                "status": "ok",
                "input_tokens": 8,
                "output_tokens": 4,
                "total_tokens": 12,
            },
        ],
        "roster_verified": True,
        "usage_verified": True,
        "context_verified": True,
        "budget_verified": True,
        "budget_detail": "totals=60; under cap 4096; per-model=12; under cap 2048",
        "no_learning_commit": True,
        "no_real_campaign_or_holdout": True,
        "failures": [],
        "pass": True,
        "final_state_digest": "deadbeef" * 8,
    }


class BuildDryRunReportTests(unittest.TestCase):
    def test_build_returns_dict_with_schema_version(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        self.assertIsInstance(report, dict)
        self.assertEqual(report["schema_version"], "C1_DRY_RUN_REPORT_V1")

    def test_build_carries_attempt_id_and_plan_version(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        self.assertEqual(report["attempt_id"], "c1-attempt-001")
        self.assertEqual(report["plan_version"], "V3.4.2-P0R2")

    def test_build_models_is_list(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        self.assertIsInstance(report["models"], list)
        self.assertEqual(len(report["models"]), 5)

    def test_build_usage_records_is_list(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        self.assertIsInstance(report["usage_records"], list)
        self.assertEqual(len(report["usage_records"]), 5)

    def test_build_failures_is_list(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        self.assertIsInstance(report["failures"], list)
        self.assertEqual(report["failures"], [])

    def test_acceptance_section_has_exactly_required_keys(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        acceptance = report["acceptance"]
        self.assertIsInstance(acceptance, dict)
        expected = {
            "real_llm_dry_run",
            "roster_verified",
            "usage_verified",
            "context_verified",
            "budget_verified",
            "no_learning_commit",
            "no_real_campaign_or_holdout",
            "failures_recorded_not_hidden",
            "report_canonical",
            "evidence_append_only",
        }
        self.assertEqual(set(acceptance.keys()), expected)

    def test_acceptance_passes_when_payload_clean(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        acceptance = report["acceptance"]
        self.assertTrue(acceptance["real_llm_dry_run"])
        self.assertTrue(acceptance["report_canonical"])
        self.assertTrue(acceptance["evidence_append_only"])
        self.assertTrue(acceptance["failures_recorded_not_hidden"])
        for key in (
            "roster_verified",
            "usage_verified",
            "context_verified",
            "budget_verified",
            "no_learning_commit",
            "no_real_campaign_or_holdout",
        ):
            self.assertTrue(acceptance[key])

    def test_failures_recorded_not_hidden_when_failures_present(self) -> None:
        payload = _realistic_payload()
        payload["failures"] = ["doubao-seed-2.0-pro:http_429"]
        payload["pass"] = False
        payload["roster_verified"] = False
        payload["usage_verified"] = False
        payload["budget_verified"] = False
        report = rollout_c1_report.build_dry_run_report(payload)
        self.assertEqual(report["failures"], ["doubao-seed-2.0-pro:http_429"])
        # failures_recorded_not_hidden is the failure list itself (truthy)
        self.assertEqual(
            report["acceptance"]["failures_recorded_not_hidden"],
            ["doubao-seed-2.0-pro:http_429"],
        )
        self.assertFalse(report["pass"])

    def test_build_is_deterministic_for_same_payload(self) -> None:
        payload = _realistic_payload()
        first = rollout_c1_report.build_dry_run_report(payload)
        second = rollout_c1_report.build_dry_run_report(payload)
        self.assertEqual(first, second)

    def test_final_state_digest_is_carried_through(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        self.assertEqual(report["final_state_digest"], "deadbeef" * 8)

    def test_top_level_fields_present(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        for field in (
            "schema_version",
            "attempt_id",
            "plan_version",
            "models",
            "started_at",
            "completed_at",
            "usage_records",
            "roster_verified",
            "usage_verified",
            "context_verified",
            "budget_verified",
            "budget_detail",
            "no_learning_commit",
            "no_real_campaign_or_holdout",
            "failures",
            "pass",
            "final_state_digest",
            "acceptance",
        ):
            self.assertIn(field, report, f"missing top-level field {field!r}")


class SerializeReportTests(unittest.TestCase):
    def test_serialize_returns_string(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        text = rollout_c1_report.serialize_report(report)
        self.assertIsInstance(text, str)

    def test_serialize_is_canonical_compact_sorted(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        text = rollout_c1_report.serialize_report(report)
        # compact: no extra spaces around separators
        self.assertNotIn(": ", text)
        self.assertNotIn(", ", text)
        # sorted: "acceptance" comes before "attempt_id" alphabetically
        self.assertLess(text.index('"acceptance"'), text.index('"attempt_id"'))

    def test_serialize_round_trips_through_json_loads(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        text = rollout_c1_report.serialize_report(report)
        loaded = json.loads(text)
        self.assertEqual(loaded["schema_version"], "C1_DRY_RUN_REPORT_V1")
        self.assertEqual(loaded["attempt_id"], "c1-attempt-001")
        self.assertEqual(loaded["plan_version"], "V3.4.2-P0R2")
        self.assertEqual(loaded["final_state_digest"], "deadbeef" * 8)
        self.assertEqual(loaded["acceptance"]["real_llm_dry_run"], True)
        self.assertEqual(loaded["acceptance"]["report_canonical"], True)
        self.assertEqual(len(loaded["models"]), 5)
        self.assertEqual(len(loaded["usage_records"]), 5)
        self.assertEqual(loaded["failures"], [])

    def test_serialize_deterministic_for_same_report(self) -> None:
        report = rollout_c1_report.build_dry_run_report(_realistic_payload())
        first = rollout_c1_report.serialize_report(report)
        second = rollout_c1_report.serialize_report(report)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
