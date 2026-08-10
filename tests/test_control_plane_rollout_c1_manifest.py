"""C1 manifest lint tests (RED at skeleton, GREEN after C0-POOL-003 lands)."""

from __future__ import annotations

import unittest

from research_automation.control_plane import rollout_c1_manifest


class C1ManifestLintTests(unittest.TestCase):
    def test_aligned_manifest_and_report_produce_no_problems(self) -> None:
        manifest = {"acceptance_matrix": {"a": True, "b": True}}
        report = {"acceptance": {"a": True, "b": True}}
        self.assertEqual(rollout_c1_manifest.lint_acceptance_mapping(manifest, report), [])

    def test_missing_acceptance_key_is_reported(self) -> None:
        manifest = {"acceptance_matrix": {"a": True, "b": True}}
        report = {"acceptance": {"a": True}}
        problems = rollout_c1_manifest.lint_acceptance_mapping(manifest, report)
        self.assertEqual(len(problems), 1)
        self.assertIn("b", problems[0])

    def test_report_shape_rejects_missing_schema(self) -> None:
        problems = rollout_c1_manifest.validate_report_shape({"acceptance": {}})
        self.assertTrue(any("schema" in problem for problem in problems))

    def test_unexpected_acceptance_key_is_reported(self) -> None:
        manifest = {"acceptance_matrix": {"a": True}}
        report = {"acceptance": {"a": True, "zzz": False}}
        problems = rollout_c1_manifest.lint_acceptance_mapping(manifest, report)
        self.assertEqual(len(problems), 1)
        self.assertIn("zzz", problems[0])

    def _well_formed_report(self) -> dict:
        return {
            "schema_version": "C1_DRY_RUN_REPORT_V1",
            "attempt_id": "c1-attempt-001",
            "models": ["doubao-seed-2.0-pro", "deepseek-chat"],
            "pass": True,
            "final_state_digest": "a" * 64,
            "usage_records": [
                {
                    "model": "deepseek-chat",
                    "status": "ok",
                    "input_tokens": 8,
                    "output_tokens": 4,
                    "total_tokens": 12,
                }
            ],
            "acceptance": {"real_llm_dry_run": True},
        }

    def test_validate_report_shape_accepts_well_formed_report(self) -> None:
        self.assertEqual(
            rollout_c1_manifest.validate_report_shape(self._well_formed_report()),
            [],
        )

    def test_validate_report_shape_reports_multiple_problems(self) -> None:
        problems = rollout_c1_manifest.validate_report_shape(
            {
                "schema_version": "C1_DRY_RUN_REPORT_V0",
                "final_state_digest": "A" * 64,
                "usage_records": [{"model": "deepseek-chat"}],
            }
        )
        joined = " | ".join(problems)
        for marker in (
            "schema",
            "attempt_id",
            "models",
            "pass",
            "final_state_digest",
            "usage_records",
            "input_tokens",
            "acceptance",
        ):
            self.assertIn(marker, joined)


if __name__ == "__main__":
    unittest.main()
