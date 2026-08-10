"""C1 manifest lint tests (RED at skeleton)."""

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


if __name__ == "__main__":
    unittest.main()
