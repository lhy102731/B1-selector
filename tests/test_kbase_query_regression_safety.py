from __future__ import annotations

import hashlib
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import yaml

from ag2_research.kbase.query_regression import run_catalog_regression


class KBaseQueryRegressionSafetyTests(unittest.TestCase):
    def test_post_release_shadow_suite_is_frozen_and_disjoint(self):
        root = Path(__file__).resolve().parent.parent / "ag2_research" / "kbase"
        shadow_path = root / "query_shadow_20260723.yaml"
        self.assertEqual(
            "12d62c6f0c3da41207a7c37ae02b364e97ce8721d0184ae5e483da16e9bae8ec",
            hashlib.sha256(shadow_path.read_bytes()).hexdigest(),
        )
        shadow = yaml.safe_load(shadow_path.read_text(encoding="utf-8"))
        fixed = yaml.safe_load((root / "query_regression.yaml").read_text(encoding="utf-8"))
        holdout = yaml.safe_load((root / "query_holdout.yaml").read_text(encoding="utf-8"))
        shadow_ids = {item["id"] for item in shadow["cases"]}
        prior_ids = {item["id"] for suite in (fixed, holdout) for item in suite["cases"]}

        self.assertEqual("never_used_for_model_or_fusion_selection", shadow["tuning_status"])
        self.assertEqual(10, len(shadow_ids))
        self.assertFalse(shadow_ids & prior_ids)

    def test_programmatic_regression_disables_and_restores_telemetry(self):
        suite = {
            "max_results": 5,
            "forbidden_scopes": [],
            "cases": [{
                "id": "negative",
                "intent": "concept",
                "query": "none",
                "expected": {"no_result": True},
            }],
        }
        observed = []

        def fake_search(*_args, **_kwargs):
            observed.append(os.environ.get("KBASE_TELEMETRY_DISABLED"))
            return '{"results": []}'

        os.environ.pop("KBASE_TELEMETRY_DISABLED", None)
        with patch("ag2_research.kbase.query_regression.kbase_search", fake_search):
            run_catalog_regression(suite)

        self.assertEqual(["1"], observed)
        self.assertNotIn("KBASE_TELEMETRY_DISABLED", os.environ)


if __name__ == "__main__":
    unittest.main()
