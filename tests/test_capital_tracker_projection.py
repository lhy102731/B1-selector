"""P6R2 T5: CapitalTracker analytics-only projection tests.

CapitalTracker must be a pure analytics projection: importing it creates no
state, write APIs fail closed, aggregate_to_json never persists, and every
projection function returns data without touching the filesystem.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import research_automation.capital_tracker as ct


class CapitalTrackerAnalyticsOnlyTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        self.subject = "t5-projection-test"
        state_patch = mock.patch.object(
            ct, "_state_dir", lambda subject: self.tmp_path / subject
        )
        state_patch.start()
        self.addCleanup(state_patch.stop)
        self.addCleanup(self._tmp.cleanup)

    def test_load_missing_subject_returns_empty_without_writes(self):
        record = ct.load(self.subject)
        self.assertEqual(record["schema_version"], "1.0")
        self.assertEqual(record["subject"], self.subject)
        self.assertEqual(record["total_experiments"], 0)
        self.assertFalse((self.tmp_path / self.subject).exists())

    def test_write_apis_fail_closed(self):
        entry = {"hypothesis": "regime split", "params": {}}
        with self.assertRaises(RuntimeError):
            ct.record_experiment(self.subject, "c1", 1, entry, agents_used=["a1"])
        with self.assertRaises(RuntimeError):
            ct.record_round(self.subject, "c1", 1)
        with self.assertRaises(RuntimeError):
            ct.save(self.subject, ct._empty_record(self.subject))
        with self.assertRaises(RuntimeError):
            ct._append_event(self.subject, {"cycle_id": "c1"})
        self.assertFalse((self.tmp_path / self.subject).exists())

    def test_aggregate_to_json_is_pure(self):
        state = self.tmp_path / self.subject
        state.mkdir(parents=True, exist_ok=True)
        events = [
            {"cycle_id": "c1", "channel": "factor", "agents": ["a1"],
             "llm_profiles": ["deepseekv4"], "info_gain": 3},
            {"cycle_id": "c2", "channel": "dimension", "agents": ["a2"],
             "llm_profiles": ["glm51"], "info_gain": 1},
        ]
        (state / "capital_events.jsonl").write_text(
            "\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8"
        )
        out = ct.aggregate_to_json(self.subject)
        self.assertEqual(out["as_of_cycles_seen"], 2)
        self.assertEqual(out["as_of_info_gain_total"], 4)
        self.assertEqual(len(out["agents"]), 2)
        self.assertEqual(len(out["llm_profiles"]), 2)
        self.assertEqual(len(out["channels"]), 2)
        self.assertFalse((state / "agent_performance.json").exists())

    def test_compute_metrics_is_pure_projection(self):
        record = ct._empty_record(self.subject)
        record["channel_usage"]["factor"]["experiments"] = 8
        record["channel_usage"]["factor"]["tokens"] = 24000
        record["channel_usage"]["factor"]["info_gain_total"] = 16
        record["channel_usage"]["dimension"]["experiments"] = 2
        record["research_return"]["factor"]["information_gain"] = 16
        metrics = ct.compute_metrics(self.subject, record)
        factor = metrics["per_channel"]["factor"]
        self.assertEqual(factor["capital_share"], 0.8)
        self.assertEqual(factor["knowledge_yield"], 2.0)
        self.assertIn("rolling_20_cycle_concentration", metrics)
        self.assertFalse((self.tmp_path / self.subject).exists())

    def test_summary_and_violation_projection_read_only(self):
        summary = ct.summary_for_director(self.subject)
        self.assertEqual(summary["total_experiments"], 0)
        self.assertIsNone(ct.check_concentration_violation(self.subject))
        self.assertIn("per_channel", summary)
        self.assertFalse((self.tmp_path / self.subject).exists())

    def test_category_spend_estimate_returns_projection(self):
        state = self.tmp_path / self.subject
        state.mkdir(parents=True, exist_ok=True)
        (state / "capital_events.jsonl").write_text(
            json.dumps({"cycle_id": "c1", "channel": "factor",
                        "llm_profiles": ["deepseekv4"]}) + "\n",
            encoding="utf-8",
        )
        out = ct.category_spend_estimate(self.subject)
        self.assertIn("spend_est", out)
        self.assertGreaterEqual(out["spend_est"]["factor"], 0.0)
        self.assertEqual(out["subject"], self.subject)
        self.assertFalse((state / "capital_tracker.yaml").exists())

    def test_import_module_has_no_subprocess_or_runner_dependency(self):
        self.assertNotIn("subprocess", vars(ct))
        self.assertNotIn("run_research_cycle", vars(ct))


if __name__ == "__main__":
    unittest.main()
