from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

from ag2_research.project_state import compile_project_state
from ag2_research.research_gap import build_research_gap_request


def _write_yaml(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(value, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


class ProjectStateAndGapTests(unittest.TestCase):
    def _project(self, root: Path) -> Path:
        _write_yaml(root / "snapshot_brick.yaml", {"snapshot": {
            "current_champion": {"name": "Brick V2 Top2"},
            "next_priority": {"primary": "executable NAV boundary validation"},
            "frozen_directions": ["production script edits"],
            "rejected_directions": ["blind industry cap"],
        }})
        _write_yaml(root / "handoff_brick_v1.yaml", {"handoff": {
            "active_focus": "rank3 and Top5 boundary decisions",
            "do_not_repeat": [{"item": "free-float factor without coverage"}],
            "escalation_conditions": [{"condition": "production promotion requested"}],
        }})
        _write_yaml(root / "registry_brick_v2.yaml", {"registry": {"experiments": [
            {"id": "failed-1", "title": "blind max2 industry cap", "status": "FAILED"},
            {"id": "open-1", "title": "executable NAV objective", "status": "OPEN"},
        ]}})
        _write_yaml(root / "project_brick_v2.yaml", {"project": {
            "name": "Brick V2", "boundary": "pre-09:25 ranking research",
        }})
        _write_yaml(root / "brick_memory.yaml", {"lessons": ["SQNAV is not executable NAV"]})

        kb = root / "ag2_research/knowledge_base/brick"
        _write_yaml(kb / "manifest.yaml", {
            "kb_version": "0.1.0",
            "subject": "brick",
            "headline": {
                "primary_open_directions": [
                    "execution-aware ranking objectives",
                    "pre-09:25 entry-open interaction features",
                ]
            },
            "artifacts": {
                "archived_factors": "archived_factors.json",
                "forbidden_directions": "forbidden_directions.json",
            },
        })
        _write_json(kb / "archived_factors.json", [{
            "factor": "free_float_ratio", "status": "data_blocked",
        }])
        _write_json(kb / "forbidden_directions.json", [{
            "direction": "market timing rescue", "reason": "prior negative evidence",
        }])

        research = root / "research_state/brick"
        research.mkdir(parents=True)
        (research / "brick_research_summary_20260711.md").write_text(
            "# Summary\n\n## Current Priority List\n\n"
            "1. Validate volume boundary tie-breakers with executable NAV.\n"
            "2. Test close-position interaction with ablation.\n",
            encoding="utf-8",
        )
        factors = research / "factor_library"
        factors.mkdir()
        (factors / "brick_effective_factor_library.md").write_text(
            "# Factor Library\n\nvol_authenticity_path_smoothness_10d: research_only\n",
            encoding="utf-8",
        )
        (root / "backtest_brick_v2.py").write_text("# production\n", encoding="utf-8")
        (root / "backtest_brick_v2_research.py").write_text("# research\n", encoding="utf-8")
        return root

    def test_compiler_builds_bounded_hashed_project_state(self) -> None:
        semantic = {
            "status": "READY",
            "bundle_fingerprint": "s" * 64,
            "catalog_version": "catalog-v1",
            "semantic_source_fingerprint": "semantic-v1",
            "models": {"embedding": "bge-m3", "reranker": "bge-reranker-v2-m3"},
            "issues": [],
        }
        with tempfile.TemporaryDirectory() as directory, patch(
            "ag2_research.project_state.inspect_semantic_release_bundle",
            return_value=semantic,
        ):
            packet = compile_project_state(
                "brick",
                "优化当前选股系统",
                project_root=self._project(Path(directory)),
                vault_path=Path(directory) / "vault",
                generated_at="2026-07-23T00:00:00Z",
            )

        self.assertEqual("ag2.project_state_packet.v1", packet["schema_version"])
        self.assertEqual("Brick V2 Top2", packet["memory"]["snapshot"]["current_champion"]["name"])
        self.assertEqual(2, packet["memory"]["registry"]["experiment_count"])
        self.assertEqual("0.1.0", packet["project_kb"]["kb_version"])
        self.assertEqual("READY", packet["kbase_release"]["status"])
        self.assertEqual(64, len(packet["project_state_fingerprint"]))
        self.assertTrue(packet["artifact_bindings"])

    def test_gap_planner_separates_open_failed_excluded_and_unseen_scan(self) -> None:
        semantic = {
            "status": "READY",
            "bundle_fingerprint": "s" * 64,
            "catalog_version": "catalog-v1",
            "semantic_source_fingerprint": "semantic-v1",
            "models": {"embedding": "bge-m3", "reranker": "bge-reranker-v2-m3"},
            "issues": [],
        }
        with tempfile.TemporaryDirectory() as directory, patch(
            "ag2_research.project_state.inspect_semantic_release_bundle",
            return_value=semantic,
        ):
            packet = compile_project_state(
                "brick", "优化当前选股系统",
                project_root=self._project(Path(directory)),
                vault_path=Path(directory) / "vault",
                generated_at="2026-07-23T00:00:00Z",
            )
            request = build_research_gap_request(packet)

        self.assertEqual("ag2.research_gap_request.v1", request["schema_version"])
        statuses = {item["status"] for item in request["project_coverage"]}
        self.assertIn("covered_shallow", statuses)
        self.assertIn("covered_failed", statuses)
        self.assertIn("excluded", statuses)
        self.assertTrue(any(item["status"] == "unseen_scan_required" for item in request["candidate_gaps"]))
        self.assertIn("factor", request["kbase_boundary"]["forbidden_outputs"])
        self.assertEqual(64, len(request["request_id"]))


if __name__ == "__main__":
    unittest.main()
