from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ag2_research.kbase.coverage import build_navigation_coverage


class KBaseCoverageTests(unittest.TestCase):
    def _release(self, root: Path, entries: list[dict]) -> Path:
        vault = root / "KBase"
        release = vault / "wiki/outputs/manifests/ag2-kbase/current"
        release.mkdir(parents=True)
        (release / "manifest.json").write_text(json.dumps({
            "catalog_schema_version": 1, "catalog_version": "test", "counts": {}
        }), encoding="utf-8")
        (release / "facets.json").write_text("{}", encoding="utf-8")
        (release / "catalog.jsonl").write_text(
            "".join(json.dumps(item) + "\n" for item in entries), encoding="utf-8"
        )
        return vault

    def test_reports_navigation_layers_and_prioritised_gaps(self) -> None:
        map_entry = {"source_id": "map:a", "object_type": "map", "title": "Map"}
        family = {"source_id": "family:a", "object_type": "family", "title": "A", "parent_ids": ["map:a"]}
        healthy = {
            "source_id": "a" * 64, "object_type": "source_packet", "title": "healthy",
            "family_id": "family:a", "parent_ids": ["family:a"], "date_start": "2026-01-02",
            "available_layers": ["summary", "statements", "evidence", "raw"],
            "paths": {"packet": "packets/a.json", "raw": "raw/a.md"}, "warnings": [],
        }
        orphan = {
            "source_id": "b" * 64, "object_type": "source_packet", "title": "orphan",
            "family_id": None, "parent_ids": [], "date_start": None,
            "available_layers": ["summary"], "paths": {"packet": "packets/b.json"},
            "warnings": ["raw_path_unresolved"],
        }
        with tempfile.TemporaryDirectory() as tmp:
            vault = self._release(Path(tmp), [map_entry, family, healthy, orphan])
            (vault / "packets").mkdir()
            (vault / "raw").mkdir()
            (vault / "packets/a.json").write_text(json.dumps({"record": {
                "summary": "summary", "methods": [{"text": "statement", "evidence_anchor": "L1"}]
            }}), encoding="utf-8")
            (vault / "raw/a.md").write_text("raw", encoding="utf-8")
            report = build_navigation_coverage(vault_path=vault)

        self.assertEqual(report["scope"]["source_packets"], 2)
        self.assertEqual(report["coverage"]["navigable_from_any_entry"], 1)
        self.assertEqual(report["coverage"]["traceable_to_raw"], 1)
        healthy_row = next(row for row in report["packets"] if row["source_id"] == "a" * 64)
        self.assertTrue(healthy_row["navigation"]["map"])
        self.assertTrue(healthy_row["verified_layers"]["evidence"])
        self.assertEqual(report["orphans"][0]["source_id"], "b" * 64)
        self.assertEqual(report["gap_queue"][0]["priority"], "P0")
        self.assertIn("missing_evidence", report["gap_queue"][0]["reasons"])
        self.assertIn("missing_raw", report["gap_queue"][0]["reasons"])

    def test_does_not_scan_unindexed_raw_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = self._release(Path(tmp), [])
            raw = vault / "raw"
            raw.mkdir()
            (raw / "unindexed.txt").write_text("not catalogued", encoding="utf-8")
            report = build_navigation_coverage(vault_path=vault)
        self.assertEqual(report["scope"]["source_packets"], 0)
        self.assertEqual(report["scope"]["policy"], "published_catalog_only")

    def test_declared_layers_do_not_hide_corrupt_packet_or_missing_raw(self) -> None:
        packet = {
            "source_id": "c" * 64, "object_type": "source_packet", "title": "broken",
            "family_id": None, "parent_ids": [], "date_start": "2026-01-03",
            "available_layers": ["summary", "statements", "evidence", "raw"],
            "paths": {"packet": "packets/c.json", "raw": "raw/missing.md"}, "warnings": [],
        }
        with tempfile.TemporaryDirectory() as tmp:
            vault = self._release(Path(tmp), [packet])
            (vault / "packets").mkdir()
            (vault / "packets/c.json").write_text("not-json", encoding="utf-8")
            report = build_navigation_coverage(vault_path=vault)
        row = report["packets"][0]
        self.assertTrue(row["declared_layers"]["evidence"])
        self.assertFalse(row["verified_layers"]["evidence"])
        self.assertFalse(row["verified_layers"]["raw"])
        self.assertIn("packet_unreadable", row["reasons"])
        self.assertIn("raw_file_missing", row["reasons"])

    def test_reviewed_non_actionable_source_only_gap_is_not_p0_queue(self) -> None:
        packet = {
            "source_id": "d" * 64, "object_type": "source_packet", "title": "greeting",
            "family_id": None, "parent_ids": [], "date_start": "2026-01-04",
            "available_layers": ["summary", "raw"],
            "paths": {"packet": "packets/d.json", "raw": "raw/d.md"},
            "warnings": ["manual_review_no_useful_content"],
        }
        with tempfile.TemporaryDirectory() as tmp:
            vault = self._release(Path(tmp), [packet])
            (vault / "packets").mkdir()
            (vault / "raw").mkdir()
            (vault / "packets/d.json").write_text(json.dumps({"record": {
                "summary": "seasonal greeting", "methods": [], "claims": [], "risks": [],
                "contradictions": [], "definitions": [], "examples": [],
            }}), encoding="utf-8")
            (vault / "raw/d.md").write_text("raw", encoding="utf-8")
            report = build_navigation_coverage(vault_path=vault)
        row = report["packets"][0]
        self.assertFalse(row["verified_layers"]["statements"])
        self.assertEqual(row["content_gap_status"], "manual_review_no_useful_content")
        self.assertIsNone(row["priority"])
        self.assertEqual(report["gap_queue"], [])


if __name__ == "__main__":
    unittest.main()
