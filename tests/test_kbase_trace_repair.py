from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ag2_research.kbase.trace_repair import generate_trace_repair_candidate


class TraceRepairTests(unittest.TestCase):
    def test_generates_candidate_without_modifying_raw_or_publishing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp) / "KBase"
            current = vault / "wiki/outputs/manifests/ag2-kbase/current"
            current.mkdir(parents=True)
            packet_root = vault / "raw/imports/demo/distillation/source-packets"
            packet_root.mkdir(parents=True)
            container = vault / "raw/videos/course/lesson"
            container.mkdir(parents=True)
            raw = container / "source.mp4"
            raw.write_bytes(b"immutable-video")
            source_id = "a" * 64
            missing_id = "b" * 64
            packet = packet_root / f"{source_id}.json"
            packet.write_text(json.dumps({
                "schema_version": 2, "sha256": source_id,
                "original_path": "raw/videos/course/lesson",
                "record": {"summary": "s", "claims": [{"text": "c", "evidence_anchor": "L1"}]},
            }), encoding="utf-8")
            missing_packet = packet_root / f"{missing_id}.json"
            missing_packet.write_text(json.dumps({
                "schema_version": 2, "sha256": missing_id,
                "original_path": "raw/videos/not-there",
                "record": {"summary": "s", "claims": [{"text": "c", "evidence_anchor": "L1"}]},
            }), encoding="utf-8")
            entries = [
                {"source_id": source_id, "object_type": "source_packet", "title": "one",
                 "family_id": None, "parent_ids": [], "date_start": "2026-01-01",
                 "available_layers": ["summary", "statements", "evidence", "raw"], "warnings": [],
                 "paths": {"packet": str(packet.relative_to(vault)).replace("\\", "/"),
                           "raw": "raw/videos/course/lesson"}},
                {"source_id": missing_id, "object_type": "source_packet", "title": "two",
                 "family_id": None, "parent_ids": [], "date_start": "2026-01-01",
                 "available_layers": ["summary", "statements", "evidence", "raw"], "warnings": [],
                 "paths": {"packet": str(missing_packet.relative_to(vault)).replace("\\", "/"),
                           "raw": "raw/videos/not-there"}},
            ]
            (current / "manifest.json").write_text(json.dumps({
                "catalog_schema_version": 1, "catalog_version": "v1",
            }), encoding="utf-8")
            (current / "facets.json").write_text("{}", encoding="utf-8")
            (current / "catalog.jsonl").write_text(
                "".join(json.dumps(entry) + "\n" for entry in entries), encoding="utf-8",
            )
            before = raw.read_bytes()
            output = Path(tmp) / "candidates"
            report = generate_trace_repair_candidate(vault_path=vault, output_root=output)

            self.assertEqual(report["counts"], {"recoverable": 1, "unresolved": 1})
            repaired = next(item for item in report["findings"] if item["source_id"] == source_id)
            self.assertEqual(repaired["candidate_patch"]["paths"]["raw"], "raw/videos/course/lesson/source.mp4")
            self.assertEqual(raw.read_bytes(), before)
            self.assertTrue(Path(report["candidate_catalog"]).is_file())
            self.assertTrue((Path(report["candidate_catalog"]).parent / "repair-report.json").is_file())
            self.assertFalse((current / "previous").exists())


if __name__ == "__main__":
    unittest.main()
