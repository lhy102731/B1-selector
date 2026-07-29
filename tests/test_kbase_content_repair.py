from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ag2_research.kbase.content_repair import (
    generate_content_repair_candidates,
    generate_reextraction_queue,
)
from ag2_research.kbase.coverage import build_navigation_coverage


class KBaseContentRepairTests(unittest.TestCase):
    def _vault(self, root: Path) -> tuple[Path, Path, Path]:
        vault = root / "KBase"
        release = vault / "wiki/outputs/manifests/ag2-kbase/current"
        packets = vault / "packets"
        release.mkdir(parents=True)
        packets.mkdir()
        recover_id, blocked_id = "a" * 64, "b" * 64
        entries = [
            {"source_id": source_id, "object_type": "source_packet", "title": source_id[0],
             "available_layers": ["summary", "statements", "evidence"],
             "paths": {"packet": f"packets/{source_id}.json"}, "parent_ids": []}
            for source_id in (recover_id, blocked_id)
        ]
        (release / "manifest.json").write_text(json.dumps({
            "catalog_schema_version": 1, "catalog_version": "test", "counts": {}
        }), encoding="utf-8")
        (release / "facets.json").write_text("{}", encoding="utf-8")
        (release / "catalog.jsonl").write_text(
            "".join(json.dumps(entry) + "\n" for entry in entries), encoding="utf-8"
        )
        recover = packets / f"{recover_id}.json"
        blocked = packets / f"{blocked_id}.json"
        recover.write_text(json.dumps({"record": {
            "summary": "not evidence",
            "claims": [{"content": "explicit legacy claim", "evidence_anchor": "[L1]",
                        "evidence_quote": "source quote"}],
        }}), encoding="utf-8")
        blocked.write_text(json.dumps({"record": {
            "summary": "must not become a statement",
            "source_type": "pdf", "claims": [{"evidence_anchor": "[L2]",
                                                  "evidence_quote": "quote without claim"}],
            "review_flags": ["OCR body missing"],
        }}), encoding="utf-8")
        return vault, recover, blocked

    def test_candidate_normalizes_only_explicit_statement_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, recover, blocked = self._vault(Path(tmp))
            original_recover = recover.read_bytes()
            original_blocked = blocked.read_bytes()
            report = generate_content_repair_candidates(vault_path=vault)
            candidate = vault / report["items"][0]["candidate_path"]
            candidate_doc = json.loads(candidate.read_text(encoding="utf-8"))

            self.assertEqual(report["counts"]["mechanically_recoverable"], 1)
            self.assertEqual(report["counts"]["requires_ocr_or_visual_extraction"], 1)
            self.assertEqual(candidate_doc["record"]["claims"][0]["text"], "explicit legacy claim")
            self.assertEqual(recover.read_bytes(), original_recover)
            self.assertEqual(blocked.read_bytes(), original_blocked)
            self.assertFalse((candidate.parent / ("b" * 64 + ".json")).exists())

    def test_coverage_accepts_content_alias_but_not_quote_as_statement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, _, _ = self._vault(Path(tmp))
            report = build_navigation_coverage(vault_path=vault)
        rows = {row["source_id"]: row for row in report["packets"]}
        self.assertTrue(rows["a" * 64]["verified_layers"]["statements"])
        self.assertTrue(rows["a" * 64]["verified_layers"]["evidence"])
        self.assertFalse(rows["b" * 64]["verified_layers"]["statements"])
        self.assertFalse(rows["b" * 64]["verified_layers"]["evidence"])

    def test_reextraction_queue_is_deterministic_and_excludes_recoverable_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, _, _ = self._vault(Path(tmp))
            first = generate_reextraction_queue(vault_path=vault)
            queue_path = vault / first["queue_path"]
            first_bytes = queue_path.read_bytes()
            second = generate_reextraction_queue(vault_path=vault)

            self.assertEqual(first_bytes, queue_path.read_bytes())
            self.assertEqual(first["statistics"]["total"], 1)
            self.assertEqual(second["tasks"][0]["source_id"], "b" * 64)
            self.assertEqual(second["tasks"][0]["gaps"], ["statements", "evidence"])
            self.assertTrue(second["policy"]["summary_as_evidence_forbidden"])
            self.assertEqual(second["statistics"]["by_priority"], {"BLOCKED_RAW_UNAVAILABLE": 1})


if __name__ == "__main__":
    unittest.main()
