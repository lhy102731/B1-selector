from __future__ import annotations

import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from ag2_research.kbase.content_quality import validate_extraction_candidate
from ag2_research.kbase.override_promotion import promote_candidate


def candidate(statement: dict, *, summary: str = "overview") -> dict:
    return {"source_id": "a" * 64, "raw_path": "raw/source.pdf", "record": {
        "summary": summary, "methods": [statement], "claims": [], "risks": [],
        "contradictions": [], "definitions": [], "examples": [],
    }}


class ContentQualityGateTests(unittest.TestCase):
    def test_accepts_explicit_statement_with_page_anchor(self) -> None:
        result = validate_extraction_candidate(candidate({
            "text": "成交量收缩后观察承接", "evidence_anchor": "第 12 页", "confidence": .91,
        }))
        self.assertEqual(result["decision"], "accept")

    def test_quote_is_valid_evidence_without_anchor(self) -> None:
        result = validate_extraction_candidate(candidate({
            "text": "等待分歧转一致", "evidence_quote": "原文逐字引用", "confidence": .9,
        }))
        self.assertEqual(result["decision"], "accept")

    def test_rejects_summary_promotion_and_opaque_anchor(self) -> None:
        result = validate_extraction_candidate(candidate({
            "text": "overview", "evidence_anchor": "chapter-alpha", "confidence": .95,
        }))
        self.assertEqual(result["decision"], "reject")
        self.assertTrue(any("duplicates summary" in value for value in result["errors"]))
        self.assertTrue(any("lacks page/line/timestamp" in value for value in result["errors"]))

    def test_low_confidence_routes_to_review(self) -> None:
        result = validate_extraction_candidate(candidate({
            "text": "弱置信度识别", "evidence_anchor": "00:03:21", "confidence": .62,
        }))
        self.assertEqual(result["decision"], "review")
        self.assertIn("non-GPT", result["warnings"][0])

    def test_missing_statement_or_provenance_fails_closed(self) -> None:
        value = candidate({"evidence_quote": "孤立引文", "confidence": .99})
        value["raw_path"] = ""
        result = validate_extraction_candidate(value)
        self.assertEqual(result["decision"], "reject")
        self.assertIn("raw_path provenance is required", result["errors"])
        self.assertTrue(any("no explicit statement" in error for error in result["errors"]))

    def test_failed_catalog_publish_rolls_back_override_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp) / "KBase"
            release = vault / "wiki/outputs/manifests/ag2-kbase/current"
            repair = vault / "wiki/outputs/manifests/ag2-kbase/candidate-repairs/atomic"
            release.mkdir(parents=True)
            repair.mkdir(parents=True)
            (release / "manifest.json").write_text(json.dumps({
                "catalog_schema_version": 1, "catalog_version": "v1",
            }), encoding="utf-8")
            (release / "facets.json").write_text("{}", encoding="utf-8")
            (release / "catalog.jsonl").write_text("", encoding="utf-8")
            (repair / "plan.json").write_text(json.dumps({
                "candidate_id": "atomic", "base_catalog_version": "v1", "patches": [],
            }), encoding="utf-8")
            override = vault / "wiki/outputs/manifests/ag2-kbase/approved-overrides.json"
            original = (json.dumps({"schema_version": 1, "entry_patches": {},
                                    "additional_entries": {}, "promotions": []}) + "\n").encode()
            override.write_bytes(original)

            with patch("ag2_research.kbase.override_promotion.publish_catalog",
                       side_effect=RuntimeError("simulated publication failure")):
                with self.assertRaisesRegex(RuntimeError, "simulated publication failure"):
                    promote_candidate(vault_path=vault, candidate_dir=repair)
            self.assertEqual(override.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
