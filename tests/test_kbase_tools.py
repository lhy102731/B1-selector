from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ag2_research.kbase.catalog_builder import publish_catalog
from ag2_research.kbase.coverage import build_navigation_coverage
from ag2_research.kbase.repository import KBaseRepository
from ag2_research.kbase.semantic_client import SemanticUnavailableError
from ag2_research.kbase.tools import (
    kbase_browse,
    kbase_open,
    kbase_overview,
    kbase_search,
    kbase_trace,
)


class KBaseToolTests(unittest.TestCase):
    def _vault(self, root: Path) -> tuple[Path, str]:
        vault = root / "KBase"
        packets = vault / "raw" / "imports" / "sample" / "distillation" / "source-packets"
        packets.mkdir(parents=True)
        (vault / "wiki" / "maps").mkdir(parents=True)
        (vault / "wiki" / "sources").mkdir(parents=True)
        sha = "a" * 64
        document = {
            "schema_version": 2,
            "pipeline_revision": 2,
            "sha256": sha,
            "original_path": "raw/books/example.md",
            "kind": "book",
            "use_mode": "reference",
            "extraction_layers": ["direct_text"],
            "record": {
                "canonical_title": "北京炒家2021-11-25盘后复盘",
                "aliases": ["20CM接盘大法"],
                "source_type": "book",
                "source_role": "primary_direct",
                "primary_people": ["北京炒家"],
                "topics": ["盘后复盘", "炸板"],
                "summary": "来源描述当日交易。",
                "methods": [{
                    "text": "来源陈述。",
                    "evidence_anchor": "[L10]",
                    "evidence_quote": "原文证据。",
                    "certainty": "high",
                    "source_voice": "author",
                }],
                "claims": [],
                "risks": [],
                "contradictions": [],
                "visual_gaps": [],
                "reliability": "medium",
                "review_flags": [],
            },
        }
        (packets / f"{sha}.json").write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
        raw_book = vault / "raw" / "books" / "example.md"
        raw_book.parent.mkdir(parents=True)
        raw_book.write_text("# 原文\n\n" + "证据文本。" * 200, encoding="utf-8")
        (vault / "wiki" / "maps" / "overview.md").write_text("# 总览\n\n来源入口。", encoding="utf-8")
        self.assertTrue(publish_catalog(vault)["published"])
        return vault, sha

    def test_progressive_tools_find_open_and_trace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, sha = self._vault(Path(tmp))
            overview = json.loads(kbase_overview(vault_path=str(vault)))
            search = json.loads(kbase_search("北京炒家 2021-11-25 20CM接盘大法", vault_path=str(vault)))
            opened = json.loads(kbase_open(sha, layer="evidence", max_chars=500, vault_path=str(vault)))
            trace = json.loads(kbase_trace(sha, vault_path=str(vault)))

            self.assertEqual(overview["counts"]["source_packets"], 1)
            self.assertIn("top_dates", overview)
            self.assertEqual(1, len(overview["content_maps"]))
            self.assertEqual("总览", overview["content_maps"][0]["title"])
            self.assertEqual(search["results"][0]["source_id"], sha)
            self.assertEqual(opened["layer"], "evidence")
            self.assertLessEqual(len(opened["content"]), 500)
            self.assertEqual(trace["evidence_anchors"][0]["anchor"], "[L10]")

    def test_browse_paginates_and_repository_blocks_path_escape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, _ = self._vault(Path(tmp))
            page = json.loads(kbase_browse("root", page_size=1, vault_path=str(vault)))
            self.assertEqual(len(page["results"]), 1)
            repo = KBaseRepository(vault)
            with self.assertRaises(ValueError):
                repo.safe_path("../outside.txt")

    def test_root_browse_relation_filters_content_maps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, _ = self._vault(Path(tmp))
            maps = json.loads(kbase_browse(
                "root", relation="maps", page_size=20, vault_path=str(vault),
            ))
            families = json.loads(kbase_browse(
                "root", relation="families", page_size=20, vault_path=str(vault),
            ))
            invalid = json.loads(kbase_browse(
                "root", relation="parents", page_size=20, vault_path=str(vault),
            ))

            self.assertEqual(["map"], [item["object_type"] for item in maps["results"]])
            self.assertTrue(all(item["object_type"] == "family" for item in families["results"]))
            self.assertIn("root relation must be", invalid["error"])

    def test_no_result_and_short_query_refinement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, _ = self._vault(Path(tmp))
            missing = json.loads(kbase_search("火星地产火星租金指数", vault_path=str(vault)))
            broad = json.loads(kbase_search("赚钱", vault_path=str(vault)))
            self.assertEqual(missing["result_count"], 0)
            self.assertTrue(broad["requires_refinement"])

    def test_navigation_search_skips_semantic_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, sha = self._vault(Path(tmp))
            with patch("ag2_research.kbase.tools.request_semantic_search") as semantic:
                payload = json.loads(kbase_search(
                    "浏览 北京炒家 资料家族", vault_path=str(vault),
                ))
            semantic.assert_not_called()
            self.assertEqual(sha, payload["results"][0]["source_id"])
            self.assertEqual("lexical", payload["search_backend"]["mode"])
            self.assertEqual(
                "skipped_navigation", payload["search_backend"]["semantic_status"],
            )

    def test_semantic_failure_falls_back_to_lexical_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault, sha = self._vault(Path(tmp))
            with patch(
                "ag2_research.kbase.tools.request_semantic_search",
                side_effect=SemanticUnavailableError("offline"),
            ) as semantic:
                payload = json.loads(kbase_search(
                    "北京炒家 2021-11-25 20CM接盘大法", vault_path=str(vault),
                ))
            semantic.assert_called_once()
            self.assertEqual(sha, payload["results"][0]["source_id"])
            self.assertEqual("lexical_fallback", payload["search_backend"]["mode"])
            self.assertEqual("unavailable", payload["search_backend"]["semantic_status"])

    def test_accepted_distilled_candidate_supplements_empty_packet_layers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp) / "KBase"
            packets = vault / "raw" / "imports" / "sample" / "distillation" / "source-packets"
            packets.mkdir(parents=True)
            sha = "b" * 64
            document = {
                "schema_version": 2,
                "sha256": sha,
                "original_path": "raw/books/empty.md",
                "kind": "book",
                "record": {
                    "canonical_title": "empty packet",
                    "source_type": "book",
                    "source_role": "primary_direct",
                    "primary_people": [],
                    "topics": [],
                    "summary": "summary only",
                    "methods": [{
                        "evidence_anchor": "legacy:opaque",
                        "evidence_quote": "legacy quote without explicit statement",
                        "certainty": "low",
                    }],
                    "claims": [],
                    "risks": [],
                    "contradictions": [],
                    "reliability": "medium",
                    "review_flags": [],
                },
            }
            (packets / f"{sha}.json").write_text(json.dumps(document), encoding="utf-8")
            raw = vault / "raw" / "books" / "empty.md"
            raw.parent.mkdir(parents=True)
            raw.write_text("source text", encoding="utf-8")
            candidate_dir = vault / "wiki" / "outputs" / "candidates" / "ag2-kbase"
            candidate_dir.mkdir(parents=True)
            candidate = candidate_dir / f"{sha}.candidate.json"
            candidate.write_text(json.dumps({
                "candidate_schema_version": 1,
                "candidate": {
                    "source_id": sha,
                    "raw_path": "raw/books/empty.md",
                    "record": {
                        "claims": [{
                            "claim": "candidate claim",
                            "confidence": 0.95,
                            "evidence_anchor": "paragraph:1",
                            "evidence_quote": "source text",
                        }]
                    },
                },
                "quality_gate": {
                    "decision": "accept",
                    "publication_eligible": True,
                    "errors": [],
                    "warnings": [],
                },
            }), encoding="utf-8")
            overrides = vault / "wiki" / "outputs" / "manifests" / "ag2-kbase" / "approved-overrides.json"
            overrides.parent.mkdir(parents=True)
            overrides.write_text(json.dumps({
                "schema_version": 1,
                "entry_patches": {
                    sha: {
                        "available_layers": ["summary", "statements", "evidence", "raw"],
                        "paths": {
                            "distilled_candidate": "wiki/outputs/candidates/ag2-kbase/" + candidate.name,
                        },
                    }
                },
                "additional_entries": {},
                "promotions": [],
            }), encoding="utf-8")
            self.assertTrue(publish_catalog(vault)["published"])

            statements = json.loads(kbase_open(sha, layer="statements", vault_path=str(vault)))
            evidence = json.loads(kbase_open(sha, layer="evidence", vault_path=str(vault)))
            trace = json.loads(kbase_trace(sha, vault_path=str(vault)))
            coverage = build_navigation_coverage(vault_path=vault)

            self.assertIn("candidate claim", statements["content"])
            self.assertIn("source text", evidence["content"])
            self.assertEqual(trace["evidence_anchors"][0]["anchor"], "paragraph:1")
            row = next(item for item in coverage["packets"] if item["source_id"] == sha)
            self.assertTrue(row["verified_layers"]["statements"])
            self.assertTrue(row["verified_layers"]["evidence"])


if __name__ == "__main__":
    unittest.main()
