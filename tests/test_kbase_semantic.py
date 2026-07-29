from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from ag2_research.kbase.hybrid_ranking import (
    comparison_parts,
    is_navigation_query,
    lexical_rank_with_anchors,
)
from ag2_research.kbase.semantic_client import merge_semantic_results
from ag2_research.kbase.semantic_index import (
    _compatible_cached_requests,
    _evaluate_suite,
    _publish_directory,
    _semantic_payload_identity,
    validate_semantic_release,
)


def _entry(source_id: str, title: str, *, family: str = "family") -> dict:
    return {
        "source_id": source_id,
        "object_type": "source_packet",
        "title": title,
        "aliases": [],
        "people": ["北京炒家"],
        "family_id": family,
        "voice_role": "primary_direct",
        "source_type": "text",
        "date_start": "2021-11-25",
        "date_end": "2021-11-25",
        "topics": ["炸板"],
        "summary": "来源描述炸板和承接。",
        "reliability": "medium",
        "review_status": "source_only",
        "available_layers": ["summary", "statements", "evidence"],
        "warnings": [],
        "paths": {"packet": f"packets/{source_id}.json"},
    }


class HybridRankingTests(unittest.TestCase):
    def test_navigation_is_pure_lexical_even_when_query_contains_yu(self) -> None:
        entries = [
            _entry("a", "北京炒家本人资料"),
            _entry("b", "北京炒家音频转述", family="second"),
        ]
        query = "浏览北京炒家本人资料与音频转述"
        lexical, protected = lexical_rank_with_anchors(entries, query, limit=10)
        self.assertTrue(is_navigation_query(query))
        self.assertEqual([], comparison_parts(query))
        self.assertEqual(
            [row["source_id"] for row in lexical],
            protected,
        )

    def test_comparison_protects_two_candidates_per_branch(self) -> None:
        entries = [
            _entry("a1", "甲方法"),
            _entry("a2", "甲方法详解", family="a-second"),
            _entry("b1", "乙方法", family="b-first"),
            _entry("b2", "乙方法详解", family="b-second"),
        ]
        lexical, protected = lexical_rank_with_anchors(
            entries, "对比甲方法与乙方法", limit=10,
        )
        self.assertEqual(["a1", "b1", "a2", "b2"], protected)
        self.assertEqual(protected, [row["source_id"] for row in lexical[:4]])

    def test_normal_query_protects_first_two_lexical_candidates(self) -> None:
        entries = [
            _entry("first", "炸板承接"),
            _entry("second", "炸板承接案例", family="second"),
            _entry("third", "炸板承接复盘", family="third"),
        ]
        lexical, protected = lexical_rank_with_anchors(entries, "炸板 承接", limit=10)
        self.assertEqual(
            [row["source_id"] for row in lexical[:2]],
            protected[:2],
        )

    def test_exact_date_person_anchor_cannot_be_demoted(self) -> None:
        exact = _entry("exact", "北京炒家2021-11-25盘后复盘")
        other = _entry("other", "北京炒家其他复盘", family="other-family")
        lexical, protected = lexical_rank_with_anchors(
            [other, exact], "北京炒家 2021-11-25 炸板", limit=10,
        )
        semantic = {
            "results": [
                {"source_id": "other", "object_id": "o2", "document_type": "claim", "dense_rank": 1, "lexical_rank": 2, "rrf_score": 0.03, "reranker_score": 9.0},
                {"source_id": "exact", "object_id": "o1", "document_type": "claim", "dense_rank": 2, "lexical_rank": 1, "rrf_score": 0.02, "reranker_score": -3.0},
            ]
        }
        merged = merge_semantic_results(
            lexical_ranked=lexical,
            entries_by_id={"exact": exact, "other": other},
            semantic_result=semantic,
            protected_ids=protected,
            limit=10,
        )
        self.assertEqual("exact", merged[0]["source_id"])
        self.assertIn("reranker:bge-reranker-v2-m3", merged[0]["_match_reasons"])

    def test_semantic_requires_a_lexical_anchor(self) -> None:
        entry = _entry("one", "任意来源")
        merged = merge_semantic_results(
            lexical_ranked=[], entries_by_id={"one": entry},
            semantic_result={"results": [{"source_id": "one", "reranker_score": 99.0}]},
            limit=5,
        )
        self.assertEqual([], merged)

    def test_negative_reranker_logits_are_not_cross_query_thresholded(self) -> None:
        entries = {
            "lexical": _entry("lexical", "词法锚点"),
            "semantic-a": _entry("semantic-a", "语义候选甲", family="a"),
            "semantic-b": _entry("semantic-b", "语义候选乙", family="b"),
        }
        lexical = [dict(entries["lexical"], _score=100, _match_reasons=[])]
        merged = merge_semantic_results(
            lexical_ranked=lexical,
            entries_by_id=entries,
            semantic_result={"results": [
                {"source_id": "semantic-a", "object_id": "a", "reranker_score": -2.0},
                {"source_id": "semantic-b", "object_id": "b", "reranker_score": -7.0},
            ]},
            limit=3,
        )
        self.assertEqual(
            ["semantic-a", "semantic-b", "lexical"],
            [row["source_id"] for row in merged],
        )

    def test_family_diversification_is_preserved(self) -> None:
        entries = {
            "a": _entry("a", "A"),
            "b": _entry("b", "B"),
            "c": _entry("c", "C"),
            "d": _entry("d", "D", family="second"),
        }
        lexical = [dict(value, _score=100 - index, _match_reasons=[]) for index, value in enumerate(entries.values())]
        rows = []
        for index, source_id in enumerate(("a", "b", "c", "d")):
            rows.append({"source_id": source_id, "object_id": source_id, "document_type": "claim", "dense_rank": index + 1, "lexical_rank": index + 1, "rrf_score": 1.0, "reranker_score": 10.0 - index})
        merged = merge_semantic_results(
            lexical_ranked=lexical, entries_by_id=entries,
            semantic_result={"results": rows}, limit=3,
        )
        self.assertEqual(["a", "b", "d"], [row["source_id"] for row in merged])


class SemanticReleaseValidationTests(unittest.TestCase):
    def test_validates_bound_tiny_release_and_rejects_bad_vector_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            release = Path(tmp)
            catalog_entry = _entry("source", "Source")
            catalog_payload = json.dumps(catalog_entry, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
            (release / "catalog.jsonl").write_text(catalog_payload, encoding="utf-8")
            catalog_manifest = {"catalog_version": "v1", "source_fingerprint": "f" * 64}
            (release / "catalog-manifest.json").write_text(json.dumps(catalog_manifest), encoding="utf-8")
            (release / "facets.json").write_text("{}", encoding="utf-8")
            (release / "build-report.json").write_text("{}", encoding="utf-8")
            text = "source_says: test"
            document = {"object_id": "object", "source_id": "source", "content_sha256": __import__("hashlib").sha256(text.encode()).hexdigest(), "document_type": "claim", "retrieval_text": text}
            documents_payload = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
            (release / "documents.jsonl").write_text(documents_payload, encoding="utf-8")
            vector = np.zeros((1, 1024), dtype=np.float32)
            vector[0, 0] = 1.0
            with (release / "vectors.npy").open("wb") as handle:
                np.save(handle, vector, allow_pickle=False)
            digest = lambda path: __import__("hashlib").sha256(path.read_bytes()).hexdigest()
            manifest = {
                "schema_version": "kbase.semantic_index.v1", "status": "COMPLETED",
                "source_fingerprint": "s" * 64, "catalog_version": "v1",
                "catalog_source_fingerprint": "f" * 64,
                "catalog_sha256": digest(release / "catalog.jsonl"),
                "catalog_files": {
                    "manifest.json": digest(release / "catalog-manifest.json"),
                    "catalog.jsonl": digest(release / "catalog.jsonl"),
                    "facets.json": digest(release / "facets.json"),
                    "build-report.json": digest(release / "build-report.json"),
                },
                "model_binding_sha256": "m" * 64, "dimension": 1024,
                "counts": {"entries": 1, "source_packets": 1, "documents": 1, "indexed_sources": 1, "indexed_source_packets": 1, "lexical_only_source_packets": 0},
                "lexical_only_sources": [],
                "files": {"documents": "documents.jsonl", "documents_sha256": digest(release / "documents.jsonl"), "vectors": "vectors.npy", "vectors_sha256": digest(release / "vectors.npy")},
                "release_gates": {"status": "PENDING"},
            }
            (release / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            self.assertEqual("PASS", validate_semantic_release(release, require_gates=False)["status"])
            with (release / "vectors.npy").open("ab") as handle:
                handle.write(b"x")
            with self.assertRaisesRegex(ValueError, "vectors hash mismatch"):
                validate_semantic_release(release, require_gates=False)

    def test_directory_publish_preserves_state_when_live_current_move_is_denied(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            current = root / "current"
            candidate = root / "candidate/build"
            current.mkdir(parents=True)
            candidate.mkdir(parents=True)
            (current / "marker").write_text("current", encoding="utf-8")
            (candidate / "marker").write_text("candidate", encoding="utf-8")
            real_replace = os.replace

            def deny_current_move(source, target):
                target_path = Path(target)
                if (
                    Path(source) == current
                    and target_path.name == "current"
                    and target_path.parent.name.startswith(".promotion.")
                ):
                    raise PermissionError("simulated live bind lock")
                return real_replace(source, target)

            with patch(
                "research_automation.foundations.immutable_release.os.replace",
                side_effect=deny_current_move,
            ):
                with self.assertRaisesRegex(PermissionError, "live bind lock"):
                    _publish_directory(
                        root=root,
                        candidate=candidate,
                        validator=lambda release: (release / "marker").read_text(encoding="utf-8"),
                    )

            self.assertEqual("current", (current / "marker").read_text(encoding="utf-8"))
            self.assertEqual("candidate", (candidate / "marker").read_text(encoding="utf-8"))

    def test_metadata_identity_ignores_gates_but_binds_index_payload(self) -> None:
        base = {
            "source_fingerprint": "s" * 64,
            "catalog_version": "v1",
            "catalog_source_fingerprint": "c" * 64,
            "catalog_sha256": "a" * 64,
            "catalog_files": {"catalog.jsonl": "a" * 64},
            "model_binding_sha256": "m" * 64,
            "models": {"embedding": {"model_id": "bge-m3"}},
            "dimension": 1024,
            "dtype": "float32",
            "counts": {"documents": 1},
            "lexical_only_sources": [],
            "files": {"vectors_sha256": "v" * 64},
            "release_gates": {"status": "REJECTED"},
        }
        regated = json.loads(json.dumps(base))
        regated["release_gates"] = {"status": "APPROVED"}
        self.assertEqual(_semantic_payload_identity(base), _semantic_payload_identity(regated))
        regated["files"]["vectors_sha256"] = "x" * 64
        self.assertNotEqual(_semantic_payload_identity(base), _semantic_payload_identity(regated))


class SemanticSuiteEvaluationTests(unittest.TestCase):
    @staticmethod
    def _evaluate(case: dict, result: dict, *, acceptance: dict | None = None) -> dict:
        request_id = f"fixed:{case['id']}"
        with tempfile.TemporaryDirectory() as tmp:
            suite_path = Path(tmp) / "suite.yaml"
            suite_path.write_text("cases: []\n", encoding="utf-8")
            suite = {
                "max_results": 5,
                "forbidden_scopes": ["wiki/outputs/projects"],
                "acceptance": acceptance or {},
            }
            contexts = {
                request_id: {"case": case, "lexical": [result], "protected": []},
            }
            manifest = {
                "source_fingerprint": "s" * 64,
                "catalog_version": "v1",
                "catalog_sha256": "c" * 64,
                "model_binding_sha256": "m" * 64,
            }
            with patch(
                "ag2_research.kbase.semantic_index.merge_semantic_results",
                return_value=[result],
            ):
                return _evaluate_suite(
                    suite_name="fixed",
                    suite_path=suite_path,
                    suite=suite,
                    contexts=contexts,
                    batch_by_id={
                        request_id: {"status": "COMPLETED", "result": {"results": []}},
                    },
                    entries_by_id={str(result["source_id"]): result},
                    semantic_manifest=manifest,
                )

    def test_positive_source_family_or_title_match_uses_formal_or_semantics(self) -> None:
        result = _entry("different-source", "unrelated title", family="wanted-family")
        case = {
            "id": "family-match",
            "intent": "family",
            "query": "browse wanted family",
            "expected": {
                "source_ids": ["missing-source"],
                "family_ids": ["wanted-family"],
                "title_contains": ["missing title"],
            },
            "evidence_layer_required": False,
        }
        report = self._evaluate(case, result)
        self.assertTrue(report["cases"][0]["hit"])
        self.assertEqual(1.0, report["metrics"]["known_item_recall_at_5"])
        self.assertEqual(1.0, report["metrics"]["trace_success"])

    def test_multiple_source_requirement_ignores_family_and_title_matches(self) -> None:
        result = _entry("target-one", "wanted title", family="wanted-family")
        case = {
            "id": "multi-source",
            "intent": "cross_source",
            "query": "compare two sources",
            "expected": {
                "source_ids": ["target-one", "target-two"],
                "family_ids": ["wanted-family"],
                "title_contains": ["wanted title"],
                "minimum_distinct_sources": 2,
            },
            "evidence_layer_required": True,
        }
        report = self._evaluate(case, result)
        self.assertFalse(report["cases"][0]["hit"])
        self.assertEqual(1, report["cases"][0]["matched_expected_sources"])
        self.assertEqual(0.0, report["metrics"]["known_item_recall_at_5"])

    def test_trace_denominator_includes_positive_without_evidence_requirement(self) -> None:
        result = _entry("family-source", "family title", family="wanted-family")
        result["paths"] = {}
        case = {
            "id": "family-trace",
            "intent": "family",
            "query": "browse family",
            "expected": {"family_ids": ["wanted-family"]},
            "evidence_layer_required": False,
        }
        report = self._evaluate(case, result)
        self.assertEqual(1, report["counts"]["known"])
        self.assertIs(report["cases"][0]["trace_success"], False)
        self.assertEqual(0.0, report["metrics"]["trace_success"])

    def test_declared_brief_compliance_gate_is_enforced(self) -> None:
        result = _entry("unsafe", "unsafe result")
        result["object_type"] = "project_output"
        result["factor_spec"] = {"expression": "forbidden"}
        case = {
            "id": "brief-compliance",
            "intent": "concept",
            "query": "unsafe result",
            "expected": {"source_ids": ["unsafe"]},
            "evidence_layer_required": True,
        }
        report = self._evaluate(case, result, acceptance={"brief_compliance": 1.0})
        self.assertEqual("FAIL", report["status"])
        self.assertEqual(0.0, report["metrics"]["brief_compliance"])
        self.assertGreater(report["counts"]["brief_noncompliance_issues"], 0)

    def test_navigation_does_not_consume_or_require_a_semantic_batch_row(self) -> None:
        result = _entry("navigation-source", "北京炒家资料", family="wanted-family")
        case = {
            "id": "navigation",
            "intent": "family",
            "query": "浏览北京炒家资料家族",
            "expected": {"family_ids": ["wanted-family"]},
            "evidence_layer_required": False,
        }
        with tempfile.TemporaryDirectory() as tmp:
            suite_path = Path(tmp) / "suite.yaml"
            suite_path.write_text("cases: []\n", encoding="utf-8")
            report = _evaluate_suite(
                suite_name="fixed",
                suite_path=suite_path,
                suite={"max_results": 5, "acceptance": {"trace_success": 1.0}},
                contexts={
                    "fixed:navigation": {
                        "case": case,
                        "lexical": [result],
                        "protected": ["navigation-source"],
                        "semantic_enabled": False,
                    },
                },
                batch_by_id={},
                entries_by_id={"navigation-source": result},
                semantic_manifest={
                    "source_fingerprint": "s" * 64,
                    "catalog_version": "v1",
                    "catalog_sha256": "c" * 64,
                    "model_binding_sha256": "m" * 64,
                },
            )
        self.assertEqual("PASS", report["status"])
        self.assertIsNone(report["cases"][0]["error"])
        self.assertFalse(report["cases"][0]["semantic_enabled"])

    def test_cache_reuse_allows_only_navigation_lexical_order_changes(self) -> None:
        base = {
            "schema_version": "kbase.semantic_batch_requests.v1",
            "source_fingerprint": "s" * 64,
            "requests": [{
                "request_id": "fixed:navigation",
                "query": "浏览资料家族",
                "lexical_ids": ["a", "b"],
                "allowed_ids": ["a", "b"],
                "candidate_limit": 64,
                "result_limit": 100,
            }],
        }
        current = json.loads(json.dumps(base))
        current["requests"][0]["lexical_ids"] = ["b", "a"]
        changed = _compatible_cached_requests(
            executed=base,
            current=current,
            contexts={"fixed:navigation": {"semantic_enabled": False}},
        )
        self.assertEqual(["fixed:navigation"], changed)
        with self.assertRaisesRegex(RuntimeError, "semantic-enabled cached request changed"):
            _compatible_cached_requests(
                executed=base,
                current=current,
                contexts={"fixed:navigation": {"semantic_enabled": True}},
            )


if __name__ == "__main__":
    unittest.main()
