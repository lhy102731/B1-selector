from __future__ import annotations

import unittest
from pathlib import Path

import yaml

from ag2_research.kbase.query_regression import DEFAULT_CASES, load_query_suite
from ag2_research.kbase.schemas import (
    ContractValidationError,
    validate_catalog_entry,
    validate_source_brief,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
KBASE_AGENTS = Path(r"D:\KBase\AGENTS.md")


class KBaseP0ContractTests(unittest.TestCase):
    def test_source_brief_accepts_source_only_handoff(self) -> None:
        brief = {
            "brief_id": "brief-001",
            "catalog_version": "2026-07-04-p0",
            "research_gap": "理解来源如何描述情绪退潮。",
            "sources_consulted": [
                {
                    "source_id": "a" * 64,
                    "voice_role": "primary_direct",
                    "date": "2021-11-25",
                    "reliability": "medium",
                    "evidence_refs": ["packet:a#claims[0]"],
                }
            ],
            "source_observations": [
                {"source_id": "a" * 64, "source_says": "来源原意。", "context": "盘后复盘。"}
            ],
            "disagreements_and_limits": ["仅代表单一来源。"],
            "missing_evidence": [],
            "agent_inference_boundary": "以下内容尚未进行AG2推断",
            "handoff_questions": ["AG2需要独立判断哪些机制可检验？"],
        }
        validate_source_brief(brief)

    def test_source_brief_rejects_project_derivation_anywhere(self) -> None:
        brief = {
            "brief_id": "brief-002",
            "catalog_version": "p0",
            "research_gap": "gap",
            "sources_consulted": [{
                "source_id": "a" * 64,
                "voice_role": "unknown",
                "date": None,
                "reliability": "unverified",
                "evidence_refs": [],
            }],
            "source_observations": [{"source_id": "a" * 64, "source_says": "x", "context": ""}],
            "disagreements_and_limits": [],
            "missing_evidence": [],
            "agent_inference_boundary": "以下内容尚未进行AG2推断",
            "handoff_questions": [],
            "factor_spec": {"expression": "forbidden"},
        }
        with self.assertRaises(ContractValidationError):
            validate_source_brief(brief)

    def test_catalog_entry_contract_is_source_only(self) -> None:
        entry = {
            "catalog_schema_version": 1,
            "source_id": "a" * 64,
            "object_type": "source_packet",
            "title": "示例来源",
            "aliases": [],
            "people": ["作者"],
            "family_id": None,
            "voice_role": "unknown",
            "source_type": "pdf",
            "date_start": None,
            "date_end": None,
            "topics": ["市场情绪"],
            "summary": "保守摘要",
            "reliability": "unverified",
            "review_status": "source_only",
            "available_layers": ["summary", "raw"],
            "warnings": [],
            "parent_ids": [],
            "paths": {"raw": "raw/example.pdf"},
            "content_fingerprint": "b" * 64,
            "source_schema_version": 2,
        }
        validate_catalog_entry(entry)
        entry["hypothesis"] = "forbidden"
        with self.assertRaises(ContractValidationError):
            validate_catalog_entry(entry)

    def test_query_suite_has_30_grounded_cases(self) -> None:
        suite = load_query_suite(DEFAULT_CASES)
        self.assertEqual(len(suite["cases"]), 30)
        intents = {case["intent"] for case in suite["cases"]}
        self.assertEqual(intents, {"exact_person_date", "family", "concept", "cross_source", "negative"})
        self.assertEqual(len({case["id"] for case in suite["cases"]}), 30)
        for case in suite["cases"]:
            self.assertTrue(case["query"].strip())
            self.assertIn("expected", case)

    def test_governance_and_agent_prompts_share_the_boundary(self) -> None:
        config = yaml.safe_load((PROJECT_ROOT / "ag2_research" / "config.yaml").read_text(encoding="utf-8"))
        for agent_id in ("alpha_hunter", "factor_engineer"):
            prompt = config["agents"][agent_id]["system_message"]
            self.assertIn("source_brief", prompt)
            self.assertIn("project-side", prompt)
        bridge = (PROJECT_ROOT / "ag2_research" / "knowledge_bridge.py").read_text(encoding="utf-8")
        self.assertNotIn("visual evidence, hypotheses", bridge)
        if KBASE_AGENTS.exists():
            rules = KBASE_AGENTS.read_text(encoding="utf-8")
            self.assertIn("does not design project hypotheses", rules)
            self.assertIn("wiki/outputs/manifests/visuals/", rules)


if __name__ == "__main__":
    unittest.main()
