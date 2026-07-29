from __future__ import annotations

from copy import deepcopy
import json
import unittest
from unittest.mock import patch

from ag2_research.config import ResearchConfig
from ag2_research.kbase.citation_inventory import (
    build_citation_inventory,
    citation_inventory_issues,
    summarize_tool_exchange,
)
from ag2_research.orchestrator import Orchestrator


CORRECT_ID = "d0f6e8ef012996e3274a4bee7f75b9a15ab2fdbfe8482400e7287565fad7be96"
OTHER_ID = "8e73359f02f2abcd1feccd14505cbb65d652263537380efcd382d0eae730cb50"
CHIMERA_ID = CORRECT_ID[:8] + OTHER_ID[8:]
UNOPENED_ID = "b" * 64


class DummyRepository:
    manifest = {"catalog_version": "catalog-test"}

    def __init__(self, *args, **kwargs):
        self._entries = {
            CORRECT_ID: {
                "source_id": CORRECT_ID,
                "title": "chip distribution course",
                "voice_role": "primary_direct",
                "date_start": None,
                "reliability": "medium",
                "summary": "source summary",
                "available_layers": ["summary", "statements", "raw"],
                "paths": {"raw": "raw/example.pptx"},
            },
            OTHER_ID: {
                "source_id": OTHER_ID,
                "title": "other search result",
                "voice_role": "secondary_commentary",
                "date_start": None,
                "reliability": "unverified",
                "summary": "other summary",
                "available_layers": ["summary"],
                "paths": {},
            },
            UNOPENED_ID: {
                "source_id": UNOPENED_ID,
                "title": "seen but unopened",
                "voice_role": "primary_direct",
                "date_start": None,
                "reliability": "medium",
                "summary": "unopened summary",
                "available_layers": ["summary"],
                "paths": {},
            },
        }

    def entries(self):
        return self._entries.values()

    def get(self, source_id):
        return self._entries.get(source_id)

    def read_packet(self, entry):
        if entry["source_id"] != CORRECT_ID:
            return {}
        return {
            "record": {
                "methods": [{"text": "three-stage method"}],
                "claims": [{"text": "cost distribution"}],
                "risks": [{"text": "support can become resistance"}],
            }
        }


def make_audit():
    search = summarize_tool_exchange(
        sequence=1,
        tool_call_id="search-1",
        tool_name="kbase_search",
        arguments={"query": "chip distribution"},
        content=json.dumps({
            "catalog_version": "catalog-test",
            "results": [
                {"source_id": OTHER_ID},
                {"source_id": CORRECT_ID},
                {"source_id": UNOPENED_ID},
            ],
        }),
    )
    opened = summarize_tool_exchange(
        sequence=2,
        tool_call_id="open-1",
        tool_name="kbase_open",
        arguments={"source_id": CORRECT_ID, "layer": "statements"},
        content=json.dumps({
            "catalog_version": "catalog-test",
            "source_id": CORRECT_ID,
            "title": "chip distribution course",
            "layer": "statements",
            "content": "{}",
        }),
    )
    traced = summarize_tool_exchange(
        sequence=3,
        tool_call_id="trace-1",
        tool_name="kbase_trace",
        arguments={"source_id": CORRECT_ID},
        content=json.dumps({
            "catalog_version": "catalog-test",
            "source_id": CORRECT_ID,
            "evidence_anchors": [{"ref": "methods[0]"}],
        }),
    )
    return [search, opened, traced]


def make_brief(source_id=CORRECT_ID, evidence_ref=None):
    evidence_ref = evidence_ref or f"{source_id}#methods[0]"
    return {
        "brief_id": "brief-test",
        "catalog_version": "catalog-test",
        "research_gap": "find an under-researched dimension",
        "sources_consulted": [{
            "source_id": source_id,
            "voice_role": "primary_direct",
            "date": None,
            "reliability": "medium",
            "evidence_refs": [evidence_ref],
        }],
        "source_observations": [{
            "source_id": source_id,
            "source_says": "The source describes a three-stage framework.",
            "context": "Qualitative course material.",
        }],
        "disagreements_and_limits": ["No independent backtest."],
        "missing_evidence": ["No visual review."],
        "agent_inference_boundary": "以下内容尚未进行AG2推断",
        "handoff_questions": ["Can downstream research test the source claim?"],
    }


class DummyRouter:
    def build_packet(self, objective=""):
        return {"registry_status": "none", "objective": objective}


class KBaseCitationInventoryTests(unittest.TestCase):
    def setUp(self):
        self.inventory = build_citation_inventory(make_audit(), repository=DummyRepository())

    def test_chimera_id_is_not_treated_as_the_opened_source(self):
        self.assertTrue(CHIMERA_ID.startswith(CORRECT_ID[:8]))
        self.assertEqual(CHIMERA_ID[8:], OTHER_ID[8:])
        brief = make_brief(CHIMERA_ID)

        issues = citation_inventory_issues(brief, self.inventory)

        self.assertEqual("source_id_not_returned_by_tools", issues[0]["code"])
        self.assertEqual([CHIMERA_ID], issues[0]["source_ids"])
        self.assertEqual(CHIMERA_ID, brief["sources_consulted"][0]["source_id"])

    def test_seen_but_unopened_source_is_not_eligible(self):
        issues = citation_inventory_issues(
            make_brief(UNOPENED_ID, f"{UNOPENED_ID}#summary"), self.inventory
        )
        self.assertEqual(
            [{"code": "source_id_not_opened", "source_ids": [UNOPENED_ID]}],
            issues,
        )

    def test_opened_source_and_exact_evidence_ref_are_eligible(self):
        self.assertEqual([CORRECT_ID], self.inventory["eligible_source_ids"])
        self.assertEqual([], citation_inventory_issues(make_brief(), self.inventory))

    def test_inventory_requires_complete_observations_limits_and_handoff_questions(self):
        brief = make_brief()
        brief["source_observations"] = []
        brief["disagreements_and_limits"] = []
        brief["missing_evidence"] = []
        brief["handoff_questions"] = []

        codes = {item["code"] for item in citation_inventory_issues(brief, self.inventory)}

        self.assertEqual(
            {
                "consulted_source_without_observation",
                "source_limit_analysis_missing",
                "handoff_questions_missing",
            },
            codes,
        )

    def test_gate_requests_bounded_revision_without_rewriting_id(self):
        orchestrator = Orchestrator.__new__(Orchestrator)
        output = make_brief(CHIMERA_ID)
        output["_citation_inventory"] = self.inventory

        with patch("ag2_research.kbase.repository.KBaseRepository", DummyRepository):
            decision, reason, _ = orchestrator._gate(
                "source_librarian", output, None, {}, {}
            )

        self.assertEqual("modify", decision)
        self.assertIn("source_id_not_returned_by_tools", reason)
        self.assertEqual(CHIMERA_ID, output["sources_consulted"][0]["source_id"])

    def test_revision_uses_controller_inventory_and_latest_passing_brief(self):
        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator.config = ResearchConfig()
        attempts = [make_brief(CHIMERA_ID), make_brief(CORRECT_ID)]
        revision_seen = []

        def invoker(stage, packet, last_outputs, topic):
            if len(attempts) == 1:
                revision_seen.append(deepcopy(last_outputs.get("__controller_revision__")))
            output = attempts.pop(0)
            output["_citation_inventory"] = deepcopy(self.inventory)
            return output

        with patch("ag2_research.kbase.repository.KBaseRepository", DummyRepository):
            result = orchestrator.run_sequential_workflow(
                "kbase_source_brief",
                topic="find a new dimension",
                strategy_id="brick",
                agent_invoker=invoker,
                memory_router=DummyRouter(),
                max_revision_attempts=2,
            )

        self.assertEqual("APPROVED", result["status"], result["reason"])
        self.assertEqual(1, result["revision_attempts"])
        self.assertEqual(["modify", "pass"], [step["gate"]["decision"] for step in result["transcript"]])
        self.assertEqual(
            CORRECT_ID,
            revision_seen[0]["citation_inventory"]["eligible_sources"][0]["source_id"],
        )
        approved = orchestrator._result_stage_output(result, "source_librarian")
        self.assertEqual(CORRECT_ID, approved["sources_consulted"][0]["source_id"])
        self.assertNotIn("_citation_inventory", approved)


if __name__ == "__main__":
    unittest.main()
