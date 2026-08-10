from __future__ import annotations

import re
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "ag2_research/config.yaml"
CONTROL_SPEC = ROOT / "ag2_research/CONTROL_LAYER_SPEC.yaml"
DESIGN = ROOT / "ag2_research/AG2_V4_DESIGN.md"
ROLE_SYSTEM = ROOT / "ag2_research/ROLE_SYSTEM.md"

OWNER_SPLIT_SENTENCE = "The P6 control plane is the sole governance and persistence owner."


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _has_sentence(text: str, sentence: str) -> bool:
    return re.sub(r"\s+", " ", text).count(sentence) > 0


class AG2ConfigCapitalProjectionContractTests(unittest.TestCase):
    def test_owner_split_sentence_present_in_all_four_documents(self) -> None:
        for path in (CONFIG, CONTROL_SPEC, DESIGN, ROLE_SYSTEM):
            with self.subTest(path=path.name):
                self.assertTrue(_has_sentence(_read_text(path), OWNER_SPLIT_SENTENCE))

    def test_config_director_governance_inputs_are_analytics_only(self) -> None:
        config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
        director = config["agents"]["research_director"]
        message = director["system_message"]
        self.assertIn("v4.2 ANALYTICS-ONLY GOVERNANCE PROJECTIONS", message)
        self.assertIn("analytics-only", message)
        self.assertIn("You never write, update, or commit them", message)
        self.assertIn("v4.2 DIRECTOR BOUNDARY (HARD)", message)
        self.assertNotIn("v4.2 GOVERNANCE RULES (HARD)", message)
        self.assertIn("DRAFT", message)
        self.assertTrue(_has_sentence(message, OWNER_SPLIT_SENTENCE))

    def test_config_director_does_not_claim_self_persistence(self) -> None:
        config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
        message = config["agents"]["research_director"]["system_message"]
        self.assertIn("draft", message.lower())
        self.assertNotIn("Your decisions are durable", message)
        self.assertIn("You never write or commit governance state", message)

    def test_control_layer_spec_has_p6_owner_split(self) -> None:
        text = _read_text(CONTROL_SPEC)
        self.assertIn("p6_owner_split", text)
        self.assertIn("analytics-only", text)
        self.assertIn("commit-request draft", text)
        self.assertIn("Within the AG2 draft workflow", text)
        self.assertTrue(_has_sentence(text, OWNER_SPLIT_SENTENCE))

    def test_role_system_orchestrator_commit_rights_are_ag2_internal(self) -> None:
        text = _read_text(ROLE_SYSTEM)
        self.assertIn("AG2-internal", text)
        self.assertIn("commit-request draft", text)
        self.assertIn("analytics-only", text)
        self.assertTrue(_has_sentence(text, OWNER_SPLIT_SENTENCE))

    def test_design_doc_persistence_and_rebalance_are_control_plane_owned(self) -> None:
        text = _read_text(DESIGN)
        self.assertIn("analytics-only", text)
        self.assertIn("RECOMMENDS", text)
        self.assertIn("persisted only by the P6 control plane", text)
        self.assertNotIn("committed by System_Orchestrator", text)
        self.assertTrue(_has_sentence(text, OWNER_SPLIT_SENTENCE))


if __name__ == "__main__":
    unittest.main()
