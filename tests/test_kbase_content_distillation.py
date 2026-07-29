import unittest
from unittest.mock import patch

from ag2_research.kbase.content_distillation import (
    EXTERNAL_UPLOAD_ENV,
    PROMPTS,
    _final_gate,
    _require_external_upload_authorization,
    _source_payload,
)


class ContentDistillationGateTests(unittest.TestCase):
    def setUp(self):
        self.source = {"segments": [{"anchor": "page:1:line:2", "text": "市场弱时应控制仓位。"}]}
        self.candidate = {"source_id": "abc", "raw_path": "raw/a.pdf", "record": {"claims": [{
            "claim": "市场弱时应控制仓位。", "evidence_quote": "市场弱时应控制仓位。",
            "evidence_anchor": "page:1:line:2", "confidence": 0.9,
        }]}}
        self.reports = {"temporal_auditor": {"verdict": "pass"},
                        "skeptical_auditor": {"verdict": "pass"}}

    def test_accept_requires_exact_quote_at_anchor_and_two_passes(self):
        self.assertEqual("accept", _final_gate(self.candidate, self.source, self.reports)["decision"])

    def test_wrong_anchor_is_rejected(self):
        self.candidate["record"]["claims"][0]["evidence_anchor"] = "page:9:line:9"
        self.assertEqual("reject", _final_gate(self.candidate, self.source, self.reports)["decision"])

    def test_non_pass_auditor_cannot_accept(self):
        self.reports["skeptical_auditor"]["verdict"] = "revise"
        self.assertEqual("review", _final_gate(self.candidate, self.source, self.reports)["decision"])

    def test_multiple_anchor_string_is_rejected(self):
        self.candidate["record"]["claims"][0]["evidence_anchor"] = "page:1:line:2,page:1:line:3"
        self.assertEqual("reject", _final_gate(self.candidate, self.source, self.reports)["decision"])

    def test_relevance_only_auditor_issue_does_not_block_acceptance(self):
        self.reports["skeptical_auditor"] = {
            "verdict": "revise",
            "issues": ["非A股直接相关，后续AG2再判断因子价值"],
        }
        self.assertEqual("accept", _final_gate(self.candidate, self.source, self.reports)["decision"])

    def test_blocking_auditor_issue_still_requires_review(self):
        self.reports["skeptical_auditor"] = {
            "verdict": "revise",
            "blocking_issues": ["证据 quote 不在原文锚点中"],
        }
        self.assertEqual("review", _final_gate(self.candidate, self.source, self.reports)["decision"])

    def test_extractor_prompt_defers_factor_relevance_to_ag2(self):
        prompt = PROMPTS["extractor"]
        self.assertIn("不要提前判断", prompt)
        self.assertIn("留给后续", prompt)
        self.assertIn("AG2", prompt)
        self.assertNotIn("对A股研究有意义", prompt)

    def test_external_upload_requires_explicit_environment_authorization(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, EXTERNAL_UPLOAD_ENV):
                _require_external_upload_authorization()
        with patch.dict("os.environ", {EXTERNAL_UPLOAD_ENV: "1"}, clear=True):
            _require_external_upload_authorization()

    def test_source_payload_exposes_chunks_without_dropping_anchors(self):
        artifact = {
            "source_id": "sid",
            "raw_path": "raw/a.pdf",
            "segments": [
                {"anchor": "page:1:line:1", "text": "A" * 10},
                {"anchor": "page:2:line:1", "text": "B" * 10},
                {"anchor": "page:3:line:1", "text": "C" * 10},
            ],
        }
        source = _source_payload(artifact, max_chars=1000, max_chars_per_chunk=20)
        self.assertGreaterEqual(len(source["chunks"]), 2)
        self.assertEqual(
            [segment["anchor"] for segment in source["segments"]],
            ["page:1:line:1", "page:2:line:1", "page:3:line:1"],
        )


if __name__ == "__main__":
    unittest.main()
