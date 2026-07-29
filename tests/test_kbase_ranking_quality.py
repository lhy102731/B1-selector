from __future__ import annotations

import unittest

from ag2_research.kbase.ranking import rank_entries


class KBaseRankingQualityTests(unittest.TestCase):
    def test_reviewed_reliable_source_wins_relevance_tie(self):
        base = {
            "object_type": "source_packet", "title": "缩量回踩研究",
            "aliases": [], "people": [], "family_id": "volume",
            "topics": ["缩量回踩"], "summary": "缩量回踩后的走势",
            "date_start": None, "date_end": None,
        }
        low = dict(
            base, source_id="low", reliability="low",
            review_status="review_required", warnings=["low_asr_confidence"],
        )
        high = dict(
            base, source_id="high", reliability="high",
            review_status="reviewed", warnings=[],
        )

        ranked = rank_entries([low, high], "缩量回踩", diversify=False)

        self.assertEqual("high", ranked[0]["source_id"])
        self.assertIn("quality_adjustment:55", ranked[0]["_match_reasons"])


if __name__ == "__main__":
    unittest.main()
