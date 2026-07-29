from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from tools import rebuild_recent_hfq_segment as rebuilder
from tools.rebuild_recent_hfq_segment import rebuild_frame


def remote_row(date: str, close: float, *, volume: int = 1_000) -> dict[str, object]:
    return {
        "date": date,
        "open": close - 1.0,
        "high": close + 2.0,
        "low": close - 2.0,
        "close": close,
        "volume": volume,
        "amount": close * volume,
        "turnover": 1.25,
        "change_pct": 1.0,
        "amplitude": 4.0,
        "change": 1.0,
    }


class RebuildRecentHfqSegmentTests(unittest.TestCase):
    def test_stage_one_resumes_from_hash_verified_result(self):
        anchor_dates = pd.date_range("2026-05-28", periods=10, freq="D")
        remote = pd.DataFrame(
            [
                remote_row(day.strftime("%Y-%m-%d"), 90.0 + index)
                for index, day in enumerate(anchor_dates)
            ]
            + [remote_row("2026-06-11", 100.0)]
        )
        local = remote.copy()
        for field in ("open", "high", "low", "close"):
            local[field] = pd.to_numeric(local[field]) * 2.0
        local["change"] = pd.to_numeric(local["change"]) * 2.0

        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir = root / "data"
            source = data_dir / "00" / "000001.csv"
            source.parent.mkdir(parents=True)
            local.to_csv(source, index=False, encoding="gbk")
            output_dir = root / "stage"

            with patch.object(rebuilder, "_fetch_with_retry", return_value=remote):
                first = rebuilder.stage_one(
                    source,
                    data_dir=data_dir,
                    output_dir=output_dir,
                    start_date=20260611,
                    end_date=20260611,
                    provider="tencent",
                )
            with patch.object(
                rebuilder,
                "_fetch_with_retry",
                side_effect=AssertionError("resume must not refetch"),
            ):
                second = rebuilder.stage_one(
                    source,
                    data_dir=data_dir,
                    output_dir=output_dir,
                    start_date=20260611,
                    end_date=20260611,
                    provider="tencent",
                )

            self.assertEqual("READY", first["status"])
            self.assertTrue(second["resumed"])
            self.assertEqual(first["staged_sha256"], second["staged_sha256"])

    def test_rebuild_uses_stable_preincident_scale_and_preserves_suspension(self):
        remote = pd.DataFrame(
            [
                remote_row("2026-06-06", 97.0),
                remote_row("2026-06-08", 98.0),
                remote_row("2026-06-10", 99.0),
                remote_row("2026-06-11", 100.0),
                remote_row("2026-06-12", 101.0),
                remote_row("2026-06-15", 102.0),
            ]
        )
        local = pd.DataFrame(
            [
                {**remote_row("2026-06-06", 97.0), "close": 194.0, "open": 192.0, "high": 198.0, "low": 190.0, "change": 2.0, "research_note": "anchor-a"},
                {**remote_row("2026-06-08", 98.0), "close": 196.0, "open": 194.0, "high": 200.0, "low": 192.0, "change": 2.0, "research_note": "anchor-b"},
                {**remote_row("2026-06-10", 99.0), "close": 198.0, "open": 196.0, "high": 202.0, "low": 194.0, "change": 2.0, "research_note": "anchor-c"},
                {**remote_row("2026-06-11", 100.0), "close": 150.0, "open": 148.5, "high": 153.0, "low": 147.0, "change": 1.5, "research_note": "keep-existing-metadata"},
                {**remote_row("2026-06-12", 101.0), "close": 151.5, "open": 150.0, "high": 154.5, "low": 148.5, "change": 1.5, "research_note": "keep-second-metadata"},
                {
                    "date": "2026-06-13",
                    "open": 151.5,
                    "high": 151.5,
                    "low": 151.5,
                    "close": 151.5,
                    "volume": 0,
                    "amount": 0.0,
                    "turnover": 0.0,
                    "change_pct": 0.0,
                    "amplitude": 0.0,
                    "change": 0.0,
                    "research_note": "suspended-explicitly",
                },
            ]
        )

        rebuilt, result, events = rebuild_frame(
            local,
            remote,
            start_date=20260611,
            end_date=20260615,
            min_anchor_points=3,
        )

        self.assertEqual("READY", result["status"])
        self.assertAlmostEqual(2.0, result["factor"])
        by_date = rebuilt.set_index("date")
        self.assertAlmostEqual(200.0, by_date.loc["2026-06-11", "close"])
        self.assertAlmostEqual(202.0, by_date.loc["2026-06-12", "close"])
        self.assertAlmostEqual(204.0, by_date.loc["2026-06-15", "close"])
        self.assertAlmostEqual(2.0, by_date.loc["2026-06-15", "change"])
        self.assertEqual(
            "keep-existing-metadata",
            by_date.loc["2026-06-11", "research_note"],
        )
        self.assertTrue(pd.isna(by_date.loc["2026-06-15", "research_note"]))
        self.assertEqual(0, by_date.loc["2026-06-13", "volume"])
        self.assertEqual("suspended-explicitly", by_date.loc["2026-06-13", "research_note"])
        self.assertEqual({"inserted", "replaced"}, {event["action"] for event in events})

    def test_unstable_preincident_scale_is_quarantined(self):
        remote = pd.DataFrame(
            [
                remote_row("2026-06-06", 100.0),
                remote_row("2026-06-08", 100.0),
                remote_row("2026-06-10", 100.0),
                remote_row("2026-06-11", 101.0),
            ]
        )
        local = remote.iloc[:3].copy()
        local["close"] = [200.0, 200.0, 150.0]

        rebuilt, result, events = rebuild_frame(
            local,
            remote,
            start_date=20260611,
            end_date=20260611,
            min_anchor_points=3,
        )

        self.assertEqual("QUARANTINED", result["status"])
        self.assertEqual("insufficient_price_variation", result["reason"])
        self.assertTrue(rebuilt.equals(local))
        self.assertEqual([], events)


if __name__ == "__main__":
    unittest.main()
