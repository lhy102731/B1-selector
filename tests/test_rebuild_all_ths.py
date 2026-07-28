from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from tools.rebuild_all_ths import _normalise_history, rebuild, validate_history
from utils.ths_data_source import THSHistoryPermissionError


class _Source:
    def __init__(self, permitted=True):
        self.permitted = permitted

    def fetch_turnover_history(self, code, start, end):
        if not self.permitted:
            raise THSHistoryPermissionError("guest permission denied")
        return pd.DataFrame({"date": pd.to_datetime(["2020-01-02", "2020-01-03"]), "turnover": [2.0, 4.0]})

    def fetch_history(self, code, start, end):
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-02", "2020-01-03"]),
                "open": [10.0, 11.0], "high": [11.0, 12.0], "low": [9.0, 10.0],
                "close": [10.0, 11.0], "close_raw": [10.0, 11.0],
                "volume": [1000, 1000], "amount": [10000.0, 11000.0],
                "turnover": [2.0, 4.0], "market_cap": [500000.0, 275000.0],
            }
        )


def _old_csv(path: Path):
    pd.DataFrame({"date": ["2020-01-03"], "close": [1.0]}).to_csv(path, index=False, encoding="gbk")


class THSRebuildTests(unittest.TestCase):
    def test_validation_rejects_unrepaired_trade_value_unit_mismatch(self):
        frame = _normalise_history(
            _Source().fetch_history("000001", "2020-01-01", "2020-01-03")
        )
        frame["volume"] = [100, 100]

        validation = validate_history(frame, "000001")

        self.assertFalse(validation["valid"])
        self.assertEqual("trade_value_price_mismatch", validation["reason"])

    def test_validation_accepts_ipo_vwap_inside_wide_intraday_range(self):
        frame = _Source().fetch_history("301487", "2023-08-09", "2023-08-09").iloc[[0]].copy()
        frame.loc[:, ["open", "high", "low", "close", "close_raw"]] = [
            22.51, 202.15, 20.40, 98.02, 98.02
        ]
        frame.loc[:, "volume"] = 45_700_655
        frame.loc[:, "amount"] = 1_232_428_460
        frame.loc[:, "turnover"] = 50.0
        frame.loc[:, "market_cap"] = (
            frame["close_raw"] * frame["volume"] * 100.0 / frame["turnover"]
        )

        validation = validate_history(frame, "301487")

        self.assertTrue(validation["valid"])
        self.assertEqual(0, validation["trade_value_mismatch_rows"])

    def test_validation_accepts_wide_ipo_after_later_backward_adjustment(self):
        frame = _Source().fetch_history("301487", "2023-08-09", "2023-08-09").iloc[[0]].copy()
        frame.loc[:, ["open", "high", "low", "close", "close_raw"]] = [
            45.02, 404.30, 40.80, 196.04, 98.02
        ]
        frame.loc[:, "volume"] = 45_700_655
        frame.loc[:, "amount"] = 1_232_428_460
        frame.loc[:, "turnover"] = 50.0
        frame.loc[:, "market_cap"] = (
            frame["close_raw"] * frame["volume"] * 100.0 / frame["turnover"]
        )

        validation = validate_history(frame, "301487")

        self.assertTrue(validation["valid"])

    def test_validation_accepts_wide_vwap_on_third_ipo_trading_day(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2024-10-08", "2024-09-30", "2024-09-27", "2024-09-26"]
                ),
                "open": [280.02, 59.00, 39.20, 30.25],
                "high": [361.00, 280.00, 73.00, 39.39],
                "low": [196.00, 51.56, 35.58, 29.11],
                "close": [228.01, 181.00, 50.64, 39.37],
                "close_raw": [228.01, 181.00, 50.64, 39.37],
                "volume": [27_681_931, 31_015_895, 30_121_964, 28_888_295],
                "amount": [7_453_052_600, 2_722_492_700, 1_357_105_300, 909_228_300],
                "turnover": [50.0, 50.0, 50.0, 50.0],
            }
        )
        frame["market_cap"] = (
            frame["close_raw"] * frame["volume"] * 100.0 / frame["turnover"]
        )

        validation = validate_history(frame, "301551")

        self.assertTrue(validation["valid"])
        self.assertEqual(0, validation["trade_value_mismatch_rows"])

    def test_validation_rejects_integer_sentinel_amount(self):
        frame = _normalise_history(
            _Source().fetch_history("000001", "2020-01-01", "2020-01-03")
        )
        frame.loc[0, "amount"] = 2_147_483_648.0

        validation = validate_history(frame, "000001")

        self.assertFalse(validation["valid"])
        self.assertEqual("amount_sentinel", validation["reason"])

    def test_validation_rejects_nonfinite_or_inverted_ohlc(self):
        for field, value, reason in (
            ("open", float("nan"), "invalid_ohlc_price"),
            ("close", -1.0, "invalid_ohlc_price"),
            ("high", 8.0, "invalid_ohlc_order"),
            ("low", 12.0, "invalid_ohlc_order"),
        ):
            with self.subTest(field=field, value=value):
                frame = _normalise_history(
                    _Source().fetch_history("000001", "2020-01-01", "2020-01-03")
                )
                frame.loc[0, field] = value

                validation = validate_history(frame, "000001")

                self.assertFalse(validation["valid"])
                self.assertEqual(reason, validation["reason"])

    def test_validation_rejects_bad_dates_and_negative_volume(self):
        cases = []
        duplicate = _Source().fetch_history("000001", "2020-01-01", "2020-01-03")
        duplicate.loc[1, "date"] = duplicate.loc[0, "date"]
        cases.append((duplicate, "duplicate_dates"))
        ascending = _Source().fetch_history("000001", "2020-01-01", "2020-01-03")
        cases.append((ascending, "dates_not_descending"))
        negative_volume = _Source().fetch_history("000001", "2020-01-01", "2020-01-03").iloc[::-1]
        negative_volume.loc[negative_volume.index[0], "volume"] = -1
        cases.append((negative_volume, "invalid_volume"))

        for frame, reason in cases:
            with self.subTest(reason=reason):
                validation = validate_history(frame, "000001")
                self.assertFalse(validation["valid"])
                self.assertEqual(reason, validation["reason"])

    def test_validation_rejects_history_truncated_against_archive_boundaries(self):
        frame = _normalise_history(
            _Source().fetch_history("000001", "2020-01-01", "2020-01-03")
        )

        too_late_start = validate_history(
            frame, "000001", expected_start="2019-01-01"
        )
        too_early_end = validate_history(
            frame, "000001", expected_end="2021-01-01"
        )

        self.assertEqual("history_start_truncated", too_late_start["reason"])
        self.assertEqual("history_end_truncated", too_early_end["reason"])

    def test_permission_probe_blocks_before_live_or_stage_stock_write(self):
        with TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            stage = Path(temp) / "stage"
            (data / "00").mkdir(parents=True)
            live = data / "00" / "000001.csv"
            _old_csv(live)
            before = live.read_bytes()

            code = rebuild(data, stage_dir=stage, source=_Source(permitted=False))

            self.assertEqual(3, code)
            self.assertEqual(before, live.read_bytes())
            self.assertFalse((stage / "00" / "000001.csv").exists())

    def test_successful_rebuild_stages_close_raw_and_formula_cap(self):
        with TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            stage = Path(temp) / "stage"
            (data / "00").mkdir(parents=True)
            live = data / "00" / "000001.csv"
            _old_csv(live)
            before = live.read_bytes()

            code = rebuild(data, stage_dir=stage, source=_Source())

            self.assertEqual(0, code)
            self.assertEqual(before, live.read_bytes())
            staged = pd.read_csv(stage / "00" / "000001.csv", encoding="gbk")
            self.assertIn("close_raw", staged.columns)
            self.assertEqual(275000.0, staged.iloc[0]["market_cap"])

    def test_commit_writes_manifest_and_preserves_old_database_backup(self):
        with TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            stage = Path(temp) / "stage"
            (data / "00").mkdir(parents=True)
            live = data / "00" / "000001.csv"
            _old_csv(live)
            old = live.read_bytes()

            code = rebuild(data, stage_dir=stage, source=_Source(), commit=True)

            self.assertEqual(0, code)
            manifest = (data / ".ths_dataset_manifest.json").read_text(encoding="utf-8")
            self.assertIn('"source": "thsdk"', manifest)
            self.assertIn('"schema_version": 3', manifest)
            self.assertIn('"data_quality_version": 4', manifest)
            backups = list(Path(temp).glob("data_ths_backup_*"))
            self.assertEqual(1, len(backups))
            self.assertEqual(old, (backups[0] / "00" / "000001.csv").read_bytes())


if __name__ == "__main__":
    unittest.main()
