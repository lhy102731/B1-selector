from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

import build_daily_ret_cache as daily_ret


class DailyReturnCacheTests(unittest.TestCase):
    def test_returns_nonzero_and_keeps_output_absent_on_corrupt_indicator(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            cache_dir = root / "indicators_cache"
            cache_dir.mkdir()
            dates = [datetime.now() - timedelta(days=2), datetime.now() - timedelta(days=1)]
            pd.DataFrame({"date": dates, "close": [10.0, 11.0]}).to_parquet(
                cache_dir / "000001.parquet", index=False
            )
            (cache_dir / "000002.parquet").write_text("not parquet", encoding="utf-8")
            output = root / "daily_ret.parquet"

            with patch.object(daily_ret, "CACHE_DIR", cache_dir), patch.object(
                daily_ret, "OUT", output
            ):
                result = daily_ret.main()

            self.assertEqual(2, result)
            self.assertFalse(output.exists())

    def test_builds_cache_when_all_indicator_files_are_readable(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            cache_dir = root / "indicators_cache"
            cache_dir.mkdir()
            dates = [datetime.now() - timedelta(days=2), datetime.now() - timedelta(days=1)]
            pd.DataFrame({"date": dates, "close": [10.0, 11.0]}).to_parquet(
                cache_dir / "000001.parquet", index=False
            )
            output = root / "daily_ret.parquet"

            with patch.object(daily_ret, "CACHE_DIR", cache_dir), patch.object(
                daily_ret, "OUT", output
            ):
                result = daily_ret.main()

            self.assertEqual(0, result)
            frame = pd.read_parquet(output)
            self.assertEqual(["000001"], frame["code"].unique().tolist())
            self.assertEqual(1, len(frame))


if __name__ == "__main__":
    unittest.main()
