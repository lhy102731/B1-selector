import unittest

import pandas as pd

from tools.backfill_daily_pcf_baostock import merge_daily_pcf


class DailyPcfBaoStockTests(unittest.TestCase):
    def test_fills_only_exact_day_missing_pcf(self):
        current = pd.DataFrame(
            {
                "date": ["2026-07-27", "2026-07-24"],
                "pcf": [pd.NA, 10.0],
                "pe_dynamic": [5.0, 4.9],
            }
        )
        remote = pd.DataFrame(
            {"date": ["2026-07-27"], "pcf": [52.16], "pe_dynamic": [99.0]}
        )

        merged, changed = merge_daily_pcf(current, remote, "2026-07-27")

        self.assertTrue(changed)
        self.assertEqual(52.16, merged.iloc[0]["pcf"])
        self.assertEqual(5.0, merged.iloc[0]["pe_dynamic"])
        self.assertEqual(10.0, merged.iloc[1]["pcf"])

    def test_does_not_replace_existing_pcf(self):
        current = pd.DataFrame({"date": ["2026-07-27"], "pcf": [12.0]})
        remote = pd.DataFrame({"date": ["2026-07-27"], "pcf": [52.16]})

        merged, changed = merge_daily_pcf(current, remote, "2026-07-27")

        self.assertFalse(changed)
        self.assertEqual(12.0, merged.iloc[0]["pcf"])


if __name__ == "__main__":
    unittest.main()
