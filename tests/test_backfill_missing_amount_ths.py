import unittest

import pandas as pd

from tools.backfill_missing_amount_ths import (
    chinese_date,
    compatible_with_current_bar,
    parse_wencai_frame,
)


class ThsMissingAmountTests(unittest.TestCase):
    def test_parse_cross_section_amount(self):
        frame = pd.DataFrame(
            {
                "股票代码": ["000001.SZ", "600519.SH"],
                "成交额[20010420]": ["172437825", "1.23E8"],
            }
        )
        allowed = {("000001", "2001-04-20")}

        values = parse_wencai_frame(frame, allowed_pairs=allowed)

        self.assertEqual({("000001", "2001-04-20"): 172437825.0}, values)

    def test_parse_multi_date_code_result(self):
        frame = pd.DataFrame(
            {
                "股票代码": ["000001.SZ"],
                "成交额[20010419]": [141135672],
                "成交额[20010420]": [172437825],
            }
        )
        allowed = {
            ("000001", "2001-04-19"),
            ("000001", "2001-04-20"),
        }

        values = parse_wencai_frame(frame, allowed_pairs=allowed)

        self.assertEqual(2, len(values))
        self.assertEqual(141135672.0, values[("000001", "2001-04-19")])

    def test_amount_must_imply_vwap_inside_raw_bar(self):
        row = pd.Series(
            {
                "volume": 10_589_100,
                "low": 471.179,
                "high": 480.665,
                "close": 471.811,
                "close_raw": 16.2,
            }
        )
        self.assertTrue(
            compatible_with_current_bar(
                row, 172_437_825.22, price_tolerance=0.001
            )
        )
        self.assertFalse(
            compatible_with_current_bar(
                row, 300_000_000.0, price_tolerance=0.001
            )
        )

    def test_chinese_date(self):
        self.assertEqual("2001年4月20日", chinese_date("2001-04-20"))


if __name__ == "__main__":
    unittest.main()
