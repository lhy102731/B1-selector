import unittest

import pandas as pd

from tools.backfill_missing_valuation_fields_ths import (
    build_exact_tasks,
    parse_wencai_valuations,
    query_plan,
)


class MissingValuationFieldsThsTests(unittest.TestCase):
    def test_parses_only_exact_allowed_code_date_fields(self):
        frame = pd.DataFrame(
            {
                "股票代码": ["600018.SH"],
                "市盈率(pe,ttm)[20000719]": [42.2],
                "市净率(pb)[20000719]": [11.9],
                "市销率(ps,ttm)[20000719]": [12.2],
                "市盈率(pe,ttm)[20000720]": [41.0],
            }
        )
        allowed = {
            ("600018", "2000-07-19", "pe_dynamic"),
            ("600018", "2000-07-19", "pb"),
            ("600018", "2000-07-19", "ps"),
        }

        values = parse_wencai_valuations(frame, allowed=allowed)

        self.assertEqual(3, len(values))
        self.assertEqual(42.2, values[("600018", "2000-07-19", "pe_dynamic")])
        self.assertNotIn(("600018", "2000-07-20", "pe_dynamic"), values)

    def test_zero_and_nonfinite_values_are_not_accepted(self):
        frame = pd.DataFrame(
            {
                "股票代码": ["000001.SZ"],
                "市盈率(pe,ttm)[20260727]": [0.0],
                "市净率(pb)[20260727]": [float("inf")],
                "市销率(ps,ttm)[20260727]": [-2.0],
            }
        )
        allowed = {
            ("000001", "2026-07-27", "pe_dynamic"),
            ("000001", "2026-07-27", "pb"),
            ("000001", "2026-07-27", "ps"),
        }

        values = parse_wencai_valuations(frame, allowed=allowed)

        self.assertEqual({("000001", "2026-07-27", "ps"): -2.0}, values)

    def test_query_plan_uses_wide_dates_and_batches_remaining_tokens(self):
        targets = {
            ("000001", "2026-07-27", "pe_dynamic"),
            ("000002", "2026-07-27", "pb"),
            ("000001", "2026-07-26", "ps"),
        }

        plan = query_plan(
            targets,
            wide_date_min_codes=2,
            token_batch_size=40,
        )

        self.assertEqual(1, plan["wide_dates"])
        self.assertEqual(2, plan["wide_target_values"])
        self.assertEqual(1, plan["initial_code_queries"])
        self.assertEqual(2, plan["exact_code_queries"])

    def test_exact_tasks_are_deterministic_and_bounded(self):
        targets = {
            ("000002", "2026-07-27", "pb"),
            ("000001", "2026-07-26", "ps"),
            ("000001", "2026-07-27", "pe_dynamic"),
        }

        tasks = build_exact_tasks(targets, token_batch_size=1)

        self.assertEqual(
            [
                ("000001", (("2026-07-26", "ps"),)),
                ("000001", (("2026-07-27", "pe_dynamic"),)),
                ("000002", (("2026-07-27", "pb"),)),
            ],
            tasks,
        )


if __name__ == "__main__":
    unittest.main()
