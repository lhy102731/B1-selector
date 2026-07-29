import unittest

import pandas as pd

from tools.repair_exact_flat_bar_amounts import derive_exact_amounts


def frame(**overrides) -> pd.DataFrame:
    values = {
        "date": ["1992-04-08"],
        "open": [170.6],
        "high": [170.6],
        "low": [170.6],
        "close": [170.6],
        "close_raw": [170.6],
        "volume": [70.0],
        "amount": [pd.NA],
    }
    values.update({key: [value] for key, value in overrides.items()})
    return pd.DataFrame(values)


class ExactFlatBarAmountTests(unittest.TestCase):
    def test_derives_amount_for_single_price_bar(self):
        repaired, counts, details = derive_exact_amounts(frame())

        self.assertEqual(11_942.0, repaired.iloc[0]["amount"])
        self.assertEqual(1, counts["derived_exact_flat_bar"])
        self.assertEqual(1, len(details))

    def test_accepts_only_float_roundoff_in_flat_prices(self):
        repaired, counts, _ = derive_exact_amounts(
            frame(high=170.6 + 5e-10),
            flat_atol=1e-9,
        )

        self.assertEqual(11_942.0, repaired.iloc[0]["amount"])
        self.assertEqual(1, counts["derived_exact_flat_bar"])

    def test_leaves_nonflat_bar_unresolved(self):
        repaired, counts, _ = derive_exact_amounts(frame(high=171.0))

        self.assertTrue(pd.isna(repaired.iloc[0]["amount"]))
        self.assertEqual(1, counts["unresolved_nonflat_bar"])

    def test_rejects_vendor_sentinel_volume(self):
        repaired, counts, _ = derive_exact_amounts(frame(volume=2_147_483_648.0))

        self.assertTrue(pd.isna(repaired.iloc[0]["amount"]))
        self.assertEqual(1, counts["rejected_invalid_input"])

    def test_preserves_existing_valid_amount(self):
        repaired, counts, details = derive_exact_amounts(frame(amount=12_000.0))

        self.assertEqual(12_000.0, repaired.iloc[0]["amount"])
        self.assertEqual(0, counts["derived_exact_flat_bar"])
        self.assertTrue(details.empty)


if __name__ == "__main__":
    unittest.main()
