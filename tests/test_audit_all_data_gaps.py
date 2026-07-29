import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tools.audit_all_data_gaps import audit_pair, valid_mask


class AuditAllDataGapsTests(unittest.TestCase):
    def test_volume_and_amount_sentinels_are_invalid(self):
        values = pd.Series([1.0, 0.0, 2_147_483_648.0])

        _, volume_valid = valid_mask(values, "volume")
        _, amount_valid = valid_mask(values, "amount")

        self.assertEqual([True, True, False], volume_valid.tolist())
        self.assertEqual([True, True, False], amount_valid.tolist())

    def test_adjudicated_non_trading_row_is_not_an_effective_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current_path = root / "current.csv"
            legacy_path = root / "legacy.csv"
            pd.DataFrame(
                {"date": ["1992-10-05"], "volume": [100.0], "amount": [1_000.0]}
            ).to_csv(current_path, index=False, encoding="gbk")
            pd.DataFrame(
                {
                    "date": ["1992-10-04", "1992-10-05"],
                    "volume": [51_900.0, 100.0],
                    "amount": [1_000.0, 1_000.0],
                }
            ).to_csv(legacy_path, index=False, encoding="gbk")

            audited = audit_pair(("000016", str(current_path), str(legacy_path)))

        self.assertEqual(1, audited["date_gap_classes"]["approved_legacy_only_non_trading"])
        self.assertEqual(0, audited["date_gap_classes"]["legacy_only_positive_volume"])
        self.assertEqual({}, dict(audited["positive_date_gaps"]["legacy_only"]))

    def test_unapproved_positive_legacy_row_remains_a_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current_path = root / "current.csv"
            legacy_path = root / "legacy.csv"
            pd.DataFrame(
                {"date": ["1992-10-05"], "volume": [100.0], "amount": [1_000.0]}
            ).to_csv(current_path, index=False, encoding="gbk")
            pd.DataFrame(
                {
                    "date": ["1992-10-04", "1992-10-05"],
                    "volume": [51_900.0, 100.0],
                    "amount": [1_000.0, 1_000.0],
                }
            ).to_csv(legacy_path, index=False, encoding="gbk")

            audited = audit_pair(("999999", str(current_path), str(legacy_path)))

        self.assertEqual(0, audited["date_gap_classes"]["approved_legacy_only_non_trading"])
        self.assertEqual(1, audited["date_gap_classes"]["legacy_only_positive_volume"])


if __name__ == "__main__":
    unittest.main()
