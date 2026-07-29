from __future__ import annotations

import pickle
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from backtest_optimized import (
    _build_signal_cache_identity,
    _load_signal_cache,
    _save_signal_cache,
)


class SignalCacheIdentityTests(unittest.TestCase):
    def test_same_size_indicator_change_invalidates_snapshot(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory)
            cache = data_dir / "indicators_cache"
            cache.mkdir()
            path = cache / "000001.parquet"
            path.write_bytes(b"AAAA")

            before = _build_signal_cache_identity(
                data_dir,
                "indicators_cache",
                ["000001"],
                ["2024-01-02"],
                contract_paths=[],
            )
            path.write_bytes(b"BBBB")
            after = _build_signal_cache_identity(
                data_dir,
                "indicators_cache",
                ["000001"],
                ["2024-01-02"],
                contract_paths=[],
            )

            self.assertNotEqual(before["data_snapshot_id"], after["data_snapshot_id"])
            self.assertEqual(before["universe_id"], after["universe_id"])

    def test_universe_and_calendar_are_independent_identity_dimensions(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory)
            cache = data_dir / "indicators_cache"
            cache.mkdir()
            (cache / "000001.parquet").write_bytes(b"one")
            (cache / "000002.parquet").write_bytes(b"two")

            baseline = _build_signal_cache_identity(
                data_dir,
                "indicators_cache",
                ["000001"],
                ["2024-01-02"],
                contract_paths=[],
            )
            other_universe = _build_signal_cache_identity(
                data_dir,
                "indicators_cache",
                ["000002"],
                ["2024-01-02"],
                contract_paths=[],
            )
            other_calendar = _build_signal_cache_identity(
                data_dir,
                "indicators_cache",
                ["000001"],
                ["2024-01-03"],
                contract_paths=[],
            )

            self.assertNotEqual(baseline["universe_id"], other_universe["universe_id"])
            self.assertNotEqual(baseline["calendar_id"], other_calendar["calendar_id"])

    def test_legacy_or_foreign_payload_is_rejected(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "signals.pkl"
            identity = {
                "schema_version": 1,
                "data_snapshot_id": "data-a",
                "universe_id": "universe-a",
                "calendar_id": "calendar-a",
                "feature_contract_id": "code-a",
                "indicator_cache_name": "indicators_cache",
            }
            signals = {"2024-01-02": [{"code": "000001"}]}

            path.write_bytes(pickle.dumps(signals))
            self.assertIsNone(_load_signal_cache(path, identity))

            _save_signal_cache(path, identity, signals)
            self.assertEqual(signals, _load_signal_cache(path, identity))

            foreign = {**identity, "data_snapshot_id": "data-b"}
            self.assertIsNone(_load_signal_cache(path, foreign))


if __name__ == "__main__":
    unittest.main()
