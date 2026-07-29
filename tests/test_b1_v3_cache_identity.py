from __future__ import annotations

import json
import pickle
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from strategy.b1_v3_config import B1V3Params
from strategy import b1_v3_strategy as strategy


class _EmptyPool:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def imap_unordered(self, function, arguments, chunksize=1):
        return iter(())


class _FailPool:
    def __init__(self, *args, **kwargs):
        raise AssertionError("valid cache hit must not start an extraction pool")


class _CallbackPool(_EmptyPool):
    callback = None

    def imap_unordered(self, function, arguments, chunksize=1):
        callback = type(self).callback
        if callback is not None:
            callback()
        return iter(())


class B1V3CacheIdentityTests(unittest.TestCase):
    def setUp(self):
        auxiliary_patch = patch.object(strategy, "RAW_CACHE_AUXILIARY_PATHS", ())
        auxiliary_patch.start()
        self.addCleanup(auxiliary_patch.stop)

    def test_identical_identity_reuses_valid_raw_cache(self):
        with TemporaryDirectory() as temp:
            cache_dir = Path(temp)
            params = B1V3Params()

            with (
                patch.object(strategy, "RAW_CACHE_DIR", cache_dir),
                patch.object(strategy.mp, "Pool", _EmptyPool),
            ):
                expected = strategy.build_raw_cache(
                    [], "2024-01-01", "2024-06-30", params, 1
                )

            with (
                patch.object(strategy, "RAW_CACHE_DIR", cache_dir),
                patch.object(strategy.mp, "Pool", _FailPool),
            ):
                actual = strategy.build_raw_cache(
                    [], "2024-01-01", "2024-06-30", params, 1
                )

            self.assertEqual(expected, actual)

    def test_same_window_with_different_parameters_creates_distinct_raw_caches(self):
        with TemporaryDirectory() as temp:
            cache_dir = Path(temp)
            baseline = B1V3Params()
            variant = B1V3Params()
            variant.j_max = baseline.j_max - 1

            with (
                patch.object(strategy, "RAW_CACHE_DIR", cache_dir),
                patch.object(strategy.mp, "Pool", _EmptyPool),
            ):
                strategy.build_raw_cache([], "2024-01-01", "2024-06-30", baseline, 1)
                strategy.build_raw_cache([], "2024-01-01", "2024-06-30", variant, 1)

            cache_files = sorted(cache_dir.glob("*.pkl"))
            metadata_files = sorted(cache_dir.glob("*.meta.json"))
            self.assertEqual(2, len(cache_files))
            self.assertEqual(2, len(metadata_files))
            metadata = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in metadata_files
            ]
            self.assertEqual(2, len({item["param_fingerprint"] for item in metadata}))
            self.assertEqual(
                sorted([baseline.j_max, variant.j_max]),
                sorted(item["params"]["j_max"] for item in metadata),
            )

    def test_same_window_and_parameters_with_different_universes_creates_distinct_raw_caches(self):
        with TemporaryDirectory() as temp:
            cache_dir = Path(temp)
            params = B1V3Params()

            with (
                patch.object(strategy, "RAW_CACHE_DIR", cache_dir),
                patch.object(strategy.mp, "Pool", _EmptyPool),
            ):
                strategy.build_raw_cache(
                    ["000001"], "2024-01-01", "2024-06-30", params, 1
                )
                strategy.build_raw_cache(
                    ["000001", "600000"],
                    "2024-01-01",
                    "2024-06-30",
                    params,
                    1,
                )

            metadata_files = sorted(cache_dir.glob("*.meta.json"))
            self.assertEqual(2, len(metadata_files))
            metadata = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in metadata_files
            ]
            self.assertEqual(2, len({item["universe_fingerprint"] for item in metadata}))
            self.assertEqual([1, 2], sorted(item["n_stocks"] for item in metadata))

    def test_changed_indicator_data_creates_a_distinct_raw_cache(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            cache_dir = root / "signal-cache"
            indicators_dir = root / "indicators"
            indicators_dir.mkdir()
            indicator_path = indicators_dir / "000001.parquet"
            indicator_path.write_bytes(b"first-indicator-snapshot")
            params = B1V3Params()

            with (
                patch.object(strategy, "RAW_CACHE_DIR", cache_dir),
                patch.object(strategy, "INDICATORS_DIR", indicators_dir),
                patch.object(strategy.mp, "Pool", _EmptyPool),
            ):
                strategy.build_raw_cache(
                    ["000001"], "2024-01-01", "2024-06-30", params, 1
                )
                indicator_path.write_bytes(b"second-indicator-snapshot")
                strategy.build_raw_cache(
                    ["000001"], "2024-01-01", "2024-06-30", params, 1
                )

            metadata = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in sorted(cache_dir.glob("*.meta.json"))
            ]
            self.assertEqual(2, len(metadata))
            self.assertEqual(
                2, len({item["data_snapshot_fingerprint"] for item in metadata})
            )

    def test_corrupt_raw_cache_is_rebuilt_instead_of_loaded(self):
        with TemporaryDirectory() as temp:
            cache_dir = Path(temp)
            params = B1V3Params()

            with (
                patch.object(strategy, "RAW_CACHE_DIR", cache_dir),
                patch.object(strategy.mp, "Pool", _EmptyPool),
            ):
                strategy.build_raw_cache([], "2024-01-01", "2024-06-30", params, 1)
                cache_path = next(cache_dir.glob("*.pkl"))
                cache_path.write_bytes(b"not-a-pickle")
                actual = strategy.build_raw_cache(
                    [], "2024-01-01", "2024-06-30", params, 1
                )

            self.assertEqual(({}, []), actual)
            with cache_path.open("rb") as handle:
                rebuilt = pickle.load(handle)
            self.assertIsInstance(rebuilt, dict)
            self.assertEqual(1, rebuilt["schema_version"])

    def test_input_change_during_extraction_is_not_published(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            cache_dir = root / "signal-cache"
            indicators_dir = root / "indicators"
            indicators_dir.mkdir()
            indicator_path = indicators_dir / "000001.parquet"
            indicator_path.write_bytes(b"before-extraction")
            _CallbackPool.callback = lambda: indicator_path.write_bytes(
                b"during-extraction"
            )

            try:
                with (
                    patch.object(strategy, "RAW_CACHE_DIR", cache_dir),
                    patch.object(strategy, "INDICATORS_DIR", indicators_dir),
                    patch.object(strategy.mp, "Pool", _CallbackPool),
                ):
                    with self.assertRaisesRegex(RuntimeError, "changed during extraction"):
                        strategy.build_raw_cache(
                            ["000001"],
                            "2024-01-01",
                            "2024-06-30",
                            B1V3Params(),
                            1,
                        )
            finally:
                _CallbackPool.callback = None

            self.assertEqual([], list(cache_dir.glob("*.pkl")))
            self.assertEqual([], list(cache_dir.glob("*.meta.json")))


if __name__ == "__main__":
    unittest.main()
