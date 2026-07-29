from __future__ import annotations

import pickle
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from research_automation.data_generation.cache_identity import (
    build_cache_identity,
)
from research_automation.data_generation.contracts import (
    GENERATION_MANIFEST_V1,
    GenerationManifest,
)
from research_automation.data_generation.generation import GenerationPublisher
from backtest_optimized import (
    _build_signal_cache_identity,
    _load_signal_cache,
    _save_signal_cache,
)


def _manifest(cutoff: str) -> GenerationManifest:
    return GenerationManifest(
        schema_version=GENERATION_MANIFEST_V1,
        csv_cutoff=cutoff,
        trading_calendar_identity=f"calendar-{cutoff}",
        point_in_time_universe_identity=f"universe-{cutoff}",
        adjustment_scheme="qfq-v1",
        missing_data_policy="four-state-v1",
        cache_manifest_references=("signal-cache",),
    )


class SignalCacheIdentityTests(unittest.TestCase):
    def test_pinned_signal_identity_changes_with_generation_and_namespace(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data"
            indicator = data_root / "indicators_cache" / "000001.parquet"
            signal = data_root / "signal_cache" / "signals.pkl"
            indicator.parent.mkdir(parents=True)
            signal.parent.mkdir(parents=True)
            indicator.write_bytes(b"indicator")
            signal.write_bytes(b"signal")
            publisher = GenerationPublisher(root / "generations")
            first = _manifest("2026-07-29")
            publisher.publish(publisher.stage(first))

            def identify(manifest, namespace):
                with publisher.pin_current(
                    expected_generation_id=manifest.generation_id,
                    data_root=data_root,
                ) as pin:
                    source = pin.verify_artifact(
                        "indicators_cache/000001.parquet",
                        content_schema="parquet.indicators.v1",
                        producer="tests.indicator_builder",
                        kind="indicator_cache",
                        logical_role="b1_indicator_frame",
                    )
                    return build_cache_identity(
                        pin,
                        relative_path="signal_cache/signals.pkl",
                        cache_namespace=namespace,
                        cache_kind="signal",
                        source_artifact_ids=(source.artifact_id,),
                        feature_contract_id="f" * 64,
                        content_schema="pickle.signal_cache.v1",
                        producer="tests.signal_builder",
                        logical_role="precomputed_b1_signals",
                    )

            production = identify(first, "production")
            research = identify(first, "research")
            second = _manifest("2026-07-30")
            publisher.publish(publisher.stage(second))
            next_generation = identify(second, "production")

            self.assertEqual("signal_cache", production.artifact.kind)
            self.assertNotEqual(production.cache_id, research.cache_id)
            self.assertNotEqual(production.cache_id, next_generation.cache_id)

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
