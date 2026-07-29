from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_automation.data_generation.cache_identity import (
    CACHE_IDENTITY_V1,
    CacheIdentity,
    build_cache_identity,
)
from research_automation.data_generation.contracts import (
    GENERATION_MANIFEST_V1,
    GenerationManifest,
)
from research_automation.data_generation.generation import (
    GenerationMutatedError,
    GenerationPublisher,
)
from research_automation.foundations.artifact_identity import (
    artifact_identity_from_bytes,
)


def _manifest(cutoff: str) -> GenerationManifest:
    return GenerationManifest(
        schema_version=GENERATION_MANIFEST_V1,
        csv_cutoff=cutoff,
        trading_calendar_identity=f"calendar-cn-a-share-{cutoff.replace('-', '')}",
        point_in_time_universe_identity=f"pit-universe-{cutoff.replace('-', '')}",
        adjustment_scheme="qfq-v1",
        missing_data_policy="four-state-v1",
        cache_manifest_references=(f"raw-parquet-production-{cutoff}",),
    )


class GenerationCacheIdentityTests(unittest.TestCase):
    def test_cache_identity_rejects_cache_kind_artifact_kind_mismatch(
        self,
    ) -> None:
        generation_id = "a" * 64
        artifact = artifact_identity_from_bytes(
            b"cache",
            content_schema="parquet.v1",
            producer="tests.raw_parquet",
            generation=generation_id,
            kind="raw_parquet_cache",
            logical_role="ascending_raw_bars",
        )

        with self.assertRaisesRegex(
            ValueError,
            "cache artifact kind must match cache_kind",
        ):
            CacheIdentity(
                schema_version=CACHE_IDENTITY_V1,
                generation_id=generation_id,
                data_snapshot_id=generation_id,
                cache_namespace="production",
                cache_kind="signal",
                artifact=artifact,
                source_artifact_ids=("b" * 64,),
                feature_contract_id="f" * 64,
                trading_calendar_identity="calendar-cn-a-share",
                point_in_time_universe_identity="pit-universe",
                adjustment_scheme="qfq-v1",
            )

    def test_cache_identity_changes_with_generation_and_namespace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_root = root / "data"
            source = data_root / "00" / "000001.csv"
            cache = data_root / "raw_parquet" / "00" / "000001.parquet"
            source.parent.mkdir(parents=True)
            cache.parent.mkdir(parents=True)
            source.write_bytes(b"source")
            cache.write_bytes(b"cache")
            publisher = GenerationPublisher(root / "generations")
            first = _manifest("2026-07-29")
            publisher.publish(publisher.stage(first))

            first_pin = publisher.pin_current(
                expected_generation_id=first.generation_id,
                data_root=data_root,
            )
            try:
                source_identity = first_pin.verify_artifact(
                    "00/000001.csv",
                    content_schema="a-share.gbk_csv.v1",
                    kind="source_csv",
                    logical_role="raw_stock_bars",
                )
                production = build_cache_identity(
                    first_pin,
                    relative_path="raw_parquet/00/000001.parquet",
                    cache_namespace="production",
                    cache_kind="raw_parquet",
                    source_artifact_ids=(source_identity.artifact_id,),
                    feature_contract_id="f" * 64,
                    content_schema="parquet.v1",
                    producer="tests.raw_parquet",
                    logical_role="ascending_raw_bars",
                )
                research = build_cache_identity(
                    first_pin,
                    relative_path="raw_parquet/00/000001.parquet",
                    cache_namespace="research",
                    cache_kind="raw_parquet",
                    source_artifact_ids=(source_identity.artifact_id,),
                    feature_contract_id="f" * 64,
                    content_schema="parquet.v1",
                    producer="tests.raw_parquet",
                    logical_role="ascending_raw_bars",
                )
            finally:
                first_pin.release()

            second = _manifest("2026-07-30")
            publisher.publish(publisher.stage(second))
            second_pin = publisher.pin_current(
                expected_generation_id=second.generation_id,
                data_root=data_root,
            )
            try:
                second_source = second_pin.verify_artifact(
                    "00/000001.csv",
                    content_schema="a-share.gbk_csv.v1",
                    kind="source_csv",
                    logical_role="raw_stock_bars",
                )
                next_generation = build_cache_identity(
                    second_pin,
                    relative_path="raw_parquet/00/000001.parquet",
                    cache_namespace="production",
                    cache_kind="raw_parquet",
                    source_artifact_ids=(second_source.artifact_id,),
                    feature_contract_id="f" * 64,
                    content_schema="parquet.v1",
                    producer="tests.raw_parquet",
                    logical_role="ascending_raw_bars",
                )
            finally:
                second_pin.release()

            self.assertNotEqual(production.cache_id, research.cache_id)
            self.assertNotEqual(production.cache_id, next_generation.cache_id)
            self.assertEqual(first.generation_id, production.generation_id)
            self.assertEqual(second.generation_id, next_generation.generation_id)

    def test_cache_identity_binds_the_declared_producer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_root = root / "data"
            source = data_root / "00" / "000001.csv"
            cache = data_root / "raw_parquet" / "00" / "000001.parquet"
            source.parent.mkdir(parents=True)
            cache.parent.mkdir(parents=True)
            source.write_bytes(b"source")
            cache.write_bytes(b"cache")
            publisher = GenerationPublisher(root / "generations")
            manifest = _manifest("2026-07-30")
            publisher.publish(publisher.stage(manifest))

            identities = []
            for producer in ("tests.raw_parquet.v1", "tests.raw_parquet.v2"):
                with publisher.pin_current(
                    expected_generation_id=manifest.generation_id,
                    data_root=data_root,
                ) as pin:
                    source_identity = pin.verify_artifact(
                        "00/000001.csv",
                        content_schema="a-share.gbk_csv.v1",
                        kind="source_csv",
                        logical_role="raw_stock_bars",
                    )
                    identities.append(
                        build_cache_identity(
                            pin,
                            relative_path="raw_parquet/00/000001.parquet",
                            cache_namespace="production",
                            cache_kind="raw_parquet",
                            source_artifact_ids=(source_identity.artifact_id,),
                            feature_contract_id="f" * 64,
                            content_schema="parquet.v1",
                            producer=producer,
                            logical_role="ascending_raw_bars",
                        )
                    )

            self.assertNotEqual(identities[0].cache_id, identities[1].cache_id)
            self.assertEqual("tests.raw_parquet.v1", identities[0].artifact.producer)
            self.assertEqual("tests.raw_parquet.v2", identities[1].artifact.producer)

    def test_cache_identity_requires_at_least_one_source_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_root = root / "data"
            cache = data_root / "raw_parquet" / "00" / "000001.parquet"
            cache.parent.mkdir(parents=True)
            cache.write_bytes(b"cache")
            publisher = GenerationPublisher(root / "generations")
            manifest = _manifest("2026-07-30")
            publisher.publish(publisher.stage(manifest))

            with publisher.pin_current(
                expected_generation_id=manifest.generation_id,
                data_root=data_root,
            ) as pin:
                with self.assertRaisesRegex(
                    ValueError,
                    "source artifact ids must not be empty",
                ):
                    build_cache_identity(
                        pin,
                        relative_path="raw_parquet/00/000001.parquet",
                        cache_namespace="production",
                        cache_kind="raw_parquet",
                        source_artifact_ids=(),
                        feature_contract_id="f" * 64,
                        content_schema="parquet.v1",
                        producer="tests.raw_parquet",
                        logical_role="ascending_raw_bars",
                    )

    def test_cache_identity_rejects_unverified_source_artifact_id(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_root = root / "data"
            cache = data_root / "raw_parquet" / "00" / "000001.parquet"
            cache.parent.mkdir(parents=True)
            cache.write_bytes(b"cache")
            publisher = GenerationPublisher(root / "generations")
            manifest = _manifest("2026-07-30")
            publisher.publish(publisher.stage(manifest))

            with publisher.pin_current(
                expected_generation_id=manifest.generation_id,
                data_root=data_root,
            ) as pin:
                with self.assertRaisesRegex(
                    ValueError,
                    "source artifact was not verified by this generation pin",
                ):
                    build_cache_identity(
                        pin,
                        relative_path="raw_parquet/00/000001.parquet",
                        cache_namespace="production",
                        cache_kind="raw_parquet",
                        source_artifact_ids=("0" * 64,),
                        feature_contract_id="f" * 64,
                        content_schema="parquet.v1",
                        producer="tests.raw_parquet",
                        logical_role="ascending_raw_bars",
                    )

    def test_cache_identity_reverifies_touched_source_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_root = root / "data"
            source = data_root / "00" / "000001.csv"
            cache = data_root / "raw_parquet" / "00" / "000001.parquet"
            source.parent.mkdir(parents=True)
            cache.parent.mkdir(parents=True)
            source.write_bytes(b"source")
            cache.write_bytes(b"cache")
            publisher = GenerationPublisher(root / "generations")
            manifest = _manifest("2026-07-30")
            publisher.publish(publisher.stage(manifest))

            with publisher.pin_current(
                expected_generation_id=manifest.generation_id,
                data_root=data_root,
            ) as pin:
                source_identity = pin.verify_artifact(
                    "00/000001.csv",
                    content_schema="a-share.gbk_csv.v1",
                    kind="source_csv",
                    logical_role="raw_stock_bars",
                )
                source.write_bytes(b"mutate")

                with self.assertRaisesRegex(
                    GenerationMutatedError,
                    "GENERATION_MUTATED",
                ):
                    build_cache_identity(
                        pin,
                        relative_path="raw_parquet/00/000001.parquet",
                        cache_namespace="production",
                        cache_kind="raw_parquet",
                        source_artifact_ids=(source_identity.artifact_id,),
                        feature_contract_id="f" * 64,
                        content_schema="parquet.v1",
                        producer="tests.raw_parquet",
                        logical_role="ascending_raw_bars",
                    )


if __name__ == "__main__":
    unittest.main()
