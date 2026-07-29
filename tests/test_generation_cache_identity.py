from __future__ import annotations

import tempfile
import unittest
from contextlib import contextmanager
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

from research_automation.data_generation.cache_identity import (
    CACHE_IDENTITY_V1,
    MAX_CACHE_IDENTITY_BYTES,
    CacheIdentity,
    CacheIdentityInvalidError,
    CacheIdentityMismatchError,
    CacheIdentityMissingError,
    build_cache_identity,
    cache_identity_contract_registry,
    load_verified_cache_identity,
    read_cache_identity_sidecar,
    write_cache_identity_sidecar,
)
from research_automation.data_generation.contracts import (
    GENERATION_MANIFEST_V1,
    GenerationManifest,
)
from research_automation.data_generation.generation import (
    GenerationMutatedError,
    GenerationPin,
    GenerationPublisher,
)
from research_automation.foundations.artifact_identity import (
    artifact_identity_from_bytes,
)
from research_automation.foundations.contract_registry import (
    ContractValidationError,
)
from research_automation.control_plane.contracts import canonical_json


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


def _contract_identity() -> CacheIdentity:
    generation_id = "a" * 64
    artifact = artifact_identity_from_bytes(
        b"cache",
        content_schema="parquet.v1",
        producer="tests.raw_parquet",
        generation=generation_id,
        kind="raw_parquet_cache",
        logical_role="ascending_raw_bars",
    )
    return CacheIdentity(
        schema_version=CACHE_IDENTITY_V1,
        generation_id=generation_id,
        data_snapshot_id=generation_id,
        cache_namespace="production",
        cache_kind="raw_parquet",
        artifact=artifact,
        source_artifact_ids=("b" * 64,),
        feature_contract_id="f" * 64,
        trading_calendar_identity="calendar-cn-a-share",
        point_in_time_universe_identity="pit-universe",
        adjustment_scheme="qfq-v1",
    )


def _identity_with_sources(
    identity: CacheIdentity,
    source_artifact_ids: tuple[str, ...],
) -> CacheIdentity:
    return CacheIdentity(
        schema_version=CACHE_IDENTITY_V1,
        generation_id=identity.generation_id,
        data_snapshot_id=identity.data_snapshot_id,
        cache_namespace=identity.cache_namespace,
        cache_kind=identity.cache_kind,
        artifact=identity.artifact,
        source_artifact_ids=source_artifact_ids,
        feature_contract_id=identity.feature_contract_id,
        trading_calendar_identity=identity.trading_calendar_identity,
        point_in_time_universe_identity=(
            identity.point_in_time_universe_identity
        ),
        adjustment_scheme=identity.adjustment_scheme,
    )


def _published_cache(
    root: Path,
) -> tuple[Path, Path, Path, GenerationPublisher, GenerationManifest]:
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
    return data_root, source, cache, publisher, manifest


@contextmanager
def _pinned_identity(
    root: Path,
) -> Iterator[tuple[GenerationPin, Path, CacheIdentity]]:
    data_root, _source, cache, publisher, manifest = _published_cache(root)
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
        identity = build_cache_identity(
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
        yield pin, cache, identity


class GenerationCacheIdentityTests(unittest.TestCase):
    def test_generation_pin_reads_and_returns_the_same_verified_bytes(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_root, _source, cache, publisher, manifest = _published_cache(
                Path(temporary)
            )
            with publisher.pin_current(
                expected_generation_id=manifest.generation_id,
                data_root=data_root,
            ) as pin:
                identity = pin.verify_artifact(
                    "raw_parquet/00/000001.parquet",
                    content_schema="parquet.v1",
                    producer="tests.raw_parquet",
                    kind="raw_parquet_cache",
                    logical_role="ascending_raw_bars",
                )
                self.assertEqual(
                    b"cache",
                    pin.read_verified_bytes(
                        "raw_parquet/00/000001.parquet",
                        identity,
                    ),
                )
                original_read_bytes = Path.read_bytes

                def substituted_bytes(path: Path) -> bytes:
                    if path == cache:
                        return b"other"
                    return original_read_bytes(path)

                with patch.object(Path, "read_bytes", substituted_bytes):
                    with self.assertRaisesRegex(
                        GenerationMutatedError,
                        "GENERATION_MUTATED",
                    ):
                        pin.read_verified_bytes(
                            "raw_parquet/00/000001.parquet",
                            identity,
                        )

    def test_sidecar_writer_rejects_unpinned_absolute_path(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with _pinned_identity(root) as (_pin, _cache, identity):
                outside = root / "outside" / "cache.parquet"
                outside.parent.mkdir()
                outside.write_bytes(b"cache")

                with self.assertRaises(TypeError):
                    write_cache_identity_sidecar(  # type: ignore[call-arg]
                        outside,
                        identity,
                    )

                self.assertFalse(
                    outside.with_name(
                        f"{outside.name}.cache-identity.json"
                    ).exists()
                )

    def test_cache_identity_registry_accepts_expected_a_share_source_count(
        self,
    ) -> None:
        identity = _identity_with_sources(
            _contract_identity(),
            tuple(f"{index:064x}" for index in range(1500)),
        )
        raw = canonical_json(identity.model_dump(mode="json")).encode("utf-8")

        parsed = cache_identity_contract_registry().parse_json(
            CACHE_IDENTITY_V1,
            raw,
        )

        self.assertEqual(identity, parsed)

    def test_generation_pin_rejects_traversal_before_outside_stat(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_root, _source, _cache, publisher, manifest = _published_cache(
                Path(temporary)
            )
            outside_candidate = data_root / ".." / "outside.csv"
            outside_candidate.write_bytes(b"outside")
            outside_stats: list[Path] = []
            original_stat = Path.stat

            def guarded_stat(path: Path, *args: object, **kwargs: object):
                if path == outside_candidate:
                    outside_stats.append(path)
                return original_stat(path, *args, **kwargs)

            with publisher.pin_current(
                expected_generation_id=manifest.generation_id,
                data_root=data_root,
            ) as pin, patch.object(Path, "stat", guarded_stat):
                with self.assertRaisesRegex(
                    GenerationMutatedError,
                    "GENERATION_MUTATED",
                ):
                    pin.verify_artifact(
                        "../outside.csv",
                        content_schema="a-share.gbk_csv.v1",
                        kind="source_csv",
                        logical_role="raw_stock_bars",
                    )

            self.assertEqual([], outside_stats)

    def test_pinned_loader_reconstructs_and_verifies_sidecar_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_root, _source, cache, publisher, manifest = _published_cache(
                Path(temporary)
            )

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
                arguments = {
                    "relative_path": "raw_parquet/00/000001.parquet",
                    "cache_namespace": "production",
                    "cache_kind": "raw_parquet",
                    "source_artifact_ids": (source_identity.artifact_id,),
                    "feature_contract_id": "f" * 64,
                    "content_schema": "parquet.v1",
                    "producer": "tests.raw_parquet",
                    "logical_role": "ascending_raw_bars",
                }
                identity = build_cache_identity(pin, **arguments)
                write_cache_identity_sidecar(
                    pin,
                    relative_path="raw_parquet/00/000001.parquet",
                    identity=identity,
                )

                loaded = load_verified_cache_identity(pin, **arguments)

            self.assertEqual(identity, loaded)

    def test_pinned_loader_rejects_missing_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_root, _source, _cache, publisher, manifest = _published_cache(
                Path(temporary)
            )
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

                with self.assertRaisesRegex(
                    CacheIdentityMissingError,
                    "CACHE_IDENTITY_MISSING",
                ):
                    load_verified_cache_identity(
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

    def test_pinned_loader_rejects_cache_mutated_before_first_touch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_root, _source, cache, publisher, manifest = _published_cache(
                Path(temporary)
            )
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
                arguments = {
                    "relative_path": "raw_parquet/00/000001.parquet",
                    "cache_namespace": "production",
                    "cache_kind": "raw_parquet",
                    "source_artifact_ids": (source_identity.artifact_id,),
                    "feature_contract_id": "f" * 64,
                    "content_schema": "parquet.v1",
                    "producer": "tests.raw_parquet",
                    "logical_role": "ascending_raw_bars",
                }
                identity = build_cache_identity(pin, **arguments)
                write_cache_identity_sidecar(
                    pin,
                    relative_path="raw_parquet/00/000001.parquet",
                    identity=identity,
                )

            cache.write_bytes(b"other")
            with publisher.pin_current(
                expected_generation_id=manifest.generation_id,
                data_root=data_root,
            ) as pin:
                current_source = pin.verify_artifact(
                    "00/000001.csv",
                    content_schema="a-share.gbk_csv.v1",
                    kind="source_csv",
                    logical_role="raw_stock_bars",
                )
                arguments["source_artifact_ids"] = (
                    current_source.artifact_id,
                )

                with self.assertRaisesRegex(
                    CacheIdentityMismatchError,
                    "CACHE_IDENTITY_MISMATCH",
                ):
                    load_verified_cache_identity(pin, **arguments)

    def test_cache_identity_sidecar_round_trips_strict_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with _pinned_identity(Path(temporary)) as (pin, cache, identity):
                sidecar = write_cache_identity_sidecar(
                    pin,
                    relative_path="raw_parquet/00/000001.parquet",
                    identity=identity,
                )

                self.assertEqual(
                    cache.with_name(f"{cache.name}.cache-identity.json"),
                    sidecar,
                )
                self.assertEqual(identity, read_cache_identity_sidecar(cache))

    def test_cache_identity_sidecar_missing_and_malformed_fail_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _data_root, _source, cache, _publisher, _manifest = (
                _published_cache(Path(temporary))
            )

            with self.assertRaisesRegex(
                CacheIdentityMissingError,
                "CACHE_IDENTITY_MISSING",
            ):
                read_cache_identity_sidecar(cache)
            sidecar = cache.with_name(f"{cache.name}.cache-identity.json")
            sidecar.write_bytes(b"{}")
            with self.assertRaisesRegex(
                CacheIdentityInvalidError,
                "CACHE_IDENTITY_INVALID",
            ):
                read_cache_identity_sidecar(cache)

    def test_sidecar_write_rejects_stale_cache_identity_before_replace(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with _pinned_identity(Path(temporary)) as (pin, cache, identity):
                sidecar = write_cache_identity_sidecar(
                    pin,
                    relative_path="raw_parquet/00/000001.parquet",
                    identity=identity,
                )
                before = sidecar.read_bytes()
                cache.write_bytes(b"other")

                with self.assertRaisesRegex(
                    CacheIdentityMismatchError,
                    "CACHE_IDENTITY_MISMATCH",
                ):
                    write_cache_identity_sidecar(
                        pin,
                        relative_path="raw_parquet/00/000001.parquet",
                        identity=identity,
                    )

                self.assertEqual(before, sidecar.read_bytes())

    def test_sidecar_writer_rejects_oversize_before_replacing_valid_sidecar(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with _pinned_identity(Path(temporary)) as (pin, _cache, identity):
                sidecar = write_cache_identity_sidecar(
                    pin,
                    relative_path="raw_parquet/00/000001.parquet",
                    identity=identity,
                )
                before = sidecar.read_bytes()
                oversized = _identity_with_sources(
                    identity,
                    tuple(f"{index:064x}" for index in range(16000)),
                )
                oversized_bytes = canonical_json(
                    oversized.model_dump(mode="json")
                ).encode("utf-8")
                self.assertGreater(
                    len(oversized_bytes),
                    MAX_CACHE_IDENTITY_BYTES,
                )

                with self.assertRaisesRegex(
                    CacheIdentityInvalidError,
                    "CACHE_IDENTITY_INVALID",
                ):
                    write_cache_identity_sidecar(
                        pin,
                        relative_path="raw_parquet/00/000001.parquet",
                        identity=oversized,
                    )

                self.assertEqual(before, sidecar.read_bytes())

    def test_cache_identity_registry_round_trips_canonical_json(self) -> None:
        identity = _contract_identity()
        raw = canonical_json(identity.model_dump(mode="json")).encode("utf-8")

        parsed = cache_identity_contract_registry().parse_json(
            CACHE_IDENTITY_V1,
            raw,
        )

        self.assertEqual(identity, parsed)

    def test_cache_identity_registry_rejects_duplicate_and_foreign_fields(
        self,
    ) -> None:
        identity = _contract_identity()
        payload = identity.model_dump(mode="json")
        foreign = {**payload, "foreign": "not-allowed"}
        duplicate = (
            b'{"schema_version":"'
            + CACHE_IDENTITY_V1.encode("ascii")
            + b'","schema_version":"'
            + CACHE_IDENTITY_V1.encode("ascii")
            + b'",'
            + canonical_json(payload).encode("utf-8")[1:]
        )
        registry = cache_identity_contract_registry()

        with self.assertRaises(ContractValidationError):
            registry.parse_mapping(CACHE_IDENTITY_V1, foreign)
        with self.assertRaises(ContractValidationError):
            registry.parse_json(CACHE_IDENTITY_V1, duplicate)

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

    def test_invalid_request_does_not_poison_a_later_valid_retry(self) -> None:
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
                arguments = {
                    "relative_path": "raw_parquet/00/000001.parquet",
                    "cache_namespace": "production",
                    "source_artifact_ids": (source_identity.artifact_id,),
                    "feature_contract_id": "f" * 64,
                    "content_schema": "parquet.v1",
                    "producer": "tests.raw_parquet",
                    "logical_role": "ascending_raw_bars",
                }

                with self.assertRaises(ValueError):
                    build_cache_identity(
                        pin,
                        cache_kind="bogus",  # type: ignore[arg-type]
                        **arguments,
                    )
                identity = build_cache_identity(
                    pin,
                    cache_kind="raw_parquet",
                    **arguments,
                )

            self.assertEqual("raw_parquet", identity.cache_kind)

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
