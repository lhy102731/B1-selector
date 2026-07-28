from __future__ import annotations

import hashlib
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pydantic import ValidationError

import research_automation.foundations.artifact_identity as artifact_module

from research_automation.foundations.artifact_identity import (
    ArtifactIdentity,
    ArtifactIdentityMismatchError,
    ArtifactLocationError,
    ArtifactLocator,
    DirectoryManifestEntry,
    InventoryOnlyFingerprint,
    artifact_identity_from_bytes,
    build_directory_manifest,
    directory_identity_from_manifest,
    identify_file,
    inventory_fingerprint,
    verify_file_identity,
)


class ArtifactIdentityTests(unittest.TestCase):
    def test_bytes_produce_a_full_content_bound_identity(self) -> None:
        identity = artifact_identity_from_bytes(
            b"alpha",
            content_schema="research.example_payload.v1",
            producer="unit-test",
            generation="generation-1",
            kind="report",
            logical_role="test-evidence",
        )

        self.assertIsInstance(identity, ArtifactIdentity)
        self.assertEqual(identity.content_sha256, hashlib.sha256(b"alpha").hexdigest())
        self.assertEqual(identity.byte_length, 5)
        self.assertRegex(identity.artifact_id, r"^[0-9a-f]{64}$")

    def test_each_semantic_binding_participates_in_the_artifact_id(self) -> None:
        base = {
            "content_schema": "research.example_payload.v1",
            "producer": "unit-test",
            "generation": "generation-1",
            "kind": "report",
            "logical_role": "test-evidence",
        }
        baseline = artifact_identity_from_bytes(b"alpha", **base)
        alternatives = {
            "content_schema": "research.example_payload.v2",
            "producer": "another-producer",
            "generation": "generation-2",
            "kind": "dataset",
            "logical_role": "training-input",
        }

        for field_name, value in alternatives.items():
            changed = artifact_identity_from_bytes(
                b"alpha",
                **{**base, field_name: value},
            )
            with self.subTest(field_name=field_name):
                self.assertNotEqual(baseline.artifact_id, changed.artifact_id)

    def test_locator_metadata_is_separate_from_content_identity(self) -> None:
        with TemporaryDirectory() as first_root, TemporaryDirectory() as second_root:
            first = ArtifactLocator(
                schema_version="research.artifact_locator.v1",
                storage_root=Path(first_root).as_posix(),
                path="first/result.json",
                size_bytes=5,
                mtime_ns=1,
            )
            second = ArtifactLocator(
                schema_version="research.artifact_locator.v1",
                storage_root=Path(second_root).as_posix(),
                path="moved/result.json",
                size_bytes=5,
                mtime_ns=999,
            )
            first_identity = artifact_identity_from_bytes(
                b"alpha",
                content_schema="research.example_payload.v1",
                producer="unit-test",
                generation="generation-1",
                kind="report",
                logical_role="test-evidence",
            )
            second_identity = artifact_identity_from_bytes(
                b"alpha",
                content_schema="research.example_payload.v1",
                producer="unit-test",
                generation="generation-1",
                kind="report",
                logical_role="test-evidence",
            )

        self.assertNotEqual(first.path, second.path)
        self.assertEqual(first_identity.artifact_id, second_identity.artifact_id)
        self.assertNotIn("path", type(first_identity).model_fields)

    def test_identifying_files_is_location_independent_and_content_sensitive(self) -> None:
        with TemporaryDirectory() as first_root, TemporaryDirectory() as second_root:
            first_path = Path(first_root) / "first.bin"
            moved_path = Path(second_root) / "moved.bin"
            changed_path = Path(second_root) / "changed.bin"
            first_path.write_bytes(b"alpha")
            moved_path.write_bytes(b"alpha")
            changed_path.write_bytes(b"bravo")

            def locator(root: str, path: Path) -> ArtifactLocator:
                stat = path.stat()
                return ArtifactLocator(
                    schema_version="research.artifact_locator.v1",
                    storage_root=Path(root).as_posix(),
                    path=path.name,
                    size_bytes=stat.st_size,
                    mtime_ns=stat.st_mtime_ns,
                )

            identities = [
                identify_file(
                    locator(root, path),
                    content_schema="research.binary.v1",
                    producer="unit-test",
                    generation="generation-1",
                    kind="file",
                    logical_role="input",
                )
                for root, path in (
                    (first_root, first_path),
                    (second_root, moved_path),
                    (second_root, changed_path),
                )
            ]

        self.assertEqual(identities[0].artifact_id, identities[1].artifact_id)
        self.assertNotEqual(identities[0].artifact_id, identities[2].artifact_id)

    def test_directory_identity_uses_a_canonical_ordered_manifest(self) -> None:
        first = DirectoryManifestEntry(
            path="b.bin",
            content_sha256=hashlib.sha256(b"bravo").hexdigest(),
            byte_length=5,
        )
        second = DirectoryManifestEntry(
            path="a.bin",
            content_sha256=hashlib.sha256(b"alpha").hexdigest(),
            byte_length=5,
        )

        left = build_directory_manifest([first, second])
        right = build_directory_manifest([second, first])
        left_identity = directory_identity_from_manifest(
            left,
            producer="unit-test",
            generation="generation-1",
            logical_role="evidence-bundle",
        )
        right_identity = directory_identity_from_manifest(
            right,
            producer="unit-test",
            generation="generation-1",
            logical_role="evidence-bundle",
        )

        self.assertEqual([entry.path for entry in left.entries], ["a.bin", "b.bin"])
        self.assertEqual(left_identity.artifact_id, right_identity.artifact_id)

    def test_drive_root_locator_remains_absolute_after_canonicalization(self) -> None:
        drive_root = Path(Path.cwd().anchor).as_posix()

        locator = ArtifactLocator(
            schema_version="research.artifact_locator.v1",
            storage_root=drive_root,
            path="artifact.bin",
            size_bytes=0,
            mtime_ns=0,
        )

        self.assertTrue(Path(locator.storage_root).is_absolute())

    def test_locator_rejects_windows_path_aliases_and_escape_syntax(self) -> None:
        root = Path.cwd().as_posix()
        invalid_paths = (
            "../artifact.bin",
            "C:/artifact.bin",
            "artifact.bin:stream",
            "folder/artifact.bin.",
            "folder/artifact.bin ",
            "NUL",
            "folder/con.txt",
        )

        for path in invalid_paths:
            with self.subTest(path=path):
                with self.assertRaises(ValidationError):
                    ArtifactLocator(
                        schema_version="research.artifact_locator.v1",
                        storage_root=root,
                        path=path,
                        size_bytes=0,
                        mtime_ns=0,
                    )

    def test_directory_manifest_rejects_windows_case_collisions(self) -> None:
        digest = hashlib.sha256(b"alpha").hexdigest()
        entries = [
            DirectoryManifestEntry(
                path="Report.bin",
                content_sha256=digest,
                byte_length=5,
            ),
            DirectoryManifestEntry(
                path="report.bin",
                content_sha256=digest,
                byte_length=5,
            ),
        ]

        with self.assertRaisesRegex(ValidationError, "Windows-unique"):
            build_directory_manifest(entries)

    def test_stale_locator_metadata_cannot_produce_an_identity(self) -> None:
        with TemporaryDirectory() as root:
            path = Path(root) / "artifact.bin"
            path.write_bytes(b"alpha")
            stat = path.stat()
            locator = ArtifactLocator(
                schema_version="research.artifact_locator.v1",
                storage_root=Path(root).as_posix(),
                path=path.name,
                size_bytes=stat.st_size,
                mtime_ns=stat.st_mtime_ns + 1,
            )

            with self.assertRaisesRegex(ArtifactLocationError, "stale"):
                identify_file(
                    locator,
                    content_schema="research.binary.v1",
                    producer="unit-test",
                    generation="generation-1",
                    kind="file",
                    logical_role="input",
                )

    def test_reparse_component_cannot_produce_an_identity(self) -> None:
        with TemporaryDirectory() as root, TemporaryDirectory() as outside:
            target = Path(outside) / "artifact.bin"
            target.write_bytes(b"alpha")
            link = Path(root) / "linked"
            try:
                link.symlink_to(Path(outside), target_is_directory=True)
            except OSError as error:
                self.skipTest(f"symlink creation unavailable: {error}")
            stat = target.stat()
            locator = ArtifactLocator(
                schema_version="research.artifact_locator.v1",
                storage_root=Path(root).as_posix(),
                path="linked/artifact.bin",
                size_bytes=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
            )

            with self.assertRaisesRegex(ArtifactLocationError, "reparse"):
                identify_file(
                    locator,
                    content_schema="research.binary.v1",
                    producer="unit-test",
                    generation="generation-1",
                    kind="file",
                    logical_role="input",
                )

    def test_parent_swap_after_resolution_cannot_hash_outside_the_root(self) -> None:
        with TemporaryDirectory() as base:
            base_path = Path(base)
            root = base_path / "root"
            outside = base_path / "outside"
            parent = root / "nested"
            parent.mkdir(parents=True)
            outside.mkdir()
            inside_file = parent / "artifact.bin"
            outside_file = outside / "artifact.bin"
            inside_file.write_bytes(b"alpha")
            outside_file.write_bytes(b"bravo")
            stat = inside_file.stat()
            os.utime(
                outside_file,
                ns=(stat.st_atime_ns, stat.st_mtime_ns),
            )
            locator = ArtifactLocator(
                schema_version="research.artifact_locator.v1",
                storage_root=root.as_posix(),
                path="nested/artifact.bin",
                size_bytes=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
            )
            original_resolver = artifact_module._resolve_bounded_locator_preflight

            def swap_parent_after_resolution(value: ArtifactLocator) -> Path:
                resolved = original_resolver(value)
                parent.rename(root / "parked")
                try:
                    parent.symlink_to(outside, target_is_directory=True)
                except OSError as error:
                    self.skipTest(f"symlink creation unavailable: {error}")
                return resolved

            with patch.object(
                artifact_module,
                "_resolve_bounded_locator_preflight",
                side_effect=swap_parent_after_resolution,
            ):
                with self.assertRaisesRegex(
                    ArtifactLocationError,
                    "escaped|containment",
                ):
                    identify_file(
                        locator,
                        content_schema="research.binary.v1",
                        producer="unit-test",
                        generation="generation-1",
                        kind="file",
                        logical_role="input",
                    )

    def test_inventory_fingerprint_cannot_enter_a_trusted_identity_check(self) -> None:
        with TemporaryDirectory() as root:
            path = Path(root) / "artifact.bin"
            path.write_bytes(b"alpha")
            stat = path.stat()
            locator = ArtifactLocator(
                schema_version="research.artifact_locator.v1",
                storage_root=Path(root).as_posix(),
                path=path.name,
                size_bytes=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
            )
            fingerprint = inventory_fingerprint(locator)

            self.assertIsInstance(fingerprint, InventoryOnlyFingerprint)
            self.assertFalse(fingerprint.authorization_eligible)
            self.assertNotIsInstance(fingerprint, ArtifactIdentity)
            with self.assertRaisesRegex(TypeError, "ArtifactIdentity"):
                verify_file_identity(locator, fingerprint)

    def test_full_identity_check_rehashes_current_bytes(self) -> None:
        with TemporaryDirectory() as root:
            path = Path(root) / "artifact.bin"
            path.write_bytes(b"alpha")
            stat = path.stat()
            locator = ArtifactLocator(
                schema_version="research.artifact_locator.v1",
                storage_root=Path(root).as_posix(),
                path=path.name,
                size_bytes=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
            )
            expected = artifact_identity_from_bytes(
                b"alpha",
                content_schema="research.binary.v1",
                producer="unit-test",
                generation="generation-1",
                kind="file",
                logical_role="input",
            )
            wrong = artifact_identity_from_bytes(
                b"bravo",
                content_schema="research.binary.v1",
                producer="unit-test",
                generation="generation-1",
                kind="file",
                logical_role="input",
            )

            self.assertIsNone(verify_file_identity(locator, expected))
            with self.assertRaises(ArtifactIdentityMismatchError):
                verify_file_identity(locator, wrong)


if __name__ == "__main__":
    unittest.main()
