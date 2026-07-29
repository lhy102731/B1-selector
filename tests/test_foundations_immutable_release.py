from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.foundations.immutable_release import (
    ImmutableReleaseStore,
    ReleaseConflictError,
)


class _ManifestAdapter:
    def validate(self, release: Path) -> str:
        document = json.loads((release / "manifest.json").read_text(encoding="utf-8"))
        return str(document["release_id"])


def _write_release(path: Path, release_id: str) -> None:
    path.mkdir(parents=True)
    (path / "manifest.json").write_text(
        json.dumps({"release_id": release_id}),
        encoding="utf-8",
    )


class ImmutableReleaseStoreTests(unittest.TestCase):
    def test_promote_failure_restores_current_previous_and_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            candidate = store.stage("v2")
            _write_release(candidate, "v2")
            real_replace = os.replace

            def fail_candidate_promotion(source: object, target: object) -> None:
                if Path(source) == candidate and Path(target) == root / "current":
                    raise PermissionError("injected candidate promotion failure")
                real_replace(source, target)

            with patch(
                "research_automation.foundations.immutable_release.os.replace",
                side_effect=fail_candidate_promotion,
            ):
                with self.assertRaisesRegex(
                    PermissionError,
                    "injected candidate promotion failure",
                ):
                    store.promote(candidate, expected_current_id="v1")

            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertEqual(
                [".publish.lock", "candidate", "current", "previous"],
                sorted(path.name for path in root.iterdir()),
            )

    def test_rollback_swaps_current_and_previous_after_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            candidate = store.stage("v2")
            _write_release(candidate, "v2")

            store.promote(candidate, expected_current_id="v1")
            receipt = store.rollback(expected_current_id="v2")

            self.assertEqual("v1", receipt.release_id)
            self.assertEqual("v2", receipt.previous_release_id)
            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v2", _ManifestAdapter().validate(root / "previous"))

    def test_recover_restores_an_interrupted_promotion_without_lock_age(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            candidate = store.stage("v2")
            _write_release(candidate, "v2")
            transaction = root / ".promotion.crash.tmp"
            transaction.mkdir()
            (transaction / "transaction.json").write_text(
                json.dumps(
                    {
                        "schema_version": "immutable_release.transaction.v1",
                        "operation": "PROMOTE",
                        "candidate_name": "v2",
                        "candidate_release_id": "v2",
                        "expected_current_id": "v1",
                    }
                ),
                encoding="utf-8",
            )
            os.replace(root / "previous", transaction / "previous")
            os.replace(root / "current", transaction / "current")
            os.replace(candidate, root / "current")
            (root / ".publish.lock").write_text("stale marker", encoding="ascii")

            receipt = store.recover()

            self.assertEqual("ROLLED_BACK_INTERRUPTED_PROMOTION", receipt.action)
            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertFalse(transaction.exists())

    def test_late_promotion_failure_restores_archived_previous_slot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            candidate = store.stage("v2")
            _write_release(candidate, "v2")
            real_replace = os.replace

            def fail_previous_rotation(source: object, target: object) -> None:
                source_path = Path(source)
                if (
                    source_path.name == "current"
                    and source_path.parent.name.startswith(".promotion.")
                    and Path(target) == root / "previous"
                ):
                    raise PermissionError("injected previous rotation failure")
                real_replace(source, target)

            with patch(
                "research_automation.foundations.immutable_release.os.replace",
                side_effect=fail_previous_rotation,
            ):
                with self.assertRaisesRegex(
                    PermissionError,
                    "injected previous rotation failure",
                ):
                    store.promote(candidate, expected_current_id="v1")

            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))

    def test_public_operations_keep_one_stable_lock_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            candidate = store.stage("v2")
            _write_release(candidate, "v2")

            store.promote(candidate, expected_current_id="v1")
            lock = root / ".publish.lock"
            first_identity = lock.stat().st_ino
            store.rollback(expected_current_id="v2")

            self.assertTrue(lock.is_file())
            self.assertEqual(first_identity, lock.stat().st_ino)

    def test_candidate_identity_cannot_change_while_acquiring_the_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            candidate = root / "candidate" / "v2"
            _write_release(candidate, "v2")

            class MutatingAdapter(_ManifestAdapter):
                mutated = False

                def validate(self, release: Path) -> str:
                    release_id = super().validate(release)
                    if release == candidate and not self.mutated:
                        self.mutated = True
                        (release / "manifest.json").write_text(
                            json.dumps({"release_id": "v3"}),
                            encoding="utf-8",
                        )
                    return release_id

            store = ImmutableReleaseStore(root, adapter=MutatingAdapter())

            with self.assertRaisesRegex(
                ReleaseConflictError,
                "candidate identity changed",
            ):
                store.promote(
                    candidate,
                    expected_current_id="v1",
                    expected_candidate_id="v2",
                )

            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v3", _ManifestAdapter().validate(candidate))

    def test_pending_transaction_blocks_a_new_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            candidate = root / "candidate" / "v2"
            _write_release(candidate, "v2")
            pending = root / ".promotion.crash.tmp"
            pending.mkdir()

            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())

            with self.assertRaisesRegex(
                ReleaseConflictError,
                "recovery required",
            ):
                store.promote(candidate, expected_current_id="v1")

            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertTrue(pending.is_dir())

    def test_pending_transaction_blocks_a_new_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v2")
            _write_release(root / "previous", "v1")
            pending = root / ".rollback.crash.tmp"
            pending.mkdir()

            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())

            with self.assertRaisesRegex(
                ReleaseConflictError,
                "recovery required",
            ):
                store.rollback(expected_current_id="v2")

            self.assertEqual("v2", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v1", _ManifestAdapter().validate(root / "previous"))
            self.assertTrue(pending.is_dir())


if __name__ == "__main__":
    unittest.main()
