from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.foundations.immutable_release import (
    ImmutableReleaseStore,
    ReleaseBusyError,
    ReleaseConflictError,
    ReleaseLeaseExpiredError,
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


def _write_promotion_transaction(
    root: Path,
    *,
    candidate_id: str,
    expected_current_id: str | None,
) -> Path:
    transaction = root / ".promotion.crash.tmp"
    transaction.mkdir()
    (transaction / "transaction.json").write_text(
        json.dumps(
            {
                "schema_version": "immutable_release.transaction.v1",
                "operation": "PROMOTE",
                "candidate_name": candidate_id,
                "candidate_release_id": candidate_id,
                "expected_current_id": expected_current_id,
            }
        ),
        encoding="utf-8",
    )
    return transaction


class ImmutableReleaseStoreTests(unittest.TestCase):
    def test_active_read_lease_prevents_current_from_moving_until_release(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            store = ImmutableReleaseStore(
                root,
                adapter=_ManifestAdapter(),
                lock_timeout_seconds=0.05,
            )
            candidate = store.stage("v2")
            _write_release(candidate, "v2")

            lease = store.acquire_read_lease(expected_release_id="v1")
            with self.assertRaises(ReleaseBusyError):
                store.promote(candidate, expected_current_id="v1")

            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))

            lease.release()
            receipt = store.promote(candidate, expected_current_id="v1")

            self.assertEqual("v2", receipt.release_id)
            self.assertEqual("v2", _ManifestAdapter().validate(root / "current"))

    def test_publication_epoch_fences_released_read_leases(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            lease = store.acquire_read_lease(expected_release_id="v1")

            self.assertEqual("v1", store.validate_read_lease(lease))
            first_token = lease.fencing_token
            lease.release()
            with self.assertRaises(ReleaseLeaseExpiredError):
                store.validate_read_lease(lease)

            candidate = store.stage("v2")
            _write_release(candidate, "v2")
            promotion = store.promote(candidate, expected_current_id="v1")
            next_lease = store.acquire_read_lease(expected_release_id="v2")

            self.assertGreater(promotion.fencing_token, first_token)
            self.assertEqual(promotion.fencing_token, next_lease.fencing_token)
            next_lease.release()

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

    def test_promotion_journal_disk_failure_leaves_no_pending_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            candidate = store.stage("v2")
            _write_release(candidate, "v2")

            with patch(
                "research_automation.foundations.immutable_release.os.fsync",
                side_effect=OSError("injected disk full"),
            ):
                with self.assertRaisesRegex(OSError, "injected disk full"):
                    store.promote(candidate, expected_current_id="v1")

            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertFalse(any(root.glob(".promotion.*.tmp")))

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

    def test_rollback_journal_disk_failure_leaves_no_pending_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v2")
            _write_release(root / "previous", "v1")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())

            with patch(
                "research_automation.foundations.immutable_release.os.fsync",
                side_effect=OSError("injected disk full"),
            ):
                with self.assertRaisesRegex(OSError, "injected disk full"):
                    store.rollback(expected_current_id="v2")

            self.assertEqual("v2", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v1", _ManifestAdapter().validate(root / "previous"))
            self.assertFalse(any(root.glob(".rollback.*.tmp")))

    def test_recover_cancels_rollback_before_the_first_move(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v2")
            _write_release(root / "previous", "v1")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            real_replace = os.replace

            def crash_before_first_move(source: object, target: object) -> None:
                if (
                    Path(source) == root / "current"
                    and Path(target).parent.name.startswith(".rollback.")
                ):
                    raise SystemExit("injected rollback crash")
                real_replace(source, target)

            with patch(
                "research_automation.foundations.immutable_release.os.replace",
                side_effect=crash_before_first_move,
            ):
                with self.assertRaisesRegex(SystemExit, "injected rollback crash"):
                    store.rollback(expected_current_id="v2")

            receipt = store.recover()

            self.assertEqual("ROLLED_BACK_INTERRUPTED_ROLLBACK", receipt.action)
            self.assertEqual("v2", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v1", _ManifestAdapter().validate(root / "previous"))
            self.assertFalse(any(root.glob(".rollback.*.tmp")))

    def test_recover_restores_current_parked_by_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v2")
            _write_release(root / "previous", "v1")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            real_replace = os.replace

            def crash_after_first_move(source: object, target: object) -> None:
                real_replace(source, target)
                if (
                    Path(source) == root / "current"
                    and Path(target).parent.name.startswith(".rollback.")
                ):
                    raise SystemExit("injected rollback crash")

            with patch(
                "research_automation.foundations.immutable_release.os.replace",
                side_effect=crash_after_first_move,
            ):
                with self.assertRaisesRegex(SystemExit, "injected rollback crash"):
                    store.rollback(expected_current_id="v2")

            receipt = store.recover()

            self.assertEqual("ROLLED_BACK_INTERRUPTED_ROLLBACK", receipt.action)
            self.assertEqual("v2", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v1", _ManifestAdapter().validate(root / "previous"))
            self.assertFalse(any(root.glob(".rollback.*.tmp")))

    def test_recover_cancels_rollback_after_previous_moves_to_current(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v2")
            _write_release(root / "previous", "v1")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            real_replace = os.replace

            def crash_after_second_move(source: object, target: object) -> None:
                real_replace(source, target)
                if Path(source) == root / "previous" and Path(target) == root / "current":
                    raise SystemExit("injected rollback crash")

            with patch(
                "research_automation.foundations.immutable_release.os.replace",
                side_effect=crash_after_second_move,
            ):
                with self.assertRaisesRegex(SystemExit, "injected rollback crash"):
                    store.rollback(expected_current_id="v2")

            receipt = store.recover()

            self.assertEqual("ROLLED_BACK_INTERRUPTED_ROLLBACK", receipt.action)
            self.assertEqual("v2", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v1", _ManifestAdapter().validate(root / "previous"))
            self.assertFalse(any(root.glob(".rollback.*.tmp")))

    def test_recover_completes_rollback_after_the_slot_swap(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v2")
            _write_release(root / "previous", "v1")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            real_replace = os.replace

            def crash_after_slot_swap(source: object, target: object) -> None:
                real_replace(source, target)
                if (
                    Path(source).parent.name.startswith(".rollback.")
                    and Path(source).name == "current"
                    and Path(target) == root / "previous"
                ):
                    raise SystemExit("injected rollback crash")

            with patch(
                "research_automation.foundations.immutable_release.os.replace",
                side_effect=crash_after_slot_swap,
            ):
                with self.assertRaisesRegex(SystemExit, "injected rollback crash"):
                    store.rollback(expected_current_id="v2")

            receipt = store.recover()

            self.assertEqual("COMPLETED_INTERRUPTED_ROLLBACK", receipt.action)
            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v2", _ManifestAdapter().validate(root / "previous"))
            self.assertFalse(any(root.glob(".rollback.*.tmp")))

    def test_cleanup_failure_after_rollback_commit_is_recovered_forward(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v2")
            _write_release(root / "previous", "v1")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            real_unlink = Path.unlink

            def fail_transaction_cleanup(path: Path, *args: object, **kwargs: object) -> None:
                if (
                    path.name == "transaction.json"
                    and path.parent.name.startswith(".rollback.")
                ):
                    raise PermissionError("injected transaction cleanup failure")
                real_unlink(path, *args, **kwargs)

            with patch.object(Path, "unlink", new=fail_transaction_cleanup):
                with self.assertRaisesRegex(
                    PermissionError,
                    "injected transaction cleanup failure",
                ):
                    store.rollback(expected_current_id="v2")

            receipt = store.recover()

            self.assertEqual("COMPLETED_INTERRUPTED_ROLLBACK", receipt.action)
            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v2", _ManifestAdapter().validate(root / "previous"))
            self.assertFalse(any(root.glob(".rollback.*.tmp")))

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

    def test_recover_cancels_a_journal_only_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            candidate = root / "candidate" / "v2"
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

            receipt = ImmutableReleaseStore(
                root,
                adapter=_ManifestAdapter(),
            ).recover()

            self.assertEqual("ROLLED_BACK_INTERRUPTED_PROMOTION", receipt.action)
            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertFalse(transaction.exists())

    def test_recover_cleans_an_empty_transaction_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            transaction = root / ".promotion.crash.tmp"
            transaction.mkdir()

            receipt = ImmutableReleaseStore(
                root,
                adapter=_ManifestAdapter(),
            ).recover()

            self.assertEqual("CLEANED_EMPTY_TRANSACTION", receipt.action)
            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertFalse(transaction.exists())

    def test_recover_rejects_a_non_object_transaction_record(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            transaction = root / ".promotion.crash.tmp"
            transaction.mkdir()
            (transaction / "transaction.json").write_text("[]", encoding="utf-8")

            with self.assertRaisesRegex(
                ReleaseConflictError,
                "transaction record is invalid",
            ):
                ImmutableReleaseStore(
                    root,
                    adapter=_ManifestAdapter(),
                ).recover()

            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertTrue(transaction.is_dir())

    def test_recover_rejects_an_operation_that_mismatches_the_transaction_name(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v2")
            _write_release(root / "previous", "v1")
            transaction = root / ".promotion.crash.tmp"
            transaction.mkdir()
            (transaction / "transaction.json").write_text(
                json.dumps(
                    {
                        "schema_version": "immutable_release.transaction.v1",
                        "operation": "ROLLBACK",
                        "current_release_id": "v2",
                        "previous_release_id": "v1",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ReleaseConflictError,
                "transaction record is invalid",
            ):
                ImmutableReleaseStore(
                    root,
                    adapter=_ManifestAdapter(),
                ).recover()

            self.assertEqual("v2", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v1", _ManifestAdapter().validate(root / "previous"))
            self.assertTrue(transaction.is_dir())

    def test_recover_rejects_unexpected_transaction_content_without_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            candidate = root / "candidate" / "v2"
            _write_release(candidate, "v2")
            transaction = _write_promotion_transaction(
                root,
                candidate_id="v2",
                expected_current_id="v1",
            )
            (transaction / "unexpected.txt").write_text("junk", encoding="utf-8")

            with self.assertRaisesRegex(
                ReleaseConflictError,
                "transaction content is invalid",
            ):
                ImmutableReleaseStore(
                    root,
                    adapter=_ManifestAdapter(),
                ).recover()

            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertTrue((transaction / "transaction.json").is_file())
            self.assertTrue((transaction / "unexpected.txt").is_file())

    def test_recover_restores_a_previous_slot_parked_by_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            candidate = root / "candidate" / "v2"
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

            receipt = ImmutableReleaseStore(
                root,
                adapter=_ManifestAdapter(),
            ).recover()

            self.assertEqual("ROLLED_BACK_INTERRUPTED_PROMOTION", receipt.action)
            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertFalse(transaction.exists())

    def test_recover_restores_current_before_candidate_move(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            candidate = root / "candidate" / "v2"
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

            receipt = ImmutableReleaseStore(
                root,
                adapter=_ManifestAdapter(),
            ).recover()

            self.assertEqual("ROLLED_BACK_INTERRUPTED_PROMOTION", receipt.action)
            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v0", _ManifestAdapter().validate(root / "previous"))
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertFalse(transaction.exists())

    def test_recover_fails_closed_when_the_prior_current_was_lost(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "previous", "v0")
            candidate = root / "candidate" / "v2"
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

            with self.assertRaisesRegex(
                ReleaseConflictError,
                "lost CURRENT",
            ):
                ImmutableReleaseStore(
                    root,
                    adapter=_ManifestAdapter(),
                ).recover()

            self.assertFalse((root / "current").exists())
            self.assertFalse((root / "previous").exists())
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertEqual(
                "v0",
                _ManifestAdapter().validate(transaction / "previous"),
            )

    def test_recover_rejects_a_parked_current_with_the_wrong_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            candidate = root / "candidate" / "v2"
            _write_release(candidate, "v2")
            transaction = _write_promotion_transaction(
                root,
                candidate_id="v2",
                expected_current_id="v1",
            )
            os.replace(root / "previous", transaction / "previous")
            os.replace(root / "current", transaction / "current")
            (transaction / "current" / "manifest.json").write_text(
                json.dumps({"release_id": "wrong"}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ReleaseConflictError,
                "parked CURRENT does not match",
            ):
                ImmutableReleaseStore(
                    root,
                    adapter=_ManifestAdapter(),
                ).recover()

            self.assertFalse((root / "current").exists())
            self.assertFalse((root / "previous").exists())
            self.assertEqual("v2", _ManifestAdapter().validate(candidate))
            self.assertEqual(
                "wrong",
                _ManifestAdapter().validate(transaction / "current"),
            )
            self.assertEqual(
                "v0",
                _ManifestAdapter().validate(transaction / "previous"),
            )

    def test_recover_rejects_a_committed_promotion_with_the_wrong_previous(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            candidate = root / "candidate" / "v2"
            _write_release(candidate, "v2")
            transaction = _write_promotion_transaction(
                root,
                candidate_id="v2",
                expected_current_id="v1",
            )
            os.replace(root / "previous", transaction / "previous")
            os.replace(root / "current", transaction / "current")
            os.replace(candidate, root / "current")
            os.replace(transaction / "current", root / "previous")
            (root / "previous" / "manifest.json").write_text(
                json.dumps({"release_id": "wrong"}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ReleaseConflictError,
                "PREVIOUS does not match",
            ):
                ImmutableReleaseStore(
                    root,
                    adapter=_ManifestAdapter(),
                ).recover()

            self.assertEqual("v2", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("wrong", _ManifestAdapter().validate(root / "previous"))
            self.assertEqual(
                "v0",
                _ManifestAdapter().validate(transaction / "previous"),
            )

    def test_recover_rejects_two_previous_occupants_before_moving_slots(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            candidate = root / "candidate" / "v2"
            _write_release(candidate, "v2")
            transaction = _write_promotion_transaction(
                root,
                candidate_id="v2",
                expected_current_id="v1",
            )
            os.replace(root / "previous", transaction / "previous")
            os.replace(root / "current", transaction / "current")
            os.replace(candidate, root / "current")
            _write_release(root / "previous", "wrong")

            with self.assertRaisesRegex(
                ReleaseConflictError,
                "previous slot has two occupants",
            ):
                ImmutableReleaseStore(
                    root,
                    adapter=_ManifestAdapter(),
                ).recover()

            self.assertEqual("v2", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("wrong", _ManifestAdapter().validate(root / "previous"))
            self.assertFalse(candidate.exists())
            self.assertEqual(
                "v1",
                _ManifestAdapter().validate(transaction / "current"),
            )
            self.assertEqual(
                "v0",
                _ManifestAdapter().validate(transaction / "previous"),
            )

    def test_recover_cancels_first_publication_before_candidate_move(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            candidate = root / "candidate" / "v1"
            _write_release(candidate, "v1")
            transaction = _write_promotion_transaction(
                root,
                candidate_id="v1",
                expected_current_id=None,
            )

            receipt = ImmutableReleaseStore(
                root,
                adapter=_ManifestAdapter(),
            ).recover()

            self.assertEqual("ROLLED_BACK_INTERRUPTED_PROMOTION", receipt.action)
            self.assertFalse((root / "current").exists())
            self.assertEqual("v1", _ManifestAdapter().validate(candidate))
            self.assertFalse(transaction.exists())

    def test_recover_completes_first_publication_after_candidate_move(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            candidate = root / "candidate" / "v1"
            _write_release(candidate, "v1")
            transaction = _write_promotion_transaction(
                root,
                candidate_id="v1",
                expected_current_id=None,
            )
            os.replace(candidate, root / "current")

            receipt = ImmutableReleaseStore(
                root,
                adapter=_ManifestAdapter(),
            ).recover()

            self.assertEqual("COMPLETED_INTERRUPTED_PROMOTION", receipt.action)
            self.assertEqual("v1", _ManifestAdapter().validate(root / "current"))
            self.assertFalse(candidate.exists())
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

    def test_cleanup_failure_after_promotion_commit_is_recovered_forward(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "releases"
            _write_release(root / "current", "v1")
            _write_release(root / "previous", "v0")
            store = ImmutableReleaseStore(root, adapter=_ManifestAdapter())
            candidate = store.stage("v2")
            _write_release(candidate, "v2")
            real_replace = os.replace

            def fail_archive_cleanup(source: object, target: object) -> None:
                source_path = Path(source)
                if (
                    source_path.name == "previous"
                    and source_path.parent.name.startswith(".promotion.")
                    and Path(target).parent == root / "archive"
                ):
                    raise PermissionError("injected archive cleanup failure")
                real_replace(source, target)

            with patch(
                "research_automation.foundations.immutable_release.os.replace",
                side_effect=fail_archive_cleanup,
            ):
                with self.assertRaisesRegex(
                    PermissionError,
                    "injected archive cleanup failure",
                ):
                    store.promote(candidate, expected_current_id="v1")

            receipt = store.recover()

            self.assertEqual("COMPLETED_INTERRUPTED_PROMOTION", receipt.action)
            self.assertEqual("v2", _ManifestAdapter().validate(root / "current"))
            self.assertEqual("v1", _ManifestAdapter().validate(root / "previous"))
            self.assertFalse(candidate.exists())

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
