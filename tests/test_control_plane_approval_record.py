"""CR-010 F-06: user approval record verification before issuance.

The coordinator must consume an unfakeable user exact-candidate approval
record before issuing any grant/ticket; a missing record keeps the legacy
provenance claim, a tampered/wrong-candidate record fails closed.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.approval_record_verifier import (
    ApprovalRecordError,
    ApprovalRecordVerifier,
)
from tests.test_control_plane_activation_coordinator import (
    GIT,
    ROOT_SECRET,
    _StoresFixture,
    _build_envelope,
)


class ApprovalRecordVerifierTests(unittest.TestCase):
    """Unit tests for the verifier itself (has/has-not/tamper branches)."""

    def _repo(self, root: Path):
        repo = root / "repo"
        repo.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
        subprocess.run(
            ["git", "-C", str(repo), "config", "user.email",
             "tests@example.invalid"],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(repo), "config", "user.name",
             "approval-record-tests"],
            check=True,
        )
        return repo

    def _commit(self, repo: Path, name: str, content: str) -> str:
        path = repo / name
        path.write_text(content, encoding="utf-8")
        subprocess.run(["git", "-C", str(repo), "add", "--", name], check=True)
        subprocess.run(
            ["git", "-C", str(repo), "commit", "-q", "-m", name],
            check=True,
        )
        return subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
        ).stdout.strip()

    def _manifest_blob_sha256(self, root: Path, envelope: str) -> str:
        """SHA-256 of the committed manifest blob (git-normalized bytes)."""
        oid = subprocess.run(
            ["git", "-C", str(root / "repo"), "rev-parse",
             f"{envelope}:manifest.json"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        blob = subprocess.run(
            ["git", "-C", str(root / "repo"), "cat-file", "blob", oid],
            capture_output=True,
        ).stdout
        return hashlib.sha256(blob).hexdigest()

    def _valid_record_bytes(
        self,
        root: Path,
        envelope: str,
        manifest_sha256: str,
    ) -> bytes:
        verifier = ApprovalRecordVerifier(root / "repo")
        tree = subprocess.run(
            ["git", "-C", str(root / "repo"), "rev-parse",
             envelope + "^{tree}"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        document = verifier.build_record(
            envelope_commit=envelope,
            manifest_sha256=manifest_sha256,
            candidate_tree=tree,
            actor="user",
        )
        return verifier.serialize(document)

    def test_valid_record_verifies(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._repo(root)
            envelope = self._commit(repo, "manifest.json", "{}")
            manifest_sha256 = self._manifest_blob_sha256(root, envelope)
            raw = self._valid_record_bytes(root, envelope, manifest_sha256)
            verifier = ApprovalRecordVerifier(root / "repo")
            result = verifier.verify_record_bytes(
                raw,
                envelope_commit=envelope,
                manifest_sha256=manifest_sha256,
            )
            self.assertEqual(result, hashlib.sha256(raw).hexdigest())

    def test_wrong_envelope_fails_closed(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._repo(root)
            envelope = self._commit(repo, "manifest.json", "{}")
            other = self._commit(repo, "other.txt", "x")
            manifest_sha256 = self._manifest_blob_sha256(root, envelope)
            raw = self._valid_record_bytes(root, envelope, manifest_sha256)
            verifier = ApprovalRecordVerifier(root / "repo")
            with self.assertRaises(ApprovalRecordError):
                verifier.verify_record_bytes(
                    raw,
                    envelope_commit=other,
                    manifest_sha256=manifest_sha256,
                )

    def test_tampered_approval_fails_closed(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._repo(root)
            envelope = self._commit(repo, "manifest.json", "{}")
            manifest_sha256 = self._manifest_blob_sha256(root, envelope)
            verifier = ApprovalRecordVerifier(root / "repo")
            tree = subprocess.run(
                ["git", "-C", str(root / "repo"), "rev-parse",
                 envelope + "^{tree}"],
                capture_output=True,
                text=True,
            ).stdout.strip()
            document = verifier.build_record(
                envelope_commit=envelope,
                manifest_sha256=manifest_sha256,
                candidate_tree=tree,
                actor="user",
            )
            document["approval"] = "DENY"
            raw = verifier.serialize(document)
            with self.assertRaises(ApprovalRecordError):
                verifier.verify_record_bytes(
                    raw,
                    envelope_commit=envelope,
                    manifest_sha256=manifest_sha256,
                )

    def test_unknown_candidate_tree_fails_closed(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._repo(root)
            envelope = self._commit(repo, "manifest.json", "{}")
            manifest_sha256 = self._manifest_blob_sha256(root, envelope)
            verifier = ApprovalRecordVerifier(root / "repo")
            document = verifier.build_record(
                envelope_commit=envelope,
                manifest_sha256=manifest_sha256,
                candidate_tree="f" * 40,  # valid shape, not committed
                actor="user",
            )
            raw = verifier.serialize(document)
            with self.assertRaises(ApprovalRecordError):
                verifier.verify_record_bytes(
                    raw,
                    envelope_commit=envelope,
                    manifest_sha256=manifest_sha256,
                )


class ApprovalRecordCoordinatorTests(unittest.TestCase):
    """Coordinator consumption: has/not/tamper branches + evidence stamp."""

    def _run(
        self,
        root: Path,
        task_id: str,
        approval_record: bytes | None,
        envelope: str,
    ):
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        coordinator = ac.ActivationCoordinator(
            root_secret=ROOT_SECRET,
            repository_root=root / "repo",
            test_runner_factory=lambda: [sys.executable, "-c", "pass"],
        )
        return coordinator.run(
            envelope_commit=envelope,
            manifest_ref="manifest.json",
            mode=ac.ActivationMode.V2_NORMAL,
            approval_record=approval_record,
        )

    def _manifest_blob_sha256(self, root: Path, envelope: str) -> str:
        oid = subprocess.run(
            ["git", "-C", str(root / "repo"), "rev-parse",
             f"{envelope}:manifest.json"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        blob = subprocess.run(
            ["git", "-C", str(root / "repo"), "cat-file", "blob", oid],
            capture_output=True,
        ).stdout
        return hashlib.sha256(blob).hexdigest()

    def test_without_approval_record_succeeds(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            stores = _StoresFixture(root)
            try:
                _, _, envelope = _build_envelope(
                    root, task_id="P0-APPROVAL-NONE"
                )
                report = self._run(root, "P0-APPROVAL-NONE", None, envelope)
                self.assertTrue(report.succeeded)
            finally:
                stores.stop()

    def test_matching_record_stamps_evidence(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            stores = _StoresFixture(root)
            try:
                _, _, envelope = _build_envelope(
                    root, task_id="P0-APPROVAL-OK"
                )
                verifier = ApprovalRecordVerifier(root / "repo")
                manifest_sha256 = self._manifest_blob_sha256(root, envelope)
                tree = subprocess.run(
                    ["git", "-C", str(root / "repo"), "rev-parse",
                     envelope + "^{tree}"],
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                raw = verifier.serialize(verifier.build_record(
                    envelope_commit=envelope,
                    manifest_sha256=manifest_sha256,
                    candidate_tree=tree,
                    actor="user",
                ))
                report = self._run(
                    root, "P0-APPROVAL-OK", raw, envelope
                )
                self.assertTrue(report.succeeded)
                evidence = json.loads(
                    (root / "repo" /
                     "research_state/control_plane/p0/attempts/"
                     "coordinator-P0-APPROVAL-OK/evidence/"
                     f"activation-{report.ticket_id[:16]}.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(
                    evidence["approval_record_sha256"],
                    hashlib.sha256(raw).hexdigest(),
                )
            finally:
                stores.stop()

    def test_denied_record_fails_closed_no_ticket(self) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            stores = _StoresFixture(root)
            try:
                _, _, envelope = _build_envelope(
                    root, task_id="P0-APPROVAL-BAD"
                )
                verifier = ApprovalRecordVerifier(root / "repo")
                manifest_sha256 = self._manifest_blob_sha256(root, envelope)
                tree = subprocess.run(
                    ["git", "-C", str(root / "repo"), "rev-parse",
                     envelope + "^{tree}"],
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                document = verifier.build_record(
                    envelope_commit=envelope,
                    manifest_sha256=manifest_sha256,
                    candidate_tree=tree,
                    actor="user",
                )
                document["approval"] = "DENY"
                raw = verifier.serialize(document)
                with self.assertRaises(ac.ActivationEnvelopeError):
                    self._run(root, "P0-APPROVAL-BAD", raw, envelope)
                connection = sqlite3.connect(root / "authority.sqlite3")
                try:
                    rows = connection.execute(
                        "SELECT COUNT(*) FROM task_tickets_v2"
                    ).fetchone()[0]
                finally:
                    connection.close()
                self.assertEqual(rows, 0)
            finally:
                stores.stop()

    def test_wrong_candidate_fails_closed(self) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            stores = _StoresFixture(root)
            try:
                _, _, envelope = _build_envelope(
                    root, task_id="P0-APPROVAL-WRONG"
                )
                # A LATER commit is a DIFFERENT candidate than the envelope.
                (root / "repo" / "later.txt").write_text(
                    "later candidate\n", encoding="utf-8"
                )
                subprocess.run(
                    ["git", "-C", str(root / "repo"), "config",
                     "user.email", "tests@example.invalid"],
                    check=True, capture_output=True,
                )
                subprocess.run(
                    ["git", "-C", str(root / "repo"), "config",
                     "user.name", "approval-record-tests"],
                    check=True, capture_output=True,
                )
                subprocess.run(
                    ["git", "-C", str(root / "repo"), "add", "--",
                     "later.txt"],
                    check=True, capture_output=True,
                )
                subprocess.run(
                    ["git", "-C", str(root / "repo"), "commit", "-q",
                     "-m", "later candidate"],
                    check=True, capture_output=True,
                )
                other_envelope = subprocess.run(
                    ["git", "-C", str(root / "repo"), "rev-parse", "HEAD"],
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                self.assertNotEqual(other_envelope, envelope)
                verifier = ApprovalRecordVerifier(root / "repo")
                manifest_sha256 = self._manifest_blob_sha256(root, envelope)
                tree = subprocess.run(
                    ["git", "-C", str(root / "repo"), "rev-parse",
                     envelope + "^{tree}"],
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                raw = verifier.serialize(verifier.build_record(
                    envelope_commit=other_envelope,  # wrong candidate
                    manifest_sha256=manifest_sha256,
                    candidate_tree=tree,
                    actor="user",
                ))
                with self.assertRaises(ac.ActivationEnvelopeError):
                    self._run(
                        root, "P0-APPROVAL-WRONG", raw, envelope
                    )
            finally:
                stores.stop()


if __name__ == "__main__":
    unittest.main()
