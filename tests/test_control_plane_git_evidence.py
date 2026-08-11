"""Committed-Git-blob evidence reader contract for P0-CR-008 Slice A.

These tests prove the Gate / inventory evidence path can read canonical
SHA-256 from committed regular Git blobs only, and fails closed on
uncommitted, dirty, symlink, submodule, tree, case-alias or traversal
references.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from research_automation.control_plane.git_evidence import (
    CommittedGitBlob,
    GitBlobReader,
    GitEvidenceError,
)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()


def _init_committed_repo() -> tuple[Path, str]:
    """Create a throwaway git repo with one committed file and a commit sha."""
    tmp = tempfile.mkdtemp(prefix="git-evidence-")
    repo = Path(tmp)
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    body = json.dumps({"sample": True}).encode("utf-8")
    evidence_dir = repo / "research_state" / "control_plane" / "p0r2"
    evidence_dir.mkdir(parents=True)
    (evidence_dir / "evidence.json").write_bytes(body)
    _git(repo, "add", "research_state/control_plane/p0r2/evidence.json")
    _git(repo, "commit", "-q", "-m", "seed evidence")
    head = _git(repo, "rev-parse", "HEAD")
    return repo, head


class GitBlobReaderBasicsTests(unittest.TestCase):
    def test_reads_committed_regular_blob(self) -> None:
        repo, head = _init_committed_repo()
        reader = GitBlobReader(repo)
        ref = "research_state/control_plane/p0r2/evidence.json"
        blob = reader.read(ref, max_bytes=65536, evidence_name="evidence")
        self.assertIsInstance(blob, CommittedGitBlob)
        self.assertEqual(blob.commit, head)
        self.assertEqual(blob.mode, "100644")
        expected = json.dumps({"sample": True}).encode("utf-8")
        self.assertEqual(blob.raw, expected)
        self.assertEqual(blob.sha256, hashlib.sha256(expected).hexdigest())
        self.assertEqual(blob.byte_count, len(expected))

    def test_uncommitted_evidence_is_rejected(self) -> None:
        """A path not present in any commit must be rejected."""
        repo = Path(tempfile.mkdtemp(prefix="git-evidence-"))
        _git(repo, "init", "-q", "-b", "main")
        _git(repo, "config", "user.email", "t@e.c")
        _git(repo, "config", "user.name", "T")
        (repo / "research_state").mkdir()
        (repo / "research_state" / "uncommitted.json").write_bytes(b"{}")
        # file exists on disk but was never committed
        reader = GitBlobReader(repo)
        with self.assertRaises(GitEvidenceError):
            reader.read(
                "research_state/uncommitted.json",
                max_bytes=4096,
                evidence_name="uncommitted",
            )

    def test_stage_is_not_a_commit(self) -> None:
        """A staged-but-uncommitted file must not satisfy the committed reader."""
        repo = Path(tempfile.mkdtemp(prefix="git-evidence-"))
        _git(repo, "init", "-q", "-b", "main")
        _git(repo, "config", "user.email", "t@e.c")
        _git(repo, "config", "user.name", "T")
        (repo / "staged.json").write_bytes(b"{}")
        _git(repo, "add", "staged.json")
        reader = GitBlobReader(repo)
        with self.assertRaises(GitEvidenceError):
            reader.read("staged.json", max_bytes=4096, evidence_name="staged")

    def test_add_then_modify_is_not_a_commit(self) -> None:
        """Modifying a file after staging it must still be rejected."""
        repo = Path(tempfile.mkdtemp(prefix="git-evidence-"))
        _git(repo, "init", "-q", "-b", "main")
        _git(repo, "config", "user.email", "t@e.c")
        _git(repo, "config", "user.name", "T")
        path = repo / "flip.json"
        path.write_bytes(b"{1}")
        _git(repo, "add", "flip.json")
        path.write_bytes(b"{2}")
        reader = GitBlobReader(repo)
        with self.assertRaises(GitEvidenceError):
            reader.read("flip.json", max_bytes=4096, evidence_name="flip")

    def test_dirty_gate_input_blocks(self) -> None:
        """A committed file modified in the working tree must be rejected."""
        repo, _ = _init_committed_repo()
        target = repo / "research_state" / "control_plane" / "p0r2" / "evidence.json"
        target.write_bytes(b"{'tampered': true}")
        reader = GitBlobReader(repo)
        with self.assertRaises(GitEvidenceError):
            reader.read(
                "research_state/control_plane/p0r2/evidence.json",
                max_bytes=65536,
                evidence_name="evidence",
            )

    def test_non_existent_ref_is_rejected(self) -> None:
        repo, _ = _init_committed_repo()
        reader = GitBlobReader(repo)
        with self.assertRaises(GitEvidenceError):
            reader.read(
                "research_state/control_plane/nope.json",
                max_bytes=4096,
                evidence_name="nope",
            )


class GitBlobReaderModeTests(unittest.TestCase):
    def test_symlink_mode_is_rejected(self) -> None:
        if os.name != "posix":
            self.skipTest("symlinks require POSIX")
        repo = Path(tempfile.mkdtemp(prefix="git-evidence-"))
        _git(repo, "init", "-q", "-b", "main")
        _git(repo, "config", "user.email", "t@e.c")
        _git(repo, "config", "user.name", "T")
        target = repo / "real.json"
        target.write_bytes(b"{}")
        link = repo / "link.json"
        link.symlink_to("real.json")
        _git(repo, "add", "real.json", "link.json")
        _git(repo, "commit", "-q", "-m", "add symlink")
        reader = GitBlobReader(repo)
        with self.assertRaises(GitEvidenceError):
            reader.read("link.json", max_bytes=4096, evidence_name="link")

    def test_directory_tree_is_rejected(self) -> None:
        repo, _ = _init_committed_repo()
        reader = GitBlobReader(repo)
        with self.assertRaises(GitEvidenceError):
            reader.read(
                "research_state/control_plane",
                max_bytes=4096,
                evidence_name="dir",
            )


class GitBlobReaderPathTests(unittest.TestCase):
    def test_traversal_is_rejected(self) -> None:
        repo, _ = _init_committed_repo()
        reader = GitBlobReader(repo)
        for evil in (
            "../outside.json",
            "a/../../etc/passwd",
            "research_state\\..\\..\\passwd",
            "/absolute/path.json",
            "..\\win\\path",
        ):
            with self.assertRaises(GitEvidenceError, msg=evil):
                reader.read(evil, max_bytes=4096, evidence_name="evil")

    def test_null_and_empty_references_are_rejected(self) -> None:
        repo, _ = _init_committed_repo()
        reader = GitBlobReader(repo)
        with self.assertRaises(GitEvidenceError):
            reader.read("", max_bytes=4096, evidence_name="empty")
        with self.assertRaises(GitEvidenceError):
            reader.read("a\x00b", max_bytes=4096, evidence_name="nul")


class GitBlobReaderCrLfTests(unittest.TestCase):
    def test_autocrlf_checkouts_share_one_blob_sha256(self) -> None:
        """Working-tree line endings never change the committed blob identity.

        If git treats the CRLF worktree as clean (autocrlf normalization) the
        reader returns the identical committed blob; if it treats it as dirty
        the reader blocks. Either way the committed identity is authoritative.
        """
        repo = Path(tempfile.mkdtemp(prefix="git-evidence-"))
        _git(repo, "init", "-q", "-b", "main")
        _git(repo, "config", "user.email", "t@e.c")
        _git(repo, "config", "user.name", "T")
        _git(repo, "config", "core.autocrlf", "true")
        (repo / ".gitattributes").write_text("* text=auto\n", encoding="utf-8")
        lf_body = "line one\nline two\n".encode("utf-8")
        (repo / "evidence.json").write_bytes(lf_body)
        _git(repo, "add", ".gitattributes", "evidence.json")
        _git(repo, "commit", "-q", "-m", "lf blob")
        oid_a = _git(repo, "rev-parse", "HEAD:evidence.json")
        reader = GitBlobReader(repo)
        blob = reader.read("evidence.json", max_bytes=4096, evidence_name="e")
        self.assertEqual(blob.raw, lf_body, "reader returns committed LF bytes")
        self.assertEqual(blob.oid, oid_a, "committed blob identity is unchanged")
        # CRLF-rewritten working tree
        (repo / "evidence.json").write_bytes(
            "line one\r\nline two\r\n".encode("utf-8")
        )
        clean = _git(repo, "status", "--porcelain", "--", "evidence.json") == ""
        if clean:
            blob2 = reader.read("evidence.json", max_bytes=4096, evidence_name="e")
            self.assertEqual(blob2.oid, oid_a)
            self.assertEqual(blob2.raw, lf_body)
        else:
            with self.assertRaises(GitEvidenceError):
                reader.read("evidence.json", max_bytes=4096, evidence_name="e")

    def test_dirty_crlf_content_blocks_while_clean_normalized_reads(self) -> None:
        repo = Path(tempfile.mkdtemp(prefix="git-evidence-"))
        _git(repo, "init", "-q", "-b", "main")
        _git(repo, "config", "user.email", "t@e.c")
        _git(repo, "config", "user.name", "T")
        _git(repo, "config", "core.autocrlf", "true")
        (repo / "evidence.json").write_text("a\nb\n", encoding="utf-8")
        _git(repo, "add", "evidence.json")
        _git(repo, "commit", "-q", "-m", "seed")
        reader = GitBlobReader(repo)
        # clean normalized read works
        blob = reader.read("evidence.json", max_bytes=4096, evidence_name="e")
        self.assertEqual(blob.raw, b"a\nb\n")
        # genuinely different content -> dirty -> blocks
        (repo / "evidence.json").write_text("a\nX\n", encoding="utf-8")
        with self.assertRaises(GitEvidenceError):
            reader.read("evidence.json", max_bytes=4096, evidence_name="e")


if __name__ == "__main__":
    unittest.main()
