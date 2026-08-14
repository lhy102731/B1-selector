"""CR010-R07 tests: full-surface no-side-effect evidence for the C0 run."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from research_automation.control_plane.c0_no_side_effect import (
    NoSideEffectError,
    build_no_side_effect_receipt,
    snapshot_surface,
    verify_surface_unchanged,
)


class C0NoSideEffectSurfaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        # minimal repository surface
        (self.root / "data").mkdir()
        (self.root / "knowledge").mkdir()
        (self.root / "config").mkdir()
        (self.root / "strategy").mkdir()
        (self.root / "research_automation").mkdir()
        (self.root / "tools").mkdir()
        (self.root / "data" / "bars.csv").write_text("a,1\n", encoding="utf-8")
        (self.root / "config" / "params.yaml").write_text("j: 29\n", encoding="utf-8")
        (self.root / "CHANGELOG.md").write_text("v3.4.2\n", encoding="utf-8")
        (self.root / "daily_run.py").write_text("raise SystemExit(0)\n", encoding="utf-8")
        (self.root / "daily_select.py").write_text("pass\n", encoding="utf-8")
        (self.root / "docs").mkdir()
        (self.root / "docs" / "b1_v3_results.md").write_text("# results\n", encoding="utf-8")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_unchanged_surface_passes(self) -> None:
        before = snapshot_surface(self.root)
        after = snapshot_surface(self.root)
        verify_surface_unchanged(before, after)
        receipt = build_no_side_effect_receipt(self.root, before, after)
        self.assertTrue(receipt["pass"])

    def test_modified_store_file_fails_closed(self) -> None:
        store = self.root / "research_state/control_plane/authority/authority.sqlite3"
        store.parent.mkdir(parents=True, exist_ok=True)
        before = snapshot_surface(self.root)
        store.write_bytes(b"tampered")
        after = snapshot_surface(self.root)
        with self.assertRaises(NoSideEffectError):
            verify_surface_unchanged(before, after)

    def test_modified_data_tree_fails_closed(self) -> None:
        before = snapshot_surface(self.root)
        (self.root / "data" / "bars.csv").write_text(
            "a,2\n", encoding="utf-8"
        )
        after = snapshot_surface(self.root)
        with self.assertRaises(NoSideEffectError):
            verify_surface_unchanged(before, after)

    def test_modified_protected_file_fails_closed(self) -> None:
        before = snapshot_surface(self.root)
        (self.root / "CHANGELOG.md").write_text(
            "tampered\n", encoding="utf-8"
        )
        after = snapshot_surface(self.root)
        with self.assertRaises(NoSideEffectError):
            verify_surface_unchanged(before, after)

    def test_unexpected_git_delta_fails_closed(self) -> None:
        import subprocess

        subprocess.run(["git", "init", "--quiet"], cwd=self.root, check=True,
                       capture_output=True)
        subprocess.run(["git", "config", "user.name", "T"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "t@i"], cwd=self.root,
                       check=True, capture_output=True)
        (self.root / "data" / "bars.csv").write_text("a,1\n", encoding="utf-8")
        subprocess.run(["git", "add", "data/bars.csv"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "commit", "--quiet", "-m", "base"],
                       cwd=self.root, check=True, capture_output=True)
        before = snapshot_surface(self.root)
        (self.root / "unexpected.txt").write_text("x", encoding="utf-8")
        after = snapshot_surface(self.root)
        with self.assertRaises(NoSideEffectError):
            verify_surface_unchanged(before, after)

    def test_allowed_evidence_git_delta_passes(self) -> None:
        import subprocess

        subprocess.run(["git", "init", "--quiet"], cwd=self.root, check=True,
                       capture_output=True)
        subprocess.run(["git", "config", "user.name", "T"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "t@i"], cwd=self.root,
                       check=True, capture_output=True)
        (self.root / "data" / "bars.csv").write_text("a,1\n", encoding="utf-8")
        subprocess.run(["git", "add", "data/bars.csv"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "commit", "--quiet", "-m", "base"],
                       cwd=self.root, check=True, capture_output=True)
        before = snapshot_surface(self.root)
        evidence = self.root / "research_state/control_plane/rollout/c0/evidence/x.json"
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text("{}", encoding="utf-8")
        after = snapshot_surface(self.root)
        verify_surface_unchanged(
            before,
            after,
            # git collapses a new untracked tree to its top dir
            allowed_git_deltas=("?? research_state/",),
        )


if __name__ == "__main__":
    unittest.main()
