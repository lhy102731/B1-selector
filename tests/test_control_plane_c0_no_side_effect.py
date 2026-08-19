"""CR010-R07 tests: full-surface no-side-effect evidence for the C0 run."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from research_automation.control_plane.c0_no_side_effect import (
    NoSideEffectError,
    SurfaceSnapshot,
    build_no_side_effect_receipt,
    snapshot_surface,
    verify_authority_row_deltas,
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

    def test_hidden_tracked_commit_fails_closed(self) -> None:
        """CR-010 F-05: a hidden tracked commit -- modify a tracked file,
        commit it, then restore the file content so ``git status`` is
        byte-identical -- moves HEAD while the status stays clean; the
        surface check MUST fail closed (HEAD/tree are compared, never
        status alone)."""
        import subprocess

        subprocess.run(["git", "init", "--quiet"], cwd=self.root, check=True,
                       capture_output=True)
        subprocess.run(["git", "config", "user.name", "T"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "t@i"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "add", "-A"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "commit", "--quiet", "-m", "base"],
                       cwd=self.root, check=True, capture_output=True)
        before = snapshot_surface(self.root)
        # hidden tracked mutation: change + commit + restore content AND
        # index, so the final status is byte-identical to the baseline
        (self.root / "data" / "bars.csv").write_text("a,2\n", encoding="utf-8")
        subprocess.run(["git", "add", "data/bars.csv"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "commit", "--quiet", "-m", "hidden"],
                       cwd=self.root, check=True, capture_output=True)
        (self.root / "data" / "bars.csv").write_text("a,1\n", encoding="utf-8")
        subprocess.run(["git", "add", "data/bars.csv"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "commit", "--quiet", "-m", "restore"],
                       cwd=self.root, check=True, capture_output=True)
        after = snapshot_surface(self.root)
        self.assertEqual(set(before.git_status), set(after.git_status))
        self.assertNotEqual(before.git_head, after.git_head)
        with self.assertRaisesRegex(NoSideEffectError, "HEAD"):
            verify_surface_unchanged(before, after, repository_root=self.root)

    def test_evidence_commit_confined_to_allowed_paths(self) -> None:
        """CR-010 F-05: when a commit moves HEAD, the diff between the
        before/after OIDs must touch ONLY the allowed evidence paths --
        a commit that smuggles a non-evidence path fails closed even when
        the caller declares the post-commit OIDs."""
        import subprocess

        subprocess.run(["git", "init", "--quiet"], cwd=self.root, check=True,
                       capture_output=True)
        subprocess.run(["git", "config", "user.name", "T"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "t@i"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "add", "-A"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "commit", "--quiet", "-m", "base"],
                       cwd=self.root, check=True, capture_output=True)
        before = snapshot_surface(self.root)
        evidence = (
            self.root
            / "research_state/control_plane/rollout/c0/evidence/ok.json"
        )
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text("{}", encoding="utf-8")
        # the commit also smuggles a non-evidence file
        (self.root / "data" / "smuggled.csv").write_text("x", encoding="utf-8")
        subprocess.run(["git", "add", "-A"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "commit", "--quiet", "-m", "evidence"],
                       cwd=self.root, check=True, capture_output=True)
        after = snapshot_surface(self.root)
        self.assertNotEqual(before.git_head, after.git_head)
        with self.assertRaisesRegex(NoSideEffectError, "commit touched"):
            verify_surface_unchanged(
                before,
                after,
                allowed_git_deltas=(
                    "research_state/control_plane/rollout/c0/evidence/ok.json",
                ),
                allowed_head_after=after.git_head,
                allowed_tree_after=after.git_tree,
                repository_root=self.root,
            )

    def test_evidence_commit_allowed_passes(self) -> None:
        """CR-010 F-05: a commit confined to the allowed evidence path
        with the declared post-commit OIDs PASSES the surface check."""
        import subprocess

        subprocess.run(["git", "init", "--quiet"], cwd=self.root, check=True,
                       capture_output=True)
        subprocess.run(["git", "config", "user.name", "T"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "t@i"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "add", "-A"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "commit", "--quiet", "-m", "base"],
                       cwd=self.root, check=True, capture_output=True)
        before = snapshot_surface(self.root)
        evidence = (
            self.root
            / "research_state/control_plane/rollout/c0/evidence/ok.json"
        )
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text("{}", encoding="utf-8")
        subprocess.run(["git", "add", "-A"], cwd=self.root,
                       check=True, capture_output=True)
        subprocess.run(["git", "commit", "--quiet", "-m", "evidence"],
                       cwd=self.root, check=True, capture_output=True)
        after = snapshot_surface(self.root)
        verify_surface_unchanged(
            before,
            after,
            allowed_git_deltas=(
                "research_state/control_plane/rollout/c0/evidence/ok.json",
            ),
            allowed_head_after=after.git_head,
            allowed_tree_after=after.git_tree,
            repository_root=self.root,
        )

    def test_provider_registry_change_fails_closed(self) -> None:
        """CR-010 F-05: a changed provider-registry fingerprint between
        the baseline and the after snapshot fails closed."""
        before = snapshot_surface(
            self.root,
            provider_registry={"c0-provider": "1" * 64},
        )
        after = snapshot_surface(
            self.root,
            provider_registry={"c0-provider": "2" * 64},
        )
        with self.assertRaisesRegex(NoSideEffectError, "provider registry"):
            verify_surface_unchanged(before, after)

    def test_provider_call_counter_change_fails_closed(self) -> None:
        """CR-010 F-05: a provider call-counter file whose CONTENT changed
        between the baseline and the after snapshot fails closed --
        including the SECOND-root counters (both roots' counters are part
        of the surface); a predeclared counter going ABSENT -> present is
        the run's own evidence and stays allowed."""
        counter = (
            self.root
            / ".c0-provider-counter-c0-cycle-001.txt"
        )
        counter.parent.mkdir(parents=True, exist_ok=True)
        counter.write_text("call\n", encoding="utf-8")
        before = snapshot_surface(
            self.root,
            provider_call_counters=(str(counter),),
        )
        counter.write_text("call\ncall\n", encoding="utf-8")
        after = snapshot_surface(
            self.root,
            provider_call_counters=(str(counter),),
        )
        with self.assertRaisesRegex(NoSideEffectError, "provider counter"):
            verify_surface_unchanged(before, after)
        # the run's own counter creation (ABSENT -> present) is allowed
        fresh = self.root / ".c0-provider-counter-fresh.txt"
        created_before = snapshot_surface(
            self.root,
            provider_call_counters=(str(fresh),),
        )
        fresh.write_text("call\n", encoding="utf-8")
        created_after = snapshot_surface(
            self.root,
            provider_call_counters=(str(fresh),),
        )
        verify_surface_unchanged(created_before, created_after)

    def test_environment_delta_fails_closed(self) -> None:
        """CR-010 F-12: a run that changed a guarded environment variable
        (and did not restore it) must fail the no-side-effect check."""
        before = snapshot_surface(
            self.root,
            environment={"OPENAI_API_KEY": "sk-before"},
        )
        after = snapshot_surface(
            self.root,
            environment={"OPENAI_API_KEY": "sk-after"},
        )
        with self.assertRaisesRegex(NoSideEffectError, "environment"):
            verify_surface_unchanged(before, after)

    def test_fixture_store_creation_allowed_only_for_extra_paths(self) -> None:
        """CR-010 F-12: the deterministic fixture root stores (the worker
        store paths) may go ABSENT -> sha256 ONLY via the explicit
        store_creation_allowed list; any other store delta still fails."""
        import tempfile
        from pathlib import Path as _Path

        with tempfile.TemporaryDirectory() as tmp:
            fixture = _Path(tmp) / "fixture-root"
            authority = fixture / "authority.sqlite3"
            operational = fixture / "operational.sqlite3"
            extra = (str(authority), str(operational))
            before = snapshot_surface(self.root, extra_store_files=extra)
            fixture.mkdir()
            authority.write_bytes(b"fixture-authority")
            operational.write_bytes(b"fixture-operational")
            after = snapshot_surface(self.root, extra_store_files=extra)
            # without the explicit allowance the creation fails closed
            with self.assertRaises(NoSideEffectError):
                verify_surface_unchanged(before, after)
            # with the explicit allowance it passes (the worker stores are
            # covered by the snapshot contract)
            verify_surface_unchanged(
                before,
                after,
                store_creation_allowed=extra,
            )
            # tampering an allowed-created fixture store still fails
            authority.write_bytes(b"tampered")
            after_tampered = snapshot_surface(self.root, extra_store_files=extra)
            with self.assertRaises(NoSideEffectError):
                verify_surface_unchanged(
                    after,
                    after_tampered,
                    store_creation_allowed=extra,
                )

    def test_snapshot_carries_provider_registry_and_counter_fields(self) -> None:
        """CR-010 C0: SurfaceSnapshot carries the provider-registry
        fingerprint, provider call-counter hashes and git HEAD/tree."""
        import tempfile
        from pathlib import Path as _Path

        with tempfile.TemporaryDirectory() as tmp:
            counter = _Path(tmp) / "counter.txt"
            counter.write_text("0", encoding="utf-8")
            snapshot = snapshot_surface(
                self.root,
                provider_registry={"c0-provider": "a" * 64},
                provider_call_counters=(str(counter),),
            )
            self.assertEqual(
                snapshot.provider_registry, {"c0-provider": "a" * 64}
            )
            self.assertIn(str(counter), snapshot.provider_call_counters)
            self.assertTrue(snapshot.provider_call_counters[str(counter)])
            payload = snapshot.to_payload()
            self.assertIn("provider_registry", payload)
            self.assertIn("provider_call_counters", payload)
            self.assertIn("git_head", payload)
            self.assertIn("git_tree", payload)

    def test_authority_row_deltas_allow_only_ticket_bound_rows(self) -> None:
        """CR-010 C0: the Authority row verification fails closed when any
        non-ticket row changes and passes when only the enumerated
        ticket-bound rows appear."""
        import sqlite3
        import tempfile
        from pathlib import Path as _Path

        with tempfile.TemporaryDirectory() as tmp:
            base = _Path(tmp)
            before_db = base / "before.sqlite3"
            after_db = base / "after.sqlite3"
            for db in (before_db, after_db):
                connection = sqlite3.connect(str(db))
                try:
                    connection.execute(
                        "CREATE TABLE task_tickets_v2 ("
                        "ticket_id TEXT PRIMARY KEY, state TEXT NOT NULL)"
                    )
                    connection.execute(
                        "CREATE TABLE authority_outbox ("
                        "event_id TEXT PRIMARY KEY, aggregate_id TEXT NOT NULL)"
                    )
                    connection.execute(
                        "CREATE TABLE unrelated_rows ("
                        "id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                    )
                    connection.execute(
                        "INSERT INTO unrelated_rows (id, value) VALUES (1, 'x')"
                    )
                    connection.commit()
                finally:
                    connection.close()
            ticket_id = "ticket-1"
            after = sqlite3.connect(str(after_db))
            try:
                after.execute(
                    "INSERT INTO task_tickets_v2 (ticket_id, state) "
                    "VALUES (?, 'IN_PROGRESS')",
                    (ticket_id,),
                )
                after.execute(
                    "INSERT INTO authority_outbox (event_id, aggregate_id) "
                    "VALUES ('event-1', ?)",
                    (ticket_id,),
                )
                after.commit()
            finally:
                after.close()
            # ticket-bound rows only -> passes
            verify_authority_row_deltas(before_db, after_db, ticket_id=ticket_id)
            # an unbound row change -> fails closed
            bad = sqlite3.connect(str(after_db))
            try:
                bad.execute(
                    "INSERT INTO unrelated_rows (id, value) VALUES (2, 'y')"
                )
                bad.commit()
            finally:
                bad.close()
            with self.assertRaisesRegex(RuntimeError, "unrelated_rows"):
                verify_authority_row_deltas(
                    before_db, after_db, ticket_id=ticket_id
                )

    def test_authority_row_deltas_require_both_files(self) -> None:
        import tempfile
        from pathlib import Path as _Path

        with tempfile.TemporaryDirectory() as tmp:
            base = _Path(tmp)
            with self.assertRaisesRegex(RuntimeError, "both DB files"):
                verify_authority_row_deltas(
                    base / "missing-before.sqlite3",
                    base / "missing-after.sqlite3",
                    ticket_id="ticket-1",
                )


if __name__ == "__main__":
    unittest.main()
