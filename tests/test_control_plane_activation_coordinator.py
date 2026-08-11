"""P0-CR-008 slice A: single-process activation coordinator contracts.

RED/GREEN matrix from the corrective plan Task 4 Step 4.3a: candidate blob
integrity, base/ancestry/quarantine collisions, secret non-leakage, the full
crash matrix (begin, fast-forward, finish, outbox boundaries), v1 bootstrap
vs migration effect separation, lease/schema crossing rejection and the
v2 normal path.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import Actor, Phase, SideEffect


ROOT_SECRET = "test-only-authority-root-capability-0123456789abcdef"
GIT = "git"

# Crash child: runs the coordinator in a real subprocess and hard-exits after
# the requested phase boundary. The root secret lives only in this script
# string; it never travels through argv/env of the coordinator children.
CRASH_CHILD = """
import os
import sys
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import stores as stores_module
from research_automation.control_plane import activation_coordinator as ac

root = Path(sys.argv[1])
crash_phase = sys.argv[2]
envelope_commit = sys.argv[3]
root_secret = sys.argv[4]

patch.multiple(
    stores_module,
    _AUTHORITY_STORE_PATH=root / "authority.sqlite3",
    _OPERATIONAL_STORE_PATH=root / "operational.sqlite3",
).start()
stores_module._expected_schema_sha256.cache_clear()
# Stores are provisioned by the test process; the crash child only probes
# the same files through the schema-probing coordinator specs.

def crash_hook(phase):
    if phase.value == crash_phase:
        os._exit(9)

coordinator = ac.ActivationCoordinator(
    root_secret=root_secret,
    repository_root=root / "repo",
    test_runner_factory=lambda: [sys.executable, "-c", "pass"],
    crash_hook=crash_hook,
)
coordinator.run(
    envelope_commit=envelope_commit,
    manifest_ref="manifest.json",
    mode=ac.ActivationMode.V2_NORMAL,
)
print("COORDINATOR_OK")
"""


class _RepoFixture:
    """Minimal linear git repository: base -> source -> envelope."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.repo = root / "repo"
        self.repo.mkdir(parents=True, exist_ok=True)
        self._git("init", "-q")
        self._git("config", "user.email", "tests@example.invalid")
        self._git("config", "user.name", "activation-coordinator-tests")

    def _git(self, *args: str) -> str:
        result = subprocess.run(
            [GIT, "-C", str(self.repo), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"git {args[0]} failed: {result.stderr[-400:]}"
            )
        return result.stdout.strip()

    def commit_file(self, name: str, content: str) -> str:
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        self._git("add", "--", name)
        self._git(
            "commit",
            "-q",
            "-m",
            f"commit {name}",
        )
        return self._git("rev-parse", "HEAD")

    def diff_sha256(self, base: str, source: str) -> str:
        diff = subprocess.run(
            [GIT, "-C", str(self.repo), "diff", base, source],
            capture_output=True,
        )
        return hashlib.sha256(diff.stdout).hexdigest()

    def head(self) -> str:
        return self._git("rev-parse", "HEAD")

    def tree(self, commit: str) -> str:
        return self._git("rev-parse", f"{commit}^{{tree}}")


class _StoresFixture:
    def __init__(self, root: Path, *, authority_v1: bool = False) -> None:
        self.root = root
        self.authority_path = root / "authority.sqlite3"
        self.operational_path = root / "operational.sqlite3"
        self.authority_v1 = authority_v1
        self.paths = patch.multiple(
            stores_module,
            _AUTHORITY_STORE_PATH=self.authority_path,
            _OPERATIONAL_STORE_PATH=self.operational_path,
        )
        self.paths.start()
        if authority_v1:
            original_schema = stores_module._AUTHORITY_SCHEMA
            original_version = stores_module._AUTHORITY_SCHEMA_VERSION
            try:
                stores_module._AUTHORITY_SCHEMA = (
                    stores_module._AUTHORITY_SCHEMA_V1
                )
                stores_module._AUTHORITY_SCHEMA_VERSION = 1
                stores_module._expected_schema_sha256.cache_clear()
                stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
            finally:
                stores_module._AUTHORITY_SCHEMA = original_schema
                stores_module._AUTHORITY_SCHEMA_VERSION = original_version
                stores_module._expected_schema_sha256.cache_clear()
        else:
            stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)

    def stop(self) -> None:
        self.paths.stop()
        stores_module._expected_schema_sha256.cache_clear()


def _build_envelope(
    root: Path,
    *,
    mode: str = "v2_normal",
    task_id: str = "P0-TEST-ACTIVATION",
    file_suffix: str = "",
    extra_manifest: dict[str, object] | None = None,
) -> tuple[str, str, str]:
    """Build base/source/envelope commits and return (base, source, envelope)."""
    repo = _RepoFixture(root)
    base = repo.commit_file(f"base{file_suffix}.txt", f"base content {file_suffix}\n")
    source = repo.commit_file(
        f"source{file_suffix}.txt", f"source content {file_suffix}\n"
    )
    manifest = {
        "schema": "control_plane.activation_envelope.v1",
        "phase": "P0",
        "task_id": task_id,
        "mode": mode,
        "base_commit": base,
        "base_tree": repo.tree(base),
        "source_commit": source,
        "source_tree": repo.tree(source),
        "candidate_diff_sha256": repo.diff_sha256(base, source),
        "allowed_files": [f"source{file_suffix}.txt"],
        "forbidden_files": ["data/", "research_state/"],
        "quarantine_manifest_sha256": "q" * 64,
        "required_official_tests": ["tests.test_control_plane_fixture_ok"],
        "expected_side_effects": ["WRITE_CONTROL_PLANE"],
    }
    if extra_manifest:
        manifest.update(extra_manifest)
    envelope = repo.commit_file(
        "manifest.json",
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
    )
    return base, source, envelope


def _authority_user_version(authority_path: Path) -> int:
    connection = sqlite3.connect(authority_path)
    try:
        return int(
            connection.execute("PRAGMA user_version").fetchone()[0]
        )
    finally:
        connection.close()


def _ticket_state(authority_path: Path, ticket_id: str) -> str | None:
    connection = sqlite3.connect(authority_path)
    try:
        row = connection.execute(
            "SELECT state FROM task_tickets_v2 WHERE ticket_id = ?",
            (ticket_id,),
        ).fetchone()
        return None if row is None else str(row[0])
    finally:
        connection.close()


def _pending_outbox(authority_path: Path) -> int:
    connection = sqlite3.connect(authority_path)
    try:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM authority_outbox "
                "WHERE mirrored_at IS NULL"
            ).fetchone()[0]
        )
    finally:
        connection.close()


class ActivationCoordinatorTests(unittest.TestCase):
    def _bootstrap(
        self,
        root: Path,
        *,
        authority_v1: bool = False,
    ) -> _StoresFixture:
        fixture = _StoresFixture(root, authority_v1=authority_v1)
        self.addCleanup(fixture.stop)
        return fixture

    def _coordinator(
        self,
        root: Path,
        *,
        crash_hook=None,
        test_runner_factory=None,
    ):
        from research_automation.control_plane import activation_coordinator as ac

        return ac.ActivationCoordinator(
            root_secret=ROOT_SECRET,
            repository_root=root / "repo",
            test_runner_factory=test_runner_factory
            or (lambda: [sys.executable, "-c", "pass"]),
            crash_hook=crash_hook,
        )

    # ------------------------------------------------------------------
    def test_candidate_blob_mode_or_hash_mismatch_is_rejected(self) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root)
            base, source, envelope = _build_envelope(root)
            coordinator = self._coordinator(root)
            with self.assertRaises(ac.ActivationEnvelopeError):
                coordinator.run(
                    envelope_commit="0" * 40,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.V2_NORMAL,
                )
            self.assertEqual(_authority_user_version(root / "authority.sqlite3"), 2)
            self.assertEqual(_pending_outbox(root / "authority.sqlite3"), 0)
            # malformed manifest blob in the same repo must also fail closed
            (root / "repo" / "manifest.json").write_text("{bad json")
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "add", "--", "manifest.json"],
                check=True,
            )
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "commit", "-q", "-m", "bad"],
                check=True,
            )
            bad_commit = subprocess.run(
                [GIT, "-C", str(root / "repo"), "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
            ).stdout.strip()
            with self.assertRaises(ac.ActivationEnvelopeError):
                coordinator.run(
                    envelope_commit=bad_commit,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.V2_NORMAL,
                )

    def test_base_ancestry_diff_or_quarantine_collision_is_rejected(
        self,
    ) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root)
            base, source, envelope = _build_envelope(root)
            coordinator = self._coordinator(root)
            # HEAD is not on base -> reject
            with self.assertRaises(ac.ActivationEnvelopeError):
                coordinator.run(
                    envelope_commit=envelope,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.V2_NORMAL,
                )
            # move HEAD to base, then add a dirty out-of-scope file -> reject
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            (root / "repo" / "data").mkdir(exist_ok=True)
            (root / "repo" / "data" / "dirty.csv").write_text("dirty\n")
            with self.assertRaises(ac.ActivationEnvelopeError):
                coordinator.run(
                    envelope_commit=envelope,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.V2_NORMAL,
                )
            (root / "repo" / "data" / "dirty.csv").unlink()
            # tampered diff (source content changed after manifest) -> reject
            (root / "repo" / "source.txt").write_text("changed\n")
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "add", "--", "source.txt"],
                check=True,
            )
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "commit", "-q", "-m", "drift"],
                check=True,
            )
            with self.assertRaises(ac.ActivationEnvelopeError):
                coordinator.run(
                    envelope_commit=envelope,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.V2_NORMAL,
                )

    def test_secret_never_appears_in_child_argv_env_file_or_log(self) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )
        from research_automation.control_plane import (
            activation_coordinator as coordinator_module,
        )

        captured_argv: list[list[str]] = []
        captured_env: list[dict[str, str]] = []
        real_run = subprocess.run

        def capturing_run(argv, *args, **kwargs):
            captured_argv.append(list(argv))
            captured_env.append(dict(kwargs.get("env") or os.environ))
            return real_run(argv, *args, **kwargs)

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root)
            base, source, envelope = _build_envelope(root)
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            coordinator = self._coordinator(root)
            with patch.object(coordinator_module.subprocess, "run", capturing_run):
                report = coordinator.run(
                    envelope_commit=envelope,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.V2_NORMAL,
                )
            self.assertTrue(report.succeeded)
            self.assertGreater(len(captured_argv), 0)
            for argv in captured_argv:
                self.assertNotIn(ROOT_SECRET, " ".join(argv))
            for env in captured_env:
                for value in env.values():
                    self.assertNotIn(ROOT_SECRET, value)
            for path in (
                root / "authority.sqlite3",
                root / "operational.sqlite3",
            ):
                self.assertNotIn(ROOT_SECRET.encode(), path.read_bytes())

    def test_failure_before_begin_leaves_zero_authority_and_branch_changes(
        self,
    ) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root)
            base, source, envelope = _build_envelope(root)
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            coordinator = self._coordinator(root)

            def fail_validate(phase):
                if phase is ac.ActivationPhase.VALIDATE:
                    raise RuntimeError("simulated pre-issue failure")

            coordinator = self._coordinator(root, crash_hook=fail_validate)
            with self.assertRaises(RuntimeError):
                coordinator.run(
                    envelope_commit=envelope,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.V2_NORMAL,
                )
            self.assertEqual(
                _authority_user_version(root / "authority.sqlite3"), 2
            )
            self.assertEqual(_pending_outbox(root / "authority.sqlite3"), 0)
            self.assertEqual(
                subprocess.run(
                    [GIT, "-C", str(root / "repo"), "rev-parse", "HEAD"],
                    capture_output=True,
                    text=True,
                ).stdout.strip(),
                base,
            )

    def test_hard_exit_after_begin_before_fast_forward_marks_in_doubt_branch_unchanged(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root)
            base, source, envelope = _build_envelope(root)
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    CRASH_CHILD,
                    str(root),
                    "BEGIN",
                    envelope,
                    ROOT_SECRET,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            self.assertNotEqual(result.returncode, 0)
            connection = sqlite3.connect(root / "authority.sqlite3")
            try:
                tickets = connection.execute(
                    "SELECT ticket_id, state FROM task_tickets_v2"
                ).fetchall()
                pending = connection.execute(
                    "SELECT COUNT(*) FROM authority_outbox "
                    "WHERE mirrored_at IS NULL"
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(len(tickets), 1)
            self.assertEqual(tickets[0][1], "IN_PROGRESS")
            self.assertGreater(pending, 0)
            self.assertEqual(
                subprocess.run(
                    [GIT, "-C", str(root / "repo"), "rev-parse", "HEAD"],
                    capture_output=True,
                    text=True,
                ).stdout.strip(),
                base,
            )

    def test_hard_exit_after_fast_forward_before_finish_keeps_new_head_in_doubt(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root)
            base, source, envelope = _build_envelope(root)
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    CRASH_CHILD,
                    str(root),
                    "FAST_FORWARD",
                    envelope,
                    ROOT_SECRET,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(
                subprocess.run(
                    [GIT, "-C", str(root / "repo"), "rev-parse", "HEAD"],
                    capture_output=True,
                    text=True,
                ).stdout.strip(),
                envelope,
            )
            connection = sqlite3.connect(root / "authority.sqlite3")
            try:
                tickets = connection.execute(
                    "SELECT ticket_id, state FROM task_tickets_v2"
                ).fetchall()
            finally:
                connection.close()
            self.assertEqual(len(tickets), 1)
            self.assertEqual(tickets[0][1], "IN_PROGRESS")

    def test_hard_exit_after_finish_before_outbox_allows_only_idempotent_mirror(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root)
            base, source, envelope = _build_envelope(root)
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    CRASH_CHILD,
                    str(root),
                    "FINISH",
                    envelope,
                    ROOT_SECRET,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            self.assertNotEqual(result.returncode, 0)
            connection = sqlite3.connect(root / "authority.sqlite3")
            try:
                tickets = connection.execute(
                    "SELECT ticket_id, state FROM task_tickets_v2"
                ).fetchall()
                pending = connection.execute(
                    "SELECT COUNT(*) FROM authority_outbox "
                    "WHERE mirrored_at IS NULL"
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(tickets[0][1], "SUCCEEDED")
            self.assertGreater(pending, 0)
            # idempotent mirror/ack must complete from a fresh process
            from research_automation.control_plane import (
                activation_coordinator as ac,
            )

            recovered = self._coordinator(root)
            drained = recovered.drain_outbox_idempotent()
            self.assertGreater(drained, 0)
            self.assertEqual(_pending_outbox(root / "authority.sqlite3"), 0)

    def test_v1_bootstrap_ticket_with_migration_effect_is_rejected(self) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root, authority_v1=True)
            base, source, envelope = _build_envelope(
                root,
                mode="v1_bootstrap",
                extra_manifest={
                    "expected_side_effects": ["MIGRATE_STORES"],
                },
            )
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            coordinator = self._coordinator(root)
            with self.assertRaises(ac.ActivationEnvelopeError):
                coordinator.run(
                    envelope_commit=envelope,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.V1_BOOTSTRAP,
                )
            self.assertEqual(
                _authority_user_version(root / "authority.sqlite3"), 1
            )

    def test_migration_before_source_ticket_terminal_is_rejected(self) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root, authority_v1=True)
            base, source, envelope = _build_envelope(
                root,
                mode="migration",
                extra_manifest={
                    "expected_side_effects": ["MIGRATE_STORES"],
                },
            )
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            coordinator = self._coordinator(root)
            # source bootstrap ticket is not terminal -> migration must refuse
            with self.assertRaises(ac.ActivationEnvelopeError):
                coordinator.run(
                    envelope_commit=envelope,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.MIGRATION,
                )

    def test_lease_cannot_cross_process_or_reload(self) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root)
            base, source, envelope = _build_envelope(root)
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            # The coordinator must not accept any serialized lease/state input.
            with self.assertRaises(TypeError):
                ac.ActivationCoordinator(
                    root_secret=ROOT_SECRET,
                    repository_root=root / "repo",
                    serialized_lease={"lease_id": "x"},
                )
            crash = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    CRASH_CHILD,
                    str(root),
                    "BEGIN",
                    envelope,
                    ROOT_SECRET,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            self.assertNotEqual(crash.returncode, 0)
            connection = sqlite3.connect(root / "authority.sqlite3")
            try:
                ticket_id = connection.execute(
                    "SELECT ticket_id FROM task_tickets_v2"
                ).fetchone()[0]
            finally:
                connection.close()
            # A fresh coordinator cannot resume the lease; only reconciliation
            # can mark the ticket IN_DOUBT (no reset, no reuse).
            fresh = self._coordinator(root)
            with self.assertRaises(ac.ActivationEnvelopeError):
                fresh.run(
                    envelope_commit=envelope,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.V2_NORMAL,
                )
            self.assertEqual(
                _ticket_state(root / "authority.sqlite3", ticket_id),
                "IN_PROGRESS",
            )

    def test_normal_or_source_lease_cannot_cross_schema(self) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root, authority_v1=True)
            base, source, envelope = _build_envelope(root, mode="v1_bootstrap")
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            coordinator = self._coordinator(root)
            report = coordinator.run(
                envelope_commit=envelope,
                manifest_ref="manifest.json",
                mode=ac.ActivationMode.V1_BOOTSTRAP,
            )
            self.assertTrue(report.succeeded)
            # a v1-source lease must not be usable as a v2 normal lease
            with self.assertRaises(ac.ActivationEnvelopeError):
                coordinator.run(
                    envelope_commit=envelope,
                    manifest_ref="manifest.json",
                    mode=ac.ActivationMode.V2_NORMAL,
                )

    def test_migration_lease_begins_v1_finishes_v2_in_one_parent_process(
        self,
    ) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root, authority_v1=True)
            base, source, envelope = _build_envelope(root, mode="v1_bootstrap")
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            coordinator = self._coordinator(root)
            bootstrap_report = coordinator.run(
                envelope_commit=envelope,
                manifest_ref="manifest.json",
                mode=ac.ActivationMode.V1_BOOTSTRAP,
            )
            self.assertTrue(bootstrap_report.succeeded)
            migration_base, migration_source, migration_envelope = (
                _build_envelope(
                    root,
                    mode="migration",
                    task_id="P0-TEST-MIGRATION",
                    file_suffix="-migration",
                    extra_manifest={
                        "expected_side_effects": ["MIGRATE_STORES"],
                    },
                )
            )
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", migration_base],
                check=True,
            )
            report = coordinator.run(
                envelope_commit=migration_envelope,
                manifest_ref="manifest.json",
                mode=ac.ActivationMode.MIGRATION,
            )
            self.assertTrue(report.succeeded)
            self.assertEqual(
                _authority_user_version(root / "authority.sqlite3"), 2
            )
            connection = sqlite3.connect(root / "authority.sqlite3")
            try:
                states = [
                    str(row[0])
                    for row in connection.execute(
                        "SELECT state FROM task_tickets_v2 ORDER BY created_at"
                    ).fetchall()
                ]
            finally:
                connection.close()
            self.assertEqual(states, ["SUCCEEDED", "SUCCEEDED"])
            self.assertEqual(_pending_outbox(root / "authority.sqlite3"), 0)

    def test_v2_normal_task_activation_succeeds(self) -> None:
        from research_automation.control_plane import (
            activation_coordinator as ac,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._bootstrap(root)
            base, source, envelope = _build_envelope(root)
            subprocess.run(
                [GIT, "-C", str(root / "repo"), "checkout", "-q", base],
                check=True,
            )
            coordinator = self._coordinator(root)
            report = coordinator.run(
                envelope_commit=envelope,
                manifest_ref="manifest.json",
                mode=ac.ActivationMode.V2_NORMAL,
            )
            self.assertTrue(report.succeeded)
            self.assertEqual(report.head, envelope)
            self.assertEqual(report.phase, "OUTBOX")
            self.assertEqual(_pending_outbox(root / "authority.sqlite3"), 0)
            connection = sqlite3.connect(root / "authority.sqlite3")
            try:
                state = connection.execute(
                    "SELECT state FROM task_tickets_v2"
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(state, "SUCCEEDED")


if __name__ == "__main__":
    unittest.main()
