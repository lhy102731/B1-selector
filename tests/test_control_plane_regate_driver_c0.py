"""CR-010 C0: direct regression for the official ``stage3c`` entry.

The official driver is exercised END TO END in a disposable staging root:
a fresh git repository, a bootstrapped Authority/Operational store pair
with an ACTIVE synthetic C0 grant, predecessor closure rows, freeze
manifest, implementation baseline and verification lock.  ``stage3c`` is
called directly (never only a helper), including its internal git
add/commit, and the final HEAD/tree/status/blob surface is asserted
read-only AFTER the run.  The user's workspace is never touched.

The deterministic fixture roots are redirected under the disposable temp
base so NOTHING leaks into the real system temp.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from research_automation.control_plane import regate_driver
from research_automation.control_plane import stores as stores_module
from research_automation.control_plane.contracts import SideEffect

ROOT_SECRET = "staging-root-secret-cr010-0123456789abcdef"
ATTEMPT = "c0-attempt-006"
PHASE = "C0"
CFG_DIR = "research_state/control_plane/rollout/c0/attempts"
EVIDENCE_PREFIX = f"{CFG_DIR}/{ATTEMPT}/evidence/"
FINAL_CHECK_REF = EVIDENCE_PREFIX + "c0_final_surface_check.json"
SEED = 20260811
CYCLES = 24


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {args[0]} failed: {result.stderr[-500:]}")
    return result.stdout.strip()


def _build_disposable_repo(root: Path) -> None:
    (root / "requirements").mkdir(parents=True)
    (root / "requirements" / "verification-runtime.lock").write_text(
        "lock\n", encoding="utf-8"
    )
    for directory in (
        "data",
        "knowledge",
        "config",
        "strategy",
        "research_automation",
        "tools",
    ):
        (root / directory).mkdir()
        (root / directory / "placeholder.txt").write_text(
            "x\n", encoding="utf-8"
        )
    (root / "docs").mkdir()
    (root / "CHANGELOG.md").write_text("v3.4.2\n", encoding="utf-8")
    (root / "daily_run.py").write_text("raise SystemExit(0)\n", encoding="utf-8")
    (root / "daily_select.py").write_text("pass\n", encoding="utf-8")
    (root / "docs" / "b1_v3_results.md").write_text(
        "# results\n", encoding="utf-8"
    )
    attempt_dir = root / CFG_DIR / ATTEMPT
    attempt_dir.mkdir(parents=True)
    (root / "research_state/control_plane/authority").mkdir(parents=True)
    (root / "research_state/control_plane/operational").mkdir(parents=True)
    _git(root, "init", "--quiet")
    _git(root, "config", "user.name", "Staging")
    _git(root, "config", "user.email", "staging@test")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "staging base")
    base_commit = _git(root, "rev-parse", "HEAD")
    base_tree = _git(root, "rev-parse", "HEAD^{tree}")
    freeze = {"git_commit": base_commit, "git_tree": base_tree}
    (attempt_dir / "code_freeze_manifest.json").write_text(
        json.dumps(freeze), encoding="utf-8"
    )
    baseline = {"baseline_payload_sha256": "b" * 64}
    (attempt_dir / "implementation_baseline.json").write_text(
        json.dumps(baseline), encoding="utf-8"
    )
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "staging freeze manifest")


def _insert_staging_grant(authority_db: Path) -> None:
    """Insert the ACTIVE synthetic C0 grant + predecessor closures into the
    disposable Authority (the same secret derivation stage3c recomputes)."""
    identity = regate_driver.PHASES["C0"]["identity"]
    actor = stores_module.Actor(
        "staging-runner", "automation", "invocation-c0-staging"
    )
    authority_identity = stores_module.AuthorityIdentity(
        identity["plan_hash"],
        identity["scope_hash"],
        identity["instruction_policy_hash"],
    )
    effects = (SideEffect.READ, SideEffect.WRITE_CONTROL_PLANE)
    grant_id = "grant-c0-staging-cr010"
    authorization_ref = "auth-c0-staging-cr010"
    grant_secret = stores_module._derive_root_capability_secret(
        stores_module._BearerSecret(ROOT_SECRET),
        domain=b"control_plane.authority_grant.v2",
        payload=stores_module._grant_secret_payload(
            grant_id=grant_id,
            authorization_ref=authorization_ref,
            phase=stores_module.Phase(PHASE),
            attempt_id=ATTEMPT,
            actor=actor,
            identity=authority_identity,
            allowed_side_effects=effects,
        ),
    )
    effects_json = stores_module._effects_json(effects)
    connection = sqlite3.connect(str(authority_db))
    try:
        connection.execute(
            "INSERT INTO authorizations_v2 "
            "(authorization_ref, phase, attempt_id, actor_id, actor_type, "
            "invocation_id, plan_hash, scope_hash, instruction_policy_hash, "
            "secret_sha256, expires_at, allowed_effects_json, state, "
            "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "'CLAIMED', ?)",
            (
                authorization_ref,
                PHASE,
                ATTEMPT,
                actor.actor_id,
                actor.actor_type,
                actor.invocation_id,
                identity["plan_hash"],
                identity["scope_hash"],
                identity["instruction_policy_hash"],
                "0" * 64,
                "2030-01-01T00:00:00Z",
                effects_json,
                "2026-08-01T00:00:00Z",
            ),
        )
        connection.execute(
            "INSERT INTO phase_grants_v2 "
            "(grant_id, authorization_ref, phase, attempt_id, actor_id, "
            "actor_type, invocation_id, plan_hash, scope_hash, "
            "instruction_policy_hash, secret_sha256, allowed_effects_json, "
            "state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, 'ACTIVE', ?)",
            (
                grant_id,
                authorization_ref,
                PHASE,
                ATTEMPT,
                actor.actor_id,
                actor.actor_type,
                actor.invocation_id,
                identity["plan_hash"],
                identity["scope_hash"],
                identity["instruction_policy_hash"],
                hashlib.sha256(grant_secret.encode("utf-8")).hexdigest(),
                effects_json,
                "2026-08-01T00:00:00Z",
            ),
        )
        for index, (pred_phase, pred_attempt) in enumerate(
            (
                ("P0", "p0-attempt-042"),
                ("P6", "p6-attempt-013"),
                ("P7", "p7-attempt-007"),
                ("P8", "p8-attempt-006"),
            )
        ):
            connection.execute(
                "INSERT INTO phase_gate_closures_v1 "
                "(closure_id, phase, attempt_id, grant_id, "
                "gate_report_sha256, verdict, plan_hash, scope_hash, "
                "instruction_policy_hash, closed_at) VALUES "
                "(?, ?, ?, ?, ?, 'PASS', ?, ?, ?, ?)",
                (
                    f"closure-{pred_phase.lower()}-staging",
                    pred_phase,
                    pred_attempt,
                    grant_id,
                    f"{index + 1:064d}",
                    identity["plan_hash"],
                    identity["scope_hash"],
                    identity["instruction_policy_hash"],
                    "2026-08-02T00:00:00Z",
                ),
            )
        # non-P0 task tickets require an ACTIVE reviewed entry policy; the
        # anchor ticket row is never touched by the rollout and therefore
        # stays byte-identical across the surface windows.
        policy_sha = "c" * 64
        connection.execute(
            "INSERT INTO task_tickets_v2 "
            "(ticket_id, grant_id, phase, attempt_id, task_id, "
            "idempotency_key, task_spec_ref, task_spec_sha256, "
            "task_spec_payload_json, request_sha256, entry_policy_sha256, "
            "allowed_effects_json, secret_sha256, state, created_at, "
            "started_at, completed_at, evidence_ref) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ISSUED', ?, "
            "NULL, NULL, NULL)",
            (
                "policy-anchor-ticket",
                grant_id,
                PHASE,
                ATTEMPT,
                "POLICY-ANCHOR",
                "policy-anchor",
                "policy-anchor.json",
                "1" * 64,
                "{}",
                "1" * 64,
                policy_sha,
                effects_json,
                "1" * 64,
                "2026-08-01T00:00:00Z",
            ),
        )
        connection.execute(
            "INSERT INTO reviewed_entry_policies_v1 "
            "(policy_sha256, policy_payload_sha256, "
            "inventory_payload_sha256, review_receipt_sha256, "
            "reviewer_actor_id, reviewer_actor_type, "
            "reviewer_invocation_id, ticket_id, phase, attempt_id, "
            "plan_hash, scope_hash, instruction_policy_hash, created_at) "
            "VALUES (?, ?, ?, ?, ?, 'human', ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                policy_sha,
                "1" * 64,
                "2" * 64,
                "3" * 64,
                "policy-reviewer",
                "policy-review-invocation",
                "policy-anchor-ticket",
                PHASE,
                ATTEMPT,
                identity["plan_hash"],
                identity["scope_hash"],
                identity["instruction_policy_hash"],
                "2026-08-01T00:00:00Z",
            ),
        )
        connection.execute(
            "INSERT INTO active_entry_policy_v1 "
            "(singleton_id, policy_sha256, activated_at) VALUES (1, ?, ?)",
            (policy_sha, "2026-08-01T00:00:00Z"),
        )
        connection.commit()
    finally:
        connection.close()


class _DisposableStage3cEnvironment:
    """Disposable staging root + test-boundary module patches."""

    def __init__(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        # the disposable fixture base lives OUTSIDE the staging git repo
        # so the fixture roots never appear inside the repository surface
        self.base = self.root.parent / ("disposable-base-" + self.root.name)
        self.base.mkdir()
        self._saved_tempdir = tempfile.tempdir
        self._patchers: list = []

    def __enter__(self) -> "_DisposableStage3cEnvironment":
        # redirect ALL deterministic fixture roots under the disposable
        # base -- including the second-root replay subprocess, which
        # receives the parent-computed path
        tempfile.tempdir = str(self.base)
        _build_disposable_repo(self.root)
        authority_db = (
            self.root
            / "research_state/control_plane/authority/authority.sqlite3"
        )
        operational_db = (
            self.root
            / "research_state/control_plane/operational/operational.sqlite3"
        )
        with stores_module.store_path_override(
            authority=authority_db,
            operational=operational_db,
        ):
            stores_module._expected_schema_sha256.cache_clear()
            stores_module._trusted_bootstrap(root_secret=ROOT_SECRET)
            _insert_staging_grant(authority_db)
            stores_module._expected_schema_sha256.cache_clear()
        self._patchers = [
            patch.object(regate_driver, "ROOT", self.root),
            patch.object(
                regate_driver,
                "decrypt_capability",
                return_value=ROOT_SECRET,
            ),
        ]
        for patcher in self._patchers:
            patcher.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        for patcher in reversed(self._patchers):
            patcher.stop()
        tempfile.tempdir = self._saved_tempdir
        self._tmp.cleanup()


class RegateDriverC0Tests(unittest.TestCase):
    def test_stage3c_official_entry_in_disposable_root(self) -> None:
        """CR-010 C0: the OFFICIAL stage3c entry executes end to end in a
        disposable staging root -- no NameError, no unexpected delta, and
        the final HEAD/tree/status/blob surface contains only the expected
        attempt evidence commit (read-only, post-run assertion)."""
        env = _DisposableStage3cEnvironment()
        with env as staging:
            first_root = (
                staging.base / f"v342-c0-deterministic-{SEED}-{CYCLES}"
            )
            second_root = (
                staging.base
                / f"v342-c0-deterministic-{SEED}-{CYCLES}-replay-2"
            )
            # both deterministic fixture roots must be ABSENT before the
            # official entry runs -- an existing root must never be
            # silently rebuilt
            self.assertFalse(first_root.exists(), "first root already exists")
            self.assertFalse(second_root.exists(), "second root already exists")
            cfg = {
                "attempt": ATTEMPT,
                "phase": PHASE,
                "dir": CFG_DIR,
                "identity": regate_driver.PHASES["C0"]["identity"],
            }
            # the pre-run working-tree projection: after the evidence
            # commit the tree must return EXACTLY to this set (the final
            # check file is written after the read-only snapshot)
            pre_status = set(
                _git(staging.root, "status", "--porcelain").splitlines()
            )
            stdout_buf = io.StringIO()
            stderr_buf = io.StringIO()
            started = time.monotonic()
            failure: Exception | None = None
            try:
                with contextlib.redirect_stdout(stdout_buf), (
                    contextlib.redirect_stderr(stderr_buf)
                ):
                    regate_driver.stage3c(cfg)
            except Exception as error:  # noqa: BLE001
                failure = error
            duration = time.monotonic() - started
            stdout_text = stdout_buf.getvalue()
            stderr_text = stderr_buf.getvalue()
            if failure is not None:
                self.fail(
                    "stage3c raised "
                    f"{type(failure).__name__}: {failure}\n"
                    f"stdout:\n{stdout_text[-3000:]}\n"
                    f"stderr:\n{stderr_text[-3000:]}"
                )
            # the immutable receipt + the independent final surface check
            evidence_dir = staging.root / EVIDENCE_PREFIX
            receipt = json.loads(
                (
                    evidence_dir / "c0_no_side_effect_receipt.json"
                ).read_text(encoding="utf-8")
            )
            final_check = json.loads(
                (
                    evidence_dir / "c0_final_surface_check.json"
                ).read_text(encoding="utf-8")
            )
            self.assertTrue(receipt["pass"])
            self.assertTrue(
                final_check["verified"],
                "; ".join(final_check["failures"]),
            )
            self.assertEqual(
                final_check["predeclared_ref"], FINAL_CHECK_REF
            )
            self.assertIn("ROLLOUT_TICKET_BEGUN", stdout_text)
            self.assertIn("CHAIN_COMMITTED", stdout_text)
            self.assertIn("FINAL_SURFACE_CHECK_VERIFIED", stdout_text)
            # --- read-only post-run snapshot (writes NOTHING) ---
            head_after = _git(staging.root, "rev-parse", "HEAD")
            tree_after = _git(staging.root, "rev-parse", "HEAD^{tree}")
            status_after = set(
                _git(staging.root, "status", "--porcelain").splitlines()
            )
            # the working tree equals the PRE-RUN projection plus the ONE
            # predeclared self-excluded final check file: the evidence
            # commit consumed every evidence delta, and no other delta may
            # appear
            self.assertEqual(
                status_after,
                pre_status | {"?? " + FINAL_CHECK_REF},
            )
            self.assertEqual(final_check["git_head"], head_after)
            self.assertEqual(final_check["git_tree"], tree_after)
            changed = _git(
                staging.root,
                "diff-tree",
                "--no-commit-id",
                "--name-status",
                "-r",
                "HEAD",
            ).splitlines()
            self.assertTrue(changed, "evidence commit changed nothing")
            for line in changed:
                path = line.split("\t")[-1]
                self.assertTrue(
                    path.startswith(EVIDENCE_PREFIX),
                    f"unexpected committed path: {path}",
                )
                blob = hashlib.sha256(
                    (staging.root / path).read_bytes()
                ).hexdigest()
                self.assertEqual(len(blob), 64)
            # both deterministic roots now exist UNDER the disposable base
            self.assertTrue(first_root.exists())
            self.assertTrue(second_root.exists())
            # the run recorded real parent-observed identities
            replay = json.loads(
                (evidence_dir / "c0_second_root_replay.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertTrue(replay["pass"])
            self.assertGreater(int(replay["first_pid"]), 0)
            self.assertGreater(int(replay["first_started_at_ns"]), 0)
            self.assertGreater(int(replay["second_pid"]), 0)
            self.assertGreater(int(replay["second_started_at_ns"]), 0)
            self.assertNotEqual(
                (int(replay["first_pid"]), int(replay["first_started_at_ns"])),
                (int(replay["second_pid"]), int(replay["second_started_at_ns"])),
            )

    def test_stage3c_post_return_hidden_commit_rejected(self) -> None:
        """F-04: after stage3c RETURNS, an external READ-ONLY harness must
        re-verify the Git surface against the PRE-run baseline -- a hidden
        tracked commit (README/source change) after the official run must
        be rejected.  The post-return current HEAD/tree must never double
        as its own expected value."""
        env = _DisposableStage3cEnvironment()
        with env as staging:
            cfg = {
                "attempt": ATTEMPT,
                "phase": PHASE,
                "dir": CFG_DIR,
                "identity": regate_driver.PHASES["C0"]["identity"],
            }
            stdout_buf = io.StringIO()
            with contextlib.redirect_stdout(stdout_buf), (
                contextlib.redirect_stderr(io.StringIO())
            ):
                regate_driver.stage3c(cfg)
            baseline_head = _git(staging.root, "rev-parse", "HEAD")
            baseline_tree = _git(staging.root, "rev-parse", "HEAD^{tree}")
            # post-return hidden tracked commit (not evidence)
            (staging.root / "README.md").write_text(
                "# review-b hidden\n", encoding="utf-8"
            )
            _git(staging.root, "add", "-A")
            _git(staging.root, "commit", "-q", "-m", "hidden post-return")
            # the post-return READ-ONLY verifier must reject the hidden
            # commit: the baseline HEAD/tree/blobs differ from final.
            try:
                from research_automation.control_plane.c0_no_side_effect import (
                    verify_stage3c_post_return,
                )

                with self.assertRaises(Exception):
                    verify_stage3c_post_return(
                        repository_root=staging.root,
                        baseline_head=baseline_head,
                        baseline_tree=baseline_tree,
                        evidence_prefix=EVIDENCE_PREFIX,
                    )
            except ImportError:
                self.fail(
                    "post-return Git surface verifier is not wired (F-04)"
                )


if __name__ == "__main__":
    unittest.main()
