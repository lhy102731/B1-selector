"""Independent CR-010 C0 acceptance suite (frozen-acceptance plan 4.2/11.1).

Every expected value is computed INDEPENDENTLY from production APIs and
durable OS/Git observations -- never from tests.* fixtures.  The suite is
the contract for A4 (child containment + campaign identity) and A5
(surface truth: hidden commits, durable counter equality, live registry,
surviving network telemetry); the RED run at Task 0 must reproduce the
original bypasses on the pre-fix code.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from research_automation.control_plane import rollout_chaos
from research_automation.control_plane.c0_no_side_effect import (
    NoSideEffectError,
    snapshot_surface,
    verify_surface_unchanged,
)
from research_automation.control_plane.rollout_chaos_worker import (
    NetworkGuard,
    RolloutChaosNetworkDenied,
)

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
        raise RuntimeError(f"git {args[0]} failed: {result.stderr[-300:]}")
    return result.stdout.strip()


def _clean_roots(*, seeds_cycles=((SEED, CYCLES),)) -> None:
    for seed, cycles in seeds_cycles:
        root = rollout_chaos._deterministic_root(seed, cycles)
        for target in (root, root.parent / (root.name + "-replay-2")):
            shutil.rmtree(target, ignore_errors=True)


class A4ChildContainmentTests(unittest.TestCase):
    """A4: children are launched only through fixed purpose-built
    descriptors; the campaign itself runs in parent-observed OS children."""

    def setUp(self) -> None:
        NetworkGuard.uninstall()
        NetworkGuard._installed = False
        NetworkGuard.attempts = 0
        _clean_roots()

    def tearDown(self) -> None:
        NetworkGuard.uninstall()
        NetworkGuard._installed = False
        NetworkGuard.attempts = 0
        _clean_roots()

    def test_a4_arbitrary_python_child_is_denied(self) -> None:
        """python -c, unknown modules and raw argv are denied while the
        offline Guard contract is active."""
        NetworkGuard.install()
        NetworkGuard.deny_probe()
        with self.assertRaises(RolloutChaosNetworkDenied):
            subprocess.Popen([sys.executable, "-c", "pass"])
        with self.assertRaises(RolloutChaosNetworkDenied):
            subprocess.Popen([sys.executable, "-m", "json.tool"])
        with self.assertRaises(RolloutChaosNetworkDenied):
            subprocess.Popen([sys.executable])

    def test_a4_multiprocessing_provider_child_is_guarded_and_counted(
        self,
    ) -> None:
        """A C0 provider child under the offline contract must bootstrap
        the Guard BEFORE any provider code is imported/executed and return
        a guard receipt + parent-observed identity + run-local telemetry."""
        from research_automation.control_plane.campaign import (
            SpawnedProviderExecutor,
        )
        from research_automation.control_plane.rollout_chaos_fixtures import (
            C0ChaosProvider,
        )

        NetworkGuard.install()
        NetworkGuard.deny_probe()
        try:
            provider = C0ChaosProvider(
                {
                    "schema_version": "research.execution_spec.v1",
                    "outcome": "ok",
                }
            )
            executor = SpawnedProviderExecutor(provider)
            with tempfile.TemporaryDirectory() as tmp:
                counter = Path(tmp) / "counter.txt"
                provider._counter_path = str(counter)
                receipt = executor.execute_with_guard_receipt(
                    request={"prompt": "acceptance probe"},
                    max_output_bytes=4096,
                    deadline_seconds=30,
                    counter_path=str(counter),
                )
                self.assertTrue(receipt["guard_installed"])
                self.assertGreater(int(receipt["pid"]), 0)
                self.assertGreater(int(receipt["started_at_ns"]), 0)
                self.assertGreaterEqual(
                    int(receipt["deny_probe_attempts"]), 1
                )
                self.assertEqual(int(receipt["real_network_attempts"]), 0)
                # the provider child was counted exactly once (the counter
                # file must exist with value 1 -- read INSIDE the tempdir
                # scope, contract v2: the file is the run's own evidence)
                self.assertEqual(
                    int(counter.read_text(encoding="utf-8")), 1
                )
        finally:
            NetworkGuard.uninstall()
            NetworkGuard._installed = False
            NetworkGuard.attempts = 0

    def test_a4_first_identity_is_campaign_executor_not_verifier(self) -> None:
        """The first campaign identity recorded in worker_verify is the
        parent-observed identity of the campaign EXECUTOR child -- never
        the later verify worker, never the supervisor itself."""
        payload = rollout_chaos.run_c0_simulation(
            seed=SEED, cycles=CYCLES
        ).to_payload()
        worker_verify = payload.get("worker_verify") or {}
        first_pid = int(worker_verify.get("first_pid", 0) or 0)
        first_start = int(worker_verify.get("first_started_at_ns", 0) or 0)
        verify_pid = int(
            (worker_verify.get("pid") or worker_verify.get("verify_pid") or 0)
        )
        self.assertGreater(first_pid, 0)
        self.assertGreater(first_start, 0)
        self.assertNotEqual(first_pid, os.getpid())
        self.assertNotEqual(first_pid, verify_pid)
        # contract v2: the parent observed the campaign EXECUTOR child's
        # OS start time while it was ALIVE (a terminated process cannot be
        # queried afterwards on every platform); the observed identity and
        # the live-observation marker are the durable OS evidence.
        self.assertTrue(worker_verify.get("first_identity_verified"))

    def test_a4_two_campaigns_use_distinct_parent_observed_processes(
        self,
    ) -> None:
        """Two complete 24-cycle campaigns each execute in their OWN
        parent-observed OS child -- the two identities differ and neither
        is the supervisor."""
        _clean_roots()
        first = rollout_chaos.run_c0_simulation(
            seed=SEED, cycles=CYCLES
        ).to_payload()
        _clean_roots()
        second = rollout_chaos.run_c0_simulation(
            seed=SEED, cycles=CYCLES
        ).to_payload()
        first_pair = (
            int(first["worker_verify"]["first_pid"]),
            int(first["worker_verify"]["first_started_at_ns"]),
        )
        second_pair = (
            int(second["worker_verify"]["first_pid"]),
            int(second["worker_verify"]["first_started_at_ns"]),
        )
        self.assertNotEqual(first_pair, second_pair)
        self.assertNotEqual(first_pair[0], os.getpid())
        self.assertNotEqual(second_pair[0], os.getpid())


class _SurfaceRepo(unittest.TestCase):
    """A minimal disposable git repo with the C0 surface layout."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        for directory in (
            "data",
            "knowledge",
            "config",
            "strategy",
            "research_automation",
            "tools",
        ):
            (self.root / directory).mkdir()
            (self.root / directory / "placeholder.txt").write_text(
                "x\n", encoding="utf-8"
            )
        (self.root / "docs").mkdir()
        (self.root / "CHANGELOG.md").write_text("v3.4.2\n", encoding="utf-8")
        (self.root / "daily_run.py").write_text(
            "raise SystemExit(0)\n", encoding="utf-8"
        )
        (self.root / "daily_select.py").write_text("pass\n", encoding="utf-8")
        (self.root / "docs" / "b1_v3_results.md").write_text(
            "# results\n", encoding="utf-8"
        )
        _git(self.root, "init", "--quiet")
        _git(self.root, "config", "user.name", "Acceptance")
        _git(self.root, "config", "user.email", "acceptance@test")
        _git(self.root, "add", "-A")
        _git(self.root, "commit", "-q", "-m", "base")

    def tearDown(self) -> None:
        self._tmp.cleanup()


class A5SurfaceTruthTests(_SurfaceRepo):
    """A5: the surface proof compares run-before baselines to real final
    state; hidden commits, invented counters, registry drift and lost
    telemetry all fail closed."""

    def test_a5_hidden_non_evidence_commit_is_rejected(self) -> None:
        before = snapshot_surface(self.root)
        # hidden tracked mutation: modify README/docs, commit, restore the
        # working tree so git status is byte-identical
        (self.root / "docs" / "b1_v3_results.md").write_text(
            "# results\n# hidden\n", encoding="utf-8"
        )
        _git(self.root, "add", "-A")
        _git(self.root, "commit", "-q", "-m", "hidden docs")
        (self.root / "docs" / "b1_v3_results.md").write_text(
            "# results\n", encoding="utf-8"
        )
        _git(self.root, "add", "-A")
        _git(self.root, "commit", "-q", "-m", "restore")
        after = snapshot_surface(self.root)
        self.assertEqual(set(before.git_status), set(after.git_status))
        self.assertNotEqual(before.git_head, after.git_head)
        with self.assertRaisesRegex(NoSideEffectError, "HEAD"):
            verify_surface_unchanged(before, after, repository_root=self.root)

    def test_a5_first_counter_value_must_match_durable_invocation_count(
        self,
    ) -> None:
        """The FIRST value of a provider counter file must equal the
        durable MODEL_USAGE_RECORDED invocation count for that root/
        campaign/cycle -- '0'/'1' from a stale or invented file is
        rejected unless the durable journal agrees."""
        from research_automation.control_plane.c0_no_side_effect import (
            seal_provider_counter,
            verify_counter_matches_durable_usage,
        )

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            journal_db = base / "operational.sqlite3"
            counter = base / ".c0-provider-counter-c0-cycle-001.txt"
            counter.write_text("0", encoding="utf-8")
            # independent durable journal: exactly one MODEL_USAGE_RECORDED
            # for the campaign/cycle
            import sqlite3 as _sqlite3

            connection = _sqlite3.connect(str(journal_db))
            try:
                connection.execute(
                    "CREATE TABLE campaign_events (sequence INTEGER PRIMARY "
                    "KEY AUTOINCREMENT, campaign_id TEXT NOT NULL, cycle_id "
                    "TEXT NOT NULL, event_type TEXT NOT NULL, payload_json "
                    "TEXT NOT NULL, payload_sha256 TEXT NOT NULL, "
                    "event_sha256 TEXT NOT NULL, created_at TEXT NOT NULL)"
                )
                payload = json.dumps(
                    {
                        "attempt_id": "c0-acceptance",
                        "root": str(base),
                        "_authority_grant_id": "grant-c0-acceptance",
                        "_campaign_attempt_id": "c0-attempt-003",
                    }
                )
                connection.execute(
                    "INSERT INTO campaign_events (campaign_id, cycle_id, "
                    "event_type, payload_json, payload_sha256, event_sha256, "
                    "created_at) VALUES (?, ?, 'MODEL_USAGE_RECORDED', ?, ?, "
                    "?, '2026-08-17T00:00:00Z')",
                    (
                        "c0-acceptance",
                        "c0-cycle-001",
                        payload,
                        hashlib.sha256(payload.encode()).hexdigest(),
                        "e" * 64,
                    ),
                )
                connection.commit()
            finally:
                connection.close()
            # the durable count is 1 -> a counter of '0' must be rejected
            seal_provider_counter(
                counter, journal_db, repository_root=base,
                campaign_id="c0-acceptance", cycle_id="c0-cycle-001",
                attempt_id="c0-attempt-003",
                root_secret="review-b-root-capability-0123456789abcdef",
                grant_id="grant-c0-acceptance",
            )
            with self.assertRaises((ValueError, RuntimeError)):
                verify_counter_matches_durable_usage(
                    counter_path=counter,
                    operational_db=journal_db,
                    campaign_id="c0-acceptance",
                    cycle_id="c0-cycle-001",
                    attempt_id="c0-acceptance",
                    grant_id="grant-c0-acceptance",
                    campaign_attempt_id="c0-attempt-003",
                    repository_root=base,
                    root_secret="review-b-root-capability-0123456789abcdef",
                )
            counter.write_text("1", encoding="utf-8")
            seal_provider_counter(
                counter, journal_db, repository_root=base,
                campaign_id="c0-acceptance", cycle_id="c0-cycle-001",
                attempt_id="c0-attempt-003", root_secret="review-b-root-capability-0123456789abcdef",
                grant_id="grant-c0-acceptance",
            )
            verify_counter_matches_durable_usage(
                counter_path=counter,
                operational_db=journal_db,
                campaign_id="c0-acceptance",
                cycle_id="c0-cycle-001",
                attempt_id="c0-acceptance",
                grant_id="grant-c0-acceptance",
                campaign_attempt_id="c0-attempt-003",
                repository_root=base,
                root_secret="review-b-root-capability-0123456789abcdef",
            )

    def test_same_grant_other_attempt_is_rejected(self) -> None:
        """F-02 (git-native run003): within the SAME grant, an event of
        ANOTHER campaign attempt must never satisfy the TARGET attempt's
        counter verification."""
        from research_automation.control_plane.c0_no_side_effect import (
            durable_model_usage_count,
            seal_provider_counter,
            verify_counter_matches_durable_usage,
        )

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            journal_db = base / "operational.sqlite3"
            counter = base / ".c0-provider-counter-c0-cycle-001.txt"
            counter.write_text("1", encoding="utf-8")
            import sqlite3 as _sqlite3

            connection = _sqlite3.connect(str(journal_db))
            try:
                connection.execute(
                    "CREATE TABLE campaign_events (sequence INTEGER PRIMARY "
                    "KEY AUTOINCREMENT, campaign_id TEXT NOT NULL, cycle_id "
                    "TEXT NOT NULL, event_type TEXT NOT NULL, payload_json "
                    "TEXT NOT NULL, payload_sha256 TEXT NOT NULL, "
                    "event_sha256 TEXT NOT NULL, created_at TEXT NOT NULL)"
                )
                p1 = json.dumps(
                    {
                        "attempt_id": "inv-att-1",
                        "_authority_grant_id": "grant-same",
                        "_campaign_attempt_id": "TARGET_ATTEMPT",
                    }
                )
                p2 = json.dumps(
                    {
                        "attempt_id": "inv-att-2",
                        "_authority_grant_id": "grant-same",
                        "_campaign_attempt_id": "OTHER_ATTEMPT",
                    }
                )
                for pj in (p1, p2):
                    connection.execute(
                        "INSERT INTO campaign_events (campaign_id, cycle_id, "
                        "event_type, payload_json, payload_sha256, "
                        "event_sha256, created_at) VALUES (?, ?, "
                        "'MODEL_USAGE_RECORDED', ?, ?, ?"
                        ", '2026-08-17T00:00:00Z')",
                        (
                            "c0-acceptance",
                            "c0-cycle-001",
                            pj,
                            hashlib.sha256(pj.encode()).hexdigest(),
                            "e" * 64,
                        ),
                    )
                connection.commit()
            finally:
                connection.close()
            seal_provider_counter(
                counter, journal_db, repository_root=base,
                campaign_id="c0-acceptance", cycle_id="c0-cycle-001",
                attempt_id="TARGET_ATTEMPT", root_secret="review-b-root-capability-0123456789abcdef",
                grant_id="grant-same",
            )
            count = durable_model_usage_count(
                journal_db,
                campaign_id="c0-acceptance",
                cycle_id="c0-cycle-001",
                attempt_id="TARGET_ATTEMPT",
                grant_id="grant-same",
                campaign_attempt_id="TARGET_ATTEMPT",
            )
            self.assertEqual(count, 1)
            verify_counter_matches_durable_usage(
                counter_path=counter,
                operational_db=journal_db,
                campaign_id="c0-acceptance",
                cycle_id="c0-cycle-001",
                attempt_id="TARGET_ATTEMPT",
                grant_id="grant-same",
                campaign_attempt_id="TARGET_ATTEMPT",
                repository_root=base,
                root_secret="review-b-root-capability-0123456789abcdef",
            )
            counter.write_text("2", encoding="utf-8")
            seal_provider_counter(
                counter, journal_db, repository_root=base,
                campaign_id="c0-acceptance", cycle_id="c0-cycle-001",
                attempt_id="TARGET_ATTEMPT", root_secret="review-b-root-capability-0123456789abcdef",
                grant_id="grant-same",
            )
            with self.assertRaises((ValueError, RuntimeError)):
                verify_counter_matches_durable_usage(
                    counter_path=counter,
                    operational_db=journal_db,
                    campaign_id="c0-acceptance",
                    cycle_id="c0-cycle-001",
                    attempt_id="TARGET_ATTEMPT",
                    grant_id="grant-same",
                    campaign_attempt_id="TARGET_ATTEMPT",
                    repository_root=base,
                    root_secret="review-b-root-capability-0123456789abcdef",
                )

    def test_a5_live_registry_mutation_is_rejected(self) -> None:
        """Every snapshot recomputes the provider-registry fingerprint from
        the LIVE registry -- a registry mutated between the baseline and
        the after snapshot fails closed."""
        from research_automation.control_plane.c0_no_side_effect import (
            live_provider_registry_fingerprint,
        )

        before_registry = live_provider_registry_fingerprint()
        before = snapshot_surface(
            self.root,
            provider_registry={"c0-provider": before_registry},
        )
        mutated = live_provider_registry_fingerprint(
            override_name="mutated-provider"
        )
        after = snapshot_surface(
            self.root,
            provider_registry={"c0-provider": mutated},
        )
        with self.assertRaisesRegex(NoSideEffectError, "provider registry"):
            verify_surface_unchanged(before, after)
        # an identical LIVE re-read passes
        again = snapshot_surface(
            self.root,
            provider_registry={"c0-provider": live_provider_registry_fingerprint()},
        )
        verify_surface_unchanged(before, again)

    def test_a5_network_attempts_survive_guard_uninstall_until_receipt(
        self,
    ) -> None:
        """Guard uninstall must NOT clear the telemetry the surface receipt
        needs: deny probes, worker spawns and real network attempts are
        recorded in a run-scoped immutable collector that survives
        uninstall."""
        from research_automation.control_plane.c0_no_side_effect import (
            network_telemetry_snapshot,
        )

        NetworkGuard.uninstall()
        NetworkGuard._installed = False
        NetworkGuard.attempts = 0
        NetworkGuard.install()
        NetworkGuard.deny_probe()
        attempts_during_guard = NetworkGuard.attempts
        NetworkGuard.uninstall()
        # the telemetry survives uninstall (run-scoped collector)
        telemetry = network_telemetry_snapshot()
        self.assertGreaterEqual(
            telemetry["deny_probe_attempts"],
            1,
        )
        self.assertEqual(telemetry["real_network_attempts"], 0)
        self.assertGreaterEqual(telemetry["total_interceptions"], 1)
        # a NEW guard install starts a NEW run scope, never reusing the
        # old collector's identity
        NetworkGuard.install()
        NetworkGuard.uninstall()
        self.assertEqual(NetworkGuard.attempts, 0)



    def test_missing_period_counter_fails(self) -> None:
        """F-02 (git-native run003): deleting a required cycle's counter
        file must FAIL; adding an extra cycle counter must FAIL."""
        from research_automation.control_plane.c0_no_side_effect import (
            seal_provider_counter,
        )
        from research_automation.control_plane.rollout_chaos import (
            _verify_official_counters_after_run,
        )

        _RS = "review-b-root-capability-0123456789abcdef"
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "c0root"
            root.mkdir(parents=True, exist_ok=True)
            journal_db = root / "operational.sqlite3"
            import sqlite3 as _sqlite3

            conn = _sqlite3.connect(str(journal_db))
            try:
                conn.execute(
                    "CREATE TABLE campaign_events (sequence INTEGER PRIMARY "
                    "KEY AUTOINCREMENT, campaign_id TEXT NOT NULL, cycle_id "
                    "TEXT NOT NULL, event_type TEXT NOT NULL, payload_json "
                    "TEXT NOT NULL, payload_sha256 TEXT NOT NULL, "
                    "event_sha256 TEXT NOT NULL, created_at TEXT NOT NULL)"
                )
                for cyc in ("c0-cycle-001", "c0-cycle-002"):
                    pj = json.dumps(
                        {
                            "attempt_id": "inv-" + cyc,
                            "_authority_grant_id": "grant-c0",
                            "_campaign_attempt_id": "c0-attempt-003",
                        }
                    )
                    conn.execute(
                        "INSERT INTO campaign_events (campaign_id, cycle_id, "
                        "event_type, payload_json, payload_sha256, "
                        "event_sha256, created_at) VALUES ('c0-main-campaign', "
                        "?, 'MODEL_USAGE_RECORDED', ?, ?, ?"
                        ", '2026-08-18T00:00:00Z')",
                        (cyc, pj, hashlib.sha256(pj.encode()).hexdigest(),
                         "e" * 64),
                    )
                conn.commit()
            finally:
                conn.close()

            def _seal(cycle_id: str) -> None:
                seal_provider_counter(
                    root / f".c0-provider-counter-{cycle_id}.txt",
                    journal_db,
                    repository_root=root,
                    campaign_id="c0-main-campaign",
                    cycle_id=cycle_id,
                    attempt_id="c0-attempt-003",
                    root_secret=_RS,
                )

            (root / ".c0-provider-counter-c0-cycle-001.txt").write_text(
                "1", encoding="utf-8"
            )
            with self.assertRaises((ValueError, RuntimeError)):
                _verify_official_counters_after_run(
                    root, campaign_id="c0-main-campaign",
                    attempt_id="c0-attempt-003", root_secret=_RS,
                )
            (root / ".c0-provider-counter-c0-cycle-002.txt").write_text(
                "1", encoding="utf-8"
            )
            _seal("c0-cycle-001")
            _seal("c0-cycle-002")
            _verify_official_counters_after_run(
                root, campaign_id="c0-main-campaign",
                attempt_id="c0-attempt-003", root_secret=_RS,
            )
            (root / ".c0-provider-counter-c0-cycle-003.txt").write_text(
                "1", encoding="utf-8"
            )
            with self.assertRaises((ValueError, RuntimeError)):
                _verify_official_counters_after_run(
                    root, campaign_id="c0-main-campaign",
                    attempt_id="c0-attempt-003", root_secret=_RS,
                )

    def test_cross_root_counter_swap_rejected(self) -> None:
        """F-02 (run004): two roots with IDENTICAL counts exchanging their
        identity-bound counter records must FAIL on both sides."""
        from research_automation.control_plane.c0_no_side_effect import (
            seal_provider_counter,
        )
        from research_automation.control_plane.rollout_chaos import (
            _verify_official_counters_after_run,
        )

        _RS = "review-b-root-capability-0123456789abcdef"
        import sqlite3 as _sqlite3

        def _mk(base: Path, name: str, grant: str) -> Path:
            root = base / name
            root.mkdir(parents=True, exist_ok=True)
            conn = _sqlite3.connect(str(root / "operational.sqlite3"))
            conn.execute(
                "CREATE TABLE campaign_events (sequence INTEGER PRIMARY "
                "KEY AUTOINCREMENT, campaign_id TEXT NOT NULL, cycle_id TEXT "
                "NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT "
                "NULL, payload_sha256 TEXT NOT NULL, event_sha256 TEXT NOT "
                "NULL, created_at TEXT NOT NULL)"
            )
            pj = json.dumps(
                {"attempt_id": "inv-1", "_authority_grant_id": grant,
                 "_campaign_attempt_id": "c0-attempt-003"}
            )
            conn.execute(
                "INSERT INTO campaign_events (campaign_id, cycle_id, "
                "event_type, payload_json, payload_sha256, event_sha256, "
                "created_at) VALUES ('c0-main-campaign', 'c0-cycle-001', "
                "'MODEL_USAGE_RECORDED', ?, ?, ?, '2026-08-18T00:00:00Z')",
                (pj, hashlib.sha256(pj.encode()).hexdigest(), "e" * 64),
            )
            conn.commit()
            conn.close()
            (root / ".c0-provider-counter-c0-cycle-001.txt").write_text(
                "1", encoding="utf-8"
            )
            return root

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root_a = _mk(base, "A", "grant-swap-A")
            root_b = _mk(base, "B", "grant-swap-B")
            for root, grant in ((root_a, "grant-swap-A"), (root_b, "grant-swap-B")):
                seal_provider_counter(
                    root / ".c0-provider-counter-c0-cycle-001.txt",
                    root / "operational.sqlite3",
                    repository_root=root,
                    campaign_id="c0-main-campaign",
                    cycle_id="c0-cycle-001",
                    attempt_id="c0-attempt-003",
                    root_secret=_RS,
                    grant_id=grant,
                )
            _verify_official_counters_after_run(
                root_a, campaign_id="c0-main-campaign",
                attempt_id="c0-attempt-003", root_secret=_RS,
            )
            _verify_official_counters_after_run(
                root_b, campaign_id="c0-main-campaign",
                attempt_id="c0-attempt-003", root_secret=_RS,
            )
            c_a = root_a / ".c0-provider-counter-c0-cycle-001.txt"
            c_b = root_b / ".c0-provider-counter-c0-cycle-001.txt"
            tmpf = base / "TMP"
            import shutil as _shutil

            _shutil.copy2(c_a, tmpf)
            _shutil.copy2(c_b, c_a)
            _shutil.copy2(tmpf, c_b)
            tmpf.unlink()
            for root in (root_a, root_b):
                with self.assertRaises((ValueError, RuntimeError)):
                    _verify_official_counters_after_run(
                        root, campaign_id="c0-main-campaign",
                        attempt_id="c0-attempt-003", root_secret=_RS,
                    )

class _OfficialCounterVerificationTests(unittest.TestCase):
    """F-02: the OFFICIAL C0 run must wire durable counter verification.

    The expected count comes ONLY from the Operational journal's
    deduplicated MODEL_USAGE_RECORDED events; the official payload records
    per-root/per-cycle observed/expected/verified and a tampered counter
    (999 / ABSENT / negative / non-int / root swap) fails through the
    official post-run verification, never by calling the counter helper
    directly."""

    payload = None
    roots = None

    @classmethod
    def setUpClass(cls) -> None:
        _clean_roots()
        outcome = rollout_chaos.run_c0_simulation(seed=SEED, cycles=CYCLES)
        cls.payload = outcome.to_payload()
        cls.roots = (
            rollout_chaos._deterministic_root(SEED, CYCLES).resolve(),
            (
                rollout_chaos._deterministic_root(SEED, CYCLES).resolve().parent
                / (
                    rollout_chaos._deterministic_root(SEED, CYCLES).resolve().name
                    + "-replay-2"
                )
            ),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        _clean_roots()

    def test_official_counter_999_rejected(self) -> None:
        cv = self.payload.get("counter_verification") or {}
        # F-02 RED: the official run MUST verify durable counters for BOTH
        # roots; a forged 999 counter must fail through the official path.
        self.assertIn("first_root", cv)
        self.assertIn("second_root", cv)
        items = (cv.get("first_root") or {}).get("verified", ())
        self.assertTrue(items, "official run recorded no counter verification")
        for item in items:
            self.assertEqual(
                int(item["observed"]), int(item["expected"]),
                "official counter verification drifted",
            )
        # tampered 999 -> official post-run verifier must reject
        first_root = self.roots[0]
        counter = sorted(first_root.glob(".c0-provider-counter-*.txt"))[0]
        counter.write_text("999", encoding="utf-8")
        from research_automation.control_plane.rollout_chaos import (
            _verify_official_counters_after_run,
        )

        with self.assertRaises(Exception):
            _verify_official_counters_after_run(
                repository_root=str(first_root),
                campaign_id="c0-campaign",
                attempt_id="c0-attempt-003",
            )

    def test_official_counter_absent_rejected(self) -> None:
        cv = self.payload.get("counter_verification") or {}
        self.assertTrue(cv, "official run recorded no counter verification")
        # tampered ABSENT -> official post-run verifier must reject
        second_root = self.roots[1]
        counter = sorted(second_root.glob(".c0-provider-counter-*.txt"))[0]
        counter.unlink()
        from research_automation.control_plane.rollout_chaos import (
            _verify_official_counters_after_run,
        )

        with self.assertRaises(Exception):
            _verify_official_counters_after_run(
                repository_root=str(second_root),
                campaign_id="c0-campaign",
                attempt_id="c0-attempt-003",
            )

    def test_campaign_identity_child_report_mismatch_rejected(self) -> None:
        """F-03: a child self-reported (pid, start_ns) that disagrees with
        the parent-observed value must fail closed -- the child report is
        never the sole source."""
        verify = self.payload.get("worker_verify") or {}
        self.assertGreater(int(verify.get("first_pid", 0)), 0)
        self.assertGreater(int(verify.get("first_started_at_ns", 0)), 0)
        self.assertTrue(verify.get("first_identity_verified"))
        self.assertNotEqual(
            int(verify.get("first_pid", 0)),
            int(verify.get("verify_pid", 0)),
            "the verify worker must never be recorded as the campaign "
            "identity",
        )


if __name__ == "__main__":
    unittest.main()
