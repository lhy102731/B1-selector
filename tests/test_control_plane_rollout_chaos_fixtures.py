"""Tests for production-owned offline chaos fixtures (C0R2 T1)."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from research_automation.control_plane.rollout_chaos_fixtures import (
    CampaignOfflineProvider,
    FakeProcessIdentityProvider,
    OfflineChaosIdentity,
    SequentialMonotonicClock,
    deterministic_member,
    deterministic_scope,
)
from research_automation.control_plane.campaign_offline_provider import (
    OfflineProviderTimeout,
)


class ProductionImportTests(unittest.TestCase):
    def test_rollout_chaos_import_has_no_tests_dependency(self) -> None:
        # Importing rollout_chaos must never pull tests.* into the
        # production dependency graph (Step 20.4).
        probe = (
            "import sys; import research_automation.control_plane.rollout_chaos "
            "as m; "
            "bad=[x for x in sys.modules if x.startswith('tests')]; "
            "print('TESTS_IMPORTS=' + ','.join(sorted(bad)))"
        )
        result = subprocess.run(
            [sys.executable, "-c", probe],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=Path(__file__).resolve().parents[1],
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("TESTS_IMPORTS=", result.stdout)
        self.assertNotIn("tests.", result.stdout.split("TESTS_IMPORTS=")[1])


class FixtureBoundaryTests(unittest.TestCase):
    def test_offline_provider_rejects_real_configuration(self) -> None:
        # The offline provider constructor must not accept URLs or clients.
        with self.assertRaises(TypeError):
            CampaignOfflineProvider({"v": 1}, url="https://example.com")  # type: ignore[call-arg]

    def test_provider_identity_is_fake(self) -> None:
        provider = CampaignOfflineProvider({"verdict": "PASS"})
        self.assertEqual(provider.provider_name, "fake-provider")
        self.assertEqual(provider.profile, "offline-local")

    def test_clock_is_deterministic(self) -> None:
        a = SequentialMonotonicClock(20260811)
        b = SequentialMonotonicClock(20260811)
        self.assertEqual([a() for _ in range(5)], [b() for _ in range(5)])
        c = SequentialMonotonicClock(1)
        self.assertNotEqual(
            [a() for _ in range(5)],
            [c() for _ in range(5)],
        )

    def test_process_identity_is_deterministic(self) -> None:
        ident = OfflineChaosIdentity(seed=20260811, pid=1234, process_started_at_ns=999)
        provider = FakeProcessIdentityProvider(ident)
        current = provider.current()
        self.assertEqual(current.pid, 1234)
        self.assertEqual(current.process_started_at_ns, 999)
        self.assertEqual(provider.probe("offline-host", 1234), 999)
        self.assertIsNone(provider.probe("offline-host", 9999))

    def test_member_is_canonical(self) -> None:
        member = deterministic_member(prompt_sha256="a" * 64)
        self.assertEqual(member.profile, "offline-local")
        self.assertEqual(member.model, "deterministic-reviewer")
        self.assertEqual(member.role, "factor_engineer")

    def test_scope_is_canonical(self) -> None:
        scope = deterministic_scope()
        self.assertIn("generation_families", scope)
        self.assertEqual(scope["generation_families"], ["c0-generation-1"])

    def test_provider_counter_persists_across_instances(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            counter = str(Path(tmp) / "counter.txt")
            first = CampaignOfflineProvider({"v": 1}, counter_path=counter)
            first.invoke("a")
            second = CampaignOfflineProvider({"v": 1}, counter_path=counter)
            self.assertEqual(second.call_count, 1)

    def test_provider_timeout_schedule_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = CampaignOfflineProvider(
                {"v": 1},
                schedule={1: "timeout"},
                counter_path=str(Path(tmp) / "c.txt"),
            )
            with self.assertRaises(OfflineProviderTimeout):
                provider.invoke("x")


if __name__ == "__main__":
    unittest.main()
