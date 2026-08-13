"""C0 rollout chaos simulation tests (RED first)."""

from __future__ import annotations

import unittest

from research_automation.control_plane import rollout_chaos
from research_automation.control_plane.artifact_semantics import parse_strict_json


class C0RolloutChaosTests(unittest.TestCase):
    """Offline fake-provider/fake-clock/fake-PID chaos simulation gate tests."""

    def test_main_campaign_completes_at_least_20_cycles_with_all_invariants(
        self,
    ) -> None:
        payload = rollout_chaos.run_c0_simulation(seed=20260811, cycles=24).to_payload()

        self.assertEqual(payload["cycles_completed"], 24)
        self.assertGreaterEqual(payload["cycles_completed"], 20)
        self.assertTrue(payload["offline_only"])
        for invariant in payload["invariants"]:
            self.assertTrue(
                invariant["passed"],
                f"{invariant['name']}: {invariant['detail']}",
            )
        self.assertTrue(payload["pass"])
        self.assertEqual(
            payload["campaign_status"],
            "COMPLETED",
        )

    def test_chaos_categories_all_covered(self) -> None:
        payload = rollout_chaos.run_c0_simulation(seed=20260811, cycles=24).to_payload()
        covered: set[str] = set()
        for entry in payload["scenario_log"]:
            for marker in rollout_chaos.CHAOS_CATEGORIES:
                if marker in entry:
                    covered.add(marker)
        for negative in payload["negative_scenarios"]:
            self.assertTrue(negative["passed"], str(negative))
            covered.add(negative["category"])

        self.assertEqual(
            covered,
            set(rollout_chaos.CHAOS_CATEGORIES),
        )

    def test_deterministic_replay_same_seed(self) -> None:
        # CR-010 F-03: no process-level cache on the official path; every
        # call re-executes the full deterministic simulation, so the replay
        # assertion is a REAL second run, not a cache hit.
        first = rollout_chaos.run_c0_simulation(seed=20260811, cycles=24).to_payload()
        second = rollout_chaos.run_c0_simulation(seed=20260811, cycles=24).to_payload()

        self.assertEqual(first["scenario_log"], second["scenario_log"])
        self.assertEqual(first["invariants"], second["invariants"])
        self.assertEqual(
            first["final_state_digest"],
            second["final_state_digest"],
        )
        self.assertEqual(
            first["negative_scenarios"],
            second["negative_scenarios"],
        )

    def test_different_seeds_produce_different_scenarios(self) -> None:
        first = rollout_chaos.run_c0_simulation(seed=1, cycles=20).to_payload()
        second = rollout_chaos.run_c0_simulation(seed=2, cycles=20).to_payload()

        self.assertNotEqual(first["scenario_log"], second["scenario_log"])

    def test_report_round_trips_through_strict_canonical_json(self) -> None:
        outcome = rollout_chaos.run_c0_simulation(seed=7, cycles=20)
        raw = rollout_chaos.serialize_report(outcome).encode("utf-8")
        parsed = parse_strict_json(raw, artifact_name="c0_report.json")

        self.assertEqual(
            parsed["schema_version"],
            "C0_CHAOS_SIMULATION_REPORT_V1",
        )
        self.assertEqual(parsed, outcome.to_payload())

    def test_min_cycles_are_enforced(self) -> None:
        with self.assertRaises(ValueError):
            rollout_chaos.run_c0_simulation(seed=1, cycles=19)

    def test_negative_scenarios_are_fail_closed(self) -> None:
        payload = rollout_chaos.run_c0_simulation(seed=42, cycles=20).to_payload()
        for negative in payload["negative_scenarios"]:
            self.assertTrue(negative["passed"], str(negative))
            self.assertEqual(negative["expected_outcome"], "FAIL_CLOSED")


if __name__ == "__main__":
    unittest.main()
