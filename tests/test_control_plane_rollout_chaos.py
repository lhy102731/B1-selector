"""C0 rollout chaos simulation tests (RED first)."""

from __future__ import annotations

import unittest

from research_automation.control_plane import rollout_chaos
from research_automation.control_plane.artifact_semantics import parse_strict_json


class C0RolloutChaosTests(unittest.TestCase):
    """Offline fake-provider/fake-clock/fake-PID chaos simulation gate tests."""

    _USED_SEEDS_CYCLES = (
        (20260811, 24),
        (20260811, 20),
        (1, 20),
        (2, 20),
        (7, 20),
        (42, 20),
    )

    def setUp(self) -> None:
        # CR-010 F-05: an EXISTING deterministic fixture root is a
        # controlled failure -- the tests remove their OWN disposable
        # roots before each run so every run starts genuinely fresh (the
        # production code never deletes a root automatically).
        import shutil as _shutil

        for seed, cycles in self._USED_SEEDS_CYCLES:
            for suffix in ("", "-replay-2"):
                root = rollout_chaos._deterministic_root(seed, cycles)
                if suffix:
                    root = root.parent / (root.name + suffix)
                _shutil.rmtree(root, ignore_errors=True)

    def test_existing_deterministic_root_fails_closed(self) -> None:
        """CR-010 F-05: a pre-existing deterministic fixture root causes a
        controlled failure -- it is NEVER deleted automatically (a stale
        root could silently be replayed as evidence)."""
        import shutil as _shutil
        from pathlib import Path as _Path

        sentinel = _Path(
            rollout_chaos._deterministic_root(20260811, 24)
        )
        sentinel.mkdir(parents=True, exist_ok=True)
        sentinel_file = sentinel / "SENTINEL.txt"
        sentinel_file.write_text("existing-root", encoding="utf-8")
        try:
            with self.assertRaisesRegex(
                RuntimeError, "already exists"
            ):
                rollout_chaos._run_main_campaign(
                    20260811,
                    24,
                    root_override=sentinel,
                )
            # the existing root was NOT deleted
            self.assertTrue(sentinel_file.exists())
            self.assertEqual(
                sentinel_file.read_text(encoding="utf-8"),
                "existing-root",
            )
        finally:
            _shutil.rmtree(sentinel, ignore_errors=True)

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
        # CR-010 F-05: the second run needs a GENUINELY fresh root -- the
        # test removes its OWN disposable fixture roots (the production
        # code never deletes an existing deterministic root).
        import shutil as _shutil

        for suffix in ("", "-replay-2"):
            root = rollout_chaos._deterministic_root(20260811, 24)
            _shutil.rmtree(
                root if not suffix else root.parent / (root.name + suffix),
                ignore_errors=True,
            )
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




class C0ExactInvariantFailClosedTests(unittest.TestCase):
    """CR010-R06/C0-3: the exact invariant set must FAIL CLOSED."""

    def setUp(self) -> None:
        import shutil as _shutil

        for seed, cycles in C0RolloutChaosTests._USED_SEEDS_CYCLES:
            root = rollout_chaos._deterministic_root(seed, cycles)
            for target in (root, root.parent / (root.name + "-replay-2")):
                _shutil.rmtree(target, ignore_errors=True)

    def test_require_exact_invariant_set_rejects_missing_invariant(self) -> None:
        from research_automation.control_plane.rollout_chaos import (
            EXACT_CHAOS_INVARIANTS,
            require_exact_invariant_set,
        )

        incomplete = {
            name: {"name": name}
            for name in EXACT_CHAOS_INVARIANTS
            if name != "network_denied"
        }
        with self.assertRaises(ValueError):
            require_exact_invariant_set(incomplete)

    def test_require_exact_invariant_set_rejects_extra_invariant(self) -> None:
        from research_automation.control_plane.rollout_chaos import (
            EXACT_CHAOS_INVARIANTS,
            require_exact_invariant_set,
        )

        extra = {
            name: {"name": name}
            for name in EXACT_CHAOS_INVARIANTS
        }
        extra["unexpected_invariant"] = {"name": "unexpected_invariant"}
        with self.assertRaises(ValueError):
            require_exact_invariant_set(extra)

    def test_require_exact_invariant_set_rejects_non_mapping(self) -> None:
        from research_automation.control_plane.rollout_chaos import (
            require_exact_invariant_set,
        )

        with self.assertRaises(ValueError):
            require_exact_invariant_set(["not-a-mapping"])

    def test_official_simulation_produces_exactly_the_mandated_set(self) -> None:
        from research_automation.control_plane import rollout_chaos

        payload = rollout_chaos.run_c0_simulation(
            seed=20260811, cycles=20
        ).to_payload()
        produced = {item["name"] for item in payload["invariants"]}
        self.assertEqual(
            produced, rollout_chaos.EXACT_CHAOS_INVARIANTS
        )
        self.assertTrue(payload["pass"])
        for name in (
            "durable_pause_resume",
            "fresh_process_identity",
            "network_denied",
        ):
            item = next(
                item for item in payload["invariants"] if item["name"] == name
            )
            self.assertTrue(item["passed"], item["detail"])


if __name__ == "__main__":
    unittest.main()
