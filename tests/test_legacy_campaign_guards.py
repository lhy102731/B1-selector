from __future__ import annotations

import argparse
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from research_automation.evolution_loop import EvolutionLoop
from research_automation.control_plane.sink_guard import ExecutionAuthorizationError
from research_automation.control_plane.provenance import stamp_legacy_result
from research_automation.experiment import Experiment, Proposal
from research_automation.registry_updater import RegistryUpdater
from research_automation.snapshot_updater import SnapshotUpdater
from research_automation.handoff_updater import HandoffUpdater
from research_automation.run_brick_sqnav_backlog import run_backlog
from research.brick_ag2_kbase_sqnav_autorun import main as autorun_main


class AuthorityWriterGuardTests(unittest.TestCase):
    def test_legacy_provenance_stamp_cannot_be_self_promoted(self) -> None:
        stamped = stamp_legacy_result(
            {
                "status": "COMPLETE",
                "controller_created": True,
                "trust_state": "controlled_research",
                "promotion_eligible": True,
            }
        )
        self.assertEqual(
            {
                "status": "COMPLETE",
                "controller_created": False,
                "trust_state": "legacy_unaudited",
                "promotion_eligible": False,
            },
            stamped,
        )

    def test_evolution_loop_fails_before_output_creation_without_lease(self) -> None:
        experiment = Experiment(
            experiment_id="unauthorized-evolution",
            strategy="brick",
            proposal=Proposal(hypothesis="must not evolve"),
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evolution-output"
            with patch(
                "research_automation.evolution_loop.output_root",
                return_value=output,
            ):
                loop = EvolutionLoop(project_root=directory)
                with self.assertRaises(ExecutionAuthorizationError):
                    loop.run_generation(experiment, max_generations=1)
            self.assertFalse(output.exists())

    def test_backlog_fails_before_output_creation_without_lease(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "backlog"
            args = argparse.Namespace(
                output_root=str(output),
                include_volume_authenticity=False,
                stop_on_failure=False,
            )
            with self.assertRaises(ExecutionAuthorizationError):
                run_backlog(args)
            self.assertFalse(output.exists())

    def test_autorun_fails_before_output_creation_without_lease(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "autorun"
            argv = [
                "brick_ag2_kbase_sqnav_autorun.py",
                "--output-dir",
                str(output),
                "--context-file",
                str(Path(directory) / "context.md"),
                "--deadline",
                "2026-07-27T00:00:00+08:00",
            ]
            with patch.object(sys, "argv", argv):
                self.assertEqual(3, autorun_main())
            self.assertFalse(output.exists())

    def test_registry_merge_without_lease_fails_before_file_change(self) -> None:
        router = MagicMock()
        router.strategy_id = "brick"
        router.registry_entries = []
        updater = RegistryUpdater(router=router)
        entry = {
            "id": "brick-exp-unauthorized",
            "title": "must not merge",
            "status": "FAILED",
        }

        with tempfile.TemporaryDirectory() as directory:
            registry = Path(directory) / "registry_brick_v2.yaml"
            original = "registry:\n  experiments: []\n"
            registry.write_text(original, encoding="utf-8")
            with self.assertRaises(ExecutionAuthorizationError):
                updater.merge_entry(entry, registry_path=registry)

            self.assertEqual(original, registry.read_text(encoding="utf-8"))

    def test_registry_delta_without_lease_fails_before_directory_or_state_change(self) -> None:
        router = MagicMock()
        router.strategy_id = "brick"
        router.registry_entries = []
        updater = RegistryUpdater(router=router)
        experiment = Experiment(
            experiment_id="unauthorized-registry",
            strategy="brick",
            proposal=Proposal(hypothesis="must not write"),
        )

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "staging"
            target = output / "registry_entry.yaml"
            with patch(
                "research_automation.safety.assert_safe_path",
                return_value=target,
            ):
                with self.assertRaises(ExecutionAuthorizationError):
                    updater.write_delta(experiment, output)

            self.assertFalse(output.exists())
            self.assertIsNone(experiment.registry_update)

    def test_snapshot_and_handoff_deltas_without_lease_fail_before_directory(self) -> None:
        experiment = Experiment(
            experiment_id="unauthorized-deltas",
            strategy="brick",
            proposal=Proposal(hypothesis="must not write"),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for updater, filename in (
                (SnapshotUpdater(), "snapshot_delta.yaml"),
                (HandoffUpdater(), "handoff_delta.yaml"),
            ):
                output = root / filename.removesuffix(".yaml")
                target = output / filename
                with patch(
                    "research_automation.safety.assert_safe_path",
                    return_value=target,
                ):
                    with self.assertRaises(ExecutionAuthorizationError):
                        updater.write_delta(experiment, output)
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
