from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from research_automation.control_plane.sink_guard import ExecutionAuthorizationError
from research_automation.experiment import Experiment, Proposal
from research_automation.registry_updater import RegistryUpdater
from research_automation.snapshot_updater import SnapshotUpdater
from research_automation.handoff_updater import HandoffUpdater


class AuthorityWriterGuardTests(unittest.TestCase):
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
