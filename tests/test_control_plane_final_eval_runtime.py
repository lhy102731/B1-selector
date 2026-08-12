"""Tests for the trusted final evaluation runtime (P8R3 T8)."""

from __future__ import annotations

import unittest

from research_automation.control_plane.final_eval_runtime import (
    FinalEvalRuntime,
    FinalEvalRuntimeInputs,
    FinalEvalRuntimeRejected,
)


class FinalEvalRuntimeTests(unittest.TestCase):
    def _runtime(self, exit_code=0):
        inputs = FinalEvalRuntimeInputs(
            authority_capability=object(),
            root_capability=object(),
            worker_launcher=lambda: exit_code,
            evidence_sink=lambda payload: "evidence-ref",
        )
        return FinalEvalRuntime(inputs=inputs)

    def test_happy_path_reaches_authority_terminal(self) -> None:
        runtime = self._runtime(exit_code=0)
        result = runtime.run()
        self.assertEqual(result["outcome"], "SUCCEEDED")
        self.assertEqual(result["saga_state"], "AUTHORITY_TERMINAL")
        self.assertEqual(len(result["steps"]), 6)

    def test_worker_timeout_maps_to_timed_out(self) -> None:
        runtime = self._runtime(exit_code=124)
        result = runtime.run()
        self.assertEqual(result["outcome"], "TIMEOUT")

    def test_worker_crash_maps_to_crashed(self) -> None:
        runtime = self._runtime(exit_code=1)
        result = runtime.run()
        self.assertEqual(result["outcome"], "CRASHED")

    def test_factory_rejects_non_inputs(self) -> None:
        with self.assertRaises(FinalEvalRuntimeRejected):
            FinalEvalRuntime(inputs=object())  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
