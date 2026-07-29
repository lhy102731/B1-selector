from __future__ import annotations

import subprocess
import unittest
from unittest.mock import patch

from research_automation.gpu_acceleration import (
    GpuCapability,
    GpuDevice,
    build_compute_acceleration_plan,
    detect_nvidia_gpu,
    infer_workload_type,
    parse_nvidia_smi_csv,
)


class GpuAccelerationTests(unittest.TestCase):
    def test_parse_nvidia_smi_csv(self):
        devices = parse_nvidia_smi_csv("NVIDIA RTX 4090, 24564, 555.85\n")

        self.assertEqual("NVIDIA RTX 4090", devices[0].name)
        self.assertEqual(24564, devices[0].memory_total_mb)
        self.assertEqual("555.85", devices[0].driver_version)

    def test_detect_nvidia_gpu_uses_safe_fallback_without_binary(self):
        with patch("research_automation.gpu_acceleration.shutil.which", return_value=None):
            capability = detect_nvidia_gpu()

        self.assertFalse(capability.available)
        self.assertIn("not found", capability.error)

    def test_detect_nvidia_gpu_with_mocked_runner(self):
        def runner(*_args, **_kwargs):
            return subprocess.CompletedProcess(
                args=["nvidia-smi"], returncode=0,
                stdout="NVIDIA A5000, 24564, 552.22\n",
                stderr="",
            )

        with patch("research_automation.gpu_acceleration.shutil.which", return_value="nvidia-smi"):
            capability = detect_nvidia_gpu(runner=runner)

        self.assertTrue(capability.available)
        self.assertEqual("NVIDIA A5000", capability.devices[0].name)

    def test_infer_workload_type(self):
        self.assertEqual("ranker_training", infer_workload_type("LightGBM LambdaRank training"))
        self.assertEqual("indicator_precompute", infer_workload_type("MA/EMA KDJ MACD indicator precompute"))
        self.assertEqual("clustering", infer_workload_type("KMeans cluster stability"))
        self.assertEqual("event_backtest", infer_workload_type("event backtest trade replay"))

    def test_build_plan_prefers_gpu_for_ranker_when_available(self):
        capability = GpuCapability(
            available=True,
            devices=[GpuDevice("NVIDIA RTX 4090", 24564, "555.85")],
            source="test",
        )

        plan = build_compute_acceleration_plan("ranker_training", capability)

        self.assertTrue(plan["gpu_applicable"])
        self.assertTrue(plan["gpu_available"])
        self.assertEqual("lightgbm_gpu_or_cuda", plan["selected_backend"])

    def test_build_plan_records_cpu_bound_backtest_reason(self):
        capability = GpuCapability(available=True, devices=[GpuDevice("GPU")], source="test")

        plan = build_compute_acceleration_plan("event_backtest", capability)

        self.assertFalse(plan["gpu_applicable"])
        self.assertEqual("cpu", plan["selected_backend"])
        self.assertIn("state-machine", plan["reason"])

    def test_build_plan_treats_indicator_precompute_as_gpu_friendly(self):
        capability = GpuCapability(available=True, devices=[GpuDevice("GPU")], source="test")

        plan = build_compute_acceleration_plan("indicator_precompute", capability)

        self.assertTrue(plan["gpu_applicable"])
        self.assertEqual("cupy_or_gpu_dataframe_if_available", plan["selected_backend"])


if __name__ == "__main__":
    unittest.main()
