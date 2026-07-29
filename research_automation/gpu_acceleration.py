"""GPU acceleration planning helpers for heavy research tasks.

This module only detects capability and builds an execution plan. It does not
change backtest semantics or silently switch algorithms.
"""
from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass, asdict
from typing import Callable, Sequence


GPU_FRIENDLY_WORKLOADS = {
    "indicator_precompute",
    "ml_training",
    "ranker_training",
    "factor_matrix",
    "parameter_sweep",
    "clustering",
    "shap",
}

CPU_BOUND_WORKLOADS = {
    "event_backtest",
    "io_bound",
    "state_machine",
}


@dataclass
class GpuDevice:
    name: str
    memory_total_mb: int | None = None
    driver_version: str | None = None


@dataclass
class GpuCapability:
    available: bool
    devices: list[GpuDevice]
    source: str
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "available": self.available,
            "devices": [asdict(device) for device in self.devices],
            "source": self.source,
            "error": self.error,
        }


def parse_nvidia_smi_csv(text: str) -> list[GpuDevice]:
    """Parse `nvidia-smi --query-gpu=name,memory.total,driver_version` CSV."""
    devices: list[GpuDevice] = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split(",")]
        name = parts[0] if parts else ""
        memory = None
        if len(parts) > 1:
            memory_text = parts[1].lower().replace("mib", "").replace("mb", "").strip()
            try:
                memory = int(float(memory_text))
            except ValueError:
                memory = None
        driver = parts[2] if len(parts) > 2 and parts[2] else None
        if name:
            devices.append(GpuDevice(name=name, memory_total_mb=memory, driver_version=driver))
    return devices


def detect_nvidia_gpu(
    runner: Callable[..., subprocess.CompletedProcess] | None = None,
) -> GpuCapability:
    """Detect NVIDIA GPU capability using nvidia-smi, with safe CPU fallback."""
    binary = shutil.which("nvidia-smi")
    if not binary:
        return GpuCapability(False, [], "nvidia-smi", "nvidia-smi not found")
    runner = runner or subprocess.run
    try:
        result = runner(
            [
                binary,
                "--query-gpu=name,memory.total,driver_version",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception as error:
        return GpuCapability(False, [], "nvidia-smi", f"{type(error).__name__}: {error}")
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "").strip() or f"exit {result.returncode}"
        return GpuCapability(False, [], "nvidia-smi", message)
    devices = parse_nvidia_smi_csv(result.stdout)
    return GpuCapability(bool(devices), devices, "nvidia-smi", None if devices else "no devices")


def infer_workload_type(description: str | Sequence[str] | dict) -> str:
    """Infer a coarse workload class from a task description."""
    if isinstance(description, dict):
        blob = " ".join(f"{key} {value}" for key, value in description.items())
    elif isinstance(description, (list, tuple, set)):
        blob = " ".join(str(item) for item in description)
    else:
        blob = str(description or "")
    text = blob.lower()
    if any(token in text for token in ("lightgbm", "lambdarank", "ranker", "model train", "training")):
        return "ranker_training"
    if any(token in text for token in ("shap", "treeexplainer")):
        return "shap"
    if any(token in text for token in ("kmeans", "cluster", "hdbscan", "gmm")):
        return "clustering"
    if any(token in text for token in ("indicator", "precompute", "ma/ema", "kdj", "macd")):
        return "indicator_precompute"
    if any(token in text for token in ("factor matrix", "feature matrix", "candidate factors", "parquet panel")):
        return "factor_matrix"
    if any(token in text for token in ("grid", "sweep", "parameter")):
        return "parameter_sweep"
    if any(token in text for token in ("event backtest", "state machine", "trade replay")):
        return "event_backtest"
    return "unknown"


def build_compute_acceleration_plan(
    workload_type: str,
    capability: GpuCapability | None = None,
) -> dict:
    """Return a structured plan that AG2 can copy into execution_record."""
    capability = capability or detect_nvidia_gpu()
    workload = (workload_type or "unknown").lower()
    gpu_applicable = workload in GPU_FRIENDLY_WORKLOADS
    cpu_bound = workload in CPU_BOUND_WORKLOADS

    if gpu_applicable and capability.available:
        if workload in {"ml_training", "ranker_training"}:
            backend = "lightgbm_gpu_or_cuda"
        elif workload == "clustering":
            backend = "rapids_or_cuml_if_available"
        elif workload == "shap":
            backend = "gpu_tree_explainer_if_supported"
        elif workload in {"factor_matrix", "indicator_precompute"}:
            backend = "cupy_or_gpu_dataframe_if_available"
        else:
            backend = "mixed_gpu_cpu"
        reason = "GPU-friendly workload and NVIDIA GPU detected."
    else:
        backend = "cpu"
        if cpu_bound:
            reason = "Workload is event/state-machine or IO bound; GPU is not the primary accelerator."
        elif not gpu_applicable:
            reason = "Workload type is unknown or not GPU-friendly."
        else:
            reason = capability.error or "No usable GPU detected."

    return {
        "workload_type": workload,
        "gpu_applicable": gpu_applicable,
        "gpu_available": capability.available,
        "selected_backend": backend,
        "fallback_backend": "cpu",
        "reason": reason,
        "devices": [asdict(device) for device in capability.devices],
    }
