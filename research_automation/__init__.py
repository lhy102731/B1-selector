"""Lazy public surface for the research automation package."""
from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORTS: dict[str, tuple[str, str | None]] = {
    "AutomationController": (".automation_controller", "AutomationController"),
    "TaskQueue": (".task_queue", "TaskQueue"),
    "ExperimentTask": (".task_queue", "ExperimentTask"),
    "Experiment": (".experiment", "Experiment"),
    "ExperimentStatus": (".experiment", "ExperimentStatus"),
    "Proposal": (".experiment", "Proposal"),
    "StandardMetrics": (".experiment", "StandardMetrics"),
    "RegistryReference": (".experiment", "RegistryReference"),
    "ApprovalGate": (".approval_gate", "ApprovalGate"),
    "ApprovalDecision": (".approval_gate", "ApprovalDecision"),
    "CodeChangeExecutor": (".experiment_runner", "CodeChangeExecutor"),
    "BacktestExecutor": (".experiment_runner", "BacktestExecutor"),
    "ClaudeCodeExecutor": (".experiment_runner", "ClaudeCodeExecutor"),
    "StubCodeChangeExecutor": (
        ".experiment_runner",
        "StubCodeChangeExecutor",
    ),
    "StubBacktestExecutor": (".experiment_runner", "StubBacktestExecutor"),
    "RealBacktestExecutor": (".experiment_runner", "RealBacktestExecutor"),
    "BacktestResultAdapter": (".experiment_runner", "BacktestResultAdapter"),
    "EntrypointSpec": (".experiment_runner", "EntrypointSpec"),
    "CodeChangeResult": (".experiment_runner", "CodeChangeResult"),
    "BacktestResult": (".experiment_runner", "BacktestResult"),
    "generate_experiment_task_md": (
        ".experiment_runner",
        "generate_experiment_task_md",
    ),
    "BacktestResultParser": (".result_parser", "BacktestResultParser"),
    "ReportGenerator": (".report_generator", "ReportGenerator"),
    "RegistryUpdater": (".registry_updater", "RegistryUpdater"),
    "RegistryMergeError": (".registry_updater", "RegistryMergeError"),
    "SnapshotUpdater": (".snapshot_updater", "SnapshotUpdater"),
    "HandoffUpdater": (".handoff_updater", "HandoffUpdater"),
    "GpuCapability": (".gpu_acceleration", "GpuCapability"),
    "GpuDevice": (".gpu_acceleration", "GpuDevice"),
    "build_compute_acceleration_plan": (
        ".gpu_acceleration",
        "build_compute_acceleration_plan",
    ),
    "detect_nvidia_gpu": (".gpu_acceleration", "detect_nvidia_gpu"),
    "infer_workload_type": (".gpu_acceleration", "infer_workload_type"),
    "parse_nvidia_smi_csv": (".gpu_acceleration", "parse_nvidia_smi_csv"),
    "ParameterProposer": (".proposer", "ParameterProposer"),
    "AG2TaskAdapter": (".ag2_task_adapter", "AG2TaskAdapter"),
    "PromotionEvaluator": (".promotion", "PromotionEvaluator"),
    "CandidatePool": (".promotion", "CandidatePool"),
    "NightlyReport": (".nightly_report", "NightlyReport"),
    "AutonomousRunnerV1": (".autonomous_runner", "AutonomousRunnerV1"),
    "safety": (".safety", None),
    "strategies": (".strategies", None),
    "StrategyProfile": (".strategies", "StrategyProfile"),
    "get_profile": (".strategies", "get_profile"),
    "require_supported": (".strategies", "require_supported"),
    "UnsupportedStrategyError": (".strategies", "UnsupportedStrategyError"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError as error:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from error
    module = import_module(module_name, __name__)
    value = module if attribute_name is None else getattr(module, attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
