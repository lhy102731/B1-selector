"""Bridge APPROVED KBase discovery handoffs to project-side Phase 6 runners.

This module does not write to KBase and does not promote factors. It only turns
an approved discovery handoff into a bounded, auditable research execution plan.
"""
from __future__ import annotations

import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from ag2_research.discovery_handoff import (
    extract_discovery_transcript,
    extract_stage_outputs,
)
from research_automation.control_plane.contracts import SideEffect
from research_automation.control_plane.sink_guard import (
    ExecutionAuthorizationError,
    ExecutionInvocation,
    ExecutionSinkGuard,
    AuthorizedSubprocess,
)
from research_automation.control_plane.stores import AuthorityReader, TaskExecutionLease


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class DiscoveryExecutionPlan:
    handoff_path: str
    strategy_id: str
    runner_id: str
    runner_script: str
    output_dir: str
    factor_names: list[str]
    reason: str
    command: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_handoff_document(path: str | Path) -> dict[str, Any]:
    handoff_path = Path(path).resolve()
    if not handoff_path.is_file():
        raise FileNotFoundError(f"handoff not found: {handoff_path}")
    document = yaml.safe_load(handoff_path.read_text(encoding="utf-8")) or {}
    if not isinstance(document, dict) or document.get("handoff_type") != "kbase_discovery":
        raise ValueError("not a kbase_discovery handoff")
    return document


def extract_factor_output(document: dict[str, Any]) -> dict[str, Any]:
    transcript = extract_discovery_transcript(document)
    if not transcript:
        raise ValueError("handoff has no discovery transcript")
    outputs = extract_stage_outputs(transcript)
    factor_output = outputs.get("factor_engineer") or {}
    if not isinstance(factor_output, dict) or not factor_output:
        raise ValueError("handoff has no factor_engineer output")
    return factor_output


def extract_factor_batch(document: dict[str, Any]) -> list[dict[str, Any]]:
    factor_output = extract_factor_output(document)
    factors = factor_output.get("factor_batch")
    if not isinstance(factors, list) or not factors:
        raise ValueError("handoff has no factor_batch")
    return [factor for factor in factors if isinstance(factor, dict)]


def _factor_names(factors: list[dict[str, Any]]) -> list[str]:
    return [str(factor.get("name") or "").strip() for factor in factors if factor.get("name")]


def _has_sector_relative_shape(factors: list[dict[str, Any]]) -> bool:
    names = _factor_names(factors)
    if not names:
        return False
    reqs = {
        str(req)
        for factor in factors
        for req in (factor.get("data_requirements") or [])
    }
    return (
        all(name.startswith("sector_relative_") for name in names)
        and "sector_classification" in reqs
    )


def _has_path_volume_shape(factors: list[dict[str, Any]]) -> bool:
    names = _factor_names(factors)
    joined = " ".join(names).lower()
    return (
        any(name.startswith("pv_") for name in names)
        or "path_volume" in joined
        or "wbottom" in joined
    )


def _has_volume_authenticity_shape(factors: list[dict[str, Any]]) -> bool:
    names = set(_factor_names(factors))
    required = {
        "stock_volume_contraction_20d",
        "market_volume_contraction_20d",
        "volume_shrinkage_authenticity_rank",
    }
    return required.issubset(names)


def _has_peer_relative_shape(factors: list[dict[str, Any]]) -> bool:
    names = set(_factor_names(factors))
    required = {
        "volume_contraction_ratio_peer_rank",
        "turnover_state_peer_rank",
        "ma_distance_peer_rank",
    }
    return required.issubset(names) or (
        bool(names)
        and all(name.endswith("_peer_rank") for name in names)
    )


def _has_brick_sequence_state_shape(factors: list[dict[str, Any]]) -> bool:
    names = set(_factor_names(factors))
    required = {
        "brick_same_color_run_length",
        "brick_reversal_recency",
        "brick_run_length_ratio",
    }
    return required.issubset(names)


def _has_pool_quality_topk_shape(factors: list[dict[str, Any]]) -> bool:
    names = set(_factor_names(factors))
    required = {
        "pullback_coherence_fraction",
        "gap_dispersion_std",
        "yellow_zone_lower_quartile_fraction",
        "composite_pool_quality_score",
    }
    joined = " ".join(names).lower()
    return (
        required.issubset(names)
        or "candidate_pool" in joined
        or "pool_density" in joined
        or "dynamic_topk" in joined
    )


def _has_regime_abstention_factor_shape(factors: list[dict[str, Any]]) -> bool:
    names = " ".join(_factor_names(factors)).lower()
    return (
        "abstention" in names
        or "no_trade" in names
        or "skip_signal" in names
        or "regime_conditioned" in names
    )


def _has_label_reconstruction_factor_shape(factors: list[dict[str, Any]]) -> bool:
    names = " ".join(_factor_names(factors)).lower()
    return (
        "label_reconstruction" in names
        or "residualized_return" in names
        or "hold_days_decay" in names
        or "training_target" in names
    )


def _has_generated_daily_factor_shape(factors: list[dict[str, Any]]) -> bool:
    return bool(_factor_names(factors))


def _has_label_reconstruction_handoff(document: dict[str, Any]) -> bool:
    text = yaml.safe_dump(document, allow_unicode=True, sort_keys=False).lower()
    return (
        (
            "label reconstruction" in text
            or "label_reconstruction" in text
            or "标签重构" in text
            or "residualized return" in text
            or "residualized_return" in text
            or "残差化" in text
        )
        and (
            "hold_days" in text
            or "持有期" in text
            or "exp(-lambda" in text
        )
    )


def _has_regime_abstention_handoff(document: dict[str, Any]) -> bool:
    text = yaml.safe_dump(document, allow_unicode=True, sort_keys=False).lower()
    return (
        "abstention" in text
        and (
            "signal_day_regime_label" in text
            or "regime-conditioned" in text
            or "regime_conditioned" in text
            or "peer_signal_count" in text
        )
    )


def select_runner(strategy_id: str, factors: list[dict[str, Any]]) -> tuple[str, Path, str]:
    """Select a registered project-side runner for a factor batch."""
    if strategy_id != "brick":
        raise ValueError(f"no discovery execution runner registered for strategy={strategy_id}")
    if _has_brick_sequence_state_shape(factors):
        return (
            "brick_sequence_state_phase6",
            PROJECT_ROOT / "research" / "brick_sequence_state_phase6.py",
            "signal-day Brick internal sequence state with blocking pre-screen and fixed 3-fold PWF",
        )
    if _has_pool_quality_topk_shape(factors):
        return (
            "brick_pool_quality_topk_phase6",
            PROJECT_ROOT / "research" / "brick_pool_quality_topk_phase6.py",
            "daily candidate-pool quality / dynamic TopK factor batch with frozen SQ NAV baseline",
        )
    if _has_label_reconstruction_factor_shape(factors):
        return (
            "brick_label_reconstruction_phase6",
            PROJECT_ROOT / "research" / "brick_label_reconstruction_phase6.py",
            "training-target label reconstruction and hold-days sample weighting with strict train-validation-test folds",
        )
    if _has_regime_abstention_factor_shape(factors):
        return (
            "brick_regime_abstention_phase6",
            PROJECT_ROOT / "research" / "brick_regime_abstention_phase6.py",
            "regime-conditioned pool-level signal abstention with strict train-validation-test folds",
        )
    if _has_peer_relative_shape(factors):
        return (
            "brick_peer_relative_sqnav_phase6",
            PROJECT_ROOT / "research" / "brick_peer_relative_sqnav_phase6.py",
            "same-day candidate-pool peer-relative factor batch with Signal Quality NAV surface",
        )
    if _has_sector_relative_shape(factors):
        return (
            "brick_sector_relative_phase6",
            PROJECT_ROOT / "research" / "brick_sector_relative_phase6.py",
            "sector-relative factor batch with sector_classification requirements",
        )
    if _has_path_volume_shape(factors):
        return (
            "brick_path_volume_phase6",
            PROJECT_ROOT / "research" / "brick_path_volume_phase6.py",
            "path-volume / W-bottom factor batch",
        )
    if _has_volume_authenticity_shape(factors):
        return (
            "brick_volume_authenticity_phase6",
            PROJECT_ROOT / "research" / "brick_volume_authenticity_phase6.py",
            "volume shrinkage authenticity factor batch",
        )
    if _has_generated_daily_factor_shape(factors):
        return (
            "brick_generated_daily_factor_sqnav_phase6",
            PROJECT_ROOT / "research" / "brick_generated_daily_factor_sqnav_phase6.py",
            "generic signal-day daily-bar factor batch with strict rolling Signal Quality NAV",
        )
    names = ", ".join(_factor_names(factors)[:8])
    raise ValueError(f"no registered Phase 6 runner for factor_batch: {names}")


def build_execution_plan(
    handoff_path: str | Path,
    *,
    output_dir: str | Path | None = None,
    timestamp: str | None = None,
) -> DiscoveryExecutionPlan:
    handoff = Path(handoff_path).resolve()
    document = load_handoff_document(handoff)
    status = str(document.get("status") or "").upper()
    if status != "APPROVED":
        raise ValueError(f"handoff status must be APPROVED, got {status or 'UNKNOWN'}")
    strategy_id = str(document.get("strategy_id") or "").lower()
    factor_output = extract_factor_output(document)
    factors: list[dict[str, Any]] = []
    factor_error = ""
    try:
        factors = extract_factor_batch(document)
        runner_id, script, reason = select_runner(strategy_id, factors)
        plan_names = _factor_names(factors)
    except ValueError as error:
        factor_error = str(error)
        mechanism = factor_output.get("research_mechanism")
        routing_source = mechanism if isinstance(mechanism, dict) else factor_output
        if strategy_id == "brick" and _has_label_reconstruction_handoff(routing_source):
            runner_id, script, reason = (
                "brick_label_reconstruction_phase6",
                PROJECT_ROOT / "research" / "brick_label_reconstruction_phase6.py",
                "training-target label reconstruction and hold-days sample weighting with strict train-validation-test folds",
            )
            plan_names = ["label_reconstruction_hold_days_decay"]
        elif strategy_id == "brick" and _has_regime_abstention_handoff(routing_source):
            runner_id, script, reason = (
                "brick_regime_abstention_phase6",
                PROJECT_ROOT / "research" / "brick_regime_abstention_phase6.py",
                "regime-conditioned pool-level signal abstention with strict train-validation-test folds",
            )
            plan_names = ["regime_conditioned_abstention"]
        elif (
            strategy_id == "brick"
            and isinstance(mechanism, dict)
            and str(mechanism.get("name") or "").strip()
            == "aggregate_liquidity_state_volume_feature_modulation"
        ):
            runner_id, script, reason = (
                "brick_aggregate_liquidity_modulation_phase6",
                PROJECT_ROOT / "research" / "brick_aggregate_liquidity_modulation_phase6.py",
                "aggregate signal-day liquidity state modulation with gated diagnostics and strict 3-fold PWF",
            )
            plan_names = ["aggregate_liquidity_state_volume_feature_modulation"]
        elif isinstance(mechanism, dict):
            name = str(mechanism.get("name") or "<unnamed>").strip()
            declared = str(mechanism.get("runner_id") or "<missing>").strip()
            raise ValueError(
                "no registered Phase 6 runner for research_mechanism "
                f"'{name}'; declared runner_id '{declared}' is advisory until the "
                "execution bridge registers a compatible --handoff-path/--output-dir runner"
            ) from error
        else:
            raise ValueError(factor_error) from error
    if not script.is_file():
        raise FileNotFoundError(f"registered runner script is missing: {script}")
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    out = Path(output_dir).resolve() if output_dir else (
        PROJECT_ROOT / "research_state" / strategy_id / f"{runner_id}_{stamp}"
    ).resolve()
    command = [
        sys.executable,
        str(script),
        "--handoff-path",
        str(handoff),
        "--output-dir",
        str(out),
    ]
    return DiscoveryExecutionPlan(
        handoff_path=str(handoff),
        strategy_id=strategy_id,
        runner_id=runner_id,
        runner_script=str(script),
        output_dir=str(out),
        factor_names=plan_names,
        reason=reason,
        command=command,
    )


def _canonical_plan_path(value: str, *, field_name: str) -> Path:
    """Resolve a plan path without permitting a relative or ambiguous sink."""
    if not isinstance(value, str) or not value.strip():
        raise ExecutionAuthorizationError(f"{field_name} must be a non-empty path")
    candidate = Path(value)
    if not candidate.is_absolute():
        raise ExecutionAuthorizationError(f"{field_name} must be absolute")
    try:
        return candidate.resolve(strict=False)
    except (OSError, ValueError) as error:
        raise ExecutionAuthorizationError(f"{field_name} cannot be resolved") from error


def _assert_plan_matches_permit(
    plan: DiscoveryExecutionPlan,
    invocation: ExecutionInvocation,
    *,
    permit: object,
) -> None:
    """Bind the caller-visible plan fields to the already verified intent.

    ``ExecutionSinkGuard`` verifies the immutable intent and lease.  The plan
    is still an ordinary dataclass supplied by the caller, so it must not be
    allowed to steer a different handoff, runner, or output directory after
    that verification.  This check is deliberately exact for the fields that
    the bridge turns into a subprocess command.
    """
    if tuple(plan.command) != invocation.argv:
        raise ExecutionAuthorizationError("execution plan command differs from invocation")
    if getattr(permit, "argv", None) != invocation.argv:
        raise ExecutionAuthorizationError("execution permit command is invalid")

    project_root = PROJECT_ROOT.resolve(strict=True)
    runner_path = _canonical_plan_path(plan.runner_script, field_name="runner_script")
    try:
        declared_runner = (project_root / invocation.runner.source_ref).resolve(strict=True)
    except (OSError, ValueError) as error:
        raise ExecutionAuthorizationError("invocation runner source is unavailable") from error
    if runner_path != declared_runner:
        raise ExecutionAuthorizationError("execution plan runner differs from invocation")

    if invocation.cwd is None:
        raise ExecutionAuthorizationError("discovery subprocess requires a working directory")
    invocation_cwd = _canonical_plan_path(invocation.cwd, field_name="invocation.cwd")
    if invocation_cwd != project_root:
        raise ExecutionAuthorizationError("discovery subprocess cwd must be the project root")

    handoff_path = _canonical_plan_path(plan.handoff_path, field_name="handoff_path")
    output_path = _canonical_plan_path(plan.output_dir, field_name="output_dir")
    command = tuple(plan.command)
    if len(command) != 6 or command[2] != "--handoff-path" or command[4] != "--output-dir":
        raise ExecutionAuthorizationError("discovery command shape is invalid")
    if _canonical_plan_path(command[1], field_name="command.runner_script") != runner_path:
        raise ExecutionAuthorizationError("command runner differs from execution plan")
    if _canonical_plan_path(command[3], field_name="command.handoff_path") != handoff_path:
        raise ExecutionAuthorizationError("command handoff differs from execution plan")
    if _canonical_plan_path(command[5], field_name="command.output_dir") != output_path:
        raise ExecutionAuthorizationError("command output differs from execution plan")
    approved_paths = tuple(getattr(permit, "resource_paths", ()))
    if runner_path not in approved_paths:
        raise ExecutionAuthorizationError("runner path is not bound by the execution intent")
    if handoff_path not in approved_paths:
        raise ExecutionAuthorizationError("handoff path is not bound by the execution intent")
    if output_path not in approved_paths:
        raise ExecutionAuthorizationError("output directory is not bound by the execution intent")


def execute_plan(
    plan: DiscoveryExecutionPlan,
    *,
    dry_run: bool = False,
    execution_lease: TaskExecutionLease | None = None,
    execution_invocation: ExecutionInvocation | None = None,
) -> subprocess.CompletedProcess | None:
    """Execute one registered discovery plan behind the P0R2 sink boundary.

    Existing callers retain their keyword signature, but a real execution now
    requires a live task lease and the matching immutable execution invocation.
    The authority check happens before the output directory is created.  A
    dry run remains a side-effect-free preview and therefore does not require a
    lease.
    """
    if dry_run:
        return None
    if execution_lease is None or execution_invocation is None:
        raise ExecutionAuthorizationError(
            "a live execution lease and immutable invocation are required"
        )

    # Use the shared guard before the first visible effect (mkdir).  The
    # AuthorizedSubprocess wrapper repeats the check immediately before the
    # subprocess call, covering changes made while the output directory was
    # being prepared.
    authority_reader = AuthorityReader()
    guard = ExecutionSinkGuard(
        authority_reader=authority_reader,
        repository_root=PROJECT_ROOT,
    )
    permit = guard.authorize(execution_lease, execution_invocation)
    if (
        permit.operation != "SUBPROCESS"
        or permit.effect is not SideEffect.START_SUBPROCESS
    ):
        raise ExecutionAuthorizationError(
            "discovery execution requires a bound subprocess operation"
        )
    _assert_plan_matches_permit(
        plan,
        execution_invocation,
        permit=permit,
    )

    output_path = _canonical_plan_path(plan.output_dir, field_name="output_dir")
    output_path.mkdir(parents=True, exist_ok=True)
    sink = AuthorizedSubprocess(
        authority_reader=authority_reader,
        repository_root=PROJECT_ROOT,
    )
    return sink.run(execution_lease, execution_invocation)
