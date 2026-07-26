"""One archived KBase -> AG2 -> research execution cycle.

The cycle is research-only. It may create or repair research runners, but it
never promotes a result into production and never writes to the KBase vault.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from ag2_research import Orchestrator
from ag2_research.discovery_handoff import save_discovery_handoff

from .discovery_execution_bridge import DiscoveryExecutionPlan, build_execution_plan
from .handoff_runner_repair import repair_handoff_runner
from .control_plane.contracts import SideEffect
from .control_plane.sink_guard import (
    AuthorizedSubprocess,
    ExecutionAuthorizationError,
    ExecutionInvocation,
    ExecutionSinkGuard,
)
from .control_plane.stores import AuthorityReader, TaskExecutionLease


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_WORKFLOW_ID = "kbase_roundtable_discovery"
PRODUCTION_BOUNDARY_FILES = (
    "backtest_brick_v2.py",
    "daily_select.py",
    "strategy/brick_chart_strategy.py",
    "models/brick/legacy/ml_v21/ml_ranker_model_v21.pkl",
    "models/brick/legacy/ml_v21/ml_ranker_scaler_v21.pkl",
    "project_brick_v2.yaml",
    "registry_brick_v2.yaml",
    "config/strategy_params.yaml",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            suffix=".tmp",
            prefix=f".{path.name}.",
            dir=path.parent,
            delete=False,
        ) as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = Path(handle.name)
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def _safe_strategy_id(strategy_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", strategy_id or ""):
        raise ValueError("strategy_id may contain only letters, numbers, '_' and '-'")
    return strategy_id.lower()


def _cycle_directory(
    strategy_id: str,
    *,
    output_dir: str | Path | None,
    timestamp: str | None,
) -> Path:
    if output_dir:
        destination = Path(output_dir).resolve()
    else:
        stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
        destination = (
            PROJECT_ROOT
            / "research_state"
            / strategy_id
            / "kbase_ag2_cycles"
            / f"kbase_ag2_full_cycle_{stamp}"
        ).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    return destination


def fingerprint_production_boundary(project_root: Path = PROJECT_ROOT) -> dict[str, Any]:
    """Fingerprint production code/models and indicator-cache metadata."""
    files: dict[str, Any] = {}
    for relative in PRODUCTION_BOUNDARY_FILES:
        path = project_root / relative
        if path.is_file():
            stat = path.stat()
            files[relative] = {
                "sha256": _sha256(path),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        else:
            files[relative] = {"missing": True}

    cache_root = project_root / "data" / "indicators_cache"
    cache_rows: list[str] = []
    total_bytes = 0
    if cache_root.is_dir():
        for path in sorted(item for item in cache_root.rglob("*") if item.is_file()):
            stat = path.stat()
            total_bytes += stat.st_size
            cache_rows.append(
                f"{path.relative_to(cache_root).as_posix()}\t{stat.st_size}\t{stat.st_mtime_ns}"
            )
    cache_digest = hashlib.sha256("\n".join(cache_rows).encode("utf-8")).hexdigest()
    return {
        "files": files,
        "indicator_cache": {
            "count": len(cache_rows),
            "total_bytes": total_bytes,
            "metadata_sha256": cache_digest,
        },
    }


def _record_stage(
    manifest: dict[str, Any],
    manifest_path: Path,
    stage: str,
    status: str,
    **details: Any,
) -> None:
    manifest.setdefault("stages", []).append(
        {
            "stage": stage,
            "status": status,
            "recorded_at": _utc_now(),
            **details,
        }
    )
    _write_json_atomic(manifest_path, manifest)


def _load_handoff(path: Path) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(document, dict) or document.get("handoff_type") != "kbase_discovery":
        raise ValueError("not a kbase_discovery handoff")
    return document


def _runner_error_is_repairable(error: Exception) -> bool:
    message = str(error).lower()
    return any(
        marker in message
        for marker in (
            "no registered phase 6 runner",
            "no discovery execution runner registered",
            "registered runner script is missing",
        )
    )


def _execute_with_logs(
    plan: DiscoveryExecutionPlan,
    *,
    stdout_path: Path,
    stderr_path: Path,
    lease: TaskExecutionLease,
    invocation: ExecutionInvocation,
    authority_reader: AuthorityReader,
) -> subprocess.CompletedProcess[Any]:
    if tuple(plan.command) != invocation.argv:
        raise ExecutionAuthorizationError("full-cycle command differs from execution intent")
    if invocation.cwd is None or Path(invocation.cwd).resolve() != PROJECT_ROOT.resolve():
        raise ExecutionAuthorizationError("full-cycle cwd differs from execution intent")

    def _runner(command, **kwargs):
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        with (
            stdout_path.open("w", encoding="utf-8", newline="\n") as stdout_handle,
            stderr_path.open("w", encoding="utf-8", newline="\n") as stderr_handle,
        ):
            return subprocess.run(
                command,
                cwd=kwargs.get("cwd", str(PROJECT_ROOT)),
                stdout=stdout_handle,
                stderr=stderr_handle,
                check=False,
            )

    return AuthorizedSubprocess(
        authority_reader=authority_reader,
        repository_root=PROJECT_ROOT,
        runner=_runner,
    ).run(lease, invocation)


def _read_runner_status(output_dir: Path) -> dict[str, Any] | None:
    status_path = output_dir / "status.json"
    if not status_path.is_file():
        return None
    try:
        value = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _finish_cycle(
    manifest: dict[str, Any],
    manifest_path: Path,
    status: str,
    **details: Any,
) -> dict[str, Any]:
    after = fingerprint_production_boundary()
    unchanged = manifest.get("production_boundary_before") == after
    manifest.update(
        {
            "status": status if unchanged else "PRODUCTION_BOUNDARY_CHANGED",
            "completed_at": _utc_now(),
            "production_boundary_after": after,
            "production_boundary_unchanged": unchanged,
            "production_promotion_performed": False,
            "kbase_write_performed": False,
            **details,
        }
    )
    _write_json_atomic(manifest_path, manifest)
    return manifest


def run_kbase_ag2_full_cycle(
    *,
    topic: str | None = None,
    strategy_id: str = "brick",
    profile: str | None = None,
    research_context: str = "",
    handoff_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    workflow_id: str = DEFAULT_WORKFLOW_ID,
    auto_repair: bool = True,
    dry_run: bool = False,
    claude_binary: str = "claude",
    repair_timeout: int = 900,
    timestamp: str | None = None,
    lease=None,
    invocation=None,
    execution_lease=None,
    execution_invocation=None,
    subprocess_lease=None,
    subprocess_invocation=None,
    execution_subprocess_lease=None,
    execution_subprocess_invocation=None,
    authority_reader=None,
    repository_root: str | Path | None = None,
) -> dict[str, Any]:
    """Run one complete archived research cycle without production promotion."""
    lease = lease if lease is not None else execution_lease
    invocation = invocation if invocation is not None else execution_invocation
    subprocess_lease = (
        subprocess_lease
        if subprocess_lease is not None
        else execution_subprocess_lease
    )
    subprocess_invocation = (
        subprocess_invocation
        if subprocess_invocation is not None
        else execution_subprocess_invocation
    )
    if not isinstance(lease, TaskExecutionLease) or not isinstance(
        invocation, ExecutionInvocation
    ):
        return {
            "status": "UNAUTHORIZED",
            "reason": "execution lease and invocation are required before full-cycle execution",
            "cycle_dir": None if output_dir is None else str(Path(output_dir).resolve()),
            "production_promotion_performed": False,
            "kbase_write_performed": False,
        }
    try:
        reader = authority_reader if isinstance(authority_reader, AuthorityReader) else AuthorityReader()
        guard = ExecutionSinkGuard(
            authority_reader=reader,
            repository_root=repository_root or PROJECT_ROOT,
        )
        permit = guard.authorize(lease, invocation)
        if (
            permit.operation != "FULL_CYCLE"
            or permit.effect is not SideEffect.RUN_RESEARCH
        ):
            raise ExecutionAuthorizationError(
                "full-cycle requires a RUN_RESEARCH FULL_CYCLE intent"
            )
    except (ExecutionAuthorizationError, OSError, ValueError) as error:
        return {
            "status": "UNAUTHORIZED",
            "reason": str(error),
            "cycle_dir": None if output_dir is None else str(Path(output_dir).resolve()),
            "production_promotion_performed": False,
            "kbase_write_performed": False,
        }
    strategy = _safe_strategy_id(strategy_id)
    cycle_dir = _cycle_directory(strategy, output_dir=output_dir, timestamp=timestamp)
    manifest_path = cycle_dir / "cycle_manifest.json"
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "cycle_id": cycle_dir.name,
        "created_at": _utc_now(),
        "status": "RUNNING",
        "strategy_id": strategy,
        "topic": topic,
        "workflow_id": workflow_id if handoff_path is None else None,
        "dry_run": bool(dry_run),
        "auto_repair": bool(auto_repair),
        "cycle_dir": str(cycle_dir),
        "production_promotion_allowed": False,
        "kbase_write_allowed": False,
        "production_boundary_before": fingerprint_production_boundary(),
        "stages": [],
    }
    _write_json_atomic(manifest_path, manifest)

    try:
        if handoff_path is None:
            if not topic:
                raise ValueError("topic is required when handoff_path is omitted")
            _record_stage(manifest, manifest_path, "discovery", "RUNNING")
            orchestrator = Orchestrator(profile=profile)
            discovery_result = orchestrator.run_workflow(
                workflow_id,
                topic=topic,
                research_context=research_context,
                strategy_id=strategy,
            )
            handoff = save_discovery_handoff(
                discovery_result,
                topic=topic,
                strategy_id=strategy,
                output_dir=cycle_dir / "discovery",
            )
            discovery_status = str(discovery_result.get("status") or "UNKNOWN").upper()
            _record_stage(
                manifest,
                manifest_path,
                "discovery",
                discovery_status,
                handoff_path=str(handoff),
                reason=discovery_result.get("reason"),
            )
        else:
            handoff = Path(handoff_path).resolve()
            document = _load_handoff(handoff)
            if str(document.get("strategy_id") or "").lower() != strategy:
                raise ValueError("handoff strategy_id does not match requested strategy")
            topic = str(document.get("topic") or topic or "")
            manifest["topic"] = topic
            discovery_status = str(document.get("status") or "UNKNOWN").upper()
            _record_stage(
                manifest,
                manifest_path,
                "discovery",
                "REUSED_HANDOFF",
                handoff_status=discovery_status,
                handoff_path=str(handoff),
                handoff_sha256=_sha256(handoff),
            )

        manifest["handoff_path"] = str(handoff)
        if discovery_status != "APPROVED":
            return _finish_cycle(
                manifest,
                manifest_path,
                "DISCOVERY_STOP",
                reason=f"handoff status is {discovery_status}",
            )

        execution_output = cycle_dir / "execution"
        _record_stage(manifest, manifest_path, "execution_plan", "RUNNING")
        try:
            plan = build_execution_plan(handoff, output_dir=execution_output)
        except (ValueError, FileNotFoundError) as error:
            if not auto_repair or not _runner_error_is_repairable(error):
                _record_stage(
                    manifest,
                    manifest_path,
                    "execution_plan",
                    "FAILED",
                    error=f"{type(error).__name__}: {error}",
                )
                return _finish_cycle(
                    manifest,
                    manifest_path,
                    "EXECUTION_PLAN_FAILED",
                    reason=str(error),
                )

            _record_stage(
                manifest,
                manifest_path,
                "runner_repair",
                "RUNNING",
                trigger=f"{type(error).__name__}: {error}",
            )
            repair = repair_handoff_runner(
                handoff_path=handoff,
                output_dir=cycle_dir / "runner_repair",
                claude_binary=claude_binary,
                timeout=repair_timeout,
                dry_run=dry_run,
                skip_code_review=False,
            )
            _record_stage(
                manifest,
                manifest_path,
                "runner_repair",
                repair.status.upper(),
                repair_result=repair.to_dict(),
            )
            if dry_run:
                return _finish_cycle(
                    manifest,
                    manifest_path,
                    "DRY_RUN_REPAIR_REQUIRED",
                    reason="runner repair prompt archived; no code generated",
                )
            if not repair.ok:
                return _finish_cycle(
                    manifest,
                    manifest_path,
                    "RUNNER_REPAIR_FAILED",
                    reason=repair.error or repair.status,
                )
            try:
                plan = build_execution_plan(handoff, output_dir=execution_output)
            except (ValueError, FileNotFoundError) as retry_error:
                return _finish_cycle(
                    manifest,
                    manifest_path,
                    "RUNNER_REPAIR_INCOMPLETE",
                    reason=f"{type(retry_error).__name__}: {retry_error}",
                )

        plan_path = cycle_dir / "execution_plan.json"
        _write_json_atomic(plan_path, plan.to_dict())
        _record_stage(
            manifest,
            manifest_path,
            "execution_plan",
            "READY",
            execution_plan_path=str(plan_path),
            runner_id=plan.runner_id,
            runner_script=plan.runner_script,
        )
        if dry_run:
            return _finish_cycle(
                manifest,
                manifest_path,
                "DRY_RUN_READY",
                execution_plan=plan.to_dict(),
            )

        _record_stage(manifest, manifest_path, "research_execution", "RUNNING")
        stdout_path = cycle_dir / "execute.stdout.log"
        stderr_path = cycle_dir / "execute.stderr.log"
        if not isinstance(subprocess_lease, TaskExecutionLease) or not isinstance(
            subprocess_invocation, ExecutionInvocation
        ):
            return _finish_cycle(
                manifest,
                manifest_path,
                "EXECUTION_UNAUTHORIZED",
                reason="subprocess lease and invocation are required",
            )
        try:
            process = _execute_with_logs(
                plan,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                lease=subprocess_lease,
                invocation=subprocess_invocation,
                authority_reader=reader,
            )
        except ExecutionAuthorizationError as error:
            return _finish_cycle(
                manifest,
                manifest_path,
                "EXECUTION_UNAUTHORIZED",
                reason=str(error),
            )
        runner_status = _read_runner_status(Path(plan.output_dir))
        _record_stage(
            manifest,
            manifest_path,
            "research_execution",
            "PROCESS_COMPLETED" if process.returncode == 0 else "PROCESS_FAILED",
            returncode=process.returncode,
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            runner_status=runner_status,
        )
        if process.returncode != 0:
            return _finish_cycle(
                manifest,
                manifest_path,
                "RESEARCH_EXECUTION_FAILED",
                returncode=process.returncode,
                runner_status=runner_status,
            )
        if not runner_status or str(runner_status.get("status") or "").lower() != "complete":
            return _finish_cycle(
                manifest,
                manifest_path,
                "RESEARCH_EXECUTION_INCOMPLETE",
                reason="runner did not produce a complete status.json",
                runner_status=runner_status,
            )

        promotion_gate_passed = bool(runner_status.get("promotion_gate_passed"))
        final_status = (
            "AWAITING_HUMAN_PROMOTION"
            if promotion_gate_passed
            else "COMPLETED_NOT_PROMOTED"
        )
        return _finish_cycle(
            manifest,
            manifest_path,
            final_status,
            research_status=runner_status.get("research_status"),
            promotion_gate_passed=promotion_gate_passed,
            runner_status=runner_status,
            execution_plan=plan.to_dict(),
        )
    except Exception as error:  # noqa: BLE001 - unexpected errors must be archived.
        _record_stage(
            manifest,
            manifest_path,
            "cycle_controller",
            "FAILED",
            error=f"{type(error).__name__}: {error}",
        )
        return _finish_cycle(
            manifest,
            manifest_path,
            "CYCLE_CONTROLLER_FAILED",
            reason=f"{type(error).__name__}: {error}",
        )


def cycle_exit_code(result: dict[str, Any]) -> int:
    """Map archived cycle states to a CLI process status."""
    successful = {
        "COMPLETED_NOT_PROMOTED",
        "AWAITING_HUMAN_PROMOTION",
        "DISCOVERY_STOP",
        "DRY_RUN_READY",
        "DRY_RUN_REPAIR_REQUIRED",
    }
    return 0 if result.get("status") in successful else 1


__all__ = [
    "DEFAULT_WORKFLOW_ID",
    "cycle_exit_code",
    "fingerprint_production_boundary",
    "run_kbase_ag2_full_cycle",
]
