"""Run the Brick AG2-KBase SQ NAV handoff backlog.

This is an operational helper, not a strategy implementation. It executes a
fixed list of APPROVED discovery handoffs into a research-only output folder
and writes a status file after every task so a long run can be monitored.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from .control_plane.contracts import SideEffect
from .control_plane.sink_guard import (
    ExecutionAuthorizationError,
    ExecutionInvocation,
    ExecutionSinkGuard,
)
from .control_plane.stores import AuthorityReader, TaskExecutionLease
from .control_plane.provenance import stamp_legacy_result


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = ROOT / "research_state" / "brick" / "backlog_sqnav_execution_20260709"

DEFAULT_TASKS = [
    ("peer_rank", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260708T190414339600Z.yaml"),
    ("pool_quality", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260708T192744232979Z.yaml"),
    ("shadow", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260708T195605011158Z.yaml"),
    ("volume_support", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260708T200252136829Z.yaml"),
    ("sentiment_pullback", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260708T202637907121Z.yaml"),
    ("volume_equilibrium", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260708T204212308448Z.yaml"),
    ("peer_count_regime", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260708T221357215265Z.yaml"),
    ("w_bottom", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260708T223401856131Z.yaml"),
    ("path_efficiency", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260708T225031960093Z.yaml"),
    ("downside_skew", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260709T001452931179Z.yaml"),
    ("vol_path_smoothness", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260709T004102298516Z.yaml"),
    ("streak_exhaustion", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260709T005051203586Z.yaml"),
    ("range_width", "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260709T010332296385Z.yaml"),
]

VOLUME_AUTHENTICITY_TASK = (
    "volume_authenticity",
    "research_state/kbase_discovery_handoffs/discovery_brick_APPROVED_20260708T201508685462Z.yaml",
)


def _write_status(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(stamp_legacy_result(payload), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def run_backlog(
    args: argparse.Namespace,
    *,
    lease: TaskExecutionLease | None = None,
    invocation: ExecutionInvocation | None = None,
    execution_lease: TaskExecutionLease | None = None,
    execution_invocation: ExecutionInvocation | None = None,
    authority_reader: AuthorityReader | None = None,
    repository_root: str | Path | None = None,
) -> dict:
    lease = lease if lease is not None else execution_lease
    invocation = invocation if invocation is not None else execution_invocation
    output_root = Path(args.output_root).resolve()
    tasks = list(DEFAULT_TASKS)
    if args.include_volume_authenticity:
        tasks.insert(4, VOLUME_AUTHENTICITY_TASK)

    try:
        permit = ExecutionSinkGuard(
            authority_reader=authority_reader or AuthorityReader(),
            repository_root=repository_root or ROOT,
        ).authorize(lease, invocation)
        if permit.operation != "BACKLOG" or permit.effect is not SideEffect.RUN_RESEARCH:
            raise ExecutionAuthorizationError(
                "SQ-NAV backlog requires a RUN_RESEARCH BACKLOG intent"
            )
        if not isinstance(invocation, ExecutionInvocation) or (
            invocation.runner.module != "research_automation.run_brick_sqnav_backlog"
            or invocation.runner.callable_name != "run_backlog"
        ):
            raise ExecutionAuthorizationError("SQ-NAV backlog entry identity is invalid")
        required_resources = {output_root}
        required_resources.update((ROOT / handoff).resolve() for _, handoff in tasks)
        if not required_resources.issubset(set(permit.resource_paths)):
            raise ExecutionAuthorizationError(
                "SQ-NAV backlog output/handoff resources differ from execution intent"
            )
    except (ExecutionAuthorizationError, OSError, ValueError) as error:
        raise ExecutionAuthorizationError(
            f"SQ-NAV backlog authority rejected: {error}"
        ) from error

    output_root.mkdir(parents=True, exist_ok=True)

    records: list[dict] = []
    status_path = output_root / "backlog_status.json"
    started_at = datetime.now().isoformat(timespec="seconds")

    def write_status(current: str | None) -> None:
        _write_status(status_path, {
            "started_at": started_at,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "current": current,
            "total": len(tasks),
            "completed": sum(1 for record in records if record.get("returncode") == 0),
            "failed": sum(1 for record in records if record.get("returncode") not in (None, 0)),
            "records": records,
        })

    write_status(None)
    for index, (slug, handoff) in enumerate(tasks, start=1):
        task_dir = output_root / slug
        task_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = task_dir / "execute_stdout.txt"
        stderr_path = task_dir / "execute_stderr.txt"
        cmd = [
            sys.executable,
            "run_research.py",
            "execute-handoff",
            "--strategy",
            "brick",
            "--handoff-path",
            handoff,
            "--output-dir",
            str(task_dir),
        ]
        record = {
            "index": index,
            "slug": slug,
            "handoff": str((ROOT / handoff).resolve()),
            "output_dir": str(task_dir.resolve()),
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "returncode": None,
            "elapsed_seconds": None,
            "stdout_path": str(stdout_path.resolve()),
            "stderr_path": str(stderr_path.resolve()),
            "produced_files": [],
        }
        records.append(record)
        write_status(slug)
        t0 = time.time()
        with stdout_path.open("w", encoding="utf-8", errors="replace") as stdout:
            with stderr_path.open("w", encoding="utf-8", errors="replace") as stderr:
                proc = subprocess.run(cmd, cwd=str(ROOT), stdout=stdout, stderr=stderr, text=True)
        record["returncode"] = proc.returncode
        record["elapsed_seconds"] = round(time.time() - t0, 3)
        record["finished_at"] = datetime.now().isoformat(timespec="seconds")
        record["produced_files"] = [
            str(path.relative_to(task_dir))
            for path in task_dir.rglob("*")
            if path.is_file()
        ][:300]
        next_task = tasks[index][0] if index < len(tasks) else "complete"
        write_status(next_task)
        if proc.returncode != 0 and args.stop_on_failure:
            break

    write_status("complete")
    return json.loads(status_path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Brick SQ NAV handoff backlog")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--include-volume-authenticity", action="store_true")
    parser.add_argument("--stop-on-failure", action="store_true")
    return parser.parse_args()


def main() -> None:
    result = run_backlog(parse_args())
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
