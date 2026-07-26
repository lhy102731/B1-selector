"""Run AG2-KBase Brick SQ-NAV optimization rounds until a deadline.

This wrapper is intentionally thin: AG2 performs discovery, the existing
discovery execution bridge chooses registered Phase 6 runners, and this file
only records round state and enforces the "do not start a new round after the
deadline" rule.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from research_automation.control_plane.contracts import SideEffect
from research_automation.control_plane.sink_guard import (
    ExecutionAuthorizationError,
    ExecutionInvocation,
    ExecutionSinkGuard,
)
from research_automation.control_plane.stores import AuthorityReader, TaskExecutionLease


ROOT = Path(__file__).resolve().parents[1]
HANDOFF_DIR = ROOT / "research_state" / "kbase_discovery_handoffs"


def now_local() -> datetime:
    return datetime.now().astimezone()


def iso_now() -> str:
    return now_local().isoformat(timespec="seconds")


def append_event(path: Path, event: dict[str, Any]) -> None:
    event = {"ts": iso_now(), **event}
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")


def write_json(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def run_command(command: list[str], *, log_path: Path, timeout: float | None = None) -> dict[str, Any]:
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    started = time.time()
    with log_path.open("w", encoding="utf-8", errors="replace", newline="\n") as log:
        log.write(f"$ {' '.join(command)}\n\n")
        log.flush()
        proc = subprocess.Popen(
            command,
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        captured: list[str] = []
        assert proc.stdout is not None
        for line in proc.stdout:
            captured.append(line)
            log.write(line)
            log.flush()
        return_code = proc.wait(timeout=timeout)
    return {
        "returncode": return_code,
        "elapsed_seconds": round(time.time() - started, 3),
        "stdout_tail": "".join(captured[-80:]),
    }


def latest_handoff_after(started_at: float) -> Path | None:
    if not HANDOFF_DIR.is_dir():
        return None
    candidates = [
        path for path in HANDOFF_DIR.glob("discovery_brick_*_*.yaml")
        if path.stat().st_mtime >= started_at - 2
    ]
    return max(candidates, key=lambda p: p.stat().st_mtime) if candidates else None


def parse_handoff_path(text: str, started_at: float) -> Path | None:
    match = re.search(r"Discovery handoff:\s*(.+)", text)
    if match:
        path = Path(match.group(1).strip().strip('"')).resolve()
        if path.is_file():
            return path
    return latest_handoff_after(started_at)


def read_handoff_status(path: Path | None) -> dict[str, Any]:
    if path is None or not path.is_file():
        return {"path": None, "status": "MISSING"}
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001 - status reporting only
        return {"path": str(path), "status": "UNREADABLE", "error": f"{type(exc).__name__}: {exc}"}
    return {
        "path": str(path),
        "status": str(doc.get("status") or "UNKNOWN").upper(),
        "topic": doc.get("topic"),
        "created_at": doc.get("created_at"),
    }


def is_missing_runner_failure(result: dict[str, Any]) -> bool:
    text = str(result.get("stdout_tail") or "")
    return (
        "no registered Phase 6 runner" in text
        or "no discovery execution runner registered" in text
    )


def produced_files(round_dir: Path) -> list[str]:
    excluded = {"context.md", "discover.log", "execute.log", "runner_repair.log", "execute_after_repair.log"}
    return [
        str(path.relative_to(round_dir))
        for path in sorted(round_dir.rglob("*"))
        if path.is_file() and path.name not in excluded
    ]


def build_round_context(base_context: str, state: dict[str, Any]) -> str:
    recent = state.get("rounds", [])[-4:]
    return (
        base_context
        + "\n\n## Recent Autorun Rounds\n\n"
        + yaml.safe_dump(recent, allow_unicode=True, sort_keys=False)
    )


def discover_topic(round_number: int) -> str:
    return (
        f"Brick V2 Signal Quality NAV optimization round {round_number}: "
        "use KBase source briefs and roundtable debate to propose genuinely new "
        "pre-09:25 factor design or interaction features for Brick candidate ranking. "
        "Primary baseline is Top3 no-timing SQ NAV; Top5 is capacity control. "
        "Avoid parameter-only sweeps and hard market timing."
    )


def main(
    *,
    lease: TaskExecutionLease | None = None,
    invocation: ExecutionInvocation | None = None,
    execution_lease: TaskExecutionLease | None = None,
    execution_invocation: ExecutionInvocation | None = None,
    authority_reader: AuthorityReader | None = None,
    repository_root: str | Path | None = None,
) -> int:
    parser = argparse.ArgumentParser(description="Brick AG2-KBase SQ-NAV autorun")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--context-file", required=True)
    parser.add_argument("--deadline", required=True, help="ISO datetime with timezone, e.g. 2026-07-09T09:00:00+08:00")
    parser.add_argument("--max-rounds", type=int, default=99)
    parser.add_argument("--sleep-between-rounds", type=int, default=20)
    parser.add_argument(
        "--auto-repair-runners",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When execute-handoff fails because no runner is registered, call the code-writing repair path and retry once.",
    )
    parser.add_argument("--runner-repair-timeout", type=int, default=900)
    parser.add_argument("--runner-repair-claude-binary", default="claude")
    parser.add_argument("--skip-runner-repair-code-review", action="store_true")
    parser.add_argument(
        "--discover-mode",
        choices=["roundtable", "sequential"],
        default="roundtable",
        help="roundtable uses kbase_roundtable_discovery; sequential uses the older single-pass kbase_discovery",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    context_path = Path(args.context_file).resolve()
    lease = lease if lease is not None else execution_lease
    invocation = invocation if invocation is not None else execution_invocation
    try:
        permit = ExecutionSinkGuard(
            authority_reader=authority_reader or AuthorityReader(),
            repository_root=repository_root or ROOT,
        ).authorize(lease, invocation)
        if permit.operation != "AUTORUN" or permit.effect is not SideEffect.RUN_RESEARCH:
            raise ExecutionAuthorizationError(
                "Brick SQ-NAV autorun requires a RUN_RESEARCH AUTORUN intent"
            )
        if not isinstance(invocation, ExecutionInvocation) or (
            invocation.runner.module != "research.brick_ag2_kbase_sqnav_autorun"
            or invocation.runner.callable_name != "main"
        ):
            raise ExecutionAuthorizationError("Brick SQ-NAV autorun entry identity is invalid")
        required_resources = {output_dir, context_path, HANDOFF_DIR.resolve()}
        if not required_resources.issubset(set(permit.resource_paths)):
            raise ExecutionAuthorizationError(
                "Brick SQ-NAV autorun resources differ from execution intent"
            )
    except (ExecutionAuthorizationError, OSError, ValueError) as error:
        print(f"[brick_sqnav_autorun] blocked: {error}")
        return 3

    output_dir.mkdir(parents=True, exist_ok=True)
    event_log = output_dir / "events.ndjson"
    status_path = output_dir / "status.json"
    base_context = context_path.read_text(encoding="utf-8")
    deadline = datetime.fromisoformat(args.deadline)

    state: dict[str, Any] = {
        "status": "running",
        "started_at": iso_now(),
        "deadline": deadline.isoformat(),
        "discover_mode": args.discover_mode,
        "baseline_context": str(Path(args.context_file).resolve()),
        "rounds": [],
    }
    write_json(status_path, state)
    append_event(event_log, {"event": "autorun_started", "deadline": deadline.isoformat()})

    for round_number in range(1, args.max_rounds + 1):
        if now_local() >= deadline:
            append_event(event_log, {"event": "deadline_reached_before_new_round", "round": round_number})
            break

        round_dir = output_dir / f"round_{round_number:02d}"
        round_dir.mkdir(parents=True, exist_ok=True)
        round_context = round_dir / "context.md"
        round_context.write_text(build_round_context(base_context, state), encoding="utf-8")

        round_record: dict[str, Any] = {
            "round": round_number,
            "started_at": iso_now(),
            "topic": discover_topic(round_number),
            "round_dir": str(round_dir),
        }
        state["rounds"].append(round_record)
        write_json(status_path, state)
        append_event(event_log, {"event": "round_started", "round": round_number})

        discover_started = time.time()
        discover_cmd = [
            sys.executable,
            "run_research.py",
            "discover",
            "--strategy",
            "brick",
            "--topic",
            discover_topic(round_number),
            "--context-file",
            str(round_context),
        ]
        if args.discover_mode == "sequential":
            discover_cmd.append("--sequential")
        round_record["discover_mode"] = args.discover_mode
        discover_result = run_command(discover_cmd, log_path=round_dir / "discover.log")
        handoff_path = parse_handoff_path(discover_result.get("stdout_tail", ""), discover_started)
        handoff_status = read_handoff_status(handoff_path)
        round_record["discover"] = {**discover_result, "handoff": handoff_status}
        write_json(status_path, state)
        append_event(event_log, {"event": "discover_finished", "round": round_number, "handoff": handoff_status})

        if discover_result["returncode"] == 0 and handoff_status.get("status") == "APPROVED":
            execution_dir = round_dir / "execution"
            execute_cmd = [
                sys.executable,
                "run_research.py",
                "execute-handoff",
                "--strategy",
                "brick",
                "--handoff-path",
                str(handoff_path),
                "--output-dir",
                str(execution_dir),
            ]
            execute_result = run_command(execute_cmd, log_path=round_dir / "execute.log")
            round_record["execute"] = {**execute_result, "produced_files": produced_files(round_dir)[-80:]}
            append_event(
                event_log,
                {"event": "execute_finished", "round": round_number, "returncode": execute_result["returncode"]},
            )
            if (
                args.auto_repair_runners
                and execute_result["returncode"] != 0
                and is_missing_runner_failure(execute_result)
            ):
                repair_dir = round_dir / "runner_repair"
                repair_cmd = [
                    sys.executable,
                    "run_research.py",
                    "repair-handoff-runner",
                    "--handoff-path",
                    str(handoff_path),
                    "--output-dir",
                    str(repair_dir),
                    "--failure-log",
                    str(round_dir / "execute.log"),
                    "--claude-binary",
                    args.runner_repair_claude_binary,
                    "--timeout",
                    str(args.runner_repair_timeout),
                ]
                if args.skip_runner_repair_code_review:
                    repair_cmd.append("--skip-code-review")
                append_event(event_log, {"event": "runner_repair_started", "round": round_number})
                repair_result = run_command(repair_cmd, log_path=round_dir / "runner_repair.log")
                round_record["runner_repair"] = repair_result
                append_event(
                    event_log,
                    {"event": "runner_repair_finished", "round": round_number, "returncode": repair_result["returncode"]},
                )
                if repair_result["returncode"] == 0:
                    retry_dir = round_dir / "execution_after_repair"
                    retry_cmd = [
                        sys.executable,
                        "run_research.py",
                        "execute-handoff",
                        "--strategy",
                        "brick",
                        "--handoff-path",
                        str(handoff_path),
                        "--output-dir",
                        str(retry_dir),
                    ]
                    retry_result = run_command(retry_cmd, log_path=round_dir / "execute_after_repair.log")
                    round_record["execute_after_repair"] = {
                        **retry_result,
                        "produced_files": produced_files(round_dir)[-120:],
                    }
                    append_event(
                        event_log,
                        {
                            "event": "execute_after_repair_finished",
                            "round": round_number,
                            "returncode": retry_result["returncode"],
                        },
                    )
        else:
            round_record["execute"] = {
                "skipped": True,
                "reason": "discovery did not return an APPROVED handoff",
            }
            append_event(event_log, {"event": "execute_skipped", "round": round_number})

        round_record["finished_at"] = iso_now()
        write_json(status_path, state)

        if now_local() >= deadline:
            append_event(event_log, {"event": "deadline_reached_after_round", "round": round_number})
            break
        time.sleep(max(0, args.sleep_between_rounds))

    state["status"] = "complete"
    state["finished_at"] = iso_now()
    write_json(status_path, state)
    append_event(event_log, {"event": "autorun_finished", "rounds": len(state["rounds"])})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
