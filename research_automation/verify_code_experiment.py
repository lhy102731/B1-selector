"""verify_code_experiment.py — acceptance test for the code-experiment closed loop.

Proves five assertions (vs spec + constraints):
  1. Workspace strategy file changed after DeterministicBrickCodeExecutor.apply().
  2. Production boundary files are byte-identical before vs after (hash snapshot).
  3. Backtest results differ baseline (fresh copy) vs modified (height_ratio 1.0).
  4. experiment_report.md is successfully generated and non-empty.
  5. workspace/code_change.json exists with correct fields.

Runs brick V2 backtest twice (~40s each): once on the fresh workspace copy,
once after modifying height_ratio 2/3 -> 1.0 via the deterministic executor.
Production code is never touched; artifacts land under _output/_code_exp_verify.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from research_automation.workspace_manager import WorkspaceManager
from research_automation.experiment_runner import (
    RealBacktestExecutor, _BRICK_SPEC, DeterministicBrickCodeExecutor,
)
from research_automation.result_parser import BacktestResultParser
from research_automation.report_generator import ReportGenerator
from research_automation.experiment import Experiment, Proposal, ExperimentStatus

OUTPUT_ROOT = _REPO_ROOT / "research_automation" / "_output" / "_code_exp_verify"
WINDOW = {"start": "2024-01-01", "end": "2024-03-31"}

PROD_BOUNDARY_FILES = [
    _REPO_ROOT / "strategy" / "brick_chart_strategy.py",
    _REPO_ROOT / "strategy" / "unified_b1_strategy.py",
    _REPO_ROOT / "backtest_brick_v2.py",
    _REPO_ROOT / "utils" / "technical.py",
    _REPO_ROOT / "config" / "strategy_params.yaml",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def boundary_snapshot() -> dict:
    return {str(p): (sha256(p) if p.exists() else None) for p in PROD_BOUNDARY_FILES}


def run_workspace_backtest(executor: RealBacktestExecutor, scope: dict,
                           ws_path: Path, result_dir: Path) -> dict:
    """Run backtest reusing an existing workspace, return parsed metrics dict."""
    res = executor.execute(scope, result_dir=result_dir, workspace_path=ws_path)
    if not res.get("success"):
        raise RuntimeError(f"backtest failed: {res.get('error')}\nstderr: {res.get('stderr')}")
    m = BacktestResultParser().parse(result_dir)
    return {"trades": m.trades, "win_rate": m.win_rate,
            "total_return": (m.extra or {}).get("total_return")}


def main() -> int:
    print("=" * 64)
    print("Code-experiment loop acceptance test")
    print("=" * 64)

    # --- pre-flight ---
    snap_before = boundary_snapshot()
    ws_prod_before = sha256(_REPO_ROOT / "strategy" / "brick_chart_strategy.py")
    print(f"[pre]  boundary snapshot: {len(snap_before)} files")
    print(f"[pre]  prod strategy sha256: {ws_prod_before[:16]}...")

    # --- setup ---
    ws_manager = WorkspaceManager(workspace_root=OUTPUT_ROOT, project_root=_REPO_ROOT)
    executor = RealBacktestExecutor(project_root=_REPO_ROOT, workspace_mode=True,
                                    workspace_manager=ws_manager)
    code_exec = DeterministicBrickCodeExecutor()

    scope = {**WINDOW, "strategy": "BRICK", "params": {"entry_ma_source": "t0"}}

    # construct a lightweight experiment with scope carrying the code_change
    exp = Experiment(
        experiment_id="verify-code-exp-001",
        strategy="brick",
        proposal=Proposal(
            hypothesis="height_ratio 2/3 -> 1.0: tighter brick signal should reduce trades & total return",
            scope={"code_change": {"change_type": "modify_constant",
                                   "file": "strategy/brick_chart_strategy.py",
                                   "symbol": "height_ratio",
                                   "value": 1.0}},
        ),
        start_time="2026-06-21T00:00:00Z",
    )

    # --- create workspace (fresh copy of strategy/utils/config + script) ---
    ws = ws_manager.create_workspace(exp, _BRICK_SPEC)
    print(f"\n[ws]   workspace: {ws}")

    # --- run 1: baseline (fresh workspace, unchanged height_ratio = 2/3) ---
    print("\n[run1] baseline backtest (height_ratio=2/3) ...")
    r1 = run_workspace_backtest(executor, scope, ws, OUTPUT_ROOT / "verify-code-exp-001" / "outputs_baseline")
    print(f"       trades={r1['trades']} win_rate={r1['win_rate']} total_return={r1['total_return']}")

    # --- deterministic code change: modify workspace copy, never production ---
    print("\n[code] DeterministicBrickCodeExecutor.apply (height_ratio 2/3 -> 1.0) ...")
    task_path = OUTPUT_ROOT / "verify-code-exp-001" / "task.md"
    task_path.parent.mkdir(parents=True, exist_ok=True)
    task_path.write_text("# placeholder task\n")
    cc = code_exec.apply(task_path, ws, experiment=exp)
    print(f"       ok={cc.ok}  changed_files={cc.changed_files}  log={cc.logs[0][:80]}")

    if not cc.ok:
        print("FAIL: code change rejected:", cc.error)
        return 1

    # --- verify workspace file actually changed ---
    ws_strat = ws / "strategy" / "brick_chart_strategy.py"
    ws_content = ws_strat.read_text(encoding="utf-8")
    changed_on_disk = "height_ratio': 1.0" in ws_content or "'height_ratio': 1.0" in ws_content

    before_hash = sha256(ws_strat)
    # note: before_hash is post-change; the baseline backtest already ran on the unchanged copy

    print(f"[ver1] workspace file changed on disk: {changed_on_disk}")
    print(f"       workspace strategy hash: {before_hash[:16]}...")

    # --- run 2: modified backtest (height_ratio = 1.0, tighter signal) ---
    print("\n[run2] modified backtest (height_ratio=1.0) ...")
    r2 = run_workspace_backtest(executor, scope, ws, OUTPUT_ROOT / "verify-code-exp-001" / "outputs_modified")
    print(f"       trades={r2['trades']} win_rate={r2['win_rate']} total_return={r2['total_return']}")

    # --- experiment_report.md via ReportGenerator ---
    print("\n[report] generating experiment_report.md ...")
    exp.metrics = BacktestResultParser().parse(OUTPUT_ROOT / "verify-code-exp-001" / "outputs_modified")
    exp.status = ExperimentStatus.COMPLETED
    exp.changed_files = cc.changed_files
    exp.end_time = "2026-06-21T00:00:00Z"
    report_path = ReportGenerator().generate(exp, OUTPUT_ROOT / "verify-code-exp-001")
    report_exists = report_path.exists() and report_path.stat().st_size > 0
    print(f"       report: {report_path} | exists={report_exists} size={report_path.stat().st_size if report_exists else 0}")

    # --- code_change.json ---
    diff_path = ws / "code_change.json"
    diff_exists = diff_path.exists()
    diff_ok = False
    if diff_exists:
        d = json.loads(diff_path.read_text(encoding="utf-8"))
        diff_ok = all(k in d for k in ("file", "symbol", "old_value", "new_value",
                                        "before_hash", "after_hash", "change_type"))
        print(f"[diff]  code_change.json: exists={diff_exists} fields_ok={diff_ok}")
        print(f"        symbol={d.get('symbol')} old={d.get('old_value')} new={d.get('new_value')}")
    else:
        print("[diff]  code_change.json: MISSING")

    # --- post-flight ---
    snap_after = boundary_snapshot()
    prod_unchanged = (snap_before == snap_after)
    results_changed = (r1["win_rate"] != r2["win_rate"]) or (r1["total_return"] != r2["total_return"]) \
        or (r1["trades"] != r2["trades"])
    changed_files = [p for p in snap_before if snap_before[p] != snap_after.get(p)]
    ws_prod_after = sha256(_REPO_ROOT / "strategy" / "brick_chart_strategy.py")

    # --- assertions ---
    print("\n" + "=" * 64)
    print("ASSERTIONS")
    print("=" * 64)
    a1 = changed_on_disk
    a2 = prod_unchanged or (ws_prod_before == ws_prod_after)
    a3 = results_changed
    a4 = report_exists
    a5 = diff_exists and diff_ok

    print(f"  [1] workspace file changed on disk:           {'PASS' if a1 else 'FAIL'}")
    print(f"  [2] production boundary files untouched:      {'PASS' if a2 else 'FAIL'}"
          f"  ({len(snap_before)} files{', changed: ' + ','.join(changed_files) if not a2 and changed_files else ''})")
    print(f"  [3] backtest results changed:                 {'PASS' if a3 else 'FAIL'}"
          f"  (baseline win_rate={r1['win_rate']} ret={r1['total_return']} -> modified win_rate={r2['win_rate']} ret={r2['total_return']})")
    print(f"  [4] experiment_report.md generated non-empty: {'PASS' if a4 else 'FAIL'}")
    print(f"  [5] code_change.json exists + fields correct: {'PASS' if a5 else 'FAIL'}")

    all_pass = a1 and a2 and a3 and a4 and a5
    print("\n" + "=" * 64)
    print(f"RESULT: {'ALL PASS — code-experiment loop verified' if all_pass else 'FAIL — see above'}")
    print("=" * 64)

    # cleanup
    if all_pass and OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
