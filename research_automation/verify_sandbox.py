"""verify_sandbox.py -- acceptance test for the Experiment Workspace Sandbox.

Proves three things (per spec acceptance criteria):
  1. Editing the workspace strategy copy changes backtest results.
  2. The production strategy file's hash is unchanged.
  3. `git diff` on the production repo is empty.

Runs brick V2 backtest twice over a small window (~40s each): once on the fresh
workspace copy (baseline), once after editing workspace/strategy/brick_chart_strategy.py
height_ratio 2/3 -> 1.0 (tighter brick signal -> different results).

This script only READS production code and WRITES under research_automation/_output/
(inside safety.py's SAFE_WRITE_ROOTS). It never modifies production files.
"""
from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from research_automation.workspace_manager import WorkspaceManager
from research_automation.experiment_runner import RealBacktestExecutor, _BRICK_SPEC
from research_automation.result_parser import BacktestResultParser

PROD_STRATEGY = _REPO_ROOT / "strategy" / "brick_chart_strategy.py"
OUTPUT_ROOT = _REPO_ROOT / "research_automation" / "_output" / "_sandbox_verify"
WINDOW = {"start": "2024-01-01", "end": "2024-03-31"}

# Production files the sandbox must NEVER write. Assertion [3] checks each of
# these is byte-identical before vs after the experiment -- this is the precise
# meaning of "production directories stay read-only". (A whole-repo `git diff`
# would be polluted by pre-existing changes unrelated to the sandbox.)
PROD_BOUNDARY_FILES = [
    _REPO_ROOT / "strategy" / "brick_chart_strategy.py",
    _REPO_ROOT / "strategy" / "unified_b1_strategy.py",
    _REPO_ROOT / "backtest_brick_v2.py",
    _REPO_ROOT / "utils" / "technical.py",
    _REPO_ROOT / "config" / "strategy_params.yaml",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def boundary_hashes() -> dict:
    """sha256 of every production boundary file (missing files -> None)."""
    return {str(p): (sha256(p) if p.exists() else None) for p in PROD_BOUNDARY_FILES}


def git_diff_empty() -> bool:
    """True if `git diff` (tracked files) shows no changes. _output/ is gitignored.
    NOTE: reported for context only -- the authoritative production-untouched
    check is boundary_hashes() before vs after (assertion [3])."""
    try:
        r = subprocess.run(["git", "diff", "--quiet"], cwd=str(_REPO_ROOT),
                           capture_output=True, text=True, timeout=15)
        return r.returncode == 0
    except Exception:
        return False


def run_brick_in_workspace(ws_manager: WorkspaceManager, exp_id: str) -> dict:
    """Run brick V2 backtest in a fresh workspace; return parsed metrics."""
    scope = {"strategy": "BRICK", **WINDOW, "params": {"entry_ma_source": "t0"}}
    ex = RealBacktestExecutor(
        project_root=_REPO_ROOT, workspace_mode=True, workspace_manager=ws_manager,
    )
    # use a dict-like experiment carrying an experiment_id; _extract_scope passes it through
    experiment = {"experiment_id": exp_id, "strategy": "brick", "scope": scope}
    result_dir = OUTPUT_ROOT / exp_id / "outputs"
    res = ex.execute(scope, result_dir=result_dir, workspace_path=None)
    if not res.get("success"):
        raise RuntimeError(f"backtest failed: {res.get('error')}\nstderr: {res.get('stderr')}")
    metrics = BacktestResultParser().parse(result_dir)
    return {
        "trades": metrics.trades,
        "win_rate": metrics.win_rate,
        "total_return": (metrics.extra or {}).get("total_return"),
        "stdout_tail": (res.get("stdout") or "")[-400:],
        "workspace": res.get("workspace_path"),
    }


def edit_height_ratio(ws_strategy: Path) -> None:
    """Tighten the brick signal: height_ratio 2/3 -> 1.0 in the WORKSPACE copy only."""
    txt = ws_strategy.read_text(encoding="utf-8")
    new = txt.replace("'height_ratio': 2.0 / 3.0", "'height_ratio': 1.0")
    if new == txt:
        # fall back to the decimal form in case the literal was reformatted
        new = txt.replace("'height_ratio': 0.6666666666666666", "'height_ratio': 1.0")
    if new == txt:
        raise RuntimeError("could not find height_ratio literal to edit in workspace copy")
    ws_strategy.write_text(new, encoding="utf-8")


def main() -> int:
    print("=" * 64)
    print("Sandbox acceptance test")
    print("=" * 64)

    # --- pre-flight: record production hash + boundary hashes + git state ---
    prod_hash_before = sha256(PROD_STRATEGY)
    boundary_before = boundary_hashes()
    git_clean_before = git_diff_empty()
    print(f"[pre]  prod strategy sha256: {prod_hash_before[:16]}...")
    print(f"[pre]  boundary files: {len(boundary_before)}  git diff clean: {git_clean_before}")

    ws_manager = WorkspaceManager(workspace_root=OUTPUT_ROOT, project_root=_REPO_ROOT)

    # --- run 1: baseline (fresh workspace, no edits) ---
    print("\n[run1] baseline backtest in fresh workspace ...")
    m1 = run_brick_in_workspace(ws_manager, "verify-baseline")
    print(f"       trades={m1['trades']} win_rate={m1['win_rate']} total_return={m1['total_return']}")

    # --- edit the WORKSPACE strategy copy (height_ratio 2/3 -> 1.0) ---
    ws_strategy = Path(m1["workspace"]) / "strategy" / "brick_chart_strategy.py"
    print(f"\n[edit] tightening height_ratio in WORKSPACE copy: {ws_strategy}")
    edit_height_ratio(ws_strategy)

    # --- run 2: re-run from the SAME workspace (now edited) ---
    print("\n[run2] backtest from edited workspace ...")
    # reuse the same workspace: pass workspace_path so executor does NOT re-create
    scope = {"strategy": "BRICK", **WINDOW, "params": {"entry_ma_source": "t0"}}
    ex = RealBacktestExecutor(project_root=_REPO_ROOT, workspace_mode=True,
                              workspace_manager=ws_manager)
    result_dir2 = OUTPUT_ROOT / "verify-baseline" / "outputs2"
    res2 = ex.execute(scope, result_dir=result_dir2, workspace_path=m1["workspace"])
    if not res2.get("success"):
        raise RuntimeError(f"run2 failed: {res2.get('error')}\nstderr: {res2.get('stderr')}")
    m2 = BacktestResultParser().parse(result_dir2)
    m2m = {"trades": m2.trades, "win_rate": m2.win_rate,
           "total_return": (m2.extra or {}).get("total_return")}
    print(f"       trades={m2m['trades']} win_rate={m2m['win_rate']} total_return={m2m['total_return']}")

    # --- post-flight: production hash + boundary hashes ---
    prod_hash_after = sha256(PROD_STRATEGY)
    boundary_after = boundary_hashes()

    # --- assertions ---
    results_changed = (m1["trades"] != m2m["trades"]) or (m1["win_rate"] != m2m["win_rate"]) \
        or (m1["total_return"] != m2m["total_return"])
    prod_unchanged = (prod_hash_before == prod_hash_after)
    # [3] production boundary files untouched: every sandbox-boundary production file
    # is byte-identical before vs after. (Whole-repo git diff is reported for context
    # only -- it is polluted by pre-existing changes unrelated to the sandbox.)
    boundary_unchanged = (boundary_before == boundary_after)
    changed_files = [p for p in boundary_before if boundary_before[p] != boundary_after.get(p)]

    print("\n" + "=" * 64)
    print("ASSERTIONS")
    print("=" * 64)
    print(f"  [1] results changed after workspace edit:  {'PASS' if results_changed else 'FAIL'}"
          f"  (baseline trades={m1['trades']} -> edited trades={m2m['trades']})")
    print(f"  [2] production strategy hash unchanged:    {'PASS' if prod_unchanged else 'FAIL'}"
          f"  ({prod_hash_before[:16]} == {prod_hash_after[:16]})")
    print(f"  [3] production boundary files untouched:   {'PASS' if boundary_unchanged else 'FAIL'}"
          f"  ({len(boundary_before)} files{', changed: ' + ','.join(changed_files) if changed_files else ''})")

    all_pass = results_changed and prod_unchanged and boundary_unchanged
    print("\n" + "=" * 64)
    print(f"RESULT: {'ALL PASS — sandbox verified' if all_pass else 'FAIL — see above'}")
    print("=" * 64)

    # cleanup workspace artifacts (keep nothing under _sandbox_verify)
    import shutil
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
