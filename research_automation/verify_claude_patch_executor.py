"""verify_claude_patch_executor.py — acceptance test for Claude Patch Executor.

Tests PatchValidator, ApplyPatch, and ClaudePatchExecutor with a real Claude CLI call.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from research_automation.patch_executor import (
    PatchValidator, _apply_patch_to_workspace, _parse_diff_files, _count_diff_hunks,
    compile_gate, ClaudePatchExecutor,
)
from research_automation.experiment import Experiment, Proposal
from research_automation.workspace_manager import WorkspaceManager
from research_automation.experiment_runner import _BRICK_SPEC
from research_automation.safety import assert_safe_path

OUTPUT_ROOT = _REPO_ROOT / "research_automation" / "_output" / "_patch_verify"


def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main() -> int:
    print("=" * 64)
    print("Claude Patch Executor acceptance test")
    print("=" * 64)
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)

    ok = 0
    total = 0

    # ================================================================
    # UNIT: PatchValidator
    # ================================================================
    print("\n--- PatchValidator ---")
    v = PatchValidator()

    # valid diff
    valid_diff = """--- a/strategy/brick_chart_strategy.py
+++ b/strategy/brick_chart_strategy.py
@@ -33,7 +33,7 @@
-            'height_ratio': 2.0 / 3.0,
+            'height_ratio': 1.0,
"""
    r = v.validate(valid_diff)
    total += 1
    a1 = r["ok"] and r["files"] == ["strategy/brick_chart_strategy.py"]
    ok += int(a1)
    print(f"  [1] valid patch passes:                   {'PASS' if a1 else 'FAIL'}  {r['reason']}")

    # forbidden file
    r2 = v.validate("""--- a/utils/technical.py
+++ b/utils/technical.py
@@ -1,1 +1,1 @@
-old
+new
""")
    total += 1
    a2 = not r2["ok"] and "forbidden" in r2["reason"]
    ok += int(a2)
    print(f"  [2] forbidden file rejected:              {'PASS' if a2 else 'FAIL'}  {r2['reason']}")

    # empty diff
    total += 1
    a3 = not v.validate("")["ok"]
    ok += int(a3)
    print(f"  [3] empty diff rejected:                  {'PASS' if a3 else 'FAIL'}")

    # ================================================================
    # UNIT: ApplyPatch to workspace
    # ================================================================
    print("\n--- ApplyPatch ---")
    ws_test = OUTPUT_ROOT / "unit_ws"
    ws_test.mkdir(parents=True, exist_ok=True)
    strat_dir = ws_test / "strategy"
    strat_dir.mkdir(exist_ok=True)
    test_file = strat_dir / "brick_chart_strategy.py"
    test_file.write_text("line1\nline2\nold_line\nline4\n", encoding="utf-8")

    diff = """--- a/strategy/brick_chart_strategy.py
+++ b/strategy/brick_chart_strategy.py
@@ -2,2 +2,2 @@
 line2
-old_line
+new_line
"""
    ap_result = _apply_patch_to_workspace(diff, ws_test)
    total += 1
    patched = test_file.read_text(encoding="utf-8")
    a4 = ap_result["ok"] and "new_line" in patched and "old_line" not in patched
    ok += int(a4)
    print(f"  [4] patch applies correctly to workspace:  {'PASS' if a4 else 'FAIL'}")

    # ================================================================
    # INTEGRATION: Claude CLI real call
    # ================================================================
    print("\n--- ClaudePatchExecutor (real Claude CLI) ---")
    wm = WorkspaceManager(workspace_root=OUTPUT_ROOT, project_root=_REPO_ROOT)
    exp = Experiment(
        "claude-patch-test", "brick",
        proposal=Proposal(
            hypothesis="Test: height_ratio 2/3 -> 1.0 for tighter brick signal",
            scope={"code_change": {"change_type": "modify_constant",
                                   "file": "strategy/brick_chart_strategy.py",
                                   "symbol": "height_ratio",
                                   "value": 1.0}},
        ),
        start_time="2026-06-21T00:00:00Z",
    )
    ws = wm.create_workspace(exp, _BRICK_SPEC)
    prod_hash_before = sha256(_REPO_ROOT / "strategy" / "brick_chart_strategy.py")

    # Generate task.md
    from research_automation.experiment_runner import generate_experiment_task_md
    task_path = generate_experiment_task_md(exp, OUTPUT_ROOT / "claude-patch-test")

    # Call Claude
    cpe = ClaudePatchExecutor(timeout=300)
    print("  calling Claude CLI (may take ~30s)...")
    result = cpe.apply(task_path, ws, experiment=exp)

    print(f"  ok={result.ok}  changed={result.changed_files}  error={result.error}")
    for log in result.logs:
        print(f"  log: {log[:120]}")

    total += 1
    a5 = result.ok
    ok += int(a5)
    print(f"  [5] Claude CLI called + patch applied:    {'PASS' if a5 else 'FAIL'}")

    # patch.diff exists
    patch_path = ws / "patch.diff"
    total += 1
    a6 = patch_path.exists() and patch_path.stat().st_size > 0
    ok += int(a6)
    print(f"  [6] patch.diff generated:                 {'PASS' if a6 else 'FAIL'}")

    # compile gate — cwd=workspace so cross-module imports resolve
    total += 1
    cg_ok = True
    cg_out = ""
    for sub in ["strategy", "utils"]:
        t = ws / sub
        if not t.exists():
            continue
        p = subprocess.run(
            [sys.executable, "-m", "compileall", "-q", str(t)],
            capture_output=True, text=True, timeout=30, cwd=str(ws),
        )
        if p.returncode != 0:
            cg_ok = False
            # Capture full stderr for the error line(s)
            full_err = (p.stderr or p.stdout or "")
            # Extract just the SyntaxError line for concise reporting
            for line in full_err.splitlines():
                if "Error" in line or "Syntax" in line or "line" in line:
                    cg_out += line.strip()[:150] + " | "
    a7 = cg_ok
    ok += int(a7)
    print(f"  [7] compile gate PASS:                     {'PASS' if a7 else 'FAIL'}  {cg_out[:200]}")
    if not cg_ok:
        patch_path = ws / "patch.diff"
        if patch_path.exists():
            print(f"       patch.diff: {patch_path.read_text(encoding='utf-8')[:300]}")
        strat_file = ws / "strategy" / "brick_chart_strategy.py"
        if strat_file.exists():
            lines = strat_file.read_text(encoding='utf-8').splitlines()
            # print lines around __init__
            for i, line in enumerate(lines):
                if 'def __init__' in line or 'height_ratio' in line:
                    print(f"       line {i+1}: {line[:100]}")

    # production hash unchanged
    total += 1
    prod_hash_after = sha256(_REPO_ROOT / "strategy" / "brick_chart_strategy.py")
    a8 = prod_hash_before == prod_hash_after
    ok += int(a8)
    print(f"  [8] production hash unchanged:             {'PASS' if a8 else 'FAIL'}")

    # workspace file changed
    total += 1
    ws_strat = ws / "strategy" / "brick_chart_strategy.py"
    ws_changed = "height_ratio': 1.0" in ws_strat.read_text(encoding="utf-8") or \
                 "'height_ratio': 1.0" in ws_strat.read_text(encoding="utf-8")
    a9 = ws_changed
    ok += int(a9)
    print(f"  [9] workspace code changed on disk:       {'PASS' if a9 else 'FAIL'}")

    all_pass = (ok == total)
    print("\n" + "=" * 64)
    print(f"RESULT: {ok}/{total} PASS" + (" — ALL PASS" if all_pass else " — FAIL"))
    print("=" * 64)
    if all_pass:
        shutil.rmtree(OUTPUT_ROOT)
    else:
        print(f"(workspace preserved: {OUTPUT_ROOT})")
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
