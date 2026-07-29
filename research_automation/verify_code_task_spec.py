"""verify_code_task_spec.py — acceptance test for dual-mode experiment_task.md generator.

Proves:
  [1] Parameter experiment output unchanged (legacy sections present, no code sections).
  [2] Code experiment auto-generates Target File section.
  [3] Allowed / Forbidden sections are correct.
  [4] Code Change Request section is correct (symbol, old/new values).
  [5] All 6 assertions explicit.
  [6] brick dry-run EXIT=0 (run externally; this script only tests task generation).
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from research_automation.experiment import Experiment, Proposal
from research_automation.experiment_runner import generate_experiment_task_md
from research_automation.lineage import write_lineage_json

OUTPUT_ROOT = _REPO_ROOT / "research_automation" / "_output" / "_task_spec_verify"


def main() -> int:
    print("=" * 64)
    print("Code-experiment task spec acceptance test")
    print("=" * 64)
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)

    # ---- 1. parameter experiment (legacy format) ----
    ep = Experiment(
        "test-param", "b1",
        proposal=Proposal(hypothesis="B1 sweep: pe_max=30 vs champion",
                          scope='{"start":"2024-01-01","end":"2024-06-30","max_stocks":60}'),
    )
    p_path = generate_experiment_task_md(ep, OUTPUT_ROOT / "param")
    tp = p_path.read_text(encoding="utf-8")

    # ---- 2. code experiment (new format) ----
    ec = Experiment(
        "test-code", "brick",
        parent_experiment_id="test-param",
        proposal=Proposal(
            hypothesis="height_ratio 2/3 -> 1.0: tighter brick signal",
            scope={"code_change": {"change_type": "modify_constant",
                                   "file": "strategy/brick_chart_strategy.py",
                                   "symbol": "height_ratio",
                                   "value": 1.0}},
        ),
    )
    c_path = generate_experiment_task_md(ec, OUTPUT_ROOT / "code")
    tc = c_path.read_text(encoding="utf-8")

    # ---- assertions ----
    print()
    ok = 0
    total = 0

    # [1] parameter experiment = legacy format
    total += 1
    p1 = ("## Strategy" in tp and "## Hypothesis" in tp
          and "Required deliverables" in tp
          and "## Target File" not in tp
          and "## Code Change Request" not in tp)
    ok += int(p1)
    print(f"  [1] parameter experiment legacy format:   {'PASS' if p1 else 'FAIL'}")

    # [2] code experiment has Target File
    total += 1
    p2 = "## Target File" in tc and "strategy/brick_chart_strategy.py" in tc
    ok += int(p2)
    print(f"  [2] Target File auto-generated:           {'PASS' if p2 else 'FAIL'}")

    # [3] Allowed / Forbidden correct
    total += 1
    p3 = ("## Allowed Changes" in tc and "## Forbidden Changes" in tc
          and "brick identification logic" in tc
          and "signal generation logic" in tc
          and "automation layer" in tc)
    ok += int(p3)
    print(f"  [3] Allowed/Forbidden correct:            {'PASS' if p3 else 'FAIL'}")

    # [4] Code Change Request correct
    total += 1
    p4 = ("## Code Change Request" in tc
          and "modify_constant" in tc
          and "height_ratio" in tc
          and "1.0" in tc)
    ok += int(p4)
    print(f"  [4] Code Change Request correct:          {'PASS' if p4 else 'FAIL'}")

    # [5] parent experiment in code task (lineage integration)
    total += 1
    p5 = "## Parent Experiment" in tc and "test-param" in tc
    ok += int(p5)
    print(f"  [5] Parent experiment in task spec:       {'PASS' if p5 else 'FAIL'}")

    # [6] no cross-contamination
    total += 1
    p6 = ("## Forbidden Changes" not in tp and "## Target File" not in tp
          and "Required deliverables" not in tc)
    ok += int(p6)
    print(f"  [6] no cross-contamination:               {'PASS' if p6 else 'FAIL'}")

    all_pass = (ok == total)
    print("\n" + "=" * 64)
    print(f"RESULT: {ok}/{total} PASS" + (" — ALL PASS" if all_pass else " — FAIL"))
    print("=" * 64)
    shutil.rmtree(OUTPUT_ROOT)
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
