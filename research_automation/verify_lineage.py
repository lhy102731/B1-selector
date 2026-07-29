"""verify_lineage.py — acceptance test for the Experiment Lineage System.

Constructs a 3-generation chain:

    exp_001 (root, generation=0, parent=None)
        ↓
    exp_002 (generation=1, parent=exp_001)
        ↓
    exp_003 (generation=2, parent=exp_002)

Verifies:
  [1] parent_experiment_id is correct
  [2] generation auto-computed correctly
  [3] lineage.json generated per experiment
  [4] lineage_tree.json generated with correct edges
  [5] experiment report includes Lineage section
  [6] get_experiment_lineage() returns full root→leaf chain

All artifacts land under _output/_lineage_verify/ (safe, gitignored).
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from research_automation.lineage import (
    write_lineage_json, build_lineage_tree, get_experiment_lineage,
)
from research_automation.experiment import Experiment, Proposal
from research_automation.report_generator import ReportGenerator
from research_automation.safety import assert_safe_path

OUTPUT_ROOT = _REPO_ROOT / "research_automation" / "_output" / "_lineage_verify"


def make_exp(eid, parent, hypothesis=""):
    return Experiment(
        experiment_id=eid,
        strategy="brick",
        parent_experiment_id=parent,
        proposal=Proposal(hypothesis=hypothesis or f"test {eid}"),
    )


def main() -> int:
    print("=" * 64)
    print("Lineage acceptance test")
    print("=" * 64)
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)

    # ---- build chain ----
    e1 = make_exp("exp_001", None, "root experiment")
    e2 = make_exp("exp_002", "exp_001", "child of exp_001")
    e3 = make_exp("exp_003", "exp_002", "child of exp_002")
    all_exps = [e1, e2, e3]
    pool = all_exps  # candidate_pool for lookups

    # ---- write per-experiment lineage.json ----
    for e in all_exps:
        out = OUTPUT_ROOT / e.experiment_id
        write_lineage_json(e, out, candidate_pool=pool)

    # ---- build lineage tree ----
    tree_path = OUTPUT_ROOT / "lineage_tree.json"
    # convert to dict-like for build_lineage_tree (it accepts both dataclass + dict)
    build_lineage_tree([{"experiment_id": e.experiment_id, "parent_experiment_id": e.parent_experiment_id}
                        for e in all_exps], tree_path)

    # ---- generate reports (with lineage block) ----
    rg = ReportGenerator()
    for e in all_exps:
        out = OUTPUT_ROOT / e.experiment_id
        rg.generate(e, out)

    # ---- assertions ----
    print()
    ok = 0
    total = 0

    # [1] parent correct
    total += 1
    p1 = (e2.parent_experiment_id == "exp_001" and e3.parent_experiment_id == "exp_002"
          and e1.parent_experiment_id is None)
    ok += int(p1)
    print(f"  [1] parent_experiment_id correct:          {'PASS' if p1 else 'FAIL'}")

    # [2] generation auto-computed (from lineage.json)
    total += 1
    gens = {}
    for e in all_exps:
        p = OUTPUT_ROOT / e.experiment_id / "lineage.json"
        gens[e.experiment_id] = json.loads(p.read_text())["generation"]
    p2 = (gens["exp_001"] == 0 and gens["exp_002"] == 1 and gens["exp_003"] == 2)
    ok += int(p2)
    print(f"  [2] generation auto-computed:              {'PASS' if p2 else 'FAIL'}"
          f"  (gen={gens})")

    # [3] lineage.json generated
    total += 1
    p3 = all((OUTPUT_ROOT / e.experiment_id / "lineage.json").exists() for e in all_exps)
    ok += int(p3)
    print(f"  [3] lineage.json generated per experiment:  {'PASS' if p3 else 'FAIL'}")

    # [4] lineage_tree.json
    total += 1
    tree = json.loads(tree_path.read_text()) if tree_path.exists() else {}
    p4 = (tree.get("root") == "exp_001" and len(tree.get("nodes", [])) == 3
          and any(n["parent"] is None for n in tree["nodes"])
          and any(n["id"] == "exp_003" and n["parent"] == "exp_002" for n in tree["nodes"]))
    ok += int(p4)
    print(f"  [4] lineage_tree.json generated:            {'PASS' if p4 else 'FAIL'}"
          f"  (root={tree.get('root')}, nodes={len(tree.get('nodes',[]))})")

    # [5] report includes Lineage
    total += 1
    r3 = (OUTPUT_ROOT / "exp_003" / "report.md").read_text(encoding="utf-8")
    p5 = "## Lineage" in r3 and "Parent: exp_002" in r3 and "Generation: 2" in r3
    ok += int(p5)
    print(f"  [5] report includes Lineage section:        {'PASS' if p5 else 'FAIL'}")

    # [6] get_experiment_lineage returns full chain
    total += 1
    chain = get_experiment_lineage("exp_003", pool)
    p6 = (chain == ["exp_001", "exp_002", "exp_003"])
    ok += int(p6)
    print(f"  [6] get_experiment_lineage full chain:      {'PASS' if p6 else 'FAIL'}"
          f"  (chain={chain})")

    # ---- cleanup ----
    all_pass = (ok == total)
    print("\n" + "=" * 64)
    print(f"RESULT: {ok}/{total} PASS" + (" — ALL PASS" if all_pass else " — FAIL"))
    print("=" * 64)
    shutil.rmtree(OUTPUT_ROOT)
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
