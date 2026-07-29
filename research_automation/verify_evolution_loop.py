"""verify_evolution_loop.py — acceptance test for the Evolution Loop.

Runs 5 generations of deterministic height_ratio evolution (0.67, 0.80, 1.00,
1.20, 1.50) against the brick V2 backtest (2024-01-01 to 2024-03-31).

Proves:
  [1] generation auto-increments (generation field correct).
  [2] parent chain correct (each child → previous parent).
  [3] each generation executed successfully (status=COMPLETED).
  [4] champion correctly selected (max total_return).
  [5] lineage_tree.json generated.
  [6] evolution_summary.md generated.

All artifacts under _output/_evolution/ (safe, gitignored).  Production untouched.
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from research_automation.evolution_loop import EvolutionLoop, EvolutionResult
from research_automation.experiment import Experiment, Proposal
from research_automation.lineage import get_experiment_lineage
from research_automation.safety import output_root

EVO_ROOT = output_root() / "_evolution"


def main() -> int:
    print("=" * 64)
    print("Evolution Loop acceptance test")
    print("=" * 64)
    # clean previous
    if EVO_ROOT.exists():
        shutil.rmtree(EVO_ROOT)

    root = Experiment(
        experiment_id="evo-root-001",
        strategy="brick",
        proposal=Proposal(hypothesis="root: height_ratio baseline 2/3"),
        start_time="2026-06-21T00:00:00Z",
    )

    loop = EvolutionLoop(backtest_window={"start": "2024-01-01", "end": "2024-03-31",
                                          "strategy": "BRICK",
                                          "params": {"entry_ma_source": "t0"}})
    result: EvolutionResult = loop.run_generation(root, max_generations=5)

    gens = result.generations
    print(f"\n{'='*60}")
    print("ASSERTIONS")
    print("=" * 60)
    ok = 0
    total = 0

    # [1] generation auto-increments
    total += 1
    a1 = all(g["generation"] == i for i, g in enumerate(gens))
    ok += int(a1)
    print(f"  [1] generation auto-increments:           {'PASS' if a1 else 'FAIL'}"
          f"  (gens={[g['generation'] for g in gens]})")

    # [2] parent chain correct
    total += 1
    parent_chain = [g["parent_id"] for g in gens]
    expected_parents = ["evo-root-001", "evo-root-001-gen00", "evo-root-001-gen01",
                        "evo-root-001-gen02", "evo-root-001-gen03"]
    a2 = all(pc == ep or (pc is None and ep is None) for pc, ep in zip(parent_chain, expected_parents))
    ok += int(a2)
    print(f"  [2] parent chain correct:                 {'PASS' if a2 else 'FAIL'}"
          f"  (chain={parent_chain})")

    # [3] each gen executed successfully
    total += 1
    a3 = all(g["status"] == "COMPLETED" for g in gens)
    ok += int(a3)
    failed = [g["generation"] for g in gens if g["status"] != "COMPLETED"]
    print(f"  [3] all generations COMPLETED:            {'PASS' if a3 else 'FAIL'}"
          f"  failed={failed}")

    # [4] champion correctly selected (max total_return)
    total += 1
    scores = [g["score"] for g in gens]
    best = max(scores)
    best_id = [g["experiment_id"] for g in gens if g["score"] == best][0]
    a4 = (result.best_experiment_id == best_id)
    ok += int(a4)
    print(f"  [4] champion selected (max return):       {'PASS' if a4 else 'FAIL'}"
          f"  (best={best_id} score={best:.2f} = max({[f'{s:.2f}' for s in scores]}))")

    # [5] lineage_tree.json
    total += 1
    tree_path = EVO_ROOT / "lineage_tree.json"
    a5 = tree_path.exists() and len(json.loads(tree_path.read_text())["nodes"]) >= 6
    ok += int(a5)
    print(f"  [5] lineage_tree.json generated:          {'PASS' if a5 else 'FAIL'}")

    # [6] evolution_summary.md
    total += 1
    summary_path = EVO_ROOT / "evolution_summary.md"
    a6 = summary_path.exists() and "Champion" in summary_path.read_text(encoding="utf-8")
    ok += int(a6)
    print(f"  [6] evolution_summary.md generated:       {'PASS' if a6 else 'FAIL'}")

    all_pass = (ok == total)
    print("\n" + "=" * 64)
    print(f"RESULT: {ok}/{total} PASS" + (" — ALL PASS" if all_pass else " — FAIL"))
    print("=" * 64)
    shutil.rmtree(EVO_ROOT)
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
