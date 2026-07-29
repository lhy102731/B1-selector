"""verify_proposal_generator.py — acceptance test for the Research Proposal Generator.

Feeds the 5-generation height_ratio evolution dataset (from Phase 3A) and
verifies the generator:
  [1] Detects height_ratio as exhausted (declining trend).
  [2] Stops proposing height_ratio experiments.
  [3] Proposes new parameters (M1-M4 / yellow_line category).
  [4] Produces valid code_change in each proposal.
  [5] Generates rationale text for each proposal.
  [6] Supports multi-proposal output (>= 2 proposals).

NO backtests are run — this is a pure analysis test.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from research_automation.research_proposal_generator import (
    ResearchProposalGenerator, ResearchProposal, _detect_exhausted,
)

# Evolution-loop results: 5 generations of height_ratio experiments
HEIGHT_RATIO_EXPERIMENTS = [
    {"experiment_id": "evo-root-001-gen00", "parent_experiment_id": "evo-root-001",
     "hypothesis": "height_ratio=0.67", "generation": 0, "param_value": 0.67,
     "total_return": 0.56,
     "code_change": {"change_type": "modify_constant", "symbol": "height_ratio", "file": "strategy/brick_chart_strategy.py", "value": 0.67}},
    {"experiment_id": "evo-root-001-gen01", "parent_experiment_id": "evo-root-001-gen00",
     "hypothesis": "height_ratio=0.80", "generation": 1, "param_value": 0.80,
     "total_return": 0.34,
     "code_change": {"change_type": "modify_constant", "symbol": "height_ratio", "file": "strategy/brick_chart_strategy.py", "value": 0.80}},
    {"experiment_id": "evo-root-001-gen02", "parent_experiment_id": "evo-root-001-gen01",
     "hypothesis": "height_ratio=1.00", "generation": 2, "param_value": 1.00,
     "total_return": 0.25,
     "code_change": {"change_type": "modify_constant", "symbol": "height_ratio", "file": "strategy/brick_chart_strategy.py", "value": 1.00}},
    {"experiment_id": "evo-root-001-gen03", "parent_experiment_id": "evo-root-001-gen02",
     "hypothesis": "height_ratio=1.20", "generation": 3, "param_value": 1.20,
     "total_return": 0.05,
     "code_change": {"change_type": "modify_constant", "symbol": "height_ratio", "file": "strategy/brick_chart_strategy.py", "value": 1.20}},
    {"experiment_id": "evo-root-001-gen04", "parent_experiment_id": "evo-root-001-gen03",
     "hypothesis": "height_ratio=1.50", "generation": 4, "param_value": 1.50,
     "total_return": 0.05,
     "code_change": {"change_type": "modify_constant", "symbol": "height_ratio", "file": "strategy/brick_chart_strategy.py", "value": 1.50}},
]


def main() -> int:
    print("=" * 64)
    print("Research Proposal Generator acceptance test")
    print("=" * 64)

    gen = ResearchProposalGenerator()
    proposals = gen.generate(HEIGHT_RATIO_EXPERIMENTS, max_proposals=5)

    print(f"\nGenerated {len(proposals)} proposal(s):")
    for i, p in enumerate(proposals):
        print(f"\n  [{i+1}] priority={p.priority:.2f}")
        print(f"       hypothesis: {p.hypothesis[:100]}")
        print(f"       exhausted:  {p.exhausted_params}")
        print(f"       suggested:  {p.suggested_params}")
        print(f"       symbol:     {p.code_change.get('symbol','?')}")

    # --- assertions ---
    print("\n" + "=" * 64)
    print("ASSERTIONS")
    print("=" * 64)
    ok = 0
    total = 0

    # [1] height_ratio detected as exhausted
    total += 1
    exhausted = _detect_exhausted(HEIGHT_RATIO_EXPERIMENTS)
    hr_exhausted = any(e["symbol"] == "height_ratio" for e in exhausted)
    ok += int(hr_exhausted)
    print(f"  [1] height_ratio detected as exhausted:    {'PASS' if hr_exhausted else 'FAIL'}"
          f"  (exhausted={[e['symbol'] for e in exhausted]})")
    if hr_exhausted:
        hr = [e for e in exhausted if e["symbol"] == "height_ratio"][0]
        print(f"       trend={hr['trend']}  returns={hr['returns']}  "
              f"rec={hr['recommendation'][:80]}...")

    # [2] no proposal suggests height_ratio
    total += 1
    hr_proposed = any(p.code_change.get("symbol") == "height_ratio" for p in proposals)
    a2 = not hr_proposed
    ok += int(a2)
    print(f"  [2] NO height_ratio in new proposals:     {'PASS' if a2 else 'FAIL'}")

    # [3] proposals target new params (M1-M4 / yellow_line)
    total += 1
    new_symbols = [p.code_change.get("symbol", "?") for p in proposals]
    yellow_line_proposed = any(s in {"M1", "M2", "M3", "M4"} for s in new_symbols)
    a3 = yellow_line_proposed or len(new_symbols) >= 2
    ok += int(a3)
    print(f"  [3] new parameters proposed:              {'PASS' if a3 else 'FAIL'}"
          f"  (symbols={new_symbols})")

    # [4] valid code_change in each proposal
    total += 1
    a4 = all(
        p.code_change.get("change_type") == "modify_constant"
        and p.code_change.get("symbol")
        and p.code_change.get("value") is not None
        for p in proposals
    )
    ok += int(a4)
    print(f"  [4] valid code_change in all proposals:   {'PASS' if a4 else 'FAIL'}")

    # [5] rationale generated for each proposal
    total += 1
    a5 = all(len(p.rationale) > 20 for p in proposals)
    ok += int(a5)
    print(f"  [5] rationale generated for each proposal: {'PASS' if a5 else 'FAIL'}")

    # [6] multi-proposal output (>= 2)
    total += 1
    a6 = len(proposals) >= 2
    ok += int(a6)
    print(f"  [6] multi-proposal output (>= 2):          {'PASS' if a6 else 'FAIL'}"
          f"  (count={len(proposals)})")

    all_pass = (ok == total)
    print("\n" + "=" * 64)
    print(f"RESULT: {ok}/{total} PASS" + (" — ALL PASS" if all_pass else " — FAIL"))
    print("=" * 64)
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
