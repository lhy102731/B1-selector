"""verify_research_director.py — acceptance test for the Research Director.

Feeds the 5-generation height_ratio evolution dataset and verifies:
  [1] findings non-empty
  [2] correctly identifies height_ratio as exhausted
  [3] correctly identifies declining trend
  [4] hypotheses >= 3
  [5] ranked_proposals >= 3
  [6] NO exhausted parameter in ranked proposals

Pure analysis — no backtests or code changes.
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from research_automation.research_director import ResearchDirector

# Same dataset as Phase 3A / 3A.5
EXPERIMENTS = [
    {"experiment_id": "evo-root-001-gen00", "parent_experiment_id": "evo-root-001",
     "hypothesis": "height_ratio=0.67", "generation": 0, "param_value": 0.67,
     "total_return": 0.56, "trades": 131, "win_rate": 0.382,
     "code_change": {"change_type": "modify_constant", "symbol": "height_ratio", "value": 0.67},
     "metrics": {"trades": 131, "win_rate": 0.382, "total_return": 0.56}},
    {"experiment_id": "evo-root-001-gen01", "parent_experiment_id": "evo-root-001-gen00",
     "hypothesis": "height_ratio=0.80", "generation": 1, "param_value": 0.80,
     "total_return": 0.34, "trades": 131, "win_rate": 0.366,
     "code_change": {"change_type": "modify_constant", "symbol": "height_ratio", "value": 0.80},
     "metrics": {"trades": 131, "win_rate": 0.366, "total_return": 0.34}},
    {"experiment_id": "evo-root-001-gen02", "parent_experiment_id": "evo-root-001-gen01",
     "hypothesis": "height_ratio=1.00", "generation": 2, "param_value": 1.00,
     "total_return": 0.25, "trades": 131, "win_rate": 0.389,
     "code_change": {"change_type": "modify_constant", "symbol": "height_ratio", "value": 1.00},
     "metrics": {"trades": 131, "win_rate": 0.389, "total_return": 0.25}},
    {"experiment_id": "evo-root-001-gen03", "parent_experiment_id": "evo-root-001-gen02",
     "hypothesis": "height_ratio=1.20", "generation": 3, "param_value": 1.20,
     "total_return": 0.05, "trades": 131, "win_rate": 0.351,
     "code_change": {"change_type": "modify_constant", "symbol": "height_ratio", "value": 1.20},
     "metrics": {"trades": 131, "win_rate": 0.351, "total_return": 0.05}},
    {"experiment_id": "evo-root-001-gen04", "parent_experiment_id": "evo-root-001-gen03",
     "hypothesis": "height_ratio=1.50", "generation": 4, "param_value": 1.50,
     "total_return": 0.05, "trades": 131, "win_rate": 0.359,
     "code_change": {"change_type": "modify_constant", "symbol": "height_ratio", "value": 1.50},
     "metrics": {"trades": 131, "win_rate": 0.359, "total_return": 0.05}},
]


def main() -> int:
    print("=" * 64)
    print("Research Director acceptance test")
    print("=" * 64)

    director = ResearchDirector()
    analysis = director.analyze(EXPERIMENTS)

    # Print findings
    print(f"\nFindings ({len(analysis.findings)}):")
    for f in analysis.findings:
        print(f"  {f.id}: {f.statement[:90]}... (confidence={f.confidence:.2f})")

    print(f"\nDimensions:")
    print(f"  exhausted: {[(d.symbol, d.trend) for d in analysis.exhausted_dimensions]}")
    print(f"  promising: {[(d.symbol, d.trend) for d in analysis.promising_dimensions]}")
    print(f"  unstable:  {[(d.symbol, d.trend) for d in analysis.unstable_dimensions]}")

    print(f"\nHypotheses ({len(analysis.hypotheses)}):")
    for h in analysis.hypotheses:
        print(f"  {h.id}: {h.statement[:80]}...")

    print(f"\nRanked Proposals ({len(analysis.ranked_proposals)}):")
    for rp in analysis.ranked_proposals:
        print(f"  {rp.proposal_id}: score={rp.score:.2f} priority={rp.priority:.2f} "
              f"symbol={rp.code_change.get('symbol','?')}")

    # --- assertions ---
    print("\n" + "=" * 64)
    print("ASSERTIONS")
    print("=" * 64)
    ok = 0
    total = 0

    # [1] findings non-empty
    total += 1
    a1 = len(analysis.findings) >= 1
    ok += int(a1)
    print(f"  [1] findings non-empty:                    {'PASS' if a1 else 'FAIL'}"
          f"  (count={len(analysis.findings)})")

    # [2] correctly identifies exhausted
    total += 1
    exh_symbols = {d.symbol for d in analysis.exhausted_dimensions}
    a2 = "height_ratio" in exh_symbols
    ok += int(a2)
    print(f"  [2] exhausted dimension: height_ratio:    {'PASS' if a2 else 'FAIL'}"
          f"  (exhausted={exh_symbols})")

    # [3] correctly identifies declining trend
    total += 1
    hr_dim = next((d for d in analysis.exhausted_dimensions
                   if d.symbol == "height_ratio"), None)
    a3 = hr_dim is not None and hr_dim.trend == "declining"
    ok += int(a3)
    print(f"  [3] declining trend detected:             {'PASS' if a3 else 'FAIL'}"
          f"  (trend={hr_dim.trend if hr_dim else 'N/A'})")

    # [4] hypotheses >= 3
    total += 1
    a4 = len(analysis.hypotheses) >= 3
    ok += int(a4)
    print(f"  [4] hypotheses >= 3:                      {'PASS' if a4 else 'FAIL'}"
          f"  (count={len(analysis.hypotheses)})")

    # [5] ranked_proposals >= 3
    total += 1
    a5 = len(analysis.ranked_proposals) >= 3
    ok += int(a5)
    print(f"  [5] ranked_proposals >= 3:                {'PASS' if a5 else 'FAIL'}"
          f"  (count={len(analysis.ranked_proposals)})")

    # [6] exhausted parameter not in ranked proposals
    total += 1
    rp_symbols = {rp.code_change.get("symbol") for rp in analysis.ranked_proposals}
    a6 = "height_ratio" not in rp_symbols
    ok += int(a6)
    print(f"  [6] no exhausted param in ranked proposals: {'PASS' if a6 else 'FAIL'}"
          f"  (proposed={rp_symbols})")

    all_pass = (ok == total)
    print("\n" + "=" * 64)
    print(f"RESULT: {ok}/{total} PASS" + (" — ALL PASS" if all_pass else " — FAIL"))
    print("=" * 64)
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
