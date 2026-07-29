# B1 V3 — Agent Briefing (injected into research_context)

## What B1 V3 is

A constraint-geometry mean-reversion strategy on A-share daily bars.
Closed at Phase 15 (2026-06-24). Treat as a completed research subject.

## Confirmed alpha components

- **Entry generator (1):** `C_wave_qualified` — necessary, not sufficient.
- **Exit alpha (1):** `t_plus_3_min_return` — the only A-class parameter
  that survived sensitivity + robustness + independence audits.

## Structural facts

- Edge lives in the **intersection of ~9-13 active filters**, not in any
  individual rule. Single-component removal under-states impact by ~10x.
- `A_J_range` is the **structural HUB** (Top-3 strongest pair interactions
  -1.02 / -0.94 / -0.78; appears in 7 of 8 strongest triples). Its
  single-removal looks anti-alpha (ΔPF +0.93) but this is a hub artefact.
- Triple interaction `F + A + B_KltD` reaches int_PF -1.58, exceeding any
  pair interaction → effective dimension of constraint geometry ≥ 3.
- `B_KltD` alone is single-removal dead (Jaccard 1.000) but pairs with
  A_J_range at int_PF -0.94 — conditional activation.

## Frozen baseline (must beat this on the acceptance bar)

| Window | Trades | Return | MaxDD | Sharpe | PF |
|---|---:|---:|---:|---:|---:|
| A_2023 | 94 | +12.93% | -3.14% | 1.57 | 2.66 |
| B_2024H1 | 36 | -1.88% | -4.66% | -0.58 | 0.64 |
| C_2024H2_latest | 144 | +7.13% | -10.86% | 0.48 | 1.35 |

## Acceptance bar (ALL windows must pass)

- PF retention ≥ 80%
- Sharpe retention ≥ 80%
- Trade-level Jaccard vs baseline ≥ 0.50

Return retention alone is NOT acceptable evidence.

## Hard rules (machine-enforced)

1. Do not delete or relax `j_max` / `j_min` / `prefilter_j_max`.
2. Do not modify `t_plus_3_min_return` without re-running Phase 10/11.
3. Do not modify exit logic (yellow/white break, profit_25pct,
   stop_loss, max_hold) without explicit human authorization.
4. Do not modify `require_wave_qualified` or wave_* thresholds.
5. Single-component ablation is NOT acceptable as redundancy evidence.
   Must include pair-removal test.
6. Re-experimenting on Phase 15 §3 falsified hypotheses is forbidden
   unless the proposal contains a phase-specific argument why the prior
   finding does not apply.

## Already disproven (do not re-propose)

- "Verified alpha components alone explain B1 V3" (Phase 12B)
- "Alpha is factorable into a small constraint subset" (Phase 12C)
- "Single ablation correctly identifies redundant components" (Phase 14B)
- "There is a 2nd independent entry generator inside the current feature
  universe" (Phase 13)
- "`C_no_wave_break` is helpful" (Phase 12A)
- "Sensitivity = robustness" (Phase 10)
- "Robustness = independence" (Phase 11)

## What is still uncertain (legitimate research targets)

- Whether alpha exists in the 6 unexplored feature categories:
  Market Regime, Breadth, Multi-Timeframe, Volatility, Relative
  Strength CS, Sector Rotation. **Extending the feature universe is
  the only sanctioned direction for B1 V3 follow-up.**
- Whether the constraint geometry generalises beyond 2026-06-22
  (out-of-sample).
- Whether unused parquet columns (`ps`, `pcf`, `main_net_flow_*`)
  carry alpha.

## Required reading before modification

1. `verdict_full.md`
2. `architecture_full.md`
3. `constraint_geometry.md`
4. `dependency_graph.md`
5. `interaction_summary.json`

If you propose a modification, your justification must cite at least one
phase + one metric value contradicting prior evidence.
