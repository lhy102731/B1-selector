# B1 V3 - Final Research Verdict

Final closure document for B1 V3. Five questions. Five answers. No
recommendations. No optimization. Each answer cites the phases that
produced its evidence.

## 1. What is B1 V3?

B1 V3 is a **mean-reversion strategy on A-share daily bars** with the
following architecture:

- One entry generator: `C_wave_qualified` (wave structure with surge-day
  detection + qualification thresholds).
- One independent exit alpha: `t_plus_3_min_return` (exit at T+3 if return
  below threshold).
- Approximately 9-13 active concentrators (filters that intersect to
  produce a constraint polytope).
- A ranking layer (`quality_score`) that combines about 25-30 G0-G10
  factor scores with weights from `B1V3Params`.
- Position management: `max_positions=8`, `position_pct=0.20`, full set
  of structural exits (yellow break / white break after launched /
  profit 25% / max_hold 35 / stop_loss low - 0.05).

It is implemented in:
`strategy/b1_v3_strategy.py` + `strategy/b1_v3_config.py` + `run_b1_v3.py`
+ `data/indicators_cache/*.parquet`.

It is **not** a generator-driven system in the traditional sense. It is a
**constraint-geometry system**: the alpha is the surface of a joint
inequality polytope, not any individual rule.

(Evidence: Phase 12B, 12C, 14B.)

## 2. Where does its edge come from?

The edge comes from the **intersection of multiple filters**, plus the
T+3 exit rule:

- `C_wave_qualified` provides candidate volume (without it, no candidates).
- Top concentrators (`E_wave_healthy`, `A_J_range`, `F_white_gt_yellow`,
  `D_white_slope`, `B_KltD`, `B4_turnover_max`) narrow the candidate pool.
  Their **joint** action - not their individual action - produces the
  observable Sharpe 1.57 / PF 2.66 baseline in A_2023.
- `t_plus_3_min_return` adds an independent +1.47 to +5.60 pp of return
  per window through exit-side selection.

Key structural fact: `A_J_range` is the **constraint-geometry hub**.
Removing it alone looks anti-alpha (ΔPF +0.93) but its pair interactions
with F / B_KltD / E are -1.02 / -0.94 / -0.78. The triple `F + A + B_KltD`
has int_PF -1.58. The edge is therefore not in any single component.

(Evidence: Phase 9, 10, 11, 12A, 12B, 12C, 14B.)

## 3. What was disproven?

The following hypotheses were tested and **falsified**:

| Hypothesis | Falsified by |
|---|---|
| "B1 V3 has more than one entry generator" | Phase 13 (8 alternatives tested, 0 fully passed) |
| "Verified alpha components alone explain B1 V3" | Phase 12B (alpha-only collapsed to Sharpe 0.68 / PF 1.16, Jaccard 0.05) |
| "Single ablation correctly identifies redundant components" | Phase 14B (`B_KltD` looked redundant alone, was actually a conditional bridge with int_PF -0.94) |
| "B1 V3 alpha is factorable into a small constraint subset" | Phase 12C (7-group restoration plateaus at Jaccard 0.51) |
| "Sensitive parameters are robust" | Phase 10 (B5_ret_5d_min and others showed high sensitivity in one window but failed cross-window consistency) |
| "Robust parameters are independent" | Phase 11 (`j_max` and `pe_max` both Phase 10 R2-conditional but Phase 11 showed proxy redundancy int_pf -11.79 pp) |
| "Removing inactive filters has no effect" | Phase 14B (`A_J_range` 'anti-alpha' single-removal masks geometric hub role) |
| "`C_no_wave_break` is helpful" | Phase 12A (Class I anti-alpha; removal adds +2.13 pp avg return) |

(Evidence sources cited above.)

## 4. What remains uncertain?

Items that were not resolved by Phase 9-14B:

1. **Whether other generators exist outside the current feature universe**.
   Phase 14A documented 3 categories with zero features (Market Regime,
   Breadth, Multi-Timeframe) and 3 more with features-defined-but-consumers-
   off (Volatility, Relative Strength CS, Sector Rotation). Phase 13 only
   searched Price Structure + Momentum + Volume on a single daily
   timeframe. The honest answer is "no second generator was found in the
   searched universe", not "no second generator exists".

2. **Whether `A_J_range`'s anti-alpha single-removal is a hub artefact or a
   regime-fitting indicator**. The Phase 10 R2 classification suggests
   conditional alpha; Phase 14B suggests structural hub. Both can be true
   simultaneously - the single-removal signal could be regime fitting AND
   the joint geometry could be real. Not separable with current data.

3. **Whether the constraint geometry generalises beyond 2023-01-01 to
   2026-06-22**. All Phase 9-14B audits used three windows in this range
   on the same 60-code universe. Out-of-sample is unknown.

4. **Whether unused parquet features (`ps`, `pcf`, `main_net_flow_*`) carry
   alpha**. Phase 14A noted they exist but are unconsumed; no experiment
   tested them.

5. **Whether `t_plus_3_min_return=0.06` is universally optimal**. Phase 10
   ranked it best in 2 of 3 windows; the optimum may be regime-dependent.
   No optimization was performed.

6. **Whether the ranking layer's ~25 G0-G10 active factors interact with
   the constraint geometry**. Phase 9-14B held ranking weights at default;
   interactions between ranking and filtering were not measured.

## 5. What should future AG2 agents remember before touching this strategy?

Six rules, in order of priority:

### Rule 1 - This is a constraint-geometry system

Do not treat B1 V3 as a sum of independent components. Removing any single
filter looks safe in isolation but can collapse joint behaviour. Always
test interactions before deletion.

Cite: `constraint_geometry.md`, `architecture_full.md`,
`interaction_summary.json`.

### Rule 2 - A_J_range is the structural hub

Do not delete or substantially relax `j_max`, `j_min`, or `prefilter_j_max`
based on the +0.93 single-removal ΔPF signal. It is the hub of 3 strongest
pair interactions and 5 strongest triple interactions.

Cite: `../../../archive/research/phase14/phase14b_report.md`, `dependency_graph.md`.

### Rule 3 - The single confirmed independent alpha is t_plus_3_min_return

Do not modify exits or remove the T+3 rule without re-running Phase 10/11
audits. It is the only A-class parameter to survive all three of
sensitivity, robustness, and independence audits.

Cite: `phase10_robustness_results.json` (R1), Phase 11 final verdict,
`knowledge/b1_v3/exit_generators.json`.

### Rule 4 - Always measure with the four-metric stack

For any change to B1 V3, report:

- Total return (and per-window)
- Max drawdown (and per-window)
- Sharpe (and per-window)
- Profit factor (and per-window)
- Trade-level Jaccard vs current baseline (and per-window)

Return retention alone is not enough. PF retention >= 80% AND Sharpe
retention >= 80% AND Jaccard >= 0.50 in all 3 windows is the bar set in
Phase 12C / 12B / 11.

### Rule 5 - The universe is incomplete; do not draw "no alpha exists" conclusions

Phase 13's single-generator result is bounded by the feature universe.
Three alpha dimensions (Market Regime, Breadth, Multi-Timeframe) have zero
features. Three more have features but disabled consumers. A negative
search result inside Price Structure + Momentum + Volume on daily
timeframe does not falsify alpha in the other 6 dimensions.

Cite: `../../../archive/research/phase14/phase14a_summary.md`, `../../../archive/research/phase14/coverage_report.md`,
`missing_universe_ranked.md`.

### Rule 6 - Read the closure documents before any modification

Required reading order for an agent picking up B1 V3:

1. `verdict_full.md` (this file) - what is true about B1 V3
2. `architecture_full.md` - per-component classification
3. `constraint_geometry.md` - why simple decomposition fails
4. `dependency_graph.md` - structural roles
5. `ag2_lessons_learned.md` - 15 reusable principles
6. `../../../archive/research/phase14/phase14a_summary.md` - what dimensions are missing
7. `*.json` - machine-readable summaries in this package

If an agent does not produce a written justification that contradicts a
specific phase's evidence with a specific phase number and a specific
metric, it is not allowed to modify B1 V3 components.

## Closure statement

> B1 V3 is a constraint-geometry mean-reversion strategy with one entry
> generator, one independent exit alpha, and approximately 9-13
> concentrators whose joint surface defines the trade set. Its measured
> baseline (A_2023 Sharpe 1.57 / PF 2.66; B_2024H1 Sharpe -0.58 / PF 0.64;
> C_2024H2+ Sharpe 0.48 / PF 1.35) cannot be reproduced from any
> single-component subset. The "alpha" is the polytope, not the rules.
>
> Further research must not begin with parameter optimization inside
> B1 V3. It should begin either with feature-universe extension (the
> 3 missing alpha categories from Phase 14A) or with a clean break to a
> different strategy.

End of B1 V3 research.
