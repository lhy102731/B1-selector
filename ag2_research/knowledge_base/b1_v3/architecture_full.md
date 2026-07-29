# B1 V3 - Final Architecture Decomposition

Every runtime component is placed into exactly one bucket based on the
evidence accumulated across Phase 9-14B. No new experiments were run for
this document. Phase references identify the experiments that produced
each piece of evidence.

## Entry Generators

A generator is a rule that produces candidate (code, date) pairs.

### C_wave_qualified

- **Mechanism**: inside `analyze_wave`, detect surge days (>= surge_min_gain_pct
  with confirming volume), group them into a wave, then accept the wave iff
  `total_gain <= 60%`, `surge_turnover_sum <= 90`, `positive_vol > negative_vol *
  1.2`, plus the volume-rank confirmations (max-vol bullish, 2nd-max bullish,
  max-high bullish).
- **Evidence**:
  - Phase 12A rank 1 (avg ΔRet -5.10 pp, avg ΔSharpe -0.51, avg ΔPF -0.65 when
    removed; only entry component negative in all 3 windows).
  - Phase 12B alpha-only Jaccard 0.03-0.05 vs baseline confirms the wave gate
    is the only entry alpha that survives in isolation.
  - Phase 13 evaluated 8 alternative families; none satisfied the 4-requirement
    bar to qualify as a second generator.
- **Supporting phases**: 12A, 12B, 12C, 13, 14A, 14B.

## Exit Generators

### t_plus_3_min_return

- **Mechanism**: at hold-day >= 3, if unrealised return < `t_plus_3_min_return`
  exit at close. Default 0.02 (2%).
- **Evidence**:
  - Phase 10 R1 class - the only A-class parameter robust in all 3 windows.
  - Phase 11 Class I - the sole independent alpha source by interaction
    analysis (avg interaction with j_max / pe_max ~= additive; no redundancy).
  - Marginal contribution +1.47 / +1.92 / +5.60 pp in A / B / C windows.
- **Supporting phases**: 9, 10, 11.

## Concentrators

Components that materially shrink or order the candidate set without
generating candidates. Ranked by combined single-effect strength and
interaction strength.

### E_wave_healthy

- **Standalone contribution**: avg ΔPF -0.37, avg ΔSharpe -0.59 when removed.
  Single-removal Jaccard 0.62-0.75 across windows.
- **Interaction contribution**: pair with A_J_range int_PF -0.78
  (3rd strongest negative interaction).
- **Supporting phases**: 12A (rank 4 single ablation), 12C (avg ΔJac +0.017),
  14B (rank 1 single-removal ΔPF, member of `A+E` interaction).

### A_J_range (Prefilter_J + B1 J range)

- **Standalone contribution**: anti-alpha alone (avg ΔPF +0.93 if removed,
  driven by 2024H1 outlier). Avg Jaccard 0.51 when removed alone.
- **Interaction contribution**: HUB constraint. Member of the top 3 strongest
  negative interactions (F+A int_PF -1.02; A+B_KltD int_PF -0.94;
  A+E int_PF -0.78) and 5 of the 8 strong triple interactions
  (top: F+A+B_KltD int_PF -1.58).
- **Supporting phases**: 9 (sensitivity A), 10 (R2 conditional),
  11 (Class III proxy), 12A (single anti-alpha), 12C (rank 2 concentrator),
  14B (hub node).

### F_white_gt_yellow

- **Standalone contribution**: avg ΔPF -0.04, avg ΔJaccard +0.036 (largest
  single-group ΔJaccard in Phase 12C).
- **Interaction contribution**: F+A int_PF -1.02 (strongest pair interaction);
  member of F+A+B_KltD triple int_PF -1.58.
- **Supporting phases**: 12A (rank 13 conditional), 12C (rank 1 concentrator),
  14B (interactive contributor).

### D_white_slope

- **Standalone contribution**: avg ΔPF -0.05; avg Δtrades -259 (largest trade
  reduction among single groups). Mixed direction across windows.
- **Interaction contribution**: weak; mostly substitutive (F+D int_PF +0.05).
- **Supporting phases**: 12A (rank 5), 12C (rank 3 concentrator), 14B.

### B_KltD (Prefilter_KltD)

- **Standalone contribution**: Jaccard 1.000 in all 3 windows, ΔPF 0.000.
  Single-removal DEAD.
- **Interaction contribution**: A+B_KltD int_PF -0.94 (2nd strongest pair);
  F+A+B_KltD int_PF -1.58 (strongest triple). Fully conditional on A_J_range.
- **Supporting phases**: 12A (Class III single), 12C (rank 5), 14B
  (interactive contributor, classic single-dead/joint-alive case).

### B4_turnover_max

- **Standalone contribution**: avg ΔPF -0.17, active in A and C windows.
- **Interaction contribution**: not exercised in Phase 14B remove-two/three
  (not in HIGH_IMPACT set); single-effect evidence places it as independent.
- **Supporting phases**: 12A (rank 2), 14B (single).

## Conditional Components

Help only in specific windows.

### B8_pb_max

- avg ΔPF -0.05 driven entirely by C window (ΔPF -0.16 in C, 0.000 in A/B).
- Conditional concentrator; not in any strong interaction.
- Supporting phases: 12A (rank 3), 14B.

### B7_pe_max

- Removal HURTS C only marginally; A window shows +2.35 pp return on removal
  (anti-alpha in trending market). Conditional on regime.
- Supporting phases: 10 (R2), 11 (Class III), 12A.

### A2_dif_min

- Active marginally in A; B/C near-zero. Conditional in trending regimes.
- Supporting phases: 12A (rank 16, mixed).

### C_wave_healthy in A_2023

- Same component classed as concentrator above. In A_2023 alone its removal
  produces +0.71 pp return; in B and C clear concentrator. Documented here
  for the conditional flag.

## Dead Components

Never materially affect the candidate space at default config in the 60-code
universe.

### G_double_cap (A3 + A4)

- Jaccard 1.000 in all 3 windows; ΔPF 0.000; Δtrades 0.
- `A3_no_double_60d`: no stock in this universe has `doubled=True` at any
  signal day - the predicate never fires.
- `A4_cap_min=2e9`: all 60 codes already have market_cap >= 4B.
- Supporting phases: 12A (Class III), 12C (avg ΔJac +0.002), 14B (single).

### C_bowl (A5 tight tolerance)

- Avg ΔJaccard -0.001 in Phase 12C (restoring TIGHT bowl tolerance increases
  trades by 25 in A and B - the gate at default `bowl_near_pct=3.5` is
  dominated by wave_qualified + white > yellow + position checks).
- Supporting phases: 12C, 14B.

### Prefilter_J (alone, when paired with B1_J_range)

- 12A flagged as redundant: setting `prefilter_j_max=BIG` produced zero new
  candidates because `j_max=30` in the B1 stage still fires.
- Single-removal Jaccard 1.000.
- Note: when J_range is removed as a *group*, both prefilter and B1 fall;
  it's the *combined* J-range removal that has effect, not Prefilter_J alone.

### B2_vol_ma5_max (under default vol_shrink_mode=v1)

- `vol_shrink_mode=v1` ignores `vol_ratio_ma5_max`. Removing it produces zero
  new candidates.
- Supporting phases: 14B.

### Five unused parquet columns

- `ps`, `pcf`, `main_net_flow_x`, `main_net_flow_y`, `main_flow_pct_x`,
  `main_flow_pct_y`: present in indicator parquet, no code path consumes
  them. Documented in Phase 14A feature inventory.

## Redundant Components

Components proven to share an information dimension.

### Prefilter_J vs B1_J_range

- Both default to `J < 30`. Setting either to BIG individually causes zero
  new candidates because the other fires. Information dimension: KDJ-J
  upper threshold.
- Phase 12A flagged both as Class III at single-removal.

### B2_vol_vs_peak vs B2_vol_ma5 (mode-dependent)

- Both express "volume shrink" via different definitions. Under
  `vol_shrink_mode=v1` only the first is active; `vol_shrink_mode=v2` only
  the second; mode `both` requires both. Same information dimension
  (volume contraction).

### Prefilter_KltD and B_KltD (same constraint)

- `prefilter_k_lt_d` is the only consumer; there's no separate B-level KltD
  gate. Listed as one constraint group `B_KltD` throughout Phase 12C / 14B.

### j_max / pe_max (Phase 11 interaction-confirmed redundancy)

- Phase 11 pairwise redundancy matrix: avg deviation from additive -11.79 pp
  (j_max + pe_max). Both filters cover the same alpha dimension (candidate
  set contraction). Listed as Class III proxy variables in Phase 11.

## Phase-by-phase evidence index

| Component | 9 | 10 | 11 | 12A | 12B | 12C | 13 | 14A | 14B |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| C_wave_qualified | - | - | - | I | * | * | * | * | * |
| t_plus_3_min_return | A | R1 | I | - | - | - | - | * | - |
| E_wave_healthy | - | - | - | II | * | concntr | - | * | rank1 |
| A_J_range | A | R2 | III | II | * | concntr | - | * | HUB |
| F_white_gt_yellow | - | - | - | II | * | concntr | - | * | * |
| D_white_slope | - | - | - | II | - | concntr | - | * | weak |
| B_KltD | - | - | - | III | - | concntr | - | * | cond |
| B4_turnover_max | - | - | - | I | - | - | - | * | * |
| B7_pe_max | A | R2 | III | II | - | - | - | * | * |
| B8_pb_max | - | - | - | II | - | - | - | * | * |
| G_double_cap | - | - | - | III | - | dead | - | * | dead |
| C_bowl | - | - | - | II | - | dead | - | * | dead |
| C_no_wave_break | - | - | - | I_anti | - | - | - | * | * |

Legend: * = mentioned/included; I/II/III = Phase 12A class; R1/R2/R3 = Phase 10
robustness class; A = Phase 9 economic class; "concntr" = Phase 12C
concentrator; "dead" = Phase 12C/14B dead; "HUB" = Phase 14B hub node.
