# B1 V3 Dependency Graph

Constructed from Phase 14B remove-two interaction matrix (5x5 HIGH_IMPACT
subset). Edge weights are average pair `int_PF` across 3 windows. Negative
edges indicate constraint-geometry interactions; positive edges indicate
substitutive overlap. Phase 14B remove-three results extend the graph to
higher-order interactions.

## Node table

| Node | Single ΔPF | Strongest pair int_PF | Triple membership | Role |
|---|---:|---:|---:|---|
| A_J_range | +0.93 | -1.02 (with F) | 7 of 8 triples | HUB |
| F_white_gt_yellow | -0.04 | -1.02 (with A) | 5 of 8 triples | bridge to A |
| E_wave_healthy | -0.37 | -0.78 (with A) | 4 of 8 triples | bridge to A |
| B_KltD | 0.00 | -0.94 (with A) | 3 of 8 triples | conditional leaf |
| D_white_slope | -0.05 | +0.09 (with E, substitute) | 4 of 8 triples | independent / substitute |

## Edge table (avg int_PF over 3 windows)

```
A_J_range -- F_white_gt_yellow      -1.02   strong negative (constraint geometry)
A_J_range -- B_KltD                 -0.94   strong negative (conditional activation)
A_J_range -- E_wave_healthy         -0.78   strong negative
A_J_range -- D_white_slope          +0.06   weak positive (substitute)
F -- E                              -0.07   weak negative
F -- D                              +0.05   weak positive
F -- B_KltD                         +0.00   none
E -- D                              +0.09   weak positive (substitute)
E -- B_KltD                         +0.00   none
D -- B_KltD                         -0.01   none
```

## ASCII graph (only |int_PF| >= 0.05 edges shown, weight magnitude in []):

```
                 [1.02 -]
       F_white_gt_yellow ----------- A_J_range -- [0.94 -] -- B_KltD
                  \  [0.07 -]            |
                   \                     | [0.78 -]
                    \                    |
                     \                   v
                      \           E_wave_healthy
                       \              /
                        \    [0.09 +]/
                         \          /
                          D_white_slope (independent)
```

(- = negative interaction = removing both is worse than additive; + = positive)

## Structural roles

### HUB

`A_J_range` is the unique hub. Evidence:
- Appears in every top-3 negative pair (F+A, A+B_KltD, A+E).
- Appears in 7 of 8 remove-three triples and 5 of 5 strongest negative ones.
- Has the largest "anti-alpha" single-removal signal (ΔPF +0.93) but the
  most damaging interactive role - the canonical fingerprint of a hub
  whose isolation is misleading.
- Strongest 3-way interaction (`F + A + B_KltD` int_PF -1.58) requires A.

### BRIDGES

A bridge connects the hub to other parts of the graph and produces large
interaction only when its hub partner is removed.

- `F_white_gt_yellow` - bridge to A (F+A int_PF -1.02). Alone harmless
  (-0.04 ΔPF). With A removed, F's removal cascades.
- `E_wave_healthy` - bridge to A (A+E int_PF -0.78). Also has its own
  single effect (ΔPF -0.37), making it the highest-utility bridge.
- `B_KltD` - bridge to A (A+B_KltD int_PF -0.94). Single-removal dead
  (Jac 1.000); the cleanest example of a conditional bridge.

### INDEPENDENT / SUBSTITUTE

- `D_white_slope` - shows mostly positive (substitute) interactions
  (D+E +0.09, A+D +0.06). Its removal under any pair partner does
  *less* damage than additive, suggesting it overlaps in coverage with
  other constraints rather than holding a unique geometric face.

### ISOLATED NODES (none with strong negative interactions in HIGH_IMPACT 5)

Phase 14B tested all 16 baseline constraint groups in remove-one, and 5 in
remove-two/three. Among the 5 HIGH_IMPACT nodes, every constraint has at
least one significant interaction edge. No HIGH_IMPACT node is structurally
isolated.

### DEAD NODES (in 60-code universe)

These constraints showed Jaccard 1.000 and ΔPF 0.000 in all 3 windows of
Phase 14B remove-one and are not part of the interaction matrix:

- `G_double_cap` (A3 + A4) - no candidate in this universe ever triggers
- `B2_vol_ma5` - `vol_shrink_mode=v1` ignores this parameter
- `Prefilter_J` (alone, holding B1 J range constant) - redundant with B1
- `Prefilter_KltD` (alone) - dominated by downstream wave / bowl filters

These are dead under default configuration; they may or may not be dead under
other configurations. Phase 14B did not test config variants.

### Structural importance ranking

Combining single-effect, pair interaction max magnitude, and triple membership:

| Rank | Node | Score basis |
|---:|---|---|
| 1 | A_J_range | HUB; 3 strong negative pairs; 7 of 8 triples |
| 2 | F_white_gt_yellow | Strongest pair partner of A; 5 of 8 triples |
| 3 | E_wave_healthy | Bridge + strongest single ΔPF among non-hubs (-0.37) |
| 4 | B_KltD | Conditional activation; cleanest geometry example |
| 5 | D_white_slope | Substitute / independent; weakest interactions |
| 6 | B4_turnover_max | Independent contributor; not in HIGH_IMPACT graph |
| 7 | B8_pb_max | Conditional on C window; not in HIGH_IMPACT graph |
| 8 | B7_pe_max | Conditional; tested in interaction by Phase 11 (with j_max int -11.79 pp) |
| 9 | C_no_wave_break | Anti-alpha; appears in graph only as a negative single effect |
| -- | G_double_cap, C_bowl, B2_vol_ma5, A2_dif_min, B5_ret_5d_min, etc. | dead or window-isolated |

## Caveats

1. The 5x5 matrix covers only the HIGH_IMPACT subset specified by Phase 14B.
   Other constraint pairs (e.g. B7_pe_max + j_max) are NOT measured in the
   14B matrix but have been measured at earlier phases (e.g. Phase 11
   redundancy matrix int_PF for j_max + pe_max = -11.79 pp across windows
   - a separate but consistent interaction effect outside the 14B graph).

2. Edge weights are averaged over 3 windows. In-window magnitudes differ
   substantially (Phase 14B raw JSON has per-window interactions). The
   averaged graph is the structural summary; per-window graphs would show
   regime-dependent variations.

3. The graph captures average behaviour; it does not predict any specific
   parameter setting outside the tested defaults.
