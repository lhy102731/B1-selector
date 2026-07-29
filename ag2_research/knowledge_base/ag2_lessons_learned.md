# AG2 Lessons Learned - from B1 V3 Research (Phase 9-14B)

Reusable research principles extracted from the B1 V3 closure. Every lesson
points to the phase(s) that produced its evidence so future agents can
verify the citation.

## 1. Single-variable ablation can be severely misleading

**Lesson.** Removing one filter at a time and measuring ΔRet / ΔPF / ΔSharpe
will under-state the importance of conditionally-active filters by an order
of magnitude.

**Evidence.**
- Phase 12A: `B_KltD` single-removal Jaccard 1.000 in all 3 windows; ΔPF
  0.000. Conclusion if stopped here: "B_KltD is dead, remove it".
- Phase 14B: `A_J_range + B_KltD` joint removal int_PF -0.94 below additive.
  B_KltD is NOT dead - it is dominated by A_J_range and activates only when
  A_J_range is absent.

**Generalisation.** When a filter sits "downstream" of a stricter filter in
the same dimension, single-ablation measures the *marginal* effect of the
*subset of cases not already covered by the upstream filter*. Always test
remove-pair (at minimum) for any candidate redundancy classification.

## 2. Interaction effects must be tested before declaring redundancy

**Lesson.** "Redundant" in single-ablation does not mean "redundant in joint
ablation". Phase 12A's Class III ("redundant") set contained at least one
constraint (`B_KltD`) that was actually a conditional bridge.

**Evidence.**
- Phase 12A: Class III set = {A3, A4, Prefilter_J, Prefilter_KltD, B1_J_range,
  B2_vol_shrink}.
- Phase 14B: of those, `Prefilter_KltD` (B_KltD) showed the cleanest
  conditional-activation signature when paired with A_J_range.

**Generalisation.** Treat single-ablation "redundancy" as a hypothesis,
not a conclusion. Confirm by checking at least one remove-pair with the
strongest neighbouring constraint in the same alpha dimension.

## 3. Return retention is insufficient for declaring alpha preservation

**Lesson.** A reduced system that preserves total return can still be a
fundamentally different strategy with worse risk-adjusted performance.

**Evidence.**
- Phase 12B: alpha-only system showed +82.6% return retention (A_2023:
  baseline +12.93%, alpha_only +10.68%, ratio 82.6%) but PF dropped from
  2.66 to 1.16 and max drawdown tripled (-3.14% → -10.26%).
- The system traded 10x more often and the trade identity (Jaccard 0.051)
  was unrecognisable.

**Generalisation.** Always report PF, Sharpe, max drawdown, AND trade-level
overlap. Return alone is a one-dimensional shadow of strategy identity.

## 4. Jaccard (trade-level overlap) should always be tracked

**Lesson.** Two backtests with similar Sharpe / PF / return can be entirely
different strategies. Without trade-level Jaccard, this difference is
invisible.

**Evidence.**
- Phase 12B: alpha-only Jaccard 0.034-0.051 vs baseline despite similar
  return magnitudes.
- Phase 13: Combined `baseline + G4_RelativeStrength` improved B-window
  but altered trade identity (Jaccard 0.118).
- Phase 14B: A+B_KltD removal preserved PF (delta -0.015) but Jaccard 0.41
  - confirms that the strategy state moved despite the metric headline
  not moving.

**Generalisation.** Treat Jaccard < 0.30 as "this is a different strategy".
Treat Jaccard 0.30-0.70 as "this is a partly different strategy". Treat
Jaccard > 0.80 as "this is approximately the same strategy". Never accept
"alpha preserved" without checking Jaccard.

## 5. PF retention is more important than return retention

**Lesson.** Profit factor proxies the quality of the win/loss distribution.
Return proxies the magnitude. A system can match return by trading more
losers and more winners simultaneously, but PF will drop.

**Evidence.**
- Phase 12B: alpha-only C_2024H2_latest return 14.21% (199% retention) but
  PF 1.10 vs baseline 1.35 - PF retention 81%, only marginally meeting an
  80% threshold despite the apparent return outperformance.
- Phase 12C: 4-constraint restoration (F+A+D+E) hit PF retention 77-123%
  (close to baseline) but Jaccard only 0.50.

**Generalisation.** For any "alpha preservation" claim, require PF
retention >= 80% AND Sharpe retention >= 80% AND Jaccard >= 0.50 in all
windows. Return is not sufficient.

## 6. Independent alpha != sufficient alpha

**Lesson.** A system can have N truly independent alpha sources (verified by
Jaccard < 0.30 and improved combined-system PF) and still fail to function
without additional concentrators.

**Evidence.**
- Phase 11 identified `t_plus_3_min_return` as the sole Class I independent
  alpha.
- Phase 12B kept it + the only entry generator `C_wave_qualified` and
  removed everything else. The system collapsed: Jaccard 0.03-0.05, PF 1.10,
  Sharpe 0.68, DD 3x worse.

**Generalisation.** Independence of alpha is a *necessary* property but not
sufficient. Test the minimum reconstruction (keep only independent alpha
sources, remove the rest) before declaring an alpha decomposition complete.

## 7. Generator-driven and constraint-geometry systems require different audits

**Lesson.** A generator-driven system can be decomposed into independent
factors. A constraint-geometry system cannot - its alpha is the surface of
a joint inequality polytope and is destroyed by single-factor reduction.

**Evidence.**
- Phase 12B alpha-only failure shows B1 V3 is not generator-driven.
- Phase 14B remove-two interaction matrix and remove-three triples
  (top triple int_PF -1.58 exceeds top pair -1.02) demonstrates the system
  has constraint-geometry effective dimension >= 3.
- Phase 12C cannot recover Jaccard > 0.65 with any 7-group subset,
  confirming trade identity is not factorisable.

**Generalisation.** Before designing experiments, classify the system:
attempt a "remove everything except verified alpha" run. If trade identity
or PF collapses, switch the audit methodology to interaction-based.

## 8. Sensitivity != robustness != independence

**Lesson.** A parameter that is highly sensitive in one window may be
robust across windows AND independent of other parameters - or it may be
just regime-fitted.

**Evidence.**
- Phase 9: `B5_ret_5d_min` ranked A-class on B_2024H1 (Δ +25 pp). Phase 10
  flagged it R3 (unstable across windows). Phase 11 confirmed: B-window
  effect did not generalise.
- `t_plus_3_min_return` passed Phase 9 (A), Phase 10 (R1), and Phase 11
  (Class I) - the only A-class parameter that survived all three audits.

**Generalisation.** Treat Phase 9 (sensitivity) as a candidate-discovery
step. Treat Phase 10 (robustness) as a filter for sample-specific noise.
Treat Phase 11 (independence) as a filter for redundancy. Only parameters
that survive all three are confirmed independent alpha sources.

## 9. Dead in default config != dead in all configs

**Lesson.** A filter that has Jaccard 1.000 under the default parameter set
may become active under different configurations.

**Evidence.**
- Phase 14B: `B_KltD` Jac 1.000 alone, but Jac 0.41 when A_J_range is
  also disabled.
- Phase 14B: `B2_vol_ma5_max` Jac 1.000 because `vol_shrink_mode=v1`
  ignores this parameter. Under `vol_shrink_mode=v2` it would be the
  binding gate.

**Generalisation.** "Dead" should be qualified as "dead under <config>".
Future agents should not delete parameters just because they look dead
under one configuration.

## 10. Universe choice shapes the visible alpha

**Lesson.** Conclusions about "no second generator exists" hold only for the
feature universe currently exposed at runtime.

**Evidence.**
- Phase 14A: 3 of 10 alpha categories have ZERO features (Market Regime,
  Breadth, Multi-Timeframe).
- Phase 14A: 3 more categories have features defined but consumers off
  (Volatility, Relative Strength CS, Sector Rotation).
- Phase 13: All 8 generator candidates tested and all 4 passers/partial-
  passers read exclusively from Price Structure + Momentum + Volume on a
  single daily timeframe.

**Generalisation.** Before declaring "no other alpha exists", check
coverage. If coverage is incomplete, the conclusion is "no other alpha is
visible in the current universe" - not "no other alpha exists".

## 11. Higher-order interactions exist and can dominate pairs

**Lesson.** Some constraint systems have effective dimension >= 3:
three-way removal effects can exceed any two-way effect.

**Evidence.**
- Phase 14B: top triple `F + A + B_KltD` int_PF -1.58 > top pair `F + A`
  int_PF -1.02. The 3rd member adds 0.56 of interaction beyond the
  pair-additive prediction.

**Generalisation.** When pair interactions are strong, test triples for
the strongest pairs. If higher-order interactions dominate, the system is
non-factorisable below that order.

## 12. The order of phases matters: discovery → robustness → independence → interaction

**Lesson.** Each audit has its own valid claim. Mixing them produces
mistaken conclusions.

**Evidence sequence.**
- Phase 9 (sensitivity): produces candidate set, but contains regime
  artefacts.
- Phase 10 (robustness): removes regime artefacts from the candidate set.
- Phase 11 (alpha isolation): removes redundancy among survivors.
- Phase 12B (sufficiency): tests whether the verified set is enough.
- Phase 12C (recovery): tests whether removed components can be restored
  to close the gap.
- Phase 14B (constraint geometry): tests whether the remaining structure
  is factorisable or geometric.

**Generalisation.** This pipeline is reusable. Every phase produces a
specific kind of claim. Do not conflate "sensitive" with "robust", "robust"
with "independent", or "independent" with "sufficient".

## 13. Anti-alpha components exist and are mis-classified by sensitivity alone

**Lesson.** A filter whose removal *improves* performance is anti-alpha at
current settings.

**Evidence.**
- Phase 12A: `C_no_wave_break` average ΔRet +2.13 pp when removed, with
  improved PF and Sharpe in most windows. Flagged as Class I anti-alpha.
- Phase 14B confirms the same single-removal signature.

**Generalisation.** Look for positive ΔPF and ΔSharpe under removal as a
red flag. Anti-alpha components should be reported as findings, not as
optimisation recommendations (and they were not).

## 14. Constraint hubs are recognised by anti-alpha single behaviour + strong negative interactions

**Lesson.** A hub constraint may look like anti-alpha by itself but be the
spine of the constraint geometry. The fingerprint is: high positive single
ΔPF (anti-alpha look) + dominant negative pair interactions with several
other constraints.

**Evidence.**
- Phase 14B: `A_J_range` single ΔPF +0.93 (looks anti-alpha) BUT
  pair int_PF -1.02 with F, -0.94 with B_KltD, -0.78 with E (all strong).
- Appears in 7 of 8 remove-three triples and all 5 strongest negative
  triples.

**Generalisation.** Never delete a constraint that shows the hub fingerprint
even if its single-removal effect is anti-alpha. The single signal may be a
regime artefact while the structural role is real.

## 15. Document with evidence, not with confidence

**Lesson.** Every claim in a closure report should cite the phase / file /
metric that produced it. Confidence statements without citations cannot be
verified by future agents.

**Evidence.** This document itself follows the pattern: every lesson cites
the source phase and the specific metric value.

**Generalisation.** AG2 agents should refuse to accept claims without
citations. Closure documents should be auditable: any reader can trace
"the system has only one entry generator" → Phase 13 final verdict →
phase13_results.json → the 8 generators × 3 windows × 4 requirements
matrix.
