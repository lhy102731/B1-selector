# Missing Universe - Ranked

Categories with **zero confirmed generators**, **zero or near-zero active
runtime filters**, and **zero or near-zero active ranking features** are
listed below. They are ranked by expected alpha potential (composite of
academic / industry precedent + low expected overlap with the wave_qualified
generator).

Expected overlap is estimated qualitatively from the dimensions the candidate
mechanism reads vs. the dimensions wave_qualified reads (price structure +
volume in a single daily timeframe).

Probabilities are heuristic and meant to rank, not to forecast.

---

## 1. Multi-Timeframe (HIGHEST potential)

**Status**: 0 generators, 0 filters, 0 ranking features.

**Why absent**: Strategy runs on a single daily timeframe. No weekly /
monthly aggregations of close, volume, J, MACD; no intraday-shaped
features (the parquet has only OHLCV daily, no minute data).

**Potential alpha mechanism**:
- Weekly-trend confirmation filter (daily setup + weekly higher-high)
- Daily-weekly KDJ divergence
- Weekly volume contraction preceding daily breakout
- Multi-resolution wave qualification (the wave detector run on a 3-day or
  weekly aggregated bar would produce a structurally different gate)

**Expected overlap with wave_qualified**: LOW. The wave detector is anchored
to daily bars; a weekly-aggregated equivalent fires on different dates by
construction. Daily/weekly divergence has near-zero same-day-same-stock
overlap with the wave bowl pullback geometry.

**Probability of producing an independent generator**: **HIGH (~0.60)**
within the current parquet (re-aggregation is achievable without new data).

---

## 2. Market Regime

**Status**: 0 generators, 0 filters, 0 ranking features. No index reference
exists in the parquet schema.

**Why absent**: B1 V3 only knows about per-stock features. There is no
aggregate market state (SSE 50, CSI 300, breadth ratio, advance/decline)
that conditions entries.

**Potential alpha mechanism**:
- Don't trade entries when market in a `risk_off` state
- Tighten / loosen position size by regime
- Different generator activation rules in trending vs. ranging markets
- Phase 10 already showed B_2024H1 is the regime-fragile window; a regime
  filter could improve robustness even without producing more entries

**Expected overlap with wave_qualified**: LOW for generation (it gates rather
than generates) but it changes WHEN wave_qualified is acted upon. As a
secondary generator it could be e.g. "market in uptrend AND any of N
secondary cues" - again structurally different from per-stock wave gating.

**Probability of producing an independent generator**: **MEDIUM-HIGH
(~0.45)**. Requires loading an index series - currently no parquet column
for that, so user might judge this as adding a new data source.

---

## 3. Relative Strength (cross-sectional vs universe)

**Status**: 19 CS rank features defined; 1 used (`cs_lower_shadow` in B9
filter). All G3 scoring weights are off by default.

**Why effectively absent**: `add_cs_ranks` runs but only 1 of 19 outputs
is consumed at runtime. The cross-sectional dimension is **defined but
disabled**.

**Potential alpha mechanism**:
- Rank candidates by 20d return percentile against the universe and only
  take top-quintile entrants
- "Buy strength, fade weakness" - momentum-RS combo where the stock is
  both technically buyable (some local gate) AND in top-decile by 20d / 60d
  return cross-sectionally
- A pure CS-rank generator could fire on days when at least N stocks lead
  the universe by some threshold

**Expected overlap with wave_qualified**: LOW-MEDIUM. wave_qualified
already biases toward stocks that have surged 4%+ on volume in recent
history - a 20d-RS leader is often the same stock. But the trigger date
differs (CS-rank generator fires whenever rank flips, not on wave-pullback
geometry), so trade-level overlap should be far below 0.30.

**Probability of producing an independent generator**: **MEDIUM
(~0.40)**. CS rank features already exist; just need a generator that
USES them. Currently no live consumer at the generator stage.

---

## 4. Volatility

**Status**: 6 features defined (`atr_14`, `volatility_10d`, `adx_14`,
`wr_14`, `max_dd_day`, `dist_to_low`). All G4/G6 scoring weights off by
default; no hard filter; no generator.

**Why effectively absent**: Features exist in `_compute_features`. Their
ranking contributions are toggled off (`q_atr`, `q_vol10`, `q_adx`,
`q_wr`, `q_max_dd_day`, `q_dist_low` all False by default).

**Potential alpha mechanism**:
- Volatility-contraction generator (Phase 13 G3 - PF 2.77 in C window
  alone, missed bar)
- ADX-based trend regime filter
- "Buy quiet" - take entries only when ATR percentile is in lowest quintile
  AND something else triggers
- VWAP-mean-reversion (already partially defined but `q_vwap` off)

**Expected overlap with wave_qualified**: LOW. wave_qualified does not
constrain volatility directly. Phase 13 G3 standalone Jaccard = 0.030
against wave_qualified - essentially independent at the candidate level.

**Probability of producing an independent generator**: **MEDIUM
(~0.35)**. Phase 13 G3 already came close (PF 2.77 in C window) but failed
the 2-of-3-windows requirement. With existing features only, the family is
moderately exploited; the alpha there is real but window-conditional.

---

## 5. Valuation

**Status**: PE (`pe_dynamic`) and PB (`pb`) used as **hard caps only**
(`pe_max=80`, `pb_max=8`). G5 q_pe / q_pb scoring weights off by default.
`ps` and `pcf` are in parquet but unconsumed.

**Why effectively absent for alpha**: Valuation enters as a rejection
gate, never as a positive signal. PE/PB rank weights are off.

**Potential alpha mechanism**:
- Low-PE breakout generator (value-shake stocks emerging from a base)
- PB compression vs sector median (uses concept_stocks data already loaded)
- Valuation-driven mean reversion on broad pullbacks

**Expected overlap with wave_qualified**: MEDIUM. The PE/PB caps already
filter the wave_qualified candidate set, so valuation-positive generators
overlap on the universe but trigger on different dates. Trade-level
overlap likely 0.05-0.25.

**Probability of producing an independent generator**: **LOW-MEDIUM
(~0.25)**. Valuation factors have weaker short-horizon predictive power on
3-week holds (B1 V3 max_hold=35 days). They tend to manifest over months.

---

## 6. Sector Rotation

**Status**: 3 features computed (`concept_rank`, `concept_dev`,
`concept_count`). All G8 toggles off. Concept data file
(`data/block/concept.json`) is loaded but the runtime ignores its
ranking output.

**Why effectively absent**: Same pattern as Relative Strength - features
computed but consumers off.

**Potential alpha mechanism**:
- Concept-rotation generator (concept with rising aggregate momentum AND
  candidate in top half of concept)
- Concept-strength filter on top of wave_qualified
- "Hot concept, technical setup" cross-product

**Expected overlap with wave_qualified**: LOW for generation; wave_qualified
operates on single-stock technicals and ignores cross-stock concept
membership. Concept membership reshuffles candidate selection without
changing per-stock wave geometry.

**Probability of producing an independent generator**: **LOW-MEDIUM
(~0.25)**. Data is available; the signal is noisy at the per-stock level
since concepts overlap heavily (a stock has many concepts) and concept
membership data is not regime-adjusted.

---

## 7. Breadth

**Status**: 0 features. No advance/decline, no count of stocks above
MA20, no new-highs / new-lows.

**Why absent**: B1 V3 only reads per-stock parquet. Aggregate breadth
requires scanning the universe per day.

**Potential alpha mechanism**:
- Generator-conditioning: only fire on broad uptrend days
- Risk-off filter when breadth deteriorates
- Cluster-arrival detection: many stocks trigger same generator together

**Expected overlap with wave_qualified**: NEAR-ZERO for generation
(breadth is a market-wide variable). Mostly useful as a regime filter,
not as a direct generator.

**Probability of producing an independent generator**: **LOW (~0.20)**.
Mostly a quality multiplier rather than an independent trigger.

---

## Ranked summary

| Rank | Missing dimension | Independence prob | Why it ranks here |
|---:|---|:-:|---|
| 1 | Multi-Timeframe | ~0.60 | New temporal resolution structurally distinct from daily wave |
| 2 | Market Regime | ~0.45 | Different question (when to act) than wave (what to act on) |
| 3 | Relative Strength CS | ~0.40 | Features exist; only consumers are off; CS leadership independent of wave shape |
| 4 | Volatility | ~0.35 | G3 already came close in 12C, family is sampled but not exploited |
| 5 | Valuation | ~0.25 | Caps only; weaker short-horizon signal |
| 6 | Sector Rotation | ~0.25 | Data available; per-stock signal is noisy |
| 7 | Breadth | ~0.20 | Mostly a regime filter, not a generator |

---

## Confirmation of audit hypothesis

> Whether Phase 13's "single-generator" result is a property of the market or a
> property of the current feature universe.

Three dimensions (Market Regime, Breadth, Multi-Timeframe) have **zero
features** defined. Three more (Volatility, Relative Strength CS, Sector
Rotation) have features defined but consumers default-off. Of the four
generators evaluated (1 confirmed + 3 standalone-passers from Phase 13),
all four read exclusively from Price Structure + Momentum + Volume in a
single daily timeframe.

This is consistent with the feature universe constraining the
search space, not with the market necessarily containing only one
generator. Phase 13's conclusion holds for what the current feature
universe can see; whether additional independent generators exist in the
market is **not refuted** by Phase 13, only **unobservable** within the
universe currently exposed at runtime.
