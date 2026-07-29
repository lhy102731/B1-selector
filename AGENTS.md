# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Common Commands

```bash
# Stock selection (daily workflow)
python main.py init                          # First-time full data fetch (~6 years)
python main.py update                        # Daily incremental update
python main.py run --b1-match                # Full B1 selection + DingTalk notify
python main.py run --b1-match --max-stocks 500  # Quick test mode

# Backtesting
python build_indicators_cache.py             # Precompute indicator cache (uses raw parquet cache by default)
python build_indicators_cache.py --raw-only  # Build cleaned raw parquet layer only
python build_indicators_cache.py --no-raw-cache  # Legacy path: read CSV directly
python build_indicators_cache.py --research-cache  # Build experimental cache under data/research_indicators_cache
python backtest_optimized.py                 # Default backtest (2021-2026)
python backtest_optimized.py --start 2010-01-01 --end 2025-12-31
python backtest_optimized.py --max-stocks 10 --min-similarity 70
python backtest_optimized.py --decouple j_threshold,volume_shrink_ratio  # Decoupled param sweep
python backtest_optimized.py --zone-j "-10~-7,23~33" --zone-vol "0.1~0.65,0.8~0.9"
python backtest_optimized.py --market-timing data/market/active_cap.csv
python backtest_optimized.py --research-indicators-cache  # Explicitly read data/research_indicators_cache

# Validation
python tests/manual/test_all_cases.py        # Verify 24 historical cases pass filters

# Web interface
python apps/web_server.py                    # Start Flask web server (port 5000)
```

## Architecture

### Core strategy: UnifiedB1Strategy (`strategy/unified_b1_strategy.py`)

A mean-reversion strategy that buys when a stock in an uptrend pulls back to the yellow line (multi-MA average) with KDJ oversold and shrinking volume. Inherits from `BaseStrategy`, loaded dynamically by `StrategyRegistry`.

**Two key indicator lines:**
- **White line (知行短期趋势线)**: `EMA(EMA(close, 10), 10)` — short-term trend
- **Yellow line (知行多空线)**: `(MA14 + MA28 + MA57 + MA114) / 4` — mid/long-term fair value

**Signal chain** (all must pass):
1. White > Yellow (uptrend confirmed)
2. J value in allowed zone (or below threshold, defaults to 33)
3. Volume ratio in allowed zone (or below 0.9 shrink threshold)
4. DIF > 0 (MACD bullish)
5. Market cap ≥ 4 billion
6. No doubling in 60 days
7. Build-position wave quality (gain ≤ 60%, turnover ≤ 90%, red vol > green vol × 1.2, no shrink limit-up, wave quality = "healthy")
8. No S1 distribution signal (detected by `utils/s1_filter.py`)
9. Price in bowl zone (between yellow and white lines), or washout exception

**Key sub-detectors** (all inside `UnifiedB1Strategy`):
- `_calc_build_position_quality()`: Evaluates the surge wave's health
- `_check_wave_break()`: Detects if the surge wave has been broken (price broke yellow line then stop-loss)
- `_detect_washout_exception()`: Washout (击穿对手盘) — price briefly dipped below yellow line on low volume, not yet recovered
- `_detect_super_b1()`: Super B1 — a prior B1 signal existed, J rebounded past 20 then dropped again
- `detect_b2_signal()`: B2 continuation — gap-up or strong bullish candle with 2x volume

### Backtest engine (`backtest_optimized.py`)

Multiprocess backtester with signal precomputation and disk caching:
1. **Precompute phase**: All stocks evaluated across all trading days via multiprocessing pool → cached to `data/signal_cache/*.pkl`
2. **Simulation phase**: Single-process loop replays trading days, looks up precomputed signals, applies parameter filters (J/vol zones, similarity threshold), and manages positions

**Position management:**
- 3-batch building: 1st batch on B1 signal, 2nd on yellow-line pullback or Super B1, 3rd on B2
- Each batch = 1/3 of position allocation (10% of total assets per stock)
- Stop-loss: signal day low − 0.05, plus washout stop-loss widening
- Exits: yellow-line break, stop-loss hit, 4-day profit < 4%, profit→loss reversal, S1 half-position cut, 20% profit take (30%), DDT take-profit, white-line break

**Parameter decoupling** (`--decouple j_threshold,volume_shrink_ratio`): Stores raw J and vol_ratio values during precompute, applies the actual thresholds during simulation. This allows sweeping these parameters without re-running precomputation.

### Pattern matching (`strategy/pattern_matcher.py`)

Six-dimension similarity engine comparing candidates against 24 historical success cases (`strategy/pattern_config.py`). Dimensions and default weights: volume (0.30), price_shape/DTW (0.25), move_power (0.18), trend (0.14), divergence (0.08), limit_state (0.05).

### Data pipeline

- **Source**: akshare / baostock / Tencent API (via `utils/akshare_fetcher.py`)
- **Storage**: CSV files under `data/{prefix}/` by exchange prefix (00, 30, 60, 68)
- **Indicator cache**: Parquet files under `data/indicators_cache/` — precomputed by `build_indicators_cache.py` using multiprocessing, consumed by backtest engine for speed
- **Signal cache**: Pickle files under `data/signal_cache/` — precomputed B1 signals for backtest

### Config flow

`config/strategy_params.yaml` → `StrategyRegistry` → strategy `__init__` params (passed via `register()`). `pattern_config.py` also reads the `B1PatternMatch` section from the same YAML. Backtest CLI args (`--zone-j`, `--zone-vol`) override YAML values at runtime.

## Testing

- After ANY modification to backtest logic, signal computation, or parameter handling (especially `set_param`, filter thresholds, trading rules), immediately run a verification backtest with a known-good parameter set (e.g., J=29, vol=0.75) to confirm results haven't regressed.
- Before starting a parameter sweep, run a 2-value sanity check and verify different parameter values produce DIFFERENT results (not identical).
- Strict forward validation is mandatory for any factor design, ML/ranker model, score threshold, parameter optimization, or promotion decision. Same-period or same-model backtests are exploratory only and must NOT be described as effective, validated, champion, or production-ready.
- Rolling forward validation is the default for multi-year claims: keep train/validation/test window lengths fixed and roll them forward continuously, e.g. train 2020-2022 -> validate 2023 -> test 2024; train 2021-2023 -> validate 2024 -> test 2025; train 2022-2024 -> validate 2025 -> test 2026. Expanding-window validation is only a robustness appendix unless the user explicitly requests it.
- Thresholds, model parameters, and factor variants may be selected only from each fold's train/validation windows. The test year is evaluated once as unseen data and must not be used to choose or revise the candidate.
- Multi-fold reports must show average test performance, worst-fold performance, pass rate across folds, and dispersion. A high average with one failed fold is not production-valid without an explicit bounded-risk rationale.
- Use purge/embargo gaps between train/validation/test windows whenever labels, holding periods, or signal windows can overlap. No random split, random K-fold, or test-window threshold selection is allowed for strategy conclusions.
- Every ML/ranker backtest report must state the exact model artifact, feature set, training range, validation range, test range, embargo days, and whether the tested years were unseen by the model. If this cannot be proven, mark the result as INVALID for promotion.
- Brick V2 9:25 feature boundary: `overnight_gap_pct`, `entry_open_to_yellow_pct`, and `entry_open_to_ma5_pct` are allowed next-open entry fields only when computed from `entry_date` open plus signal-day known references. Match `daily_select.py`: `entry_date` open vs signal-day close/yellow/MA5. Never use `entry_date` high, low, close, intraday future data, T+1 close-derived MA/yellow values, `return_pct`, `exit_date`, `exit_price`, or `hold_days` as model inputs.

## Code Change Rules

- NEVER modify parameter restoration logic (set_param / reset / rollback) without explicit user request. These changes have caused multiple polluted backtest runs.
- When debugging, prefer adding diagnostic logging over changing core logic. Remove debug logs after confirming the fix.
- Before proposing a combined/restructured approach, present the plan and get user approval — don't restructure without asking.

## Task Execution Rules (STRICT)

- **NEVER stop, cancel, or abort a running task without explicit user permission.** Long-running tasks (backtests, sweeps, precomputes) are expected and the user is aware they take time.
- **NEVER change task parameters (sample size, date range, step size, thresholds, etc.) without explicit user permission.** Run exactly what the user specified.
- **NEVER change the test design or scope without explicit user permission.** If a task is too slow, inform the user and ask — don't silently reduce scope.
- **The user's original instructions are the spec.** Only deviate if the user explicitly approves a change.
- **Do not downgrade around system failures.** If Codex or AG2-KBase fails to use the intended workflow, roundtable roster, tools, GPU path, cache path, code executor, or validation design, first diagnose and repair the root cause. Do not switch to a smaller table, single model, reduced scope, weaker validation, CPU fallback, or manual workaround merely to get a result unless the user explicitly authorizes it; record the root cause and repair attempt before any approved fallback.

## Workflow Preferences

- For parameter optimization, use GRID TEST methodology, not auto-iterate/self-iterating loops. Define parameter ranges explicitly and run systematic sweeps.
- GRID TEST values may be selected only on each rolling fold's training/validation side; never choose a threshold or factor variant by looking at any test window.
- When running large sweeps (>100 combinations), always precompute signals to disk cache first and load from cache during the sweep.
- For indicator precompute, heavy ML/ranker training, factor-matrix generation, clustering, SHAP, or large sweeps, check GPU feasibility first and record the selected backend plus CPU fallback reason. Do not use GPU acceleration to change validation scope, parameters, or test design.
- Prefer the raw parquet layer (`data/raw_parquet/{prefix}/{code}.parquet`) for indicator precompute. CSV remains the source of truth; raw parquet is a cleaned, date-ascending cache that should be rebuilt when CSV changes.
- Keep research indicator experiments separate from production: write meeting-generated or hypothesis-specific precomputed indicators to `data/research_indicators_cache`, and read them only with explicit research flags. Do not overwrite `data/indicators_cache` during exploratory factor work.
- Keep Brick production and research scripts separate: `backtest_brick_v2.py` is the production signal generator; AG2 experiments must use research-only runners or `backtest_brick_v2_research.py` unless the user explicitly requests production reproduction.
- Preserve empirically useful Brick factors in a research factor library even when the current usage fails. Directionally confirmed positive factors, severe negative factors, anti-factors, and failed hard-gate factors must be recorded with evidence paths, validation status, failed usage mode, and suggested future usage. Do not discard them merely because hard filtering, direct TopN replacement, or the first V2 integration attempt underperformed; mark them as `research_only`, `not_promoted`, or `do_not_hard_gate` until strict forward validation proves a safe use.
- KBase discovery must optimize for information gain, not source abundance. Do not repeatedly choose a source family merely because the user's archive has more material there; dense/repeated KBase areas require explicit novelty or reopen justification.
- AG2-KBase discovery defaults to the multi-LLM roundtable workflow, then the gated source_librarian -> alpha_hunter -> falsification_officer -> factor_engineer handoff. Use the old single-pass workflow only when explicitly requested.
- Never use Unicode emojis in Python source code — they crash execution on this environment.

## Signal & Strategy Conventions

- The DDT (滴滴) signal is a TAKE-PROFIT (止盈) signal, NOT a stop-loss (止损). It only triggers after `launched` (profit > 5% AND close > white line).
- MA function direction matters: ascending vs. descending order produces completely different similarity matching results. Always verify the sort order when using MA-based comparisons.
- All backtests use A-share data. Stock codes are 6-digit strings (e.g., `"600366"`, `"000977"`). Data files use **GBK encoding**, not UTF-8.
- Data is stored in reverse chronological order (latest first) in CSVs. `UnifiedB1Strategy.calculate_indicators()` sorts ascending to compute indicators, then reverses back. The backtest cache (Parquet) stores ascending.
- `white_line` / `yellow_line` / `J` / `white_gt_yellow` / `volume_shrink` / `DIF` / `doubled` columns are precomputed into the indicators Parquet cache — they must exist for the backtest fast path to work.
