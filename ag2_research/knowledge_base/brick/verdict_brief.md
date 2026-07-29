# Brick V2 Research Brief

This package is project-side AG2 memory for Brick research. It is not a KBase
source-layer writeback.

## Current Verdict

- The current static-SQ-objective Top3 direct-promotion ML ranker path is
  archived as failed. This does not reject all ML ranking and does not reject
  the production Brick signal source.
- Cost-fixed Top2/Top3/Top5 Signal Quality NAV remains high, but executable NAV
  is weak. SQNAV alone is not a promotion metric for this path.
- Direct rank-return monotonicity failed in recent unseen windows: 2024 and
  2025 Top3 underperformed ranks 4-10, and 2026 was mostly flat.
- Top3 selection is train-window sensitive. Same-test three-ranker Kendall W
  was 0.437 in 2024, 0.264 in 2025, and 0.209 in partial 2026.
- Score gaps are not a reliable confidence measure in the current path.

## Archived Factors

Do not rediscover or hard-gate these seven factors without a different
mechanism and a new strict-forward validation design:

- `vol_authenticity_path_smoothness_10d`
- `streak_exhaustion_max_20d_peer_rank`
- `pullback_depth_percentile`
- `downside_vol_skew_20d`
- `sentiment_x_pullback_interaction`
- `sentiment_pullback`
- `streak_exhaustion_peer_rank`

They remain research-only diagnostics, not promoted factors.

## Industry Cap

`max_per_ind=2` is a portfolio/list construction diagnostic. In the cost-fixed
rerun it improved Top3 average SQNAV and executable CAGR but worsened worst
executable CAGR; Top5 became worse. It is not a validated alpha factor or a
universal hard default.

## Open Directions

Prefer proposals that address the failure mechanism:

- execution-aware ranking objectives and labels;
- abstention/no-trade diagnostics selected only inside train/validation folds;
- signal-sequence state as sample weights or grouped model dimensions;
- entry-day executable features respecting the pre-09:25 boundary;
- full-candidate-pool data capture for bottom-bucket monotonicity tests.

## Hard Boundary

Use strict forward validation by `entry_date`. Purge train labels with
`exit_date < test_start`. Do not use `return_pct`, `exit_date`, `exit_price`,
or `hold_days` as model inputs. Do not modify `backtest_brick_v2.py`.
