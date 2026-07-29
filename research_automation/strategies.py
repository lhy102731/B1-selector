"""strategies.py -- which strategy can the automation layer optimize, and how.

Single source of truth for strategy selection. Each profile declares capability,
the backtest entrypoint key (resolved in experiment_runner._STRATEGY_SPECS), the
CLI-tunable param set, normalization aliases, and any caveat shown at runtime.

Capabilities:
  full         -- champion config is CLI-reproducible; real "delta vs champion".
  experimental -- runnable, but baseline != registered champion (caveat applies).
  none         -- not optimizable (no backtest harness); selection is rejected.
"""
from __future__ import annotations

from dataclasses import dataclass, field


class UnsupportedStrategyError(RuntimeError):
    pass


_B1_CLI = {
    "j_max", "j_min", "vol_mode", "vol_peak", "vol_ma5", "turnover", "pe_max", "pb_max",
    "cs_shadow", "top_n", "wave_qual", "wave_health", "wave_break", "washout",
    "wave_max_gain", "wave_max_turnover", "wave_red_green_ratio", "wave_health_ratio",
    "surge_min_gain", "wave_break_width", "group_gap", "group_back_gap",
}
_B1_NORM = {"turnover_max": "turnover", "top_n_per_day": "top_n",
            "vol_vs_wave_peak_max": "vol_peak", "vol_shrink_mode": "vol_mode"}

_BRICK_CLI = {"method", "top_n", "max_per_ind", "min_score", "entry_ma_source"}


@dataclass
class StrategyProfile:
    id: str
    supported: bool
    capability: str                 # full | experimental | none
    cli_params: set = field(default_factory=set)
    normalize_map: dict = field(default_factory=dict)
    reason: str = ""                # why unsupported / caveat for experimental
    caveat: str = ""                # printed at runtime when experimental


PROFILES = {
    "b1": StrategyProfile(
        id="b1", supported=True, capability="full",
        cli_params=_B1_CLI, normalize_map=_B1_NORM,
    ),
    "brick": StrategyProfile(
        id="brick", supported=True, capability="experimental",
        cli_params=_BRICK_CLI, normalize_map={},
        caveat=("Brick champion is V2.1 = V2 + executive-reduction (高管减持) screening, which "
                "lives in filter_exec_reduce.py / daily_run.py as a SIGNAL post-filter and is NOT "
                "in backtest_brick_v2.py. This automation optimizes the V2 ranking backtest only; "
                "results are V2-backtest-relative, NOT V2.1-champion-equivalent, and 高管减持 is "
                "not backtest-measurable. Metrics = stdout Avg-return / Win-rate / Total."),
    ),
    "b3": StrategyProfile(
        id="b3", supported=False, capability="none",
        reason=("B3 has no backtest harness (no B3Params class, no config section, thresholds are "
                "hardcoded literals duplicated in run_b3.py and main.py). It cannot be auto-optimized "
                "until a B3 backtest is built (see archive/reports/parameter_remediation_plan.md 3.5)."),
    ),
}


def get_profile(strategy: str) -> StrategyProfile:
    p = PROFILES.get((strategy or "").lower())
    if p is None:
        raise UnsupportedStrategyError(
            f"unknown strategy '{strategy}'. Known: {sorted(PROFILES)}")
    return p


def require_supported(strategy: str) -> StrategyProfile:
    p = get_profile(strategy)
    if not p.supported:
        raise UnsupportedStrategyError(f"strategy '{p.id}' is not optimizable: {p.reason}")
    return p
