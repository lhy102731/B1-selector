"""Test exit rules using the REAL simulate function."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from strategy.b1_v3_config import B1V3Params
from strategy.b1_v3_strategy import build_raw_cache, filter_and_rank, simulate, calc_metrics
from strategy import b1_v3_strategy as strat
import pandas as pd

CODES = sorted([f.stem for f in Path("data/indicators_cache").glob("*.parquet") if f.stem.isdigit() and len(f.stem)==6])
PERIOD = ('2021-01-01','2026-05-30')
OPT6 = ['q_vol_dec_accel','q_nodist','q_vs_yellow','q_vs_60h','q_red_vol_dec','q_ind_rank']

original = strat.compute_factor_scores
def patched(s, p):
    sc = original(s, p)
    if p.q_vs_yellow: sc['vs_yellow_score'] = max(0,5-abs(s['vs_yellow']))/5
    return sc
strat.compute_factor_scores = patched

def run_test(profit_25pct, use_s1=False, use_ddt=False):
    p = B1V3Params()
    for attr in dir(p):
        if attr.startswith('q_') and not attr.startswith('quality_'):
            try: setattr(p, attr, attr in set(OPT6))
            except: pass
    p.j_max = 20.0
    p.require_vol_price_improving = True
    p.require_no_dist_10d = True
    p.exit_profit_25pct = profit_25pct
    p.exit_s1_clear = use_s1
    p.exit_ddt = use_ddt

    by_date,_ = build_raw_cache(CODES, PERIOD[0], PERIOD[1], p)
    filtered = filter_and_rank(by_date, p)
    eq, trades = simulate(filtered, p)
    m = calc_metrics(trades, eq, p.initial_capital)
    return m

if __name__ == '__main__':
    # Build cache once (shared)
    p_base = B1V3Params()
    for attr in dir(p_base):
        if attr.startswith('q_') and not attr.startswith('quality_'):
            try: setattr(p_base, attr, attr in set(OPT6))
            except: pass
    p_base.j_max = 20.0
    p_base.require_vol_price_improving = True
    p_base.require_no_dist_10d = True
    by_date,_ = build_raw_cache(CODES, PERIOD[0], PERIOD[1], p_base)

    # Test with shared cache, only change exit params
    base_ret = None
    # Baseline (-25pct, no S1, no DDT)
    def make_p(**kwargs):
        p = B1V3Params()
        for attr in dir(p):
            if attr.startswith('q_') and not attr.startswith('quality_'):
                try: setattr(p, attr, attr in set(OPT6))
                except: pass
        p.j_max = 20.0
        p.exit_profit_25pct = False  # remove 25% profit exit
        for k, v in kwargs.items():
            setattr(p, k, v)
        return p

    p_base = make_p()
    filtered = filter_and_rank(by_date, p_base)
    eq, trades = simulate(filtered, p_base)
    m = calc_metrics(trades, eq)
    base_ret = m['total_return']
    print(f"Base (-25pct): Ret={m['total_return']:.1f}% WR={m['win_rate']:.1f}% DD={m['max_drawdown']:.1f}% T={m['total_trades']}")

    s1_tests = [
        ("S1 all", frozenset()),
        ("S1 放量巨阴", frozenset({'顶部大风车', '次高点放量'})),
        ("S1 顶部大风车", frozenset({'放量巨阴', '次高点放量'})),
        ("S1 次高点放量", frozenset({'放量巨阴', '顶部大风车'})),
    ]
    for label, skip in s1_tests:
        p = make_p(exit_s1_clear=True, s1_skip_types=skip)
        filtered = filter_and_rank(by_date, p)
        eq, trades = simulate(filtered, p)
        m = calc_metrics(trades, eq)
        d = m['total_return'] - base_ret
        print(f"{label:<16s}: Ret={m['total_return']:.1f}% (d{d:+.1f}%) WR={m['win_rate']:.1f}% DD={m['max_drawdown']:.1f}%")

    strat.compute_factor_scores = original
    print("Done")
