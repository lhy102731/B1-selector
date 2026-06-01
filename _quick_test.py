import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from strategy.b1_v3_config import B1V3Params
from strategy.b1_v3_strategy import build_raw_cache, filter_and_rank, simulate, calc_metrics
from strategy import b1_v3_strategy as strat

CODES = sorted([f.stem for f in Path("data/indicators_cache").glob("*.parquet") if f.stem.isdigit() and len(f.stem)==6])
PERIOD = ('2021-01-01','2026-05-30')
OPT6 = ['q_vol_dec_accel','q_nodist','q_vs_yellow','q_vs_60h','q_red_vol_dec','q_ind_rank']

orig = strat.compute_factor_scores
def pch(s, p):
    sc = orig(s, p)
    if p.q_vs_yellow: sc['vs_yellow_score'] = max(0,5-abs(s['vs_yellow']))/5
    return sc
strat.compute_factor_scores = pch

def mkp(**kw):
    p = B1V3Params()
    for a in dir(p):
        if a.startswith('q_') and not a.startswith('quality_'):
            try: setattr(p, a, a in set(OPT6))
            except: pass
    p.j_max = 20.0
    p.exit_profit_25pct = False
    p.require_no_dist_10d = True
    p.require_vol_price_improving = True
    p.pe_max = 30
    for k, v in kw.items():
        setattr(p, k, v)
    return p

if __name__ == '__main__':
    p = mkp()
    by_date, _ = build_raw_cache(CODES, PERIOD[0], PERIOD[1], p)
    f = filter_and_rank(by_date, p)
    e, t = simulate(f, p)
    m = calc_metrics(t, e)
    print(f"Base (-25pct): Ret={m['total_return']:.1f}% WR={m['win_rate']:.1f}% DD={m['max_drawdown']:.1f}%")

    p2 = mkp(exit_s1_clear=True)
    f2 = filter_and_rank(by_date, p2)
    e2, t2 = simulate(f2, p2)
    m2 = calc_metrics(t2, e2)
    print(f"+S1: Ret={m2['total_return']:.1f}% (d{m2['total_return']-m['total_return']:+.1f}%) WR={m2['win_rate']:.1f}% DD={m2['max_drawdown']:.1f}%")

    strat.compute_factor_scores = orig
