# -*- coding: utf-8 -*-
"""
B1 V3 CLI: select / backtest / sweep
"""

import sys, os, time, argparse, pickle, itertools, json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))

from strategy.b1_v3_config import B1V3Params, build_fac_list, SWEEP_PRESETS
from strategy.b1_v3_strategy import (
    build_raw_cache, filter_and_rank, simulate, calc_metrics, compare_senior,
    INDICATORS_DIR, RAW_CACHE_DIR,
)

# ============================================================
# STOCK LIST
# ============================================================

def get_stock_list(max_stocks=0):
    codes = sorted([f.stem for f in INDICATORS_DIR.glob("*.parquet")
                    if f.stem.isdigit() and len(f.stem) == 6])
    if max_stocks and max_stocks > 0:
        codes = codes[:max_stocks]
    return codes


# ============================================================
# SELECT MODE: daily stock picks
# ============================================================

def cmd_select(args):
    """Generate today's B1 picks and send via DingTalk."""
    import yaml
    from utils.dingtalk_notifier import DingTalkNotifier

    p = B1V3Params()
    today = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - pd.Timedelta(days=7)).strftime('%Y-%m-%d')

    codes = get_stock_list(args.max_stocks)
    print(f"Scanning {len(codes)} stocks for B1 signals on {today}...")

    # Extract today's signals (no cache for select mode)
    all_signals = []
    for code in codes:
        from strategy.b1_v3_strategy import extract_signals_single
        sigs = extract_signals_single(code, start_date, today, p)
        all_signals.extend(sigs)

    # Get today's only
    today_sigs = [s for s in all_signals if str(s['date'])[:10] == today]
    print(f"Found {len(today_sigs)} signals for {today}")

    if not today_sigs:
        print("No B1 signals today.")
        return

    # Score + rank
    by_date = defaultdict(list)
    for s in today_sigs:
        by_date[s['date']].append(s)
    filtered = filter_and_rank(by_date, p)
    ranked = filtered.get(pd.Timestamp(today), [])

    # Build report
    lines = [f"## B1 V3 Daily Picks ({today})", ""]
    for i, s in enumerate(ranked[:10], 1):
        code = s['code']
        name = _get_stock_name(code)
        washout = "W" if s.get('is_washout') else ""
        super_b1 = "S" if s.get('is_super_b1') else ""
        tags = "/".join(filter(None, [washout, super_b1]))
        lines.append(f"{i}. **{code}** {name} | score:{s['quality_score']:.1f} "
                     f"J:{s['J']:.1f} | {tags}")
    lines.append(f"\nTotal candidates today: {len(ranked)}")

    # DingTalk notify
    try:
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
        dd = cfg['dingtalk']
        n = DingTalkNotifier(webhook_url=dd['webhook_url'], secret=dd['secret'])
        n.send_markdown(f'B1 V3 Daily ({today})', '\n'.join(lines))
        print("DingTalk sent OK")
    except Exception as e:
        print(f"DingTalk failed: {e}")
        print('\n'.join(lines))


def _get_stock_name(code):
    """Get stock name from csv files."""
    import csv
    prefix = code[:2]
    csv_path = Path(f"data/{prefix}/{code}.csv")
    if csv_path.exists():
        try:
            with open(csv_path, 'r', encoding='gbk') as f:
                reader = csv.reader(f)
                next(reader)  # header
                row = next(reader)
                return row[2] if len(row) > 2 else ""
        except:
            pass
    return ""


# ============================================================
# BACKTEST MODE
# ============================================================

def cmd_backtest(args):
    """Single backtest with given parameters."""
    p = B1V3Params()

    # Apply CLI overrides
    if args.j_max is not None: p.j_max = args.j_max
    if args.j_min is not None: p.j_min = args.j_min
    if args.vol_mode: p.vol_shrink_mode = args.vol_mode
    if args.vol_peak is not None: p.vol_vs_wave_peak_max = args.vol_peak
    if args.vol_ma5 is not None: p.vol_ratio_ma5_max = args.vol_ma5
    if args.turnover is not None: p.turnover_max = args.turnover
    if args.pe_max is not None: p.pe_max = args.pe_max
    if args.pb_max is not None: p.pb_max = args.pb_max
    if args.cs_shadow is not None: p.cs_shadow_max = args.cs_shadow
    if args.top_n is not None: p.top_n_per_day = args.top_n
    if args.wave_qual is not None: p.require_wave_qualified = args.wave_qual
    if args.wave_health is not None: p.require_wave_healthy = args.wave_health
    if args.wave_break is not None: p.require_no_wave_break = args.wave_break
    if args.washout is not None: p.washout_enabled = args.washout

    # Apply factor toggles
    for fac_name in dir(p):
        if fac_name.startswith('q_') and hasattr(args, fac_name) and getattr(args, fac_name) is not None:
            setattr(p, fac_name, getattr(args, fac_name))

    # Apply module params (map CLI arg names to B1V3Params attribute names)
    cli_to_param = {
        'wave_max_gain': 'wave_max_gain_pct',
        'wave_max_turnover': 'wave_max_turnover_sum',
        'wave_red_green_ratio': 'wave_red_green_vol_ratio',
        'wave_health_ratio': 'wave_health_surge_vol_ratio',
        'surge_min_gain': 'surge_min_gain_pct',
        'wave_break_width': 'wave_break_stop_width',
        'group_gap': 'group_forward_gap_max',
        'group_back_gap': 'group_back_merge_gap_max',
    }
    for cli_name, param_name in cli_to_param.items():
        if hasattr(args, cli_name) and getattr(args, cli_name) is not None:
            setattr(p, param_name, getattr(args, cli_name))

    print(f"Params: J=[{p.j_min},{p.j_max}], vol={p.vol_shrink_mode}, "
          f"top_n={p.top_n_per_day}, PE<={p.pe_max}, PB<={p.pb_max}")
    print(f"Wave: qual={p.require_wave_qualified}, health={p.require_wave_healthy}, "
          f"break={p.require_no_wave_break}, washout={p.washout_enabled}")

    codes = get_stock_list(args.max_stocks)
    print(f"Stocks: {len(codes)} | {args.start} to {args.end}")

    # Phase 0: extract raw signals
    t0 = time.time()
    by_date, _ = build_raw_cache(codes, args.start, args.end, p)
    total_raw = sum(len(v) for v in by_date.values())
    print(f"  Phase 0: {time.time()-t0:.0f}s | {total_raw} raw signals across {len(by_date)} days")

    # Phase 1: filter + rank
    t1 = time.time()
    filtered = filter_and_rank(by_date, p)
    total_filt = sum(len(v) for v in filtered.values())
    print(f"  Phase 1: {time.time()-t1:.0f}s | {total_filt} filtered signals across {len(filtered)} days")

    # Phase 2: simulate
    t2 = time.time()
    eq_df, trades_df = simulate(filtered, p)
    print(f"  Phase 2: {time.time()-t2:.0f}s | {len(trades_df)} trades")

    # Metrics
    m = calc_metrics(trades_df, eq_df, p.initial_capital)
    bc = compare_senior(m)

    print(f"\n  Return:{m['total_return']:7.1f}%  WR:{m['win_rate']:5.1f}%  "
          f"DD:{m['max_drawdown']:5.1f}%  Sharpe:{m['sharpe']:5.2f}  "
          f"PF:{m['profit_factor']:4.1f}  T:{m['total_trades']}")
    print(f"  AvgW:{m.get('avg_win',0):5.2f}%  AvgL:{m.get('avg_loss',0):5.2f}%  "
          f"WLR:{m.get('wl_ratio',0):4.1f}  HoldW:{m.get('avg_hold_win',0):4.0f}d  "
          f"HoldL:{m.get('avg_hold_loss',0):4.0f}d")
    print(f"  Score: {m['score']:.1f}")

    for k, v in bc.items():
        if k == 'composite':
            print(f"  [Composite: {v['pct']:.0f}% >= senior, dominates={v['dominates']}]")
        else:
            icon = '>' if v['status'] == 'WIN' else '<'
            print(f"  vs {k}: {v['current']:.2f} {icon} {v['target']:.2f} ({v['delta']:+.2f})")

    # Save
    trades_df.to_csv("backtest_trades_v3.csv", index=False, encoding='utf-8-sig')
    eq_df.to_csv("backtest_equity_v3.csv", index=False, encoding='utf-8-sig')
    print(f"\n  Trades saved to backtest_trades_v3.csv")

    return m, p


# ============================================================
# SWEEP MODE
# ============================================================

POST_EXTRACTION_SWEEP_PARAMS = frozenset({
    "cs_shadow_max",
    "quality_score_min",
    "top_n_per_day",
    "stop_loss_width",
    "t_plus_3_min_return",
    "exit_break_yellow",
    "exit_break_white",
    "exit_profit_25pct",
    "max_hold_days",
    "exit_ddt",
    "exit_distribution",
    "exit_s1_clear",
    "s1_skip_types",
    "s1_exit_mode",
    "s1_exit_skip",
    "exit_s1_half",
    "max_positions",
    "position_pct",
    "initial_capital",
})


def _is_post_extraction_sweep_param(name):
    if name.startswith("w_"):
        return True
    if name.startswith("q_") and name != "q_pattern_sim":
        return True
    return name in POST_EXTRACTION_SWEEP_PARAMS

def cmd_sweep(args):
    """Grid search over parameter combinations."""
    p = B1V3Params()

    codes = get_stock_list(args.max_stocks)
    print(f"Sweep: {len(codes)} stocks | {args.start} to {args.end}")

    # Parse sweep params from --sweep args
    sweep_params = {}
    if args.sweep:
        for item in args.sweep:
            key, vals_str = item.split('=', 1)
            vals = []
            for v in vals_str.split(','):
                v = v.strip()
                if v.lower() == 'true':
                    vals.append(True)
                elif v.lower() == 'false':
                    vals.append(False)
                else:
                    try:
                        vals.append(float(v) if '.' in v else int(v))
                    except ValueError:
                        vals.append(v)
            sweep_params[key] = vals

    if args.sweep_preset:
        if args.sweep_preset not in SWEEP_PRESETS:
            raise ValueError(f"unknown sweep preset: {args.sweep_preset}")
        preset = SWEEP_PRESETS[args.sweep_preset]
        sweep_params.update(preset)
        print(f"Applied preset: {args.sweep_preset}")

    if not sweep_params:
        print("No sweep params. Use --sweep param=val1,val2,...")
        print("Presets available:", list(SWEEP_PRESETS.keys()))
        return

    valid_param_names = set(B1V3Params.__dataclass_fields__)
    unknown_params = sorted(set(sweep_params) - valid_param_names)
    if unknown_params:
        raise ValueError(
            "unknown B1 V3 parameter(s): " + ", ".join(unknown_params)
        )

    print(f"Sweep params: {list(sweep_params.keys())}")
    for k, v in sweep_params.items():
        print(f"  {k}: {v}")

    # Generate combos
    keys = list(sweep_params.keys())
    combos = list(itertools.product(*sweep_params.values()))
    print(f"Total combinations: {len(combos)}")

    results = []
    best_score = -1e9
    best_result = None
    raw_cache_by_extraction_params = {}

    for combo_idx, combo in enumerate(combos):
        # Apply combo to params
        p_sweep = B1V3Params()
        for k in dir(p_sweep):
            val = getattr(p, k)
            if not callable(val) and not k.startswith('__'):
                setattr(p_sweep, k, val)
        for key, val in zip(keys, combo):
            setattr(p_sweep, key, val)

        label = ','.join(f"{k}={v}" for k, v in zip(keys, combo))

        extraction_values = tuple(
            (key, val)
            for key, val in zip(keys, combo)
            if not _is_post_extraction_sweep_param(key)
        )
        if extraction_values not in raw_cache_by_extraction_params:
            raw_params = B1V3Params(**vars(p))
            for key, val in extraction_values:
                setattr(raw_params, key, val)
            t0 = time.time()
            by_date, _ = build_raw_cache(codes, args.start, args.end, raw_params)
            raw_cache_by_extraction_params[extraction_values] = by_date
            total_raw = sum(len(v) for v in by_date.values())
            print(f"  Phase 0: {time.time()-t0:.0f}s | {total_raw} raw signals")
        else:
            by_date = raw_cache_by_extraction_params[extraction_values]

        # Filter + rank (fast)
        filtered = filter_and_rank(by_date, p_sweep)
        total_filt = sum(len(v) for v in filtered.values())

        if total_filt < 50:
            print(f"  [{combo_idx+1}/{len(combos)}] {label} -> {total_filt} signals (SKIP)")
            results.append({'label': label, 'total_return': 0, 'win_rate': 0,
                           'max_drawdown': 0, 'sharpe': 0, 'profit_factor': 0,
                           'total_trades': 0, 'score': -1e9})
            continue

        # Simulate
        eq_df, trades_df = simulate(filtered, p_sweep)
        m = calc_metrics(trades_df, eq_df, p_sweep.initial_capital)

        print(f"  [{combo_idx+1}/{len(combos)}] {label} -> "
              f"Ret:{m['total_return']:.1f}% WR:{m['win_rate']:.1f}% "
              f"DD:{m['max_drawdown']:.1f}% T:{m['total_trades']} "
              f"Score:{m['score']:.1f}")

        result = {
            'label': label,
            'total_return': m['total_return'],
            'win_rate': m['win_rate'],
            'max_drawdown': m['max_drawdown'],
            'sharpe': m['sharpe'],
            'profit_factor': m['profit_factor'],
            'total_trades': m['total_trades'],
            'score': m['score'],
        }
        results.append(result)

        if m['score'] > best_score:
            best_score = m['score']
            best_result = {**result, 'params': {k: v for k, v in zip(keys, combo)}}

    # Summary
    print(f"\n{'='*60}")
    print(f"SWEEP RESULTS (sorted by score)")
    print(f"{'='*60}")
    results.sort(key=lambda x: x['score'], reverse=True)
    for i, r in enumerate(results[:20]):
        print(f"  {i+1}. {r['label']} -> Ret:{r['total_return']:.1f}% "
              f"WR:{r['win_rate']:.1f}% DD:{r['max_drawdown']:.1f}% "
              f"T:{r['total_trades']} Score:{r['score']:.1f}")

    if best_result:
        print(f"\n  Best: {best_result['label']}")
        print(f"  Return:{best_result['total_return']:.1f}% WR:{best_result['win_rate']:.1f}% "
              f"DD:{best_result['max_drawdown']:.1f}% Score:{best_result['score']:.1f}")

    # Save results
    pd.DataFrame(results).to_csv("sweep_results_v3.csv", index=False, encoding='utf-8-sig')
    print(f"\n  Results saved to sweep_results_v3.csv")

    return results


# ============================================================
# FACTOR TEST MODE
# ============================================================

def cmd_factor_test(args):
    """Test individual factors: enable one at a time, compare vs baseline."""
    p = B1V3Params()
    codes = get_stock_list(args.max_stocks)

    print(f"Factor test: {len(codes)} stocks | {args.start} to {args.end}")

    # Phase 0: raw signals (shared)
    t0 = time.time()
    by_date, _ = build_raw_cache(codes, args.start, args.end, p)
    total_raw = sum(len(v) for v in by_date.values())
    print(f"  Phase 0: {time.time()-t0:.0f}s | {total_raw} raw signals")

    # Baseline
    filtered_base = filter_and_rank(by_date, p)
    eq_base, trades_base = simulate(filtered_base, p)
    m_base = calc_metrics(trades_base, eq_base, p.initial_capital)
    print(f"\n  BASELINE: Ret={m_base['total_return']:.1f}% WR={m_base['win_rate']:.1f}% "
          f"DD={m_base['max_drawdown']:.1f}% T={m_base['total_trades']} "
          f"Score={m_base['score']:.1f}")

    # Get all factor params (q_* booleans)
    fac_list = build_fac_list(p)
    # Only test currently-OFF factors
    off_factors = [(name, group) for name, _, _, enabled, group in fac_list if not enabled]

    if args.factor:
        off_factors = [(n, g) for n, g in off_factors if n == args.factor or n.startswith(args.factor)]

    print(f"\n  Testing {len(off_factors)} OFF factors one at a time...\n")

    results = []
    for fac_name, group in off_factors:
        p_test = B1V3Params()
        # Copy all base params
        for k in dir(p_test):
            if not k.startswith('__') and not callable(getattr(p_test, k)):
                setattr(p_test, k, getattr(p, k))
        # Enable this one factor
        setattr(p_test, fac_name, True)

        filtered = filter_and_rank(by_date, p_test)
        eq_df, trades_df = simulate(filtered, p_test)
        m = calc_metrics(trades_df, eq_df, p_test.initial_capital)

        delta_ret = m['total_return'] - m_base['total_return']
        delta_wr = m['win_rate'] - m_base['win_rate']
        print(f"  {fac_name:<25s} [{group}] "
              f"Ret:{m['total_return']:7.1f}% ({delta_ret:+6.1f}) "
              f"WR:{m['win_rate']:5.1f}% ({delta_wr:+5.1f}) "
              f"T:{m['total_trades']:5d} Score:{m['score']:6.1f}")

        results.append({
            'factor': fac_name,
            'group': group,
            'total_return': m['total_return'],
            'delta_return': delta_ret,
            'win_rate': m['win_rate'],
            'delta_wr': delta_wr,
            'max_drawdown': m['max_drawdown'],
            'sharpe': m['sharpe'],
            'total_trades': m['total_trades'],
            'score': m['score'],
        })

    # Summary
    results.sort(key=lambda x: x['score'], reverse=True)
    print(f"\n  ---- TOP FACTORS ----")
    for i, r in enumerate(results[:10]):
        print(f"  {i+1}. {r['factor']:<25s} dRet={r['delta_return']:+6.1f}% "
              f"dWR={r['delta_wr']:+5.1f}% Score={r['score']:.1f}")

    pd.DataFrame(results).to_csv("factor_test_results_v3.csv", index=False, encoding='utf-8-sig')
    print(f"\n  Results saved to factor_test_results_v3.csv")

    return results


# ============================================================
# CLI
# ============================================================

def main():
    ap = argparse.ArgumentParser(description='B1 V3 Unified Strategy')
    ap.add_argument('command', nargs='?', default='backtest',
                    choices=['select', 'backtest', 'sweep', 'factor_test', 'factors'])
    ap.add_argument('--start', default='2021-01-01')
    ap.add_argument('--end', default='2026-05-30')
    ap.add_argument('--max-stocks', type=int, default=0)

    # Threshold overrides
    ap.add_argument('--j-max', type=float)
    ap.add_argument('--j-min', type=float)
    ap.add_argument('--vol-mode', choices=['v1', 'v2', 'both', 'either'])
    ap.add_argument('--vol-peak', type=float)
    ap.add_argument('--vol-ma5', type=float)
    ap.add_argument('--turnover', type=float)
    ap.add_argument('--pe-max', type=float)
    ap.add_argument('--pb-max', type=float)
    ap.add_argument('--cs-shadow', type=float)
    ap.add_argument('--top-n', type=int)

    # Module switches
    ap.add_argument('--wave-qual', type=lambda x: x.lower() == 'true')
    ap.add_argument('--wave-health', type=lambda x: x.lower() == 'true')
    ap.add_argument('--wave-break', type=lambda x: x.lower() == 'true')
    ap.add_argument('--washout', type=lambda x: x.lower() == 'true')

    # Module params
    ap.add_argument('--wave-max-gain', type=float)
    ap.add_argument('--wave-max-turnover', type=float)
    ap.add_argument('--wave-red-green-ratio', type=float)
    ap.add_argument('--wave-health-ratio', type=float)
    ap.add_argument('--surge-min-gain', type=float)
    ap.add_argument('--wave-break-width', type=float)
    ap.add_argument('--group-gap', type=int)
    ap.add_argument('--group-back-gap', type=int)

    # Sweep
    ap.add_argument('--sweep', action='append', help='param=val1,val2,...')
    ap.add_argument('--sweep-preset', help='Use a predefined sweep preset')

    # Factor test
    ap.add_argument('--factor', help='Specific factor to test')

    args = ap.parse_args()

    if args.command == 'select':
        cmd_select(args)
    elif args.command == 'backtest':
        cmd_backtest(args)
    elif args.command == 'sweep':
        cmd_sweep(args)
    elif args.command == 'factor_test':
        cmd_factor_test(args)
    elif args.command == 'factors':
        from strategy.b1_v3_config import B1V3Params, build_fac_list, count_factors_by_group
        p = B1V3Params()
        fac = build_fac_list(p)
        counts = count_factors_by_group(fac)
        print(f"Total factors: {len(fac)}")
        for g in sorted(counts):
            group_facs = [(n, 'ON' if e else 'OFF', w) for n, _, w, e, gr in fac if gr == g]
            on_count = sum(1 for _, s, _ in group_facs if s == 'ON')
            print(f"  {g}: {len(group_facs)} factors ({on_count} ON)")
            for n, s, w in group_facs:
                print(f"    {n:<25s} w={w:4.1f} [{s}]")


if __name__ == "__main__":
    main()
