# -*- coding: utf-8 -*-
"""
Brick chart strategy V2 backtest.
Two scoring methods:
  A: daily percentile + 4-dimension weighted sum
  B: rolling Z-score + IC weighted sum
Both rank daily and pick top N.
"""

import sys, argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

from strategy.brick_chart_strategy import BrickChartStrategy
from utils.market_timing import MarketTiming

# ---- Config ----
DATA_DIR = Path('data')
PREFIXES = ['00', '30', '60', '68']
ACTIVE_CAP_PATH = 'data/market/active_cap.csv'
DEFAULT_OUTPUT_DIR = Path('artifacts/backtests/brick')


def _output_path(args, filename):
    output_dir = Path(getattr(args, 'output_dir', DEFAULT_OUTPUT_DIR))
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / filename

# ---- Feature list (25 unique features) ----
FEATURES = [
    'red_height', 'brick_slope_3d', 'brick_slope_5d', 'brick_value', 'red_green_ratio',
    'rsi_6', 'rsi_14', 'bb_pct_b', 'wr_14', 'close_to_yellow_pct',
    'close_to_ma5_pct', 'close_to_ma10_pct', 'close_to_ma20_pct', 'close_to_ma60_pct',
    'close_to_white_pct', 'ret_5d', 'ret_10d', 'bullish_ratio_5d', 'bullish_ratio_10d',
    'new_high_20d',
    'obv_trend_up', 'vol_ratio_5', 'macd_hist_rising', 'turnover_ratio_5', 'vol_ratio_20',
]
RAW_SIGNAL_COLUMNS = [
    'code', 'entry_date', 'entry_price', 'exit_date', 'exit_price',
    'return_pct', 'hold_days', *FEATURES,
    'overnight_gap_pct', 'entry_open_to_yellow_pct', 'entry_open_to_ma5_pct',
]

# Method A: dimension weights (from Cohen's d analysis)
DIM_INTRA = {
    'brick': {'red_height': 0.322, 'brick_slope_3d': 0.247, 'brick_slope_5d': 0.137,
              'brick_value': 0.115, 'red_green_ratio': 0.179},
    'position': {'rsi_6': 0.281, 'rsi_14': 0.220, 'bb_pct_b': 0.203,
                 'wr_14': 0.189, 'close_to_yellow_pct': 0.107},
    'trend': {'close_to_ma5_pct': 0.172, 'close_to_ma10_pct': 0.130,
              'close_to_ma20_pct': 0.095, 'close_to_ma60_pct': 0.059,
              'close_to_white_pct': 0.105, 'ret_5d': 0.117, 'ret_10d': 0.075,
              'bullish_ratio_5d': 0.108, 'bullish_ratio_10d': 0.075, 'new_high_20d': 0.063},
    'volume': {'obv_trend_up': 0.291, 'vol_ratio_5': 0.213, 'macd_hist_rising': 0.191,
               'turnover_ratio_5': 0.158, 'vol_ratio_20': 0.147},
}
DIM_INTER = {'brick': 0.275, 'position': 0.284, 'trend': 0.244, 'volume': 0.198}

# Method B: Profit-based weights (Top20% - Bottom20% daily return spread)
IC_W = {
    'close_to_ma5_pct': 0.073, 'red_height': 0.067, 'rsi_6': 0.066,
    'red_green_ratio': 0.064, 'close_to_ma10_pct': 0.056, 'rsi_14': 0.054,
    'ret_5d': 0.053, 'wr_14': 0.053, 'bb_pct_b': 0.052, 'brick_slope_3d': 0.050,
    'close_to_white_pct': 0.046, 'close_to_ma20_pct': 0.041, 'close_to_yellow_pct': 0.038,
    'ret_10d': 0.034, 'vol_ratio_5': 0.034, 'bullish_ratio_5d': 0.032,
    'close_to_ma60_pct': 0.030, 'turnover_ratio_5': 0.030, 'brick_slope_5d': 0.028,
    'vol_ratio_20': 0.023, 'brick_value': 0.021, 'bullish_ratio_10d': 0.020,
    'macd_hist_rising': 0.018, 'obv_trend_up': 0.015, 'new_high_20d': 0.010,
}

# ---- Limit board helpers ----
def _limit_pct(code):
    return 0.20 if str(code).startswith(('30', '68')) else 0.10

def _one_word_limit_up(code, o, c, pc):
    if pc <= 0: return False
    lp = _limit_pct(code)
    return (c - pc) / pc >= lp * 0.99 and abs(o - c) / pc < 0.001

def _one_word_limit_down(code, o, c, pc):
    if pc <= 0: return False
    lp = _limit_pct(code)
    return (c - pc) / pc <= -lp * 0.99 and abs(o - c) / pc < 0.001

def _open_limit_up(code, o, pc):
    if pc <= 0: return False
    return (o - pc) / pc >= _limit_pct(code) * 0.99

# ---- Extra indicators (from research/brick/legacy/v1/analyze_brick_v1.py) ----
def _compute_extra(df):
    """Add MACD, RSI, BB, WR, OBV, ATR, returns, MAs to an ascending DataFrame."""
    c = df['close'].values.astype(float)
    h = df['high'].values.astype(float)
    l = df['low'].values.astype(float)
    v = df['volume'].values.astype(float)
    n = len(c)

    ema12 = pd.Series(c).ewm(span=12, adjust=False, min_periods=1).mean().values
    ema26 = pd.Series(c).ewm(span=26, adjust=False, min_periods=1).mean().values
    dif = ema12 - ema26
    dea = pd.Series(dif).ewm(span=9, adjust=False, min_periods=1).mean().values
    df['macd_dif'] = dif
    df['macd_dea'] = dea
    df['macd_hist'] = (dif - dea) * 2
    df['macd_hist_rising'] = pd.Series(df['macd_hist'].values).diff(3) > 0

    delta = np.diff(c, prepend=c[0])
    gain, loss = np.where(delta > 0, delta, 0), np.where(delta < 0, -delta, 0)
    for p in [6, 14]:
        ag = pd.Series(gain).ewm(span=p, adjust=False, min_periods=1).mean().values
        al = pd.Series(loss).ewm(span=p, adjust=False, min_periods=1).mean().values
        df[f'rsi_{p}'] = 100 - 100 / (1 + ag / np.where(al == 0, 1e-9, al))

    ma20 = pd.Series(c).rolling(20, min_periods=1).mean().values
    std20 = pd.Series(c).rolling(20, min_periods=1).std().values
    df['bb_upper'] = ma20 + 2 * std20
    df['bb_lower'] = ma20 - 2 * std20
    denom_bb = df['bb_upper'] - df['bb_lower']
    df['bb_pct_b'] = np.where(denom_bb > 0, (c - df['bb_lower']) / denom_bb, 0.5)

    hhv14 = pd.Series(h).rolling(14, min_periods=1).max().values
    llv14 = pd.Series(l).rolling(14, min_periods=1).min().values
    wr_denom = hhv14 - llv14
    df['wr_14'] = np.where(wr_denom > 0, (hhv14 - c) / wr_denom * -100, -50)

    obv = np.zeros(n)
    for i in range(1, n):
        if c[i] > c[i-1]: obv[i] = obv[i-1] + v[i]
        elif c[i] < c[i-1]: obv[i] = obv[i-1] - v[i]
        else: obv[i] = obv[i-1]
    df['obv'] = obv
    df['obv_ma5'] = pd.Series(obv).rolling(5, min_periods=1).mean()
    df['obv_trend_up'] = obv > df['obv_ma5'].values

    tr = np.maximum(h - l, np.abs(h - np.roll(c, 1)))
    tr[0] = h[0] - l[0]
    df['atr_10'] = pd.Series(tr).rolling(10, min_periods=1).mean()
    df['atr_20'] = pd.Series(tr).rolling(20, min_periods=1).mean()

    ret_s = pd.Series(c).pct_change()
    df['std_ret_10d'] = ret_s.rolling(10, min_periods=1).std() * 100

    df['ret_5d'] = (c / pd.Series(c).shift(5).values - 1) * 100
    df['ret_10d'] = (c / pd.Series(c).shift(10).values - 1) * 100
    df['ret_20d'] = (c / pd.Series(c).shift(20).values - 1) * 100
    df['ret_60d'] = (c / pd.Series(c).shift(60).values - 1) * 100

    bull = (c > pd.Series(c).shift(1).values).astype(int)
    df['bullish_ratio_5d'] = pd.Series(bull).rolling(5, min_periods=1).mean()
    df['bullish_ratio_10d'] = pd.Series(bull).rolling(10, min_periods=1).mean()

    for p in [5, 10, 20, 60]:
        df[f'ma{p}'] = pd.Series(c).rolling(p, min_periods=1).mean()
    df['new_high_20d'] = pd.Series(c) == pd.Series(h).rolling(20, min_periods=1).max()

    ema10 = pd.Series(c).ewm(span=10, adjust=False, min_periods=1).mean()
    df['white_line'] = ema10.ewm(span=10, adjust=False, min_periods=1).mean()

    df['vol_ma5'] = df['volume'].rolling(5, min_periods=1).mean()
    df['vol_ma20'] = df['volume'].rolling(20, min_periods=1).mean()

    return df


# ---- Feature extraction at signal ----
def _extract_features(result, idx):
    """Extract all feature values from indicator DataFrame at row idx."""
    row = result.iloc[idx]
    rc = float(row['close'])
    ry = float(row['yellow_line'])
    f = {}

    f['red_height'] = float(row['red_height'])
    f['brick_slope_3d'] = (float(row['brick']) - float(result['brick'].iloc[max(0, idx - 3)])) / max(3, 1)
    f['brick_slope_5d'] = (float(row['brick']) - float(result['brick'].iloc[max(0, idx - 5)])) / max(5, 1)
    f['brick_value'] = float(row['brick'])
    f['red_green_ratio'] = float(row['red_height']) / max(abs(float(row['green_height'])), 1e-6)

    f['rsi_6'] = float(row['rsi_6'])
    f['rsi_14'] = float(row['rsi_14'])
    f['bb_pct_b'] = float(row['bb_pct_b'])
    f['wr_14'] = float(row['wr_14'])
    f['close_to_yellow_pct'] = (rc - ry) / ry * 100 if ry > 0 else 0

    for p in [5, 10, 20, 60]:
        ma = float(row[f'ma{p}'])
        f[f'close_to_ma{p}_pct'] = (rc - ma) / ma * 100 if ma > 0 else 0

    f['close_to_white_pct'] = (rc - float(row['white_line'])) / float(row['white_line']) * 100 if float(row['white_line']) > 0 else 0
    f['ret_5d'] = float(row['ret_5d'])
    f['ret_10d'] = float(row['ret_10d'])
    f['bullish_ratio_5d'] = float(row['bullish_ratio_5d'])
    f['bullish_ratio_10d'] = float(row['bullish_ratio_10d'])
    f['new_high_20d'] = bool(row['new_high_20d'])

    f['obv_trend_up'] = bool(float(row['obv']) > float(row['obv_ma5']))
    f['vol_ratio_5'] = float(row['volume']) / max(float(row['vol_ma5']), 1)
    f['vol_ratio_20'] = float(row['volume']) / max(float(row['vol_ma20']), 1)
    f['macd_hist_rising'] = bool(row['macd_hist_rising'])
    f['turnover_ratio_5'] = float(row.get('turnover', 0)) / max(float(row.get('turnover_ma5', 1)), 1e-6)

    return f


# ---- Phase 1: signal collection ----
def collect_signals(args_tuple):
    csv_path, bullish_dates, pre_start, start_date, end_date = args_tuple

    try:
        df = pd.read_csv(csv_path, encoding='gbk')
    except Exception:
        return []

    if len(df) < 120 or 'date' not in df.columns:
        return []

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date']).sort_values('date')
    df = df[df['date'] >= pre_start]
    if len(df) < 60:
        return []

    code = Path(csv_path).stem
    s = BrickChartStrategy()
    try:
        result = s.calculate_indicators(df)
    except Exception:
        return []
    if result is None or result.empty:
        return []

    if 'turnover' in df.columns and len(df) >= len(result):
        result['turnover'] = df['turnover'].values[:len(result)]
    else:
        result['turnover'] = 0
    result['turnover_ma5'] = result['turnover'].rolling(5, min_periods=1).mean()

    result = _compute_extra(result)

    brick = result['brick'].values
    close = result['close'].values
    open_ = result['open'].values
    dates = result['date'].values
    n = len(result)
    signals = []

    i = 2
    while i < n:
        if not result['brick_signal'].iloc[i]:
            i += 1
            continue

        sig_date = str(dates[i])[:10]
        # Date range filter (after scanning full indicator data)
        if sig_date < start_date:
            i += 1
            continue
        if end_date and sig_date > end_date:
            i += 1
            continue
        if bullish_dates is not None and sig_date not in bullish_dates:
            i += 1
            continue

        # Entry: next day open
        entry_i = i + 1
        entry_ok = entry_i < n and not _open_limit_up(code, open_[entry_i], close[entry_i - 1])

        if entry_ok:
            entry_price = open_[entry_i]
            entry_date = str(dates[entry_i])[:10]
        else:
            # Signal at data boundary: use signal-day close as placeholder
            entry_price = close[i]
            entry_date = str(dates[i])[:10]

        # Find exit: first green brick after entry
        exit_i = None
        exit_price = close[i]  # placeholder
        for j in range(max(entry_i, i + 1), n):
            if brick[j] < brick[j - 1]:
                exit_i = j
                while exit_i < n and _one_word_limit_down(code, open_[exit_i], close[exit_i], close[exit_i - 1]):
                    exit_i += 1
                if exit_i >= n:
                    exit_i = None
                break

        ret_pct = 0.0
        if exit_i is not None and entry_ok:
            exit_price = close[exit_i]
            ret_pct = (exit_price - entry_price) / entry_price * 100

        # Core features at signal day
        feats = _extract_features(result, i)

        # Overnight features (use entry_i if available, else i for placeholder)
        feat_idx = entry_i if entry_ok else i
        erow = result.iloc[feat_idx]
        e_open = float(erow['open'])
        e_yellow = float(erow['yellow_line'])
        e_ma5 = float(erow['ma5'])
        sig_close = float(result.iloc[i]['close'])
        feats['overnight_gap_pct'] = (e_open - sig_close) / sig_close * 100 if sig_close > 0 else 0
        feats['entry_open_to_yellow_pct'] = (e_open - e_yellow) / e_yellow * 100 if e_yellow > 0 else 0
        feats['entry_open_to_ma5_pct'] = (e_open - e_ma5) / e_ma5 * 100 if e_ma5 > 0 else 0

        signals.append({
            'code': code,
            'entry_date': entry_date,
            'entry_price': round(entry_price, 2),
            'exit_date': str(dates[exit_i])[:10] if exit_i else str(dates[i])[:10],
            'exit_price': round(exit_price, 2),
            'return_pct': round(ret_pct, 2),
            'hold_days': (exit_i - entry_i) if exit_i else 0,
            **feats,
        })
        i += 1

    return signals


# ---- Phase 2: scoring ----
def score_method_a(df):
    """Daily percentile -> 4-dimension -> weighted sum."""
    for f in FEATURES:
        if f in df.columns:
            df[f'_p_{f}'] = df.groupby('entry_date')[f].rank(pct=True)

    for dim, intra in DIM_INTRA.items():
        s = pd.Series(0., index=df.index)
        total = 0.0
        for f, w in intra.items():
            col = f'_p_{f}'
            if col in df.columns:
                s += df[col].fillna(0.5) * w
                total += w
        df[f'_dim_{dim}'] = s / total if total > 0 else s

    df['score'] = sum(df[f'_dim_{d}'] * DIM_INTER[d] for d in DIM_INTER)
    df['score'] = df['score'].fillna(0.5)
    return df


def score_method_b(df, window=60):
    """Rolling 60-day Z-score -> IC weighted sum."""
    df = df.sort_values('entry_date').reset_index(drop=True)
    unique_dates = sorted(df['entry_date'].unique())

    for f in FEATURES:
        if f not in df.columns:
            continue
        zcol = f'_z_{f}'
        df[zcol] = 0.0
        for i, today in enumerate(unique_dates):
            tm = df['entry_date'] == today
            ws = max(0, i - window)
            wd = unique_dates[ws:i]
            if len(wd) < 10:
                continue
            hist = df[df['entry_date'].isin(wd)][f].dropna()
            if len(hist) < 10:
                continue
            mu, sigma = hist.mean(), hist.std()
            if sigma > 0:
                df.loc[tm, zcol] = (df.loc[tm, f].values - mu) / sigma

    df['score'] = 0.0
    total_ic = sum(IC_W.values())
    for f, ic in IC_W.items():
        zcol = f'_z_{f}'
        if zcol in df.columns:
            df['score'] += df[zcol].fillna(0) * ic / total_ic
    df['score'] = df['score'].fillna(0)
    return df


FEATS_ML = [
    'red_height','brick_slope_3d','brick_slope_5d','brick_value','red_green_ratio',
    'rsi_6','rsi_14','bb_pct_b','wr_14','close_to_yellow_pct',
    'close_to_ma5_pct','close_to_ma10_pct','close_to_ma20_pct','close_to_ma60_pct',
    'close_to_white_pct','ret_5d','ret_10d','bullish_ratio_5d','bullish_ratio_10d',
    'new_high_20d','obv_trend_up','vol_ratio_5','macd_hist_rising',
    'turnover_ratio_5','vol_ratio_20',
    'overnight_gap_pct','entry_open_to_yellow_pct','entry_open_to_ma5_pct',
]


def score_method_ml(df, model_path='models/brick/v2/ml_ranker_model.pkl',
                    scaler_path='models/brick/v2/ml_ranker_scaler.pkl'):
    import joblib
    ranker = joblib.load(model_path)
    scaler = joblib.load(scaler_path)
    feats = [f for f in FEATS_ML if f in df.columns]
    X = df[feats].fillna(0)
    X_s = scaler.transform(X)
    df = df.copy()
    df['score'] = ranker.predict(X_s)
    return df


def rank_and_trim(df, top_n):
    df = df.copy()
    df['_rank'] = df.groupby('entry_date')['score'].rank(ascending=False, method='first')
    return df[df['_rank'] <= top_n]


def save_raw_signals(signals, path):
    """Always publish a fresh raw-signal artifact, including zero-signal days."""
    raw_path = Path(path)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    frame = (
        pd.DataFrame(signals)
        if signals
        else pd.DataFrame(columns=RAW_SIGNAL_COLUMNS)
    )
    if "code" in frame.columns:
        frame["code"] = (
            frame["code"]
            .astype("string")
            .str.replace(r"\.0$", "", regex=True)
            .str.zfill(6)
        )
    # Keep the non-empty and zero-signal artifacts on one stable schema/order;
    # downstream readers should not depend on dict insertion order.
    frame = frame.reindex(columns=RAW_SIGNAL_COLUMNS)
    frame.to_csv(raw_path, index=False, encoding='gbk')
    return len(frame)


# ---- Main ----
def main():
    ap = argparse.ArgumentParser(description='Brick V2 backtest')
    ap.add_argument('--method', type=str, default='ml', choices=['A', 'B', 'ml'],
                    help='Scoring: A=percentile+dim, B=Z-score+IC, ml=LightGBM (default)')
    ap.add_argument('--top-n', type=int, default=3, help='Daily top N (default 3)')
    ap.add_argument('--start', type=str, default='2024-01-01')
    ap.add_argument('--end', type=str, default=None)
    ap.add_argument('--market-timing', type=str, default=None)
    ap.add_argument('--no-timing', action='store_true')
    ap.add_argument('--save-raw', type=str, default=None,
                    help='Save ALL raw signals (with signal-day features) to CSV, skip Phase 2')
    ap.add_argument('--output-dir', type=str, default=str(DEFAULT_OUTPUT_DIR),
                    help='Directory for generated trades/NAV CSV files')
    ap.add_argument('--max-per-ind', type=int, default=999,
                    help='Max stocks per industry block (default=999=none)')
    ap.add_argument('--softmax', action='store_true', help='Softmax weight top N')
    ap.add_argument('--min-score', type=float, default=None,
                    help='Min prediction score for confidence filter')
    ap.add_argument('--account', action='store_true',
                    help='Enable account-level NAV backtest')
    ap.add_argument('--commission', type=float, default=3,
                    help='Commission in bp (default 3 = 万3)')
    ap.add_argument('--stamp', type=float, default=0.05,
                    help='Stamp tax in %% (default 0.05)')
    ap.add_argument('--slippage', type=float, default=0.1,
                    help='Slippage in %% (default 0.1)')
    args = ap.parse_args()

    start_date = args.start
    end_date = args.end
    pre_dt = pd.to_datetime(start_date) - pd.DateOffset(months=7)
    pre_start = pre_dt.strftime('%Y-%m-%d')

    use_timing = not args.no_timing
    timing_path = Path(args.market_timing or ACTIVE_CAP_PATH)
    method_map = {'A': 'percentile+dim', 'B': 'Z-score+IC', 'ml': 'LightGBM'}
    method_name = method_map.get(args.method, 'LightGBM')

    print('=' * 60)
    label = f'Brick V2 [{method_name}] Top{args.top_n}'
    if use_timing:
        label += ' +timing'
    print(label)
    print('=' * 60)
    print(f'Period: {start_date} ~ {end_date or "now"}')
    print(f'Method: {method_name}')

    bullish_dates = None
    if use_timing and timing_path.exists():
        mt = MarketTiming(str(timing_path))
        bullish_dates = set()
        for d, s in mt.states.items():
            if s == 'bullish':
                bullish_dates.add(str(d)[:10])
        print(f'Bullish days: {len(bullish_dates)}')
    elif use_timing:
        print('[WARN] timing file not found, running without')
        use_timing = False

    csv_files = []
    for prefix in PREFIXES:
        d = DATA_DIR / prefix
        if d.exists():
            csv_files.extend(d.glob('*.csv'))
    print(f'Stock files: {len(csv_files)}')

    nw = max(1, cpu_count() - 1)
    print(f'Workers: {nw}')
    print('Phase 1: scanning all stocks...')

    pool_args = [(str(f), bullish_dates, pre_start, start_date, end_date) for f in csv_files]
    all_sigs = []
    done = 0
    total = len(csv_files)
    t0 = datetime.now()

    with Pool(nw) as pool:
        for sigs in pool.imap_unordered(collect_signals, pool_args, chunksize=20):
            if sigs:
                all_sigs.extend(sigs)
            done += 1
            if done % 500 == 0 or done == total:
                elapsed = (datetime.now() - t0).total_seconds()
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                print(f'  [{done}/{total}] signals: {len(all_sigs)}  rate: {rate:.1f}/s  eta: {eta:.0f}s')

    t1 = (datetime.now() - t0).total_seconds()
    print(f'Phase 1 done: {t1:.0f}s, raw signals: {len(all_sigs)}')

    if not all_sigs:
        print('No signals!')
        if args.save_raw:
            count = save_raw_signals([], args.save_raw)
            print('Raw signals saved: {} ({})'.format(args.save_raw, count))
        return

    # Save raw signals (signal-day features only)
    if args.save_raw:
        count = save_raw_signals(all_sigs, args.save_raw)
        print('Raw signals saved: {} ({})'.format(args.save_raw, count))
        return

    print('Phase 2: scoring + ranking...')
    df_raw = pd.DataFrame(all_sigs)
    df_raw['entry_date'] = pd.to_datetime(df_raw['entry_date'])

    if args.method == 'A':
        df_scored = score_method_a(df_raw)
    elif args.method == 'B':
        df_scored = score_method_b(df_raw)
    else:
        df_scored = score_method_ml(df_raw)

    # Confidence filter
    if args.min_score is not None:
        before = len(df_scored)
        df_scored = df_scored[df_scored['score'] >= args.min_score].copy()
        print('Confidence filter (score>={}): {} -> {} signals'.format(
            args.min_score, before, len(df_scored)))

    df_final = rank_and_trim(df_scored, args.top_n)

    # Softmax weighting: adjust return_pct by score-based weight
    if args.softmax and len(df_final) > 0:
        for date, idx in df_final.groupby('entry_date').groups.items():
            grp_idx = list(idx)
            scores = df_final.loc[grp_idx, 'score'].values
            w = np.exp(scores - scores.max())
            w = w / w.sum()
            w = w * len(grp_idx)  # normalize so sum = group size
            df_final.loc[grp_idx, 'return_pct'] = df_final.loc[grp_idx, 'return_pct'] * w

    # Industry constraint
    if args.max_per_ind < 999:
        import json
        with open('data/block/ths_industry_map.json', 'r', encoding='utf-8') as f:
            stock_blocks = json.load(f)['stocks']
        filtered = []
        for date, grp in df_final.groupby('entry_date'):
            grp = grp.sort_values('score', ascending=False)
            ind_count = {}
            kept = []
            for _, row in grp.iterrows():
                ind = stock_blocks.get(str(row['code']).zfill(6), ['UNKNOWN'])[0]
                if ind_count.get(ind, 0) >= args.max_per_ind:
                    continue
                kept.append(row.name)
                ind_count[ind] = ind_count.get(ind, 0) + 1
                if len(kept) >= args.top_n:
                    break
            filtered.extend(kept)
        df_final = df_final.loc[filtered]

    daily = df_final.groupby('entry_date').size()
    print(f'Filtered: {len(df_final)} trades, {len(daily)} days, avg {daily.mean():.1f}/day')

    # Stats
    ret = df_final['return_pct']
    wr = (ret > 0).sum() / len(ret) * 100

    print()
    print('=' * 60)
    print(f'V2 [{method_name}] Top{args.top_n} Results')
    print('=' * 60)
    print('Total:        {:6d}'.format(len(df_final)))
    print('Win rate:     {:6.1f}%'.format(wr))
    print('Avg return:   {:+.2f}%'.format(ret.mean()))
    print('Median return:{:+.2f}%'.format(ret.median()))
    print('Max win:      {:+.2f}%'.format(ret.max()))
    print('Max loss:     {:+.2f}%'.format(ret.min()))
    print('Std:          {:6.2f}%'.format(ret.std()))
    print('Avg hold:     {:5.1f}d'.format(df_final['hold_days'].mean()))

    # Monthly
    df_final['emonth'] = df_final['entry_date'].dt.to_period('M')
    monthly = df_final.groupby('emonth').agg(
        N=('return_pct', 'count'),
        WR=('return_pct', lambda x: (x > 0).sum() / len(x) * 100),
        AVG=('return_pct', 'mean')
    ).round(2)

    print()
    print('-- Monthly --')
    for m, row in monthly.iterrows():
        bar = '+' * max(1, int(row['N'] / monthly['N'].max() * 40))
        print('  {}  N:{:4d}  WR:{:5.1f}%  AVG:{:+6.2f}%  {}'.format(
            m, int(row['N']), row['WR'], row['AVG'], bar))

    # Save
    suffix = 'a' if args.method == 'A' else 'b'
    cols_out = [c for c in df_final.columns if not c.startswith('_') and c not in ('score', 'emonth')]
    out = _output_path(args, 'backtest_brick_v2_{}_trades.csv'.format(suffix))
    df_final[cols_out].to_csv(out, index=False, encoding='gbk')
    print()
    print('Saved: {}'.format(out))


    # Account-level NAV tracking
    if args.account:
        print()
        print('=' * 60)
        print('Account-Level NAV Backtest')
        print('=' * 60)
        build_account_nav(df_final, start_date, end_date, args)


def build_account_nav(trades, start_date, end_date, args):
    codes = trades['code'].unique()
    price_cache = {}
    for code in codes:
        padded = str(code).zfill(6)
        prefix = padded[:2]
        csv_path = DATA_DIR / prefix / '{}.csv'.format(padded)
        if not csv_path.exists():
            continue
        try:
            df_p = pd.read_csv(csv_path, encoding='gbk')
            df_p['date'] = pd.to_datetime(df_p['date'])
            df_p = df_p.sort_values('date')
            price_cache[str(code)] = dict(zip(df_p['date'].dt.normalize(), df_p['close']))
        except Exception:
            continue

    trades = trades.copy()
    trades['entry_date'] = pd.to_datetime(trades['entry_date']).dt.normalize()
    trades['exit_date'] = pd.to_datetime(trades['exit_date']).dt.normalize()

    all_dates = pd.date_range(start_date, end_date or trades['exit_date'].max(), freq='B')
    if len(all_dates) == 0:
        all_dates = sorted(set(trades['entry_date']) | set(trades['exit_date']))
    all_dates = [pd.Timestamp(d).normalize() for d in all_dates]

    positions = []
    daily_returns = []

    for today in all_dates:
        new_trades = trades[trades['entry_date'] == today]
        for _, t in new_trades.iterrows():
            positions.append({
                'code': str(t['code']),
                'entry': today,
                'exit': t['exit_date'],
            })
        positions = [p for p in positions if p['exit'] >= today]

        if positions:
            pos_rets = []
            for p in positions:
                cache = price_cache.get(p['code'], {})
                prev_date = (today - pd.Timedelta(days=1)).normalize()
                cur_date = today.normalize()
                prev = cache.get(prev_date, None)
                cur = cache.get(cur_date, None)
                if prev and cur and prev > 0:
                    pos_rets.append((cur - prev) / prev)
            daily_returns.append(np.mean(pos_rets) if pos_rets else 0.0)
        else:
            daily_returns.append(0.0)

    if not daily_returns:
        return

    # Apply costs: deduct from daily return on entry/exit days
    cost_entry = args.commission / 10000.0 + args.slippage / 100.0
    cost_exit = args.commission / 10000.0 + args.stamp / 100.0 + args.slippage / 100.0
    n_positions_per_day = args.top_n  # approximate
    cost_entry_pct = cost_entry / n_positions_per_day  # spread across portfolio
    cost_exit_pct = cost_exit / n_positions_per_day

    date_to_idx = {d: i for i, d in enumerate(all_dates)}
    daily_cost = np.zeros(len(daily_returns))
    for _, t in trades.iterrows():
        ed = pd.Timestamp(t['entry_date']).normalize()
        xd = pd.Timestamp(t['exit_date']).normalize()
        if ed in date_to_idx:
            daily_cost[date_to_idx[ed]] -= cost_entry_pct
        if xd in date_to_idx:
            daily_cost[date_to_idx[xd]] -= cost_exit_pct

    daily_returns = [r + c for r, c in zip(daily_returns, daily_cost)]

    r = np.array(daily_returns)
    nav = (1 + r).cumprod()
    cum_ret = (nav[-1] - 1) * 100
    peak = np.maximum.accumulate(nav)
    dd = (nav - peak) / peak
    mdd = dd.min() * 100
    ann_ret = (1 + cum_ret/100) ** (252 / len(r)) - 1 if cum_ret > -100 else -1
    ann_vol = np.std(r, ddof=1) * np.sqrt(252) if len(r) > 1 else 0
    sharpe = (ann_ret - 0.02) / ann_vol if ann_vol > 0 else 0
    calmar = ann_ret / abs(mdd/100) if mdd < 0 else 0
    daily_wr = (r > 0).sum() / len(r) * 100

    print('Costs: commission={}bp stamp={}% slippage={}%'.format(args.commission/100, args.stamp, args.slippage))
    print('Days: {}  Trades: {}'.format(len(daily_returns), len(trades)))
    print()
    print('CAGR:          {:+.1f}%'.format(ann_ret * 100))
    print('Cum Return:    {:+.1f}%'.format(cum_ret))
    print('Max Drawdown:  {:.1f}%'.format(mdd))
    print('Sharpe Ratio:  {:.3f}'.format(sharpe))
    print('Calmar Ratio:  {:.3f}'.format(calmar))
    print('Daily WR:      {:.1f}%'.format(daily_wr))
    print('Daily Avg Ret: {:+.2f}%'.format(np.mean(r) * 100))

    nav_df = pd.DataFrame({'date': all_dates[:len(daily_returns)], 'nav': nav, 'ret': r})
    out = _output_path(args, 'backtest_brick_nav.csv')
    nav_df.to_csv(out, index=False, encoding='gbk')
    print('NAV saved: {}'.format(out))


if __name__ == '__main__':
    main()
