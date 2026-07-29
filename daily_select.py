# -*- coding: utf-8 -*-
"""
每日选股脚本 — T+1日 9:25 运行
Step 1: 加载 T 日收盘生成的候选信号池 (artifacts/daily/brick/signals_today.csv)
Step 2: 腾讯财经获取所有候选股的开盘价
Step 3: 计算 3 个入场开盘特征
Step 4: ML 打分 + 排名 + 行业约束
Step 5: 输出 Top 5 买入清单
"""

import sys, json, urllib.request, warnings, time, argparse
from pathlib import Path
from datetime import datetime, timedelta, time as dtime
sys.path.insert(0, str(Path(__file__).parent))

import numpy as np, pandas as pd
import joblib
warnings.filterwarnings('ignore')

MARKET_OPEN = dtime(9, 25)
MARKET_CLOSE = dtime(18, 0)


def _get_latest_trading_day(date):
    """Return the latest COMPLETED trading day."""
    today = datetime.now().date()
    try:
        import akshare as ak
        trade_cal = ak.tool_trade_date_hist_sina()
        trade_dates = pd.to_datetime(trade_cal['trade_date']).dt.date
        sorted_dates = sorted(trade_dates, reverse=True)
        effective_date = date
        if date == today and datetime.now().time() < MARKET_CLOSE:
            for d in sorted_dates:
                if d < today:
                    return d.strftime('%Y-%m-%d')
        for d in sorted_dates:
            if d <= effective_date:
                return d.strftime('%Y-%m-%d')
    except Exception:
        pass
    wd = date.weekday()
    if wd == 5:
        date = date - timedelta(days=1)
    elif wd == 6:
        date = date - timedelta(days=2)
    elif wd == 0:
        date = date - timedelta(days=3)
    if date == today and datetime.now().time() < MARKET_CLOSE:
        wd = date.weekday()
        date = date - timedelta(days=3) if wd == 0 else date - timedelta(days=1)
    return date.strftime('%Y-%m-%d')


def _is_market_open():
    """Check if market is currently open (9:25-15:00 on a trading day)."""
    now = datetime.now()
    today = now.date()
    if today.weekday() >= 5:
        return False
    try:
        import akshare as ak
        trade_dates = set(pd.to_datetime(ak.tool_trade_date_hist_sina()['trade_date']).dt.date)
        if today not in trade_dates:
            return False
    except Exception:
        pass
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE

try:
    from mootdx.quotes import Quotes
    MOOTDX = Quotes.factory(market='std')
except Exception:
    MOOTDX = None

# ---- Config ----
SIGNALS_FILE = 'artifacts/daily/brick/signals_today.csv'
MODEL_FILE = 'models/brick/v2/ml_ranker_model.pkl'
SCALER_FILE = 'models/brick/v2/ml_ranker_scaler.pkl'
INDUSTRY_FILE = 'data/block/ths_industry_map.json'
TOP_N = 5
MAX_PER_IND = 2

FEATS_ML = [
    'red_height','brick_slope_3d','brick_slope_5d','brick_value','red_green_ratio',
    'rsi_6','rsi_14','bb_pct_b','wr_14','close_to_yellow_pct',
    'close_to_ma5_pct','close_to_ma10_pct','close_to_ma20_pct','close_to_ma60_pct',
    'close_to_white_pct','ret_5d','ret_10d','bullish_ratio_5d','bullish_ratio_10d',
    'new_high_20d','obv_trend_up','vol_ratio_5','macd_hist_rising',
    'turnover_ratio_5','vol_ratio_20',
    'overnight_gap_pct','entry_open_to_yellow_pct','entry_open_to_ma5_pct',
]


# ---- Step 2: 腾讯财经获取开盘价 ----
def fetch_open_prices(codes):
    """批量获取开盘价。分批请求，每批最多 50 只"""
    codes = [str(c).zfill(6) for c in codes]
    opens = {}
    for i in range(0, len(codes), 50):
        batch = codes[i:i+50]
        prefixed = []
        for c in batch:
            if c.startswith(('6', '9')):
                prefixed.append(f'sh{c}')
            elif c.startswith('8'):
                prefixed.append(f'bj{c}')
            else:
                prefixed.append(f'sz{c}')

        url = 'https://qt.gtimg.cn/q=' + ','.join(prefixed)
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0')
        try:
            resp = urllib.request.urlopen(req, timeout=10)
            data = resp.read().decode('gbk')
        except Exception:
            continue

        for line in data.strip().split(';'):
            if '=' not in line or '"' not in line:
                continue
            key = line.split('=')[0].split('_')[-1]
            vals = line.split('"')[1].split('~')
            if len(vals) < 10:
                continue
            code = key[2:]
            opens[code] = {
                'open': float(vals[5]) if vals[5] else 0,
                'price': float(vals[3]) if vals[3] else 0,
                'name': vals[1],
                'last_close': float(vals[4]) if vals[4] else 0,
                'change_pct': float(vals[32]) if vals[32] else 0,
            }
    return opens


def fetch_open_prices_mootdx(codes):
    """备用: mootdx TCP获取开盘价"""
    if MOOTDX is None:
        return {}
    codes = [str(c).zfill(6) for c in codes]
    opens = {}
    for batch in [codes[i:i+50] for i in range(0, len(codes), 50)]:
        market_map = {}
        for c in batch:
            market_map[c] = 1 if c.startswith('6') else 0
        try:
            quotes = MOOTDX.quotes(symbol=batch)
        except Exception:
            continue
        if quotes is None:
            continue
        for _, row in quotes.iterrows():
            code = str(row.get('code', '')).zfill(6)
            if len(code) != 6:
                continue
            opens[code] = {
                'open': float(row.get('open', 0)),
                'price': float(row.get('price', 0)),
                'name': str(row.get('name', code)),
                'last_close': float(row.get('last_close', 0)),
                'change_pct': float(row.get('change_pct', 0)) if 'change_pct' in row else 0,
            }
        time.sleep(0.1)
    return opens


def fetch_open_prices_robust(codes):
    """腾讯优先，mootdx 备用"""
    opens = fetch_open_prices(codes)
    if len(opens) < len(codes) * 0.5:
        print('[WARN] 腾讯仅获取到 {}/{} 只, 切换到 mootdx...'.format(len(opens), len(codes)))
        opens_mootdx = fetch_open_prices_mootdx(codes)
        # 合并: mootdx 补入腾讯缺失的
        for code in codes:
            c = str(code).zfill(6)
            if c not in opens or opens[c]['open'] == 0:
                if c in opens_mootdx and opens_mootdx[c]['open'] > 0:
                    opens[c] = opens_mootdx[c]
    return opens


# ---- Step 3: 计算入场开盘特征 ----
def _load_csv_adjust_factor(code, signal_date, raw_last_close):
    """Infer the project's local adjusted-price factor from the CSV close."""
    if not raw_last_close or raw_last_close <= 0:
        return None
    code = str(code).zfill(6)
    path = Path('data') / code[:2] / '{}.csv'.format(code)
    if not path.exists():
        return None
    try:
        local = pd.read_csv(path, encoding='gbk', dtype={'date': str}, usecols=['date', 'close'])
    except Exception:
        return None
    row = local[local['date'].astype(str).str[:10] == str(signal_date)[:10]]
    if row.empty:
        return None
    local_close = pd.to_numeric(row.iloc[0]['close'], errors='coerce')
    if pd.isna(local_close) or float(local_close) <= 0:
        return None
    factor = float(local_close) / float(raw_last_close)
    if not np.isfinite(factor) or factor <= 0:
        return None
    return factor


def adjust_open_prices_to_csv_scale(opens, signal_date):
    """Convert raw quote prices to the same adjusted scale as local CSV files."""
    adjusted = {}
    missing_factor = []
    suspicious_factor = []
    for code, quote in opens.items():
        code = str(code).zfill(6)
        q = dict(quote)
        raw_open = float(q.get('open') or 0)
        raw_price = float(q.get('price') or 0)
        raw_last_close = float(q.get('last_close') or 0)
        factor = _load_csv_adjust_factor(code, signal_date, raw_last_close)
        q['raw_open'] = raw_open
        q['raw_price'] = raw_price
        q['raw_last_close'] = raw_last_close
        q['adjust_factor'] = factor if factor else 1.0
        if factor:
            q['open'] = raw_open * factor if raw_open > 0 else 0
            q['price'] = raw_price * factor if raw_price > 0 else 0
            q['last_close'] = raw_last_close * factor if raw_last_close > 0 else 0
            if factor < 0.2 or factor > 500:
                suspicious_factor.append((code, factor))
        else:
            missing_factor.append(code)
        adjusted[code] = q

    ok = len(adjusted) - len(missing_factor)
    print('复权转换: {}/{} 只使用CSV同口径因子'.format(ok, len(adjusted)))
    if missing_factor:
        print('[WARN] {} 只未能反推复权因子，将保留原始开盘价: {}'.format(
            len(missing_factor), ', '.join(missing_factor[:20])))
    if suspicious_factor:
        sample = ', '.join('{}:{:.4f}'.format(c, f) for c, f in suspicious_factor[:20])
        print('[WARN] 复权因子异常样本: {}'.format(sample))
    return adjusted


def compute_entry_features(df, opens):
    """补入 overnight_gap_pct, entry_open_to_yellow_pct, entry_open_to_ma5_pct.
    CSV has entry_price (=signal-day close at boundary) and close_to_*_pct features,
    from which we reverse-engineer yellow_line and MA5."""
    df = df.copy()
    for i, row in df.iterrows():
        code = str(row['code']).zfill(6)
        oq = opens.get(code, {})
        e_open = oq.get('open', 0)
        sig_close = row.get('entry_price', 0)
        if e_open > 0 and sig_close > 0:
            df.at[i, 'overnight_gap_pct'] = (e_open - sig_close) / sig_close * 100
        else:
            df.at[i, 'overnight_gap_pct'] = 0

        # Reverse-engineer yellow_line and MA5 from close_to_*_pct
        yellow_pct = row.get('close_to_yellow_pct', 0)
        ma5_pct = row.get('close_to_ma5_pct', 0)
        yellow = sig_close / (1 + yellow_pct / 100) if yellow_pct != 0 and sig_close > 0 else 1
        ma5 = sig_close / (1 + ma5_pct / 100) if ma5_pct != 0 and sig_close > 0 else 1
        if e_open > 0:
            df.at[i, 'entry_open_to_yellow_pct'] = (e_open - yellow) / yellow * 100 if yellow > 0 else 0
            df.at[i, 'entry_open_to_ma5_pct'] = (e_open - ma5) / ma5 * 100 if ma5 > 0 else 0
        else:
            df.at[i, 'entry_open_to_yellow_pct'] = 0
            df.at[i, 'entry_open_to_ma5_pct'] = 0
    return df


# ---- 主流程 ----
def main():
    ap = argparse.ArgumentParser(description='T+1 Brick ML stock selection')
    ap.add_argument('--date', type=str, default=None, help='T day (signal date), e.g. 2026-06-05')
    ap.add_argument('--signals', type=str, default=SIGNALS_FILE, help='Signal CSV path')
    args = ap.parse_args()

    # --- Date resolution ---
    raw_date = datetime.strptime(args.date, '%Y-%m-%d').date() if args.date else datetime.now().date()
    effective_date = _get_latest_trading_day(raw_date)
    if raw_date.strftime('%Y-%m-%d') != effective_date:
        print('[INFO] {} -> effective signal date: {}'.format(raw_date.strftime('%Y-%m-%d'), effective_date))

    # --- Check if we can get real opening prices ---
    if not args.date:
        now = datetime.now()
        if now.weekday() >= 5:
            print('[ERROR] Today is weekend. Market is closed. Use --date to specify a signal date.')
            return
        t = now.time()
        if t < MARKET_OPEN:
            print('[ERROR] Market not open yet (now {}, opens 09:25). Opening prices are not yet determined.'.format(
                t.strftime('%H:%M')))
            print('        Re-run after 9:25 when call auction completes.')
            return
        if t > MARKET_CLOSE:
            print('[ERROR] Market already closed (now {}). Opening prices are stale. Use --date instead.'.format(
                t.strftime('%H:%M')))
            return
        if not _is_market_open():
            print('[ERROR] Today is not a trading day. Use --date to specify a signal date.')
            return
        print('Market open confirmed. Fetching live opening prices...')

    # Step 1: 加载候选池
    signals_path = Path(args.signals)
    if not signals_path.exists():
        # Fallback to conc-enabled version
        alt = signals_path.parent / 'signals_today_with_conc.csv'
        if alt.exists():
            signals_path = alt
        else:
            print('[ERROR] Signal file not found: {}'.format(signals_path))
            print('       Run first: python daily_run.py --skip-b1 --skip-update')
            return

    df = pd.read_csv(signals_path, encoding='gbk')
    if df.empty:
        print('[ERROR] Signal file is empty: {}'.format(signals_path))
        print('        There are no Brick candidates to rank/select.')
        print('        Run daily_run.py after the target trading day CSV data is fully updated.')
        return

    # conc_resonance may not exist in old signal files; fill with 0
    if 'conc_resonance' not in df.columns:
        df['conc_resonance'] = 0.0
    print('Phase 1 候选信号: {} 条 (date={})'.format(len(df), effective_date))

    # Step 2: 获取开盘价
    codes = df['code'].unique()
    print('获取 {} 只股票开盘价...'.format(len(codes)))
    opens = fetch_open_prices_robust(codes)
    print('获取到 {} 只开盘价 (腾讯+mootdx双源)'.format(len(opens)))

    # Step 2.5: 开盘价质量校验
    valid_open = sum(1 for v in opens.values() if v['open'] > 0)
    open_diff_close = sum(1 for v in opens.values()
                          if v['open'] > 0 and v['last_close'] > 0 and abs(v['open'] - v['last_close']) > 0.001)
    if valid_open < len(codes) * 0.8:
        print('[ERROR] Only {}/{} stocks have valid open price. Market likely not open yet.'.format(valid_open, len(codes)))
        print('        Wait until 9:25 call auction completes, then re-run.')
        return
    if open_diff_close < len(codes) * 0.3:
        print('[ERROR] Only {}/{} stocks have open != last_close. Data looks stale (pre-open?).'.format(open_diff_close, len(codes)))
        print('        Wait until 9:25 call auction completes, then re-run.')
        return
    print('开盘价校验通过: valid={}, open≠close={}'.format(valid_open, open_diff_close))

    # Step 2.6: 排除停牌 / open=0 的股票
    opens = adjust_open_prices_to_csv_scale(opens, effective_date)

    suspended_codes = {c for c, v in opens.items() if v['open'] <= 0}
    if suspended_codes:
        df = df[~df['code'].astype(str).str.zfill(6).isin(suspended_codes)]
        for c in suspended_codes:
            del opens[c]
        if len(df) == 0:
            print('[ERROR] All stocks excluded due to open=0 (suspended). Nothing to select.')
            return
        print('排除 {} 只停牌/open=0: {}'.format(len(suspended_codes), ', '.join(sorted(suspended_codes))))

    # Step 3: 补入场特征
    df = compute_entry_features(df, opens)

    # Step 3.5: 校验入场特征已获取真实开盘价
    entry_cols = ['overnight_gap_pct', 'entry_open_to_yellow_pct', 'entry_open_to_ma5_pct']
    if (df[entry_cols] == 0).all().all():
        print('[ERROR] Entry-day features are all zero — opening price fetch failed.')
        print('        The CSV contains placeholder zeros, real prices were not obtained.')
        print('        Re-run after 9:25 when call auction completes.')
        return

    # Step 4: ML 评分
    ranker = joblib.load(MODEL_FILE)
    scaler = joblib.load(SCALER_FILE)
    feats_avail = [f for f in FEATS_ML if f in df.columns]
    X = df[feats_avail].fillna(0)
    X_s = scaler.transform(X)
    df['score'] = ranker.predict(X_s)

    # Step 5: 排名 + 行业约束
    with open(INDUSTRY_FILE, 'r', encoding='utf-8') as f:
        stock_blocks = json.load(f)['stocks']

    df = df.sort_values('score', ascending=False)
    # Softmax weights
    scores = df['score'].values
    w = np.exp(scores - scores.max())
    w = w / w.sum()
    df['weight'] = np.round(w * 100, 1)

    ind_count = {}
    selected = []
    for _, row in df.iterrows():
        code = str(row['code']).zfill(6)
        ind = stock_blocks.get(code, ['UNKNOWN'])[0]
        if ind_count.get(ind, 0) >= MAX_PER_IND:
            continue
        selected.append(row)
        ind_count[ind] = ind_count.get(ind, 0) + 1
        if len(selected) >= TOP_N:
            break

    # Step 6: 输出
    print()
    print('=' * 70)
    print('  T+1 买入清单 (信号日: {})'.format(effective_date))
    print('=' * 70)
    print('{:>4s} {:>8s} {:>10s} {:>10s} {:>7s} {:>6s} {:>8s}'.format(
        '排序', '代码', '名称', '开盘价', '涨幅%', '权重%', 'ML分'))
    print('-' * 70)

    for i, row in enumerate(selected):
        code = str(row['code']).zfill(6)
        oq = opens.get(code, {})
        name = oq.get('name', code)
        e_open = oq.get('open', 0)
        raw_open = oq.get('raw_open', e_open)
        factor = oq.get('adjust_factor', 1.0)
        chg = oq.get('change_pct', 0)
        print('{:4d} {:>8s} {:>10s} {:>10.2f} {:>+6.2f}% {:>5.1f}% {:>8.2f}  raw={:.2f} factor={:.4f}'.format(
            i+1, code, name, e_open, chg, row['weight'], row['score'], raw_open, factor))

    # Summary
    total_w = sum(r['weight'] for r in selected)
    print('-' * 70)
    print('总权重: {}%'.format(round(total_w, 1)))
    print()
    print('持仓规则: 次日开盘买入, 绿砖日收盘卖出, 活跃市值空头暂停开仓')
    # DingTalk notify
    if selected:
        try:
            import yaml
            from utils.dingtalk_notifier import DingTalkNotifier
            with open('config/config.yaml', 'r', encoding='utf-8') as fh:
                cfg = yaml.safe_load(fh)
            dd = cfg['dingtalk']
            n = DingTalkNotifier(webhook_url=dd['webhook_url'], secret=dd['secret'])
            msg_lines = ['## Brick ML Top 5 (信号日: ' + effective_date + ')', '']
            for i, row in enumerate(selected):
                code = str(row['code']).zfill(6)
                oq = opens.get(code, {})
                name = oq.get('name', code)
                msg_lines.append(
                    '{}. **{}** {} — 权重 {:.1f}% | ML {:.2f}'.format(
                        i + 1, code, name, row['weight'], row['score']))
            msg_lines.append('')
            msg_lines.append('候选池: {} 只'.format(len(df)))
            n.send_markdown("Brick ML (" + effective_date + ")", chr(10).join(msg_lines))
            print('DingTalk sent OK')
        except Exception as e:
            print('DingTalk failed: ' + str(e))

if __name__ == '__main__':
    main()
