# -*- coding: utf-8 -*-
"""
B3 standalone daily selection (extracted from main.py)

B3 conditions (7 rules from 通达信 formula B3XG):
  1. yesterday return >= 9%
  2. today return between 2%-4%
  3. today close > today open (bullish candle)
  4. today open vs yesterday close gap < 1%
  5. upper shadow <= 2%
  6. lower shadow <= 2%
  7. volume ratio 0.6-0.9 (volume shrinking after surge)

Plus:
  - white_line > yellow_line (uptrend)
  - close > yellow_line
  - market_cap > 5B (if available)

Usage: python run_b3.py [--max-stocks N]
"""

import sys, argparse, warnings, yaml
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import numpy as np, pandas as pd
from strategy.base_strategy import BaseStrategy
warnings.filterwarnings('ignore')

DATA_DIR = Path('data')
PREFIXES = ['00', '30', '60', '68']


class B3Scanner:
    def __init__(self):
        pass

    def _calc_indicators(self, df):
        """Minimal indicators: white/yellow line. Volume > 0 only."""
        df = df[df['volume'] > 0].copy()
        if len(df) < 5:
            return df
        for col in ['open', 'high', 'low', 'close']:
            if col in df.columns:
                df[col] = df[col].round(2)
        if 'date' in df.columns:
            df = df.sort_values('date').reset_index(drop=True)
        c = df['close'].values

        # Yellow line: (MA14+MA28+MA57+MA114)/4
        def _ma(s, n):
            return pd.Series(s).rolling(n, min_periods=1).mean()
        df['yellow_line'] = (_ma(c, 14) + _ma(c, 28) + _ma(c, 57) + _ma(c, 114)) / 4

        # White line: EMA(EMA(close,10),10)
        ema10 = pd.Series(c).ewm(span=10, adjust=False, min_periods=1).mean()
        df['white_line'] = ema10.ewm(span=10, adjust=False, min_periods=1).mean()
        return df

    def check_b3(self, df):
        """Check B3 conditions. Returns dict or None."""
        df = self._calc_indicators(df)
        if len(df) < 3:
            return None

        t = df.iloc[-1]   # today
        y = df.iloc[-2]   # yesterday
        y2 = df.iloc[-3]  # day before yesterday

        ret_today = (t['close'] / y['close'] - 1) if y['close'] > 0 else 0
        ret_ytd = (y['close'] / y2['close'] - 1) if y2['close'] > 0 else 0

        # 7 conditions from B3XG
        if not (ret_ytd >= 0.09):
            return None
        if not (0.02 <= ret_today <= 0.04):
            return None
        if not (t['close'] > t['open']):
            return None
        if not ((t['open'] / y['close'] - 1) < 0.01):
            return None
        if not ((t['high'] - t['close']) / max(t['close'], 0.01) <= 0.02):
            return None
        if not ((t['open'] - t['low']) / max(t['open'], 0.01) <= 0.02):
            return None
        vr = t['volume'] / y['volume'] if y['volume'] > 0 else 999
        if not (0.6 <= vr <= 0.9):
            return None

        # Trend + market cap
        if not (t.get('white_line', 0) > t.get('yellow_line', 0)):
            return None
        if not (t['close'] > t.get('yellow_line', 0)):
            return None
        cap = t.get('market_cap', np.nan)
        if pd.notna(cap) and cap <= 5_000_000_000:
            return None

        return {
            'ret_today_pct': round(ret_today * 100, 2),
            'ret_ytd_pct': round(ret_ytd * 100, 2),
            'vol_ratio': round(vr, 2),
        }


def main():
    ap = argparse.ArgumentParser(description='B3 daily scan')
    ap.add_argument('--max-stocks', type=int, default=None)
    args = ap.parse_args()

    scanner = B3Scanner()
    results = []

    # Collect all stock codes
    codes = []
    for pfx in PREFIXES:
        d = DATA_DIR / pfx
        if d.exists():
            for f in d.glob('*.csv'):
                codes.append(f.stem)
    if args.max_stocks:
        codes = codes[:args.max_stocks]

    print('B3 scan: {} stocks'.format(len(codes)))
    for i, code in enumerate(codes):
        csv_path = DATA_DIR / code[:2] / '{}.csv'.format(code)
        if not csv_path.exists():
            continue
        try:
            df = pd.read_csv(csv_path, encoding='gbk')
        except Exception:
            continue
        if len(df) < 120:
            continue
        info = scanner.check_b3(df)
        if info:
            t = df.iloc[0] if 'date' not in df.columns else df.iloc[-1]
            results.append({
                'code': code,
                'close': float(t['close']),
                'vol_ratio': info['vol_ratio'],
                **info,
            })
        if (i + 1) % 500 == 0:
            print('  [{}/{}] signals: {}'.format(i + 1, len(codes), len(results)))

    print()
    print('=' * 60)
    print('B3 Results: {} signals'.format(len(results)))
    print('=' * 60)
    for r in results[:20]:
        print('  {} | close={:.2f} | ret_t={:+.1f}% ret_y={:+.1f}% | vr={:.2f}'.format(
            r['code'], r['close'], r['ret_today_pct'], r['ret_ytd_pct'], r['vol_ratio']))

    # Send via DingTalk
    if results:
        try:
            from utils.dingtalk_notifier import DingTalkNotifier
            with open('config/config.yaml', 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
            dd = cfg['dingtalk']
            n = DingTalkNotifier(webhook_url=dd['webhook_url'], secret=dd['secret'])
            today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
            lines = ['## B3 Daily Picks ({})'.format(today_str), '']
            for i, r in enumerate(results[:10], 1):
                lines.append('{}. **{}** | close={:.2f} | ret_t={:+.1f}% ret_y={:+.1f}% | vr={:.2f}'.format(
                    i, r['code'], r['close'], r['ret_today_pct'], r['ret_ytd_pct'], r['vol_ratio']))
            lines.append('')
            lines.append('Total: {} signals'.format(len(results)))
            n.send_markdown('B3 Daily ({})'.format(today_str), '\n'.join(lines))
            print('DingTalk sent OK')
        except Exception as e:
            print('DingTalk failed: {}'.format(e))


if __name__ == '__main__':
    main()
