"""
Layer 3: 大单画像分析 (Deep Trade style)
基于大单明细 + K线数据，检测吸筹/出货模式
参考: DeepCharts Deep Trades indicator 的核心逻辑
"""
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


class BigDealAnalyzer:
    """大单画像分析器 - 基于 Deep Trades 逻辑"""

    # 大单强度级别 (分位数阈值)
    INTENSITY = {
        'low': 0.70,     # top 30%
        'medium': 0.85,  # top 15%
        'strong': 0.95,  # top 5%
    }

    def __init__(self, data_dir=None, csv_manager=None):
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data' / 'block'
        self.data_dir = Path(data_dir)
        self.csv_manager = csv_manager

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------
    def load_big_deal_history(self, days=5):
        """加载最近N天的大单明细"""
        deal_dir = self.data_dir / 'big_deal'
        if not deal_dir.exists():
            return pd.DataFrame()

        files = sorted(deal_dir.glob('*.csv'), reverse=True)[:days]
        if not files:
            return pd.DataFrame()

        dfs = []
        for fp in files:
            try:
                df = pd.read_csv(fp, encoding='utf-8-sig', dtype={'code': str})
                df['date'] = fp.stem
                dfs.append(df)
            except Exception as e:
                print(f'  [WARN] failed to read {fp}: {e}')

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def load_kline_data(self, stock_code):
        """加载股票K线数据（倒序→正序）"""
        if self.csv_manager is None:
            return pd.DataFrame()
        df = self.csv_manager.read_stock(stock_code)
        if df.empty:
            return df
        df = df.sort_values('date').reset_index(drop=True)
        return df

    # ------------------------------------------------------------------
    # Dynamic threshold
    # ------------------------------------------------------------------
    def compute_threshold(self, deals_df, intensity='medium'):
        """动态计算大单阈值（基于近N日成交量分布）"""
        if deals_df.empty:
            return 0
        percentile = self.INTENSITY.get(intensity, 0.85)
        # Use amount (成交额) as primary metric, fallback to volume
        col = 'amount' if 'amount' in deals_df.columns else 'volume'
        if col not in deals_df.columns:
            return 0
        vals = pd.to_numeric(deals_df[col], errors='coerce').dropna()
        if len(vals) < 10:
            return 0
        return float(vals.quantile(percentile))

    # ------------------------------------------------------------------
    # K-line position classification
    # ------------------------------------------------------------------
    @staticmethod
    def classify_position(row, o, h, l, c):
        """判断大单成交价在K线中的位置（包容复权差异）"""
        price = row.get('price', 0)
        if price <= 0:
            return 'outside'

        body_top = max(o, c)
        body_bot = min(o, c)

        # 极端价位（大宗交易/复权差异）→ 映射到最近端
        if price > h:
            return 'upper_wick'
        if price < l:
            return 'lower_wick'
        if price > body_top:
            return 'upper_wick'
        if price < body_bot:
            return 'lower_wick'
        return 'body'

    @staticmethod
    def candle_type(o, c):
        """判断K线类型"""
        return 'bullish' if c >= o else 'bearish'

    # ------------------------------------------------------------------
    # Deep Trades pattern matching
    # ------------------------------------------------------------------
    def analyze_patterns(self, stock_code, days=5, intensity='medium'):
        """
        分析大单模式
        返回: {
            'absorption_score': 0-100,   # 吸筹评分
            'pattern_signals': [...],      # 检测到的模式
            'buy_zone': (low, high),       # 买方大单密集区
            'cost_zone': (low, high),      # 主力成本区
            'buy_ratio': float,            # 大单买入占比
            'dominant_direction': str,     # 主导方向
        }
        """
        deals = self.load_big_deal_history(days)
        if deals.empty:
            return self._empty_pattern_result()

        stock_deals = deals[deals['code'] == stock_code].copy()
        if len(stock_deals) < 20:
            return self._empty_pattern_result()

        # Load K-line data for candle classification
        kline = self.load_kline_data(stock_code)
        kline_dict = {}
        if not kline.empty and 'date' in kline.columns:
            for _, row in kline.iterrows():
                d = str(row['date'])[:10]
                kline_dict[d] = {
                    'open': float(row['open']), 'high': float(row['high']),
                    'low': float(row['low']), 'close': float(row['close']),
                }

        # Compute dynamic threshold
        threshold = self.compute_threshold(stock_deals, intensity)

        # Filter to "big" deals only
        col = 'amount' if 'amount' in stock_deals.columns else 'volume'
        if col in stock_deals.columns:
            stock_deals[col] = pd.to_numeric(stock_deals[col], errors='coerce')
            big_deals = stock_deals[stock_deals[col] >= threshold].copy()
        else:
            big_deals = stock_deals.copy()

        if len(big_deals) < 10:
            return self._empty_pattern_result()

        # Classify each big deal
        patterns = []
        for _, deal in big_deals.iterrows():
            # Use 'time' column (actual trade datetime) for matching, fallback to 'date'
            deal_time = str(deal.get('time', deal.get('date', '')))
            deal_date = deal_time[:10]  # '2026-05-15 15:00:02' -> '2026-05-15'
            k = kline_dict.get(deal_date)
            if k is None:
                continue

            position = self.classify_position(
                deal, k['open'], k['high'], k['low'], k['close'])
            ct = self.candle_type(k['open'], k['close'])
            direction = str(deal.get('direction', '')).lower()

            patterns.append({
                'date': deal_date,
                'price': float(deal.get('price', 0)),
                'amount': float(deal.get('amount', 0)),
                'direction': direction,
                'position': position,
                'candle': ct,
            })

        if not patterns:
            return self._empty_pattern_result()

        patterns_df = pd.DataFrame(patterns)

        # --- Pattern signal detection ---
        signals = []

        # 1. Absorption: BUY in bearish lower wick (most bullish for B1)
        buy_bear_wick = patterns_df[
            (patterns_df['direction'] == 'buy') &
            (patterns_df['candle'] == 'bearish') &
            (patterns_df['position'] == 'lower_wick')
        ]
        if len(buy_bear_wick) > 0:
            signals.append({
                'pattern': 'buy_in_bear_wick',
                'meaning': '卖方在低位被吸收，反转信号',
                'bullish': True,
                'count': len(buy_bear_wick),
                'total_amount': round(buy_bear_wick['amount'].sum(), 2),
            })

        # 2. SELL in bearish body = continuation (bearish, confirming washout)
        sell_bear_body = patterns_df[
            (patterns_df['direction'] == 'sell') &
            (patterns_df['candle'] == 'bearish') &
            (patterns_df['position'] == 'body')
        ]
        if len(sell_bear_body) > 0:
            signals.append({
                'pattern': 'sell_in_bear_body',
                'meaning': '卖方主导，洗盘进行中',
                'bullish': False,
                'count': len(sell_bear_body),
                'total_amount': round(sell_bear_body['amount'].sum(), 2),
            })

        # 3. BUY in bullish body = continuation (bullish)
        buy_bull_body = patterns_df[
            (patterns_df['direction'] == 'buy') &
            (patterns_df['candle'] == 'bullish') &
            (patterns_df['position'] == 'body')
        ]
        if len(buy_bull_body) > 0:
            signals.append({
                'pattern': 'buy_in_bull_body',
                'meaning': '买方主动推动，看涨延续',
                'bullish': True,
                'count': len(buy_bull_body),
                'total_amount': round(buy_bull_body['amount'].sum(), 2),
            })

        # 4. SELL in bullish upper wick = reversal (bearish absorption)
        sell_bull_wick = patterns_df[
            (patterns_df['direction'] == 'sell') &
            (patterns_df['candle'] == 'bullish') &
            (patterns_df['position'] == 'upper_wick')
        ]
        if len(sell_bull_wick) > 0:
            signals.append({
                'pattern': 'sell_in_bull_wick',
                'meaning': '买方在上方被吸收，见顶信号',
                'bullish': False,
                'count': len(sell_bull_wick),
                'total_amount': round(sell_bull_wick['amount'].sum(), 2),
            })

        # --- Absorption scoring ---
        total_buy = patterns_df[patterns_df['direction'] == 'buy']['amount'].sum()
        total_sell = patterns_df[patterns_df['direction'] == 'sell']['amount'].sum()
        total_all = total_buy + total_sell
        buy_ratio = round(total_buy / total_all * 100, 1) if total_all > 0 else 50

        # Absorption score components
        score = 50.0

        # Bonus: buy in bearish wick (the key B1 signal)
        if len(buy_bear_wick) > 0:
            bear_wick_ratio = buy_bear_wick['amount'].sum() / max(total_all, 1)
            score += min(30, bear_wick_ratio * 100 * 2)

        # Bonus: buy ratio above 50%
        if buy_ratio > 50:
            score += min(15, (buy_ratio - 50) * 0.5)

        # Penalty: sell in bearish body dominating
        if len(sell_bear_body) > 0 and len(buy_bear_wick) == 0:
            score -= 10

        # Bonus: buy in bullish body (momentum confirmation)
        if len(buy_bull_body) > 0:
            score += min(10, buy_bull_body['amount'].sum() / max(total_all, 1) * 100)

        # Penalty: sell in bullish wick (reversal signal)
        if len(sell_bull_wick) > 0:
            score -= min(10, sell_bull_wick['amount'].sum() / max(total_all, 1) * 100)

        score = max(0, min(100, round(score, 1)))

        # --- Price zone analysis ---
        buy_zones = patterns_df[patterns_df['direction'] == 'buy']
        if len(buy_zones) > 0:
            prices = buy_zones['price'].values
            # Simple clustering: mean ± 1 std of buy prices
            buy_center = float(np.mean(prices))
            buy_std = float(np.std(prices)) if len(prices) > 1 else buy_center * 0.01
            buy_zone = (round(buy_center - buy_std, 2), round(buy_center + buy_std, 2))
        else:
            buy_zone = (0, 0)

        # Dominant direction
        dom = 'buy' if buy_ratio >= 50 else 'sell'

        return {
            'absorption_score': score,
            'pattern_signals': signals,
            'buy_zone_low': buy_zone[0],
            'buy_zone_high': buy_zone[1],
            'buy_ratio': buy_ratio,
            'dominant_direction': dom,
            'threshold': round(threshold, 2),
            'total_big_deals': len(patterns_df),
            'total_buy_amount': round(total_buy, 2),
            'total_sell_amount': round(total_sell, 2),
        }

    def _empty_pattern_result(self):
        return {
            'absorption_score': 0,
            'pattern_signals': [],
            'buy_zone_low': 0, 'buy_zone_high': 0,
            'buy_ratio': 50, 'dominant_direction': 'neutral',
            'threshold': 0, 'total_big_deals': 0,
            'total_buy_amount': 0, 'total_sell_amount': 0,
        }

    # ------------------------------------------------------------------
    # B1-specific: washout absorption analysis
    # ------------------------------------------------------------------
    def analyze_washout(self, stock_code, days=5, intensity='medium'):
        """
        B1洗盘专用分析
        在洗盘区间内检测是否有主力吸筹迹象
        """
        result = self.analyze_patterns(stock_code, days, intensity)

        # Check for the key B1 signal: buyer absorption in bearish candles
        has_absorption = False
        absorption_strength = 0
        for sig in result['pattern_signals']:
            if sig['pattern'] == 'buy_in_bear_wick':
                has_absorption = True
                absorption_strength = sig['total_amount']
                break

        # Current price vs buy zone
        price_in_buy_zone = False
        if result['buy_zone_low'] > 0:
            # Need K-line to check current price
            kline = self.load_kline_data(stock_code)
            if not kline.empty:
                current_price = float(kline.iloc[-1]['close'])
                price_in_buy_zone = (
                    result['buy_zone_low'] <= current_price <= result['buy_zone_high']
                )

        return {
            **result,
            'has_absorption': has_absorption,
            'absorption_strength': round(absorption_strength, 2),
            'price_in_buy_zone': price_in_buy_zone,
            'washout_quality': self._rate_washout(result, has_absorption, price_in_buy_zone),
        }

    @staticmethod
    def _rate_washout(result, has_absorption, price_in_zone):
        """综合评估洗盘质量"""
        if result['total_big_deals'] < 10:
            return 'insufficient_data'
        score = result['absorption_score']
        if has_absorption and price_in_zone and score >= 70:
            return 'excellent'
        elif has_absorption and score >= 60:
            return 'good'
        elif score >= 50:
            return 'neutral'
        else:
            return 'distribution_likely'


if __name__ == '__main__':
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.csv_manager import CSVManager

    analyzer = BigDealAnalyzer(
        data_dir=Path(__file__).parent.parent / 'data' / 'block',
        csv_manager=CSVManager(Path(__file__).parent.parent / 'data' / 'stock'),
    )

    print('=== 大单画像分析测试 ===')
    test_stocks = ['600366', '002580']

    for code in test_stocks:
        result = analyzer.analyze_patterns(code, days=1, intensity='low')
        print(f"\n--- {code} ---")
        print(f"  absorption_score: {result['absorption_score']}")
        print(f"  buy_ratio: {result['buy_ratio']}%")
        print(f"  dominant: {result['dominant_direction']}")
        print(f"  buy_zone: {result['buy_zone_low']:.2f} - {result['buy_zone_high']:.2f}")
        print(f"  threshold: {result['threshold']}")
        print(f"  total big deals: {result['total_big_deals']}")
        for sig in result['pattern_signals']:
            print(f"  [{sig['pattern']}] {sig['meaning']} "
                  f"count={sig['count']} amount={sig['total_amount']}万")
