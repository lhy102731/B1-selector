# -*- coding: utf-8 -*-
"""
砖型图选股策略 — 通达信公式翻译

信号逻辑：黄色多空线上方，砖型图绿转红且红柱高度>=绿柱高度*2/3

公式来源: C:/Users/Administrator/Documents/砖型图.txt
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from strategy.base_strategy import BaseStrategy


def _sma(series, n, m):
    """通达信SMA: Y = (X*M + Y'*(N-M)) / N, 升序数据专用"""
    result = pd.Series(np.nan, index=series.index, dtype=float)
    result.iloc[0] = series.iloc[0]
    for i in range(1, len(series)):
        result.iloc[i] = (series.iloc[i] * m + result.iloc[i - 1] * (n - m)) / n
    return result


class BrickChartStrategy(BaseStrategy):
    """砖型图选股策略"""

    def __init__(self, params=None):
        default_params = {
            'M1': 14, 'M2': 28, 'M3': 57, 'M4': 114,
            'height_ratio': 2.0 / 3.0,
        }
        if params:
            default_params.update(params)
        super().__init__("砖型图策略", default_params)

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = result[result['volume'] > 0]

        for col in ['open', 'high', 'low', 'close']:
            if col in result.columns:
                result[col] = result[col].round(2)

        # 升序排列（最旧在前，最新在后）
        if 'date' in result.columns:
            result = result.sort_values('date').reset_index(drop=True)
        else:
            result = result.reset_index(drop=True)

        close = result['close']
        high = result['high']
        low = result['low']
        m1, m2, m3, m4 = (self.params['M1'], self.params['M2'],
                          self.params['M3'], self.params['M4'])

        # --- 知行多空线（黄线）---
        result['yellow_line'] = self._calc_yellow_line(close, m1, m2, m3, m4)
        result['above_yellow'] = close > result['yellow_line']

        # --- 砖型图核心计算 ---
        # HHV(HIGH, 4) / LLV(LOW, 4) — 含当根K线的4日最高/最低
        hhv4 = high.rolling(4, min_periods=1).max()
        llv4 = low.rolling(4, min_periods=1).min()

        denom = hhv4 - llv4
        denom_safe = denom.where(denom != 0, 1e-9)

        # VAR1A := ((HHV(HIGH,4)-CLOSE)/(HHV(HIGH,4)-LLV(LOW,4)))*100-90
        var1a = ((hhv4 - close) / denom_safe) * 100.0 - 90.0

        # VAR2A := SMA(VAR1A, 4, 1) + 100
        var2a = _sma(var1a, 4, 1) + 100.0

        # VAR3A := (CLOSE-LLV(LOW,4))/(HHV(HIGH,4)-LLV(LOW,4))*100
        var3a = ((close - llv4) / denom_safe) * 100.0

        # VAR4A := SMA(VAR3A, 6, 1)
        var4a = _sma(var3a, 6, 1)

        # VAR5A := SMA(VAR4A, 6, 1) + 100
        var5a = _sma(var4a, 6, 1) + 100.0

        # VAR6A := VAR5A - VAR2A
        var6a = var5a - var2a

        # 砖型图 := IF(VAR6A > 4, VAR6A - 4, 0)
        brick = np.where(var6a.values > 4.0, var6a.values - 4.0, 0.0)
        result['brick'] = brick

        # --- 红绿柱判断 ---
        # REF(X, 1): 升序数据中 shift(1) 即为前一根K线
        brick_shift1 = result['brick'].shift(1)
        brick_shift2 = result['brick'].shift(2)

        # 今天红柱 := 砖型图 > REF(砖型图, 1)
        result['red_today'] = result['brick'] > brick_shift1

        # 昨天绿柱 := REF(砖型图, 1) < REF(砖型图, 2)
        result['green_yesterday'] = brick_shift1 < brick_shift2

        # 红柱高度 := 砖型图 - REF(砖型图, 1)
        result['red_height'] = result['brick'] - brick_shift1

        # 绿柱高度 := REF(砖型图, 2) - REF(砖型图, 1)
        result['green_height'] = brick_shift2 - brick_shift1

        # 高度达标 := 红柱高度 >= 绿柱高度 * 2/3
        ratio = self.params['height_ratio']
        result['height_ok'] = result['red_height'] >= result['green_height'] * ratio

        # --- 最终选股信号 ---
        result['brick_signal'] = (
            result['green_yesterday'].astype(bool) &
            result['red_today'].astype(bool) &
            result['height_ok'].astype(bool) &
            result['above_yellow'].astype(bool)
        )

        return result

    @staticmethod
    def _calc_yellow_line(close, m1, m2, m3, m4):
        ma1 = close.rolling(m1, min_periods=1).mean()
        ma2 = close.rolling(m2, min_periods=1).mean()
        ma3 = close.rolling(m3, min_periods=1).mean()
        ma4 = close.rolling(m4, min_periods=1).mean()
        return (ma1 + ma2 + ma3 + ma4) / 4.0

    def select_stocks(self, df, stock_name='') -> list:
        if df is None or df.empty or len(df) < 10:
            return []

        signal_rows = df[df['brick_signal'] == True]
        if signal_rows.empty:
            return []

        signals = []
        for _, row in signal_rows.iterrows():
            signals.append({
                'date': str(row.get('date', '')),
                'close': float(row['close']),
                'brick': float(row['brick']),
                'red_height': float(row['red_height']),
                'green_height': float(row['green_height']),
                'yellow_line': float(row['yellow_line']),
                'type': 'brick_chart',
            })
        return signals
