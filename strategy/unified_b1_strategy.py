# -*- coding: utf-8 -*-
"""
统一B1策略 - 基于师傅直播总结的完整B1条件
含MACD、超级B1、B2检测
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from strategy.base_strategy import BaseStrategy
from utils.technical import KDJ, calculate_zhixing_trend
from utils.s1_filter import detect_s1_signal
from utils.washout_detector import detect_washout


class UnifiedB1Strategy(BaseStrategy):
    """统一B1策略"""

    def __init__(self, params=None):
        default_params = {
            'M1': 14, 'M2': 28, 'M3': 57, 'M4': 114,
            'white_period': 10,
            'j_threshold': 30,
            'j_super_threshold': 20,
            'volume_shrink_ratio': 0.618,
            'cap_threshold': 4000000000,
            'max_gain_pct': 60,
            'max_surge_turnover': 90,          # ★ 换手累加上限改为90%
            'near_pct': 3.5,
        }
        if params:
            default_params.update(params)
        super().__init__("统一B1策略", default_params)

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算所有指标（数据转为正序计算，最后转回倒序输出）"""
        result = df.copy()
        result = result[result['volume'] > 0]  # ★ 确保无停牌日
        for col in ['open', 'high', 'low', 'close']:
            if col in result.columns:
                result[col] = result[col].round(2)

        if 'date' in result.columns:
            result = result.sort_values('date').reset_index(drop=True)
        else:
            result = result.reset_index(drop=True)

        close = result['close'].values
        high = result['high'].values
        low = result['low'].values
        volume = result['volume'].values
        n = len(close)

        # ---------- 双线 ----------
        def ema(series, span):
            alpha = 2.0 / (span + 1)
            out = np.zeros_like(series)
            out[0] = series[0]
            for i in range(1, len(series)):
                out[i] = alpha * series[i] + (1 - alpha) * out[i-1]
            return out

        ema10 = ema(close, 10)
        white_line = ema(ema10, 10)
        result['white_line'] = white_line

        def ma(series, window):
            out = np.zeros_like(series)
            for i in range(len(series)):
                start = max(0, i - window + 1)
                out[i] = np.mean(series[start:i+1])
            return out

        ma14 = ma(close, 14)
        ma28 = ma(close, 28)
        ma57 = ma(close, 57)
        ma114 = ma(close, 114)
        yellow_line = (ma14 + ma28 + ma57 + ma114) / 4.0
        result['yellow_line'] = yellow_line

        # ---------- KDJ ----------
        kdj_df = KDJ(result, n=9, m1=3, m2=3)
        result['K'] = kdj_df['K'].values
        result['D'] = kdj_df['D'].values
        result['J'] = kdj_df['J'].values

        # ---------- 均线 ----------
        result['MA5'] = ma(close, 5)
        result['MA10'] = ma(close, 10)
        result['MA20'] = ma(close, 20)
        result['MA30'] = ma(close, 30)
        result['MA40'] = ma(close, 40)

        # ---------- 缩量条件 ----------
        hhv_vol_20 = pd.Series(volume).rolling(window=20, min_periods=1).max().values
        result['volume_shrink'] = volume < hhv_vol_20 * self.params['volume_shrink_ratio']

        # ---------- 位置条件 ----------
        result['white_gt_yellow'] = white_line > yellow_line
        result['fall_in_bowl'] = (close >= yellow_line) & (close <= white_line)
        near_pct = self.params['near_pct'] / 100.0
        result['near_yellow'] = (close >= yellow_line) & ((close - yellow_line) / yellow_line <= near_pct)
        result['near_white'] = np.abs(close - white_line) / white_line <= near_pct

        # ---------- 翻倍检测 ----------
        high_60 = pd.Series(high).rolling(window=60, min_periods=1).max().values
        low_60 = pd.Series(low).rolling(window=60, min_periods=1).min().values
        result['doubled'] = high_60 > low_60 * 2.0

        # ---------- MACD ----------
        ema12 = ema(close, 12)
        ema26 = ema(close, 26)
        dif = ema12 - ema26
        dea = ema(dif, 9)
        macd_hist = (dif - dea) * 2
        result['DIF'] = dif
        result['DEA'] = dea
        result['MACD'] = macd_hist

        result = result.sort_values('date', ascending=False).reset_index(drop=True)
        return result

    def _calc_build_position_quality(self, df: pd.DataFrame) -> dict:
        """建仓波质量检测（与之前相同，省略）"""
        # 保持原有代码
        if len(df) < 20:
            return {'total_gain': 0, 'surge_turnover_sum': 0, 'is_qualified': False,
                    'has_limit_up': False, 'has_shrink_limit_up': False, 'has_one_word_limit': False,
                    'surge_start_idx': None}
        df_asc = df.sort_values('date').reset_index(drop=True)
        close = df_asc['close']
        volume = df_asc['volume']
        open_price = df_asc['open']
        high = df_asc['high']
        low = df_asc['low']

        avg_vol_5 = volume.rolling(5, min_periods=1).mean().shift(1)
        pct_chg = close.pct_change() * 100
        gain_cond = pct_chg >= 4.0
        positive_cond = close > open_price
        volume_cond = volume > avg_vol_5
        gap_up = (low > high.shift(1) * 1.02)
        is_surge_day = gain_cond & positive_cond & volume_cond

        def is_long_upper_shadow(row):
            total_range = row['high'] - row['low']
            if total_range == 0:
                return False
            upper_shadow = row['high'] - max(row['close'], row['open'])
            return (upper_shadow / total_range) > 0.618

        current_idx = len(df_asc) - 1
        last_surge_idx = None
        for i in range(current_idx, -1, -1):
            if is_surge_day.iloc[i]:
                last_surge_idx = i
                break
        if last_surge_idx is None:
            return {'total_gain': 0, 'surge_turnover_sum': 0, 'is_qualified': False,
                    'has_limit_up': False, 'has_shrink_limit_up': False, 'has_one_word_limit': False,
                    'surge_start_idx': None}

        start_idx = last_surge_idx
        gap_count = 0
        for i in range(last_surge_idx - 1, -1, -1):
            if is_surge_day.iloc[i]:
                if gap_count <= 5:
                    start_idx = i
                    gap_count = 0
                else:
                    break
            else:
                gap_count += 1

        for i in range(start_idx, last_surge_idx + 1):
            if is_surge_day.iloc[i] and gap_up.iloc[i]:
                return {'total_gain': 0, 'surge_turnover_sum': 0, 'is_qualified': False,
                        'has_limit_up': False, 'has_shrink_limit_up': True, 'has_one_word_limit': False,
                        'surge_start_idx': None}

        long_shadow_count = 0
        for i in range(start_idx, last_surge_idx + 1):
            if is_long_upper_shadow(df_asc.iloc[i]):
                long_shadow_count += 1
        if long_shadow_count > 3:
            return {'total_gain': 0, 'surge_turnover_sum': 0, 'is_qualified': False,
                    'has_limit_up': False, 'has_shrink_limit_up': False, 'has_one_word_limit': False,
                    'surge_start_idx': None}

        start_price = df_asc.iloc[start_idx]['open']
        look_forward = min(60, len(df_asc) - start_idx)
        max_close_idx = start_idx
        max_close = start_price
        for i in range(start_idx, start_idx + look_forward):
            if df_asc.iloc[i]['close'] > max_close:
                max_close = df_asc.iloc[i]['close']
                max_close_idx = i
        for i in range(start_idx, max_close_idx):
            if i > 0:
                day_gain = (df_asc.iloc[i]['close'] - df_asc.iloc[i-1]['close']) / df_asc.iloc[i-1]['close'] * 100
                if day_gain <= -5 and df_asc.iloc[i]['volume'] > avg_vol_5.iloc[i] * 1.5:
                    max_close_idx = i - 1
                    break
        end_price = df_asc.iloc[max_close_idx]['close']
        total_gain = (end_price / start_price - 1) * 100

        surge_turnover_sum = 0.0
        if 'turnover' in df_asc.columns:
            for i in range(start_idx, max_close_idx + 1):
                if is_surge_day.iloc[i]:
                    surge_turnover_sum += df_asc.iloc[i]['turnover']

        period_df = df_asc.iloc[start_idx:max_close_idx+1]
        positive_vol = period_df[period_df['close'] > period_df['open']]['volume'].sum()
        negative_vol = period_df[period_df['close'] < period_df['open']]['volume'].sum()
        if positive_vol <= negative_vol * 1.2:
            return {'total_gain': round(total_gain, 2), 'surge_turnover_sum': round(surge_turnover_sum, 2),
                    'is_qualified': False, 'has_limit_up': False, 'has_shrink_limit_up': False,
                    'has_one_word_limit': False, 'surge_start_idx': start_idx}

        has_limit_up = False
        has_shrink_limit_up = False
        has_one_word_limit = False
        for i in range(start_idx, max_close_idx + 1):
            row = df_asc.iloc[i]
            if i > 0:
                day_gain = (row['close'] - df_asc.iloc[i-1]['close']) / df_asc.iloc[i-1]['close'] * 100
            else:
                day_gain = 0
            is_limit = (day_gain >= 9.8) or ((row['high'] - row['low']) / row['low'] * 100 < 0.1 and row['close'] == row['high'])
            if is_limit:
                has_limit_up = True
                if (abs(row['open'] - row['close']) / row['open'] * 100 < 0.5 and
                        abs(row['high'] - row['low']) / row['low'] * 100 < 0.5):
                    has_one_word_limit = True
                    has_shrink_limit_up = True
                else:
                    if row['volume'] < avg_vol_5.iloc[i] * 1:
                        has_shrink_limit_up = True

        is_qualified = (total_gain <= self.params['max_gain_pct']) and (
                    surge_turnover_sum <= self.params['max_surge_turnover'])
        if has_shrink_limit_up:
            is_qualified = False

        return {
            'total_gain': round(total_gain, 2),
            'surge_turnover_sum': round(surge_turnover_sum, 2),
            'is_qualified': is_qualified,
            'has_limit_up': has_limit_up,
            'has_shrink_limit_up': has_shrink_limit_up,
            'has_one_word_limit': has_one_word_limit,
            'surge_start_idx': start_idx
        }

    def _detect_washout_exception(self, df: pd.DataFrame) -> bool:
        is_washout, _, _ = detect_washout(df)
        return is_washout

    def detect_b2_signal(self, df: pd.DataFrame) -> bool:
        """检测持仓标的是否出现B2信号（放量中大阳线）"""
        if df.empty or len(df) < 5:
            return False
        # df 是倒序，取今天和昨天
        today = df.iloc[0]
        yesterday = df.iloc[1]
        pct_chg = (today['close'] - yesterday['close']) / yesterday['close'] * 100
        # 涨幅≥4%
        if pct_chg < 4.0:
            return False
        # 无长上影 (近似：上影线长度不超过实体长度，或不超过K线总长度的1/3)
        if today['close'] < today['open']:  # 阴线不管
            return False
        upper_shadow = today['high'] - today['close']
        body = today['close'] - today['open']
        if upper_shadow > body * 1.2:  # 有较长上影线
            return False
        # 倍量
        avg_vol = df.iloc[1:6]['volume'].mean() if len(df) >= 6 else df['volume'].iloc[1]
        if today['volume'] < yesterday['volume'] * 1.5 or today['volume'] < avg_vol * 1.5:
            return False
        return True

    def select_stocks(self, df, stock_name=''):
        if df.empty or len(df) < 60:
            return []

        if stock_name:
            invalid_keywords = ['退', '未知', '退市', '已退']
            if any(kw in stock_name for kw in invalid_keywords):
                return []
            if stock_name.startswith('ST') or stock_name.startswith('*ST'):
                return []

        latest = df.iloc[0]
        j = latest['J']
        j_thresh = self.params['j_threshold']

        # 基础必达条件
        if not latest.get('white_gt_yellow', False):
            return []
        if j >= j_thresh:
            return []
        if not latest.get('volume_shrink', False):
            return []

        # MACD 多头区间: DIF > 0
        if latest.get('DIF', -1) <= 0:
            return []

        # 市值检查
        if 'market_cap' in df.columns:
            if latest['market_cap'] < self.params['cap_threshold']:
                return []

        # 翻倍过滤
        if latest.get('doubled', False):
            return []

        # 建仓波质量检测
        build_quality = self._calc_build_position_quality(df)
        if not build_quality['is_qualified']:
            return []

        # S1信号过滤
        surge_start_date = None
        df_asc = df.sort_values('date').reset_index(drop=True)
        start_idx = build_quality.get('surge_start_idx')
        if start_idx is not None and start_idx < len(df_asc):
            surge_start_date = df_asc.iloc[start_idx]['date']
        has_s1, _, _ = detect_s1_signal(df, lookback_days=35, surge_start_date=surge_start_date)
        if has_s1:
            return []

        position_ok = (latest.get('fall_in_bowl', False) or
                       latest.get('near_yellow', False) or
                       latest.get('near_white', False))
        is_washout = self._detect_washout_exception(df)

        if not position_ok and not is_washout:
            return []

        # 超级B1识别（仅供标记，用于后续加仓决策）
        is_super_b1 = False
        if position_ok or is_washout:
            is_super_b1 = self._detect_super_b1(df)

        reasons = ['击穿对手盘' if is_washout else '标准B1']
        if is_super_b1:
            reasons.insert(0, '超级B1')
        reasons.append(f"涨幅{build_quality['total_gain']:.1f}%")
        reasons.append(f"换手{build_quality['surge_turnover_sum']:.1f}%")
        if build_quality.get('has_limit_up'):
            reasons.append('含涨停')

        return [{
            'date': latest['date'],
            'close': round(latest['close'], 2),
            'J': round(latest['J'], 2),
            'category': 'unified_b1',
            'is_washout': is_washout,
            'is_super_b1': is_super_b1,         # ★ 新增字段
            'build_gain': build_quality['total_gain'],
            'surge_turnover': build_quality['surge_turnover_sum'],
            'has_limit_up': build_quality.get('has_limit_up', False),
            'has_shrink_limit_up': build_quality.get('has_shrink_limit_up', False),
            'has_one_word_limit': build_quality.get('has_one_word_limit', False),
            'surge_start_date': surge_start_date.strftime('%Y-%m-%d') if surge_start_date is not None else None,
            'reasons': reasons
        }]

    def _detect_super_b1(self, df: pd.DataFrame) -> bool:
        """超级B1检测：20日内出现过B1，J值曾回升至≥20，当前J<20且斜率变缓，股价未脱离成本区"""
        if len(df) < 40:
            return False
        # 使用正序数据
        asc = df.sort_values('date').reset_index(drop=True)
        n = len(asc)
        # 查找最近20个交易日内是否出现过B1信号（简化：J<30且缩量且双线多头）
        lookback = 20
        start_idx = max(0, n - lookback)
        recent = asc.iloc[start_idx:]
        b1_dates = []
        for i in range(1, len(recent)):
            row = recent.iloc[i]
            if (row['J'] < self.params['j_threshold'] and
                row['white_gt_yellow'] and
                row['volume_shrink']):
                b1_dates.append(recent.index[i])
        if not b1_dates:
            return False
        last_b1_idx = b1_dates[-1]  # 最近一次B1所在位置
        # 检查B1之后J值是否曾大于等于20
        after_b1 = asc.loc[last_b1_idx:]
        if (after_b1['J'] >= 20).any():
            # 当前J值<20且斜率变缓
            cur_j = asc.iloc[-1]['J']
            if cur_j < self.params['j_super_threshold']:
                # 斜率变缓检查
                if n >= 3:
                    j0 = asc.iloc[-1]['J']
                    j1 = asc.iloc[-2]['J']
                    j2 = asc.iloc[-3]['J']
                    if (j0 - j1) < (j1 - j2):
                        return False
                # 股价脱离成本区检查：与上次B1拐头时的收盘价相比，涨跌幅<4%
                b1_price = asc.loc[last_b1_idx, 'close']
                cur_price = asc.iloc[-1]['close']
                if abs(cur_price / b1_price - 1) < 0.04:
                    return True
        return False