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



class UnifiedB1Strategy(BaseStrategy):
    """统一B1策略"""

    def __init__(self, params=None):
        default_params = {
            'M1': 14, 'M2': 28, 'M3': 57, 'M4': 114,
            'white_period': 10,
            'j_threshold': 34,
            'j_super_threshold': 20,
            'volume_shrink_ratio': 0.9,
            'cap_threshold': 4000000000,
            'max_gain_pct': 60,
            'max_surge_turnover': 90,          # ★ 换手累加上限改为90%
            'near_pct': 3.5,
            # 区域过滤（优先级高于 j_threshold / volume_shrink_ratio）
            'zone_j_ranges': None,       # 如 [(-10,-7),(23,33)] 或 None
            'zone_vol_ranges': None,     # 如 [(0.1,0.65),(0.8,0.9)] 或 None
            # 主观条件开关（回测验证用，False=正常生效）
            'skip_wave_quality': False,   # 跳过波质量检测(异动量/积累均量>2)
            'skip_wave_break': False,     # 跳过波断检查
            'skip_s1': False,             # 跳过S1出货信号检测
            'skip_washout': False,        # 跳过击穿对手盘（不用此通道）
            'skip_bullish_max_vol': False,   # 跳过最大量阳量检查
            'skip_2nd_max_vol_bullish': False, # 跳过第二大量阳量检查
            'skip_max_high_vol_rank': False, # 跳过最高价量排名检查
            'red_green_vol_ratio': 1.2,   # 红量/绿量最低比值
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
        """建仓波质量检测 - v2两阶段异动群识别"""
        if len(df) < 20:
            return {'total_gain': 0, 'surge_turnover_sum': 0, 'is_qualified': False,
                    'has_limit_up': False, 'has_shrink_limit_up': False, 'has_one_word_limit': False,
                    'surge_start_idx': None, 'wave_quality': 'weak'}
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

        # 阶段一：找异动群（间隔<=3天合并）
        surge_indices = df_asc.index[is_surge_day].tolist()
        if not surge_indices:
            return {'total_gain': 0, 'surge_turnover_sum': 0, 'is_qualified': False,
                    'has_limit_up': False, 'has_shrink_limit_up': False, 'has_one_word_limit': False,
                    'surge_start_idx': None}

        groups = []
        current = [surge_indices[0]]
        for idx in surge_indices[1:]:
            if idx - current[-1] <= 4:
                current.append(idx)
            else:
                groups.append(current)
                current = [idx]
        groups.append(current)

        # 异动群摘要：每群的终点 = 异动启动日到下一群之间的最高收盘价
        group_info = []
        for gi, g in enumerate(groups):
            pre_idx = max(0, g[0] - 1)
            start_close = close.iloc[pre_idx] if pre_idx < g[0] else open_price.iloc[g[0]]
            # 群的终点：从群起点到下一个群起点（或末尾）之间，最高价的位置
            next_start = groups[gi + 1][0] if gi + 1 < len(groups) else len(df_asc)
            seg = df_asc.iloc[g[0]:next_start]
            max_high_idx = seg['high'].idxmax()
            # 异动期最大量：从群起点到最高价之间，取成交量最大值
            surge_max_vol = float(df_asc.iloc[g[0]:max_high_idx + 1]['volume'].max())
            group_info.append({
                'start_idx': int(g[0]),
                'end_idx': int(max_high_idx),    # ★ 群的终点 = 最高收盘价
                'surge_max_vol': surge_max_vol,
                'start_close': float(start_close),
            })

        # 阶段二：从末群往前追溯合并
        last = group_info[-1]
        for g in reversed(group_info[:-1]):
            gap_days = last['start_idx'] - g['end_idx'] - 1
            if gap_days > 15:
                break
            # 波断检查：gap期间是否跌破黄线+止损
            gap_slice = df_asc.iloc[g['end_idx'] + 1:last['start_idx']]
            wave_broken = False
            if 'yellow_line' in df_asc.columns and len(gap_slice) > 0:
                check_range = df_asc.iloc[g['end_idx'] + 1:last['start_idx'] + 1]
                yv = check_range['yellow_line']; cv = check_range['close']; lv = check_range['low']
                for gi in range(len(cv)):
                    if cv.iloc[gi] < yv.iloc[gi]:
                        stop_ref = lv.iloc[gi] - 0.05
                        if gi + 1 < len(cv) and any(cv.iloc[gi+1:].values < stop_ref):
                            wave_broken = True
                            break
            if wave_broken:
                break
            # 量能收缩：gap后段均量需 < 前群异动期最大量×0.8
            late_n = min(5, max(3, len(gap_slice) // 2))
            late_vol = gap_slice['volume'].tail(late_n).mean() if late_n > 0 else 0
            if late_vol >= g['surge_max_vol'] * 0.7:
                break
            # 不破位（前群起点*90%）
            gap_low = gap_slice['close'].min()
            if gap_low < g['start_close'] * 0.90:
                break
            # 合并
            last['start_idx'] = g['start_idx']
            last['start_close'] = g['start_close']
            last['surge_max_vol'] = max(last['surge_max_vol'], g['surge_max_vol'])

        start_idx = last['start_idx']
        wave_end = len(df_asc)

        # gap_up/涨停信息保留（供匹配器评分），不再硬拦截
        long_shadow_count = 0
        for i in range(start_idx, wave_end):
            if is_long_upper_shadow(df_asc.iloc[i]):
                long_shadow_count += 1
        shadow_limit = max(3, (wave_end - start_idx) // 6)  # 每6个交易日允许多1根，最少3根
        if long_shadow_count > shadow_limit:
            return {'total_gain': 0, 'surge_turnover_sum': 0, 'is_qualified': False,
                    'has_limit_up': False, 'has_shrink_limit_up': False, 'has_one_word_limit': False,
                    'surge_start_idx': None}

        pre_idx = max(0, start_idx - 1)
        start_price = df_asc.iloc[pre_idx]['close'] if pre_idx < start_idx else df_asc.iloc[start_idx]['open']
        max_high_idx = start_idx
        max_close = start_price
        for i in range(start_idx, len(df_asc)):
            if df_asc.iloc[i]['close'] > max_close:
                max_close = df_asc.iloc[i]['close']
                max_high_idx = i
        for i in range(start_idx, max_high_idx):
            if i > 0:
                day_gain = (df_asc.iloc[i]['close'] - df_asc.iloc[i-1]['close']) / df_asc.iloc[i-1]['close'] * 100
                if day_gain <= -5 and df_asc.iloc[i]['volume'] > avg_vol_5.iloc[i] * 1.5:
                    max_high_idx = i - 1
                    break
        end_price = df_asc.iloc[max_high_idx]['close']
        total_gain = (end_price / start_price - 1) * 100

        surge_turnover_sum = 0.0
        if 'turnover' in df_asc.columns:
            for i in range(start_idx, max_high_idx + 1):
                if is_surge_day.iloc[i]:
                    surge_turnover_sum += df_asc.iloc[i]['turnover']

        period_df = df_asc.iloc[start_idx:max_high_idx+1]
        positive_vol = period_df[period_df['close'] > period_df['open']]['volume'].sum()
        negative_vol = period_df[period_df['close'] < period_df['open']]['volume'].sum()
        if positive_vol <= negative_vol * self.params['red_green_vol_ratio']:
            return {'total_gain': round(total_gain, 2), 'surge_turnover_sum': round(surge_turnover_sum, 2),
                    'is_qualified': False, 'has_limit_up': False, 'has_shrink_limit_up': False,
                    'has_one_word_limit': False, 'surge_start_idx': start_idx}

        has_limit_up = False
        has_shrink_limit_up = False
        has_one_word_limit = False
        for i in range(start_idx, max_high_idx + 1):
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

        # 异动起始日~选股日 量价条件（共用full_slice）
        if is_qualified:
            full_slice = df_asc.iloc[start_idx:]
            sorted_vols = sorted(full_slice['volume'].values, reverse=True) if len(full_slice) > 0 else []

            # 最大量那天必须为阳量
            if not self.params.get('skip_bullish_max_vol', False) and len(full_slice) > 0:
                max_vol_idx = full_slice['volume'].idxmax()
                if df_asc.iloc[max_vol_idx]['close'] <= df_asc.iloc[max_vol_idx]['open']:
                    is_qualified = False

            # 第二大成交量的那天必须为阳量
            if is_qualified and not self.params.get('skip_2nd_max_vol_bullish', False) and len(full_slice) >= 2:
                # 找第二大量对应的日期
                second_vol = sorted_vols[1]
                second_mask = full_slice['volume'] == second_vol
                # 取第一次出现的位置（处理平量情况）
                second_idx = full_slice[second_mask].index[0]
                if df_asc.iloc[second_idx]['close'] <= df_asc.iloc[second_idx]['open']:
                    is_qualified = False

            # 最高价那天必须阳线 + 成交量前2名
            if is_qualified and not self.params.get('skip_max_high_vol_rank', False) and len(full_slice) >= 2:
                max_high_idx2 = full_slice['high'].idxmax()
                max_high_row = df_asc.iloc[max_high_idx2]
                if max_high_row['close'] <= max_high_row['open']:
                    is_qualified = False
                else:
                    max_high_vol = max_high_row['volume']
                    threshold = sorted_vols[1] if len(sorted_vols) >= 2 else sorted_vols[0]
                    if max_high_vol < threshold:
                        is_qualified = False

        # 涨停信息保留为 has_limit_up/has_one_word_limit/has_shrink_limit_up
        # 不再硬拦截，交给 pattern_matcher._calc_limit_similarity 评分

        # 波质量检测：异动日量 / 积累区(前10日)均量 > 2.0
        wave_quality = 'weak'
        if start_idx > 0:
            pre_range = max(0, start_idx - 10)
            pre_slice = df_asc.iloc[pre_range:start_idx]
            if len(pre_slice) >= 3:
                vol_base = pre_slice['volume'].median()
                vol_surge = df_asc.iloc[start_idx]['volume']
                if vol_base > 0 and vol_surge / vol_base > 2.0:
                    wave_quality = 'healthy'

        # 异动起始日~选股日: 最高价日量排名 (硬条件保证只有rank=1或2)
        max_high_vol_rank = 0
        vol_resonance_score = 0.0
        if start_idx is not None:
            full_slice = df_asc.iloc[start_idx:]
            if len(full_slice) >= 2:
                sorted_vols = sorted(full_slice['volume'].values, reverse=True)
                mh_idx = full_slice['high'].idxmax()
                mh_vol = df_asc.iloc[mh_idx]['volume']
                if mh_vol >= sorted_vols[0]:
                    max_high_vol_rank = 1
                    vol_resonance_score = 1.0
                elif mh_vol >= sorted_vols[1]:
                    max_high_vol_rank = 2
                    vol_resonance_score = 0.0

        return {
            'total_gain': round(total_gain, 2),
            'surge_turnover_sum': round(surge_turnover_sum, 2),
            'is_qualified': is_qualified,
            'has_limit_up': has_limit_up,
            'has_shrink_limit_up': has_shrink_limit_up,
            'has_one_word_limit': has_one_word_limit,
            'surge_start_idx': start_idx,
            'wave_quality': wave_quality,
            'max_high_vol_rank': max_high_vol_rank,
            'vol_resonance_score': vol_resonance_score,
        }

    def _check_wave_break(self, df: pd.DataFrame, surge_start_idx: int) -> dict:
        """检测异动波是否已被打断：跌破黄线+破止损→切断区间，无新异动则排除
        返回: {'broken': bool, 'new_surge_idx': int or None}"""
        if surge_start_idx is None:
            return {'broken': False, 'new_surge_idx': None}

        df_asc = df.sort_values('date').reset_index(drop=True)
        if 'yellow_line' not in df_asc.columns:
            return {'broken': False, 'new_surge_idx': None}

        close = df_asc['close']
        low = df_asc['low']
        yellow = df_asc['yellow_line']
        volume = df_asc['volume']
        open_p = df_asc['open']

        # 从异动起点往后扫描
        for i in range(surge_start_idx + 1, len(df_asc)):
            # 跌破黄线
            if close.iloc[i] < yellow.iloc[i]:
                # 止损 = 当日最低价 - 0.05
                stop_ref = low.iloc[i] - 0.05
                # 扫描后续所有天，是否曾跌破止损
                broke_stop = False
                break_day = None
                for j in range(i + 1, len(df_asc)):
                    if close.iloc[j] < stop_ref:
                        broke_stop = True
                        break_day = j
                        break
                if not broke_stop:
                    return {'broken': False, 'new_surge_idx': None}  # 虽破黄线但未破止损，不断

                # 确认跌破：寻找此后的新异动
                after_break = df_asc.iloc[break_day:]
                avg_vol_5 = after_break['volume'].rolling(5, min_periods=1).mean().shift(1)
                pct_chg = after_break['close'].pct_change() * 100
                surge = (pct_chg >= 4.0) & (after_break['close'] > after_break['open']) & (after_break['volume'] > avg_vol_5)
                surge_indices = after_break.index[surge].tolist()
                if surge_indices:
                    new_idx = surge_indices[0]
                    start = new_idx
                    gap = 0
                    for k in range(new_idx - 1, break_day - 1, -1):
                        if surge.iloc[k - break_day] if k >= break_day else False:
                            if gap <= 5:
                                start = k
                                gap = 0
                            else:
                                break
                        else:
                            gap += 1
                    return {'broken': True, 'new_surge_idx': start}
                else:
                    return {'broken': True, 'new_surge_idx': None}

        return {'broken': False, 'new_surge_idx': None}

    def _detect_washout_exception(self, df: pd.DataFrame, surge_start_date=None) -> bool:
        """击穿对手盘选股通道：异动波内当前价在黄线下方（洗盘进行中），且不破止损"""
        if len(df) < 10:
            return False

        # 以异动起点为界
        if surge_start_date is not None:
            sdt = pd.to_datetime(surge_start_date)
            recent = df[df['date'] >= sdt].copy()
        else:
            recent = df.head(20).copy()
        if len(recent) < 5:
            return False
        recent = recent.sort_values('date').reset_index(drop=True)
        if 'bull_bear_line' in recent.columns:
            hl_col = 'bull_bear_line'
        elif 'yellow_line' in recent.columns:
            hl_col = 'yellow_line'
        else:
            return False

        latest = recent.iloc[-1]
        close = latest['close']
        yellow = latest[hl_col]

        # 条件1: 当前价必须在黄线下方（正在洗盘中，未收回）
        if close >= yellow:
            return False

        # 条件2: 找到最近的破位日（前一日在黄线上方，当日跌破）
        break_idx = None
        for i in range(len(recent) - 1, 0, -1):
            cur = recent.iloc[i]
            prev = recent.iloc[i - 1]
            if cur['close'] < cur[hl_col] and prev['close'] > prev[hl_col]:
                break_idx = i
                break

        if break_idx is None:
            return False

        # 条件3: 破位日距当前不超过5个交易日
        if len(recent) - 1 - break_idx > 5:
            return False

        # 条件4: 破位日缩量
        break_row = recent.iloc[break_idx]
        avg_vol_5 = recent.iloc[max(0, break_idx - 5):break_idx]['volume'].mean()
        if break_row['volume'] > avg_vol_5 * 0.7:
            return False

        # 条件5: 洗盘期间最低价，当前价不破止损（最低价 - 0.05）
        washout_low = recent.iloc[break_idx:]['low'].min()
        if close <= washout_low - 0.05:
            return False

        return True

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
        if df.empty or len(df) < self.params['M4']:  # M4=114
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
        # J过滤：优先用区间，否则用阈值
        zone_j = self.params.get('zone_j_ranges')
        if zone_j is not None:
            if not any(lo <= j <= hi for lo, hi in zone_j):
                return []
        elif j >= j_thresh:
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

        # 波质量检测：异动日量 < 积累区均量×2 → 假突破，排除
        if not self.params.get('skip_wave_quality') and build_quality.get('wave_quality') != 'healthy':
            return []

        # ★ 波断检查：异动波是否已被打断（跌破黄线+破止损）
        start_idx = build_quality.get('surge_start_idx')
        if start_idx is not None and not self.params.get('skip_wave_break'):
            wave = self._check_wave_break(df, start_idx)
            if wave['broken']:
                if wave['new_surge_idx'] is None:
                    return []    # 波断后无新异动，排除
                start_idx = wave['new_surge_idx']  # 有新异动，更新起点

        # 正序数据（后续 S1/缩量/超级B1 共用）
        df_asc = df.sort_values('date').reset_index(drop=True)

        # S1不再拦截选股（回测验证：择时+80%相似度下S1对选股质量无影响）
        # S1仅用于持仓管理中减仓50%
        surge_start_date = None
        if start_idx is not None and start_idx < len(df_asc):
            surge_start_date = df_asc.iloc[start_idx]['date']

        # ★ 缩量条件动态重算（限制在异动波内）
        df_vol = df
        if start_idx is not None and start_idx < len(df_asc):
            df_vol = df_asc.iloc[start_idx:].sort_values('date', ascending=False).reset_index(drop=True)
        vol_20d = df_vol.head(20)['volume']
        hhv_vol_20 = vol_20d.max()
        raw_vol_ratio = latest['volume'] / hhv_vol_20 if hhv_vol_20 > 0 else 1.0
        zone_vol = self.params.get('zone_vol_ranges')
        if zone_vol is not None:
            if not any(lo <= raw_vol_ratio <= hi for lo, hi in zone_vol):
                return []
        elif latest['volume'] >= hhv_vol_20 * self.params['volume_shrink_ratio']:
            return []

        # ★ 位置条件动态重算（不从缓存列读，保证 near_pct 可调）
        close = latest['close']
        yellow = latest['yellow_line']
        white = latest['white_line']
        fall_in_bowl = (close >= yellow) and (close <= white)
        near_pct = self.params['near_pct'] / 100.0
        near_yellow = (close >= yellow) and ((close - yellow) / yellow <= near_pct)
        near_white = abs(close - white) / white <= near_pct
        position_ok = (fall_in_bowl or near_yellow or near_white)
        is_washout = False if self.params.get('skip_washout') else self._detect_washout_exception(df, surge_start_date)

        if not position_ok and not is_washout:
            return []

        # 超级B1识别（仅供标记，用于后续加仓决策）
        is_super_b1 = False
        if position_ok or is_washout:
            is_super_b1 = self._detect_super_b1(df, surge_start_date)

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
            'max_high_vol_rank': build_quality.get('max_high_vol_rank', 0),
            'vol_resonance_score': build_quality.get('vol_resonance_score', 0.2),
            'surge_start_date': surge_start_date.strftime('%Y-%m-%d') if surge_start_date is not None else None,
            'reasons': reasons
        }]

    def _detect_super_b1(self, df: pd.DataFrame, surge_start_date=None) -> bool:
        """超级B1检测：异动波内出现过B1，J值曾回升至≥20，当前J<20且斜率变缓，股价未脱离成本区"""
        if len(df) < 40:
            return False
        asc = df.sort_values('date').reset_index(drop=True)
        n = len(asc)
        # 查找范围：从异动起点开始
        if surge_start_date is None:
            return False
        surge_dt = pd.to_datetime(surge_start_date)
        mask = asc['date'] >= surge_dt
        if mask.sum() < 5:
            return False
        start_idx = mask.idxmax()
        recent = asc.iloc[start_idx:]
        b1_dates = []
        for i in range(1, len(recent)):
            row = recent.iloc[i]
            zj = self.params.get('zone_j_ranges')
            j_ok = any(lo <= row['J'] <= hi for lo, hi in zj) if zj else row['J'] < self.params['j_threshold']
            if (j_ok and row['white_gt_yellow'] and row['volume_shrink']):
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