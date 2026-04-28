"""
基于知行指标的特征提取模块
复用项目已有的 technical.py 指标计算
"""
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.technical import (
    MA, EMA, KDJ, calculate_zhixing_trend, REF, LLV, HHV
)


class PatternFeatureExtractor:
    """从股票数据中提取完美图形特征"""
    
    def __init__(self, lookback_days=25):
        self.lookback_days = lookback_days
    
    def extract(self, df: pd.DataFrame, lookback_days: int = None) -> dict:
        """
        提取完整特征向量
        df: 倒序排列的DataFrame（最新在前）
        lookback_days: 回看天数，None则使用默认值
        """
        if df.empty or len(df) < 10:
            return self._empty_features()
        
        days = lookback_days if lookback_days is not None else self.lookback_days
        window_df = df.head(days).copy()
        window_df = window_df.sort_values('date').reset_index(drop=True)

        # 计算知行指标
        trend_df = calculate_zhixing_trend(window_df)
        window_df['short_term_trend'] = trend_df['short_term_trend']
        window_df['bull_bear_line'] = trend_df['bull_bear_line']

        # 计算KDJ
        kdj_df = KDJ(window_df, n=9, m1=3, m2=3)
        window_df['K'] = kdj_df['K']
        window_df['D'] = kdj_df['D']
        window_df['J'] = kdj_df['J']

        features = {
            "trend_structure": self._extract_trend_features(window_df),
            "kdj_state": self._extract_kdj_features(window_df),
            "volume_pattern": self._extract_volume_features(window_df),
            "price_shape": self._extract_shape_features(window_df),
            "move_strength": self._extract_move_strength_features(window_df),
            "build_health": self._extract_build_health(window_df),
        }

        return features

    def _empty_features(self) -> dict:
        return {
            "trend_structure": {},
            "kdj_state": {},
            "volume_pattern": {},
            "price_shape": {},
            "move_strength": {},
            "build_health": {},
        }

    def _extract_trend_features(self, df: pd.DataFrame) -> dict:
        if len(df) < 5:
            return {}

        latest = df.iloc[-1]

        short_bullbear_ratio = latest['short_term_trend'] / latest['bull_bear_line'] if latest['bull_bear_line'] != 0 else 1.0

        short_slope = (df['short_term_trend'].iloc[-1] / df['short_term_trend'].iloc[-5] - 1) * 100 if df['short_term_trend'].iloc[-5] != 0 else 0
        bullbear_slope = (df['bull_bear_line'].iloc[-1] / df['bull_bear_line'].iloc[-5] - 1) * 100 if df['bull_bear_line'].iloc[-5] != 0 else 0

        price_vs_short_pct = (latest['close'] - latest['short_term_trend']) / latest['short_term_trend'] * 100 if latest['short_term_trend'] != 0 else 0
        price_vs_bullbear_pct = (latest['close'] - latest['bull_bear_line']) / latest['bull_bear_line'] * 100 if latest['bull_bear_line'] != 0 else 0

        is_in_bowl = (latest['short_term_trend'] > latest['close'] > latest['bull_bear_line'])
        trend_spread_pct = (latest['short_term_trend'] - latest['bull_bear_line']) / latest['bull_bear_line'] * 100 if latest['bull_bear_line'] != 0 else 0

        avg_trend = (latest['short_term_trend'] + latest['bull_bear_line']) / 2
        price_bias_pct = (latest['close'] - avg_trend) / avg_trend * 100 if avg_trend != 0 else 0

        return {
            "short_vs_bullbear": round(short_bullbear_ratio, 4),
            "short_slope": round(short_slope, 4),
            "bullbear_slope": round(bullbear_slope, 4),
            "price_vs_short_pct": round(price_vs_short_pct, 4),
            "price_vs_bullbear_pct": round(price_vs_bullbear_pct, 4),
            "is_in_bowl": is_in_bowl,
            "trend_spread_pct": round(trend_spread_pct, 4),
            "price_bias_pct": round(price_bias_pct, 4),
        }

    def _extract_kdj_features(self, df: pd.DataFrame) -> dict:
        if len(df) < 2 or 'J' not in df.columns:
            return {}

        latest = df.iloc[-1]
        j_values = df['J'].values

        if len(j_values) >= 5:
            x = np.arange(5)
            recent_j = j_values[-5:]
            j_trend = np.polyfit(x, recent_j, 1)[0] if not np.isnan(recent_j).any() else 0
        else:
            j_trend = 0

        k_cross_d = False
        if len(df) >= 2 and not pd.isna(latest['K']) and not pd.isna(latest['D']):
            prev = df.iloc[-2]
            k_cross_d = (prev['K'] < prev['D']) and (latest['K'] > latest['D'])

        j_val = latest['J'] if not pd.isna(latest['J']) else 50
        if j_val <= 30:
            j_position = "低位"
        elif j_val >= 80:
            j_position = "高位"
        else:
            j_position = "中位"

        j_rebound = j_values[-1] > j_values[-3] if len(j_values) >= 3 else False

        if len(df) >= 10:
            price_low_idx = df['close'].iloc[-10:].idxmin()
            j_low_idx = df['J'].iloc[-10:].idxmin()
            j_divergence = price_low_idx > j_low_idx
        else:
            j_divergence = False

        return {
            "j_value": round(float(j_val), 2),
            "j_trend": round(float(j_trend), 4),
            "j_min_lookback": round(float(df['J'].min()), 2),
            "k_cross_d": k_cross_d,
            "j_position": j_position,
            "j_rebound": j_rebound,
            "j_divergence": j_divergence,
        }

    def _extract_volume_features(self, df: pd.DataFrame) -> dict:
        if 'volume' not in df.columns or len(df) < 5:
            return {}

        volumes = df['volume'].values

        if 'turnover' in df.columns:
            turnovers = df['turnover'].values
            recent_turnover_avg = np.mean(turnovers[-5:]) if len(turnovers) >= 5 else 0
            long_turnover_avg = np.mean(turnovers[-20:]) if len(turnovers) >= 20 else recent_turnover_avg
            turnover_ratio = recent_turnover_avg / long_turnover_avg if long_turnover_avg > 0 else 1.0
            max_turnover = np.max(turnovers) if len(turnovers) > 0 else 0
            if len(turnovers) >= 3:
                turnover_ma3 = np.convolve(turnovers, np.ones(3) / 3, mode='valid')
                if len(turnover_ma3) >= 5:
                    x = np.arange(5)
                    y = turnover_ma3[-5:]
                    turnover_slope = np.polyfit(x, y, 1)[0] if not np.isnan(y).any() else 0
                else:
                    turnover_slope = 0
        else:
            turnover_ratio = 1.0
            max_turnover = 0
            turnover_slope = 0

        price_changes = df['close'].pct_change() * 100
        big_gain_mask = price_changes >= 4.0
        if big_gain_mask.any() and 'turnover' in df.columns:
            big_gain_turnover_avg = df.loc[big_gain_mask, 'turnover'].mean()
        else:
            big_gain_turnover_avg = 0.0

        if len(volumes) >= 10:
            recent_avg = np.mean(volumes[-10:])
            before_avg = np.mean(volumes[-20:-10]) if len(volumes) >= 20 else recent_avg
            avg_volume_ratio = recent_avg / before_avg if before_avg > 0 else 1.0
        else:
            avg_volume_ratio = 1.0

        vol_ratios = []
        for i in range(1, min(len(volumes), 20)):
            if volumes[i - 1] > 0:
                vol_ratios.append(volumes[i] / volumes[i - 1])
        max_volume_ratio = max(vol_ratios) if vol_ratios else 1.0

        shrink_then_expand = self._detect_shrink_expand(volumes)

        key_candles = 0
        for i in range(len(df)):
            if i > 0 and df['volume'].iloc[i] > df['volume'].iloc[i - 1] * 2 and df['close'].iloc[i] > df['open'].iloc[i]:
                key_candles += 1

        volume_trend = self._classify_volume_trend(volumes)

        return {
            "avg_volume_ratio": round(float(avg_volume_ratio), 2),
            "max_volume_ratio": round(float(max_volume_ratio), 2),
            "volume_trend": volume_trend,
            "key_candles_count": int(key_candles),
            "shrink_then_expand": shrink_then_expand,
            "turnover_ratio": round(float(turnover_ratio), 4),
            "max_turnover": round(float(max_turnover), 4),
            "turnover_slope": round(float(turnover_slope), 4),
            "volume_compress": round(float(volumes[-1] / np.mean(volumes[-20:])), 4) if len(volumes) >= 20 else 1.0,
            "big_gain_turnover_avg": round(float(big_gain_turnover_avg), 4),
        }

    def _extract_shape_features(self, df: pd.DataFrame) -> dict:
        if len(df) < 5:
            return {}

        closes = df['close'].values

        price_min = closes.min()
        price_max = closes.max()
        if price_max > price_min:
            normalized = (closes - price_min) / (price_max - price_min)
        else:
            normalized = np.zeros_like(closes)

        FIXED_LEN = 100
        if len(normalized) != FIXED_LEN:
            x_orig = np.linspace(0, 1, len(normalized))
            x_new = np.linspace(0, 1, FIXED_LEN)
            normalized = np.interp(x_new, x_orig, normalized)

        peak = np.maximum.accumulate(closes)
        drawdown = (peak - closes) / peak
        max_drawdown = drawdown.max() * 100

        breakout_strength = (closes[-1] / closes[-2] - 1) * 100 if len(closes) >= 2 else 0

        if len(closes) >= 2:
            returns = np.diff(closes) / closes[:-1]
            volatility = np.std(returns) * 100
        else:
            volatility = 0

        consolidation_days = self._count_consolidation_days(df)

        overall_trend = "上升" if closes[-1] > closes[0] * 1.05 else "下降" if closes[-1] < closes[0] * 0.95 else "震荡"

        last_close = df['close'].iloc[-1]
        short_trend = df['short_term_trend'].iloc[-1] if 'short_term_trend' in df.columns else last_close
        near_short_trend = abs(last_close - short_trend) / short_trend < 0.03

        df['amplitude'] = (df['high'] - df['low']) / df['close']
        volatility_shrink = df['amplitude'].iloc[-5:].mean() / df['amplitude'].iloc[-20:].mean() if len(df) >= 20 else 1.0

        return {
            "consolidation_days": int(consolidation_days),
            "max_drawdown": round(float(max_drawdown), 2),
            "breakout_strength": round(float(breakout_strength), 2),
            "normalized_curve": normalized.tolist(),
            "volatility": round(float(volatility), 4),
            "overall_trend": overall_trend,
            "near_short_trend": near_short_trend,
            "volatility_shrink": volatility_shrink,
        }

    def _detect_shrink_expand(self, volumes: np.ndarray) -> bool:
        if len(volumes) < 10:
            return False
        mid = len(volumes) // 2
        early_avg = np.mean(volumes[:mid])
        late_avg = np.mean(volumes[mid:])
        overall_avg = np.mean(volumes)
        return late_avg > early_avg * 1.3 and early_avg < overall_avg * 0.9

    def _classify_volume_trend(self, volumes: np.ndarray) -> str:
        if len(volumes) < 5:
            return "unknown"
        x = np.arange(len(volumes))
        slope = np.polyfit(x, volumes, 1)[0]
        avg_vol = np.mean(volumes)
        slope_pct = slope / avg_vol * 100 if avg_vol > 0 else 0
        if slope_pct > 5:
            return "持续放量"
        elif slope_pct < -5:
            return "持续缩量"
        elif self._detect_shrink_expand(volumes):
            return "缩量后放量"
        else:
            return "量能平稳"

    def _count_consolidation_days(self, df: pd.DataFrame) -> int:
        if len(df) < 5:
            return 0
        closes = df['close'].values
        max_price = closes.max()
        min_price = closes.min()
        if max_price > 0 and (max_price - min_price) / max_price < 0.10:
            return len(df)
        consolidation_range = 0.05
        max_days = 0
        current_days = 0
        for i in range(len(df) - 5):
            window = closes[i:i+5]
            window_max = window.max()
            window_min = window.min()
            if window_max > 0 and (window_max - window_min) / window_max < consolidation_range:
                current_days += 1
                max_days = max(max_days, current_days)
            else:
                current_days = 0
        return max_days

    def _extract_move_strength_features(self, df: pd.DataFrame) -> dict:
        if len(df) < 5:
            return {}
        key_mask = (df['volume'] > df['volume'].shift(1) * 2) & (df['close'] > df['open'])
        key_indices = df.index[key_mask].tolist()
        if not key_indices:
            return {
                "move_avg_gain": 0,
                "move_max_gain": 0,
                "move_total_gain": 0,
                "move_days": 0,
                "move_first_last_ratio": 0,
            }
        moves = []
        current_move = []
        for idx in key_indices:
            if not current_move or idx == current_move[-1] + 1:
                current_move.append(idx)
            else:
                moves.append(current_move)
                current_move = [idx]
        if current_move:
            moves.append(current_move)
        move_gains = []
        for move in moves:
            start_idx = move[0]
            end_idx = move[-1]
            start_price = df.iloc[start_idx]['close']
            end_price = df.iloc[end_idx]['close']
            gain = (end_price / start_price - 1) * 100
            move_gains.append(gain)
        move_avg_gain = np.mean(move_gains) if move_gains else 0
        move_max_gain = np.max(move_gains) if move_gains else 0
        move_total_gain = np.sum(move_gains) if move_gains else 0
        move_days = sum(len(m) for m in moves)
        if len(move_gains) >= 2:
            first_last_ratio = move_gains[0] / move_gains[-1] if move_gains[-1] != 0 else 1
        else:
            first_last_ratio = 1
        return {
            "move_avg_gain": round(float(move_avg_gain), 2),
            "move_max_gain": round(float(move_max_gain), 2),
            "move_total_gain": round(float(move_total_gain), 2),
            "move_days": int(move_days),
            "move_first_last_ratio": round(float(first_last_ratio), 4),
        }

    def _extract_build_health(self, df: pd.DataFrame) -> dict:
        """建仓波健康度（涨幅、换手累加、均线多头、涨停减分）"""
        if len(df) < 20:
            return {'build_health_score': 0}

        df_asc = df.sort_values('date').reset_index(drop=True)
        close = df_asc['close']
        volume = df_asc['volume']
        high = df_asc['high']
        low = df_asc['low']
        open_price = df_asc['open']

        # 前5日平均量能
        avg_vol_5 = volume.rolling(5, min_periods=1).mean().shift(1)

        pct_chg = close.pct_change() * 100
        gain_cond = pct_chg >= 4.0
        positive_cond = close > open_price
        volume_cond = volume > avg_vol_5
        is_surge_day = gain_cond & positive_cond & volume_cond

        periods = []
        curr = []
        for idx, val in is_surge_day.items():
            if val:
                curr.append(idx)
            elif curr:
                if len(curr) >= 1:
                    periods.append(curr)
                curr = []
        if curr and len(curr) >= 1:
            periods.append(curr)

        if not periods:
            return {'build_health_score': 0}

        last_period = periods[-1]
        period_df = df_asc.loc[last_period]

        start_price = period_df.iloc[0]['close'] if last_period[0] == 0 else df_asc.iloc[last_period[0] - 1]['close']
        end_price = period_df.iloc[-1]['close']
        total_gain = (end_price / start_price - 1) * 100

        pct_chg_period = period_df['close'].pct_change() * 100
        big_gain_mask = pct_chg_period >= 4.0
        surge_turnover_sum = period_df.loc[big_gain_mask, 'turnover'].sum() if 'turnover' in period_df.columns else 0

        # ---------- 均线多头评分 ----------
        ma5 = close.rolling(5).mean().iloc[-1]
        ma10 = close.rolling(10).mean().iloc[-1]
        ma20 = close.rolling(20).mean().iloc[-1]
        ma30 = close.rolling(30).mean().iloc[-1]
        ma40 = close.rolling(40).mean().iloc[-1]

        ma_score = 0
        if ma5 >= ma30:
            if ma40 < ma30:
                ma_score += 25
            if ma30 < ma20:
                ma_score += 25
            if ma20 < ma10:
                ma_score += 25
            if ma10 < ma5:
                ma_score += 25

        # 涨停检测（扣分）
        has_limit_up = False
        has_one_word_limit = False
        vol_ma20 = volume.rolling(20, min_periods=1).mean()
        for idx in last_period:
            row = df_asc.iloc[idx]
            if idx > 0:
                day_gain = (row['close'] - df_asc.iloc[idx - 1]['close']) / df_asc.iloc[idx - 1]['close'] * 100
            else:
                day_gain = 0
            is_limit = (day_gain >= 9.8) or (
                    (row['high'] - row['low']) / row['low'] * 100 < 0.1 and row['close'] == row['high'])
            if is_limit:
                has_limit_up = True
                if (abs(row['open'] - row['close']) / row['open'] * 100 < 0.5 and
                        abs(row['high'] - row['low']) / row['low'] * 100 < 0.5):
                    has_one_word_limit = True
                break

        # 涨幅评分（30%最优，线性衰减）
        gain_score = max(0, 100 - abs(total_gain - 30) * 2)
        # 换手评分（30%最优，线性衰减）
        turnover_score = max(0, 100 - abs(surge_turnover_sum - 30) * 2)

        # 综合健康度 = 涨幅*0.3 + 换手*0.3 + 均线*0.4
        health_score = gain_score * 0.3 + turnover_score * 0.3 + ma_score * 0.4

        if has_one_word_limit:
            health_score = max(0, health_score - 40)
        elif has_limit_up:
            health_score = max(0, health_score - 20)

        return {
            'build_health_score': round(health_score, 2),
            'build_gain': round(total_gain, 2),
            'surge_turnover_sum': round(surge_turnover_sum, 2),
            'ma_score': round(ma_score, 2),
            'has_limit_up': has_limit_up,
            'has_one_word_limit': has_one_word_limit
        }