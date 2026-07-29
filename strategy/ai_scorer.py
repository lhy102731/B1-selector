"""
AI多因子评分模块
基于师兄交割单逆向推理 + 概念共振分析
"""
import json, re
import pandas as pd
import numpy as np
from pathlib import Path


class AIScorer:
    """AI多因子评分器 - 直接评估B1信号质量（非历史相似度匹配）"""

    # 元概念（过于宽泛，不参与计数和共振）
    META_CONCEPTS = {
        '融资融券', '深股通', '沪股通', '证金持股', '同花顺漂亮50',
        '标普道琼斯A股', 'MSCI概念', '富时罗素概念', '富时罗素概念股',
    }

    def __init__(self, data_dir=None):
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data'
        self.data_dir = Path(data_dir)
        self.concept_data = None        # concept.json
        self.concept_size = {}           # concept_name -> stock count
        self.stock_concepts = {}         # stock_code -> [concept_names]
        self.signal_cache = None         # date_str -> [signal_dicts]
        self._loaded = False

    def _load(self):
        if self._loaded:
            return
        # 加载概念定义
        concept_path = self.data_dir / 'block' / 'concept.json'
        if concept_path.exists():
            with open(concept_path, 'r', encoding='utf-8') as f:
                self.concept_data = json.load(f)
            # 构建概念大小和股票映射
            for bk, stocks in self.concept_data.get('stock_map', {}).items():
                name = self.concept_data['name_map'].get(bk, bk)
                if name not in self.META_CONCEPTS:
                    self.concept_size[name] = len(stocks)
            self.stock_concepts = self.concept_data.get('stock_to_blocks', {})
        self._loaded = True

    def load_signal_cache(self, cache_path=None):
        """加载预计算信号缓存用于概念共振计算"""
        import pickle
        if cache_path is None:
            cache_path = self.data_dir / 'signal_cache'
            files = list(cache_path.glob('2025*.pkl'))
            if files:
                cache_path = files[0]
            else:
                return
        with open(cache_path, 'rb') as f:
            self.signal_cache = pickle.load(f)

    # ------------------------------------------------------------------
    # 因子计算
    # ------------------------------------------------------------------
    def _compute_J_score(self, J):
        """J值: 师兄均值4.7, 信号池均值15.4 — 越低越好"""
        if J is None or pd.isna(J):
            return 0
        if -5 <= J < 10:
            return 1.0
        elif -15 <= J < -5:
            return 0.6
        elif 10 <= J < 15:
            return 0.4
        elif 15 <= J < 25:
            return 0.1
        else:
            return -0.3

    def _compute_position_score(self, dist_yellow, dist_white):
        """距黄线 2-8%最优"""
        if dist_yellow is None or pd.isna(dist_yellow):
            return 0
        if 2 <= dist_yellow < 8:
            return 1.0
        elif 0 <= dist_yellow < 2:
            return 0.6
        elif -3 <= dist_yellow < 0:
            return 0.8  # 黄线下方，击穿洗盘
        elif 8 <= dist_yellow < 12:
            return 0.3
        else:
            return -0.1

    def _compute_volume_score(self, vol_ratio):
        """量比 >1放量确认"""
        if vol_ratio is None or pd.isna(vol_ratio):
            return 0
        if 1.0 <= vol_ratio < 2.0:
            return 1.0
        elif 0.8 <= vol_ratio < 1.0:
            return 0.5
        elif vol_ratio >= 2.0:
            return 0.3  # 放量太大也不好
        else:
            return 0.0

    def _compute_candle_score(self, candle_body):
        """K线形态: 阳线或十字星最优(师兄数据: 均值+0.88)"""
        if candle_body is None or pd.isna(candle_body):
            return 0
        if 0 <= candle_body < 2:
            return 1.0  # 小阳线(洗盘结束信号)
        elif -1 <= candle_body < 0:
            return 0.8  # 十字星
        elif 2 <= candle_body < 5:
            return 0.5  # 中阳
        elif -3 <= candle_body < -1:
            return 0.3  # 小阴
        else:
            return 0.1

    def _compute_kdj_days_score(self, k_below_d_days):
        """K<D持续5-8天最优"""
        if k_below_d_days is None:
            return 0
        if 5 <= k_below_d_days <= 8:
            return 1.0
        elif 3 <= k_below_d_days < 5:
            return 0.6
        elif 8 < k_below_d_days <= 15:
            return 0.4
        else:
            return 0.1

    def _compute_concept_coverage_score(self, n_concepts):
        """概念覆盖数 >7个加分"""
        if n_concepts < 3:
            return 0
        elif n_concepts < 7:
            return 0.2
        elif n_concepts < 12:
            return 0.5
        elif n_concepts < 18:
            return 0.8
        else:
            return 1.0

    def _compute_ret5d_score(self, ret_5d):
        """前5日回调 -4~-8%最优"""
        if ret_5d is None or pd.isna(ret_5d):
            return 0
        if -8 <= ret_5d <= -4:
            return 1.0
        elif -12 <= ret_5d < -8:
            return 0.5
        elif -4 < ret_5d <= -1:
            return 0.6
        elif ret_5d > 2:
            return -0.2  # 没回调
        else:
            return 0.3

    def _compute_DIF_score(self, DIF):
        if DIF is None or pd.isna(DIF):
            return 0
        if DIF > 2:
            return 1.0
        elif DIF > 1:
            return 0.7
        elif DIF > 0:
            return 0.4
        else:
            return -0.2

    def _compute_white_slope_score(self, slope):
        if slope is None or pd.isna(slope):
            return 0
        if slope > 1.5:
            return 1.0
        elif slope > 0.5:
            return 0.6
        elif slope > 0:
            return 0.3
        else:
            return -0.5  # 白线向下，趋势破坏

    def _compute_resonance_score(self, code, date_str):
        """概念共振: 0.5-1%密度最优"""
        if self.signal_cache is None or not self.stock_concepts:
            return 0
        concepts = self.stock_concepts.get(code, [])
        concepts = [c for c in concepts if c not in self.META_CONCEPTS]
        if not concepts:
            return 0

        densities = []
        for cn in concepts:
            if cn not in self.concept_size:
                continue
            nearby = set()
            for d in range(-2, 3):
                cd = pd.to_datetime(date_str) + pd.Timedelta(days=d)
                cs = cd.strftime('%Y-%m-%d')
                if self.signal_cache and cs in self.signal_cache:
                    for s in self.signal_cache[cs]:
                        if s['code'] in self.stock_concepts.get(s['code'], []):
                            if cn in self.stock_concepts.get(s['code'], []):
                                nearby.add(s['code'])
            nearby.discard(code)
            if len(nearby) > 0:
                densities.append(len(nearby) / self.concept_size[cn] * 100)

        if not densities:
            return 0
        max_den = max(densities)
        if 0.3 <= max_den < 1.5:
            return 1.0
        elif 1.5 <= max_den < 3:
            return 0.5
        elif max_den >= 5:
            return 0.0  # 太拥挤
        else:
            return 0.3

    # ------------------------------------------------------------------
    # 综合评分
    # ------------------------------------------------------------------
    def score(self, signal_dict, indicators_df):
        """
        计算AI综合评分 (0-100)
        signal_dict: 来自预计算缓存的信号
        indicators_df: _get_realtime_indicators返回的指标DataFrame（降序）
        """
        self._load()
        code = signal_dict.get('code', '')

        # 从指标DataFrame提取数据
        sr = indicators_df.iloc[0]
        pre5 = indicators_df.iloc[1:6] if len(indicators_df) > 5 else indicators_df.iloc[1:]

        J = sr.get('J', 0)
        dist_yellow = (sr['close'] - sr['yellow_line']) / sr['yellow_line'] * 100 if 'yellow_line' in indicators_df.columns else None
        dist_white = (sr['close'] - sr['white_line']) / sr['white_line'] * 100 if 'white_line' in indicators_df.columns else None
        vol_ratio = sr['volume'] / pre5['volume'].mean() if len(pre5) > 0 and pre5['volume'].mean() > 0 else 0

        open_p = sr.get('open', sr['close'])
        candle_body = (sr['close'] - open_p) / open_p * 100 if open_p and open_p != 0 else 0

        k_below_d = 0
        for i in range(1, min(20, len(indicators_df))):
            if indicators_df.iloc[i].get('K', 0) > indicators_df.iloc[i].get('D', 0):
                k_below_d = i; break
        if k_below_d == 0:
            k_below_d = 20

        ret_5d = (sr['close'] / pre5.iloc[-1]['close'] - 1) * 100 if len(pre5) >= 5 else 0
        DIF = sr.get('DIF', 0)
        white_slope = (pre5.iloc[0]['white_line'] / pre5.iloc[-1]['white_line'] - 1) * 100 if len(pre5) >= 5 and 'white_line' in indicators_df.columns else 0

        n_concepts = len([c for c in self.stock_concepts.get(code, []) if c not in self.META_CONCEPTS])

        # 计算各因子分
        scores = {
            'J': self._compute_J_score(J),
            'position': self._compute_position_score(dist_yellow, dist_white),
            'volume': self._compute_volume_score(vol_ratio),
            'candle': self._compute_candle_score(candle_body),
            'kdj_days': self._compute_kdj_days_score(k_below_d),
            'concept_cov': self._compute_concept_coverage_score(n_concepts),
            'ret5d': self._compute_ret5d_score(ret_5d),
            'DIF': self._compute_DIF_score(DIF),
            'white_slope': self._compute_white_slope_score(white_slope),
        }

        # 权重校准（基于师兄190笔训练数据）
        weights = {
            'J': 0.30,
            'position': 0.12,
            'volume': 0.08,
            'candle': 0.08,
            'kdj_days': 0.08,
            'concept_cov': 0.12,
            'ret5d': 0.08,
            'DIF': 0.08,
            'white_slope': 0.06,
        }

        total = sum(scores[k] * weights[k] for k in weights)
        score = max(0, min(100, total * 100))

        return {
            'ai_score': round(score, 1),
            'breakdown': {k: round(scores[k], 2) for k in scores},
        }
