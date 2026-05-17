# utils/stock_scorer.py
"""
股票评分模块 - 仅B1相似度评分
"""
from typing import Dict, Optional
import pandas as pd


class StockScorer:
    def __init__(self, csv_manager, registry, exclude_limit_state=False):
        self.csv_manager = csv_manager
        self.registry = registry
        self.exclude_limit_state = exclude_limit_state
        self.b1_library = None
        self._init_library()

    def _init_library(self):
        try:
            from strategy.pattern_library import B1PatternLibrary
            self.b1_library = B1PatternLibrary(self.csv_manager, exclude_limit_state=self.exclude_limit_state)
        except Exception as e:
            print(f"B1库初始化失败: {e}")
            self.b1_library = None

    def score_stock(self, code: str, df_with_indicators: Optional[pd.DataFrame] = None,
                    lookback_days: int = 40, start_date=None) -> Dict:
        """
        返回B1相似度评分
        :param start_date: 异动起点，评分只用到此日之后的数据
        """
        result = {
            'b1_score': 0.0,
            'matched_case': None,
            'matched_date': None,
            'breakdown': None,
            'tags': [],
        }

        if self.b1_library and self.b1_library.cases and df_with_indicators is not None:
            df_use = df_with_indicators
            if start_date is not None:
                sdt = pd.to_datetime(start_date)
                # 找到异动前的波段低点作为起点（低点→建仓→异动的完整波形）
                df_asc = df_with_indicators.sort_values('date').reset_index(drop=True)
                pre_mask = df_asc['date'] < sdt
                if pre_mask.sum() > 0:
                    pre_surge = df_asc[pre_mask].tail(120)  # 异动前最多120天
                    if len(pre_surge) > 0:
                        wave_low_dt = df_asc.loc[pre_surge['close'].idxmin(), 'date']
                        df_use = df_with_indicators[df_with_indicators['date'] >= wave_low_dt]
            if len(df_use) < 20:
                return result
            try:
                match_result = self.b1_library.find_best_match(code, df_use, lookback_days=lookback_days)
                if match_result.get('best_match'):
                    best = match_result['best_match']
                    result['b1_score'] = best['similarity_score']
                    result['matched_case'] = best.get('case_name')
                    result['matched_date'] = best.get('case_date')
                    result['breakdown'] = best.get('breakdown')
                    result['tags'] = best.get('tags', [])
            except Exception as e:
                print(f"B1匹配 {code} 失败: {e}")

        return result