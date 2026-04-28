# utils/stock_scorer.py
"""
股票评分模块 - 仅B1相似度评分
"""
from typing import Dict, Optional
import pandas as pd


class StockScorer:
    def __init__(self, csv_manager, registry):
        self.csv_manager = csv_manager
        self.registry = registry
        self.b1_library = None
        self._init_library()

    def _init_library(self):
        try:
            from strategy.pattern_library import B1PatternLibrary
            self.b1_library = B1PatternLibrary(self.csv_manager)
        except Exception as e:
            print(f"B1库初始化失败: {e}")
            self.b1_library = None

    def score_stock(self, code: str, df_with_indicators: Optional[pd.DataFrame] = None,
                    lookback_days: int = 40) -> Dict:
        """
        返回B1相似度评分
        """
        result = {
            'b1_score': 0.0,
            'matched_case': None,
            'matched_date': None,
            'breakdown': None,
            'tags': [],
        }

        if self.b1_library and self.b1_library.cases and df_with_indicators is not None:
            try:
                match_result = self.b1_library.find_best_match(code, df_with_indicators, lookback_days=lookback_days)
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