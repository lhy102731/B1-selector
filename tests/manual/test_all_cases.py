#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测系统 - 测试典型案例能否被B1策略选出且相似度≥75%
输出详细原因、匹配案例、相似度及异动启动点日期
"""
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple

project_root = Path(__file__).resolve()
while not (project_root / 'AGENTS.md').exists() and project_root != project_root.parent:
    project_root = project_root.parent
sys.path.insert(0, str(project_root))

from utils.csv_manager import CSVManager
from strategy.unified_b1_strategy import UnifiedB1Strategy
from strategy.pattern_config import B1_PERFECT_CASES, SIMILARITY_WEIGHTS, MATCH_TOLERANCES
from strategy.pattern_feature_extractor import PatternFeatureExtractor
from strategy.pattern_matcher import PatternMatcher
from utils.s1_filter import detect_s1_signal
from utils.washout_detector import detect_washout


class DetailedBacktest:
    def __init__(self, case: dict, csv_manager: CSVManager):
        self.case = case
        self.code = case["code"]
        self.name = case["name"]
        self.breakout_date = case["breakout_date"]
        self.lookback_days = case.get("lookback_days", 25)
        self.csv_manager = csv_manager
        self.strategy = UnifiedB1Strategy()
        self.extractor = PatternFeatureExtractor()
        self.matcher = PatternMatcher(SIMILARITY_WEIGHTS, MATCH_TOLERANCES)

    def get_data_up_to_date(self, end_date: str) -> pd.DataFrame:
        df = self.csv_manager.read_stock(self.code)
        if df.empty:
            return pd.DataFrame()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        end_dt = pd.to_datetime(end_date)
        df = df[df['date'] <= end_dt].copy()
        return df

    def diagnose_b1_failure(self, df_indicators: pd.DataFrame) -> str:
        if df_indicators.empty or len(df_indicators) < 60:
            return "数据不足60天"
        latest = df_indicators.iloc[0]

        if not latest.get('white_gt_yellow', False):
            return "白线 ≤ 黄线"
        j_th = self.strategy.params.get('j_threshold', 30)
        if latest.get('J', 100) >= j_th:
            return f"J值 {latest['J']:.1f} ≥ {j_th}"
        if not latest.get('volume_shrink', False):
            return "未缩量（成交量 ≥ 20日最高量*0.55）"
        if latest.get('doubled', False):
            return "60日内已翻倍"
        build_quality = self.strategy._calc_build_position_quality(df_indicators)
        if not build_quality.get('is_qualified', False):
            gain = build_quality.get('total_gain', 0)
            turnover = build_quality.get('surge_turnover_sum', 0)
            if gain > self.strategy.params.get('max_gain_pct', 60):
                return f"建仓涨幅 {gain:.1f}% > {self.strategy.params['max_gain_pct']}%"
            if turnover > self.strategy.params.get('max_surge_turnover', 80):
                return f"换手累加 {turnover:.1f}% > {self.strategy.params['max_surge_turnover']}%"
            if build_quality.get('has_shrink_limit_up'):
                return "建仓波内出现缩量涨停/一字板"
            return "建仓波质量不合格（红肥绿瘦不足或跳空等）"
        surge_start_date = None
        if build_quality.get('surge_start_idx') is not None:
            df_asc = df_indicators.sort_values('date').reset_index(drop=True)
            idx = build_quality['surge_start_idx']
            if idx < len(df_asc):
                surge_start_date = df_asc.iloc[idx]['date']
        has_s1, _, s1_type = detect_s1_signal(df_indicators, lookback_days=35, surge_start_date=surge_start_date)
        if has_s1:
            return f"S1出货信号: {s1_type}"
        position_ok = (latest.get('fall_in_bowl', False) or
                       latest.get('near_yellow', False) or
                       latest.get('near_white', False))
        is_washout = detect_washout(df_indicators)[0]
        if not position_ok and not is_washout:
            return "位置条件不满足（不在碗内/靠近黄线/靠近白线）且非击穿对手盘"
        return "未知原因（请检查策略参数）"

    def is_selected_with_reason(self, df: pd.DataFrame) -> Tuple[bool, Optional[dict], str, Optional[str]]:
        if df.empty or len(df) < 60:
            return False, None, "数据不足60天", None
        try:
            df_indicators = self.strategy.calculate_indicators(df)
            signals = self.strategy.select_stocks(df_indicators, self.name)
            if signals:
                surge_start_date = signals[0].get('surge_start_date')
                return True, signals[0], "", surge_start_date
            else:
                # 未选出时尝试获取异动起点（用于诊断）
                build_quality = self.strategy._calc_build_position_quality(df_indicators)
                surge_date = None
                if build_quality.get('surge_start_idx') is not None:
                    df_asc = df_indicators.sort_values('date').reset_index(drop=True)
                    idx = build_quality['surge_start_idx']
                    if idx < len(df_asc):
                        surge_date = df_asc.iloc[idx]['date'].strftime('%Y-%m-%d')
                reason = self.diagnose_b1_failure(df_indicators)
                return False, None, reason, surge_date
        except Exception as e:
            return False, None, f"策略执行异常: {str(e)[:80]}", None

    def extract_features_safe(self, df: pd.DataFrame, lookback_days: int) -> dict:
        if df.empty or len(df) < lookback_days:
            return {}
        window_df = df.tail(lookback_days).copy()
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if window_df[col].isna().sum() > len(window_df) * 0.3:
                return {}
        window_desc = window_df.sort_values('date', ascending=False).reset_index(drop=True)
        try:
            features = self.extractor.extract(window_desc, lookback_days=lookback_days)
            if not features.get('kdj_state') or features['kdj_state'].get('j_value') is None:
                return {}
            return features
        except Exception:
            return {}

    def run(self, all_case_features: Dict[str, dict]) -> dict:
        result = {
            'case_id': self.case['id'],
            'name': self.name,
            'code': self.code,
            'breakout_date': self.breakout_date,
            'selected_by_b1': False,
            'fail_reason': '',
            'surge_start_date': '',
            'best_similarity': 0.0,
            'best_match_case': '',
            'similarity_ge_75': False,
        }

        df = self.get_data_up_to_date(self.breakout_date)
        if df.empty:
            result['fail_reason'] = f"无数据 (截止{self.breakout_date})"
            return result

        selected, _, fail_reason, surge_start = self.is_selected_with_reason(df)
        result['selected_by_b1'] = selected
        result['fail_reason'] = fail_reason
        result['surge_start_date'] = surge_start if surge_start else ''
        if not selected:
            return result

        # 已选出，进行相似度匹配
        candidate_features = self.extract_features_safe(df, self.lookback_days)
        if not candidate_features:
            result['fail_reason'] = "特征提取失败（无法匹配）"
            result['selected_by_b1'] = False
            return result

        best_score = 0.0
        best_id = None
        for other_id, other_data in all_case_features.items():
            if other_id == self.case['id']:
                continue
            other_feat = other_data.get('features')
            if not other_feat:
                continue
            try:
                match = self.matcher.match(candidate_features, other_feat)
                score = match['total_score']
                if score > best_score:
                    best_score = score
                    best_id = other_id
            except Exception:
                continue

        result['best_similarity'] = round(best_score, 2)
        if best_id:
            result['best_match_case'] = all_case_features[best_id]['name']
        result['similarity_ge_75'] = best_score >= 75.0
        return result


class DetailedBacktestEngine:
    def __init__(self, data_dir=None):
        data_path = Path(data_dir) if data_dir is not None else project_root / "data"
        if not data_path.is_absolute():
            data_path = project_root / data_path
        self.csv_manager = CSVManager(str(data_path))
        self.cases = B1_PERFECT_CASES
        self.case_features = {}

    def precompute_case_features(self):
        print("=" * 60)
        print("预计算所有案例的特征（用于相似度匹配）...")
        for case in self.cases:
            case_id = case["id"]
            code = case["code"]
            name = case["name"]
            breakout_date = case["breakout_date"]
            lookback = case.get("lookback_days", 25)

            df = self.csv_manager.read_stock(code)
            if df.empty:
                print(f"  ⚠️ {name} 无数据，跳过")
                self.case_features[case_id] = {'features': {}, 'name': name}
                continue

            df = df.sort_values('date').reset_index(drop=True)
            df['date'] = pd.to_datetime(df['date'])
            end_dt = pd.to_datetime(breakout_date)
            df = df[df['date'] <= end_dt].copy()
            if len(df) < lookback:
                print(f"  ⚠️ {name} 数据不足{lookback}天，跳过")
                self.case_features[case_id] = {'features': {}, 'name': name}
                continue

            window_df = df.tail(lookback).copy()
            window_desc = window_df.sort_values('date', ascending=False).reset_index(drop=True)
            try:
                extractor = PatternFeatureExtractor()
                features = extractor.extract(window_desc, lookback_days=lookback)
                if features.get('kdj_state') and features['kdj_state'].get('j_value') is not None:
                    self.case_features[case_id] = {'features': features, 'name': name}
                    print(f"  ✓ {name} 特征计算完成")
                else:
                    raise ValueError("特征无效")
            except Exception as e:
                print(f"  ✗ {name} 特征提取失败: {str(e)[:60]}")
                self.case_features[case_id] = {'features': {}, 'name': name}
        print("=" * 60)

    def run_all(self):
        print("\n开始回测...")
        results = []
        for case in self.cases:
            print(f"\n测试案例: {case['name']} ({case['code']}) 突破日 {case['breakout_date']}")
            bt = DetailedBacktest(case, self.csv_manager)
            res = bt.run(self.case_features)
            results.append(res)

            if res['selected_by_b1']:
                status = "✓ 选出"
                print(f"  {status} | 异动起点: {res['surge_start_date']} | 相似度: {res['best_similarity']}% | 匹配案例: {res['best_match_case']} | 达标75%: {'是' if res['similarity_ge_75'] else '否'}")
            else:
                print(f"  ✗ 未选出 | 异动起点: {res['surge_start_date']} | 原因: {res['fail_reason']}")
        self.print_summary(results)
        return results

    def print_summary(self, results):
        print("\n" + "=" * 60)
        print("回测汇总")
        total = len(results)
        selected = sum(1 for r in results if r['selected_by_b1'])
        sim_ge75 = sum(1 for r in results if r['similarity_ge_75'])
        both = sum(1 for r in results if r['selected_by_b1'] and r['similarity_ge_75'])
        print(f"总案例数: {total}")
        print(f"B1策略选出: {selected} ({selected/total*100:.1f}%)")
        print(f"相似度≥75%: {sim_ge75} ({sim_ge75/total*100:.1f}%)")
        print(f"同时满足(选出且相似度≥75%): {both} ({both/total*100:.1f}%)")

        # 详细表格保存
        df_out = pd.DataFrame(results)
        df_out = df_out[['name', 'code', 'breakout_date', 'selected_by_b1', 'fail_reason',
                         'surge_start_date', 'best_similarity', 'best_match_case', 'similarity_ge_75']]
        output_path = project_root / 'artifacts' / 'research' / 'b1' / 'manual' / 'backtest_detailed_results.csv'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n详细结果已保存至 {output_path}")

        # 控制台打印未选出原因列表
        print("\n未选出案例及原因:")
        for r in results:
            if not r['selected_by_b1']:
                print(f"  {r['name']} (异动起点 {r['surge_start_date']}): {r['fail_reason']}")


def main():
    print("A股量化选股系统 - 案例回测（含异动起点日期）")
    print("输出未选出的具体原因、异动起点、匹配案例及相似度\n")
    engine = DetailedBacktestEngine()
    engine.precompute_case_features()
    engine.run_all()


if __name__ == "__main__":
    main()
