"""
Layer 1: 概念主线识别
利用概念资金流数据识别市场主线板块，在选股时优先选择主线概念中的标的
"""
import json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


class ConceptAnalyzer:
    """概念主线分析器"""

    # 过于宽泛的"元概念"，不参与主线判断
    META_CONCEPTS = {
        '融资融券', '深股通', '沪股通', '证金持股', '同花顺漂亮50',
        '标普道琼斯A股', 'MSCI概念', '富时罗素概念', '富时罗素概念股',
    }

    def __init__(self, data_dir=None):
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data' / 'block'
        self.data_dir = Path(data_dir)
        self.concept_map = None       # concept_code -> name
        self.stock_to_concepts = None # stock_code -> [concept_names]
        self._load_definitions()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------
    def _load_definitions(self):
        """加载概念定义（名称映射 + 股票归属）"""
        concept_path = self.data_dir / 'concept.json'
        if not concept_path.exists():
            print(f'[WARN] concept.json not found at {concept_path}')
            return
        with open(concept_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self.concept_map = data.get('name_map', {})
        self.stock_to_concepts = data.get('stock_to_blocks', {})

    def load_concept_flow_history(self, days=5):
        """加载最近N天的概念资金流数据"""
        flow_dir = self.data_dir / 'concept_flow'
        if not flow_dir.exists():
            return pd.DataFrame()

        files = sorted(flow_dir.glob('*.csv'), reverse=True)[:days]
        if not files:
            return pd.DataFrame()

        dfs = []
        for fp in files:
            date_str = fp.stem
            try:
                df = pd.read_csv(fp, encoding='utf-8-sig')
                df['date'] = date_str
                dfs.append(df)
            except Exception as e:
                print(f'  [WARN] failed to read {fp}: {e}')

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    # ------------------------------------------------------------------
    # Concept strength scoring
    # ------------------------------------------------------------------
    def compute_concept_strength(self, days=5):
        """
        计算每个概念的强度评分
        综合: 累计净流入 + 日度胜率 + 近期趋势
        """
        df = self.load_concept_flow_history(days)
        if df.empty:
            return pd.DataFrame()

        df['net_flow'] = pd.to_numeric(df['net_flow'], errors='coerce')
        df['inflow'] = pd.to_numeric(df['inflow'], errors='coerce')
        df['change_pct'] = df['change_pct'].apply(
            lambda x: float(str(x).replace('%', '')) if pd.notna(x) else 0)

        results = []
        for concept, grp in df.groupby('concept'):
            if concept in self.META_CONCEPTS:
                continue
            grp = grp.sort_values('date')
            n = len(grp)
            if n < 1:
                continue

            # 1. Total net inflow (cumulative)
            total_net = grp['net_flow'].sum()

            # 2. Win rate: days with positive net_flow
            win_rate = (grp['net_flow'] > 0).sum() / n

            # 3. Recent trend: slope of net_flow over time (linear regression)
            x = np.arange(n)
            y = grp['net_flow'].values
            if n >= 3 and np.isfinite(y).all():
                slope = np.polyfit(x, y, 1)[0]
            else:
                slope = 0

            # 4. Average daily inflow magnitude
            avg_inflow = grp['inflow'].mean()

            # 5. Momentum: average change_pct
            avg_change = grp['change_pct'].mean()

            # Net flow efficiency: net / (inflow + outflow) as percentage
            total_flow = grp['inflow'].sum() + grp['outflow'].sum()
            net_efficiency = (total_net / total_flow * 100) if total_flow > 0 else 0

            results.append({
                'concept': concept,
                'days': n,
                'total_net': round(total_net, 2),
                'win_rate': round(win_rate, 3),
                'trend_slope': round(slope, 3),
                'avg_change': round(avg_change, 2),
                'net_efficiency': round(net_efficiency, 2),
            })

        result = pd.DataFrame(results)
        if result.empty:
            return result

        # Use percentile ranking for robustness against outliers
        result['net_rank'] = result['total_net'].rank(pct=True)
        result['eff_rank'] = result['net_efficiency'].rank(pct=True)
        result['change_rank'] = result['avg_change'].rank(pct=True)
        result['win_rank'] = result['win_rate'].rank(pct=True)

        result['strength_score'] = round(
            result['net_rank'] * 35 +
            result['eff_rank'] * 25 +
            result['change_rank'] * 20 +
            result['win_rank'] * 20, 2
        )
        result = result.sort_values('strength_score', ascending=False).reset_index(drop=True)
        return result

    def get_mainline_concepts(self, top_n=20, days=5):
        """获取主线概念列表"""
        strength = self.compute_concept_strength(days)
        if strength.empty:
            return []
        return strength.head(top_n)['concept'].tolist()

    # ------------------------------------------------------------------
    # Stock-level scoring
    # ------------------------------------------------------------------
    def score_stock_concept_alignment(self, stock_code, mainline_concepts=None, top_n=20, days=5):
        """
        评估股票是否在主线概念中
        返回: {'score': 0-100, 'matched_concepts': [...], 'in_mainline': bool}
        """
        if mainline_concepts is None:
            mainline_concepts = self.get_mainline_concepts(top_n, days)

        if not self.stock_to_concepts:
            return {'score': 0, 'matched_concepts': [], 'in_mainline': False}

        stock_concepts = self.stock_to_concepts.get(stock_code, [])
        if not stock_concepts:
            return {'score': 0, 'matched_concepts': [], 'in_mainline': False}

        matched = [c for c in stock_concepts if c in mainline_concepts]
        if not matched:
            return {'score': 0, 'matched_concepts': [], 'in_mainline': False}

        # Score: ratio of matched concepts * 100
        score = min(100, round(len(matched) / max(1, len(stock_concepts)) * 100, 1))
        return {
            'score': score,
            'matched_concepts': matched,
            'in_mainline': len(matched) > 0,
            'total_concepts': len(stock_concepts),
        }

    # ------------------------------------------------------------------
    # Summary report
    # ------------------------------------------------------------------
    def report(self, top_n=20, days=5):
        """打印主线概念报告"""
        strength = self.compute_concept_strength(days)
        if strength.empty:
            print('[ConceptAnalyzer] No concept flow data available')
            return

        print(f'=== 概念主线分析 (近{days}日) ===')
        print(f'数据覆盖: {len(strength)} 个概念')

        mainline = strength.head(top_n)
        print(f'\n主线概念 TOP{top_n}:')
        for i, row in mainline.iterrows():
            direction = '+' if row['total_net'] > 0 else ''
            print(f"  {i+1:2d}. {row['concept']:12s}  "
                  f"score={row['strength_score']:6.1f}  "
                  f"net={direction}{row['total_net']:.1f}亿  "
                  f"win={row['win_rate']:.0%}  "
                  f"slope={row['trend_slope']:.2f}")

        # Bottom concepts (outflow)
        bottom = strength.tail(10).iloc[::-1]
        print(f'\n资金流出最多的概念:')
        for _, row in bottom.head(5).iterrows():
            print(f"  {row['concept']:12s}  net={row['total_net']:.1f}亿")


# ----------------------------------------------------------------------
if __name__ == '__main__':
    analyzer = ConceptAnalyzer()
    analyzer.report(days=1)

    # Test stock scoring
    if analyzer.stock_to_concepts:
        mainline = analyzer.get_mainline_concepts(top_n=20, days=1)
        print(f'\n=== 主板B1候选股的主线检测 ===')
        test_stocks = ['600366', '000977', '688256', '002580', '600601']
        for code in test_stocks:
            result = analyzer.score_stock_concept_alignment(
                code, mainline_concepts=mainline)
            print(f"  {code}: score={result['score']:.0f}  "
                  f"matched={result['matched_concepts'][:3]}  "
                  f"in_mainline={result['in_mainline']}")
