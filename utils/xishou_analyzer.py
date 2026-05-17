"""
Layer 2: 个股惜售分析
检测洗盘期间主力资金流出是否逐日减少（惜售倾向）
"""
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


class XishouAnalyzer:
    """惜售分析器"""

    def __init__(self, data_dir=None):
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data' / 'block'
        self.data_dir = Path(data_dir)

    def load_stock_flow_history(self, days=10):
        """加载最近N天的个股资金流数据"""
        flow_dir = self.data_dir / 'stock_flow'
        if not flow_dir.exists():
            return pd.DataFrame()

        files = sorted(flow_dir.glob('*.csv'), reverse=True)[:days]
        if not files:
            return pd.DataFrame()

        dfs = []
        for fp in files:
            date_str = fp.stem
            try:
                df = pd.read_csv(fp, encoding='utf-8-sig', dtype={'code': str})
                df['date'] = date_str
                dfs.append(df)
            except Exception as e:
                print(f'  [WARN] failed to read {fp}: {e}')

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def analyze_stock(self, stock_code, days=5):
        """
        分析单只股票的惜售倾向
        返回: {'xishou_score': 0-100, 'trend': 'shrinking'|'flat'|'expanding',
               'outflow_slope': float, 'turnover_trend': float, 'details': [...]}
        """
        df = self.load_stock_flow_history(days)
        if df.empty:
            return self._empty_result()

        stock_df = df[df['code'] == stock_code].sort_values('date')
        if len(stock_df) < 3:
            return self._empty_result()

        # Ensure numeric
        stock_df['net_flow'] = pd.to_numeric(stock_df['net_flow'], errors='coerce')
        stock_df['turnover'] = pd.to_numeric(stock_df['turnover'], errors='coerce')
        stock_df['inflow'] = pd.to_numeric(stock_df['inflow'], errors='coerce')
        stock_df['outflow'] = pd.to_numeric(stock_df['outflow'], errors='coerce')

        n = len(stock_df)
        x = np.arange(n)

        # 1. Net flow trend: positive slope = outflow shrinking (good)
        y_net = stock_df['net_flow'].values
        net_slope = np.polyfit(x, y_net, 1)[0] if n >= 3 and np.isfinite(y_net).all() else 0

        # 2. Outflow trend: negative slope = selling shrinking (good)
        y_out = stock_df['outflow'].values
        out_slope = np.polyfit(x, y_out, 1)[0] if n >= 3 and np.isfinite(y_out).all() else 0

        # 3. Turnover trend: negative slope = activity declining (good for washout)
        y_turn = stock_df['turnover'].values
        if np.isfinite(y_turn).all():
            turn_slope = np.polyfit(x, y_turn, 1)[0] if n >= 3 else 0
        else:
            turn_slope = 0

        # 4. Net flow consistency: consecutive improvement
        consistency = 0
        if n >= 3:
            improvements = 0
            for i in range(1, n):
                if y_net[i] > y_net[i-1]:  # net flow improved (less negative or more positive)
                    improvements += 1
            consistency = improvements / (n - 1)

        # 5. Net/Total ratio trend: is net becoming less negative relative to total?
        total_flow = stock_df['inflow'] + stock_df['outflow']
        net_ratio = (stock_df['net_flow'] / total_flow.replace(0, np.nan)).fillna(0)
        ratio_slope = np.polyfit(x, net_ratio.values, 1)[0] if n >= 3 else 0

        # Composite score
        # Normalize slopes relative to typical values
        score = 50.0  # baseline

        # Outflow shrinking (decreasing outflow = positive)
        avg_outflow = stock_df['outflow'].abs().mean()
        if avg_outflow > 0:
            score += min(25, max(-25, -out_slope / (avg_outflow / n) * 25))

        # Net flow improving
        avg_net = stock_df['net_flow'].abs().mean()
        if avg_net > 0:
            score += min(15, max(-15, net_slope / (avg_net / n) * 15))

        # Consistency bonus
        score += consistency * 10

        # Turnover declining (during pullback = good)
        avg_turn = stock_df['turnover'].mean()
        if avg_turn > 0:
            score += min(10, max(-10, -turn_slope / (avg_turn / n) * 10))

        score = max(0, min(100, round(score, 1)))

        # Trend classification
        if net_slope > 0.05:
            trend = 'shrinking'
        elif net_slope < -0.05:
            trend = 'expanding'
        else:
            trend = 'flat'

        return {
            'xishou_score': score,
            'trend': trend,
            'net_slope': round(net_slope, 3),
            'out_slope': round(out_slope, 3),
            'turn_slope': round(turn_slope, 3),
            'consistency': round(consistency, 2),
            'daily_net': y_net.tolist(),
            'daily_outflow': y_out.tolist(),
            'dates': stock_df['date'].tolist(),
        }

    def _empty_result(self):
        return {
            'xishou_score': 0,
            'trend': 'insufficient_data',
            'net_slope': 0, 'out_slope': 0, 'turn_slope': 0,
            'consistency': 0, 'daily_net': [], 'daily_outflow': [], 'dates': [],
        }

    def scan_top_xishou(self, top_n=20, days=5):
        """扫描全市场惜售得分最高的股票"""
        df = self.load_stock_flow_history(days)
        if df.empty:
            return pd.DataFrame()

        results = []
        for code in df['code'].unique()[:500]:  # limit for speed
            r = self.analyze_stock(code, days)
            if r['xishou_score'] > 0:
                results.append({
                    'code': code,
                    'xishou_score': r['xishou_score'],
                    'trend': r['trend'],
                    'consistency': r['consistency'],
                })

        result = pd.DataFrame(results)
        if result.empty:
            return result
        return result.sort_values('xishou_score', ascending=False).head(top_n)


if __name__ == '__main__':
    analyzer = XishouAnalyzer()
    print('=== 惜售分析测试 (需要多日数据) ===')

    # Test with known stocks
    test_stocks = ['600366', '000977', '688256', '002580']
    for code in test_stocks:
        r = analyzer.analyze_stock(code, days=1)
        print(f"  {code}: score={r['xishou_score']:.0f} trend={r['trend']} "
              f"net={r['daily_net']} out={r['daily_outflow']}")

    if len(analyzer.load_stock_flow_history(5)) > 0:
        top = analyzer.scan_top_xishou(top_n=10, days=1)
        if not top.empty:
            print(f'\n=== 全市场惜售 TOP10 ===')
            for _, row in top.iterrows():
                print(f"  {row['code']}: score={row['xishou_score']:.0f} trend={row['trend']}")
