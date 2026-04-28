# -*- coding: utf-8 -*-
"""
获取全市场活跃度数据，作为指南针活跃市值的代理指标

数据源: akshare 上证指数日线 (含成交额)
活跃市值本质 = 全市场活跃交易资金规模
上证指数成交额与之高度相关，可作代理

用法: python fetch_market_data.py
输出: data/market/active_cap.csv
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import akshare as ak


def fetch_sh_index():
    """获取上证指数历史日线（含成交额）"""
    print("获取上证指数日线数据...")
    df = ak.stock_zh_index_daily_em(symbol="sh000001")
    df = df.rename(columns={
        'date': 'date',
        'amount': 'amount',       # 成交额（元）
        'close': 'close',         # 收盘点位
        'volume': 'volume',       # 成交量（手）
    })
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    print(f"  获取到 {len(df)} 个交易日, {df['date'].min().strftime('%Y-%m-%d')} ~ {df['date'].max().strftime('%Y-%m-%d')}")
    return df


def main():
    out_dir = Path('data/market')
    out_dir.mkdir(parents=True, exist_ok=True)

    df = fetch_sh_index()

    # 输出为活跃市值代理 CSV（列名兼容 market_timing.py）
    out = df[['date', 'amount']].copy()
    out.columns = ['date', 'active_cap']
    out_path = out_dir / 'active_cap.csv'
    out.to_csv(out_path, index=False)
    print(f"已保存: {out_path}")
    print(f"列: date, active_cap (上证成交额, 单位: 元)")


if __name__ == '__main__':
    main()
