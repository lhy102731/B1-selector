# -*- coding: utf-8 -*-
"""
市场择时模块 - 基于指南针活跃市值判断多头/空头区间

状态机逻辑:
  多头区间开始:
    - 连续两天涨幅总和 >= 4%（两天都为正）
    - 或单日涨幅 >= 4.1%
  空头区间开始:
    - 单日跌幅 > 2.3%

  初始状态: 空头区间（保守）
  状态切换: 当天信号优先，空头信号优先于多头信号检查
"""
import pandas as pd
from pathlib import Path


class MarketTiming:
    """活跃市值择时状态机"""

    def __init__(self, csv_path=None):
        self.df = None              # 活跃市值日线数据: date, active_cap, pct_chg
        self.states = {}            # date -> 'bullish' | 'bearish'
        self._initial_state = 'bearish'
        self._bullish_threshold = 4.0     # 连续两日涨幅总和的触发阈值
        self._bullish_single = 4.1        # 单日涨幅触发阈值
        self._bearish_threshold = -2.3    # 单日跌幅触发阈值（负值）

        if csv_path:
            self.load(csv_path)

    def load(self, csv_path):
        """加载活跃市值 CSV 并计算状态"""
        df = pd.read_csv(csv_path)
        if 'date' not in df.columns:
            raise ValueError("活跃市值 CSV 必须包含 'date' 列")

        # 自动检测数值列
        val_col = None
        for col in df.columns:
            if col.lower() in ('active_cap', 'active', 'cap', 'value', 'close', '活跃市值'):
                val_col = col
                break
        if val_col is None:
            # 取第一个非 date 的数值列
            for col in df.columns:
                if col != 'date' and df[col].dtype in ('float64', 'int64'):
                    val_col = col
                    break
        if val_col is None:
            raise ValueError("活跃市值 CSV 缺少数值列")

        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        df['pct_chg'] = df[val_col].pct_change() * 100

        self.df = df
        self._compute_states()
        return self

    def _compute_states(self):
        """遍历所有交易日计算每日的多头/空头状态"""
        df = self.df
        self.states = {}
        current_state = self._initial_state

        for i in range(len(df)):
            date = df.iloc[i]['date']
            pct = df.iloc[i]['pct_chg']

            if pd.isna(pct):
                self.states[date] = current_state
                continue

            # 空头信号优先：当日跌幅 > 2.3%
            if pct <= self._bearish_threshold:
                current_state = 'bearish'
                self.states[date] = current_state
                continue

            # 多头信号：单日 >= 4.1%
            if pct >= self._bullish_single:
                current_state = 'bullish'
                self.states[date] = current_state
                continue

            # 多头信号：连续两日涨幅总和 >= 4%
            if i >= 1:
                prev_pct = df.iloc[i - 1]['pct_chg']
                if not pd.isna(prev_pct) and pct > 0 and prev_pct > 0:
                    if pct + prev_pct >= self._bullish_threshold:
                        current_state = 'bullish'
                        self.states[date] = current_state
                        continue

            self.states[date] = current_state

    def is_bullish(self, date):
        """查询某日是否为多头区间（允许开仓）"""
        if isinstance(date, str):
            date = pd.to_datetime(date)
        if date not in self.states:
            # 向前找最近的已知状态
            known_dates = sorted(self.states.keys())
            for d in reversed(known_dates):
                if d <= date:
                    return self.states[d] == 'bullish'
            return False
        return self.states[date] == 'bullish'

    def can_open(self, date):
        """某日是否允许开新仓（= is_bullish）"""
        return self.is_bullish(date)

    def get_state_df(self):
        """返回含状态标签的完整 DataFrame"""
        if self.df is None:
            return pd.DataFrame()
        df = self.df.copy()
        df['state'] = df['date'].map(self.states)
        return df

    def summary(self):
        """打印择时统计摘要"""
        if self.df is None:
            print("无数据")
            return
        df = self.get_state_df()
        bullish_days = (df['state'] == 'bullish').sum()
        bearish_days = (df['state'] == 'bearish').sum()
        total = len(df)
        print(f"活跃市值择时统计:")
        print(f"  总交易日: {total}")
        print(f"  多头区间: {bullish_days} 天 ({bullish_days/total*100:.1f}%)")
        print(f"  空头区间: {bearish_days} 天 ({bearish_days/total*100:.1f}%)")

        # 统计每个区间的持续时间
        states = df['state'].values
        intervals = []
        current_len = 1
        for i in range(1, len(states)):
            if states[i] == states[i-1]:
                current_len += 1
            else:
                intervals.append((states[i-1], current_len))
                current_len = 1
        intervals.append((states[-1], current_len))

        bull_intervals = [l for s, l in intervals if s == 'bullish']
        bear_intervals = [l for s, l in intervals if s == 'bearish']
        if bull_intervals:
            print(f"  多头区间平均持续: {sum(bull_intervals)/len(bull_intervals):.1f} 天")
        if bear_intervals:
            print(f"  空头区间平均持续: {sum(bear_intervals)/len(bear_intervals):.1f} 天")


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        mt = MarketTiming(sys.argv[1])
        mt.summary()
    else:
        print("用法: python market_timing.py <active_cap.csv>")
