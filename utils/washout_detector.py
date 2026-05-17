"""
击穿对手盘识别 - 师傅直播核心洗盘形态
"""
import pandas as pd
import numpy as np


def detect_washout(df: pd.DataFrame, lookback_days: int = 20):
    """
    检测是否存在击穿对手盘信号（缩量破黄线后快速收回）
    必须满足：前一日收盘价 > 黄线，当日缩量跌破黄线，且3日内收回
    返回: (is_washout, break_date, recover_date, washout_low)
    washout_low: 洗盘期间（破位日到收回日）的最低价，用于止损参考
    """
    if len(df) < lookback_days:
        return False, None, None, None

    # 取最近 lookback_days 条，转为正序（从旧到新）
    recent = df.head(lookback_days).copy().sort_values('date').reset_index(drop=True)
    # 兼容两种列名：bull_bear_line(旧) 和 yellow_line(新)
    if 'bull_bear_line' in recent.columns:
        hl_col = 'bull_bear_line'
    elif 'yellow_line' in recent.columns:
        hl_col = 'yellow_line'
    else:
        return False, None, None, None

    # 寻找破黄线的日子（收盘价 < 黄线）
    below_mask = recent['close'] < recent[hl_col]
    break_indices = []
    for i in range(1, len(recent)):
        # 当日破黄线，且前一日收盘价在黄线之上
        if below_mask.iloc[i] and not below_mask.iloc[i - 1]:
            if recent.iloc[i - 1]['close'] > recent.iloc[i - 1][hl_col]:
                break_indices.append(i)

    if not break_indices:
        return False, None, None, None

    # 检查每个破位点
    for idx in break_indices:
        # 破位当天是否缩量（成交量 < 前5日均量 * 0.7）
        break_vol = recent.iloc[idx]['volume']
        avg_vol = recent.iloc[max(0, idx - 5):idx]['volume'].mean()
        if break_vol > avg_vol * 0.7:
            continue

        # 检查后续3天内是否快速收回（收盘价 > 黄线）
        for j in range(idx + 1, min(idx + 4, len(recent))):
            if recent.iloc[j]['close'] > recent.iloc[j][hl_col]:
                washout_low = recent.iloc[idx:j+1]['low'].min()
                return True, recent.iloc[idx]['date'], recent.iloc[j]['date'], washout_low

    return False, None, None, None