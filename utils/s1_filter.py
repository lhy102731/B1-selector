"""
S1出货信号过滤器 - 师傅直播核心卖点识别
假阴真阳统一视为阴线处理
"""
import pandas as pd
import numpy as np


def detect_s1_signal(df: pd.DataFrame, lookback_days: int = 15, surge_start_date=None):
    """
    检测是否存在S1出货信号
    :param df: 倒序数据（最新在前）
    :param lookback_days: 回看天数
    :param surge_start_date: 异动启动日（字符串或 datetime），用于次高点放量的区间最大量验证
    :return: (has_s1, s1_date, s1_type)
    """
    if len(df) < lookback_days:
        return False, None, None

    # 取最近 lookback_days 条，转为正序
    recent = df.head(lookback_days).copy().sort_values('date').reset_index(drop=True)
    if len(recent) < 5:
        return False, None, None

    # 计算20日均量
    vol_ma20 = recent['volume'].rolling(20, min_periods=1).mean()
    recent['vol_ratio_ma20'] = recent['volume'] / vol_ma20.shift(1)
    recent['vol_ratio_prev'] = recent['volume'] / recent['volume'].shift(1)
    recent_max_vol = recent['volume'].max()
    recent['is_max_vol'] = recent['volume'] >= recent_max_vol * 0.99

    # 定义"真实阴线"
    recent['is_real_bearish'] = (recent['close'] <= recent['open']) | (
        (recent['close'] < recent['close'].shift(1)) & (recent['close'] > recent['open'])
    )

    # 定位异动起点在 recent 中的索引
    surge_start_idx = None
    if surge_start_date is not None:
        surge_start_dt = pd.to_datetime(surge_start_date)
        mask = recent['date'] >= surge_start_dt
        if mask.any():
            surge_start_idx = mask.idxmax()  # 注意：idxmax返回的是标签（可能是行号），但后续loc可以用

    # 类型1：放量巨阴
    for i in range(len(recent) - 1, max(len(recent) - 10, -1), -1):
        row = recent.iloc[i]
        prev_close = recent.iloc[i - 1]['close'] if i > 0 else row['close']
        pct_chg = (row['close'] - prev_close) / prev_close * 100
        if (pct_chg <= -3 and
            row['is_real_bearish'] and
            (row['vol_ratio_prev'] >= 2.0 or row['vol_ratio_ma20'] >= 2.0) and
            row['is_max_vol']):
            return True, row['date'], '放量巨阴'

    # 类型2：顶部大风车
    for i in range(len(recent) - 1, max(len(recent) - 10, -1), -1):
        row = recent.iloc[i]
        total_range = row['high'] - row['low']
        if total_range == 0:
            continue
        upper_shadow = row['high'] - max(row['close'], row['open'])
        # 涨幅 < 2%（防止把大阳线误判为风车）
        prev_close = recent.iloc[i - 1]['close'] if i > 0 else row['close']
        day_gain = (row['close'] - prev_close) / prev_close * 100
        if day_gain >= 2:
            continue
        if (upper_shadow / total_range > 0.618 and
                row['volume'] > recent['volume'].iloc[:i].mean() * 1.5 and
                row['is_max_vol']):
            return True, row['date'], '顶部大风车'

    # 类型3：次高点放量（用户定制版）
    high_20 = recent['high'].rolling(20, min_periods=1).max()
    recent['is_high'] = recent['high'] >= high_20 * 0.97

    # 检查范围：从异动起点开始，若无则最近15天
    if surge_start_idx is not None:
        check_start = surge_start_idx
    else:
        check_start = max(0, len(recent) - 15)

    for i in range(check_start, len(recent)):
        row = recent.iloc[i]
        if not (row['is_high'] and row['is_real_bearish']):
            continue

        # 条件1：距离异动起点至少3天（异动起点本身不算，第1、2天跳过）
        if surge_start_idx is None or i - surge_start_idx < 3:
            continue

        # 条件2：成交量接近区间最大量（区间从异动起点到当前日期的前一天）
        interval_vol = recent.loc[surge_start_idx:i-1, 'volume']
        if interval_vol.empty:
            continue
        interval_max = interval_vol.max()
        if row['volume'] < interval_max * 0.99:
            continue

        # 条件3：开盘价相对于区间内最高价收盘价的跌幅在 (0, 3.82%) 之间
        interval_df = recent.loc[surge_start_idx:i]
        if interval_df.empty:
            continue
        max_high_idx = interval_df['high'].idxmax()
        max_high_close = interval_df.loc[max_high_idx, 'close']
        open_price = row['open']
        if max_high_close == 0:
            continue
        drop_pct = (max_high_close - open_price) / max_high_close * 100
        if not (0 < drop_pct < 3.82):
            continue

        # 三个条件全部满足，判定为次高点放量
        return True, row['date'], '次高点放量'

    return False, None, None