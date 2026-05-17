"""
S1出货信号过滤器 - 师傅直播核心卖点识别
假阴真阳统一视为阴线处理
"""
import pandas as pd
import numpy as np


# 全局开关：跳过指定 S1 类型（用于回测分析）
# 类型: "放量巨阴", "顶部大风车", "次高点放量"
SKIP_S1_TYPES = set()


def detect_s1_signal(df: pd.DataFrame, surge_start_date=None, skip_types=None, s1_config=None):
    """
    检测是否存在S1出货信号（以异动起点为扫描起点，整波回看）
    :param df: 倒序数据（最新在前）
    :param surge_start_date: 异动启动日（字符串或 datetime）
    :param skip_types: 要跳过的S1类型集合，如 {'放量巨阴'}
    :param s1_config: 放量巨阴细粒度参数 {'vol_ratio_prev': 1.5, 'vol_ratio_ma20': 2.0, 'max_bull_vol_ratio': 0.9, 'price_pos': 0.70}
                       设为None则禁用该条件
    :return: (has_s1, s1_date, s1_type)
    """
    _skip = skip_types or SKIP_S1_TYPES  # 优先用传参，fallback到全局开关
    if s1_config is None:
        s1_config = {}
    if len(df) < 5:
        return False, None, None

    # ★ 以异动起点为界取数据，太近则回退到波段低点
    surge_start_dt = pd.to_datetime(surge_start_date) if surge_start_date is not None else None
    if surge_start_dt is not None:
        recent = df[df['date'] >= surge_start_dt].copy()
        if len(recent) < 10:
            # 异动太近，回退到波段低点：从surge往前120天找最低收盘价作为扫描起点
            df_asc = df.sort_values('date').reset_index(drop=True)
            pre_mask = df_asc['date'] < surge_start_dt
            if pre_mask.sum() > 0:
                pre_surge = df_asc[pre_mask].tail(120)
                if len(pre_surge) > 0:
                    wave_low_dt = df_asc.loc[pre_surge['close'].idxmin(), 'date']
                    recent = df[df['date'] >= wave_low_dt].copy()
        if len(recent) < 5:
            return False, None, None
        recent = recent.sort_values('date').reset_index(drop=True)
    else:
        return False, None, None

    # 异动起点在 recent 中的索引（波段低点回退后可能 >0）
    surge_start_idx = 0
    if surge_start_dt is not None:
        mask = recent['date'] >= surge_start_dt
        if mask.any():
            surge_start_idx = mask.idxmax()

    # vr_ma20：20日均量只从异动起点算，避免pre-surge低迷量拉偏
    vol_ma20 = recent['volume'].copy()
    vol_ma20.iloc[:surge_start_idx] = np.nan
    vol_ma20 = vol_ma20.rolling(20, min_periods=1).mean()
    recent['vol_ratio_ma20'] = recent['volume'] / vol_ma20.shift(1)
    recent['vol_ratio_prev'] = recent['volume'] / recent['volume'].shift(1)
    recent_max_vol = recent['volume'].max()
    recent['is_max_vol'] = recent['volume'] >= recent_max_vol * 0.99
    recent['is_real_bearish'] = (recent['close'] <= recent['open']) | (
        (recent['close'] < recent['close'].shift(1)) & (recent['close'] > recent['open'])
    )
    has_ma = 'MA5' in recent.columns and 'MA10' in recent.columns and 'MA20' in recent.columns

    # 类型1：放量巨阴（整波扫描）
    # 顶部3天反转豁免：波内前3高价日后≤3天的放量阴线，无视price_pos
    wave_all = recent.iloc[surge_start_idx:]
    bullish_mask = wave_all['close'] > wave_all['open']
    max_bull_vol = wave_all.loc[bullish_mask, 'volume'].max() if bullish_mask.any() else 0
    top3_high_indices = set(wave_all['high'].nlargest(3).index)
    peak_neighbors = set()
    for pi in top3_high_indices:
        for offset in [1, 2, 3]:
            nxt = pi + offset
            if nxt < len(recent):
                peak_neighbors.add(nxt)

    wave_20 = recent.iloc[max(surge_start_idx, len(recent) - 20):]
    range_high = wave_20['high'].max()
    range_low = wave_20['low'].min()
    for i in range(len(recent) - 1, surge_start_idx - 1, -1):
        row = recent.iloc[i]
        bullish = (row['MA5'] > row['MA10'] > row['MA20']) if has_ma else True
        if not bullish: continue
        is_bearish = row['close'] < row['open']
        # 条件3: 放量（三选一，可单独禁用）
        jcfg = s1_config
        use_prev = jcfg.get('vol_ratio_prev') is not None
        use_ma20 = jcfg.get('vol_ratio_ma20') is not None
        use_max = jcfg.get('max_bull_vol_ratio') is not None
        th_prev = jcfg.get('vol_ratio_prev', 1.5)
        th_ma20 = jcfg.get('vol_ratio_ma20', 2.0)
        th_max = jcfg.get('max_bull_vol_ratio', 0.9)
        is_heavy_vol = ((use_prev and row['vol_ratio_prev'] >= th_prev) or
                        (use_ma20 and row['vol_ratio_ma20'] >= th_ma20))
        # 区间最大量 = 量超过波内所有阳线（阴线量大于阳线量 = 出货压倒吸筹）
        is_max_vol = (use_max and max_bull_vol > 0 and row['volume'] >= max_bull_vol * th_max)
        # 条件4: 高位（可禁用）
        use_price_pos = jcfg.get('price_pos') is not None
        th_pos = jcfg.get('price_pos', 0.70)
        price_pos = (row['close'] - range_low) / (range_high - range_low) if range_high > range_low else 0
        price_ok = (not use_price_pos) or (price_pos >= th_pos) or (i in peak_neighbors)
        if (is_bearish and (is_heavy_vol or is_max_vol) and price_ok):
            if '放量巨阴' in _skip:
                continue
            return True, row['date'], '放量巨阴'

    # 类型2：顶部大风车（整波扫描）
    for i in range(len(recent) - 1, surge_start_idx, -1):
        row = recent.iloc[i]
        bullish = (row['MA5'] > row['MA10'] > row['MA20']) if has_ma else True
        if not bullish: continue
        total_range = row['high'] - row['low']
        if total_range == 0:
            continue
        upper_shadow = row['high'] - max(row['close'], row['open'])
        prev_close = recent.iloc[i - 1]['close'] if i > 0 else row['close']
        day_gain = (row['close'] - prev_close) / prev_close * 100
        if day_gain >= 2:
            continue
        if (upper_shadow / total_range > 0.618 and
                row['volume'] > recent['volume'].iloc[:i].mean() * 1.5 and
                row['is_max_vol']):
            if '顶部大风车' in _skip:
                continue
            return True, row['date'], '顶部大风车'

    # 类型3：次高点放量（整波扫描）
    high_20 = recent['high'].rolling(20, min_periods=1).max()
    recent['is_high'] = recent['high'] >= high_20 * 0.97
    for i in range(surge_start_idx, len(recent)):
        row = recent.iloc[i]
        bullish = (row['MA5'] > row['MA10'] > row['MA20']) if has_ma else True
        if not bullish: continue
        if not (row['is_high'] and row['is_real_bearish']):
            continue
        if i - surge_start_idx < 3:
            continue
        interval_vol = recent.loc[surge_start_idx:i-1, 'volume']
        if interval_vol.empty:
            continue
        interval_max = interval_vol.max()
        if row['volume'] < interval_max * 0.99:
            continue
        interval_df = recent.loc[surge_start_idx:i]
        max_high_idx = interval_df['high'].idxmax()
        max_high_close = interval_df.loc[max_high_idx, 'close']
        open_price = row['open']
        if max_high_close == 0:
            continue
        drop_pct = (max_high_close - open_price) / max_high_close * 100
        if not (0 < drop_pct < 3.82):
            continue
        if '次高点放量' in _skip:
            continue
        return True, row['date'], '次高点放量'

    return False, None, None