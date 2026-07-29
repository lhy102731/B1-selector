"""
技术指标计算模块 - 通达信公式函数实现
"""
import pandas as pd
import numpy as np
import pandas_ta as ta


def MA(series, n):
    """
    简单移动平均 - 正确处理倒序排列的数据
    
    对于倒序数据，MA(n)应该取当前及之后n-1个数据的平均值
    实现方式：反转数据 -> 计算rolling -> 反转回来
    """
    # 反转数据，使数据按时间正序排列
    reversed_series = series.iloc[::-1]
    
    # 在正序数据上计算MA（向前看n个值）
    ma_reversed = reversed_series.rolling(window=n, min_periods=1).mean()
    
    # 反转回来，恢复倒序
    return ma_reversed.iloc[::-1].reset_index(drop=True).set_axis(series.index)


def EMA(series, n):
    """
    指数移动平均 - 正确处理倒序排列的数据
    """
    reversed_series = series.iloc[::-1]
    ema_reversed = reversed_series.ewm(span=n, adjust=False, min_periods=1).mean()
    return ema_reversed.iloc[::-1].reset_index(drop=True).set_axis(series.index)


def LLV(series, n):
    """
    N周期最低值 - 正确处理倒序排列的数据
    """
    reversed_series = series.iloc[::-1]
    llv_reversed = reversed_series.rolling(window=n, min_periods=1).min()
    return llv_reversed.iloc[::-1].reset_index(drop=True).set_axis(series.index)


def HHV(series, n):
    """
    N周期最高值 - 正确处理倒序排列的数据
    """
    reversed_series = series.iloc[::-1]
    hhv_reversed = reversed_series.rolling(window=n, min_periods=1).max()
    return hhv_reversed.iloc[::-1].reset_index(drop=True).set_axis(series.index)


def SMA(X, n, m):
    """
    移动平均 - 通达信风格
    SMA(X,N,M): X的N日移动平均, M为权重
    公式: Y = (X*M + Y'*(N-M)) / N
    """
    result = pd.Series(index=X.index, dtype=float)
    result.iloc[0] = X.iloc[0]
    for i in range(1, len(X)):
        result.iloc[i] = (X.iloc[i] * m + result.iloc[i-1] * (n - m)) / n
    return result


def REF(series, n):
    """
    向前引用N周期 - 正确处理倒序排列的数据
    
    对于倒序数据（最新在前），REF(series, 1)应该获取"前一天"的数据
    实现方式：反转数据 -> shift -> 反转回来
    """
    reversed_series = series.iloc[::-1]
    ref_reversed = reversed_series.shift(n)
    return ref_reversed.iloc[::-1].reset_index(drop=True).set_axis(series.index)


def EXIST(cond, n):
    """
    N周期内是否存在满足COND的情况 - 正确处理倒序排列的数据
    """
    reversed_cond = cond.iloc[::-1]
    exist_reversed = reversed_cond.rolling(window=n, min_periods=1).max().astype(bool)
    return exist_reversed.iloc[::-1].reset_index(drop=True).set_axis(cond.index)


def FINANCE(df, field_code):
    """
    财务数据获取
    39: 流通市值（元）；生产 CSV 的 market_cap 统一采用该口径。
    """
    if field_code == 39:
        return df.get('market_cap', pd.Series([0] * len(df), index=df.index))
    return pd.Series([0] * len(df), index=df.index)


def KDJ(df, n=9, m1=3, m2=3):
    """
    使用 pandas_ta 计算 KDJ 指标。

    参数
    ----------
    df : DataFrame
        必须包含 'high', 'low', 'close' 三列，以及可选的 'date' 列用于顺序判断。
    n : int, default 9
        RSV 的周期。
    m1 : int, default 3
        K 值的平滑周期（必须等于 m2，因为 pandas_ta 使用同一 signal 参数）。
    m2 : int, default 3
        D 值的平滑周期（必须等于 m1，否则会引发 ValueError）。

    返回
    -------
    DataFrame
        包含 'K', 'D', 'J' 三列，索引与输入 df 一致。
    """
    # pandas_ta 的 kdj 要求 m1 == m2（共用 signal 参数）
    if m1 != m2:
        raise ValueError("pandas_ta 版本的 KDJ 要求 m1 == m2，因为底层使用相同的 signal 参数。")

    # 检查必需的数据列
    required_cols = ['high', 'low', 'close']
    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"DataFrame 中缺少必要的列: {col}")

    # --- 处理数据顺序（保持与原函数行为一致）---
    # 优先使用 'date' 列判断顺序；否则假设索引是日期类型且单调
    if 'date' in df.columns:
        date_series = pd.to_datetime(df['date'])
        is_descending = date_series.iloc[0] > date_series.iloc[-1]
    else:
        # 若索引是 DatetimeIndex，则根据首尾判断；否则默认升序（不反转）
        if isinstance(df.index, pd.DatetimeIndex):
            is_descending = df.index[0] > df.index[-1]
        else:
            is_descending = False

    # 若数据为降序（最新在前），则反转成升序进行计算
    if is_descending:
        df_calc = df.iloc[::-1].copy().reset_index(drop=True)
    else:
        df_calc = df.copy().reset_index(drop=True)

    # --- 使用 pandas_ta 计算 KDJ ---
    kdj_result = ta.kdj(
        high=df_calc['high'],
        low=df_calc['low'],
        close=df_calc['close'],
        length=n,
        signal=m1          # 此处 signal 同时用于 K 和 D 的平滑
    )

    # pandas_ta 返回的列名通常为 f'K_{n}', f'D_{n}', f'J_{n}'，重命名为标准名称
    if kdj_result is not None and not kdj_result.empty:
        # 尝试用标准列名提取，若失败则取前三列并重命名
        col_k = f'K_{n}'
        col_d = f'D_{n}'
        col_j = f'J_{n}'
        if col_k in kdj_result.columns:
            kdj_df = kdj_result[[col_k, col_d, col_j]].rename(
                columns={col_k: 'K', col_d: 'D', col_j: 'J'}
            )
        else:
            # 兼容旧版本或不同命名规则
            kdj_df = kdj_result.iloc[:, :3].copy()
            kdj_df.columns = ['K', 'D', 'J']
    else:
        # 计算失败时返回空列（例如数据行数不足）
        kdj_df = pd.DataFrame({'K': pd.Series(dtype=float),
                               'D': pd.Series(dtype=float),
                               'J': pd.Series(dtype=float)})

    # --- 恢复原始顺序并设置索引 ---
    if is_descending:
        kdj_df = kdj_df.iloc[::-1].reset_index(drop=True)

    kdj_df.index = df.index

    return kdj_df

def calculate_zhixing_trend(df, m1=14, m2=28, m3=57, m4=114):
    """
    计算知行趋势线指标
    
    指标定义:
    - 知行短期趋势线 = EMA(EMA(CLOSE,10),10)
      对收盘价连续做两次10日指数移动平均
    
    - 知行多空线 = (MA(CLOSE,m1) + MA(CLOSE,m2) + MA(CLOSE,m3) + MA(CLOSE,m4)) / 4
      四条均线平均值，默认使用 14, 28, 57, 114
    
    参数:
        m1, m2, m3, m4: 多空线计算用的MA周期，默认14, 28, 57, 114
    """
    # 知行短期趋势线 = EMA(EMA(CLOSE,10),10)
    short_term_trend = EMA(EMA(df['close'], 10), 10)
    
    # 知行多空线 = (MA(m1) + MA(m2) + MA(m3) + MA(m4)) / 4
    bull_bear_line = (MA(df['close'], m1) + MA(df['close'], m2) + 
                      MA(df['close'], m3) + MA(df['close'], m4)) / 4
    
    return pd.DataFrame({
        'short_term_trend': short_term_trend,
        'bull_bear_line': bull_bear_line
    }, index=df.index)
def calculate_white_line(df, period=10):
    """
    白线（知行短期趋势线）= EMA(EMA(CLOSE, period), period)
    """
    return EMA(EMA(df['close'], period), period)


def calculate_yellow_line(df, m1=14, m2=28, m3=57, m4=114):
    """
    黄线（大哥线/多空线）= (MA14 + MA28 + MA57 + MA114) / 4
    """
    return (MA(df['close'], m1) + MA(df['close'], m2) +
            MA(df['close'], m3) + MA(df['close'], m4)) / 4
