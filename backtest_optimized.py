"""
针对 Ryzen 9800X3D 优化的选股系统回测脚本（师傅战法完整版 · 终极极速版）
特性：
- 分批建仓（1/3）
- 第二批：回踩黄线(J<13+缩量) 或 超级B1
- 第三批：B2
- 止损：信号日最低价-0.05（含击穿对手盘放宽）
- 动态仓位：单票买入金额 = 当前总资产 × 10% × 1/3
- S1减仓≥50%，盈利20%止盈30%，滴滴/白线减仓30%（需launched）
- launched：浮盈>5%且收盘>白线
- 同一天减仓信号只执行第一个
- 极速模式：进程池复用策略引擎 + 子进程自加载缓存，无大数据传递
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
import multiprocessing as mp
from functools import partial
import warnings
import argparse
import json
import pickle
import hashlib
import os as _os
import re
import tempfile

warnings.filterwarnings('ignore')
sys.path.insert(0, str(Path(__file__).parent))
from utils.csv_manager import CSVManager
from strategy.strategy_registry import get_registry
from utils.stock_scorer import StockScorer
from utils.s1_filter import detect_s1_signal

# ======================== 模块级辅助函数（多进程优化版） ========================

# ---- 子进程全局变量 ----
_worker_strategy = None
_worker_scorer = None
_worker_stock_names = None
_worker_data_dir = None
_worker_indicators_cache_name = "indicators_cache"
_worker_parquet_cache = {}       # ★ 内存缓存：code -> full DataFrame，消除磁盘 I/O
_worker_exclude_limit = False


def _indicator_cache_path(data_dir, cache_name, code):
    return Path(data_dir) / cache_name / f"{code}.parquet"


def _safe_cache_name(cache_name):
    value = str(cache_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("indicator cache name may contain only letters, numbers, '_' and '-'")
    return value


def _exact_latest_index(dates, target_date):
    """Return the target session row, never an earlier stock-specific bar."""
    target = pd.Timestamp(target_date).normalize()
    cutoff = np.searchsorted(dates, target.to_datetime64(), side='right')
    if cutoff == 0:
        return None
    latest_idx = cutoff - 1
    if pd.Timestamp(dates[latest_idx]).normalize() != target:
        return None
    return latest_idx


def _sha256_path(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _build_signal_cache_identity(
    data_dir,
    indicator_cache_name,
    codes,
    trading_days,
    *,
    contract_paths=None,
):
    """Bind signal results to exact data, universe, calendar, and code bytes."""
    data_root = Path(data_dir)
    normalized_codes = sorted({str(code).zfill(6) for code in codes})

    data_digest = hashlib.sha256()
    for code in normalized_codes:
        path = _indicator_cache_path(data_root, indicator_cache_name, code)
        data_digest.update(code.encode('ascii'))
        data_digest.update(b'\0')
        data_digest.update(
            _sha256_path(path).encode('ascii') if path.exists() else b'MISSING'
        )
        data_digest.update(b'\n')

    if contract_paths is None:
        root = Path(__file__).resolve().parent
        contract_paths = [
            Path(__file__).resolve(),
            root / 'strategy' / 'unified_b1_strategy.py',
            root / 'strategy' / 'pattern_matcher.py',
            root / 'strategy' / 'pattern_config.py',
            root / 'utils' / 's1_filter.py',
            root / 'utils' / 'stock_scorer.py',
        ]
    contract_digest = hashlib.sha256()
    for path_value in sorted((Path(path).resolve() for path in contract_paths), key=str):
        contract_digest.update(str(path_value).encode('utf-8'))
        contract_digest.update(b'\0')
        contract_digest.update(
            _sha256_path(path_value).encode('ascii')
            if path_value.exists()
            else b'MISSING'
        )
        contract_digest.update(b'\n')

    return {
        'schema_version': 1,
        'data_snapshot_id': data_digest.hexdigest(),
        'universe_id': hashlib.sha256(
            '\n'.join(normalized_codes).encode('utf-8')
        ).hexdigest(),
        'calendar_id': hashlib.sha256(
            '\n'.join(str(day) for day in trading_days).encode('utf-8')
        ).hexdigest(),
        'feature_contract_id': contract_digest.hexdigest(),
        'indicator_cache_name': str(indicator_cache_name),
    }


def _load_signal_cache(path, expected_identity):
    try:
        with Path(path).open('rb') as handle:
            payload = pickle.load(handle)
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get('schema_version') != 1:
        return None
    if payload.get('identity') != expected_identity:
        return None
    signals = payload.get('signals')
    return signals if isinstance(signals, dict) else None


def _save_signal_cache(path, identity, signals):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='wb',
            dir=destination.parent,
            prefix=f'.{destination.name}.',
            suffix='.tmp',
            delete=False,
        ) as handle:
            pickle.dump(
                {
                    'schema_version': 1,
                    'identity': identity,
                    'signals': signals,
                },
                handle,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
            handle.flush()
            _os.fsync(handle.fileno())
            temporary = Path(handle.name)
        _os.replace(temporary, destination)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()

def _process_initializer(strategy_params_path, data_dir, stock_names, exclude_limit_state=False, skip_params=None, indicators_cache_name="indicators_cache"):
    """子进程初始化：加载策略、评分器、股票名称（静默）"""
    global _worker_strategy, _worker_scorer, _worker_stock_names, _worker_data_dir, _worker_exclude_limit, _worker_indicators_cache_name
    import os
    old_stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w', encoding='utf-8')
    try:
        registry = get_registry(strategy_params_path)
        registry.auto_register_from_directory("strategy")
        strategy = registry.get_strategy('UnifiedB1Strategy')
        if not strategy:
            from strategy.unified_b1_strategy import UnifiedB1Strategy
            strategy = registry.register(UnifiedB1Strategy, name='UnifiedB1Strategy')
        if skip_params:
            for k, v in skip_params.items():
                strategy.params[k] = v
        _worker_strategy = strategy
        _worker_scorer = StockScorer(CSVManager(data_dir), registry, exclude_limit_state=exclude_limit_state)
        _worker_stock_names = stock_names
        _worker_data_dir = data_dir
        _worker_indicators_cache_name = indicators_cache_name
        _worker_exclude_limit = exclude_limit_state
    finally:
        sys.stdout = old_stdout

def _load_and_evaluate_unpack(args):
    """imap_unordered 包装器：解包元组参数"""
    return _load_and_evaluate(*args)


def _load_and_evaluate(code, end_date, min_similarity):
    """子进程任务：加载一只股票的缓存并评估，返回信号或 None"""
    global _worker_parquet_cache
    try:
        # 延迟加载 + 内存缓存：首次访问从磁盘读取，后续所有交易日从内存读取
        entry = _worker_parquet_cache.get(code)
        if entry is None:
            if code in _worker_parquet_cache:  # 已标记为不存在
                return None
            cache_path = _indicator_cache_path(_worker_data_dir, _worker_indicators_cache_name, code)
            if not cache_path.exists():
                _worker_parquet_cache[code] = None
                return None
            df_cache = pd.read_parquet(cache_path)
            if df_cache.empty:
                _worker_parquet_cache[code] = None
                return None
            df_cache['date'] = pd.to_datetime(df_cache['date'])
            dates = df_cache['date'].values          # ★ 预提取日期数组供二分查找
            _worker_parquet_cache[code] = (df_cache, dates)
        else:
            df_cache, dates = entry

        # ★ 用 searchsorted 二分定位截至日期行号（O(log N)），替代全量布尔掩码（O(N)）
        latest_idx = _exact_latest_index(dates, end_date)
        if latest_idx is None:
            return None
        cutoff = latest_idx + 1

        # 快速预筛选：直接从原始 DataFrame 读最新行的列（O(1)，无需创建过滤 DataFrame）
        row = df_cache.iloc[latest_idx]
        if not row['white_gt_yellow']:
            return None
        if row['J'] >= _worker_strategy.params.get('j_threshold', 30):
            return None
        # ★ 缩量动态重算：取当天及前19天的最高量
        vol_win = df_cache.iloc[max(0, latest_idx - 19):latest_idx + 1]['volume']
        hhv_vol_20 = vol_win.max()
        vol_shrink_ratio = _worker_strategy.params.get('volume_shrink_ratio', 0.618)
        if row['volume'] >= hhv_vol_20 * vol_shrink_ratio:
            return None
        if row['DIF'] <= 0:
            return None
        if row['doubled']:
            return None
        if row.get('market_cap', 999e9) < _worker_strategy.params.get('cap_threshold', 4_000_000_000):
            return None

        name = _worker_stock_names.get(code, '未知')
        if any(kw in name for kw in ['退', 'ST', '*ST']):
            return None

        # 预筛选通过 → 创建过滤后的 DataFrame 供 select_stocks 使用（仅 ~5% 股票到达此处）
        df_filtered = df_cache.iloc[:cutoff]
        df_filtered = df_filtered.sort_values('date', ascending=False).reset_index(drop=True)

        signals = _worker_strategy.select_stocks(df_filtered, name)
        if not signals:
            return None

        signal = signals[0]
        surge = signal.get('surge_start_date')
        score_info = _worker_scorer.score_stock(code, df_filtered, start_date=surge)
        b1_score = score_info.get('b1_score', 0)
        if b1_score < min_similarity:
            return None

        bonus = _calc_bonus_static(df_filtered)
        b1_score += bonus
        b1_score = min(b1_score, 100)
        return {
            'code': code, 'name': name,
            'b1_score': b1_score,
            'close': signal['close'],
            'signal': signal,
            'is_washout': signal.get('is_washout', False),
            'is_super_b1': signal.get('is_super_b1', False),
            'build_gain': signal.get('build_gain', 0),
            'surge_turnover': signal.get('surge_turnover', 0),
            'surge_start_date': signal.get('surge_start_date'),
            'signal_day_low': row['low']
        }
    except Exception as e:
        import traceback
        sys.stderr.write(f"[ERROR] {code}: {e}\n{traceback.format_exc()}\n")
        return None

def _calc_bonus_static(df):
    """加分项（静态版本）"""
    bonus = 0
    if df.empty or len(df) < 50:
        return 0
    asc = df.sort_values('date').reset_index(drop=True)
    n = len(asc)
    if n >= 50:
        recent = asc.iloc[-50:]
        price_low = recent['close'].min()
        vol_low = recent['volume'].min()
        if asc.iloc[-1]['close'] <= price_low * 1.02 and asc.iloc[-1]['volume'] > vol_low * 1.1:
            bonus += 5
        dif_low = recent['DIF'].min()
        if asc.iloc[-1]['DIF'] > dif_low and asc.iloc[-1]['close'] <= price_low * 1.02:
            bonus += 5
    if n >= 10:
        c10 = asc['close'].iloc[-10:]
        v10 = asc['volume'].iloc[-10:]
        if np.polyfit(np.arange(10), c10, 1)[0] > 0 and np.polyfit(np.arange(10), v10, 1)[0] > 0:
            bonus += 3
    return bonus

# ---- 优化4：预计算信号日（模块级 worker 函数） ----
def _precompute_stock_unpack(args):
    return _precompute_stock(*args)

def _precompute_stock(code, trading_dates, trading_date_strs, min_similarity, decouple_j, decouple_vol, decouple_near):
    """一次加载 parquet，遍历所有交易日，返回该股票所有信号日的列表
    decouple_j=True: J不在此处过滤，存raw_j供后筛（宽松边界J<35）
    decouple_vol=True: 缩量不在此处过滤，存raw_vol_ratio供后筛（宽松边界<0.90）
    decouple_near=True: near_pct不在此处过滤，存raw位置距离供后筛（宽松边界100%）
    未解耦的参数用当前config值直接过滤，不存raw值——减少缓存体积"""
    global _worker_parquet_cache
    try:
        entry = _worker_parquet_cache.get(code)
        if entry is None:
            if code in _worker_parquet_cache:
                return []
            cache_path = _indicator_cache_path(_worker_data_dir, _worker_indicators_cache_name, code)
            if not cache_path.exists():
                _worker_parquet_cache[code] = None
                return []
            df_cache = pd.read_parquet(cache_path)
            if df_cache.empty:
                _worker_parquet_cache[code] = None
                return []
            df_cache['date'] = pd.to_datetime(df_cache['date'])
            dates = df_cache['date'].values
            _worker_parquet_cache[code] = (df_cache, dates)
        else:
            df_cache, dates = entry

        name = _worker_stock_names.get(code, '未知')
        if any(kw in name for kw in ['退', 'ST', '*ST']):
            return []

        j_threshold = _worker_strategy.params.get('j_threshold', 30)
        vol_shrink = _worker_strategy.params.get('volume_shrink_ratio', 0.618)

        results = []
        for i, end_ts in enumerate(trading_dates):
            latest_idx = _exact_latest_index(dates, end_ts)
            if latest_idx is None:
                continue
            cutoff = latest_idx + 1
            row = df_cache.iloc[latest_idx]

            # 参数无关的硬性预筛
            if not row['white_gt_yellow']:
                continue
            if row['DIF'] <= 0:
                continue
            if row['doubled']:
                continue
            if row.get('market_cap', 999e9) < 4_000_000_000:
                continue

            # J 阈值：解耦时宽松边界裁剪+存raw，未解耦时用config直接过滤
            raw_j = row['J']
            if decouple_j:
                if raw_j >= 200:   # 与YAML J=200一致，覆盖全范围
                    continue
            else:
                if raw_j >= j_threshold:
                    continue

            # 缩量比：解耦时宽松边界裁剪+存raw，未解耦时用config直接过滤
            vol_win = df_cache.iloc[max(0, latest_idx - 19):latest_idx + 1]['volume']
            hhv_vol_20 = vol_win.max()
            raw_vol_ratio = row['volume'] / hhv_vol_20 if hhv_vol_20 > 0 else 1.0
            if decouple_vol:
                if raw_vol_ratio >= 2.0:   # 无实际限制（成交量/20日均量很少超过2）
                    continue
            else:
                if raw_vol_ratio >= vol_shrink:
                    continue

            df_filtered = df_cache.iloc[:cutoff]
            df_filtered = df_filtered.sort_values('date', ascending=False).reset_index(drop=True)

            # ★ 解耦参数：临时将策略阈值改为宽松值，确保select_stocks生成全部潜在信号
            old_j = _worker_strategy.params.get('j_threshold')
            old_vol = _worker_strategy.params.get('volume_shrink_ratio')
            old_near = _worker_strategy.params.get('near_pct', 3.5)
            if decouple_j:
                _worker_strategy.params['j_threshold'] = 35
            if decouple_vol:
                _worker_strategy.params['volume_shrink_ratio'] = 0.90
            if decouple_near:
                _worker_strategy.params['near_pct'] = 100.0

            signals = _worker_strategy.select_stocks(df_filtered, name)

            _worker_strategy.params['j_threshold'] = old_j
            _worker_strategy.params['volume_shrink_ratio'] = old_vol
            _worker_strategy.params['near_pct'] = old_near

            # ★ 捕捉原始位置数据（解耦near_pct时用于后筛）
            raw_fall_in_bowl = None
            raw_dist_yellow_pct = None
            raw_dist_white_pct = None
            if decouple_near:
                r_close = row['close']
                r_yellow = row['yellow_line']
                r_white = row['white_line']
                raw_fall_in_bowl = (r_close >= r_yellow) and (r_close <= r_white)
                if r_close >= r_yellow and r_yellow > 0:
                    raw_dist_yellow_pct = round((r_close - r_yellow) / r_yellow * 100, 2)
                if r_white > 0:
                    raw_dist_white_pct = round(abs(r_close - r_white) / r_white * 100, 2)
            if not signals:
                continue

            surge = signals[0].get('surge_start_date')
            score_info = _worker_scorer.score_stock(code, df_filtered, start_date=surge)
            b1_score = score_info.get('b1_score', 0)
            if b1_score <= 0:
                continue

            bonus = _calc_bonus_static(df_filtered)
            b1_score += bonus
            b1_score = min(b1_score, 100)

            signal = signals[0]
            entry = {
                'date_str': trading_date_strs[i],
                'code': code, 'name': name,
                'b1_score': b1_score,
                'close': signal['close'],
                'signal': signal,
                'is_washout': signal.get('is_washout', False),
                'is_super_b1': signal.get('is_super_b1', False),
                'build_gain': signal.get('build_gain', 0),
                'surge_turnover': signal.get('surge_turnover', 0),
                'surge_start_date': signal.get('surge_start_date'),
                'signal_day_low': row['low'],
            }
            if decouple_j:
                entry['raw_j'] = raw_j
            if decouple_vol:
                entry['raw_vol_ratio'] = raw_vol_ratio
            if decouple_near:
                entry['fall_in_bowl'] = raw_fall_in_bowl
                entry['raw_dist_yellow_pct'] = raw_dist_yellow_pct
                entry['raw_dist_white_pct'] = raw_dist_white_pct
            results.append(entry)
        return results
    except Exception:
        return []


# ======================== 主回测类 ========================
class OptimizedBacktester:
    def __init__(self, data_dir='data', use_cache=False, initial_cash=1_000_000, strategy_config="config/strategy_params.yaml", indicators_cache_name="indicators_cache"):
        self.data_dir = Path(data_dir)
        self.cache_dir = self.data_dir / "cache"
        self.use_cache = use_cache
        self.csv_manager = CSVManager(str(data_dir))
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.strategy_config = strategy_config
        self.indicators_cache_name = _safe_cache_name(indicators_cache_name)

        self.registry = get_registry(strategy_config)
        self.registry.auto_register_from_directory("strategy")
        self.strategy = self.registry.get_strategy('UnifiedB1Strategy')
        if not self.strategy:
            from strategy.unified_b1_strategy import UnifiedB1Strategy
            self.strategy = self.registry.register(UnifiedB1Strategy, name='UnifiedB1Strategy')
        self.scorer = StockScorer(self.csv_manager, self.registry)

        self.all_stock_codes = self.csv_manager.list_all_stocks()
        self.stock_names = self._load_stock_names()

        self.max_stocks_per_day = 10
        self.position_pct = 0.10
        self.batch_pct = self.position_pct / 3
        self.commission = 0.0003
        self.slippage = 0.001
        self.min_similarity = 60
        self.exclude_limit_state = False
        self.skip_wave_quality = False
        self.skip_wave_break = False
        self.skip_s1 = False
        self.skip_washout = False
        self.decouple_params = set()   # 解耦参数集合，如 {'j_threshold', 'volume_shrink_ratio'}
        self._near_pct_override = None # 命令行覆盖 near_pct 值（用于扫参，不修改YAML）
        self._near_pct_bull = None     # 多头区间的 near_pct（动态切换）
        self._near_pct_bear = None     # 空头区间的 near_pct（动态切换）
        self.zone_j_ranges = None      # J值允许区间列表 [(low,high), ...] 或 None(不过滤)
        self.zone_vol_ranges = None    # 缩量比允许区间列表

        self.positions = []
        self.closed_trades = []
        self.daily_equity = []
        self.use_indicators_cache = True
        self._indicators_cache = {}     # ★ 持久缓存 code -> full DataFrame
        self._daily_indicators = {}     # 日内缓存 (code, end_date) -> filtered DataFrame
        self.trading_days = []
        self.market_timing = None       # 活跃市值择时开关，默认关闭
        self.use_ai_scorer = False     # 使用AI多因子评分替代相似度排序
        self.ai_scorer = None          # AIScorer实例

    def _init_ai_scorer(self):
        if self.ai_scorer is None:
            from strategy.ai_scorer import AIScorer
            self.ai_scorer = AIScorer(str(self.data_dir))
            # 尝试加载信号缓存用于概念共振
            import pickle
            cache_dir = Path(self.data_dir) / 'signal_cache'
            for f in sorted(cache_dir.glob('*.pkl'), reverse=True):
                try:
                    with open(f, 'rb') as pf:
                        self.ai_scorer.signal_cache = pickle.load(pf)
                    break
                except:
                    pass

    def _load_stock_names(self):
        names_file = self.data_dir / 'stock_names.json'
        if names_file.exists():
            with open(names_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def _get_realtime_indicators(self, code, end_date):
        # 日内缓存：同一日同一股票不重复过滤
        cache_key = (code, end_date)
        if cache_key in self._daily_indicators:
            return self._daily_indicators[cache_key]

        # 持久缓存：首次加载 parquet，后续从内存读取
        if code not in self._indicators_cache:
            cache_path = _indicator_cache_path(self.data_dir, self.indicators_cache_name, code)
            if self.use_indicators_cache and cache_path.exists():
                try:
                    df_full = pd.read_parquet(cache_path)
                    if not df_full.empty:
                        df_full['date'] = pd.to_datetime(df_full['date'])
                        self._indicators_cache[code] = df_full
                    else:
                        self._indicators_cache[code] = None
                except Exception:
                    self._indicators_cache[code] = None
            else:
                self._indicators_cache[code] = None

        df_full = self._indicators_cache.get(code)
        if df_full is not None:
            df_filtered = df_full[df_full['date'] <= pd.to_datetime(end_date)]
            if not df_filtered.empty:
                df_filtered = df_filtered.sort_values('date', ascending=False).reset_index(drop=True)
                self._daily_indicators[cache_key] = df_filtered
                return df_filtered

        # 兜底：从原始 CSV 读取并实时计算指标
        df_raw = self.csv_manager.read_stock(code)
        if df_raw.empty:
            self._daily_indicators[cache_key] = pd.DataFrame()
            return pd.DataFrame()
        df_raw['date'] = pd.to_datetime(df_raw['date'])
        df_raw = df_raw[df_raw['date'] <= pd.to_datetime(end_date)].copy()
        df_raw = df_raw[df_raw['volume'] > 0]
        if len(df_raw) < 60:
            self._daily_indicators[cache_key] = pd.DataFrame()
            return pd.DataFrame()
        df_raw = df_raw.sort_values('date', ascending=False).reset_index(drop=True)
        df_indicators = self.strategy.calculate_indicators(df_raw)
        self._daily_indicators[cache_key] = df_indicators
        return df_indicators

    def _get_latest_price(self, code, date):
        df = self._get_realtime_indicators(code, date)
        if df.empty:
            return 0
        return df.iloc[0]['close']

    def _get_tradable_indicators_on_date(self, code, date):
        """Return indicators only for an actual tradable bar on exactly date."""
        if date is None:
            return pd.DataFrame()
        df = self._get_realtime_indicators(code, date)
        if df.empty:
            return pd.DataFrame()
        latest = df.iloc[0]
        latest_date = pd.to_datetime(latest.get('date'), errors='coerce')
        target_date = pd.to_datetime(date, errors='coerce')
        if pd.isna(latest_date) or pd.isna(target_date):
            return pd.DataFrame()
        if latest_date.normalize() != target_date.normalize():
            return pd.DataFrame()
        volume = pd.to_numeric(pd.Series([latest.get('volume')]), errors='coerce').iloc[0]
        if pd.isna(volume) or float(volume) <= 0:
            return pd.DataFrame()
        return df

    def _get_next_trading_day(self, date):
        if date not in self.trading_days:
            return None
        idx = self.trading_days.index(date)
        if idx + 1 < len(self.trading_days):
            return self.trading_days[idx + 1]
        return None

    def run_selection_on_date(self, date, pool, sample_size=None):
        target_date = pd.to_datetime(date)
        codes = self.all_stock_codes
        if sample_size:
            codes = codes[:sample_size]

        tasks = [(code, target_date, self.min_similarity) for code in codes]
        results = []
        # chunksize 设为任务数/worker数/4，平衡 IPC 开销和负载均衡
        chunksize = max(1, len(tasks) // (mp.cpu_count() * 4))
        for res in pool.imap_unordered(_load_and_evaluate_unpack, tasks, chunksize=chunksize):
            if res is not None:
                results.append(res)

        results.sort(key=lambda x: (-x['b1_score'], x['code']))
        return results[:self.max_stocks_per_day]

    def buy_stock(self, signal_date, stock_info, current_total_asset):
        """连续B1逐日建仓1/3，三批共用最后一次的止损和计时"""
        code = stock_info['code']
        next_date = self._get_next_trading_day(signal_date)
        df = self._get_tradable_indicators_on_date(code, next_date)
        if df.empty:
            return False
        buy_price = df.iloc[0]['open']
        if not np.isfinite(buy_price) or buy_price <= 0:
            return False
        target_money = current_total_asset * self.position_pct
        batch_money = target_money / 3
        shares = int(batch_money / buy_price / 100) * 100
        if shares == 0 or batch_money > self.cash:
            return False
        cost = shares * buy_price * (1 + self.commission)
        if cost > self.cash:
            return False
        stop_ref = stock_info['signal_day_low'] - 0.05

        # 查找是否已有同一股的仓位
        existing_pos = None
        for pos in self.positions:
            if pos['code'] == code:
                existing_pos = pos
                break

        if existing_pos and existing_pos.get('batch_count', 1) < 3:
            # 连续B1：加仓一批，更新止损和买入日为最后一次信号
            existing_pos['shares'] += shares
            existing_pos['cost'] += cost
            existing_pos['batch_count'] += 1
            existing_pos['batch_prices'].append(buy_price)
            existing_pos['batch_dates'].append(next_date)
            # ★ 止损更新为最后一次B1，但4天计时保持第一批买入日不变
            existing_pos['stop_loss_ref'] = stop_ref
            existing_pos['stop_loss_ref_active'] = stop_ref
            existing_pos['buy_date'] = signal_date
            existing_pos['washout_start_date'] = None
            existing_pos['washout_low'] = None
            self.cash -= cost
            return True

        if existing_pos:
            return False  # 已有3批

        # 新仓位：第一批
        self.cash -= cost
        pos = {
            'code': code, 'shares': shares,
            'batch_count': 1,
            'batch_prices': [buy_price],
            'batch_dates': [next_date],
            'buy_date': signal_date, 'actual_buy_date': next_date,
            'buy_price': buy_price, 'cost': cost,
            'b1_score': stock_info['b1_score'],
            'is_washout': stock_info['is_washout'],
            'is_super_b1': stock_info.get('is_super_b1', False),
            'signal_j': stock_info.get('raw_j'),        # ★ 买入信号时的J值
            'ai_score': stock_info.get('ai_score'),      # ★ AI多因子评分
            'signal_vol_ratio': stock_info.get('raw_vol_ratio'),  # ★ 买入信号时的缩量比
            'surge_start_date': stock_info.get('surge_start_date'),  # ★ 异动起点
            'stop_loss_ref': stop_ref, 'stop_loss_ref_active': stop_ref,
            'launched': False, 'washout_start_date': None, 'washout_low': None,
            'partial_sold': 0, 's1_sold': 0, 'didi_sold': 0, 'white_sold': 0,
            'has_been_profitable': False,
        }
        self.positions.append(pos)
        return True

    def _is_volume_shrink(self, df):
        if len(df) < 6: return False
        latest_vol = df.iloc[0]['volume']
        avg_vol = df.iloc[1:6]['volume'].mean()
        return latest_vol < avg_vol * 0.7

    def _is_super_b1_on_position(self, pos, df):
        signals = self.strategy.select_stocks(df, self.stock_names.get(pos['code'], ''))
        if signals and signals[0].get('is_super_b1'):
            return True
        return False

    def _add_batch(self, pos, date, price, reason, signal_low=None):
        target_money = (self.cash + self._total_market_value(date)) * self.position_pct / 3
        shares = int(target_money / price / 100) * 100
        if shares == 0: return
        cost = shares * price * (1 + self.commission)
        if cost > self.cash: return
        self.cash -= cost
        pos['shares'] += shares
        pos['cost'] += cost
        pos['batch_count'] += 1
        pos['batch_prices'].append(price)
        # 止损统一用信号日最低价 - 0.05（每批独立）
        stop_ref = signal_low if signal_low is not None else price
        pos['batch_stops'].append(stop_ref - 0.05)
        pos['stop_loss_ref_active'] = min(pos['batch_stops'])
        pos['batch_entry_log'].append(f"{reason}@{price}")

    def check_batch_entry(self, date):
        for pos in self.positions:
            if pos['batch_count'] >= 3: continue
            code = pos['code']
            df = self._get_tradable_indicators_on_date(code, date)
            if df.empty or len(df) < 5: continue
            latest = df.iloc[0]

            if pos['batch_count'] == 1:
                cond1 = (abs(latest['close'] - latest['yellow_line']) / latest['yellow_line'] < 0.02 and
                         latest['J'] < 13 and self._is_volume_shrink(df))
                cond2 = self._is_super_b1_on_position(pos, df)
                if cond1 or cond2:
                    next_date = self._get_next_trading_day(date)
                    next_df = self._get_tradable_indicators_on_date(code, next_date)
                    if not next_df.empty:
                        buy_price = next_df.iloc[0]['open']
                        self._add_batch(pos, date, buy_price, '第二批加仓', signal_low=latest['low'])

            elif pos['batch_count'] == 2:
                if self.strategy.detect_b2_signal(df):
                    next_date = self._get_next_trading_day(date)
                    next_df = self._get_tradable_indicators_on_date(code, next_date)
                    if not next_df.empty:
                        buy_price = next_df.iloc[0]['open']
                        self._add_batch(pos, date, buy_price, '第三批B2加仓', signal_low=latest['low'])

    def _total_market_value(self, date):
        mv = 0
        for pos in self.positions:
            price = self._get_latest_price(pos['code'], date)
            mv += pos['shares'] * price
        return mv

    def _sell_shares(self, pos, date, price, shares, reason):
        if shares <= 0 or shares > pos['shares']:
            shares = pos['shares']
        revenue = shares * price * (1 - self.commission)
        self.cash += revenue
        cost_part = pos['cost'] * (shares / pos['shares']) if pos['shares'] > 0 else 0
        pnl = revenue - cost_part
        self.closed_trades.append({
            'code': pos['code'], 'buy_date': pos['buy_date'], 'sell_date': date,
            'buy_price': pos['buy_price'], 'sell_price': price,
            'shares': shares, 'pnl': pnl,
            'pnl_pct': (pnl / cost_part) * 100 if cost_part > 0 else 0, 'reason': reason,
            'signal_j': pos.get('signal_j'),           # ★ 买入时的J值
            'ai_score': pos.get('ai_score'),            # ★ AI评分
            'signal_vol_ratio': pos.get('signal_vol_ratio'),  # ★ 买入时的缩量比
        })
        pos['shares'] -= shares
        pos['cost'] -= cost_part

    def _detect_s1_on_position(self, df, start_date=None):
        has_s1, _, _ = detect_s1_signal(df, surge_start_date=start_date)
        return has_s1

    def _detect_didi(self, df):
        """滴滴信号：T+1收盘 < T-1的min(开盘价,收盘价)"""
        if len(df) < 3: return False
        row0 = df.iloc[0]   # T+1
        row2 = df.iloc[2]   # T-1
        ref = min(row2['open'], row2['close'])
        return row0['close'] < ref

    def check_exits_master(self, date):
        to_remove = []
        for pos in self.positions:
            df = self._get_tradable_indicators_on_date(pos['code'], date)
            if df.empty or len(df) < 5: continue
            latest = df.iloc[0]
            sell_price = latest['close']
            avg_cost = pos['cost'] / pos['shares'] if pos['shares'] > 0 else 0
            profit_pct = (sell_price / avg_cost - 1) if avg_cost > 0 else 0
            if profit_pct > 0:
                pos['has_been_profitable'] = True
            if not pos.get('launched', False) and profit_pct > 0.05 and sell_price > latest['white_line']:
                pos['launched'] = True

            # 用交易日计算持有天数（避免周末/节假日误判）
            buy_day = pos['actual_buy_date']
            if buy_day in self.trading_days and date in self.trading_days:
                hold_days = self.trading_days.index(date) - self.trading_days.index(buy_day)
            else:
                hold_days = (pd.to_datetime(date) - pd.to_datetime(buy_day)).days

            # fix: 移除T+1滴滴战法止损 — 滴滴应为止盈信号(launched后生效)，非买入次日止损

            # 1. 黄线
            if sell_price < latest['yellow_line']:
                is_shrink = self._is_volume_shrink(df)
                if not is_shrink:
                    self._sell_shares(pos, date, sell_price, pos['shares'], '放量跌破黄线清仓')
                    to_remove.append(pos); continue
                else:
                    if pos.get('washout_start_date') is None:
                        pos['washout_start_date'] = date
                        pos['washout_low'] = latest['low']
                        pos['stop_loss_ref_active'] = pos['washout_low'] - 0.05
                    else:
                        # 击穿对手盘止损只设一次，不更新——跌破即清仓
                        if sell_price < pos['stop_loss_ref_active']:
                            self._sell_shares(pos, date, sell_price, pos['shares'], '击穿对手盘失败清仓')
                            to_remove.append(pos); continue
            else:
                if pos.get('washout_start_date') is not None:
                    pos['washout_start_date'] = None
                    pos['washout_low'] = None
                    pos['stop_loss_ref_active'] = pos['stop_loss_ref']

            # 2. 基础止损
            active_stop = pos.get('stop_loss_ref_active', pos.get('stop_loss_ref', pos['batch_prices'][0] - 0.05))
            if sell_price < active_stop:
                self._sell_shares(pos, date, sell_price, pos['shares'], '基础止损')
                to_remove.append(pos); continue

            # 3. T+3 盈利不足2%（仅检测一次）
            if hold_days == 3 and profit_pct < 0.02:
                self._sell_shares(pos, date, sell_price, pos['shares'], 'T+3盈利不足4%')
                to_remove.append(pos); continue

            # 4. 盈转亏已移除

            # 5. S1减仓≥50%
            if self._detect_s1_on_position(df, start_date=pos.get('surge_start_date')) and pos.get('s1_sold', 0) == 0:
                sell_shares = max(int(pos['shares'] * 0.50), 100)
                if sell_shares > 0 and sell_shares <= pos['shares']:
                    self._sell_shares(pos, date, sell_price, sell_shares, 'S1信号减仓≥50%')
                    pos['s1_sold'] = 1
                    if pos['shares'] == 0:
                        to_remove.append(pos); continue

            # 6. launched 才执行的减仓
            if pos.get('launched', False):
                if profit_pct >= 0.20 and pos.get('partial_sold', 0) == 0:
                    sell_shares = int(pos['shares'] * 0.30)
                    if sell_shares > 0:
                        self._sell_shares(pos, date, sell_price, sell_shares, '盈利20%止盈30%')
                        pos['partial_sold'] = 1
                        if pos['shares'] == 0: to_remove.append(pos); continue

                didi = self._detect_didi(df)
                white = sell_price < latest['white_line']
                # 同一天只执行一个减仓信号（滴滴优先），各自可独立触发（不同天互不阻止）
                didi_fired = False
                if didi and pos.get('didi_sold', 0) == 0:
                    sell_shares = int(pos['shares'] * 0.30)
                    if sell_shares > 0:
                        self._sell_shares(pos, date, sell_price, sell_shares, '滴滴信号减仓30%')
                        pos['didi_sold'] = 1
                        didi_fired = True
                        if pos['shares'] == 0: to_remove.append(pos); continue
                if white and pos.get('white_sold', 0) == 0 and not didi_fired:
                    sell_shares = int(pos['shares'] * 0.30)
                    if sell_shares > 0:
                        self._sell_shares(pos, date, sell_price, sell_shares, '跌破白线减仓30%')
                        pos['white_sold'] = 1
                        if pos['shares'] == 0: to_remove.append(pos); continue

        for p in to_remove:
            if p in self.positions:
                self.positions.remove(p)

    def mark_to_market(self, date):
        self._daily_indicators = {}
        total_mv = 0
        for pos in self.positions:
            df = self._get_realtime_indicators(pos['code'], date)
            if not df.empty:
                total_mv += pos['shares'] * df.iloc[0]['close']
        total_asset = self.cash + total_mv
        self.daily_equity.append({
            'date': date, 'cash': self.cash,
            'market_value': total_mv, 'total': total_asset
        })

    def _build_trading_calendar(self, start_date, end_date):
        # 优先用活跃市值CSV的日期列（已覆盖1993年以来全量交易日），避免扫描500只股票CSV
        if self.market_timing is not None and self.market_timing.df is not None:
            dates = self.market_timing.df['date'].dt.strftime('%Y-%m-%d').tolist()
            return [d for d in dates if start_date <= d <= end_date]

        all_dates = set()
        sample_codes = self.all_stock_codes[:500] if len(self.all_stock_codes) > 500 else self.all_stock_codes
        for code in tqdm(sample_codes, desc="构建交易日历", leave=False):
            df = self.csv_manager.read_stock(code)
            if not df.empty:
                all_dates.update(pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d').tolist())
        trading_days = sorted(list(all_dates))
        return [d for d in trading_days if start_date <= d <= end_date]

    def run(self, start_date, end_date, sample_size=None, n_workers=8):
        print(f"\n开始回测: {start_date} -> {end_date}")
        if self.market_timing:
            self.market_timing.summary()
        self.trading_days = self._build_trading_calendar(start_date, end_date)
        print(f"   交易日数: {len(self.trading_days)}")
        # ★ 预热OS页缓存：主进程读一遍所有 parquet，后续 worker 读取时命中内存
        cache_dir = Path(self.data_dir) / self.indicators_cache_name
        if cache_dir.exists():
            import time as _t
            _t0 = _t.time()
            files = list(cache_dir.glob("*.parquet"))
            for f in tqdm(files, desc="预热缓存", leave=False):
                with open(f, 'rb') as _f:
                    _f.read()
            print(f"   预热完成 ({len(files)} 文件, {_t.time()-_t0:.1f}s)")

        # ★ 优化4：预计算所有信号日（缓存到磁盘，解耦参数可复用）
        codes = self.all_stock_codes[:sample_size] if sample_size else self.all_stock_codes
        decouple_j = 'j_threshold' in self.decouple_params
        decouple_vol = 'volume_shrink_ratio' in self.decouple_params
        decouple_near = 'near_pct' in self.decouple_params
        dc_tag = ('J' if decouple_j else '') + ('V' if decouple_vol else '') + ('N' if decouple_near else '') or 'none'
        # 解耦参数用占位符哈希，使修改解耦参数值时仍能命中缓存
        with open(self.strategy_config, 'rb') as _pf:
            raw = _pf.read()
        if decouple_j or decouple_vol or decouple_near:
            import yaml as _yaml
            _cfg = _yaml.safe_load(raw)
            if decouple_j:
                _cfg['UnifiedB1Strategy']['j_threshold'] = '__DECOUPLED__'
            if decouple_vol:
                _cfg['UnifiedB1Strategy']['volume_shrink_ratio'] = '__DECOUPLED__'
            if decouple_near:
                _cfg['UnifiedB1Strategy']['near_pct'] = '__DECOUPLED__'
            params_hash = hashlib.md5(_yaml.dump(_cfg).encode()).hexdigest()[:8]
        else:
            params_hash = hashlib.md5(raw).hexdigest()[:8]
        skip_params = {}
        for flag in ('skip_wave_quality', 'skip_wave_break', 'skip_s1', 'skip_washout'):
            if getattr(self, flag, False):
                skip_params[flag] = True
        if getattr(self, 's1_skip_types', None):
            skip_params['s1_skip_types'] = self.s1_skip_types
        # 放量巨阴子条件配置
        s1_juyin = self.strategy.params.get('s1_juyin_config')
        if s1_juyin is not None:
            skip_params['s1_juyin_config'] = s1_juyin
        ls_tag = "_nolimit" if self.exclude_limit_state else ""
        skip_parts = []
        for k in sorted(skip_params):
            v = skip_params[k]
            if isinstance(v, set):
                skip_parts.append(f"{k}={'_'.join(sorted(v))}")
            else:
                skip_parts.append(k)
        skip_tag = "_" + "_".join(skip_parts) if skip_parts else ""
        cache_identity = _build_signal_cache_identity(
            self.data_dir,
            self.indicators_cache_name,
            codes,
            self.trading_days,
        )
        identity_hash = hashlib.sha256(
            json.dumps(cache_identity, sort_keys=True).encode('utf-8')
        ).hexdigest()[:16]
        print(
            f"   signal-cache identity: data={cache_identity['data_snapshot_id'][:12]} "
            f"universe={cache_identity['universe_id'][:12]} "
            f"calendar={cache_identity['calendar_id'][:12]}"
        )
        cache_key = f"sig_v4_{start_date}_{end_date}_{self.min_similarity}_{params_hash}_{dc_tag}_{identity_hash}{ls_tag}{skip_tag}"
        cache_hash = hashlib.md5(cache_key.encode()).hexdigest()[:12]
        cache_dir = Path(self.data_dir) / "signal_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f"{start_date}_{end_date}_{dc_tag}_{cache_hash}.pkl"

        cached_signals = _load_signal_cache(cache_path, cache_identity) if cache_path.exists() else None
        if cached_signals is not None:
            print(f"\n   加载预计算缓存 [{dc_tag}]: {cache_path.name}")
            self._precomputed_signals = cached_signals
            print(f"   加载完成，共 {sum(len(v) for v in self._precomputed_signals.values())} 条信号")
        else:
            if cache_path.exists():
                print(f"   rejected legacy or mismatched signal cache: {cache_path.name}")
            print(f"\n   预计算信号日 [{dc_tag}] ({n_workers} workers)...")
            trading_dates_ts = [np.datetime64(d) for d in self.trading_days]
            tasks = [(code, trading_dates_ts, self.trading_days, self.min_similarity, decouple_j, decouple_vol, decouple_near) for code in codes]

            self._precomputed_signals = {}  # date_str → [signal_dict, ...]
            with mp.Pool(processes=n_workers,
                         initializer=_process_initializer,
                         initargs=(self.strategy_config, str(self.data_dir), self.stock_names, self.exclude_limit_state, skip_params, self.indicators_cache_name)) as pool:
                chunksize = max(1, len(tasks) // (n_workers * 4))
                for stock_results in tqdm(pool.imap_unordered(_precompute_stock_unpack, tasks, chunksize=chunksize),
                                           total=len(tasks), desc="预计算信号"):
                    for r in stock_results:
                        d = r.pop('date_str')
                        if d not in self._precomputed_signals:
                            self._precomputed_signals[d] = []
                        self._precomputed_signals[d].append(r)

            for d in self._precomputed_signals:
                self._precomputed_signals[d].sort(key=lambda x: (-x['b1_score'], x['code']))

            print(f"   预计算完成，共 {sum(len(v) for v in self._precomputed_signals.values())} 条信号")
            print(f"   保存缓存到: {cache_path.name}")
            _save_signal_cache(cache_path, cache_identity, self._precomputed_signals)

        # ★ 回测主循环：只需查字典 + 解耦参数后筛，无需进程池
        print(f"\n   回测主循环...")
        j_threshold = self.strategy.params.get('j_threshold', 30)
        vol_shrink = self.strategy.params.get('volume_shrink_ratio', 0.618)
        for i, date in enumerate(tqdm(self.trading_days, desc="回测进度")):
            self.mark_to_market(date)
            self.check_exits_master(date)

            candidates = self._precomputed_signals.get(date, [])
            filtered = []
            for s in candidates:
                if s['b1_score'] < self.min_similarity:
                    continue
                # J过滤：优先用区间，否则用解耦阈值
                if self.zone_j_ranges is not None:
                    rj = s.get('raw_j', 0)
                    if not any(lo <= rj <= hi for lo, hi in self.zone_j_ranges):
                        continue
                elif decouple_j and s.get('raw_j', 0) >= j_threshold:
                    continue
                # 缩量比过滤：优先用区间，否则用解耦阈值
                if self.zone_vol_ranges is not None:
                    rv = s.get('raw_vol_ratio', 1)
                    if not any(lo <= rv <= hi for lo, hi in self.zone_vol_ranges):
                        continue
                elif decouple_vol and s.get('raw_vol_ratio', 1) >= vol_shrink:
                    continue
                # near_pct过滤（解耦时后筛位置条件）, 支持按多/空头动态切换
                if decouple_near:
                    np_val = self._near_pct_override if self._near_pct_override is not None else self.strategy.params.get('near_pct', 3.5)
                    # 动态 near_pct：根据当日择时状态切换
                    if self._near_pct_bull is not None and self._near_pct_bear is not None and self.market_timing:
                        if self.market_timing.can_open(date):
                            np_val = self._near_pct_bull
                        else:
                            np_val = self._near_pct_bear
                    if not s.get('fall_in_bowl', False):
                        dist_y = s.get('raw_dist_yellow_pct')
                        dist_w = s.get('raw_dist_white_pct')
                        if dist_y is not None and dist_y <= np_val:
                            pass
                        elif dist_w is not None and dist_w <= np_val:
                            pass
                        elif s.get('is_washout', False):
                            pass
                        else:
                            continue
                filtered.append(s)
            # AI多因子重新评分
            if self.use_ai_scorer and filtered:
                self._init_ai_scorer()
                for s in filtered:
                    ind = self._get_realtime_indicators(s['code'], date)
                    if not ind.empty and len(ind) >= 20:
                        result = self.ai_scorer.score(s, ind)
                        s['ai_score'] = result['ai_score']
                        s['ai_breakdown'] = result['breakdown']
                    else:
                        s['ai_score'] = 0
                filtered.sort(key=lambda x: -x.get('ai_score', 0))
            selected = filtered[:self.max_stocks_per_day]

            if selected:
                if self.market_timing and not self.market_timing.can_open(date):
                    selected = [s for s in selected if s['b1_score'] >= 90]
            if selected:
                current_total = self.cash + self._total_market_value(date)
                for stock in selected:
                    success = self.buy_stock(date, stock, current_total)
                    if not success: break

        final_date = self.trading_days[-1]
        for pos in self.positions[:]:
            price = self._get_latest_price(pos['code'], final_date)
            if price > 0:
                self._sell_shares(pos, final_date, price, pos['shares'], '回测结束清仓')
        self.mark_to_market(final_date)
        self.print_summary()

    def print_summary(self):
        df_equity = pd.DataFrame(self.daily_equity)
        if df_equity.empty:
            print("无有效数据")
            return

        start_val = self.initial_cash
        end_val = df_equity.iloc[-1]['total']
        total_return = (end_val / start_val - 1) * 100

        cummax = df_equity['total'].cummax()
        drawdown = (df_equity['total'] - cummax) / cummax * 100
        max_dd = drawdown.min()

        df_trades = pd.DataFrame(self.closed_trades)
        if not df_trades.empty:
            win_trades = df_trades[df_trades['pnl'] > 0]
            win_rate = len(win_trades) / len(df_trades) * 100
            avg_win = win_trades['pnl'].mean() if len(win_trades) > 0 else 0
            avg_loss = df_trades[df_trades['pnl'] <= 0]['pnl'].mean() if len(df_trades[df_trades['pnl'] <= 0]) > 0 else 0
            total_trades = len(df_trades)
        else:
            win_rate = avg_win = avg_loss = total_trades = 0

        print("\n" + "=" * 50)
        print("回测结果汇总")
        print("=" * 50)
        print(f"初始资金: {self.initial_cash:,.0f}")
        print(f"最终资产: {end_val:,.0f}")
        print(f"总收益率: {total_return:.2f}%")
        print(f"最大回撤: {max_dd:.2f}%")
        print(f"交易次数: {total_trades}")
        print(f"胜率: {win_rate:.2f}%")
        print(f"平均盈利: {avg_win:,.0f}")
        print(f"平均亏损: {avg_loss:,.0f}")

        output_dir = Path(getattr(self, 'output_dir', 'artifacts/backtests/b1'))
        output_dir.mkdir(parents=True, exist_ok=True)
        eq_file = output_dir / 'backtest_equity.csv'
        trades_file = output_dir / 'backtest_trades.csv'
        df_equity.to_csv(eq_file, index=False)
        if not df_trades.empty:
            df_trades.to_csv(trades_file, index=False)
        print(f"\n详细数据已保存至 {eq_file} 和 {trades_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='2021-01-01')
    parser.add_argument('--end', default='2026-04-17')
    parser.add_argument('--sample', type=int, default=None)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--max-stocks', type=int, default=10)
    parser.add_argument('--min-similarity', type=float, default=60)
    parser.add_argument('--market-timing', type=str, default=None,
                        help='活跃市值CSV路径，如 data/market/active_cap.csv')
    parser.add_argument('--decouple', type=str, default='',
                        help='解耦参数，逗号分隔，如 j_threshold,volume_shrink_ratio')
    parser.add_argument('--zone-j', type=str, default=None,
                        help='J值允许区间，如 -10~-7,23~33')
    parser.add_argument('--zone-vol', type=str, default=None,
                        help='缩量比允许区间，如 0.1~0.6')
    parser.add_argument('--strategy-config', type=str, default='config/strategy_params.yaml',
                        help='策略参数配置文件路径')
    parser.add_argument('--indicators-cache', type=str, default='indicators_cache',
                        help='Indicator cache folder under data/, default=indicators_cache')
    parser.add_argument('--research-indicators-cache', action='store_true',
                        help='Read data/research_indicators_cache instead of production indicators_cache')
    parser.add_argument('--near-pct', type=float, default=None,
                        help='覆盖 near_pct 值（需配合 --decouple near_pct 使用，无需改YAML即可扫参）')
    parser.add_argument('--near-pct-bull', type=float, default=None,
                        help='多头区间的 near_pct（需配合择时使用）')
    parser.add_argument('--near-pct-bear', type=float, default=None,
                        help='空头区间的 near_pct（需配合择时使用）')
    parser.add_argument('--no-limit-score', action='store_true',
                        help='排除limit_state评分维度（涨停匹配）')
    parser.add_argument('--skip-wave-quality', action='store_true',
                        help='跳过波质量检测(异动量/积累均量>2)')
    parser.add_argument('--skip-wave-break', action='store_true',
                        help='跳过波断检查')
    parser.add_argument('--skip-s1', action='store_true',
                        help='跳过S1出货信号检测')
    parser.add_argument('--skip-washout', action='store_true',
                        help='跳过击穿对手盘通道')
    parser.add_argument('--s1-skip-types', type=str, default='',
                        help='S1跳过类型，逗号分隔，如 放量巨阴,顶部大风车')
    parser.add_argument('--s1-juyin-config', type=str, default='',
                        help='S1放量巨阴子条件: none=禁用, 如 vol_ratio_prev=none,price_pos=none')
    parser.add_argument('--initial-cash', type=float, default=1_000_000,
                        help='初始资金')
    parser.add_argument('--position-pct', type=float, default=0.10,
                        help='单只股票仓位占比')
    parser.add_argument('--output-prefix', type=str, default='',
                        help='输出文件前缀，如 _iter1 则输出 _iter1_equity.csv')
    parser.add_argument('--output-dir', type=str, default='artifacts/backtests/b1',
                        help='Directory for generated equity/trades CSV files')
    args = parser.parse_args()
    prefix = args.output_prefix
    indicators_cache_name = 'research_indicators_cache' if args.research_indicators_cache else args.indicators_cache
    backtester = OptimizedBacktester(data_dir='data', use_cache=False, strategy_config=args.strategy_config, initial_cash=args.initial_cash, indicators_cache_name=indicators_cache_name)
    backtester.output_dir = Path(args.output_dir)
    print(f"Indicator cache: data/{backtester.indicators_cache_name}")
    backtester.max_stocks_per_day = args.max_stocks
    backtester.min_similarity = args.min_similarity
    backtester.position_pct = args.position_pct
    backtester.batch_pct = args.position_pct / 3
    backtester.exclude_limit_state = args.no_limit_score
    backtester.skip_wave_quality = args.skip_wave_quality
    backtester.skip_wave_break = args.skip_wave_break
    backtester.skip_s1 = args.skip_s1
    backtester.skip_washout = args.skip_washout
    # 逐笔回测支持：--s1-juyin-config
    if args.s1_juyin_config:
        cfg = {}
        for pair in args.s1_juyin_config.split(','):
            kv = pair.strip().split('=')
            if len(kv) == 2:
                k, v = kv[0].strip(), kv[1].strip()
                cfg[k] = None if v.lower() == 'none' else float(v)
        if cfg:
            backtester.strategy.params['s1_juyin_config'] = cfg
    if args.s1_skip_types:
        backtester.s1_skip_types = set(t.strip() for t in args.s1_skip_types.split(',') if t.strip())
    else:
        backtester.s1_skip_types = None
    if args.decouple:
        backtester.decouple_params = set(p.strip() for p in args.decouple.split(',') if p.strip())
        print(f"解耦参数: {backtester.decouple_params}")
    if args.near_pct is not None:
        backtester._near_pct_override = args.near_pct
        if 'near_pct' not in backtester.decouple_params:
            backtester.decouple_params.add('near_pct')
            print(f"[自动] near_pct={args.near_pct} → 自动启用解耦")
    if args.near_pct_bull is not None and args.near_pct_bear is not None:
        backtester._near_pct_bull = args.near_pct_bull
        backtester._near_pct_bear = args.near_pct_bear
        if 'near_pct' not in backtester.decouple_params:
            backtester.decouple_params.add('near_pct')
        print(f"[动态] near_pct: 多头={args.near_pct_bull}, 空头={args.near_pct_bear}")

    def _parse_ranges(s):
        if not s: return None
        ranges = []
        for part in s.split(','):
            part = part.strip()
            if '~' in part:
                lo, hi = part.split('~')
                ranges.append((float(lo), float(hi)))
            else:
                v = float(part)
                ranges.append((v, v))
        return ranges

    if args.zone_j:
        backtester.zone_j_ranges = _parse_ranges(args.zone_j)
        print(f"J区间过滤: {backtester.zone_j_ranges}")
    if args.zone_vol:
        backtester.zone_vol_ranges = _parse_ranges(args.zone_vol)
        print(f"缩量比区间过滤: {backtester.zone_vol_ranges}")

    # 活跃市值择时开关
    if args.market_timing:
        from utils.market_timing import MarketTiming
        csv_path = args.market_timing
        if Path(csv_path).exists():
            print(f"加载活跃市值数据: {csv_path}")
            backtester.market_timing = MarketTiming(csv_path)
        else:
            print(f"活跃市值文件不存在: {csv_path}，跳过择时")

    backtester.run(start_date=args.start, end_date=args.end,
                   sample_size=args.sample, n_workers=args.workers)
