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
_worker_parquet_cache = {}       # ★ 内存缓存：code -> full DataFrame，消除磁盘 I/O

def _process_initializer(strategy_params_path, data_dir, stock_names):
    """子进程初始化：加载策略、评分器、股票名称（静默）"""
    global _worker_strategy, _worker_scorer, _worker_stock_names, _worker_data_dir
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
        _worker_strategy = strategy
        _worker_scorer = StockScorer(CSVManager(data_dir), registry)
        _worker_stock_names = stock_names
        _worker_data_dir = data_dir
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
            cache_path = Path(_worker_data_dir) / "indicators_cache" / f"{code}.parquet"
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
        cutoff = np.searchsorted(dates, np.datetime64(end_date), side='right')
        if cutoff == 0:
            return None
        latest_idx = cutoff - 1

        # 快速预筛选：直接从原始 DataFrame 读最新行的 6 列（O(1)，无需创建过滤 DataFrame）
        row = df_cache.iloc[latest_idx]
        if not row['white_gt_yellow']:
            return None
        if row['J'] >= 30:
            return None
        if not row['volume_shrink']:
            return None
        if row['DIF'] <= 0:
            return None
        if row['doubled']:
            return None
        if row.get('market_cap', 999e9) < 40e8:
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

        score_info = _worker_scorer.score_stock(code, df_filtered)
        b1_score = score_info.get('b1_score', 0)
        if b1_score < min_similarity:
            return None

        bonus = _calc_bonus_static(df_filtered)
        b1_score += bonus
        b1_score = min(b1_score, 100)

        signal = signals[0]
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
    dif = asc['DIF'].values
    dea = asc['DEA'].values
    if n >= 2 and dif[-1] > 0 and dif[-2] > dea[-2] and dif[-1] >= dif[-2]:
        bonus += 5
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
        if np.polyfit(np.arange(10), asc['DIF'].iloc[-10:], 1)[0] > 0:
            bonus += 3
    return bonus

# ======================== 主回测类 ========================
class OptimizedBacktester:
    def __init__(self, data_dir='data', use_cache=False, initial_cash=1_000_000):
        self.data_dir = Path(data_dir)
        self.cache_dir = self.data_dir / "cache"
        self.use_cache = use_cache
        self.csv_manager = CSVManager(str(data_dir))
        self.initial_cash = initial_cash
        self.cash = initial_cash

        self.registry = get_registry("config/strategy_params.yaml")
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

        self.positions = []
        self.closed_trades = []
        self.daily_equity = []
        self.use_indicators_cache = True
        self._indicators_cache = {}     # ★ 持久缓存 code -> full DataFrame
        self._daily_indicators = {}     # 日内缓存 (code, end_date) -> filtered DataFrame
        self.trading_days = []
        self.market_timing = None       # 活跃市值择时开关，默认关闭

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
            cache_path = Path(self.data_dir) / "indicators_cache" / f"{code}.parquet"
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

    def _get_next_trading_day(self, date):
        if date not in self.trading_days:
            return date
        idx = self.trading_days.index(date)
        if idx + 1 < len(self.trading_days):
            return self.trading_days[idx + 1]
        return date

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
        """第一批建仓"""
        code = stock_info['code']
        for pos in self.positions:
            if pos['code'] == code:
                return False
        next_date = self._get_next_trading_day(signal_date)
        df = self._get_realtime_indicators(code, next_date)
        if df.empty:
            return False
        buy_price = df.iloc[0]['open']
        target_money = current_total_asset * self.position_pct
        first_money = target_money / 3
        shares = int(first_money / buy_price / 100) * 100
        if shares == 0 or first_money > self.cash:
            return False
        cost = shares * buy_price * (1 + self.commission)
        if cost > self.cash:
            return False
        stop_ref = stock_info['signal_day_low'] - 0.05
        self.cash -= cost
        pos = {
            'code': code, 'shares': shares, 'initial_shares': shares,
            'batch_count': 1, 'batch_stops': [stop_ref], 'batch_prices': [buy_price],
            'buy_date': signal_date, 'actual_buy_date': next_date,
            'buy_price': buy_price, 'cost': cost,
            'b1_score': stock_info['b1_score'],
            'is_washout': stock_info['is_washout'],
            'is_super_b1': stock_info.get('is_super_b1', False),
            'stop_loss_ref': stop_ref, 'stop_loss_ref_active': stop_ref,
            'launched': False, 'washout_start_date': None, 'washout_low': None,
            'partial_sold': 0, 's1_sold': 0, 'didi_sold': 0, 'white_sold': 0,
            'has_been_profitable': False,
            'batch_entry_log': [f"第一批@{buy_price}"]
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
            df = self._get_realtime_indicators(code, date)
            if df.empty or len(df) < 5: continue
            latest = df.iloc[0]

            if pos['batch_count'] == 1:
                cond1 = (abs(latest['close'] - latest['yellow_line']) / latest['yellow_line'] < 0.02 and
                         latest['J'] < 13 and self._is_volume_shrink(df))
                cond2 = self._is_super_b1_on_position(pos, df)
                if cond1 or cond2:
                    next_date = self._get_next_trading_day(date)
                    next_df = self._get_realtime_indicators(code, next_date)
                    if not next_df.empty:
                        buy_price = next_df.iloc[0]['open']
                        self._add_batch(pos, date, buy_price, '第二批加仓', signal_low=latest['low'])

            elif pos['batch_count'] == 2:
                if self.strategy.detect_b2_signal(df):
                    next_date = self._get_next_trading_day(date)
                    next_df = self._get_realtime_indicators(code, next_date)
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
            'pnl_pct': (pnl / cost_part) * 100 if cost_part > 0 else 0, 'reason': reason
        })
        pos['shares'] -= shares
        pos['cost'] -= cost_part

    def _detect_s1_on_position(self, df):
        has_s1, _, _ = detect_s1_signal(df, lookback_days=35)
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
            df = self._get_realtime_indicators(pos['code'], date)
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

            # 0. 滴滴战法核心止损：买入次日收盘 < 信号日 min(open,close) → 立即清仓
            if hold_days == 1:
                sig_df = self._get_realtime_indicators(pos['code'], pos['buy_date'])
                if not sig_df.empty:
                    sig_row = sig_df.iloc[0]
                    sig_ref = min(sig_row['open'], sig_row['close'])
                    if sell_price < sig_ref:
                        self._sell_shares(pos, date, sell_price, pos['shares'], '滴滴战法止损')
                        to_remove.append(pos); continue

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
                        # 更新洗盘最低点
                        if latest['low'] < pos['washout_low']:
                            pos['washout_low'] = latest['low']
                            pos['stop_loss_ref_active'] = pos['washout_low'] - 0.05
                        if sell_price < pos['washout_low'] - 0.05:
                            self._sell_shares(pos, date, sell_price, pos['shares'], '击穿对手盘失败清仓')
                            to_remove.append(pos); continue
            else:
                if pos.get('washout_start_date') is not None:
                    pos['washout_start_date'] = None
                    pos['washout_low'] = None
                    pos['stop_loss_ref_active'] = min(pos['batch_stops'])

            # 2. 基础止损
            active_stop = pos.get('stop_loss_ref_active',
                                  min(pos['batch_stops']) if pos.get('batch_stops') else pos['batch_prices'][0]-0.05)
            if sell_price < active_stop:
                self._sell_shares(pos, date, sell_price, pos['shares'], '基础止损')
                to_remove.append(pos); continue

            # 3. 持有4天盈利不足4%
            if hold_days >= 4 and profit_pct < 0.04:
                self._sell_shares(pos, date, sell_price, pos['shares'], '持有4天盈利不足4%')
                to_remove.append(pos); continue

            # 4. 盈转亏
            if pos.get('has_been_profitable', False) and sell_price < avg_cost:
                self._sell_shares(pos, date, sell_price, pos['shares'], '盈转亏清仓')
                to_remove.append(pos); continue

            # 5. S1减仓≥50%
            if self._detect_s1_on_position(df) and pos.get('s1_sold', 0) == 0:
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
        print(f"   启动工作进程池 ({n_workers} workers)...")

        # ★ 复用进程池：整个回测期间只创建一次，避免每天 spawn 开销
        with mp.Pool(processes=n_workers,
                     initializer=_process_initializer,
                     initargs=("config/strategy_params.yaml", str(self.data_dir), self.stock_names)) as pool:
            for i, date in enumerate(tqdm(self.trading_days, desc="回测进度")):
                self.mark_to_market(date)
                self.check_batch_entry(date)
                self.check_exits_master(date)
                selected = self.run_selection_on_date(date, pool, sample_size=sample_size)
                if selected:
                    # 活跃市值择时：空头区间不开新仓，但综合评分 > 80 的强信号除外
                    if self.market_timing and not self.market_timing.can_open(date):
                        selected = [s for s in selected if s['b1_score'] > 80]
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

        df_equity.to_csv('backtest_equity.csv', index=False)
        if not df_trades.empty:
            df_trades.to_csv('backtest_trades.csv', index=False)
        print("\n详细数据已保存至 backtest_equity.csv 和 backtest_trades.csv")


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
    args = parser.parse_args()
    backtester = OptimizedBacktester(data_dir='data', use_cache=False)
    backtester.max_stocks_per_day = args.max_stocks
    backtester.min_similarity = args.min_similarity

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
