import pandas as pd
from pathlib import Path
from tqdm import tqdm
from utils.csv_manager import CSVManager
from strategy.unified_b1_strategy import UnifiedB1Strategy
import multiprocessing as mp
from functools import partial

def build_one(code, data_dir, cache_dir):
    cache_file = cache_dir / f"{code}.parquet"
    csv_file = Path(data_dir) / code[:2] / f"{code}.csv"
    if cache_file.exists() and csv_file.exists():
        if cache_file.stat().st_mtime >= csv_file.stat().st_mtime:
            return f"SKIP {code}"
    try:
        csv_manager = CSVManager(data_dir)
        df_raw = csv_manager.read_stock(code)
        if df_raw.empty or len(df_raw) < 60:
            return f"SKIP {code} (数据不足)"
        df_raw['date'] = pd.to_datetime(df_raw['date'])
        df_raw = df_raw[df_raw['volume'] > 0]   # 去停牌
        if len(df_raw) < 60:
            return f"SKIP {code} (有效数据不足)"

        strategy = UnifiedB1Strategy()
        df_ind = strategy.calculate_indicators(df_raw)
        if df_ind.empty:
            return f"FAIL {code} (指标计算失败)"

        # 升序排列，方便回测时按日期筛选
        df_ind = df_ind.sort_values('date').reset_index(drop=True)
        df_ind.to_parquet(cache_file, index=False)
        return f"OK   {code}"
    except Exception as e:
        return f"FAIL {code} ({e})"

def main():
    data_dir = "data"
    cache_dir = Path(data_dir) / "indicators_cache"
    cache_dir.mkdir(exist_ok=True)

    csv_manager = CSVManager(data_dir)
    codes = csv_manager.list_all_stocks()
    print(f"共 {len(codes)} 只股票，开始生成全量指标缓存...")

    with mp.Pool(16) as pool:
        func = partial(build_one, data_dir=data_dir, cache_dir=cache_dir)
        results = list(tqdm(pool.imap(func, codes), total=len(codes), desc="生成指标缓存"))

    ok = sum(1 for r in results if r.startswith("OK"))
    fail = sum(1 for r in results if "FAIL" in r)
    skip = sum(1 for r in results if "SKIP" in r)
    print(f"\n完成：成功 {ok}，跳过 {skip}，失败 {fail}")

if __name__ == "__main__":
    main()