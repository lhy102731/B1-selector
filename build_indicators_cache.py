"""Precompute B1 indicator cache for all A-share stocks.

CSV remains the source of truth. By default the script first builds or reuses
data/raw_parquet/{prefix}/{code}.parquet, then calculates indicators into
data/indicators_cache/{code}.parquet.
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import re
from functools import partial
from pathlib import Path

from tqdm import tqdm

from strategy.unified_b1_strategy import UnifiedB1Strategy
from utils.csv_manager import CSVManager
from utils.market_asset_store import MarketAssetStore
from utils.raw_parquet_cache import RawParquetCache, normalize_raw_stock_frame
from utils.selection_universe import SelectionUniverse


def _csv_path(data_dir: str | Path, code: str) -> Path:
    return Path(data_dir) / code[:2] / f"{code}.csv"


def _indicator_cache_is_current(cache_file: Path, csv_file: Path) -> bool:
    return cache_file.exists() and csv_file.exists() and cache_file.stat().st_mtime >= csv_file.stat().st_mtime


def safe_cache_name(cache_name: str) -> str:
    """Restrict cache folder names to avoid accidental path escape."""
    value = str(cache_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("cache name may contain only letters, numbers, '_' and '-'")
    return value


def read_raw_stock(
    code: str,
    data_dir: str | Path,
    use_raw_cache: bool = True,
    rebuild_raw: bool = False,
):
    """Read normalized raw bars, preferring data/raw_parquet when enabled."""
    if use_raw_cache:
        return RawParquetCache(data_dir).read_stock(code, refresh=rebuild_raw)
    csv_manager = CSVManager(data_dir)
    return normalize_raw_stock_frame(csv_manager.read_stock(code))


def build_raw_one(code: str, data_dir: str | Path, rebuild_raw: bool = False) -> str:
    """Build only the raw parquet cache for one stock."""
    csv_file = _csv_path(data_dir, code)
    if not csv_file.exists():
        return f"SKIP {code} (CSV missing)"
    cache = RawParquetCache(data_dir)
    if not rebuild_raw and cache.is_current(code):
        return f"SKIP {code} (raw parquet current)"
    try:
        df_raw = cache.read_stock(code, refresh=rebuild_raw)
        if df_raw.empty:
            return f"SKIP {code} (raw data empty)"
        return f"OK   {code}"
    except Exception as error:
        return f"FAIL {code} ({error})"


def build_one(
    code: str,
    data_dir: str | Path,
    cache_dir: str | Path,
    strategy_params: dict | None = None,
    use_raw_cache: bool = True,
    rebuild_raw: bool = False,
) -> str:
    """Build indicator cache for a single stock. Returns a status string."""
    cache_dir = Path(cache_dir)
    cache_file = cache_dir / f"{code}.parquet"
    csv_file = _csv_path(data_dir, code)

    if not csv_file.exists():
        return f"SKIP {code} (CSV missing)"
    if not rebuild_raw and _indicator_cache_is_current(cache_file, csv_file):
        return f"SKIP {code} (indicator cache current)"

    try:
        df_raw = read_raw_stock(
            code,
            data_dir,
            use_raw_cache=use_raw_cache,
            rebuild_raw=rebuild_raw,
        )
        if df_raw.empty or len(df_raw) < 60:
            return f"SKIP {code} (insufficient data)"

        strategy = UnifiedB1Strategy()
        if strategy_params:
            strategy.params.update(strategy_params)
        df_ind = strategy.calculate_indicators(df_raw)
        if df_ind.empty:
            return f"FAIL {code} (indicator calculation empty)"

        cache_dir.mkdir(parents=True, exist_ok=True)
        df_ind = df_ind.sort_values("date").reset_index(drop=True)
        df_ind.to_parquet(cache_file, index=False)
        return f"OK   {code}"
    except Exception as error:
        return f"FAIL {code} ({error})"


def build_etf_one(
    code: str,
    data_dir: str | Path = "data",
    cache_dir: str | Path | None = None,
    strategy_params: dict | None = None,
) -> str:
    """Build one typed ETF indicator cache outside the stock cache namespace."""
    data_dir = Path(data_dir)
    store = MarketAssetStore(data_dir)
    csv_file = store.history_path("etf", code)
    cache_dir = Path(cache_dir) if cache_dir is not None else data_dir / "indicators_cache" / "etf"
    cache_file = cache_dir / f"{code}.parquet"
    if not csv_file.exists():
        return f"SKIP {code} (ETF CSV missing)"
    if _indicator_cache_is_current(cache_file, csv_file):
        return f"SKIP {code} (ETF indicator cache current)"
    try:
        df_raw = normalize_raw_stock_frame(store.read_history("etf", code))
        if df_raw.empty or len(df_raw) < 60:
            return f"SKIP {code} (insufficient ETF data)"
        strategy = UnifiedB1Strategy()
        if strategy_params:
            strategy.params.update(strategy_params)
        df_ind = strategy.calculate_indicators(df_raw)
        if df_ind.empty:
            return f"FAIL {code} (ETF indicator calculation empty)"
        df_ind["asset_type"] = "etf"
        df_ind["instrument_id"] = f"etf:{code}"
        cache_dir.mkdir(parents=True, exist_ok=True)
        df_ind.sort_values("date").reset_index(drop=True).to_parquet(cache_file, index=False)
        return f"OK   {code}"
    except Exception as error:
        return f"FAIL {code} ({error})"


def _run_pool(codes: list[str], func, workers: int, chunksize: int, desc: str) -> list[str]:
    with mp.Pool(workers) as pool:
        return list(tqdm(
            pool.imap_unordered(func, codes, chunksize=chunksize),
            total=len(codes),
            desc=desc,
        ))


def _print_summary(results: list[str]) -> tuple[int, int, int]:
    ok = sum(1 for result in results if result.startswith("OK"))
    fail = sum(1 for result in results if "FAIL" in result)
    skip = sum(1 for result in results if "SKIP" in result)
    print(f"\nDone: ok={ok}, skipped={skip}, failed={fail}")
    return ok, skip, fail


def result_exit_code(results: list[str]) -> int:
    """Fail the process when any worker reports an indicator/cache error."""
    return 2 if any(result.startswith("FAIL") for result in results) else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build raw parquet and indicator parquet caches.")
    parser.add_argument("--data-dir", default="data", help="Root data directory")
    parser.add_argument("--cache-name", default="indicators_cache", help="Indicator cache folder under data-dir")
    parser.add_argument("--research-cache", action="store_true", help="Write indicators to data/research_indicators_cache")
    parser.add_argument("--workers", type=int, default=None, help="Worker processes")
    parser.add_argument("--chunksize", type=int, default=20, help="Pool chunksize")
    parser.add_argument("--raw-only", action="store_true", help="Only build data/raw_parquet cache")
    parser.add_argument("--no-raw-cache", action="store_true", help="Read CSV directly, bypassing raw parquet")
    parser.add_argument("--rebuild-raw", action="store_true", help="Rebuild raw parquet even if current")
    parser.add_argument("--stocks-only", action="store_true", help="Do not build ETF indicator caches")
    parser.add_argument("--etf-only", action="store_true", help="Only build isolated ETF indicator caches")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.stocks_only and args.etf_only:
        raise ValueError("--stocks-only and --etf-only are mutually exclusive")
    if args.raw_only and args.etf_only:
        raise ValueError("--raw-only does not support --etf-only")
    data_dir = args.data_dir
    csv_manager = CSVManager(data_dir)
    codes = [] if args.etf_only else csv_manager.list_all_stocks()
    etf_codes = []
    if not args.stocks_only and not args.raw_only:
        etf_codes = [
            asset.code
            for asset in SelectionUniverse(data_dir).list_assets(include_etfs=True)
            if asset.asset_type == "etf"
        ]
    workers = args.workers or min(mp.cpu_count(), 16)
    workers = max(1, workers)
    cache_name = safe_cache_name("research_indicators_cache" if args.research_cache else args.cache_name)

    if args.raw_only:
        print(f"Building raw parquet cache for {len(codes)} stocks...")
        func = partial(build_raw_one, data_dir=data_dir, rebuild_raw=args.rebuild_raw)
        results = _run_pool(codes, func, workers, args.chunksize, "raw parquet")
        _print_summary(results)
        return result_exit_code(results)

    cache_dir = Path(data_dir) / cache_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    use_raw_cache = not args.no_raw_cache
    source = "raw parquet" if use_raw_cache else "CSV"
    results: list[str] = []
    if codes:
        print(f"Building indicator cache for {len(codes)} stocks from {source} into {cache_dir}...")
        func = partial(
            build_one,
            data_dir=data_dir,
            cache_dir=cache_dir,
            use_raw_cache=use_raw_cache,
            rebuild_raw=args.rebuild_raw,
        )
        results = _run_pool(codes, func, workers, args.chunksize, "indicators")
        _print_summary(results)
    if etf_codes:
        etf_cache_dir = cache_dir / "etf"
        print(f"Building indicator cache for {len(etf_codes)} ETFs into {etf_cache_dir}...")
        etf_func = partial(
            build_etf_one,
            data_dir=data_dir,
            cache_dir=etf_cache_dir,
        )
        etf_results = _run_pool(etf_codes, etf_func, workers, args.chunksize, "ETF indicators")
        _print_summary(etf_results)
        results.extend(etf_results)
    return result_exit_code(results)


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(main())
