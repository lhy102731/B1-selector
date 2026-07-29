"""Precompute B1 indicator cache for all A-share stocks.

CSV remains the source of truth. By default the script first builds or reuses
data/raw_parquet/{prefix}/{code}.parquet, then calculates indicators into
data/indicators_cache/{code}.parquet.
"""
from __future__ import annotations

import argparse
import hashlib
import inspect
import multiprocessing as mp
import re
from functools import partial
from pathlib import Path

from tqdm import tqdm

from research_automation.control_plane.contracts import canonical_json
from research_automation.data_generation.cache_identity import (
    build_cache_identity,
    load_verified_cache_identity,
    write_cache_identity_sidecar,
)
from research_automation.data_generation.generation import GenerationPin
from strategy.unified_b1_strategy import UnifiedB1Strategy
from utils.csv_manager import CSVManager
from utils.market_asset_store import MarketAssetStore
from utils.raw_parquet_cache import (
    RawParquetCache,
    normalize_raw_stock_frame,
    read_stock_csv_bytes,
)
from utils.selection_universe import SelectionUniverse


def _csv_path(data_dir: str | Path, code: str) -> Path:
    return Path(data_dir) / code[:2] / f"{code}.csv"


def _indicator_cache_is_current(cache_file: Path, csv_file: Path) -> bool:
    return cache_file.exists() and csv_file.exists() and cache_file.stat().st_mtime >= csv_file.stat().st_mtime


def _source_sha256(callable_object) -> str:
    try:
        source = inspect.getsource(callable_object)
    except (OSError, TypeError) as error:
        raise ValueError("unable to identify indicator implementation") from error
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def _indicator_feature_contract_id(
    strategy,
    *,
    use_raw_cache: bool,
) -> str:
    payload = canonical_json(
        {
            "contract": "a-share.unified-b1-indicators.v2",
            "effective_strategy_params": strategy.params,
            "calculate_indicators_sha256": _source_sha256(
                strategy.calculate_indicators
            ),
            "raw_normalizer_sha256": _source_sha256(
                normalize_raw_stock_frame
            ),
            "input_mode": "raw_parquet" if use_raw_cache else "direct_csv",
        }
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _pinned_indicator_location(
    generation_pin: GenerationPin,
    data_dir: str | Path,
    cache_dir: str | Path,
    code: str,
) -> tuple[str, str]:
    data_root = Path(data_dir).resolve(strict=True)
    if data_root != generation_pin.data_root:
        raise ValueError("data_dir does not match the generation pin root")
    cache_root = Path(cache_dir)
    if cache_root.is_symlink() or getattr(
        cache_root,
        "is_junction",
        lambda: False,
    )():
        raise ValueError("pinned indicator cache directory is a reparse point")
    resolved_cache_root = cache_root.resolve(strict=False)
    locations = {
        generation_pin.data_root / "indicators_cache": (
            "production",
            "indicators_cache",
        ),
        generation_pin.data_root / "research_indicators_cache": (
            "research",
            "research_indicators_cache",
        ),
    }
    try:
        namespace, relative_root = locations[resolved_cache_root]
    except KeyError as error:
        raise ValueError(
            "pinned indicator cache directory does not match its namespace"
        ) from error
    relative_path = f"{relative_root}/{code}.parquet"
    generation_pin.artifact_path(relative_path)
    return namespace, relative_path


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
    generation_pin: GenerationPin | None = None,
):
    """Read normalized raw bars, preferring data/raw_parquet when enabled."""
    if (
        generation_pin is not None
        and Path(data_dir).resolve(strict=True) != generation_pin.data_root
    ):
        raise ValueError("data_dir does not match the generation pin root")
    if use_raw_cache:
        return RawParquetCache(
            data_dir,
            generation_pin=generation_pin,
        ).read_stock(code, refresh=rebuild_raw)
    if generation_pin is not None:
        relative_path = _csv_path(data_dir, code).relative_to(
            Path(data_dir)
        ).as_posix()
        identity = generation_pin.verify_artifact(
            relative_path,
            content_schema="a-share.gbk_csv.v1",
            kind="source_csv",
            logical_role="raw_stock_bars",
        )
        frame = read_stock_csv_bytes(
            generation_pin.read_verified_bytes(relative_path, identity)
        )
        generation_pin.verify_artifact(
            relative_path,
            content_schema=identity.content_schema,
            producer=identity.producer,
            kind=identity.kind,
            logical_role=identity.logical_role,
        )
        return normalize_raw_stock_frame(frame)
    csv_manager = CSVManager(data_dir)
    return normalize_raw_stock_frame(csv_manager.read_stock(code))


def build_raw_one(
    code: str,
    data_dir: str | Path,
    rebuild_raw: bool = False,
    generation_pin: GenerationPin | None = None,
) -> str:
    """Build only the raw parquet cache for one stock."""
    csv_file = _csv_path(data_dir, code)
    if not csv_file.exists():
        return f"SKIP {code} (CSV missing)"
    cache = RawParquetCache(data_dir, generation_pin=generation_pin)
    try:
        if not rebuild_raw and cache.is_current(code):
            return f"SKIP {code} (raw parquet current)"
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
    generation_pin: GenerationPin | None = None,
) -> str:
    """Build indicator cache for a single stock. Returns a status string."""
    cache_dir = Path(cache_dir)
    cache_file = cache_dir / f"{code}.parquet"
    csv_file = _csv_path(data_dir, code)
    pinned_location: tuple[str, str] | None = None

    if generation_pin is not None:
        try:
            pinned_location = _pinned_indicator_location(
                generation_pin,
                data_dir,
                cache_dir,
                code,
            )
        except Exception as error:
            return f"FAIL {code} ({error})"

    if not csv_file.exists():
        return f"SKIP {code} (CSV missing)"
    if (
        generation_pin is None
        and not rebuild_raw
        and _indicator_cache_is_current(cache_file, csv_file)
    ):
        return f"SKIP {code} (indicator cache current)"

    try:
        source_artifact_id: str | None = None
        cache_namespace: str | None = None
        cache_relative_path: str | None = None
        pinned_raw_cache: RawParquetCache | None = None
        if pinned_location is not None:
            cache_namespace, cache_relative_path = pinned_location
        strategy = UnifiedB1Strategy()
        if strategy_params:
            strategy.params.update(strategy_params)
        feature_contract_id = _indicator_feature_contract_id(
            strategy,
            use_raw_cache=use_raw_cache,
        )
        if generation_pin is not None and use_raw_cache:
            pinned_raw_cache = RawParquetCache(
                data_dir,
                generation_pin=generation_pin,
            )
            df_raw = pinned_raw_cache.read_stock(code, refresh=rebuild_raw)
            source_artifact_id = (
                pinned_raw_cache.verified_identity(code).artifact.artifact_id
            )
        else:
            pinned_csv_source = None
            if generation_pin is not None:
                pinned_csv_source = generation_pin.verify_artifact(
                    _csv_path(data_dir, code).relative_to(
                        Path(data_dir)
                    ).as_posix(),
                    content_schema="a-share.gbk_csv.v1",
                    kind="source_csv",
                    logical_role="raw_stock_bars",
                )
            df_raw = read_raw_stock(
                code,
                data_dir,
                use_raw_cache=use_raw_cache,
                rebuild_raw=rebuild_raw,
                generation_pin=generation_pin,
            )
            if generation_pin is not None and pinned_csv_source is not None:
                source_artifact_id = generation_pin.verify_artifact(
                    _csv_path(data_dir, code).relative_to(
                        Path(data_dir)
                    ).as_posix(),
                    content_schema="a-share.gbk_csv.v1",
                    kind="source_csv",
                    logical_role="raw_stock_bars",
                ).artifact_id
        if (
            generation_pin is not None
            and not rebuild_raw
            and cache_file.exists()
            and source_artifact_id is not None
            and cache_namespace is not None
            and cache_relative_path is not None
        ):
            load_verified_cache_identity(
                generation_pin,
                relative_path=cache_relative_path,
                cache_namespace=cache_namespace,
                cache_kind="indicator",
                source_artifact_ids=(source_artifact_id,),
                feature_contract_id=feature_contract_id,
                content_schema="parquet.unified_b1_indicators.v1",
                producer="build_indicators_cache.build_one",
                logical_role="b1_indicator_frame",
            )
            return f"SKIP {code} (indicator cache current)"
        if df_raw.empty or len(df_raw) < 60:
            return f"SKIP {code} (insufficient data)"

        df_ind = strategy.calculate_indicators(df_raw)
        if df_ind.empty:
            return f"FAIL {code} (indicator calculation empty)"

        if pinned_raw_cache is not None and source_artifact_id is not None:
            current_raw_identity = pinned_raw_cache.verified_identity(code)
            if current_raw_identity.artifact.artifact_id != source_artifact_id:
                raise ValueError("pinned raw cache identity changed")

        cache_dir.mkdir(parents=True, exist_ok=True)
        df_ind = df_ind.sort_values("date").reset_index(drop=True)
        df_ind.to_parquet(cache_file, index=False)
        if pinned_raw_cache is not None and source_artifact_id is not None:
            current_raw_identity = pinned_raw_cache.verified_identity(code)
            if current_raw_identity.artifact.artifact_id != source_artifact_id:
                raise ValueError("pinned raw cache identity changed")
        if (
            generation_pin is not None
            and source_artifact_id is not None
            and cache_namespace is not None
            and cache_relative_path is not None
        ):
            identity = build_cache_identity(
                generation_pin,
                relative_path=cache_relative_path,
                cache_namespace=cache_namespace,
                cache_kind="indicator",
                source_artifact_ids=(source_artifact_id,),
                feature_contract_id=feature_contract_id,
                content_schema="parquet.unified_b1_indicators.v1",
                producer="build_indicators_cache.build_one",
                logical_role="b1_indicator_frame",
            )
            write_cache_identity_sidecar(
                generation_pin,
                relative_path=cache_relative_path,
                identity=identity,
            )
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
