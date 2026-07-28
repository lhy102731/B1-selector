"""Fetch native THS indices and ETFs into their isolated data trees."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.market_asset_store import MarketAssetStore
from utils.ths_data_source import THSDataSource


VALID_ASSET_TYPES = ("industry", "concept", "etf")


def _is_no_history_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "not data" in message or "returned no daily bars" in message


def _fetch_catalog(source: THSDataSource, asset_type: str) -> dict[str, dict[str, Any]]:
    if asset_type == "etf":
        return source.fetch_etf_universe()
    return source.fetch_index_catalog(asset_type)


def run(
    data_dir: str | Path = "data",
    *,
    asset_types: Iterable[str] = VALID_ASSET_TYPES,
    start: str = "1990-01-01",
    end: str | None = None,
    max_assets: int | None = None,
    source: THSDataSource | None = None,
    use_cached_catalogs: bool = False,
    refresh: bool = False,
    codes: Iterable[str] | None = None,
) -> int:
    """Synchronize requested asset classes and return nonzero on any failure."""
    requested = tuple(dict.fromkeys(str(value).strip().lower() for value in asset_types))
    invalid = [value for value in requested if value not in VALID_ASSET_TYPES]
    if invalid:
        raise ValueError(f"unsupported asset types: {invalid}")
    if max_assets is not None and max_assets <= 0:
        raise ValueError("max_assets must be positive")
    requested_codes = None
    if codes is not None:
        requested_codes = {str(code).strip().zfill(6) for code in codes}
        if any(len(code) != 6 or not code.isdigit() for code in requested_codes):
            raise ValueError("codes must contain six-digit values")
    end = end or datetime.now().strftime("%Y-%m-%d")
    if pd.Timestamp(start) > pd.Timestamp(end):
        raise ValueError("start date must not be after end date")

    data_dir = Path(data_dir)
    store = MarketAssetStore(data_dir)
    own_source = source is None
    source = source or THSDataSource()
    rows: list[dict[str, Any]] = []
    catalog_counts: dict[str, int] = {}
    catalog_sources: dict[str, str] = {}
    try:
        for asset_type in requested:
            try:
                if use_cached_catalogs:
                    catalog = store.read_catalog(asset_type)
                    if not catalog:
                        raise RuntimeError(f"no saved {asset_type} catalog is available")
                    catalog_sources[asset_type] = "cache"
                else:
                    catalog = _fetch_catalog(source, asset_type)
                    store.write_catalog(asset_type, catalog)
                    catalog_sources[asset_type] = "thsdk"
                catalog_counts[asset_type] = len(catalog)
            except Exception as exc:
                rows.append(
                    {
                        "asset_type": asset_type,
                        "code": None,
                        "status": "catalog_failed",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue

            items = sorted(catalog.items())
            if asset_type == "etf":
                items = [item for item in items if bool(item[1].get("selection_eligible", False))]
            if requested_codes is not None:
                items = [item for item in items if item[0] in requested_codes]
            if max_assets is not None:
                items = items[:max_assets]
            for position, (code, metadata) in enumerate(items, 1):
                try:
                    existing = store.read_history(asset_type, code)
                    fetch_start = pd.Timestamp(start)
                    if not refresh and not existing.empty:
                        local_dates = pd.to_datetime(existing["date"], errors="coerce").dropna()
                        if not local_dates.empty:
                            local_last = local_dates.max()
                            if local_last >= pd.Timestamp(end):
                                rows.append(
                                    {
                                        "asset_type": asset_type,
                                        "code": code,
                                        "status": "unchanged",
                                        "source": (
                                            "yuanhang"
                                            if asset_type in {"industry", "concept"}
                                            else "thsdk"
                                        ),
                                        "rows": len(existing),
                                        "first_date": local_dates.min().strftime("%Y-%m-%d"),
                                        "last_date": local_last.strftime("%Y-%m-%d"),
                                    }
                                )
                                continue
                            fetch_start = max(fetch_start, local_last)
                    history = source.fetch_market_history(
                        metadata["ths_code"],
                        fetch_start.strftime("%Y-%m-%d"),
                        end,
                        asset_type=asset_type,
                    )
                    if history.empty:
                        raise RuntimeError("THS returned no daily bars")
                    history_source = str(history.attrs.get("source", "thsdk"))
                    combined = pd.concat([existing, history], ignore_index=True)
                    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
                    combined = combined.drop_duplicates("date", keep="last")
                    store.write_history(asset_type, code, combined)
                    rows.append(
                        {
                            "asset_type": asset_type,
                            "code": code,
                            "status": "updated",
                            "source": history_source,
                            "rows": len(combined),
                            "first_date": pd.Timestamp(combined["date"].min()).strftime("%Y-%m-%d"),
                            "last_date": pd.Timestamp(combined["date"].max()).strftime("%Y-%m-%d"),
                        }
                    )
                except Exception as exc:
                    rows.append(
                        {
                            "asset_type": asset_type,
                            "code": code,
                            "status": "no_history" if _is_no_history_error(exc) else "failed",
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                if position == 1 or position % 100 == 0:
                    print(
                        f"THS {asset_type}: {position}/{len(items)} "
                        f"status={rows[-1]['status']}",
                        flush=True,
                    )
    finally:
        if own_source:
            source.close()

    report_dir = data_dir / "_market_assets"
    report_dir.mkdir(parents=True, exist_ok=True)
    report = pd.DataFrame(rows)
    report.to_csv(report_dir / "latest_sync.csv", index=False, encoding="utf-8-sig")
    failed = sum(row.get("status") not in {"updated", "unchanged", "no_history"} for row in rows)
    used_yuanhang = any(row.get("source") == "yuanhang" for row in rows)
    summary = {
        "source": "thsdk+yuanhang" if used_yuanhang else "thsdk",
        "status": "completed" if failed == 0 else "partial",
        "started_from": start,
        "requested_end": end,
        "asset_types": list(requested),
        "refresh": bool(refresh),
        "codes": sorted(requested_codes) if requested_codes is not None else None,
        "catalog_counts": catalog_counts,
        "catalog_sources": catalog_sources,
        "updated": sum(row.get("status") == "updated" for row in rows),
        "unchanged": sum(row.get("status") == "unchanged" for row in rows),
        "no_history": sum(row.get("status") == "no_history" for row in rows),
        "failed": failed,
        "completed_at": datetime.now().isoformat(timespec="seconds"),
    }
    MarketAssetStore._atomic_json(summary, report_dir / "latest_sync.json")
    return 0 if failed == 0 else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--asset-types", default=",".join(VALID_ASSET_TYPES))
    parser.add_argument("--start", default="1990-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--max-assets", type=int, default=None)
    parser.add_argument("--cached-catalogs", action="store_true")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--codes", default=None, help="Comma-separated six-digit codes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run(
        data_dir=args.data_dir,
        asset_types=tuple(value.strip() for value in args.asset_types.split(",") if value.strip()),
        start=args.start,
        end=args.end,
        max_assets=args.max_assets,
        use_cached_catalogs=args.cached_catalogs,
        refresh=args.refresh,
        codes=(
            tuple(value.strip() for value in args.codes.split(",") if value.strip())
            if args.codes
            else None
        ),
    )


if __name__ == "__main__":
    raise SystemExit(main())
