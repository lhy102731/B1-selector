"""Backfill safe historical ``amount`` gaps from the audited legacy source.

THS raw history contains zero or vendor-sentinel amount values on a set of
mostly 2000-2001 trading dates.  This tool fills only exact code/date matches
whose legacy amount is compatible with both the THS volume and THS raw OHLC
envelope.  It never inserts dates or changes any other column.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_valuation_fields import (
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _read_csv,
    _update_manifest,
    code_path,
    stock_files,
)


COMMON_SENTINELS = frozenset(
    {2_147_483_647.0, 2_147_483_648.0, 4_294_967_295.0, 999_999_999.0}
)


def valid_positive(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return (
        numeric.notna()
        & numeric.map(lambda value: math.isfinite(float(value)))
        & numeric.gt(0)
        & ~numeric.isin(COMMON_SENTINELS)
    )


def merge_amount(
    current: pd.DataFrame,
    legacy: pd.DataFrame,
    *,
    max_volume_relative_diff: float,
    price_tolerance: float,
) -> tuple[pd.DataFrame, Counter]:
    result = current.copy()
    stats: Counter = Counter()
    required = {"date", "low", "high", "close", "close_raw", "volume", "amount"}
    if current.empty or not required.issubset(current.columns):
        stats["invalid_current_schema"] += 1
        return result, stats
    if legacy.empty or not {"date", "volume", "amount"}.issubset(legacy.columns):
        stats["missing_legacy_source"] += 1
        return result, stats

    current_dates = _date_keys(current["date"])
    old = legacy.assign(_date=_date_keys(legacy["date"]))
    old = old.dropna(subset=["_date"]).drop_duplicates("_date", keep="last").set_index("_date")

    current_amount = pd.to_numeric(current["amount"], errors="coerce")
    current_volume = pd.to_numeric(current["volume"], errors="coerce")
    missing = ~valid_positive(current_amount) & current_volume.gt(0)
    stats["current_missing_positive_volume"] = int(missing.sum())
    if not missing.any():
        return result, stats

    mapped_amount = pd.to_numeric(current_dates.map(old["amount"]), errors="coerce")
    mapped_volume = pd.to_numeric(current_dates.map(old["volume"]), errors="coerce")
    legacy_valid = valid_positive(mapped_amount) & valid_positive(mapped_volume)
    stats["legacy_exact_date_valid"] = int((missing & legacy_valid).sum())

    volume_relative_diff = (mapped_volume - current_volume).abs() / current_volume
    volume_compatible = volume_relative_diff.le(max_volume_relative_diff)
    stats["rejected_volume_mismatch"] = int(
        (missing & legacy_valid & ~volume_compatible).sum()
    )

    adjusted_close = pd.to_numeric(current["close"], errors="coerce")
    raw_close = pd.to_numeric(current["close_raw"], errors="coerce")
    adjusted_low = pd.to_numeric(current["low"], errors="coerce")
    adjusted_high = pd.to_numeric(current["high"], errors="coerce")
    factor = adjusted_close / raw_close
    raw_low = adjusted_low / factor
    raw_high = adjusted_high / factor
    vwap = mapped_amount / current_volume
    price_compatible = (
        factor.gt(0)
        & raw_low.gt(0)
        & raw_high.gt(0)
        & vwap.ge(raw_low * (1.0 - price_tolerance))
        & vwap.le(raw_high * (1.0 + price_tolerance))
    )
    stats["rejected_vwap_outside_raw_ohlc"] = int(
        (missing & legacy_valid & volume_compatible & ~price_compatible).sum()
    )

    fill = missing & legacy_valid & volume_compatible & price_compatible
    stats["filled_amount"] = int(fill.sum())
    stats["unresolved_amount"] = int(missing.sum() - fill.sum())
    if fill.any():
        result.loc[fill, "amount"] = mapped_amount.loc[fill].astype(float)
    return result, stats


def run(args: argparse.Namespace) -> int:
    data_dir = Path(args.data_dir).resolve()
    legacy_dir = Path(args.legacy_dir).resolve()
    files = stock_files(data_dir)
    total: Counter = Counter()
    changed_files = 0
    failed: list[dict[str, str]] = []
    started = time.time()
    for index, path in enumerate(files, 1):
        code = path.stem
        try:
            current = _read_csv(path)
            legacy = _read_csv(code_path(legacy_dir, code))
            merged, stats = merge_amount(
                current,
                legacy,
                max_volume_relative_diff=args.max_volume_relative_diff,
                price_tolerance=args.price_tolerance,
            )
            total.update(stats)
            if stats["filled_amount"]:
                changed_files += 1
                if args.apply:
                    _atomic_csv(merged, path)
        except Exception as exc:
            failed.append(
                {"code": code, "type": type(exc).__name__, "error": str(exc)[:500]}
            )
        if index == 1 or index % 200 == 0 or failed and failed[-1].get("code") == code:
            print(
                f"amount {index}/{len(files)} {code} fillable={total['filled_amount']} "
                f"failed={len(failed)} elapsed={time.time() - started:.1f}s",
                flush=True,
            )

    report = {
        "status": "COMPLETED" if not failed else "FAILED",
        "applied": bool(args.apply),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_dir": str(data_dir),
        "legacy_dir": str(legacy_dir),
        "files": len(files),
        "changed_files": changed_files,
        "max_volume_relative_diff": args.max_volume_relative_diff,
        "price_tolerance": args.price_tolerance,
        "counts": dict(total),
        "failed": failed,
        "elapsed_seconds": round(time.time() - started, 3),
        "source_semantics": "BaoStock-compatible traded amount in CNY",
    }
    report_path = (ROOT / args.report).resolve()
    _atomic_json(report, report_path)
    if args.apply and not failed:
        _update_manifest(
            data_dir,
            "historical_amount_legacy_baostock",
            {
                "source": str(legacy_dir),
                "match": "exact code+date",
                "max_volume_relative_diff": args.max_volume_relative_diff,
                "raw_ohlc_price_tolerance": args.price_tolerance,
                "filled": int(total["filled_amount"]),
                "unresolved": int(total["unresolved_amount"]),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"counts={dict(total)} failed={len(failed)}", flush=True)
    return 0 if not failed else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument(
        "--legacy-dir", default="data_pre_ths_backup_20260727_110350"
    )
    parser.add_argument("--max-volume-relative-diff", type=float, default=0.01)
    parser.add_argument("--price-tolerance", type=float, default=0.001)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--report",
        default="artifacts/maintenance/all_data_gaps/amount_backfill_report.json",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
