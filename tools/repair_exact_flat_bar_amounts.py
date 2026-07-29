"""Derive missing traded amounts for exact single-price daily bars.

For a daily bar whose open, high, low, and close are identical, every trade
occurred at the same raw price.  Its traded amount is therefore exactly
``close_raw * volume`` at the precision stored in the THS archive.  Bars with
any intraday price range are deliberately left unresolved because OHLC does
not reveal VWAP.

The repair is fail-closed: vendor sentinel values, non-positive inputs,
non-finite values, and incomplete schemas are never used in the derivation.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_missing_amount import valid_positive
from tools.backfill_missing_bars_ths import invalidate_caches
from tools.backfill_valuation_fields import (
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _read_csv,
    _update_manifest,
    stock_files,
)


PRICE_COLUMNS = ("open", "high", "low", "close")
REQUIRED_COLUMNS = {"date", *PRICE_COLUMNS, "close_raw", "volume", "amount"}


def derive_exact_amounts(
    frame: pd.DataFrame,
    *,
    flat_atol: float = 1e-9,
) -> tuple[pd.DataFrame, Counter, pd.DataFrame]:
    """Fill only mathematically determined single-price bar amounts."""
    result = frame.copy()
    stats: Counter = Counter()
    detail_columns = (
        "date",
        "close_raw",
        "volume",
        "derived_amount",
        "adjusted_price_spread",
    )
    if result.empty or not REQUIRED_COLUMNS.issubset(result.columns):
        stats["invalid_schema"] += 1
        return result, stats, pd.DataFrame(columns=detail_columns)

    dates = _date_keys(result["date"])
    prices = result[list(PRICE_COLUMNS)].apply(pd.to_numeric, errors="coerce")
    close_raw = pd.to_numeric(result["close_raw"], errors="coerce")
    volume = pd.to_numeric(result["volume"], errors="coerce")
    amount = pd.to_numeric(result["amount"], errors="coerce")

    missing = dates.notna() & volume.gt(0) & ~valid_positive(amount)
    stats["missing_amount_positive_volume"] = int(missing.sum())
    if not missing.any():
        return result, stats, pd.DataFrame(columns=detail_columns)

    valid_inputs = valid_positive(volume) & valid_positive(close_raw)
    for column in PRICE_COLUMNS:
        valid_inputs &= valid_positive(prices[column])

    spread = prices.max(axis=1) - prices.min(axis=1)
    flat = spread.le(float(flat_atol))
    fill = missing & valid_inputs & flat
    # Remove binary floating-point noise (for example 9.45 * 11,300 being
    # represented as 106,784.99999999999) without changing source precision.
    derived = (close_raw * volume).round(6)
    fill &= valid_positive(derived)

    stats["derived_exact_flat_bar"] = int(fill.sum())
    stats["rejected_invalid_input"] = int((missing & ~valid_inputs).sum())
    stats["unresolved_nonflat_bar"] = int((missing & valid_inputs & ~flat).sum())
    stats["unresolved_total"] = int(missing.sum() - fill.sum())

    if fill.any():
        result.loc[fill, "amount"] = derived.loc[fill].astype(float)
    details = pd.DataFrame(
        {
            "date": dates.loc[fill],
            "close_raw": close_raw.loc[fill],
            "volume": volume.loc[fill],
            "derived_amount": derived.loc[fill],
            "adjusted_price_spread": spread.loc[fill],
        }
    ).reset_index(drop=True)
    return result, stats, details


def run(args: argparse.Namespace) -> int:
    data_dir = Path(args.data_dir).resolve()
    backup_dir = (ROOT / args.backup_dir).resolve()
    report_path = (ROOT / args.report).resolve()
    details_path = (ROOT / args.details).resolve()
    files = stock_files(data_dir)
    total: Counter = Counter()
    detail_frames: list[pd.DataFrame] = []
    changed_files = 0
    failed: list[dict[str, str]] = []
    started = time.time()

    for index, path in enumerate(files, 1):
        code = path.stem
        try:
            current = _read_csv(path)
            repaired, stats, details = derive_exact_amounts(
                current,
                flat_atol=args.flat_atol,
            )
            total.update(stats)
            if not details.empty:
                changed_files += 1
                details.insert(0, "code", code)
                detail_frames.append(details)
                if args.apply:
                    backup_path = backup_dir / path.relative_to(data_dir)
                    backup_path.parent.mkdir(parents=True, exist_ok=True)
                    if not backup_path.exists():
                        shutil.copy2(path, backup_path)
                    _atomic_csv(repaired, path)
                    invalidate_caches(data_dir, code)
        except Exception as exc:
            failed.append(
                {"code": code, "type": type(exc).__name__, "error": str(exc)[:500]}
            )
        if index == 1 or index % 500 == 0 or (failed and failed[-1]["code"] == code):
            print(
                f"flat-amount {index}/{len(files)} {code} "
                f"derivable={total['derived_exact_flat_bar']} failed={len(failed)} "
                f"elapsed={time.time() - started:.1f}s",
                flush=True,
            )

    all_details = (
        pd.concat(detail_frames, ignore_index=True)
        if detail_frames
        else pd.DataFrame(
            columns=(
                "code",
                "date",
                "close_raw",
                "volume",
                "derived_amount",
                "adjusted_price_spread",
            )
        )
    )
    details_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_csv(all_details, details_path)
    report: dict[str, Any] = {
        "status": "COMPLETED" if not failed else "FAILED",
        "applied": bool(args.apply),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_dir": str(data_dir),
        "files": len(files),
        "changed_files": changed_files,
        "counts": dict(total),
        "failed": failed,
        "policy": {
            "formula": "amount = close_raw * volume",
            "eligibility": "missing amount, valid non-sentinel inputs, adjusted OHLC flat",
            "flat_absolute_tolerance": args.flat_atol,
            "nonflat_bars": "left unresolved because daily OHLC does not determine VWAP",
        },
        "details": str(details_path),
        "backup_dir": str(backup_dir),
        "elapsed_seconds": round(time.time() - started, 3),
    }
    _atomic_json(report, report_path)
    if args.apply and not failed:
        _update_manifest(
            data_dir,
            "historical_amount_exact_flat_bar",
            {
                "source": "derived from THS raw close and traded share volume",
                "formula": "close_raw * volume",
                "flat_absolute_tolerance": args.flat_atol,
                "filled": int(total["derived_exact_flat_bar"]),
                "unresolved": int(total["unresolved_total"]),
                "report": str(report_path),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"details={details_path}", flush=True)
    print(f"counts={dict(total)} failed={len(failed)}", flush=True)
    return 0 if not failed else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--flat-atol", type=float, default=1e-9)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--backup-dir",
        default="artifacts/maintenance/all_data_gaps/flat_bar_amount_backup",
    )
    parser.add_argument(
        "--details",
        default="artifacts/maintenance/all_data_gaps/flat_bar_amount_derivations.csv",
    )
    parser.add_argument(
        "--report",
        default="artifacts/maintenance/all_data_gaps/flat_bar_amount_report.json",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
