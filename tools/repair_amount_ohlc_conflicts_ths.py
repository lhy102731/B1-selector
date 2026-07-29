"""Repair amount gaps caused by a conflicting THS K-line OHLC row.

The ordinary amount repair intentionally rejects a trade pair whose VWAP is
outside the committed raw-price envelope.  For a small subset of those rows,
THS Wencai returns a complete exact-date raw bar whose OHLC, volume, amount,
turnover, and market cap are internally consistent.  This tool replaces an
existing row only when that full bar passes the economic checks and its
backward-adjustment factor has an independent same-day or two-sided anchor.
"""

from __future__ import annotations

import argparse
import math
import shutil
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_missing_amount import valid_positive
from tools.backfill_missing_bars_ths import (
    invalidate_caches,
    read_results,
    stable_adjustment_factor,
    validate_raw_bar,
)
from tools.backfill_valuation_fields import (
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _read_csv,
    _update_manifest,
    code_path,
)


def _number(value: Any) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed) or not math.isfinite(float(parsed)):
        return None
    return float(parsed)


def _same_day_factor(
    row: pd.Series,
    bar: dict[str, float],
    *,
    raw_close_rtol: float,
    raw_close_atol: float,
) -> float | None:
    adjusted_close = _number(row.get("close"))
    committed_raw_close = _number(row.get("close_raw"))
    source_raw_close = _number(bar.get("close_raw"))
    if (
        adjusted_close is None
        or committed_raw_close is None
        or source_raw_close is None
        or adjusted_close <= 0
        or committed_raw_close <= 0
        or source_raw_close <= 0
        or not np.isclose(
            committed_raw_close,
            source_raw_close,
            rtol=raw_close_rtol,
            atol=raw_close_atol,
        )
    ):
        return None
    return adjusted_close / committed_raw_close


def _recompute_change_fields(result: pd.DataFrame, changed_days: set[str]) -> pd.DataFrame:
    if not changed_days:
        return result
    output = result.copy()
    dates = _date_keys(output["date"])
    order = pd.DataFrame({"_date": dates, "_index": output.index}).dropna(subset=["_date"])
    order = order.sort_values("_date", kind="stable")
    ordered = output.loc[order["_index"]].copy()
    ordered_dates = _date_keys(ordered["date"])
    close = pd.to_numeric(ordered["close"], errors="coerce")
    high = pd.to_numeric(ordered["high"], errors="coerce")
    low = pd.to_numeric(ordered["low"], errors="coerce")
    previous = close.shift(1)
    affected = ordered_dates.isin(changed_days) | ordered_dates.shift(1).isin(changed_days)
    valid_previous = previous.notna() & previous.gt(0)
    if "change" in ordered.columns:
        ordered.loc[affected, "change"] = (close - previous).where(valid_previous).loc[affected]
    if "change_pct" in ordered.columns:
        ordered.loc[affected, "change_pct"] = (
            (close / previous - 1.0) * 100.0
        ).where(valid_previous).loc[affected]
    if "amplitude" in ordered.columns:
        ordered.loc[affected, "amplitude"] = (
            (high - low) / previous * 100.0
        ).where(valid_previous).loc[affected]
    output.loc[ordered.index, ordered.columns] = ordered
    return output


def repair_frame(
    current: pd.DataFrame,
    raw_bars: dict[str, dict[str, float]],
    *,
    price_tolerance: float,
    raw_close_rtol: float,
    raw_close_atol: float,
    factor_window: int,
    factor_relative_tolerance: float,
) -> tuple[pd.DataFrame, dict[str, int], list[dict[str, Any]]]:
    result = current.copy()
    counts: Counter[str] = Counter()
    details: list[dict[str, Any]] = []
    required = {
        "date", "open", "high", "low", "close", "close_raw", "volume",
        "amount", "turnover", "market_cap",
    }
    if result.empty or not required.issubset(result.columns):
        return result, {"invalid_schema": 1}, details

    source = current.copy()
    source_dates = _date_keys(source["date"])
    result_dates = _date_keys(result["date"])
    planned: list[tuple[int, str, dict[str, float], float, str]] = []
    for day, bar in sorted(raw_bars.items()):
        mask = result_dates.eq(day)
        if not mask.any():
            counts["target_date_missing"] += 1
            continue
        index = int(result.index[mask][0])
        row = source.loc[index]
        current_volume = _number(row.get("volume"))
        if current_volume is None or current_volume <= 0 or valid_positive(
            pd.Series([row.get("amount")])
        ).iloc[0]:
            counts["not_a_positive_volume_amount_gap"] += 1
            continue
        reason = validate_raw_bar(bar, price_tolerance)
        if reason is not None:
            counts[f"rejected_{reason}"] += 1
            continue

        factor = _same_day_factor(
            row,
            bar,
            raw_close_rtol=raw_close_rtol,
            raw_close_atol=raw_close_atol,
        )
        method = "same_day_close_anchor"
        if factor is None:
            factor, factor_reason, _ = stable_adjustment_factor(
                source,
                day,
                factor_window=factor_window,
                factor_relative_tolerance=factor_relative_tolerance,
            )
            method = "two_sided_factor"
            if factor is None:
                counts[f"rejected_{factor_reason or 'missing_adjustment_factor'}"] += 1
                continue
        if not math.isfinite(float(factor)) or float(factor) <= 0:
            counts["rejected_invalid_adjustment_factor"] += 1
            continue
        planned.append((index, day, bar, float(factor), method))

    changed_days: set[str] = set()
    for index, day, bar, factor, method in planned:
        old = result.loc[index].copy()
        for target, raw_field in (
            ("open", "open_raw"),
            ("high", "high_raw"),
            ("low", "low_raw"),
            ("close", "close_raw"),
        ):
            result.at[index, target] = float(bar[raw_field]) * factor
        for field in ("close_raw", "volume", "amount", "turnover"):
            result.at[index, field] = float(bar[field])

        direct_cap = _number(bar.get("market_cap"))
        if direct_cap is None or direct_cap <= 0:
            shares = float(bar["volume"]) * 100.0 / float(bar["turnover"])
            direct_cap = float(bar["close_raw"]) * shares
            counts["market_cap_derived_from_turnover"] += 1
        result.at[index, "market_cap"] = direct_cap
        changed_days.add(day)
        counts["filled_full_wencai_bar"] += 1
        counts[f"factor_{method}"] += 1
        details.append(
            {
                "date": day,
                "factor_method": method,
                "adjustment_factor": factor,
                "old_close": _number(old.get("close")),
                "new_close": _number(result.at[index, "close"]),
                "old_close_raw": _number(old.get("close_raw")),
                "new_close_raw": float(bar["close_raw"]),
                "old_volume": _number(old.get("volume")),
                "new_volume": float(bar["volume"]),
                "new_amount": float(bar["amount"]),
            }
        )

    result = _recompute_change_fields(result, changed_days)
    return result, dict(counts), details


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument(
        "--raw-bars",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_unresolved_amount_raw_bars.csv",
    )
    parser.add_argument("--price-tolerance", type=float, default=0.02)
    parser.add_argument("--raw-close-rtol", type=float, default=0.001)
    parser.add_argument("--raw-close-atol", type=float, default=0.01)
    parser.add_argument("--factor-window", type=int, default=5)
    parser.add_argument("--factor-relative-tolerance", type=float, default=0.01)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--backup-dir",
        default="artifacts/maintenance/all_data_gaps/amount_ohlc_conflict_backup",
    )
    parser.add_argument(
        "--report",
        default="artifacts/maintenance/all_data_gaps/ths_amount_ohlc_conflict_report.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.time()
    data_dir = Path(args.data_dir).resolve()
    raw_bars_path = (ROOT / args.raw_bars).resolve()
    backup_dir = (ROOT / args.backup_dir).resolve()
    report_path = (ROOT / args.report).resolve()
    values = read_results(raw_bars_path)
    by_code: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for (code, day), bar in values.items():
        by_code[code][day] = bar

    total: Counter[str] = Counter()
    changed_files = 0
    repaired_rows: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    for code in sorted(by_code):
        path = code_path(data_dir, code)
        try:
            current = _read_csv(path)
            repaired, counts, details = repair_frame(
                current,
                by_code[code],
                price_tolerance=args.price_tolerance,
                raw_close_rtol=args.raw_close_rtol,
                raw_close_atol=args.raw_close_atol,
                factor_window=args.factor_window,
                factor_relative_tolerance=args.factor_relative_tolerance,
            )
            total.update(counts)
            repaired_rows.extend({"code": code, **detail} for detail in details)
            changed = counts.get("filled_full_wencai_bar", 0) > 0
            if changed:
                changed_files += 1
                if args.apply:
                    relative = path.relative_to(data_dir)
                    backup = backup_dir / relative
                    backup.parent.mkdir(parents=True, exist_ok=True)
                    if not backup.exists():
                        shutil.copy2(path, backup)
                    _atomic_csv(repaired, path)
                    invalidate_caches(data_dir, code)
        except Exception as exc:
            failed.append(
                {"code": code, "type": type(exc).__name__, "error": str(exc)[:500]}
            )

    report = {
        "status": "COMPLETED" if not failed else "FAILED",
        "applied": bool(args.apply),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_dir": str(data_dir),
        "raw_bars": str(raw_bars_path),
        "backup_dir": str(backup_dir),
        "source_priority": ["THSDK Wencai exact full raw bar"],
        "changed_files": changed_files,
        "counts": dict(total),
        "repaired_rows": repaired_rows,
        "failed": failed,
        "policy": {
            "raw_bar_price_tolerance": args.price_tolerance,
            "same_day_raw_close_rtol": args.raw_close_rtol,
            "same_day_raw_close_atol": args.raw_close_atol,
            "factor_window": args.factor_window,
            "factor_relative_tolerance": args.factor_relative_tolerance,
        },
        "elapsed_seconds": round(time.time() - started, 3),
    }
    _atomic_json(report, report_path)
    if args.apply and not failed:
        _update_manifest(
            data_dir,
            "amount_ohlc_conflicts_from_ths_full_bars",
            {
                "counts": dict(total),
                "policy": report["policy"],
                "report": str(report_path),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"counts={dict(total)} failed={len(failed)}", flush=True)
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
