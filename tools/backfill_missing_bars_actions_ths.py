"""Repair the last THS raw bars whose adjusted prices are unavailable.

Some early candles exist in THS Wencai only as raw OHLCV.  This final price
stage reconstructs post-adjusted closes from THS corporate actions and accepts
them only under one of two auditable gates:

* the reconstructed series differs from the committed THS adjusted series by
  one stable scale over the full overlap; or
* every missing row leads the committed series, there was no corporate action
  before the first committed row, and the leading overlap has one stable scale.

The second rule covers a clean pre-action prefix.  It deliberately rejects a
leading gap that crosses unverified rights/dividend events.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_missing_bars_adjusted_ths import insert_adjusted_bars
from tools.backfill_missing_bars_ths import invalidate_caches, read_results as read_raw_results
from tools.backfill_valuation_fields import (
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _read_csv,
    _update_manifest,
    code_path,
)
from utils.ths_data_source import THSDataSource


def reconstruct_adjusted_closes(
    current: pd.DataFrame,
    bars_by_day: dict[str, dict[str, float]],
    actions: pd.DataFrame,
    *,
    minimum_overlap_rows: int,
    full_p99_tolerance: float,
    full_max_tolerance: float,
    leading_tolerance: float,
) -> tuple[dict[str, float], dict[str, Any]]:
    dates = _date_keys(current["date"])
    existing_dates = set(dates.dropna())
    missing_days = sorted(day for day in bars_by_day if day not in existing_dates)
    audit: dict[str, Any] = {
        "candidate_rows": len(missing_days),
        "accepted": False,
        "method": None,
    }
    if not missing_days:
        audit["reason"] = "no_missing_rows"
        return {}, audit

    current_close = pd.to_numeric(current["close"], errors="coerce")
    current_raw_close = pd.to_numeric(current["close_raw"], errors="coerce")
    valid_current = (
        dates.notna()
        & current_close.notna()
        & current_raw_close.notna()
        & np.isfinite(current_close)
        & np.isfinite(current_raw_close)
        & current_close.gt(0)
        & current_raw_close.gt(0)
    )
    committed = pd.DataFrame(
        {
            "date": pd.to_datetime(dates[valid_current]),
            "committed_close": current_close[valid_current].astype(float),
            "raw_close": current_raw_close[valid_current].astype(float),
        }
    )
    if len(committed) < minimum_overlap_rows:
        audit["reason"] = "insufficient_committed_overlap"
        return {}, audit

    raw_existing = pd.DataFrame(
        {
            "date": committed["date"],
            "open": committed["raw_close"],
            "high": committed["raw_close"],
            "low": committed["raw_close"],
            "close": committed["raw_close"],
        }
    )
    raw_missing = pd.DataFrame(
        [
            {
                "date": pd.Timestamp(day),
                "open": float(bars_by_day[day]["open_raw"]),
                "high": float(bars_by_day[day]["high_raw"]),
                "low": float(bars_by_day[day]["low_raw"]),
                "close": float(bars_by_day[day]["close_raw"]),
            }
            for day in missing_days
            if all(
                field in bars_by_day[day]
                and math.isfinite(float(bars_by_day[day][field]))
                and float(bars_by_day[day][field]) > 0
                for field in ("open_raw", "high_raw", "low_raw", "close_raw")
            )
        ]
    )
    if len(raw_missing) != len(missing_days):
        audit["reason"] = "missing_raw_price"
        return {}, audit
    raw = (
        pd.concat([raw_existing, raw_missing], ignore_index=True)
        .drop_duplicates("date", keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )
    rebuilt = THSDataSource._apply_backward_adjustment(raw, actions)
    overlap = rebuilt[["date", "close"]].merge(
        committed[["date", "committed_close"]], on="date", validate="one_to_one"
    )
    ratios = overlap["committed_close"] / pd.to_numeric(overlap["close"], errors="coerce")
    ratios = ratios[np.isfinite(ratios) & ratios.gt(0)]
    if len(ratios) < minimum_overlap_rows:
        audit["reason"] = "insufficient_valid_overlap"
        return {}, audit

    full_scale = float(ratios.median())
    full_error = (ratios / full_scale - 1.0).abs()
    full_p99 = float(full_error.quantile(0.99))
    full_max = float(full_error.max())
    audit.update(
        {
            "overlap_rows": int(len(ratios)),
            "full_scale": full_scale,
            "full_p99_relative_error": full_p99,
            "full_max_relative_error": full_max,
        }
    )
    scale: float | None = None
    if full_p99 <= full_p99_tolerance and full_max <= full_max_tolerance:
        scale = full_scale
        audit["method"] = "full_overlap_stable_scale"
    else:
        first_current = committed["date"].min()
        leading_only = pd.Timestamp(max(missing_days)) < first_current
        action_dates = (
            pd.to_datetime(actions.get("date"), errors="coerce")
            if actions is not None and not actions.empty and "date" in actions
            else pd.Series(dtype="datetime64[ns]")
        )
        actions_before_boundary = int((action_dates <= first_current).sum())
        leading = ratios.head(minimum_overlap_rows)
        leading_scale = float(leading.median())
        leading_max = float((leading / leading_scale - 1.0).abs().max())
        audit.update(
            {
                "leading_only": bool(leading_only),
                "actions_before_boundary": actions_before_boundary,
                "leading_scale": leading_scale,
                "leading_max_relative_error": leading_max,
            }
        )
        if (
            leading_only
            and actions_before_boundary == 0
            and leading_max <= leading_tolerance
        ):
            scale = leading_scale
            audit["method"] = "clean_pre_action_prefix"

    if scale is None:
        audit["reason"] = "adjustment_scale_not_proven"
        return {}, audit
    missing_rebuilt = rebuilt.loc[
        rebuilt["date"].dt.strftime("%Y-%m-%d").isin(missing_days), ["date", "close"]
    ].copy()
    output = {
        pd.Timestamp(row.date).strftime("%Y-%m-%d"): float(row.close) * scale
        for row in missing_rebuilt.itertuples(index=False)
    }
    audit["accepted"] = len(output) == len(missing_days)
    audit["scale"] = scale
    audit["reconstructed_rows"] = len(output)
    if not audit["accepted"]:
        audit["reason"] = "incomplete_reconstruction"
        return {}, audit
    return output, audit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument(
        "--raw-results",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_bars.csv",
    )
    parser.add_argument(
        "--report",
        default="artifacts/maintenance/all_data_gaps/ths_actions_missing_bars_report.json",
    )
    parser.add_argument(
        "--backup-dir",
        default="artifacts/maintenance/all_data_gaps/missing_bars_actions_backup",
    )
    parser.add_argument("--minimum-overlap-rows", type=int, default=20)
    parser.add_argument("--full-p99-tolerance", type=float, default=0.001)
    parser.add_argument("--full-max-tolerance", type=float, default=0.005)
    parser.add_argument("--leading-tolerance", type=float, default=0.001)
    parser.add_argument("--price-tolerance", type=float, default=0.001)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir).resolve()
    raw_path = (ROOT / args.raw_results).resolve()
    backup_dir = (ROOT / args.backup_dir).resolve()
    raw_values = read_raw_results(raw_path)
    raw_by_code: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for (code, day), bar in raw_values.items():
        raw_by_code[code][day] = bar
    aggregate: defaultdict[str, int] = defaultdict(int)
    audits: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, str]] = []
    changed_files = 0
    started = time.time()
    with THSDataSource() as source:
        for index, code in enumerate(sorted(raw_by_code), 1):
            path = code_path(data_dir, code)
            current = _read_csv(path)
            existing = set(_date_keys(current["date"]).dropna())
            remaining = {
                day: bar for day, bar in raw_by_code[code].items() if day not in existing
            }
            if not remaining:
                continue
            try:
                actions = source.fetch_corporate_actions(code)
                adjusted, audit = reconstruct_adjusted_closes(
                    current,
                    remaining,
                    actions,
                    minimum_overlap_rows=args.minimum_overlap_rows,
                    full_p99_tolerance=args.full_p99_tolerance,
                    full_max_tolerance=args.full_max_tolerance,
                    leading_tolerance=args.leading_tolerance,
                )
                audit["corporate_action_count"] = int(len(actions))
                audits[code] = audit
                if not adjusted:
                    aggregate[f"rejected_{audit.get('reason', 'unknown')}"] += len(remaining)
                    continue
                adjusted_opens = {
                    day: float(adjusted[day])
                    * float(remaining[day]["open_raw"])
                    / float(remaining[day]["close_raw"])
                    for day in adjusted
                }
                merged, stats, _ = insert_adjusted_bars(
                    current,
                    code,
                    remaining,
                    adjusted_opens,
                    calibration_scale=1.0,
                    adjusted_close_by_day=adjusted,
                    price_tolerance=args.price_tolerance,
                )
                for key, value in stats.items():
                    aggregate[key] += value
                if stats.get("inserted", 0):
                    changed_files += 1
                    if args.apply:
                        relative = path.relative_to(data_dir)
                        backup_path = backup_dir / relative
                        backup_path.parent.mkdir(parents=True, exist_ok=True)
                        if not backup_path.exists():
                            shutil.copy2(path, backup_path)
                        _atomic_csv(merged, path)
                        invalidate_caches(data_dir, code)
            except Exception as exc:
                failures.append(
                    {"code": code, "type": type(exc).__name__, "error": str(exc)[:500]}
                )
            if index == 1 or index % 25 == 0:
                print(
                    f"actions {index}/{len(raw_by_code)} inserted={aggregate['inserted']} "
                    f"failed={len(failures)}",
                    flush=True,
                )

    report = {
        "status": "COMPLETED" if not failures else "PARTIAL",
        "applied": bool(args.apply),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "THSDK Wencai raw bars + THSDK corporate actions",
        "data_dir": str(data_dir),
        "raw_results": str(raw_path),
        "backup_dir": str(backup_dir),
        "changed_files": changed_files,
        "counts": dict(aggregate),
        "code_audits": audits,
        "failures": failures,
        "policy": {
            "minimum_overlap_rows": args.minimum_overlap_rows,
            "full_p99_tolerance": args.full_p99_tolerance,
            "full_max_tolerance": args.full_max_tolerance,
            "leading_tolerance": args.leading_tolerance,
        },
        "elapsed_seconds": round(time.time() - started, 3),
    }
    report_path = (ROOT / args.report).resolve()
    _atomic_json(report, report_path)
    if args.apply and not failures:
        _update_manifest(
            data_dir,
            "historical_missing_bars_ths_actions",
            {
                "source": "THSDK Wencai raw bars + THSDK corporate actions",
                "inserted": int(aggregate["inserted"]),
                "policy": report["policy"],
                "report": str(report_path),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"counts={dict(aggregate)} failed={len(failures)}", flush=True)
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
