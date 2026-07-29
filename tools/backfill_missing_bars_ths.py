"""Restore genuine missing THS daily bars via historical Wencai raw fields.

Legacy rows are used only to identify candidate code/date pairs with positive
volume.  All inserted market data comes from THS Wencai.  Adjusted OHLC is
rebuilt only when the committed THS bars on both sides imply a stable
backward-adjustment factor.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import shutil
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_missing_amount import COMMON_SENTINELS, valid_positive
from tools.backfill_missing_amount_ths import (
    batched,
    chinese_date,
    query_with_retry,
)
from tools.backfill_valuation_fields import (
    VALUATION_COLUMNS,
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _read_csv,
    _update_manifest,
    code_path,
    stock_files,
)


RAW_FIELDS = (
    "开盘价",
    "最高价",
    "最低价",
    "收盘价",
    "成交量",
    "成交额",
    "换手率",
    "流通市值",
)
OUTPUT_FIELDS = (
    "open_raw",
    "high_raw",
    "low_raw",
    "close_raw",
    "volume",
    "amount",
    "turnover",
    "market_cap",
)


def identify_targets(
    current_dir: Path,
    legacy_dir: Path,
) -> tuple[dict[str, set[str]], dict[tuple[str, str], dict[str, float]]]:
    targets: dict[str, set[str]] = defaultdict(set)
    legacy_evidence: dict[tuple[str, str], dict[str, float]] = {}
    for current_path in stock_files(current_dir):
        code = current_path.stem
        legacy_path = code_path(legacy_dir, code)
        current = _read_csv(current_path)
        try:
            legacy = _read_csv(legacy_path)
        except Exception:
            continue
        if (
            current.empty
            or legacy.empty
            or "date" not in current.columns
            or not {"date", "volume"}.issubset(legacy.columns)
        ):
            continue
        current_dates = set(_date_keys(current["date"]).dropna())
        old = legacy.assign(_date=_date_keys(legacy["date"]))
        old_volume = pd.to_numeric(old["volume"], errors="coerce")
        old_amount = pd.to_numeric(
            old.get("amount", pd.Series(pd.NA, index=old.index)), errors="coerce"
        )
        candidate = old["_date"].notna() & old_volume.gt(0) & ~old["_date"].isin(
            current_dates
        )
        for idx in old.index[candidate]:
            day = str(old.at[idx, "_date"])
            targets[day].add(code)
            evidence = {"volume": float(old_volume.at[idx])}
            if pd.notna(old_amount.at[idx]) and float(old_amount.at[idx]) > 0:
                evidence["amount"] = float(old_amount.at[idx])
            legacy_evidence[(code, day)] = evidence
    return dict(targets), legacy_evidence


def _field_kind(column: str) -> str | None:
    text = str(column)
    if text.startswith("开盘价:不复权["):
        return "open_raw"
    if text.startswith("最高价:不复权["):
        return "high_raw"
    if text.startswith("最低价:不复权["):
        return "low_raw"
    if text.startswith("收盘价:不复权["):
        return "close_raw"
    if text.startswith("成交量["):
        return "volume"
    if text.startswith("成交额["):
        return "amount"
    if text.startswith("换手率["):
        return "turnover"
    if text.lower().startswith("a股市值(不含限售股)[") or text.startswith(
        "流通市值["
    ):
        return "market_cap"
    return None


def _column_date(column: str) -> str | None:
    text = str(column)
    if "[" not in text or not text.endswith("]"):
        return None
    compact = text[text.rfind("[") + 1 : -1]
    parsed = pd.to_datetime(compact, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else pd.Timestamp(parsed).strftime("%Y-%m-%d")


def parse_wencai_bars(
    frame: pd.DataFrame,
    *,
    allowed_pairs: set[tuple[str, str]],
) -> dict[tuple[str, str], dict[str, float]]:
    if frame is None or frame.empty:
        return {}
    code_column = next(
        (column for column in ("股票代码", "代码", "证券代码") if column in frame.columns),
        None,
    )
    if code_column is None:
        return {}
    mapped: dict[tuple[str, str], Any] = {}
    for column in frame.columns:
        kind = _field_kind(str(column))
        day = _column_date(str(column))
        if kind and day:
            mapped[(kind, day)] = column
    output: dict[tuple[str, str], dict[str, float]] = {}
    for _, row in frame.iterrows():
        code = str(row[code_column]).strip()[:6].zfill(6)
        if not code.isdigit():
            continue
        for _, day in sorted(mapped):
            key = (code, day)
            if key not in allowed_pairs:
                continue
            values = output.setdefault(key, {})
            for field in OUTPUT_FIELDS:
                column = mapped.get((field, day))
                if column is None:
                    continue
                value = pd.to_numeric(pd.Series([row[column]]), errors="coerce").iloc[0]
                if (
                    pd.notna(value)
                    and math.isfinite(float(value))
                    and float(value) not in COMMON_SENTINELS
                ):
                    values[field] = float(value)
    return output


def validate_raw_bar(bar: dict[str, float], price_tolerance: float) -> str | None:
    required = ("open_raw", "high_raw", "low_raw", "close_raw", "volume", "amount")
    if any(field not in bar or not math.isfinite(bar[field]) or bar[field] <= 0 for field in required):
        return "missing_required_raw_field"
    low, high = bar["low_raw"], bar["high_raw"]
    if low > high:
        return "raw_low_above_high"
    if min(bar["open_raw"], bar["close_raw"]) < low * (1.0 - price_tolerance):
        return "raw_body_below_low"
    if max(bar["open_raw"], bar["close_raw"]) > high * (1.0 + price_tolerance):
        return "raw_body_above_high"
    vwap = bar["amount"] / bar["volume"]
    if not (
        vwap >= low * (1.0 - price_tolerance)
        and vwap <= high * (1.0 + price_tolerance)
    ):
        return "vwap_outside_raw_ohlc"
    turnover = bar.get("turnover")
    if turnover is None or not math.isfinite(turnover) or turnover <= 0 or turnover > 1000:
        return "invalid_turnover"
    direct_cap = bar.get("market_cap")
    if direct_cap is not None:
        if not math.isfinite(direct_cap) or direct_cap <= 0:
            return "invalid_market_cap"
        shares = bar["volume"] * 100.0 / turnover
        derived_cap = bar["close_raw"] * shares
        if abs(direct_cap / derived_cap - 1.0) > 0.01:
            return "market_cap_turnover_mismatch"
    return None


def stable_adjustment_factor(
    current: pd.DataFrame,
    day: str,
    *,
    factor_window: int,
    factor_relative_tolerance: float,
) -> tuple[float | None, str | None, float | None]:
    dates = _date_keys(current["date"])
    work = current.assign(_date=dates).dropna(subset=["_date"]).sort_values("_date")
    before = work[work["_date"].lt(day)].tail(factor_window)
    after = work[work["_date"].gt(day)].head(factor_window)
    if before.empty or after.empty:
        return None, "missing_two_sided_factor_anchor", None

    def ratios(frame: pd.DataFrame) -> pd.Series:
        adjusted = pd.to_numeric(frame["close"], errors="coerce")
        raw = pd.to_numeric(frame["close_raw"], errors="coerce")
        values = adjusted / raw
        return values[np.isfinite(values) & values.gt(0)]

    before_values, after_values = ratios(before), ratios(after)
    if before_values.empty or after_values.empty:
        return None, "invalid_factor_anchor", None
    before_factor = float(before_values.median())
    after_factor = float(after_values.median())
    gap = abs(before_factor / after_factor - 1.0)
    if gap > factor_relative_tolerance:
        return None, "factor_anchor_mismatch", gap
    return float(pd.concat([before_values, after_values]).median()), None, gap


def build_inserted_row(
    columns: list[str],
    day: str,
    bar: dict[str, float],
    factor: float,
) -> dict[str, Any]:
    row: dict[str, Any] = {column: pd.NA for column in columns}
    row["date"] = day
    for field in ("open", "high", "low", "close"):
        row[field] = round(float(bar[f"{field}_raw"]) * factor, 3)
    row["close_raw"] = float(bar["close_raw"])
    row["volume"] = float(bar["volume"])
    row["amount"] = float(bar["amount"])
    row["turnover"] = float(bar["turnover"])
    shares = row["volume"] * 100.0 / row["turnover"]
    derived_cap = row["close_raw"] * shares
    direct_cap = bar.get("market_cap")
    if direct_cap and direct_cap > 0 and abs(direct_cap / derived_cap - 1.0) <= 0.01:
        row["market_cap"] = float(direct_cap)
    else:
        row["market_cap"] = float(derived_cap)
    for field in VALUATION_COLUMNS:
        if field in row:
            row[field] = pd.NA
    return row


def insert_bars(
    current: pd.DataFrame,
    code: str,
    bars_by_day: dict[str, dict[str, float]],
    *,
    factor_window: int,
    factor_relative_tolerance: float,
    price_tolerance: float,
) -> tuple[pd.DataFrame, dict[str, int], list[dict[str, Any]]]:
    result = current.copy()
    stats: defaultdict[str, int] = defaultdict(int)
    details: list[dict[str, Any]] = []
    existing_dates = set(_date_keys(result["date"]).dropna())
    rows: list[dict[str, Any]] = []
    inserted_days: set[str] = set()
    columns = list(result.columns)
    for day, bar in sorted(bars_by_day.items()):
        if day in existing_dates:
            stats["already_present"] += 1
            continue
        reason = validate_raw_bar(bar, price_tolerance)
        if reason:
            stats[f"rejected_{reason}"] += 1
            details.append({"code": code, "date": day, "status": reason})
            continue
        factor, reason, factor_gap = stable_adjustment_factor(
            result,
            day,
            factor_window=factor_window,
            factor_relative_tolerance=factor_relative_tolerance,
        )
        if reason or factor is None:
            stats[f"rejected_{reason}"] += 1
            details.append(
                {"code": code, "date": day, "status": reason, "factor_gap": factor_gap}
            )
            continue
        rows.append(build_inserted_row(columns, day, bar, factor))
        inserted_days.add(day)
        stats["inserted"] += 1
        details.append(
            {"code": code, "date": day, "status": "inserted", "factor": factor, "factor_gap": factor_gap}
        )
    if not rows:
        return result, dict(stats), details

    result = pd.concat([result, pd.DataFrame(rows, columns=columns)], ignore_index=True)
    result["_date"] = _date_keys(result["date"])
    result = result.dropna(subset=["_date"]).drop_duplicates("_date", keep="last")
    result = result.sort_values("_date").reset_index(drop=True)
    affected = set(inserted_days)
    for day in inserted_days:
        later = result.loc[result["_date"].gt(day), "_date"]
        if not later.empty:
            affected.add(str(later.iloc[0]))
    previous = pd.to_numeric(result["close"], errors="coerce").shift(1)
    close = pd.to_numeric(result["close"], errors="coerce")
    high = pd.to_numeric(result["high"], errors="coerce")
    low = pd.to_numeric(result["low"], errors="coerce")
    affected_mask = result["_date"].isin(affected) & previous.gt(0)
    if "change" in result.columns:
        result.loc[affected_mask, "change"] = close - previous
    if "change_pct" in result.columns:
        result.loc[affected_mask, "change_pct"] = (close / previous - 1.0) * 100.0
    if "amplitude" in result.columns:
        result.loc[affected_mask, "amplitude"] = (high - low) / previous * 100.0
    result["date"] = result["_date"]
    result = result.sort_values("_date", ascending=False).drop(columns=["_date"])
    return result[columns], dict(stats), details


def fetch_bars(
    targets_by_date: dict[str, set[str]],
    *,
    wide_date_min_codes: int,
    code_date_batch: int,
    min_interval: float,
) -> tuple[dict[tuple[str, str], dict[str, float]], dict[str, Any]]:
    from thsdk import THS

    allowed_pairs = {
        (code, day) for day, codes in targets_by_date.items() for code in codes
    }
    wide_dates = sorted(
        day for day, codes in targets_by_date.items() if len(codes) >= wide_date_min_codes
    )
    remaining: dict[str, list[str]] = defaultdict(list)
    for day, codes in targets_by_date.items():
        if day not in wide_dates:
            for code in codes:
                remaining[code].append(day)
    logging.disable(logging.CRITICAL)
    client = THS()
    connected = client.connect()
    if not connected.success:
        raise RuntimeError(f"THSDK connect failed: {connected.error}")
    values: dict[tuple[str, str], dict[str, float]] = {}
    failed_queries: list[dict[str, str]] = []
    last_query_at = 0.0
    queries = 0

    def run_query(condition: str, expected: set[tuple[str, str]]) -> None:
        nonlocal queries, last_query_at
        wait = min_interval - (time.monotonic() - last_query_at)
        if wait > 0:
            time.sleep(wait)
        try:
            frame = query_with_retry(client, condition)
            parsed = parse_wencai_bars(frame, allowed_pairs=expected)
            for key, fields in parsed.items():
                values.setdefault(key, {}).update(fields)
        except Exception as exc:
            failed_queries.append(
                {"condition": condition[:500], "error": f"{type(exc).__name__}: {exc}"}
            )
        last_query_at = time.monotonic()
        queries += 1
        if queries == 1 or queries % 25 == 0:
            print(f"bar queries={queries} values={len(values)} failed={len(failed_queries)}", flush=True)

    try:
        for day in wide_dates:
            expected = {(code, day) for code in targets_by_date[day]}
            run_query(
                f"{chinese_date(day)} A股 " + " ".join(RAW_FIELDS),
                expected,
            )
        for code in sorted(remaining):
            for days in batched(sorted(set(remaining[code])), code_date_batch):
                expected = {(code, day) for day in days}
                fields = " ".join(
                    f"{chinese_date(day)}{field}" for day in days for field in RAW_FIELDS
                )
                run_query(f"{code} {fields}", expected)
    finally:
        client.disconnect()
        logging.disable(logging.NOTSET)
    return values, {
        "target_pairs": len(allowed_pairs),
        "target_dates": len(targets_by_date),
        "wide_dates": len(wide_dates),
        "wide_target_pairs": sum(len(targets_by_date[day]) for day in wide_dates),
        "remaining_codes": len(remaining),
        "queries": queries,
        "failed_queries": failed_queries,
    }


def atomic_results(values: dict[tuple[str, str], dict[str, float]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"code": code, "date": day, **fields}
        for (code, day), fields in sorted(values.items())
    ]
    frame = pd.DataFrame(rows)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8-sig", newline="", dir=path.parent,
            prefix=f".{path.name}.", suffix=".tmp", delete=False,
        ) as handle:
            frame.to_csv(handle, index=False)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def read_results(path: Path) -> dict[tuple[str, str], dict[str, float]]:
    frame = pd.read_csv(path, encoding="utf-8-sig", dtype={"code": str, "date": str})
    output: dict[tuple[str, str], dict[str, float]] = {}
    for _, row in frame.iterrows():
        fields: dict[str, float] = {}
        for field in OUTPUT_FIELDS:
            value = pd.to_numeric(pd.Series([row.get(field)]), errors="coerce").iloc[0]
            if pd.notna(value):
                fields[field] = float(value)
        output[(str(row["code"]).zfill(6), str(row["date"]))] = fields
    return output


def invalidate_caches(data_dir: Path, code: str) -> None:
    for path in (
        data_dir / "raw_parquet" / code[:2] / f"{code}.parquet",
        data_dir / "indicators_cache" / f"{code}.parquet",
    ):
        if path.exists():
            path.unlink()


def backup_file(data_dir: Path, backup_dir: Path, path: Path) -> None:
    relative = path.relative_to(data_dir)
    target = backup_dir / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        shutil.copy2(path, target)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--legacy-dir", default="data_pre_ths_backup_20260727_110350")
    parser.add_argument("--wide-date-min-codes", type=int, default=100)
    parser.add_argument("--code-date-batch", type=int, default=5)
    parser.add_argument("--min-interval", type=float, default=0.25)
    parser.add_argument("--factor-window", type=int, default=5)
    parser.add_argument("--factor-relative-tolerance", type=float, default=0.01)
    parser.add_argument("--price-tolerance", type=float, default=0.001)
    parser.add_argument("--reuse-results", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--backup-dir",
        default="artifacts/maintenance/all_data_gaps/missing_bars_backup",
    )
    parser.add_argument("--results", default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_bars.csv")
    parser.add_argument("--report", default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_bars_report.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.code_date_batch > 5:
        raise ValueError("full-bar Wencai batches above five dates are not validated")
    data_dir, legacy_dir = Path(args.data_dir).resolve(), Path(args.legacy_dir).resolve()
    backup_dir = (ROOT / args.backup_dir).resolve()
    started = time.time()
    targets, legacy_evidence = identify_targets(data_dir, legacy_dir)
    results_path = (ROOT / args.results).resolve()
    if args.reuse_results:
        values = read_results(results_path)
        fetch = {"target_pairs": sum(map(len, targets.values())), "reused_results": True, "failed_queries": []}
    else:
        values, fetch = fetch_bars(
            targets,
            wide_date_min_codes=args.wide_date_min_codes,
            code_date_batch=args.code_date_batch,
            min_interval=args.min_interval,
        )
        atomic_results(values, results_path)

    cross = defaultdict(int)
    for key, bar in values.items():
        old = legacy_evidence.get(key)
        if not old:
            continue
        cross["compared_volume"] += 1
        if "volume" in bar and np.isclose(bar["volume"], old["volume"], rtol=0.01, atol=1.0):
            cross["volume_within_1pct"] += 1
        if "amount" in bar and "amount" in old:
            cross["compared_amount"] += 1
            if np.isclose(bar["amount"], old["amount"], rtol=0.01, atol=1.0):
                cross["amount_within_1pct"] += 1

    by_code: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for (code, day), bar in values.items():
        by_code[code][day] = bar
    aggregate: defaultdict[str, int] = defaultdict(int)
    details: list[dict[str, Any]] = []
    for code in sorted(by_code):
        path = code_path(data_dir, code)
        current = _read_csv(path)
        merged, stats, rows = insert_bars(
            current,
            code,
            by_code[code],
            factor_window=args.factor_window,
            factor_relative_tolerance=args.factor_relative_tolerance,
            price_tolerance=args.price_tolerance,
        )
        for key, value in stats.items():
            aggregate[key] += value
        details.extend(rows)
        if args.apply and stats.get("inserted", 0):
            backup_file(data_dir, backup_dir, path)
            _atomic_csv(merged, path)
            invalidate_caches(data_dir, code)

    report = {
        "status": "COMPLETED" if not fetch.get("failed_queries") else "PARTIAL",
        "applied": bool(args.apply),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "THSDK wencai_nlp historical raw daily fields",
        "data_dir": str(data_dir),
        "legacy_dir": str(legacy_dir),
        "fetch": fetch,
        "returned_pairs": len(values),
        "crosscheck_legacy": dict(cross),
        "counts": dict(aggregate),
        "details": details[:500],
        "results": str(results_path),
        "factor_window": args.factor_window,
        "factor_relative_tolerance": args.factor_relative_tolerance,
        "price_tolerance": args.price_tolerance,
        "elapsed_seconds": round(time.time() - started, 3),
    }
    report_path = (ROOT / args.report).resolve()
    _atomic_json(report, report_path)
    if args.apply and not fetch.get("failed_queries"):
        _update_manifest(
            data_dir,
            "historical_missing_bars_ths_wencai",
            {
                "source": "THSDK wencai_nlp raw OHLCV/amount/turnover/cap",
                "candidate_pairs": fetch["target_pairs"],
                "returned_pairs": len(values),
                "inserted": int(aggregate["inserted"]),
                "factor_relative_tolerance": args.factor_relative_tolerance,
                "price_tolerance": args.price_tolerance,
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"returned={len(values)} counts={dict(aggregate)}", flush=True)
    return 0 if not fetch.get("failed_queries") else 2


if __name__ == "__main__":
    raise SystemExit(main())
