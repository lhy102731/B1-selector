"""Prioritize THS Wencai historical amount for original THS K-line gaps."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
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
from tools.backfill_valuation_fields import (
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _read_csv,
    _update_manifest,
    code_path,
    stock_files,
)


def chinese_date(value: str) -> str:
    stamp = pd.Timestamp(value)
    return f"{stamp.year}年{stamp.month}月{stamp.day}日"


def original_targets(baseline_dir: Path) -> dict[str, set[str]]:
    by_date: dict[str, set[str]] = defaultdict(set)
    for path in stock_files(baseline_dir):
        frame = _read_csv(path)
        if frame.empty or not {"date", "volume", "amount"}.issubset(frame.columns):
            continue
        dates = _date_keys(frame["date"])
        volume = pd.to_numeric(frame["volume"], errors="coerce")
        amount = pd.to_numeric(frame["amount"], errors="coerce")
        missing = volume.gt(0) & ~valid_positive(amount)
        for day in dates.loc[missing].dropna().unique():
            by_date[str(day)].add(path.stem)
    return dict(by_date)


def parse_wencai_frame(
    frame: pd.DataFrame,
    *,
    allowed_pairs: set[tuple[str, str]],
) -> dict[tuple[str, str], float]:
    if frame is None or frame.empty:
        return {}
    code_column = next(
        (column for column in ("股票代码", "代码", "证券代码") if column in frame.columns),
        None,
    )
    if code_column is None:
        return {}
    amount_columns: dict[str, Any] = {}
    for column in frame.columns:
        text = str(column)
        if text.startswith("成交额[") and text.endswith("]"):
            compact = text[text.find("[") + 1 : -1]
            parsed = pd.to_datetime(compact, format="%Y%m%d", errors="coerce")
            if pd.notna(parsed):
                amount_columns[pd.Timestamp(parsed).strftime("%Y-%m-%d")] = column
    output: dict[tuple[str, str], float] = {}
    for _, row in frame.iterrows():
        code = str(row[code_column]).strip()[:6].zfill(6)
        if not code.isdigit():
            continue
        for day, column in amount_columns.items():
            key = (code, day)
            if key not in allowed_pairs:
                continue
            value = pd.to_numeric(pd.Series([row[column]]), errors="coerce").iloc[0]
            if (
                pd.notna(value)
                and math.isfinite(float(value))
                and float(value) > 0
                and float(value) not in COMMON_SENTINELS
            ):
                output[key] = float(value)
    return output


def query_with_retry(client: Any, condition: str, attempts: int = 3) -> pd.DataFrame:
    error = "unknown"
    for attempt in range(attempts):
        response = client.wencai_nlp(condition)
        if response.success:
            return response.df
        error = response.error
        time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"THS Wencai failed after {attempts} attempts: {error}")


def batched(values: list[str], size: int) -> Iterable[list[str]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def fetch_ths_amounts(
    targets_by_date: dict[str, set[str]],
    *,
    wide_date_min_codes: int,
    code_date_batch: int,
    min_interval: float,
) -> tuple[dict[tuple[str, str], float], dict[str, Any]]:
    from thsdk import THS

    allowed_pairs = {
        (code, day) for day, codes in targets_by_date.items() for code in codes
    }
    wide_dates = sorted(
        day for day, codes in targets_by_date.items() if len(codes) >= wide_date_min_codes
    )
    remaining_by_code: dict[str, list[str]] = defaultdict(list)
    for day, codes in targets_by_date.items():
        if day in wide_dates:
            continue
        for code in codes:
            remaining_by_code[code].append(day)

    logging.disable(logging.CRITICAL)
    client = THS()
    connected = client.connect()
    if not connected.success:
        raise RuntimeError(f"THSDK connect failed: {connected.error}")
    values: dict[tuple[str, str], float] = {}
    failed_queries: list[dict[str, str]] = []
    queries = 0
    last_query_at = 0.0

    def run_query(condition: str, expected: set[tuple[str, str]]) -> None:
        nonlocal queries, last_query_at
        wait = min_interval - (time.monotonic() - last_query_at)
        if wait > 0:
            time.sleep(wait)
        try:
            frame = query_with_retry(client, condition)
            values.update(parse_wencai_frame(frame, allowed_pairs=expected))
        except Exception as exc:
            failed_queries.append(
                {"condition": condition[:300], "error": f"{type(exc).__name__}: {exc}"}
            )
        last_query_at = time.monotonic()
        queries += 1
        if queries == 1 or queries % 25 == 0:
            print(
                f"wencai queries={queries} values={len(values)} failed={len(failed_queries)}",
                flush=True,
            )

    try:
        for day in wide_dates:
            expected = {(code, day) for code in targets_by_date[day]}
            run_query(f"{chinese_date(day)} A股 成交额", expected)
        for code in sorted(remaining_by_code):
            days = sorted(set(remaining_by_code[code]))
            for group in batched(days, code_date_batch):
                expected = {(code, day) for day in group}
                fields = " ".join(f"{chinese_date(day)}成交额" for day in group)
                run_query(f"{code} {fields}", expected)
    finally:
        client.disconnect()
        logging.disable(logging.NOTSET)
    return values, {
        "target_pairs": len(allowed_pairs),
        "wide_dates": len(wide_dates),
        "wide_target_pairs": sum(len(targets_by_date[day]) for day in wide_dates),
        "remaining_codes": len(remaining_by_code),
        "queries": queries,
        "failed_queries": failed_queries,
    }


def compatible_with_current_bar(
    row: pd.Series,
    amount: float,
    *,
    price_tolerance: float,
) -> bool:
    values = pd.to_numeric(
        pd.Series(
            {
                "volume": row.get("volume"),
                "low": row.get("low"),
                "high": row.get("high"),
                "close": row.get("close"),
                "close_raw": row.get("close_raw"),
            }
        ),
        errors="coerce",
    )
    if values.isna().any() or (values <= 0).any():
        return False
    factor = values["close"] / values["close_raw"]
    raw_low = values["low"] / factor
    raw_high = values["high"] / factor
    vwap = float(amount) / values["volume"]
    return bool(
        vwap >= raw_low * (1.0 - price_tolerance)
        and vwap <= raw_high * (1.0 + price_tolerance)
    )


def apply_values(
    data_dir: Path,
    targets_by_date: dict[str, set[str]],
    values: dict[tuple[str, str], float],
    *,
    price_tolerance: float,
    apply: bool,
) -> dict[str, int]:
    by_code: dict[str, dict[str, float]] = defaultdict(dict)
    for (code, day), amount in values.items():
        by_code[code][day] = amount
    stats: defaultdict[str, int] = defaultdict(int)
    targets_by_code: dict[str, list[str]] = defaultdict(list)
    for day, codes in targets_by_date.items():
        for code in codes:
            targets_by_code[code].append(day)
    for code in sorted(targets_by_code):
        path = code_path(data_dir, code)
        current = _read_csv(path)
        if current.empty:
            stats["missing_current_file"] += 1
            continue
        dates = _date_keys(current["date"])
        changed = False
        for day in sorted(set(targets_by_code[code])):
            mask = dates.eq(day)
            if not mask.any():
                stats["target_date_missing_current"] += 1
                continue
            before = pd.to_numeric(current.loc[mask, "amount"], errors="coerce").iloc[0]
            amount = by_code.get(code, {}).get(day)
            if amount is None:
                if pd.notna(before) and float(before) > 0:
                    stats["fallback_existing_valid"] += 1
                else:
                    stats["unresolved"] += 1
                continue
            row = current.loc[mask].iloc[0]
            if not compatible_with_current_bar(
                row, amount, price_tolerance=price_tolerance
            ):
                stats["rejected_price_incompatible"] += 1
                continue
            stats["ths_compatible"] += 1
            if pd.notna(before) and float(before) > 0:
                stats["ths_vs_existing_compared"] += 1
                if np.isclose(float(before), amount, rtol=1e-7, atol=0.01):
                    stats["ths_vs_existing_matching"] += 1
                else:
                    stats["ths_vs_existing_different"] += 1
            else:
                stats["filled_blank"] += 1
            current.loc[mask, "amount"] = float(amount)
            changed = True
        if changed:
            stats["changed_files"] += 1
            if apply:
                _atomic_csv(current, path)
    return dict(stats)


def atomic_results(values: dict[tuple[str, str], float], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        [
            {"code": code, "date": day, "amount": amount}
            for (code, day), amount in sorted(values.items())
        ]
    )
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8-sig",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            frame.to_csv(handle, index=False)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--baseline-dir", default="data_ths")
    parser.add_argument("--wide-date-min-codes", type=int, default=100)
    parser.add_argument("--code-date-batch", type=int, default=40)
    parser.add_argument("--min-interval", type=float, default=0.25)
    parser.add_argument("--price-tolerance", type=float, default=0.001)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--results",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_amount_values.csv",
    )
    parser.add_argument(
        "--report",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_amount_report.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.code_date_batch > 40:
        raise ValueError("code-date batches above 40 are not stable in THS Wencai")
    baseline_dir = Path(args.baseline_dir).resolve()
    data_dir = Path(args.data_dir).resolve()
    started = time.time()
    targets = original_targets(baseline_dir)
    values, fetch = fetch_ths_amounts(
        targets,
        wide_date_min_codes=args.wide_date_min_codes,
        code_date_batch=args.code_date_batch,
        min_interval=args.min_interval,
    )
    results_path = (ROOT / args.results).resolve()
    atomic_results(values, results_path)
    apply_stats = apply_values(
        data_dir,
        targets,
        values,
        price_tolerance=args.price_tolerance,
        apply=args.apply,
    )
    report = {
        "status": "COMPLETED" if not fetch["failed_queries"] else "PARTIAL",
        "applied": bool(args.apply),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_dir": str(data_dir),
        "baseline_dir": str(baseline_dir),
        "source": "THSDK wencai_nlp historical amount",
        "fetch": fetch,
        "returned_positive_pairs": len(values),
        "apply_stats": apply_stats,
        "results": str(results_path),
        "elapsed_seconds": round(time.time() - started, 3),
    }
    report_path = (ROOT / args.report).resolve()
    _atomic_json(report, report_path)
    if args.apply and not fetch["failed_queries"]:
        _update_manifest(
            data_dir,
            "historical_amount_ths_wencai",
            {
                "source": "THSDK wencai_nlp",
                "target_pairs": fetch["target_pairs"],
                "returned_positive_pairs": len(values),
                "price_tolerance": args.price_tolerance,
                "apply_stats": apply_stats,
                "fallback": "audited legacy BaoStock amount",
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"returned={len(values)} apply_stats={apply_stats}", flush=True)
    return 0 if not fetch["failed_queries"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
