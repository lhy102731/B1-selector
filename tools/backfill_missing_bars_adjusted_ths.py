"""Finish THS missing-bar repairs with Wencai post-adjusted opens.

The first-stage repair in :mod:`tools.backfill_missing_bars_ths` inserts only
rows whose neighbouring committed candles provide a stable adjustment factor.
Leading archive gaps have no left-hand anchor.  For those rows this tool asks
THS Wencai for the exact post-adjusted open, calibrates the Wencai adjustment
scale against multiple nearby, already committed THS candles, and then applies
the same-day factor to the raw OHLC fields fetched by the first stage.  The
multi-anchor gate is deliberately strict: every returned calibration candle
must fit the median scale within the configured tolerance.

Legacy data is not used as a value source.  It only identified the original
candidate dates before the first-stage THS fetch.
"""

from __future__ import annotations

import argparse
import hashlib
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
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_missing_amount_ths import batched, chinese_date, query_with_retry
from tools.backfill_missing_bars_ths import (
    _column_date,
    build_inserted_row,
    invalidate_caches,
    read_results as read_raw_results,
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


def file_signature(path: Path) -> tuple[int, int, str]:
    stat = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return stat.st_size, stat.st_mtime_ns, digest.hexdigest()


def atomic_backup(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
        shutil.copy2(source, temporary)
        os.replace(temporary, destination)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def parse_adjusted_opens(
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
    mapped: dict[str, Any] = {}
    for column in frame.columns:
        text = str(column)
        day = _column_date(text)
        if day and text.startswith("开盘价:后复权["):
            mapped[day] = column
    output: dict[tuple[str, str], float] = {}
    for _, row in frame.iterrows():
        code = str(row[code_column]).strip()[:6].zfill(6)
        if not code.isdigit():
            continue
        for day, column in mapped.items():
            key = (code, day)
            if key not in allowed_pairs:
                continue
            value = pd.to_numeric(pd.Series([row[column]]), errors="coerce").iloc[0]
            if pd.notna(value) and math.isfinite(float(value)) and float(value) > 0:
                output[key] = float(value)
    return output


def _valid_open_rows(frame: pd.DataFrame) -> pd.DataFrame:
    dates = _date_keys(frame["date"])
    opened = pd.to_numeric(frame["open"], errors="coerce")
    valid = dates.notna() & opened.notna() & np.isfinite(opened) & opened.gt(0)
    candidates = pd.DataFrame(
        {"date": dates[valid].astype(str), "open": opened[valid].astype(float)}
    ).drop_duplicates("date", keep="first")
    candidates = candidates.sort_values("date").reset_index(drop=True)
    return candidates


def remaining_targets(
    data_dir: Path,
    raw_values: dict[tuple[str, str], dict[str, float]],
    *,
    anchor_window: int,
    local_anchor_window: int,
) -> tuple[
    dict[str, set[str]],
    dict[str, dict[str, float]],
    dict[str, pd.DataFrame],
    dict[tuple[str, str], str],
    dict[str, dict[str, float]],
    dict[str, str],
    dict[str, dict[str, Any]],
]:
    by_code: dict[str, set[str]] = defaultdict(set)
    for code, day in raw_values:
        by_code[code].add(day)
    remaining: dict[str, set[str]] = {}
    anchors: dict[str, dict[str, float]] = {}
    frames: dict[str, pd.DataFrame] = {}
    target_groups: dict[tuple[str, str], str] = {}
    group_anchors: dict[str, dict[str, float]] = {}
    group_codes: dict[str, str] = {}
    group_meta: dict[str, dict[str, Any]] = {}
    for code in sorted(by_code):
        frame = _read_csv(code_path(data_dir, code))
        dates = _date_keys(frame["date"])
        existing = set(dates.dropna())
        missing = {day for day in by_code[code] if day not in existing}
        if not missing:
            continue
        frames[code] = frame
        remaining[code] = missing
        candidates = _valid_open_rows(frame)
        if candidates.empty:
            anchors[code] = {}
            continue
        candidate_days = candidates["date"].tolist()
        grouped_days: dict[tuple[str | None, str | None], set[str]] = defaultdict(set)
        for day in sorted(missing):
            position = int(np.searchsorted(candidate_days, day))
            left = candidate_days[position - 1] if position else None
            right = candidate_days[position] if position < len(candidate_days) else None
            grouped_days[(left, right)].add(day)

        code_anchors: dict[str, float] = {}
        for group_number, ((left, right), days) in enumerate(
            sorted(grouped_days.items(), key=lambda item: min(item[1])), 1
        ):
            if left is None:
                selected = candidates.head(anchor_window)
                kind = "leading"
            elif right is None:
                selected = candidates.tail(anchor_window)
                kind = "trailing"
            else:
                right_position = candidate_days.index(right)
                start = max(0, right_position - local_anchor_window)
                stop = min(len(candidates), right_position + local_anchor_window)
                selected = candidates.iloc[start:stop]
                kind = "internal"
            group_id = f"{code}:{group_number}:{left or 'START'}:{right or 'END'}"
            expected = dict(
                zip(selected["date"].astype(str), selected["open"].astype(float))
            )
            group_anchors[group_id] = expected
            group_codes[group_id] = code
            group_meta[group_id] = {
                "code": code,
                "kind": kind,
                "left": left,
                "right": right,
                "target_count": len(days),
                "first_target": min(days),
                "last_target": max(days),
            }
            code_anchors.update(expected)
            for day in days:
                target_groups[(code, day)] = group_id
        anchors[code] = code_anchors
    return (
        remaining,
        anchors,
        frames,
        target_groups,
        group_anchors,
        group_codes,
        group_meta,
    )


def fetch_adjusted_opens(
    targets_by_code: dict[str, set[str]],
    anchors_by_code: dict[str, dict[str, float]],
    *,
    code_date_batch: int,
    min_interval: float,
) -> tuple[dict[tuple[str, str], float], dict[str, Any]]:
    from thsdk import THS

    allowed_pairs = {
        (code, day)
        for code, days in targets_by_code.items()
        for day in set(days).union(anchors_by_code.get(code, {}))
    }
    logging.disable(logging.CRITICAL)
    client = THS()
    connected = client.connect()
    if not connected.success:
        raise RuntimeError(f"THSDK connect failed: {connected.error}")
    values: dict[tuple[str, str], float] = {}
    failed_queries: list[dict[str, str]] = []
    last_query_at = 0.0
    queries = 0

    def run_query(code: str, days: list[str]) -> None:
        nonlocal queries, last_query_at
        expected = {(code, day) for day in days}
        fields = " ".join(f"{chinese_date(day)}后复权开盘价" for day in days)
        condition = f"{code} {fields}"
        wait = min_interval - (time.monotonic() - last_query_at)
        if wait > 0:
            time.sleep(wait)
        try:
            frame = query_with_retry(client, condition)
            values.update(parse_adjusted_opens(frame, allowed_pairs=expected))
        except Exception as exc:
            failed_queries.append(
                {"condition": condition[:500], "error": f"{type(exc).__name__}: {exc}"}
            )
        last_query_at = time.monotonic()
        queries += 1
        if queries == 1 or queries % 25 == 0:
            print(
                f"adjusted queries={queries} values={len(values)} failed={len(failed_queries)}",
                flush=True,
            )

    try:
        for code in sorted(targets_by_code):
            anchor_days = sorted(anchors_by_code.get(code, {}))
            for days in batched(anchor_days, code_date_batch):
                run_query(code, list(days))
            for days in batched(sorted(targets_by_code[code]), code_date_batch):
                run_query(code, list(days))
    finally:
        client.disconnect()
        logging.disable(logging.NOTSET)
    target_pairs = sum(len(days) for days in targets_by_code.values())
    returned_targets = sum(
        (code, day) in values for code, days in targets_by_code.items() for day in days
    )
    return values, {
        "target_pairs": target_pairs,
        "anchor_pairs": sum(len(values) for values in anchors_by_code.values()),
        "returned_target_pairs": int(returned_targets),
        "queries": queries,
        "failed_queries": failed_queries,
    }


def calibrate_adjusted_scale(
    anchors_by_code: dict[str, dict[str, float]],
    adjusted_values: dict[tuple[str, str], float],
    *,
    key_codes: dict[str, str] | None = None,
    relative_tolerance: float,
    absolute_tolerance: float,
    minimum_matches: int,
    minimum_return_ratio: float,
) -> tuple[dict[str, float | None], dict[str, dict[str, Any]]]:
    scales: dict[str, float | None] = {}
    details: dict[str, dict[str, Any]] = {}
    for key, expected in anchors_by_code.items():
        code = (key_codes or {}).get(key, key)
        available_values: list[tuple[str, float, float]] = []
        for day, committed in expected.items():
            fetched = adjusted_values.get((code, day))
            if fetched is not None and math.isfinite(float(fetched)) and float(fetched) > 0:
                available_values.append((day, float(committed), float(fetched)))

        requested = len(expected)
        required = max(
            int(minimum_matches),
            int(math.ceil(requested * float(minimum_return_ratio))),
        )
        scale = None
        if available_values:
            ratios = [committed / fetched for _, committed, fetched in available_values]
            candidate = float(np.median(ratios))
            if math.isfinite(candidate) and candidate > 0:
                scale = candidate

        comparisons = []
        for day, committed in expected.items():
            fetched = adjusted_values.get((code, day))
            available = fetched is not None and scale is not None
            calibrated = float(fetched) * scale if available else None
            relative_error = (
                abs(calibrated / float(committed) - 1.0) if available else None
            )
            matching = available and bool(
                np.isclose(
                    float(calibrated),
                    float(committed),
                    rtol=relative_tolerance,
                    atol=absolute_tolerance,
                )
            )
            comparisons.append(
                {
                    "date": day,
                    "committed": float(committed),
                    "wencai": fetched,
                    "calibrated": calibrated,
                    "relative_error": relative_error,
                    "available": fetched is not None,
                    "matching": matching,
                }
            )
        available = [item for item in comparisons if item["available"]]
        passed = (
            requested >= int(minimum_matches)
            and len(available) >= required
            and scale is not None
            and all(item["matching"] for item in available)
        )
        scales[key] = scale if passed else None
        details[key] = {
            "passed": passed,
            "scale": scale,
            "requested_anchors": requested,
            "required_matches": required,
            "available_anchors": len(available),
            "max_relative_error": max(
                (
                    float(item["relative_error"])
                    for item in available
                    if item["relative_error"] is not None
                ),
                default=None,
            ),
            "comparisons": comparisons,
        }
    return scales, details


def insert_adjusted_bars(
    current: pd.DataFrame,
    code: str,
    bars_by_day: dict[str, dict[str, float]],
    adjusted_by_day: dict[str, float],
    *,
    calibration_scale: float | None = None,
    calibration_scales_by_day: dict[str, float | None] | None = None,
    adjusted_close_by_day: dict[str, float] | None = None,
    factor_crosscheck_tolerance: float = 0.01,
    price_tolerance: float,
) -> tuple[pd.DataFrame, dict[str, int], list[dict[str, Any]]]:
    result = current.copy()
    stats: defaultdict[str, int] = defaultdict(int)
    details: list[dict[str, Any]] = []
    existing_dates = set(_date_keys(result["date"]).dropna())
    columns = list(result.columns)
    rows: list[dict[str, Any]] = []
    inserted_days: set[str] = set()
    for day, bar in sorted(bars_by_day.items()):
        day_scale = (
            (calibration_scales_by_day or {}).get(day)
            if calibration_scales_by_day is not None
            else calibration_scale
        )
        if day in existing_dates:
            stats["already_present"] += 1
            continue
        if day_scale is None:
            stats["rejected_anchor_validation"] += 1
            details.append({"code": code, "date": day, "status": "anchor_validation"})
            continue
        reason = validate_raw_bar(bar, price_tolerance)
        if reason:
            stats[f"rejected_{reason}"] += 1
            details.append({"code": code, "date": day, "status": reason})
            continue
        adjusted_open = adjusted_by_day.get(day)
        if adjusted_open is None or not math.isfinite(adjusted_open) or adjusted_open <= 0:
            stats["rejected_missing_adjusted_open"] += 1
            details.append({"code": code, "date": day, "status": "missing_adjusted_open"})
            continue
        calibrated_open = float(adjusted_open) * float(day_scale)
        unscaled_factor = float(adjusted_open) / float(bar["open_raw"])
        adjusted_close = (adjusted_close_by_day or {}).get(day)
        factor_gap = None
        if adjusted_close is not None:
            close_factor = float(adjusted_close) / float(bar["close_raw"])
            factor_gap = abs(unscaled_factor / close_factor - 1.0)
            if factor_gap > factor_crosscheck_tolerance:
                stats["rejected_adjusted_open_close_factor_mismatch"] += 1
                details.append(
                    {
                        "code": code,
                        "date": day,
                        "status": "adjusted_open_close_factor_mismatch",
                        "factor_gap": factor_gap,
                    }
                )
                continue
            stats["adjusted_open_close_crosschecked"] += 1
        factor = unscaled_factor * float(day_scale)
        if not math.isfinite(factor) or factor <= 0:
            stats["rejected_invalid_adjustment_factor"] += 1
            details.append({"code": code, "date": day, "status": "invalid_adjustment_factor"})
            continue
        row = build_inserted_row(columns, day, bar, factor)
        row["open"] = round(calibrated_open, 3)
        rows.append(row)
        inserted_days.add(day)
        stats["inserted"] += 1
        details.append(
            {
                "code": code,
                "date": day,
                "status": "inserted",
                "factor": factor,
                "open_close_factor_gap": factor_gap,
                "calibration_scale": day_scale,
            }
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


def write_adjusted_results(values: dict[tuple[str, str], float], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        [
            {"code": code, "date": day, "open_adjusted": value}
            for (code, day), value in sorted(values.items())
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


def read_adjusted_results(path: Path) -> dict[tuple[str, str], float]:
    frame = pd.read_csv(path, encoding="utf-8-sig", dtype={"code": str, "date": str})
    frame["code"] = frame["code"].astype(str).str.zfill(6)
    if "open_adjusted" not in frame.columns:
        raise ValueError(f"adjusted-open result missing open_adjusted column: {path}")
    frame["open_adjusted"] = pd.to_numeric(frame["open_adjusted"], errors="coerce")
    valid = frame["open_adjusted"].notna() & frame["open_adjusted"].gt(0)
    return {
        (str(row.code), str(row.date)): float(row.open_adjusted)
        for row in frame.loc[valid].itertuples(index=False)
    }


def read_adjusted_close_results(path: Path) -> dict[tuple[str, str], float]:
    if not path.exists():
        return {}
    frame = pd.read_csv(path, encoding="utf-8-sig", dtype={"code": str, "date": str})
    if "close_adjusted" not in frame.columns:
        return {}
    frame["code"] = frame["code"].astype(str).str.zfill(6)
    frame["close_adjusted"] = pd.to_numeric(frame["close_adjusted"], errors="coerce")
    valid = frame["close_adjusted"].notna() & frame["close_adjusted"].gt(0)
    return {
        (str(row.code), str(row.date)): float(row.close_adjusted)
        for row in frame.loc[valid].itertuples(index=False)
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument(
        "--raw-results",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_bars.csv",
    )
    parser.add_argument(
        "--adjusted-results",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_bars_adjusted_interval_open.csv",
    )
    parser.add_argument(
        "--report",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_bars_adjusted_interval_report.json",
    )
    parser.add_argument(
        "--adjusted-close-results",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_bars_adjusted.csv",
    )
    parser.add_argument(
        "--backup-dir",
        default="artifacts/maintenance/all_data_gaps/missing_bars_adjusted_interval_backup",
    )
    parser.add_argument("--code-date-batch", type=int, default=40)
    parser.add_argument("--min-interval", type=float, default=0.25)
    parser.add_argument("--anchor-relative-tolerance", type=float, default=0.01)
    parser.add_argument("--anchor-absolute-tolerance", type=float, default=0.001)
    parser.add_argument("--anchor-window", type=int, default=20)
    parser.add_argument("--local-anchor-window", type=int, default=10)
    parser.add_argument("--minimum-anchor-matches", type=int, default=10)
    parser.add_argument("--minimum-anchor-return-ratio", type=float, default=0.5)
    parser.add_argument("--factor-crosscheck-tolerance", type=float, default=0.01)
    parser.add_argument("--price-tolerance", type=float, default=0.001)
    parser.add_argument("--reuse-results", action="store_true")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.code_date_batch > 40:
        raise ValueError("adjusted-open Wencai batches above 40 are not validated")
    data_dir = Path(args.data_dir).resolve()
    raw_path = (ROOT / args.raw_results).resolve()
    adjusted_path = (ROOT / args.adjusted_results).resolve()
    adjusted_close_path = (ROOT / args.adjusted_close_results).resolve()
    backup_root = (ROOT / args.backup_dir).resolve()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = backup_root / run_id
    started = time.time()
    raw_values = read_raw_results(raw_path)
    adjusted_close_values = read_adjusted_close_results(adjusted_close_path)
    (
        targets,
        anchors,
        frames,
        target_groups,
        group_anchors,
        group_codes,
        group_meta,
    ) = remaining_targets(
        data_dir,
        raw_values,
        anchor_window=args.anchor_window,
        local_anchor_window=args.local_anchor_window,
    )
    source_signatures = {
        code: file_signature(code_path(data_dir, code)) for code in targets
    }
    print(
        f"prepared codes={len(targets)} targets={sum(len(days) for days in targets.values())} "
        f"anchors={sum(len(values) for values in anchors.values())}",
        flush=True,
    )
    if args.reuse_results:
        adjusted_values = read_adjusted_results(adjusted_path)
        fetch = {
            "target_pairs": sum(len(days) for days in targets.values()),
            "anchor_pairs": sum(len(values) for values in anchors.values()),
            "reused_results": True,
            "failed_queries": [],
        }
    else:
        adjusted_values, fetch = fetch_adjusted_opens(
            targets,
            anchors,
            code_date_batch=args.code_date_batch,
            min_interval=args.min_interval,
        )
        write_adjusted_results(adjusted_values, adjusted_path)

    calibration_scales, anchor_details = calibrate_adjusted_scale(
        group_anchors,
        adjusted_values,
        key_codes=group_codes,
        relative_tolerance=args.anchor_relative_tolerance,
        absolute_tolerance=args.anchor_absolute_tolerance,
        minimum_matches=args.minimum_anchor_matches,
        minimum_return_ratio=args.minimum_anchor_return_ratio,
    )
    raw_by_code: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for (code, day), bar in raw_values.items():
        if code in targets and day in targets[code]:
            raw_by_code[code][day] = bar
    adjusted_by_code: dict[str, dict[str, float]] = defaultdict(dict)
    for (code, day), value in adjusted_values.items():
        adjusted_by_code[code][day] = value
    adjusted_close_by_code: dict[str, dict[str, float]] = defaultdict(dict)
    for (code, day), value in adjusted_close_values.items():
        adjusted_close_by_code[code][day] = value
    calibration_by_code_day: dict[str, dict[str, float | None]] = defaultdict(dict)
    for (code, day), group_id in target_groups.items():
        calibration_by_code_day[code][day] = calibration_scales.get(group_id)

    aggregate: defaultdict[str, int] = defaultdict(int)
    details: list[dict[str, Any]] = []
    pending_writes: dict[str, pd.DataFrame] = {}
    for code in sorted(raw_by_code):
        current = frames[code]
        merged, stats, rows = insert_adjusted_bars(
            current,
            code,
            raw_by_code[code],
            adjusted_by_code.get(code, {}),
            calibration_scales_by_day=calibration_by_code_day.get(code, {}),
            adjusted_close_by_day=adjusted_close_by_code.get(code, {}),
            factor_crosscheck_tolerance=args.factor_crosscheck_tolerance,
            price_tolerance=args.price_tolerance,
        )
        for key, value in stats.items():
            aggregate[key] += value
        details.extend(rows)
        if stats.get("inserted", 0):
            pending_writes[code] = merged

    source_conflicts: list[str] = []
    if args.apply and not fetch.get("failed_queries"):
        for code in pending_writes:
            path = code_path(data_dir, code)
            if file_signature(path) != source_signatures[code]:
                source_conflicts.append(code)
    effective_apply = bool(
        args.apply and not fetch.get("failed_queries") and not source_conflicts
    )
    applied_files = 0
    if effective_apply:
        for code, merged in pending_writes.items():
            path = code_path(data_dir, code)
            relative = path.relative_to(data_dir)
            atomic_backup(path, backup_dir / relative)
            _atomic_csv(merged, path)
            invalidate_caches(data_dir, code)
            applied_files += 1

    partial = bool(fetch.get("failed_queries") or source_conflicts)

    report = {
        "status": "PARTIAL" if partial else "COMPLETED",
        "requested_apply": bool(args.apply),
        "applied": effective_apply,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "THSDK wencai_nlp post-adjusted open + first-stage THS raw bars",
        "data_dir": str(data_dir),
        "raw_results": str(raw_path),
        "adjusted_results": str(adjusted_path),
        "adjusted_close_crosscheck_results": str(adjusted_close_path),
        "backup_dir": str(backup_dir),
        "source_conflicts": source_conflicts,
        "fetch": fetch,
        "anchor_policy": {
            "relative_tolerance": args.anchor_relative_tolerance,
            "absolute_tolerance": args.anchor_absolute_tolerance,
            "anchor_window": args.anchor_window,
            "local_anchor_window": args.local_anchor_window,
            "minimum_matches": args.minimum_anchor_matches,
            "minimum_return_ratio": args.minimum_anchor_return_ratio,
            "factor_crosscheck_tolerance": args.factor_crosscheck_tolerance,
            "passed_groups": sum(
                scale is not None for scale in calibration_scales.values()
            ),
            "failed_groups": sorted(
                group_id
                for group_id, scale in calibration_scales.items()
                if scale is None
            ),
            "groups": group_meta,
            "details": anchor_details,
        },
        "changed_files": len(pending_writes),
        "applied_files": applied_files,
        "counts": dict(aggregate),
        "details": details,
        "elapsed_seconds": round(time.time() - started, 3),
    }
    report_path = (ROOT / args.report).resolve()
    _atomic_json(report, report_path)
    if effective_apply:
        _update_manifest(
            data_dir,
            "historical_missing_bars_ths_wencai_adjusted_intervals",
            {
                "source": "THSDK wencai_nlp post-adjusted open",
                "candidate_pairs": int(fetch["target_pairs"]),
                "inserted": int(aggregate["inserted"]),
                "anchor_relative_tolerance": args.anchor_relative_tolerance,
                "anchor_absolute_tolerance": args.anchor_absolute_tolerance,
                "anchor_window": args.anchor_window,
                "local_anchor_window": args.local_anchor_window,
                "minimum_anchor_matches": args.minimum_anchor_matches,
                "minimum_anchor_return_ratio": args.minimum_anchor_return_ratio,
                "report": str(report_path),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"counts={dict(aggregate)} failed_queries={len(fetch.get('failed_queries', []))}", flush=True)
    return 0 if not partial else 2


if __name__ == "__main__":
    raise SystemExit(main())
