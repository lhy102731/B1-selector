"""Repair isolated amount gaps with exact-date THS Wencai trade pairs.

Stable unit regimes are handled by ``repair_historical_trade_units.py``.  This
tool handles the remaining isolated rows by fetching THS Wencai ``成交量`` and
``成交额`` together.  A pair is accepted only when its VWAP lies inside the THS
raw OHLC envelope.  If Wencai does not return a usable volume, an exact legacy
volume may be used only when the legacy amount agrees with the Wencai amount
and the resulting pair passes the same raw-price gate.
"""

from __future__ import annotations

import argparse
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

from tools.backfill_missing_amount import valid_positive
from tools.backfill_missing_amount_ths import batched, chinese_date, query_with_retry
from tools.backfill_missing_bars_ths import invalidate_caches
from tools.backfill_valuation_fields import (
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _read_csv,
    _update_manifest,
    code_path,
    stock_files,
)
from utils.ths_data_source import THSDataSource


def identify_targets(data_dir: Path) -> set[tuple[str, str]]:
    targets: set[tuple[str, str]] = set()
    for path in stock_files(data_dir):
        frame = _read_csv(path)
        if frame.empty or not {"date", "volume", "amount"}.issubset(frame.columns):
            continue
        dates = _date_keys(frame["date"])
        volume = pd.to_numeric(frame["volume"], errors="coerce")
        missing = dates.notna() & volume.gt(0) & ~valid_positive(frame["amount"])
        targets.update((path.stem, str(day)) for day in dates.loc[missing].unique())
    return targets


def parse_trade_pairs(
    frame: pd.DataFrame,
    *,
    allowed: set[tuple[str, str]],
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
        text = str(column)
        field = "volume" if text.startswith("成交量[") else "amount" if text.startswith("成交额[") else None
        if field is None or not text.endswith("]"):
            continue
        parsed = pd.to_datetime(text[text.rfind("[") + 1 : -1], format="%Y%m%d", errors="coerce")
        if pd.notna(parsed):
            mapped[(field, pd.Timestamp(parsed).strftime("%Y-%m-%d"))] = column
    output: dict[tuple[str, str], dict[str, float]] = {}
    for _, row in frame.iterrows():
        code = str(row[code_column]).strip()[:6].zfill(6)
        if not code.isdigit():
            continue
        for (field, day), column in mapped.items():
            key = (code, day)
            if key not in allowed:
                continue
            value = pd.to_numeric(pd.Series([row[column]]), errors="coerce").iloc[0]
            if pd.notna(value) and math.isfinite(float(value)) and float(value) > 0:
                output.setdefault(key, {})[field] = float(value)
    return output


def fetch_trade_pairs(
    targets: set[tuple[str, str]],
    *,
    wide_date_min_codes: int,
    code_date_batch: int,
    min_interval: float,
) -> tuple[dict[tuple[str, str], dict[str, float]], dict[str, Any]]:
    from thsdk import THS

    by_date: dict[str, set[str]] = defaultdict(set)
    for code, day in targets:
        by_date[day].add(code)
    wide_dates = sorted(
        day for day, codes in by_date.items() if len(codes) >= wide_date_min_codes
    )
    client = THS()
    logging.disable(logging.CRITICAL)
    connected = client.connect()
    if not connected.success:
        raise RuntimeError(f"THSDK connect failed: {connected.error}")
    values: dict[tuple[str, str], dict[str, float]] = {}
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
            parsed = parse_trade_pairs(frame, allowed=expected)
            for key, pair in parsed.items():
                values.setdefault(key, {}).update(pair)
        except Exception as exc:
            failed_queries.append(
                {"condition": condition[:500], "error": f"{type(exc).__name__}: {exc}"}
            )
        last_query_at = time.monotonic()
        queries += 1
        if queries == 1 or queries % 25 == 0:
            print(f"trade-pair queries={queries} values={len(values)} failed={len(failed_queries)}", flush=True)

    try:
        for day in wide_dates:
            expected = {(code, day) for code in by_date[day]}
            run_query(f"{chinese_date(day)} A股 成交量 成交额", expected)
        incomplete = {
            key for key in targets if not {"volume", "amount"}.issubset(values.get(key, {}))
        }
        by_code: dict[str, list[str]] = defaultdict(list)
        for code, day in incomplete:
            by_code[code].append(day)
        for code in sorted(by_code):
            for days in batched(sorted(set(by_code[code])), code_date_batch):
                expected = {(code, day) for day in days}
                terms = " ".join(
                    f"{chinese_date(day)}成交量 {chinese_date(day)}成交额" for day in days
                )
                run_query(f"{code} {terms}", expected)
    finally:
        client.disconnect()
        logging.disable(logging.NOTSET)
    return values, {
        "target_pairs": len(targets),
        "target_dates": len(by_date),
        "wide_dates": len(wide_dates),
        "queries": queries,
        "complete_pairs": sum(
            {"volume", "amount"}.issubset(pair) for pair in values.values()
        ),
        "failed_queries": failed_queries,
    }


def raw_price_envelope(row: pd.Series) -> tuple[float, float] | None:
    values = pd.to_numeric(
        pd.Series(
            {
                "low": row.get("low"),
                "high": row.get("high"),
                "close": row.get("close"),
                "close_raw": row.get("close_raw"),
            }
        ),
        errors="coerce",
    )
    if values.isna().any() or (values <= 0).any():
        return None
    factor = values["close"] / values["close_raw"]
    return float(values["low"] / factor), float(values["high"] / factor)


def pair_is_compatible(
    row: pd.Series,
    *,
    volume: float,
    amount: float,
    price_tolerance: float,
) -> bool:
    envelope = raw_price_envelope(row)
    if envelope is None or volume <= 0 or amount <= 0:
        return False
    low, high = envelope
    vwap = amount / volume
    return bool(
        vwap >= low * (1.0 - price_tolerance)
        and vwap <= high * (1.0 + price_tolerance)
    )


def normalise_trade_pair(
    row: pd.Series,
    *,
    volume: float,
    amount: float,
    price_tolerance: float,
) -> tuple[float, float] | None:
    """Return the uniquely price-compatible volume and its source multiplier.

    A few early exchange archives expose volume in a historical lot basis while
    amount remains in yuan.  The main THS adapter repairs only persistent
    regimes; isolated rows separated by suspended days deliberately remain
    unresolved there.  For an exact-date pair we can apply the same finite unit
    vocabulary, but only when exactly one multiplier makes VWAP compatible with
    the independently fetched raw OHLC envelope.
    """
    multipliers = tuple(
        dict.fromkeys(
            (
                1.0,
                *THSDataSource.VOLUME_UNIT_CANDIDATES,
                *(1.0 / factor for factor in THSDataSource.VOLUME_UNIT_CANDIDATES),
            )
        )
    )
    candidates: list[tuple[float, float]] = []
    for multiplier in multipliers:
        normalised_volume = float(volume) * float(multiplier)
        if pair_is_compatible(
            row,
            volume=normalised_volume,
            amount=float(amount),
            price_tolerance=price_tolerance,
        ):
            candidates.append((normalised_volume, float(multiplier)))
    if len(candidates) != 1:
        return None
    return candidates[0]


def repair_frame(
    current: pd.DataFrame,
    legacy: pd.DataFrame,
    ths_pairs: dict[str, dict[str, float]],
    *,
    price_tolerance: float,
    amount_match_rtol: float,
    amount_match_atol: float,
) -> tuple[pd.DataFrame, dict[str, int]]:
    result = current.copy()
    counts: defaultdict[str, int] = defaultdict(int)
    dates = _date_keys(result["date"])
    old = None
    if not legacy.empty and {"date", "volume", "amount"}.issubset(legacy.columns):
        old = legacy.assign(_date=_date_keys(legacy["date"]))
        old = old.dropna(subset=["_date"]).drop_duplicates("_date", keep="last").set_index("_date")
    for day, pair in ths_pairs.items():
        mask = dates.eq(day)
        if not mask.any():
            counts["target_date_missing"] += 1
            continue
        row_index = result.index[mask][0]
        row = result.loc[row_index]
        current_volume = pd.to_numeric(pd.Series([row.get("volume")]), errors="coerce").iloc[0]
        current_amount = pd.to_numeric(pd.Series([row.get("amount")]), errors="coerce").iloc[0]
        if pd.notna(current_amount) and float(current_amount) > 0:
            counts["already_valid"] += 1
            continue
        ths_volume = pair.get("volume")
        ths_amount = pair.get("amount")
        selected_volume: float | None = None
        selected_amount: float | None = None
        source: str | None = None
        ths_candidate = (
            normalise_trade_pair(
                row,
                volume=float(ths_volume),
                amount=float(ths_amount),
                price_tolerance=price_tolerance,
            )
            if ths_volume is not None and ths_amount is not None
            else None
        )
        volume_multiplier = 1.0
        if ths_candidate is not None:
            selected_volume, volume_multiplier = ths_candidate
            selected_amount = float(ths_amount)
            source = (
                "ths_pair"
                if np.isclose(volume_multiplier, 1.0, rtol=0.0, atol=1e-12)
                else "ths_pair_normalised_volume"
            )
        elif old is not None and day in old.index:
            old_volume = pd.to_numeric(pd.Series([old.at[day, "volume"]]), errors="coerce").iloc[0]
            old_amount = pd.to_numeric(pd.Series([old.at[day, "amount"]]), errors="coerce").iloc[0]
            legacy_valid = (
                pd.notna(old_volume)
                and pd.notna(old_amount)
                and float(old_volume) > 0
                and float(old_amount) > 0
            )
            amount_agrees = ths_amount is None or bool(
                np.isclose(
                    float(ths_amount),
                    float(old_amount),
                    rtol=amount_match_rtol,
                    atol=amount_match_atol,
                )
            )
            candidate_amount = float(ths_amount) if ths_amount is not None else float(old_amount)
            legacy_candidate = (
                normalise_trade_pair(
                    row,
                    volume=float(old_volume),
                    amount=candidate_amount,
                    price_tolerance=price_tolerance,
                )
                if legacy_valid and amount_agrees
                else None
            )
            if legacy_candidate is not None:
                selected_volume, volume_multiplier = legacy_candidate
                selected_amount = candidate_amount
                base_source = "legacy_volume_ths_amount" if ths_amount is not None else "legacy_pair"
                source = (
                    base_source
                    if np.isclose(volume_multiplier, 1.0, rtol=0.0, atol=1e-12)
                    else f"{base_source}_normalised_volume"
                )
        if source is None or selected_volume is None or selected_amount is None:
            counts["unresolved"] += 1
            continue

        result.at[row_index, "amount"] = selected_amount
        volume_changed = pd.isna(current_volume) or not np.isclose(
            float(current_volume), selected_volume, rtol=1e-12, atol=1e-9
        )
        if volume_changed:
            result.at[row_index, "volume"] = selected_volume
            raw_close = pd.to_numeric(pd.Series([row.get("close_raw")]), errors="coerce").iloc[0]
            market_cap = pd.to_numeric(pd.Series([row.get("market_cap")]), errors="coerce").iloc[0]
            turnover = pd.to_numeric(pd.Series([row.get("turnover")]), errors="coerce").iloc[0]
            shares = None
            if pd.notna(raw_close) and float(raw_close) > 0 and pd.notna(market_cap) and float(market_cap) > 0:
                shares = float(market_cap) / float(raw_close)
            elif pd.notna(current_volume) and float(current_volume) > 0 and pd.notna(turnover) and float(turnover) > 0:
                shares = float(current_volume) * 100.0 / float(turnover)
            if shares is None or shares <= 0:
                counts["rejected_missing_share_anchor"] += 1
                result.at[row_index, "amount"] = current_amount
                result.at[row_index, "volume"] = current_volume
                continue
            result.at[row_index, "turnover"] = selected_volume * 100.0 / shares
            counts["volume_changed"] += 1
            counts["turnover_recomputed"] += 1
        counts[f"filled_{source}"] += 1
        if not np.isclose(volume_multiplier, 1.0, rtol=0.0, atol=1e-12):
            multiplier_key = f"{volume_multiplier:.12g}".replace(".", "p").replace("-", "m")
            counts[f"volume_multiplier_{multiplier_key}"] += 1
    return result, dict(counts)


def write_results(values: dict[tuple[str, str], dict[str, float]], path: Path) -> None:
    frame = pd.DataFrame(
        [
            {"code": code, "date": day, **pair}
            for (code, day), pair in sorted(values.items())
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
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
    for row in frame.itertuples(index=False):
        pair = {}
        for field in ("volume", "amount"):
            value = pd.to_numeric(pd.Series([getattr(row, field, None)]), errors="coerce").iloc[0]
            if pd.notna(value) and float(value) > 0:
                pair[field] = float(value)
        output[(str(row.code).zfill(6), str(row.date))] = pair
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--legacy-dir", default="data_pre_ths_backup_20260727_110350")
    parser.add_argument("--wide-date-min-codes", type=int, default=100)
    parser.add_argument("--code-date-batch", type=int, default=20)
    parser.add_argument("--min-interval", type=float, default=0.25)
    parser.add_argument("--price-tolerance", type=float, default=0.02)
    parser.add_argument("--amount-match-rtol", type=float, default=0.001)
    parser.add_argument("--amount-match-atol", type=float, default=1000.0)
    parser.add_argument("--reuse-results", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--results", default="artifacts/maintenance/all_data_gaps/ths_wencai_remaining_trade_pairs.csv")
    parser.add_argument("--report", default="artifacts/maintenance/all_data_gaps/ths_wencai_remaining_trade_pairs_report.json")
    parser.add_argument("--backup-dir", default="artifacts/maintenance/all_data_gaps/remaining_trade_pair_backup")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.code_date_batch > 20:
        raise ValueError("two-field Wencai batches above 20 dates are not validated")
    data_dir = Path(args.data_dir).resolve()
    legacy_dir = Path(args.legacy_dir).resolve()
    results_path = (ROOT / args.results).resolve()
    report_path = (ROOT / args.report).resolve()
    backup_dir = (ROOT / args.backup_dir).resolve()
    started = time.time()
    targets = identify_targets(data_dir)
    if args.reuse_results:
        values = read_results(results_path)
        fetch = {"target_pairs": len(targets), "reused_results": True, "failed_queries": []}
    else:
        values, fetch = fetch_trade_pairs(
            targets,
            wide_date_min_codes=args.wide_date_min_codes,
            code_date_batch=args.code_date_batch,
            min_interval=args.min_interval,
        )
        write_results(values, results_path)

    by_code: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for code, day in targets:
        by_code[code][day] = values.get((code, day), {})
    totals: defaultdict[str, int] = defaultdict(int)
    changed_files = 0
    for code in sorted({code for code, _ in targets}):
        path = code_path(data_dir, code)
        current = _read_csv(path)
        legacy = _read_csv(code_path(legacy_dir, code))
        repaired, counts = repair_frame(
            current,
            legacy,
            by_code.get(code, {}),
            price_tolerance=args.price_tolerance,
            amount_match_rtol=args.amount_match_rtol,
            amount_match_atol=args.amount_match_atol,
        )
        for key, value in counts.items():
            totals[key] += value
        changed = sum(value for key, value in counts.items() if key.startswith("filled_")) > 0
        if changed:
            changed_files += 1
            if args.apply:
                relative = path.relative_to(data_dir)
                backup_path = backup_dir / relative
                backup_path.parent.mkdir(parents=True, exist_ok=True)
                if not backup_path.exists():
                    shutil.copy2(path, backup_path)
                _atomic_csv(repaired, path)
                invalidate_caches(data_dir, code)
    unresolved_without_pair = len(targets - set(values))
    report = {
        "status": "COMPLETED" if not fetch.get("failed_queries") else "PARTIAL",
        "applied": bool(args.apply),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_priority": ["THSDK Wencai exact volume+amount", "audited exact-date legacy pair"],
        "data_dir": str(data_dir),
        "legacy_dir": str(legacy_dir),
        "results": str(results_path),
        "backup_dir": str(backup_dir),
        "fetch": fetch,
        "changed_files": changed_files,
        "counts": dict(totals),
        "unresolved_without_pair": unresolved_without_pair,
        "policy": {
            "raw_ohlc_price_tolerance": args.price_tolerance,
            "legacy_amount_match_rtol": args.amount_match_rtol,
            "legacy_amount_match_atol": args.amount_match_atol,
        },
        "elapsed_seconds": round(time.time() - started, 3),
    }
    _atomic_json(report, report_path)
    if args.apply and not fetch.get("failed_queries"):
        _update_manifest(
            data_dir,
            "isolated_trade_pairs_ths_then_legacy",
            {
                "source_priority": report["source_priority"],
                "counts": dict(totals),
                "policy": report["policy"],
                "report": str(report_path),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"counts={dict(totals)} unresolved_without_pair={unresolved_without_pair}", flush=True)
    return 0 if not fetch.get("failed_queries") else 2


if __name__ == "__main__":
    raise SystemExit(main())
