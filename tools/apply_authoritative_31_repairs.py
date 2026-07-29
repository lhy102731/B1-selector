"""Apply the user's authoritative adjudication for the final 31 history rows.

The source table reports volume in hands.  Project CSVs store traded shares,
so ordinary rows are converted with 100 shares per hand.  The three explicitly
adjudicated tenfold archive-unit rows use their corrected hand counts before
the same conversion.  Every retained row must pass an exact raw VWAP envelope
gate before any file is written.
"""

from __future__ import annotations

import argparse
import math
import shutil
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_missing_bars_ths import invalidate_caches
from tools.backfill_valuation_fields import (
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _read_csv,
    _update_manifest,
    code_path,
)


@dataclass(frozen=True)
class Correction:
    code: str
    day: str
    raw_open: float
    raw_high: float
    raw_low: float
    raw_close: float
    expected_old_volume: float
    volume_shares: float
    amount_yuan: float
    adjusted_low: float
    adjusted_high: float
    float_shares: float | None = None
    turnover_override: float | None = None
    adjusted_open: float | None = None
    adjusted_close: float | None = None


@dataclass(frozen=True)
class Deletion:
    code: str
    day: str
    expected_raw_close: float
    expected_volume: float
    reason: str


CORRECTIONS: tuple[Correction, ...] = (
    Correction("000002", "1991-06-08", 7.80, 8.12, 7.75, 7.95, 500, 50_000, 394_650, 11.08, 12.15),
    Correction("000002", "1991-06-15", 6.80, 7.05, 6.72, 6.90, 8_000, 800_000, 5_512_800, 9.42, 10.45),
    Correction("000002", "1991-06-22", 6.40, 6.62, 6.35, 6.50, 17_700, 1_770_000, 11_502_150, 9.09, 9.71),
    Correction("000002", "1991-06-29", 6.48, 6.65, 6.42, 6.55, 13_800, 1_380_000, 9_027_240, 9.45, 9.75),
    Correction("000002", "1991-07-06", 6.00, 6.21, 5.95, 6.10, 18_300, 1_830_000, 11_102_850, 8.53, 8.86),
    Correction("000002", "1991-07-13", 4.90, 5.12, 4.85, 5.00, 19_100, 1_910_000, 9_574_600, 6.68, 7.03),
    Correction("000002", "1991-07-20", 5.40, 5.65, 5.32, 5.50, 40_600, 4_060_000, 22_393_700, 7.52, 7.81),
    Correction(
        "000002", "1991-07-27", 5.10, 5.30, 5.05, 5.20,
        12_600, 1_260_000, 6_532_800, 7.18, 7.36,
        adjusted_open=7.216, adjusted_close=7.288,
    ),
    Correction(
        "000002", "1991-08-03", 5.20, 5.35, 5.10, 5.25,
        48_500, 4_850_000, 25_421_250, 7.22, 7.40,
        adjusted_open=7.292, adjusted_close=7.328,
    ),
    Correction("000002", "1991-08-17", 4.95, 5.10, 4.82, 5.00, 48_000, 4_800_000, 24_024_000, 6.75, 6.97),
    Correction("000002", "1991-08-24", 5.00, 5.18, 4.92, 5.09, 65_000, 6_500_000, 33_108_850, 6.83, 7.05),
    Correction("000002", "1991-08-31", 4.90, 5.05, 4.80, 4.95, 5_200, 520_000, 2_562_200, 6.58, 6.78),
    Correction("000002", "1991-11-16", 14.50, 15.10, 14.30, 14.80, 158_400, 15_840_000, 233_913_600, 23.25, 24.80),
    Correction("000004", "1991-06-29", 5.10, 5.22, 5.05, 5.15, 700, 70_000, 360_800, 6.12, 6.79),
    Correction("000004", "1991-07-06", 4.75, 4.88, 4.70, 4.80, 3_500, 350_000, 1_688_750, 5.47, 5.82),
    Correction("000004", "1991-07-13", 3.80, 3.92, 3.78, 3.85, 1_500, 150_000, 574_500, 4.51, 4.73),
    Correction("000004", "1991-07-20", 4.32, 4.48, 4.28, 4.40, 6_700, 670_000, 2_958_440, 4.68, 5.34),
    Correction("000004", "1991-07-27", 3.95, 4.08, 3.90, 4.00, 4_100, 410_000, 1_636_000, 4.75, 4.98),
    Correction("000004", "1991-08-01", 4.98, 5.12, 4.90, 5.05, 3_200, 320_000, 1_617_600, 5.81, 6.18),
    Correction("000004", "1991-11-16", 14.60, 14.92, 14.45, 14.75, 13_900, 1_390_000, 20_457_050, 17.49, 18.62),
    Correction("000004", "1991-11-17", 16.95, 17.30, 16.80, 17.10, 17_600, 1_760_000, 30_093_600, 20.26, 20.71),
    Correction("600601", "1990-12-24", 214.0, 214.6, 214.2, 214.5, 100, 10_000, 2_143_500, 214.35, 214.72),
    Correction("600602", "1990-12-24", 423.4, 445.2, 430.1, 444.6, 1_100, 110_000, 48_963_500, 422.8, 446.1),
    Correction("600608", "1992-10-28", 68.2, 71.5, 67.1, 70.0, 276_400, 2_764_000, 193_221_800, 66.9, 72.3),
    Correction("600608", "1993-09-08", 8.75, 9.02, 8.68, 8.88, 98_700, 987_000, 8_750_160, 14.96, 15.95),
    Correction(
        "002042", "2006-12-06", 3.44, 3.47, 3.30, 3.33,
        2_147_483_648, 219_700_000, 740_495_000, 4.415, 4.452,
        float_shares=219_700_000, turnover_override=100.0,
    ),
)


DELETIONS: tuple[Deletion, ...] = (
    Deletion("000016", "1992-10-04", 23.70, 51_900, "adjudicated non-trading Sunday"),
    Deletion("000020", "1992-10-04", 15.95, 30_300, "adjudicated non-trading Sunday"),
    Deletion("000529", "2001-01-01", 7.79, 996_500, "adjudicated New Year closure"),
    Deletion("000681", "2001-01-01", 20.76, 255_600, "adjudicated New Year closure"),
    Deletion("600602", "1990-12-21", 423.40, 1_100, "explicit user deletion adjudication"),
)


def _number(value: Any) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(parsed) if pd.notna(parsed) else math.nan


def _same(left: Any, right: float, *, atol: float = 1e-9) -> bool:
    value = _number(left)
    return math.isfinite(value) and bool(np.isclose(value, right, rtol=0.0, atol=atol))


def _set_number(frame: pd.DataFrame, index: Any, column: str, value: float) -> bool:
    old = _number(frame.at[index, column])
    if math.isfinite(old) and np.isclose(old, value, rtol=0.0, atol=1e-9):
        return False
    frame.at[index, column] = float(value)
    return True


def _expected_derived(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    work["_parsed_date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.sort_values("_parsed_date", kind="stable")
    close = pd.to_numeric(work["close"], errors="coerce")
    high = pd.to_numeric(work["high"], errors="coerce")
    low = pd.to_numeric(work["low"], errors="coerce")
    previous = close.shift(1)
    expected = pd.DataFrame(index=work.index)
    expected["change"] = close - previous
    expected["change_pct"] = (close / previous - 1.0) * 100.0
    expected["amplitude"] = (high - low) / previous * 100.0
    invalid = previous.isna() | previous.le(0)
    expected.loc[invalid, ["change", "change_pct", "amplitude"]] = np.nan
    return expected


def repair_frame(
    code: str,
    current: pd.DataFrame,
    *,
    corrections: tuple[Correction, ...] = CORRECTIONS,
    deletions: tuple[Deletion, ...] = DELETIONS,
) -> tuple[pd.DataFrame, Counter, list[dict[str, Any]], list[str]]:
    result = current.copy()
    counts: Counter = Counter()
    details: list[dict[str, Any]] = []
    errors: list[str] = []
    required = {
        "date", "open", "high", "low", "close", "close_raw", "volume",
        "amount", "turnover", "market_cap", "change", "change_pct", "amplitude",
    }
    if result.empty or not required.issubset(result.columns):
        return result, counts, details, [f"{code}: invalid current schema"]

    dates = _date_keys(result["date"])
    deleted_days: list[str] = []
    correction_days: list[str] = []
    close_changed_days: list[str] = []

    for deletion in (item for item in deletions if item.code == code):
        mask = dates.eq(deletion.day)
        if int(mask.sum()) == 0:
            counts["already_deleted"] += 1
            deleted_days.append(deletion.day)
            continue
        if int(mask.sum()) != 1:
            errors.append(f"{code}/{deletion.day}: expected one row, found {int(mask.sum())}")
            continue
        index = result.index[mask][0]
        row = result.loc[index]
        if not _same(row.get("close_raw"), deletion.expected_raw_close, atol=1e-6):
            errors.append(f"{code}/{deletion.day}: close_raw precondition failed")
            continue
        if not _same(row.get("volume"), deletion.expected_volume, atol=1e-6):
            errors.append(f"{code}/{deletion.day}: volume precondition failed")
            continue
        details.append(
            {
                "code": code,
                "date": deletion.day,
                "action": "delete_non_trading_row",
                "old_volume": _number(row.get("volume")),
                "old_amount": _number(row.get("amount")),
                "reason": deletion.reason,
            }
        )
        result = result.drop(index=index)
        dates = _date_keys(result["date"])
        deleted_days.append(deletion.day)
        counts["deleted_rows"] += 1

    for correction in (item for item in corrections if item.code == code):
        mask = dates.eq(correction.day)
        if int(mask.sum()) != 1:
            errors.append(f"{code}/{correction.day}: expected one row, found {int(mask.sum())}")
            continue
        index = result.index[mask][0]
        row = result.loc[index].copy()
        if not _same(row.get("close_raw"), correction.raw_close, atol=1e-6):
            errors.append(f"{code}/{correction.day}: close_raw precondition failed")
            continue
        old_volume = _number(row.get("volume"))
        if not (
            np.isclose(old_volume, correction.expected_old_volume, rtol=0.0, atol=1e-6)
            or np.isclose(old_volume, correction.volume_shares, rtol=0.0, atol=1e-6)
        ):
            errors.append(
                f"{code}/{correction.day}: unexpected current volume {old_volume}"
            )
            continue
        vwap = correction.amount_yuan / correction.volume_shares
        if not (correction.raw_low <= vwap <= correction.raw_high):
            errors.append(
                f"{code}/{correction.day}: VWAP {vwap:.12g} outside raw envelope"
            )
            continue
        adjusted_open = (
            correction.adjusted_open
            if correction.adjusted_open is not None
            else _number(row.get("open"))
        )
        adjusted_close = (
            correction.adjusted_close
            if correction.adjusted_close is not None
            else _number(row.get("close"))
        )
        if not (
            correction.adjusted_low <= adjusted_open <= correction.adjusted_high
            and correction.adjusted_low <= adjusted_close <= correction.adjusted_high
        ):
            errors.append(f"{code}/{correction.day}: retained open/close outside new envelope")
            continue

        market_cap = _number(row.get("market_cap"))
        if correction.float_shares is not None:
            float_shares = correction.float_shares
            new_market_cap = correction.raw_close * float_shares
        else:
            if not math.isfinite(market_cap) or market_cap <= 0:
                errors.append(f"{code}/{correction.day}: missing market-cap share anchor")
                continue
            float_shares = market_cap / correction.raw_close
            new_market_cap = market_cap
        new_turnover = correction.volume_shares * 100.0 / float_shares
        if correction.turnover_override is not None:
            if not np.isclose(
                new_turnover,
                correction.turnover_override,
                rtol=0.0,
                atol=1e-9,
            ):
                errors.append(f"{code}/{correction.day}: turnover override is inconsistent")
                continue
            new_turnover = correction.turnover_override

        changed_fields: list[str] = []
        updates = [
            ("low", correction.adjusted_low),
            ("high", correction.adjusted_high),
            ("volume", correction.volume_shares),
            ("amount", correction.amount_yuan),
            ("turnover", new_turnover),
            ("market_cap", new_market_cap),
        ]
        if correction.adjusted_open is not None:
            updates.append(("open", correction.adjusted_open))
        if correction.adjusted_close is not None:
            updates.append(("close", correction.adjusted_close))
        for column, value in updates:
            if _set_number(result, index, column, value):
                changed_fields.append(column)
        correction_days.append(correction.day)
        if "close" in changed_fields:
            close_changed_days.append(correction.day)
        if changed_fields:
            counts["updated_rows"] += 1
        else:
            counts["already_correct"] += 1
        details.append(
            {
                "code": code,
                "date": correction.day,
                "action": "update_authoritative_trade_row",
                "changed_fields": ",".join(changed_fields),
                "old_volume": old_volume,
                "new_volume": correction.volume_shares,
                "old_amount": _number(row.get("amount")),
                "new_amount": correction.amount_yuan,
                "vwap": vwap,
                "raw_low": correction.raw_low,
                "raw_high": correction.raw_high,
                "old_turnover": _number(row.get("turnover")),
                "new_turnover": new_turnover,
                "old_market_cap": market_cap,
                "new_market_cap": new_market_cap,
                "old_open": _number(row.get("open")),
                "new_open": adjusted_open,
                "old_close": _number(row.get("close")),
                "new_close": adjusted_close,
            }
        )

    if errors:
        return current.copy(), counts, details, errors

    expected = _expected_derived(result)
    current_dates = _date_keys(result["date"])
    derived_targets: dict[str, set[str]] = {
        day: {"amplitude"} for day in correction_days
    }
    parsed_dates = pd.to_datetime(current_dates, errors="coerce")
    for deleted_day in deleted_days:
        later = parsed_dates[parsed_dates > pd.Timestamp(deleted_day)]
        if later.empty:
            continue
        successor_index = later.idxmin()
        successor_day = str(current_dates.loc[successor_index])
        derived_targets.setdefault(successor_day, set()).update(
            {"change", "change_pct", "amplitude"}
        )
    for changed_day in close_changed_days:
        derived_targets.setdefault(changed_day, set()).update(
            {"change", "change_pct", "amplitude"}
        )
        later = parsed_dates[parsed_dates > pd.Timestamp(changed_day)]
        if later.empty:
            continue
        successor_index = later.idxmin()
        successor_day = str(current_dates.loc[successor_index])
        derived_targets.setdefault(successor_day, set()).update(
            {"change", "change_pct", "amplitude"}
        )
    for day, columns in derived_targets.items():
        matching = current_dates.eq(day)
        if int(matching.sum()) != 1:
            errors.append(f"{code}/{day}: cannot identify derived-field target")
            continue
        index = result.index[matching][0]
        for column in columns:
            value = expected.at[index, column]
            if pd.isna(value):
                if pd.notna(result.at[index, column]):
                    result.at[index, column] = pd.NA
                    counts["derived_fields_recomputed"] += 1
            elif _set_number(result, index, column, float(value)):
                counts["derived_fields_recomputed"] += 1

    result = result.sort_values("date", ascending=False, kind="stable").reset_index(drop=True)
    return result, counts, details, errors


def run(args: argparse.Namespace) -> int:
    data_dir = Path(args.data_dir).resolve()
    backup_dir = (ROOT / args.backup_dir).resolve()
    report_path = (ROOT / args.report).resolve()
    details_path = (ROOT / args.details).resolve()
    codes = sorted({item.code for item in CORRECTIONS}.union(item.code for item in DELETIONS))
    repaired_by_code: dict[str, pd.DataFrame] = {}
    current_by_code: dict[str, pd.DataFrame] = {}
    totals: Counter = Counter()
    details: list[dict[str, Any]] = []
    errors: list[str] = []
    started = time.time()

    for code in codes:
        path = code_path(data_dir, code)
        current = _read_csv(path)
        repaired, counts, code_details, code_errors = repair_frame(code, current)
        current_by_code[code] = current
        repaired_by_code[code] = repaired
        totals.update(counts)
        details.extend(code_details)
        errors.extend(code_errors)

    changed_codes = sorted(
        code
        for code in codes
        if totals and not repaired_by_code[code].equals(current_by_code[code])
    )
    if args.apply and not errors:
        for code in changed_codes:
            path = code_path(data_dir, code)
            backup_path = backup_dir / path.relative_to(data_dir)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            if not backup_path.exists():
                shutil.copy2(path, backup_path)
            _atomic_csv(repaired_by_code[code], path)
            invalidate_caches(data_dir, code)

    detail_frame = pd.DataFrame(details)
    details_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_csv(detail_frame, details_path)
    report = {
        "status": "COMPLETED" if not errors else "FAILED",
        "applied": bool(args.apply and not errors),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_dir": str(data_dir),
        "correction_rows": len(CORRECTIONS),
        "deletion_rows": len(DELETIONS),
        "changed_codes": changed_codes,
        "counts": dict(totals),
        "errors": errors,
        "policy": {
            "stored_volume_unit": "shares",
            "source_hand_conversion": "corrected hands * 100 shares",
            "amount_unit": "CNY yuan",
            "vwap_gate": "raw_low <= amount / stored_share_volume <= raw_high",
            "adjusted_prices": "replace high/low; retain current open/close",
            "derived_fields": "recompute target amplitude and deletion successors",
        },
        "details": str(details_path),
        "backup_dir": str(backup_dir),
        "elapsed_seconds": round(time.time() - started, 3),
    }
    _atomic_json(report, report_path)
    if args.apply and not errors:
        _update_manifest(
            data_dir,
            "authoritative_final_31_rows",
            {
                "correction_rows": len(CORRECTIONS),
                "deleted_non_trading_rows": len(DELETIONS),
                "stored_volume_unit": "shares",
                "amount_unit": "yuan",
                "vwap_gate": report["policy"]["vwap_gate"],
                "report": str(report_path),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"details={details_path}", flush=True)
    print(f"counts={dict(totals)} errors={len(errors)}", flush=True)
    for error in errors:
        print(f"ERROR {error}", flush=True)
    return 0 if not errors else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--backup-dir",
        default="artifacts/maintenance/all_data_gaps/authoritative_31_backup",
    )
    parser.add_argument(
        "--details",
        default="artifacts/maintenance/all_data_gaps/authoritative_31_details.csv",
    )
    parser.add_argument(
        "--report",
        default="artifacts/maintenance/all_data_gaps/authoritative_31_report.json",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
