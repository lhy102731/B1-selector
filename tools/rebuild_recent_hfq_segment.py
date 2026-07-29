"""Stage a recent A-share HFQ segment rebuild without touching source CSVs.

The source files use a local/Baostock HFQ baseline.  Eastmoney HFQ has the
same economic return semantics but a different numeric baseline, so this tool
derives one stable scale from dates strictly before the repair interval.  It
then stages complete replacement CSVs and a hash-bound manifest.  This module
does not apply, move, or delete production files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.update_today_em_client import (
    estimate_hfq_affine_transform,
    fetch_hfq_rows,
    fetch_tencent_hfq_rows,
    write_csv_atomic,
    write_text_atomic,
)


SOURCE_PREFIXES = ("00", "30", "60", "68")
PRICE_FIELDS = ("open", "high", "low", "close")
DIRECT_FIELDS = (
    "volume",
    "amount",
    "turnover",
)


def _date_int(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y%m%d")


def _as_iso(date_int: int) -> str:
    return pd.to_datetime(str(int(date_int)), format="%Y%m%d").strftime("%Y-%m-%d")


def _equal_value(left: object, right: object) -> bool:
    if pd.isna(left) and pd.isna(right):
        return True
    try:
        return abs(float(left) - float(right)) <= 1e-10 * max(
            1.0,
            abs(float(left)),
            abs(float(right)),
        )
    except (TypeError, ValueError):
        return str(left) == str(right)


def rebuild_frame(
    local: pd.DataFrame,
    remote: pd.DataFrame,
    *,
    start_date: int,
    end_date: int,
    max_anchor_error: float = 0.0025,
    min_anchor_points: int = 10,
) -> tuple[pd.DataFrame, dict[str, object], list[dict[str, object]]]:
    """Return a staged frame, diagnostics, and row-level change events."""

    original = local.copy()
    if local.empty or "date" not in local.columns:
        return original, {"status": "QUARANTINED", "reason": "bad_local_frame"}, []
    if remote.empty or "date" not in remote.columns:
        return original, {"status": "QUARANTINED", "reason": "no_remote_rows"}, []

    start_date = int(start_date)
    end_date = int(end_date)
    remote_dates = _date_int(remote["date"])
    remote_target = remote_dates.dropna().astype(int).between(start_date, end_date)
    if not remote_target.any():
        local_dates = _date_int(local["date"])
        local_mask = local_dates.notna()
        local_target = pd.Series(False, index=local.index)
        local_target.loc[local_mask] = local_dates.loc[local_mask].astype(int).between(
            start_date,
            end_date,
        )
        if "volume" in local:
            local_target &= pd.to_numeric(local["volume"], errors="coerce").fillna(0) > 0
        if not local_target.any():
            return original, {
                "status": "NOT_APPLICABLE",
                "reason": "no_trading_rows_in_target_interval",
            }, []
        return original, {"status": "QUARANTINED", "reason": "no_remote_target_rows"}, []

    transform, anchor = estimate_hfq_affine_transform(
        local,
        remote,
        target_date=start_date,
        max_points=60,
        min_points=min_anchor_points,
        max_relative_error=max_anchor_error,
    )
    if transform is None:
        return (
            original,
            {
                "status": "QUARANTINED",
                "reason": str(anchor.get("anchor_status") or "invalid_anchor"),
                **anchor,
            },
            [],
        )
    slope, intercept = transform

    original_columns = list(local.columns)
    work = local.copy()
    work["_date_int"] = _date_int(work["date"])
    if work["_date_int"].isna().any():
        return original, {"status": "QUARANTINED", "reason": "invalid_local_date"}, []
    work["_date_int"] = work["_date_int"].astype(int)
    if work["_date_int"].duplicated().any():
        return original, {"status": "QUARANTINED", "reason": "duplicate_local_date"}, []

    source = remote.copy()
    source["_date_int"] = _date_int(source["date"])
    source = source.dropna(subset=["_date_int"]).copy()
    source["_date_int"] = source["_date_int"].astype(int)
    source = source.sort_values("_date_int").reset_index(drop=True)
    for field in PRICE_FIELDS:
        source[field] = pd.to_numeric(source[field], errors="coerce") * slope + intercept
    source["_mapped_previous_close"] = source["close"].shift(1)
    source["change"] = source["close"] - source["_mapped_previous_close"]
    source["change_pct"] = (
        source["close"] / source["_mapped_previous_close"] - 1.0
    ) * 100.0
    source["amplitude"] = (
        (source["high"] - source["low"]) / source["_mapped_previous_close"] * 100.0
    )
    source = source[source["_date_int"].between(start_date, end_date)]
    if source.empty:
        return original, {"status": "QUARANTINED", "reason": "no_remote_target_rows"}, []
    if source["_date_int"].duplicated().any():
        return original, {"status": "QUARANTINED", "reason": "duplicate_remote_date"}, []

    events: list[dict[str, object]] = []
    for _, remote_row in source.sort_values("_date_int").iterrows():
        date_value = int(remote_row["_date_int"])
        mask = work["_date_int"] == date_value
        action = "replaced" if mask.any() else "inserted"
        if mask.any():
            row_index = work.index[mask][0]
            before_close = work.at[row_index, "close"] if "close" in work else None
        else:
            row_index = (int(work.index.max()) + 1) if len(work.index) else 0
            work.loc[row_index, :] = pd.NA
            work.at[row_index, "_date_int"] = date_value
            before_close = None

        work.at[row_index, "date"] = _as_iso(date_value)
        changed_fields: list[str] = []
        for field in (*PRICE_FIELDS, "change"):
            if field not in original_columns or field not in source.columns:
                continue
            if pd.isna(remote_row[field]):
                continue
            value = float(remote_row[field])
            if not _equal_value(work.at[row_index, field], value):
                changed_fields.append(field)
            work.at[row_index, field] = value
        for field in (*DIRECT_FIELDS, "change_pct", "amplitude"):
            if field not in original_columns or field not in source.columns:
                continue
            value = remote_row[field]
            if pd.isna(value):
                continue
            if not _equal_value(work.at[row_index, field], value):
                changed_fields.append(field)
            work.at[row_index, field] = value

        if action == "inserted" or changed_fields:
            events.append(
                {
                    "date": _as_iso(date_value),
                    "action": action,
                    "before_close": before_close,
                    "after_close": work.at[row_index, "close"] if "close" in work else None,
                    "changed_fields": changed_fields,
                }
            )

    work["_date_ts"] = pd.to_datetime(work["date"], errors="coerce")
    if work["_date_ts"].isna().any():
        return original, {"status": "QUARANTINED", "reason": "invalid_output_date"}, []
    work = work.sort_values("_date_ts", ascending=False)
    output = work[original_columns].reset_index(drop=True)

    for field in ("open", "high", "low", "close"):
        if field in output:
            output[field] = pd.to_numeric(output[field], errors="coerce")
    if all(field in output for field in ("open", "high", "low", "close")):
        target_mask = pd.to_datetime(output["date"], errors="coerce").dt.strftime("%Y%m%d")
        target_mask = target_mask.astype(int).between(start_date, end_date)
        target = output.loc[target_mask]
        invalid_ohlc = (
            (target["high"] < target[["open", "close"]].max(axis=1))
            | (target["low"] > target[["open", "close"]].min(axis=1))
            | (target["low"] > target["high"])
        )
        if invalid_ohlc.any():
            return original, {"status": "QUARANTINED", "reason": "invalid_output_ohlc"}, []

    return (
        output,
        {
            "status": "READY",
            "factor": slope,
            "offset": intercept,
            "target_remote_rows": len(source),
            "changed_rows": len(events),
            **anchor,
        },
        events,
    )


def iter_stock_files(data_dir: Path, codes: set[str] | None = None) -> list[Path]:
    files: list[Path] = []
    for prefix in SOURCE_PREFIXES:
        root = data_dir / prefix
        if root.is_dir():
            files.extend(sorted(root.glob("*.csv")))
    if codes is not None:
        files = [path for path in files if path.stem in codes]
    return files


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _fetch_start(local: pd.DataFrame, start_date: int, points: int = 80) -> str | None:
    dates = pd.to_datetime(local.get("date"), errors="coerce").dropna().sort_values()
    dates = dates[dates.dt.strftime("%Y%m%d").astype(int) < int(start_date)]
    if dates.empty:
        return None
    return dates.iloc[max(0, len(dates) - int(points))].strftime("%Y%m%d")


def _fetch_with_retry(
    code: str,
    start: str,
    end: str,
    *,
    provider: str,
    retries: int = 3,
) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(max(1, int(retries))):
        try:
            if provider == "tencent":
                return fetch_tencent_hfq_rows(code, start, end)
            return fetch_hfq_rows(code, start, end)
        except Exception as error:  # noqa: BLE001 - archived per-code in the manifest.
            last_error = error
            if attempt + 1 < retries:
                time.sleep(0.5 * (2**attempt))
    if last_error is not None:
        raise last_error
    return pd.DataFrame()


def _stage_one_fresh(
    source_path: Path,
    *,
    data_dir: Path,
    output_dir: Path,
    start_date: int,
    end_date: int,
    provider: str,
) -> dict[str, Any]:
    code = source_path.stem
    relative = source_path.relative_to(data_dir)
    source_hash = sha256_file(source_path)
    try:
        local = pd.read_csv(source_path, encoding="gbk")
        fetch_start = _fetch_start(local, start_date)
        if fetch_start is None:
            return {
                "code": code,
                "relative_path": str(relative),
                "status": "QUARANTINED",
                "reason": "no_preincident_anchor",
                "source_sha256": source_hash,
            }
        remote = _fetch_with_retry(
            code,
            fetch_start,
            str(end_date),
            provider=provider,
        )
        if remote.empty:
            recent = local.copy()
            recent_dates = pd.to_datetime(recent.get("date"), errors="coerce").dt.strftime("%Y%m%d")
            recent = recent[recent_dates.notna()].copy()
            recent_dates = recent_dates[recent_dates.notna()].astype(int)
            recent = recent.loc[recent_dates.between(start_date, end_date)]
            if "volume" in recent and not (pd.to_numeric(recent["volume"], errors="coerce") > 0).any():
                return {
                    "code": code,
                    "relative_path": str(relative),
                    "status": "NOT_APPLICABLE",
                    "reason": "no_remote_or_local_trading_rows",
                    "source_sha256": source_hash,
                }
        rebuilt, result, events = rebuild_frame(
            local,
            remote,
            start_date=start_date,
            end_date=end_date,
        )
        record: dict[str, Any] = {
            "code": code,
            "relative_path": str(relative),
            "source_sha256": source_hash,
            **result,
            "events": events,
        }
        if result.get("status") != "READY":
            return record
        if sha256_file(source_path) != source_hash:
            return {
                **record,
                "status": "QUARANTINED",
                "reason": "source_changed_during_stage",
            }
        staged_path = output_dir / "staged" / relative
        write_csv_atomic(rebuilt, staged_path, encoding="gbk")
        record["staged_path"] = str(staged_path.resolve())
        record["staged_sha256"] = sha256_file(staged_path)
        record["staged_bytes"] = staged_path.stat().st_size
        return record
    except Exception as error:  # noqa: BLE001 - every code must be represented.
        return {
            "code": code,
            "relative_path": str(relative),
            "status": "FAILED",
            "reason": f"{type(error).__name__}: {error}",
            "source_sha256": source_hash,
        }


def stage_one(
    source_path: Path,
    *,
    data_dir: Path,
    output_dir: Path,
    start_date: int,
    end_date: int,
    provider: str,
) -> dict[str, Any]:
    """Stage one code and durably checkpoint its result for exact resume."""

    relative = source_path.relative_to(data_dir)
    source_hash = sha256_file(source_path)
    result_path = output_dir / "results" / relative.with_suffix(".json")
    context = {
        "builder_schema_version": 2,
        "provider": str(provider),
        "start_date": int(start_date),
        "end_date": int(end_date),
        "source_sha256": source_hash,
    }
    if result_path.exists():
        try:
            cached = json.loads(result_path.read_text(encoding="utf-8"))
            matches = all(cached.get(key) == value for key, value in context.items())
            if matches and cached.get("status") != "FAILED":
                if cached.get("status") == "READY":
                    staged_path = Path(str(cached.get("staged_path") or ""))
                    matches = (
                        staged_path.is_file()
                        and sha256_file(staged_path) == cached.get("staged_sha256")
                    )
                if matches:
                    return {**cached, "resumed": True}
        except (OSError, ValueError, json.JSONDecodeError):
            pass

    record = _stage_one_fresh(
        source_path,
        data_dir=data_dir,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        provider=provider,
    )
    record.update(context)
    write_text_atomic(
        result_path,
        json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=str(PROJECT_ROOT / "data"))
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start", type=int, default=20260611)
    parser.add_argument("--end", type=int, default=20260723)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--provider", choices=("eastmoney", "tencent"), default="eastmoney")
    parser.add_argument("--codes", help="Comma-separated six-digit codes for a smoke run")
    parser.add_argument("--fail-on-quarantine", action="store_true")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    codes = None
    if args.codes:
        codes = {value.strip().zfill(6) for value in args.codes.split(",") if value.strip()}
    files = iter_stock_files(data_dir, codes)
    records: list[dict[str, Any]] = []
    started = time.time()

    with ThreadPoolExecutor(max_workers=max(1, int(args.max_workers))) as pool:
        futures = {
            pool.submit(
                stage_one,
                path,
                data_dir=data_dir,
                output_dir=output_dir,
                start_date=int(args.start),
                end_date=int(args.end),
                provider=str(args.provider),
            ): path
            for path in files
        }
        for index, future in enumerate(as_completed(futures), start=1):
            record = future.result()
            records.append(record)
            if index == 1 or index % 50 == 0 or record.get("status") != "READY":
                print(
                    f"{index}/{len(files)} code={record['code']} status={record['status']} "
                    f"elapsed={time.time() - started:.1f}s",
                    flush=True,
                )

    records.sort(key=lambda item: str(item["code"]))
    counts = pd.Series([record["status"] for record in records]).value_counts().to_dict()
    summary = {
        "schema_version": 1,
        "mode": "STAGE_ONLY_NO_SOURCE_WRITES",
        "data_dir": str(data_dir),
        "output_dir": str(output_dir),
        "start_date": int(args.start),
        "end_date": int(args.end),
        "provider": str(args.provider),
        "files_requested": len(files),
        "status_counts": {str(key): int(value) for key, value in counts.items()},
        "elapsed_seconds": time.time() - started,
        "records": records,
    }
    summary_path = output_dir / "rebuild_manifest.json"
    write_text_atomic(
        summary_path,
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    print(f"summary={summary_path}", flush=True)
    print(f"status_counts={summary['status_counts']}", flush=True)
    has_problem = any(record["status"] in {"FAILED", "QUARANTINED"} for record in records)
    return 2 if args.fail_on_quarantine and has_problem else 0


if __name__ == "__main__":
    raise SystemExit(main())
