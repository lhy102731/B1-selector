from __future__ import annotations

import math
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.akshare_fetcher import AKShareFetcher
from utils.checkpoint_retention import prune_checkpoint_history
from utils.eastmoney_fetcher import EastmoneyFetcher


DATA_DIR = ROOT / "data"
TODAY_DATE = date.today()
while TODAY_DATE.weekday() >= 5:
    TODAY_DATE -= timedelta(days=1)
TODAY = int(TODAY_DATE.strftime("%Y%m%d"))
TODAY_STR = TODAY_DATE.isoformat()
REPAIR_DIR = DATA_DIR / "_daily_updates"
BACKUP_DIR = REPAIR_DIR / TODAY_STR / "backup"
STALE_CACHE_DIR = REPAIR_DIR / "stale_indicators_cache"
REPORT_PATH = REPAIR_DIR / TODAY_STR / "update_report.csv"
UPDATE_CACHE_PATH = DATA_DIR / ".update_cache.json"
MIN_UPDATE_SUCCESS_RATIO = 0.99
MAX_UNEXPECTED_UNCHANGED_RATIO = 0.005


def iter_stock_files() -> list[Path]:
    files: list[Path] = []
    for prefix in ("00", "30", "60", "68"):
        files.extend(sorted((DATA_DIR / prefix).glob("*.csv")))
    return files


def backup_file(path: Path) -> None:
    rel = path.relative_to(DATA_DIR)
    dst = BACKUP_DIR / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.copy2(path, dst)


def mark_cache_stale(code: str) -> None:
    src = DATA_DIR / "indicators_cache" / f"{code}.parquet"
    if not src.exists():
        return
    # Indicator caches are derived and must be rebuilt after source CSV
    # changes.  Keeping a second 3+ GiB copy under _daily_updates adds no
    # rollback value and risks accidental reuse of stale indicators.
    src.unlink()


def validate_checkpoint_run(
    rows: list[dict[str, object]],
    stock_files: list[Path],
    backup_dir: Path,
) -> dict[str, object]:
    """Validate a delta checkpoint before old checkpoints may be pruned.

    ``no_today_bar`` is a legitimate unchanged state for suspended stocks.
    Optional point-in-time fields may remain null; only successful rows need
    valid price/volume values and a matching pre-write backup.
    """

    expected_codes = [path.stem for path in stock_files]
    reported_codes = [str(row.get("code") or "").zfill(6) for row in rows]
    expected_set = set(expected_codes)
    reported_set = set(reported_codes)
    duplicate_codes = sorted({code for code in reported_codes if reported_codes.count(code) > 1})
    successful_rows = [row for row in rows if row.get("status") in {"inserted", "updated"}]
    successful_codes = {str(row.get("code") or "").zfill(6) for row in successful_rows}
    suspended_codes = {
        str(row.get("code") or "").zfill(6)
        for row in rows
        if row.get("status") == "no_today_bar"
    }
    unexpected_rows = [
        row for row in rows
        if row.get("status") not in {"inserted", "updated", "no_today_bar"}
    ]
    backup_codes = {path.stem for path in backup_dir.rglob("*.csv")} if backup_dir.exists() else set()
    missing_backups = sorted(successful_codes - backup_codes)
    invalid_success_rows: list[str] = []
    for row in successful_rows:
        code = str(row.get("code") or "").zfill(6)
        close = pd.to_numeric(pd.Series([row.get("close")]), errors="coerce").iloc[0]
        volume = pd.to_numeric(pd.Series([row.get("volume")]), errors="coerce").iloc[0]
        if pd.isna(close) or float(close) <= 0 or pd.isna(volume) or float(volume) < 0:
            invalid_success_rows.append(code)

    total = len(expected_codes)
    success_ratio = len(successful_codes) / total if total else 0.0
    max_unexpected = max(1, math.ceil(total * MAX_UNEXPECTED_UNCHANGED_RATIO))
    reasons: list[str] = []
    if len(rows) != total:
        reasons.append(f"report_rows={len(rows)} expected={total}")
    if duplicate_codes:
        reasons.append(f"duplicate_codes={len(duplicate_codes)}")
    missing_codes = sorted(expected_set - reported_set)
    extra_codes = sorted(reported_set - expected_set)
    if missing_codes:
        reasons.append(f"missing_codes={len(missing_codes)}")
    if extra_codes:
        reasons.append(f"extra_codes={len(extra_codes)}")
    if success_ratio < MIN_UPDATE_SUCCESS_RATIO:
        reasons.append(f"success_ratio={success_ratio:.6f}")
    if len(unexpected_rows) > max_unexpected:
        reasons.append(f"unexpected_unchanged={len(unexpected_rows)}>{max_unexpected}")
    if missing_backups:
        reasons.append(f"missing_backups={len(missing_backups)}")
    if invalid_success_rows:
        reasons.append(f"invalid_success_rows={len(invalid_success_rows)}")

    return {
        "valid": not reasons,
        "reasons": reasons,
        "expected_files": total,
        "reported_rows": len(rows),
        "successful": len(successful_codes),
        "success_ratio": success_ratio,
        "suspended_no_today_bar": len(suspended_codes),
        "unexpected_unchanged": len(unexpected_rows),
        "max_unexpected_unchanged": max_unexpected,
        "backup_files": len(backup_codes),
        "missing_backups": missing_backups,
        "invalid_success_rows": sorted(invalid_success_rows),
    }


def robust_median(values: list[float]) -> float | None:
    clean = [v for v in values if math.isfinite(v) and v > 0]
    if not clean:
        return None
    clean.sort()
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2.0


def fetch_hfq_rows(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    return EastmoneyFetcher().fetch_kline(
        code,
        start=start_date,
        end=end_date,
        adjust="hfq",
    )


def fetch_tencent_hfq_rows(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch Tencent HFQ and derive fields Tencent's endpoint omits."""

    start_ts = pd.to_datetime(start_date, format="%Y%m%d")
    end_ts = pd.to_datetime(end_date, format="%Y%m%d")
    calendar_days = max(120, int((pd.Timestamp.today().normalize() - start_ts).days) + 20)
    frame = AKShareFetcher()._fetch_stock_update_tencent(code, days=calendar_days)
    if frame is None or frame.empty:
        return pd.DataFrame()
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"])
    frame = frame[frame["date"].between(start_ts, end_ts)].sort_values("date")
    if frame.empty:
        return frame
    for field in ("open", "high", "low", "close"):
        frame[field] = pd.to_numeric(frame[field], errors="coerce")
    previous_close = frame["close"].shift(1)
    frame["change_pct"] = (frame["close"] / previous_close - 1.0) * 100.0
    frame["amplitude"] = (frame["high"] - frame["low"]) / previous_close * 100.0
    frame["change"] = frame["close"] - previous_close
    # Tencent's hfq endpoint used here does not provide these fields.  Missing
    # is safer than overwriting a same-date point-in-time observation with 0.
    frame["amount"] = pd.NA
    frame["turnover"] = pd.NA
    return frame.sort_values("date", ascending=False).reset_index(drop=True)


def fetch_hfq_rows_with_fallback(
    code: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Fetch HFQ rows with explicit, auditable provider provenance."""

    fallback_reason: str | None = None
    try:
        primary = fetch_hfq_rows(code, start_date, end_date)
    except requests.exceptions.RequestException as exc:
        primary = pd.DataFrame()
        fallback_reason = type(exc).__name__
    if primary is not None and not primary.empty:
        return primary, {
            "provider": "eastmoney",
            "fallback_reason": None,
        }

    if fallback_reason is None:
        fallback_reason = "empty_eastmoney_response"
    fallback = fetch_tencent_hfq_rows(code, start_date, end_date)
    if fallback is not None and not fallback.empty:
        return fallback, {
            "provider": "tencent",
            "fallback_reason": fallback_reason,
        }
    return pd.DataFrame(), {
        "provider": "unavailable",
        "fallback_reason": fallback_reason,
    }


def estimate_hfq_affine_transform(
    local: pd.DataFrame,
    remote: pd.DataFrame,
    *,
    target_date: int,
    max_points: int = 60,
    min_points: int = 3,
    max_relative_error: float = 0.0025,
) -> tuple[tuple[float, float] | None, dict[str, object]]:
    """Fit local HFQ = slope * provider HFQ + intercept, fail closed."""

    if local.empty or remote.empty or "close" not in local or "close" not in remote:
        return None, {"anchor_status": "missing_close"}

    left = local.copy()
    right = remote.copy()
    left["date_int"] = pd.to_datetime(left["date"], errors="coerce").dt.strftime("%Y%m%d")
    right["date_int"] = pd.to_datetime(right["date"], errors="coerce").dt.strftime("%Y%m%d")
    left = left.dropna(subset=["date_int"])
    right = right.dropna(subset=["date_int"])
    left["date_int"] = left["date_int"].astype(int)
    right["date_int"] = right["date_int"].astype(int)
    left = left[left["date_int"] < int(target_date)]
    right = right[right["date_int"] < int(target_date)]
    if "volume" in left:
        left = left[pd.to_numeric(left["volume"], errors="coerce") > 0]

    overlap = left[["date_int", "close"]].merge(
        right[["date_int", "close"]],
        on="date_int",
        suffixes=("_local", "_remote"),
    )
    overlap["close_local"] = pd.to_numeric(overlap["close_local"], errors="coerce")
    overlap["close_remote"] = pd.to_numeric(overlap["close_remote"], errors="coerce")
    overlap = overlap[
        (overlap["close_local"] > 0)
        & (overlap["close_remote"] > 0)
    ].sort_values("date_int", ascending=False).head(max(1, int(max_points)))
    diagnostics: dict[str, object] = {
        "anchor_points": len(overlap),
        "anchor_first_date": int(overlap["date_int"].min()) if len(overlap) else None,
        "anchor_last_date": int(overlap["date_int"].max()) if len(overlap) else None,
    }
    if len(overlap) < int(min_points):
        diagnostics["anchor_status"] = "insufficient_overlap"
        return None, diagnostics

    x = overlap["close_remote"].to_numpy(dtype=float)
    y = overlap["close_local"].to_numpy(dtype=float)
    remote_variation = (float(np.max(x)) - float(np.min(x))) / max(
        abs(float(np.median(x))),
        1e-12,
    )
    diagnostics["anchor_remote_variation"] = remote_variation
    if remote_variation < 0.005:
        diagnostics["anchor_status"] = "insufficient_price_variation"
        return None, diagnostics

    design = np.column_stack([x, np.ones(len(x))])
    slope, intercept = np.linalg.lstsq(design, y, rcond=None)[0]
    if len(x) >= 10:
        initial = np.abs((slope * x + intercept) / y - 1.0)
        cutoff = float(np.quantile(initial, 0.90))
        keep = initial <= cutoff
        if int(keep.sum()) >= max(int(min_points), 3):
            design_kept = np.column_stack([x[keep], np.ones(int(keep.sum()))])
            slope, intercept = np.linalg.lstsq(design_kept, y[keep], rcond=None)[0]

    predicted = slope * x + intercept
    relative_error = np.abs(predicted / y - 1.0)
    max_error = float(np.max(relative_error))
    diagnostics.update(
        {
            "anchor_slope": float(slope),
            "anchor_intercept": float(intercept),
            "anchor_max_relative_error": max_error,
            "anchor_p95_relative_error": float(np.quantile(relative_error, 0.95)),
        }
    )
    if not math.isfinite(float(slope)) or float(slope) <= 0:
        diagnostics["anchor_status"] = "invalid_affine_slope"
        return None, diagnostics
    if max_error > float(max_relative_error):
        diagnostics["anchor_status"] = "unstable_affine"
        return None, diagnostics
    diagnostics["anchor_status"] = "stable_affine"
    return (float(slope), float(intercept)), diagnostics


def write_csv_atomic(frame: pd.DataFrame, path: Path, *, encoding: str) -> None:
    """Replace a CSV only after the complete replacement has reached disk."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding=encoding,
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


def write_text_atomic(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding=encoding,
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def estimate_float_shares(df: pd.DataFrame, around_date: int = TODAY) -> float | None:
    if "volume" not in df.columns or "turnover" not in df.columns:
        return None
    nearby = df[(df["date_int"] - around_date).abs() <= 30].copy()
    nearby["volume_num"] = pd.to_numeric(nearby["volume"], errors="coerce")
    nearby["turnover_num"] = pd.to_numeric(nearby["turnover"], errors="coerce")
    nearby = nearby[(nearby["volume_num"] > 0) & (nearby["turnover_num"] > 0)]
    if nearby.empty:
        return None
    vals = (nearby["volume_num"] / (nearby["turnover_num"] / 100.0)).dropna().sort_values()
    if vals.empty:
        return None
    return float(vals.iloc[len(vals) // 2])


def update_one(path: Path, quote: dict[str, object] | None) -> dict[str, object]:
    code = path.stem
    df = pd.read_csv(path, encoding="gbk")
    if df.empty or "date" not in df.columns:
        return {"code": code, "status": "empty_or_bad_csv"}
    original_columns = list(df.columns)
    df["date_ts"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_ts"]).copy()
    df["date_int"] = df["date_ts"].dt.strftime("%Y%m%d").astype(int)

    prior_dates = df.loc[df["date_int"] < TODAY, "date_ts"].dropna().sort_values()
    if prior_dates.empty:
        return {"code": code, "status": "no_prior_anchor"}
    start_date = prior_dates.iloc[max(0, len(prior_dates) - 60)].strftime("%Y%m%d")
    remote, provider = fetch_hfq_rows_with_fallback(
        code,
        start_date,
        str(TODAY),
    )
    if remote.empty:
        return {"code": code, "status": "no_hfq_rows", **provider}
    remote["date_int"] = pd.to_datetime(remote["date"], errors="coerce").dt.strftime("%Y%m%d")
    remote = remote.dropna(subset=["date_int"]).copy()
    remote["date_int"] = remote["date_int"].astype(int)
    today_rows = remote[remote["date_int"] == TODAY]
    if today_rows.empty:
        return {"code": code, "status": "no_today_bar", **provider}
    today_bar = today_rows.iloc[0]
    transform, anchor = estimate_hfq_affine_transform(df, remote, target_date=TODAY)
    if transform is None:
        return {
            "code": code,
            "status": str(anchor["anchor_status"]),
            **provider,
            **anchor,
        }
    slope, intercept = transform

    remote_ascending = remote.sort_values("date_int").reset_index(drop=True)
    today_position = remote_ascending.index[remote_ascending["date_int"] == TODAY]
    if len(today_position) != 1 or int(today_position[0]) == 0:
        return {
            "code": code,
            "status": "no_previous_hfq_bar",
            **provider,
            **anchor,
        }
    previous_bar = remote_ascending.iloc[int(today_position[0]) - 1]
    mapped_previous_close = float(previous_bar["close"]) * slope + intercept

    # A row from another date is not a valid template.  Unknown, unavailable,
    # and point-in-time fields must remain missing instead of silently becoming
    # stale observations.  A same-day row may be retained for idempotent retry.
    row = {col: pd.NA for col in original_columns}
    if (df["date_int"] == TODAY).any():
        row.update(df.loc[df["date_int"] == TODAY].iloc[0].to_dict())
        action = "updated"
    else:
        action = "inserted"

    row["date"] = TODAY_STR
    row["open"] = float(today_bar["open"]) * slope + intercept
    row["high"] = float(today_bar["high"]) * slope + intercept
    row["low"] = float(today_bar["low"]) * slope + intercept
    row["close"] = float(today_bar["close"]) * slope + intercept
    row["volume"] = int(today_bar["volume"])
    if pd.notna(today_bar.get("amount")):
        row["amount"] = float(today_bar["amount"])
    row["change_pct"] = (row["close"] / mapped_previous_close - 1.0) * 100.0
    if "amplitude" in original_columns:
        row["amplitude"] = (row["high"] - row["low"]) / mapped_previous_close * 100.0
    if "change" in original_columns:
        row["change"] = row["close"] - mapped_previous_close

    quote = quote or {}
    if "turnover" in original_columns:
        q_turnover = float(quote.get("turnover") or 0)
        if q_turnover > 0:
            row["turnover"] = q_turnover
        else:
            shares = estimate_float_shares(df)
            remote_turnover = float(today_bar.get("turnover") or 0)
            row["turnover"] = remote_turnover if remote_turnover > 0 else (
                int(today_bar["volume"]) / shares * 100.0
                if shares and shares > 0
                else row.get("turnover", pd.NA)
            )
    if "market_cap" in original_columns:
        q_cap = int(quote.get("market_cap") or 0)
        if q_cap > 0:
            row["market_cap"] = q_cap
    for src, dst in (("pe_dynamic", "pe_dynamic"), ("pb", "pb")):
        if dst in original_columns:
            value = quote.get(src)
            if value not in (None, "", 0):
                row[dst] = value

    if TODAY in set(df["date_int"]):
        mask = df["date_int"] == TODAY
        for col in original_columns:
            if col in row:
                df.loc[mask, col] = row[col]
    else:
        df = pd.concat([df, pd.DataFrame([{col: row.get(col, pd.NA) for col in original_columns}])], ignore_index=True)

    backup_file(path)
    df["date_ts"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date_ts", ascending=False)
    write_csv_atomic(df[original_columns], path, encoding="gbk")
    mark_cache_stale(code)
    return {
        "code": code,
        "status": action,
        **provider,
        "factor": slope,
        "offset": intercept,
        **anchor,
        "close": row["close"],
        "volume": row["volume"],
        "amount": row["amount"],
        "turnover": row.get("turnover"),
        "market_cap": row.get("market_cap"),
    }


def main() -> int:
    files = iter_stock_files()
    codes = [p.stem for p in files]
    print(f"files={len(files)}", flush=True)
    quote_data = AKShareFetcher()._fetch_quote_batch_tencent(codes)
    print(f"quote_codes={len(quote_data)}", flush=True)

    rows: list[dict[str, object]] = []
    started = time.time()
    for idx, path in enumerate(files, start=1):
        try:
            result = update_one(path, quote_data.get(path.stem))
        except Exception as exc:
            result = {"code": path.stem, "status": f"failed:{type(exc).__name__}", "error": repr(exc)}
        rows.append(result)
        if idx == 1 or idx % 100 == 0 or str(result.get("status", "")).startswith("failed"):
            ok = sum(1 for r in rows if r.get("status") in {"inserted", "updated"})
            print(f"{idx}/{len(files)} ok={ok} status={result.get('status')} elapsed={time.time()-started:.1f}s", flush=True)

    write_csv_atomic(pd.DataFrame(rows), REPORT_PATH, encoding="utf-8-sig")
    successful = sum(1 for row in rows if row.get("status") in {"inserted", "updated"})
    validation = validate_checkpoint_run(rows, files, BACKUP_DIR)
    validation_path = REPORT_PATH.parent / "checkpoint_validation.json"
    write_text_atomic(
        validation_path,
        json.dumps(validation, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if validation["valid"]:
        cache = {}
        if UPDATE_CACHE_PATH.exists():
            try:
                cache = json.loads(UPDATE_CACHE_PATH.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                cache = {}
        cache["last_update_date"] = TODAY_STR
        write_text_atomic(
            UPDATE_CACHE_PATH,
            json.dumps(cache, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"update_cache={UPDATE_CACHE_PATH} date={TODAY_STR}", flush=True)
        cleanup = prune_checkpoint_history(REPAIR_DIR, REPORT_PATH.parent)
        cleanup_path = REPORT_PATH.parent / "checkpoint_cleanup.json"
        write_text_atomic(
            cleanup_path,
            json.dumps(cleanup, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(
            f"checkpoint_cleanup={cleanup_path} removed_files={cleanup['removed_files']} "
            f"removed_bytes={cleanup['removed_bytes']}",
            flush=True,
        )
    else:
        print(
            f"checkpoint_validation=FAILED reasons={validation['reasons']} "
            "old checkpoints retained",
            flush=True,
        )
    print(f"report={REPORT_PATH}", flush=True)
    print(f"checkpoint_validation={validation_path}", flush=True)
    print(pd.Series([r.get("status") for r in rows]).value_counts().to_string(), flush=True)
    return 0 if validation["valid"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
