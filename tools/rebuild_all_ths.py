"""用 THSDK 在隔离目录重建整套日线数据库。

默认范围为 1990-01-01 至今天，股票代码来自现有 CSV 文件名（文件名只是
证券标识，不会把旧 CSV 的任何数值混入新数据）。脚本先做历史字段权限
探针，再写入 staging；只有显式 ``--commit`` 且所有股票通过校验时才会
备份并原子切换四个股票目录。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.ths_data_source import THSDataSource, THSDataSourceError, THSHistoryPermissionError


STOCK_PREFIXES = ("00", "30", "60", "68")
DATASET_MANIFEST = ".ths_dataset_manifest.json"
DATASET_SCHEMA_VERSION = 3
DATA_QUALITY_VERSION = 4
BASE_COLUMNS = [
    "date", "open", "high", "low", "close", "close_raw", "volume", "amount",
    "turnover", "change_pct", "pe_dynamic", "pb", "ps", "pcf", "market_cap",
    "amplitude", "change",
]


def stock_files(data_dir: Path, max_stocks: int | None = None) -> list[Path]:
    files: list[Path] = []
    for prefix in STOCK_PREFIXES:
        files.extend(sorted((data_dir / prefix).glob("*.csv")))
    return files[:max_stocks] if max_stocks else files


def _atomic_write(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="gbk", newline="", dir=path.parent,
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


def _normalise_history(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=BASE_COLUMNS)
    result = frame.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.tz_localize(None)
    result = result.dropna(subset=["date"]).drop_duplicates("date", keep="last")
    numeric = [column for column in ("open", "high", "low", "close", "close_raw", "volume", "amount", "turnover", "market_cap", "change_pct", "amplitude", "change") if column in result]
    for column in numeric:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result = result.sort_values("date", ascending=False).reset_index(drop=True)
    for column in BASE_COLUMNS:
        if column not in result.columns:
            result[column] = pd.NA
    return result[BASE_COLUMNS]


def validate_history(
    frame: pd.DataFrame,
    code: str,
    *,
    min_cap_coverage: float = 0.90,
    min_amount_coverage: float = 0.80,
    expected_start: str | date | datetime | pd.Timestamp | None = None,
    expected_end: str | date | datetime | pd.Timestamp | None = None,
    boundary_tolerance_days: int = 14,
) -> dict[str, Any]:
    result: dict[str, Any] = {"code": code, "rows": int(len(frame)), "valid": False}
    if frame.empty:
        result["reason"] = "empty_history"
        return result
    required = (
        "date", "open", "high", "low", "close", "close_raw", "volume",
        "amount", "turnover", "market_cap",
    )
    missing = [column for column in required if column not in frame.columns]
    if missing:
        result["reason"] = f"missing_columns:{','.join(missing)}"
        return result
    dates = pd.to_datetime(frame["date"], errors="coerce")
    if dates.isna().any():
        result["reason"] = "invalid_dates"
        return result
    if dates.duplicated().any():
        result["reason"] = "duplicate_dates"
        return result
    if not dates.is_monotonic_decreasing:
        result["reason"] = "dates_not_descending"
        return result
    actual_start = pd.Timestamp(dates.min()).tz_localize(None)
    actual_end = pd.Timestamp(dates.max()).tz_localize(None)
    result["date_start"] = actual_start.strftime("%Y-%m-%d")
    result["date_end"] = actual_end.strftime("%Y-%m-%d")
    tolerance = pd.Timedelta(days=max(0, int(boundary_tolerance_days)))
    if expected_start is not None:
        archive_start = pd.Timestamp(expected_start).tz_localize(None)
        result["expected_date_start"] = archive_start.strftime("%Y-%m-%d")
        if actual_start > archive_start + tolerance:
            result["reason"] = "history_start_truncated"
            return result
    if expected_end is not None:
        archive_end = pd.Timestamp(expected_end).tz_localize(None)
        result["expected_date_end"] = archive_end.strftime("%Y-%m-%d")
        if actual_end < archive_end - tolerance:
            result["reason"] = "history_end_truncated"
            return result

    prices = {
        column: pd.to_numeric(frame[column], errors="coerce")
        for column in ("open", "high", "low", "close", "close_raw")
    }
    invalid_price = pd.Series(False, index=frame.index)
    for values in prices.values():
        invalid_price |= values.isna() | ~values.map(math.isfinite) | (values <= 0)
    result["invalid_price_rows"] = int(invalid_price.sum())
    if invalid_price.any():
        result["reason"] = "invalid_ohlc_price"
        return result
    price_tolerance = 1e-9
    invalid_ohlc = (
        (prices["high"] + price_tolerance < prices["open"])
        | (prices["high"] + price_tolerance < prices["close"])
        | (prices["low"] - price_tolerance > prices["open"])
        | (prices["low"] - price_tolerance > prices["close"])
        | (prices["high"] + price_tolerance < prices["low"])
    )
    result["invalid_ohlc_rows"] = int(invalid_ohlc.sum())
    if invalid_ohlc.any():
        result["reason"] = "invalid_ohlc_order"
        return result
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    amount = pd.to_numeric(frame["amount"], errors="coerce")
    close_raw = pd.to_numeric(frame["close_raw"], errors="coerce")
    invalid_volume = volume.isna() | ~volume.map(math.isfinite) | (volume < 0)
    result["invalid_volume_rows"] = int(invalid_volume.sum())
    if invalid_volume.any():
        result["reason"] = "invalid_volume"
        return result
    positive_volume = volume > 0
    amount_sentinel = amount.isin(THSDataSource.MISSING_SENTINELS)
    result["amount_sentinel_rows"] = int(amount_sentinel.sum())
    if amount_sentinel.any():
        result["reason"] = "amount_sentinel"
        return result
    valid_amount = positive_volume & amount.notna() & (amount > 0)
    amount_coverage = float(valid_amount.sum() / max(1, positive_volume.sum()))
    result["amount_rows"] = int(valid_amount.sum())
    result["amount_coverage"] = amount_coverage
    if amount_coverage < min_amount_coverage:
        result["reason"] = f"amount_coverage<{min_amount_coverage}"
        return result
    # A broad VWAP/close check catches share/lot unit changes without assuming
    # close is the exact daily average.  Backward adjustment can be affine
    # (multiplier plus cash-dividend additive), so adjusted OHLC cannot in
    # general be divided by close/close_raw to reconstruct raw high/low.
    close = pd.to_numeric(frame["close"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    high = pd.to_numeric(frame["high"], errors="coerce")
    vwap = amount / volume.replace(0, pd.NA)
    vwap_to_close = vwap / close_raw.replace(0, pd.NA)
    broad_mismatch = ~vwap_to_close.between(0.5, 2.0)
    # IPO first days can legitimately close several times above their VWAP.
    # The source adapter already checked every non-null amount against the raw
    # intraday range before dropping raw OHLC. Give the opening window a bounded
    # 0.2x lower limit so a later affine adjustment does not create a false
    # rejection, while obviously corrupt amounts and persistent unit regimes
    # still fail. ChiNext/STAR listings can remain uncapped for their first five
    # trading days, so the window covers those five sessions.
    ipo_window_dates = dates.sort_values(kind="stable").iloc[:5]
    ipo_exception = dates.isin(ipo_window_dates) & vwap_to_close.between(0.2, 2.0)
    trade_value_mismatch = valid_amount & broad_mismatch & ~ipo_exception
    result["trade_value_mismatch_rows"] = int(trade_value_mismatch.sum())
    if trade_value_mismatch.any():
        result["reason"] = "trade_value_price_mismatch"
        return result
    valid_cap = (
        positive_volume
        & (close_raw > 0)
        & (pd.to_numeric(frame["turnover"], errors="coerce") > 0)
        & pd.to_numeric(frame["market_cap"], errors="coerce").notna()
    )
    coverage = float(valid_cap.sum() / max(1, positive_volume.sum()))
    result["positive_volume_rows"] = int(positive_volume.sum())
    result["market_cap_rows"] = int(valid_cap.sum())
    result["market_cap_coverage"] = coverage
    if coverage < min_cap_coverage:
        result["reason"] = f"market_cap_coverage<{min_cap_coverage}"
        return result
    # Recompute the mandated formula and reject unit/field mixups.
    expected = (
        pd.to_numeric(frame["close_raw"], errors="coerce")
        * pd.to_numeric(frame["volume"], errors="coerce")
        * 100.0
        / pd.to_numeric(frame["turnover"], errors="coerce")
    )
    comparable = valid_cap & expected.notna() & (expected > 0)
    relative_error = ((pd.to_numeric(frame["market_cap"], errors="coerce") / expected) - 1.0).abs()
    max_error = float(relative_error[comparable].max()) if comparable.any() else float("inf")
    result["formula_max_relative_error"] = max_error
    if max_error > 1e-9:
        result["reason"] = "market_cap_formula_mismatch"
        return result
    result["valid"] = True
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _permission_probe(source: THSDataSource, code: str, start: str, end: str) -> dict[str, Any]:
    """Probe the exact historical turnover route before staging any files."""
    try:
        frame = source.fetch_turnover_history(code, start, end)
    except THSHistoryPermissionError as exc:
        return {"ok": False, "error_type": type(exc).__name__, "error": str(exc)}
    except THSDataSourceError as exc:
        return {"ok": False, "error_type": type(exc).__name__, "error": str(exc)}
    return {"ok": not frame.empty, "rows": int(len(frame)), "error": None if not frame.empty else "empty_history_turnover"}


def _commit_staging(data_dir: Path, stage_dir: Path, backup_root: Path) -> dict[str, Any]:
    """Back up stock dirs/caches and switch staged stock dirs into place."""
    data_dir = data_dir.resolve()
    stage_dir = stage_dir.resolve()
    backup_root = backup_root.resolve()
    if data_dir in backup_root.parents or backup_root == data_dir:
        raise RuntimeError("backup path must be outside the live data directory")
    if backup_root.exists():
        raise RuntimeError(f"backup path already exists: {backup_root}")
    backup_root.mkdir(parents=True, exist_ok=True)
    installs: list[tuple[Path, Path]] = []
    for prefix in STOCK_PREFIXES:
        live = data_dir / prefix
        staged = stage_dir / prefix
        if not staged.exists():
            # A legitimate universe may have no files under one prefix.  Make
            # that empty directory explicit so the swap remains deterministic.
            staged.mkdir(parents=True, exist_ok=True)
        installs.append((staged, live))

    # Move every live artifact to the backup first. If any later rename fails,
    # restore them in reverse order so the database never remains half-swapped.
    backup_items: list[tuple[Path, Path]] = []
    for prefix in STOCK_PREFIXES:
        live = data_dir / prefix
        if live.exists():
            backup_items.append((live, backup_root / prefix))
    for cache_name in ("indicators_cache", "raw_parquet", "signal_cache"):
        cache = data_dir / cache_name
        if cache.exists():
            backup_items.append((cache, backup_root / cache_name))
    old_manifest = data_dir / DATASET_MANIFEST
    if old_manifest.exists():
        backup_items.append((old_manifest, backup_root / DATASET_MANIFEST))

    moved_backups: list[tuple[Path, Path]] = []
    installed: list[tuple[Path, Path]] = []
    try:
        for live, backup in backup_items:
            os.replace(live, backup)
            moved_backups.append((live, backup))
        for staged, live in installs:
            os.replace(staged, live)
            installed.append((staged, live))
    except Exception:
        for staged, live in reversed(installed):
            if live.exists():
                os.replace(live, staged)
        for live, backup in reversed(moved_backups):
            if backup.exists():
                os.replace(backup, live)
        raise
    return {"backup_root": str(backup_root), "switched_prefixes": list(STOCK_PREFIXES)}


def rebuild(
    data_dir: str | Path = "data",
    *,
    start: str = "1990-01-01",
    end: str | None = None,
    max_stocks: int | None = None,
    stage_dir: str | Path | None = None,
    commit: bool = False,
    min_cap_coverage: float = 0.90,
    source: THSDataSource | None = None,
) -> int:
    data_dir = Path(data_dir).resolve()
    end = end or date.today().isoformat()
    existing_files = stock_files(data_dir)
    existing_codes = {path.stem for path in existing_files}
    if commit and max_stocks is not None:
        raise ValueError("--commit requires the complete local universe; use --max-stocks only for staging tests")
    # Keep staging outside live data so a failed run cannot alter CSVs.
    temporary_stage = stage_dir is None
    if stage_dir is None:
        stage_dir = Path(tempfile.mkdtemp(prefix="ths-rebuild-", dir=str(data_dir.parent)))
    else:
        stage_dir = Path(stage_dir).resolve()
        stage_dir.mkdir(parents=True, exist_ok=True)
    if stage_dir == data_dir or data_dir in stage_dir.parents:
        raise ValueError("THS staging directory must be outside the live data directory")
    if stage_dir.anchor.lower() != data_dir.anchor.lower():
        raise ValueError("THS staging and live data must be on the same volume for atomic directory swaps")
    report_path = stage_dir / "rebuild_report.json"
    own_source = source is None
    source = source or THSDataSource()
    report: dict[str, Any] = {}
    try:
        if hasattr(source, "fetch_stock_universe"):
            current_universe = source.fetch_stock_universe()
            current_codes = set(current_universe)
        else:
            # Injected unit-test sources may intentionally implement only the
            # history slice under test.
            current_codes = set()
        codes = sorted(existing_codes | current_codes)
        if max_stocks is not None:
            codes = codes[:max_stocks]
        if not codes:
            raise RuntimeError("THSDK and the local archive returned an empty stock universe")
        report = {
            "source": "thsdk",
            "data_dir": str(data_dir),
            "start": start,
            "end": end,
            "existing_archive_codes": len(existing_codes),
            "current_ths_codes": len(current_codes),
            "requested_files": len(codes),
            "commit_requested": commit,
            "permission_probe": None,
            "stocks": [],
        }
        probe_code = "000001" if "000001" in codes else codes[0]
        probe = _permission_probe(source, probe_code, start, end)
        report["permission_probe"] = probe
        if not probe.get("ok"):
            report["status"] = "BLOCKED_HISTORICAL_PERMISSION"
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[BLOCKED] THSDK 历史换手率不可用: {probe.get('error')}")
            return 3
        failed = 0
        for index, code in enumerate(codes, 1):
            try:
                history = _normalise_history(source.fetch_history(code, start, end))
                validation = validate_history(history, code, min_cap_coverage=min_cap_coverage)
                if not validation["valid"]:
                    failed += 1
                    validation["status"] = "failed_validation"
                else:
                    target = stage_dir / code[:2] / f"{code}.csv"
                    _atomic_write(history, target)
                    validation["status"] = "staged"
                    validation["sha256"] = _sha256(target)
            except Exception as exc:
                failed += 1
                validation = {"code": code, "status": "failed", "error_type": type(exc).__name__, "error": str(exc)}
            report["stocks"].append(validation)
            if index == 1 or index % 100 == 0 or validation.get("status", "").startswith("failed"):
                print(f"{index}/{len(codes)} {code} {validation.get('status')}", flush=True)
        report["failed"] = failed
        report["staged"] = len(codes) - failed
        if failed:
            report["status"] = "FAILED_NO_COMMIT"
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[STOP] {failed} 个股票未通过校验，未替换现有数据库。stage={stage_dir}")
            return 2
        report["status"] = "STAGED_READY" if not commit else "COMMITTING"
        if commit:
            backup_root = data_dir.parent / f"{data_dir.name}_ths_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            report["commit"] = _commit_staging(data_dir, stage_dir, backup_root)
            report["status"] = "COMMITTED"
            manifest = {
                "status": "COMMITTED",
                "source": "thsdk",
                "schema_version": DATASET_SCHEMA_VERSION,
                "data_quality_version": DATA_QUALITY_VERSION,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "start": start,
                "end": end,
                "stock_count": len(codes),
                "price_adjustment": "backward",
                "raw_close_column": "close_raw",
                "market_cap_semantics": "circulating_market_cap_cny",
                "market_cap_formula": "close_raw * volume * 100 / turnover_pct",
                "backup_root": str(backup_root),
            }
            manifest_path = data_dir / DATASET_MANIFEST
            temporary_manifest = manifest_path.with_suffix(".tmp")
            temporary_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temporary_manifest, manifest_path)
            report["dataset_manifest"] = str(manifest_path)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] THS 重建完成: {report['status']} files={len(codes)}")
        if not commit:
            print(f"隔离目录: {stage_dir}; 如需切换请在复核报告后加 --commit")
        return 0
    finally:
        if own_source:
            source.close()
        if temporary_stage and (not report.get("status") in {"STAGED_READY", "COMMITTED"}):
            # Preserve failed/blocked reports for diagnosis; successful staged
            # runs are also preserved because commit is an explicit second step.
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="THSDK 全量历史数据库重建")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--start", default="1990-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--max-stocks", type=int, default=None)
    parser.add_argument("--stage-dir", default=None)
    parser.add_argument("--commit", action="store_true", help="所有校验通过后备份并切换 live CSV")
    parser.add_argument("--min-cap-coverage", type=float, default=0.90)
    args = parser.parse_args()
    return rebuild(
        args.data_dir,
        start=args.start,
        end=args.end,
        max_stocks=args.max_stocks,
        stage_dir=args.stage_dir,
        commit=args.commit,
        min_cap_coverage=args.min_cap_coverage,
    )


if __name__ == "__main__":
    raise SystemExit(main())
