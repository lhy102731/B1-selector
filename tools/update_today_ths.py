"""同花顺每日增量更新。

该脚本只写入 THSDK 返回的 K 线和当前换手率；不会在历史换手率权限不足时
悄悄回退到东方财富、腾讯或 Baostock。首次完整重建由
``tools/rebuild_all_ths.py`` 负责。
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.csv_manager import CSVManager
from utils.checkpoint_retention import prune_checkpoint_history
from utils.process_lock import process_lock
from utils.ths_data_source import THSDataSource, THSDataSourceError, THSHistoryPermissionError
from tools.rebuild_all_ths import _normalise_history, validate_history


DATA_DIR = ROOT / "data"
TODAY_DATE = date.today()
TODAY_STR = TODAY_DATE.isoformat()
CHECKPOINT_DIR = DATA_DIR / "_daily_updates" / TODAY_STR
BACKUP_DIR = CHECKPOINT_DIR / "backup"
REPORT_PATH = CHECKPOINT_DIR / "ths_update_report.csv"
VALIDATION_PATH = CHECKPOINT_DIR / "checkpoint_validation.json"
UPDATE_CACHE_PATH = DATA_DIR / ".update_cache.json"
DATASET_MANIFEST = ".ths_dataset_manifest.json"
UPDATE_LOCK_FILENAME = ".ths_daily_update.lock"
MIN_DATASET_SCHEMA_VERSION = 3
MIN_DATA_QUALITY_VERSION = 4


def iter_stock_files(data_dir: Path = DATA_DIR, max_stocks: int | None = None) -> list[Path]:
    files: list[Path] = []
    for prefix in ("00", "30", "60", "68"):
        files.extend(sorted((data_dir / prefix).glob("*.csv")))
    return files[:max_stocks] if max_stocks else files


def validate_dataset_manifest(
    data_dir: Path,
    *,
    recover_interrupted_new_history: bool = False,
) -> dict[str, Any]:
    """Require a fully committed THS baseline before incremental writes."""
    path = data_dir / DATASET_MANIFEST
    if not path.exists():
        raise RuntimeError(
            "THS daily update requires a fully rebuilt THS baseline; "
            f"missing {path}"
        )
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid THS dataset manifest: {path}") from exc
    identity = (manifest.get("source"), manifest.get("status"))
    accepted = {("thsdk", "COMMITTED"), ("thsdk+yuanhang", "COMPLETED")}
    if identity not in accepted:
        raise RuntimeError("THS dataset manifest is not a completed unified THS baseline")
    for field, minimum in (
        ("schema_version", MIN_DATASET_SCHEMA_VERSION),
        ("data_quality_version", MIN_DATA_QUALITY_VERSION),
    ):
        try:
            version = int(manifest.get(field, 0))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"THS dataset manifest has invalid {field}") from exc
        if version < minimum:
            raise RuntimeError(
                f"THS dataset manifest requires {field}>={minimum}; got {version}"
            )
    live_codes = _live_inventory_codes(data_dir)
    live_count = len(live_codes)
    try:
        recorded_count = int(manifest.get("stock_count", -1))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("THS dataset manifest has invalid stock_count") from exc
    inventory_codes = _manifest_inventory_codes(manifest)
    if inventory_codes is not None and len(inventory_codes) != recorded_count:
        raise RuntimeError(
            "THS dataset manifest inventory_codes count "
            f"{len(inventory_codes)} does not match stock_count={recorded_count}"
        )
    if recorded_count != live_count and inventory_codes is None:
        raise RuntimeError(
            "THS dataset manifest lacks inventory_codes; an existing count "
            "mismatch cannot be recovered safely. Run one consistent unbounded "
            "update to bootstrap the exact inventory first."
        )
    if recorded_count != live_count or (
        inventory_codes is not None and inventory_codes != live_codes
    ):
        if recover_interrupted_new_history:
            manifest = _recover_interrupted_new_history_inventory(
                data_dir,
                manifest,
                live_count=live_count,
            )
            live_codes = _live_inventory_codes(data_dir)
            live_count = len(live_codes)
            recorded_count = int(manifest.get("stock_count", -1))
            inventory_codes = _manifest_inventory_codes(manifest)
    if recorded_count != live_count:
        raise RuntimeError(
            f"THS dataset manifest stock_count={manifest.get('stock_count')} "
            f"does not match live CSV count={live_count}"
        )
    if inventory_codes is not None and inventory_codes != live_codes:
        missing = sorted(inventory_codes - live_codes)
        unexpected = sorted(live_codes - inventory_codes)
        raise RuntimeError(
            "THS dataset inventory mismatch: "
            f"missing={missing[:10]} unexpected={unexpected[:10]}"
        )
    return manifest


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
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


def _atomic_json(payload: dict[str, Any], path: Path) -> None:
    """Replace a dataset metadata file without exposing a partial JSON file.

    The stock CSVs are committed one at a time during a daily run.  Keeping
    the manifest replacement atomic is important because ``validate_dataset_manifest``
    is used as the next run's consistency gate: readers must see either the
    old, self-consistent manifest or the new one, never a truncated document.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", dir=path.parent,
            prefix=f".{path.name}.", suffix=".tmp", delete=False,
        ) as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _universe_codes(universe: Any) -> set[str]:
    """Normalise the two universe shapes accepted by THS test/live clients."""
    values = universe.keys() if isinstance(universe, dict) else universe
    codes: set[str] = set()
    for value in values or ():
        code = str(value).strip().zfill(6)
        if len(code) == 6 and code.isdigit() and code.startswith(("00", "30", "60", "68")):
            codes.add(code)
    return codes


def _is_no_history_error(exc: BaseException) -> bool:
    """Return whether a THS error means that a symbol has no K-line yet.

    Permission/authentication errors must remain hard failures.  The Yuanhang
    bridge and THSDK have used several slightly different messages for an
    unknown/new listing, so classify only explicit *no data* wording and never
    a generic exception.
    """
    if isinstance(exc, THSHistoryPermissionError):
        return False
    message = str(exc).casefold()
    permission_markers = (
        "permission",
        "authorized",
        "authorization",
        "auth",
        "guest",
        "denied",
        "login",
        "access",
        "登录",
        "权限",
        "授权",
    )
    if any(marker in message for marker in permission_markers):
        return False
    markers = (
        "not data",
        "no data",
        "no history",
        "empty history",
        "no kline",
        "empty kline",
        "no bar",
        "no price",
        "not found",
        "returned empty",
        "empty response",
        "没有数据",
        "无数据",
        "无历史",
    )
    if any(marker in message for marker in markers):
        return True
    return "empty" in message and any(
        token in message for token in ("kline", "history", "bar", "price", "response", "data")
    )


def _manifest_no_history_codes(manifest: dict[str, Any] | None) -> set[str]:
    if not manifest:
        return set()
    values = manifest.get("no_history_codes", [])
    if not isinstance(values, (list, tuple, set)):
        return set()
    return {
        code
        for code in (str(value).strip().zfill(6) for value in values)
        if len(code) == 6 and code.isdigit()
    }


def _manifest_pending_inventory_additions(manifest: dict[str, Any] | None) -> set[str]:
    if not manifest:
        return set()
    values = manifest.get("pending_inventory_additions", [])
    if not isinstance(values, (list, tuple, set)):
        return set()
    return {
        code
        for code in (str(value).strip().zfill(6) for value in values)
        if len(code) == 6 and code.isdigit()
    }


def _manifest_inventory_codes(manifest: dict[str, Any] | None) -> set[str] | None:
    if not manifest or "inventory_codes" not in manifest:
        return None
    values = manifest.get("inventory_codes")
    if not isinstance(values, (list, tuple, set)):
        raise RuntimeError("THS dataset manifest has invalid inventory_codes")
    normalized = [str(value).strip().zfill(6) for value in values]
    if any(
        len(code) != 6
        or not code.isdigit()
        or not code.startswith(("00", "30", "60", "68"))
        for code in normalized
    ):
        raise RuntimeError("THS dataset manifest has invalid inventory_codes")
    codes = set(normalized)
    if len(codes) != len(normalized):
        raise RuntimeError("THS dataset manifest has duplicate inventory_codes")
    return codes


def _live_inventory_codes(data_dir: Path) -> set[str]:
    files = iter_stock_files(data_dir)
    codes = [path.stem for path in files]
    unique = set(codes)
    if len(unique) != len(codes):
        raise RuntimeError("THS dataset inventory contains duplicate stock CSV codes")
    return unique


def _sync_manifest_inventory(
    data_dir: Path,
    manifest: dict[str, Any] | None,
    no_history_codes: set[str],
    *,
    pending_inventory_additions: set[str] | None = None,
    recovery_codes: set[str] | None = None,
    mark_daily_update: bool = False,
    completed_data_date: str | None = None,
) -> dict[str, Any] | None:
    """Persist stock-file inventory independently of the full daily checkpoint.

    Existing stock files are updated idempotently, but a newly listed stock
    changes the manifest's structural inventory.  Persist the pending intent
    before the CSV and the exact inventory after it so either side of an
    interruption remains recoverable without weakening the strict gate.
    """
    if manifest is None:
        return None
    live_codes = _live_inventory_codes(data_dir)
    live_count = len(live_codes)
    normalized_no_history = sorted(no_history_codes)
    normalized_pending = sorted(
        _manifest_pending_inventory_additions(manifest)
        if pending_inventory_additions is None
        else pending_inventory_additions
    )
    daily_marker = completed_data_date
    if mark_daily_update and daily_marker is None:
        raise ValueError(
            "completed_data_date is required when marking a daily update"
        )
    current_inventory = _manifest_inventory_codes(manifest)
    if current_inventory is not None:
        current_pending = _manifest_pending_inventory_additions(manifest)
        # Only a previously committed intent (or an explicitly validated
        # recovery candidate) may authorize a new live file.  The pending set
        # supplied by this same write cannot retroactively bless an orphan.
        allowed_additions = current_pending | set(recovery_codes or ())
        missing = current_inventory - live_codes
        unexpected = live_codes - current_inventory - allowed_additions
        if missing or unexpected:
            raise RuntimeError(
                "THS dataset inventory transition mismatch: "
                f"missing={sorted(missing)[:10]} "
                f"unexpected={sorted(unexpected)[:10]}"
            )
    changed = (
        int(manifest.get("stock_count", -1)) != live_count
        or sorted(_manifest_no_history_codes(manifest)) != normalized_no_history
        or sorted(_manifest_pending_inventory_additions(manifest)) != normalized_pending
        or current_inventory != live_codes
        or (
            mark_daily_update
            and (
                manifest.get("last_daily_update") != daily_marker
                or manifest.get("last_daily_update_source") != "thsdk"
            )
        )
    )
    if not changed and not recovery_codes:
        return manifest
    updated = dict(manifest)
    updated["stock_count"] = live_count
    updated["inventory_codes"] = sorted(live_codes)
    updated["no_history_codes"] = normalized_no_history
    if normalized_pending:
        updated["pending_inventory_additions"] = normalized_pending
    else:
        updated.pop("pending_inventory_additions", None)
    updated["updated_at"] = datetime.now().isoformat(timespec="seconds")
    if recovery_codes:
        updated["last_inventory_recovery"] = {
            "reason": "interrupted_new_history_manifest_commit",
            "codes": sorted(recovery_codes),
            "recovered_at": datetime.now().isoformat(timespec="seconds"),
        }
    if mark_daily_update:
        updated["last_daily_update"] = daily_marker
        updated["last_daily_update_source"] = "thsdk"
    _atomic_json(updated, data_dir / DATASET_MANIFEST)
    manifest.clear()
    manifest.update(updated)
    return manifest


def _recover_interrupted_new_history_inventory(
    data_dir: Path,
    manifest: dict[str, Any],
    *,
    live_count: int,
) -> dict[str, Any]:
    """Recover only the bounded orphan-file state produced by a killed run.

    A safe orphan must already have a write-ahead pending intent and must pass
    the same history validator as a normal THS rebuild.  A legacy no-history
    marker alone cannot prove who wrote a new CSV, so it remains a hard failure.
    """
    try:
        recorded_count = int(manifest.get("stock_count", -1))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("THS dataset manifest has invalid stock_count") from exc
    excess = live_count - recorded_count
    if excess <= 0:
        return manifest
    no_history_codes = _manifest_no_history_codes(manifest)
    pending_additions = _manifest_pending_inventory_additions(manifest)
    inventory_codes = _manifest_inventory_codes(manifest)
    candidates = [
        path
        for path in iter_stock_files(data_dir)
        if path.stem in pending_additions
    ]
    if len(candidates) != excess:
        return manifest
    candidate_codes = {path.stem for path in candidates}
    if inventory_codes is not None:
        expected_live_codes = inventory_codes | candidate_codes
        live_codes = _live_inventory_codes(data_dir)
        if live_codes != expected_live_codes:
            missing = sorted(inventory_codes - live_codes)
            unexpected = sorted(live_codes - expected_live_codes)
            raise RuntimeError(
                "THS dataset inventory mismatch during recovery: "
                f"missing={missing[:10]} unexpected={unexpected[:10]}"
            )
    recovered: set[str] = set()
    for path in candidates:
        history = _normalise_history(_read(path))
        validation = validate_history(history, path.stem)
        if not validation.get("valid"):
            return manifest
        recovered.add(path.stem)
    no_history_codes.difference_update(recovered)
    pending_additions.difference_update(recovered)
    _sync_manifest_inventory(
        data_dir,
        manifest,
        no_history_codes,
        pending_inventory_additions=pending_additions,
        recovery_codes=recovered,
    )
    print(
        "[RECOVER] reconciled interrupted THS listing inventory: "
        + ",".join(sorted(recovered)),
        flush=True,
    )
    return manifest


def _fetch_realtime_batch_or_empty(
    source: THSDataSource,
    codes: list[str],
) -> dict[str, dict[str, Any]]:
    """Avoid issuing an empty THS snapshot request after an all-new listing run."""
    if not codes:
        return {}
    return source.fetch_realtime_batch(codes)


def _add_new_history_files(
    *,
    data_dir: Path,
    source: THSDataSource,
    current_codes: set[str],
    local_codes: set[str],
    manifest: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], set[str], set[str], set[str]]:
    """Fetch and validate current THS listings absent from the local tree.

    This is intentionally separate from the incremental K-line merge.  A new
    listing needs a full ``fetch_history`` call and should be written only
    after the same normalisation/validation gate used by ``fetch_all_ths``.
    The returned ``added_codes`` lets the caller avoid fetching the same full
    history a second time in ``_merge_one``.
    """
    missing = sorted(current_codes - local_codes)
    if not missing:
        return (
            [],
            _manifest_no_history_codes(manifest),
            set(),
            _manifest_pending_inventory_additions(manifest),
        )

    # Preserve the baseline's requested start where available.  New listings
    # generally have a much shorter history, but asking THS from the baseline
    # start keeps the semantics identical to a full rebuild and handles a
    # relisted/late-completed symbol without a second provider.
    start = str((manifest or {}).get("start") or "1990-01-01")
    end = TODAY_STR
    no_history = _manifest_no_history_codes(manifest)
    pending_additions = _manifest_pending_inventory_additions(manifest)
    rows: list[dict[str, Any]] = []
    added_codes: set[str] = set()

    for code in missing:
        started = datetime.now().timestamp()
        target = data_dir / code[:2] / f"{code}.csv"
        try:
            fetched = source.fetch_history(code, start, end)
            if fetched is None or fetched.empty:
                pending_additions.discard(code)
                no_history.add(code)
                _sync_manifest_inventory(
                    data_dir,
                    manifest,
                    no_history,
                    pending_inventory_additions=pending_additions,
                )
                rows.append({
                    "code": code,
                    "status": "no_history_yet",
                    "reason": "THS current universe contains the code but no K-line exists through end date",
                    "elapsed_seconds": round(datetime.now().timestamp() - started, 4),
                })
                continue

            history = _normalise_history(fetched)
            validation = validate_history(history, code)
            validation["status"] = "added" if validation.get("valid") else "failed_validation"
            validation["elapsed_seconds"] = round(datetime.now().timestamp() - started, 4)
            if validation.get("valid"):
                pending_additions.add(code)
                _sync_manifest_inventory(
                    data_dir,
                    manifest,
                    no_history,
                    pending_inventory_additions=pending_additions,
                )
                _atomic_csv(history, target)
                added_codes.add(code)
                no_history.discard(code)
                pending_additions.discard(code)
                _invalidate_indicator_cache(code, data_dir)
                _sync_manifest_inventory(
                    data_dir,
                    manifest,
                    no_history,
                    pending_inventory_additions=pending_additions,
                )
            else:
                pending_additions.discard(code)
                _sync_manifest_inventory(
                    data_dir,
                    manifest,
                    no_history,
                    pending_inventory_additions=pending_additions,
                )
                rows.append(validation)
                continue
            rows.append(validation)
        except Exception as exc:
            if _is_no_history_error(exc):
                pending_additions.discard(code)
                no_history.add(code)
                _sync_manifest_inventory(
                    data_dir,
                    manifest,
                    no_history,
                    pending_inventory_additions=pending_additions,
                )
                rows.append({
                    "code": code,
                    "status": "no_history_yet",
                    "reason": str(exc),
                    "error_type": type(exc).__name__,
                    "elapsed_seconds": round(datetime.now().timestamp() - started, 4),
                })
            else:
                if code in pending_additions and not target.exists():
                    pending_additions.discard(code)
                    _sync_manifest_inventory(
                        data_dir,
                        manifest,
                        no_history,
                        pending_inventory_additions=pending_additions,
                    )
                rows.append({
                    "code": code,
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "elapsed_seconds": round(datetime.now().timestamp() - started, 4),
                })

    return rows, no_history, added_codes, pending_additions


def _backup(path: Path, data_dir: Path, backup_dir: Path) -> None:
    relative = path.relative_to(data_dir)
    target = backup_dir / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        shutil.copy2(path, target)


def _invalidate_indicator_cache(code: str, data_dir: Path) -> None:
    cache = data_dir / "indicators_cache" / f"{code}.parquet"
    if cache.exists():
        cache.unlink()


def _read(path: Path) -> pd.DataFrame:
    # Existing rows are serialized again when a new daily bar is appended.
    # Preserve unrelated floating-point values exactly across that round trip.
    return pd.read_csv(path, encoding="gbk", float_precision="round_trip")


def _normalise_dates(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
    return out.dropna(subset=["date"])


def _rebase_remote_adjusted_ohlc(
    local: pd.DataFrame,
    remote: pd.DataFrame,
) -> pd.DataFrame:
    """Anchor a short THS price slice to the committed full-history scale.

    Most symbols can use THS's short-window adjusted OHLC directly.  For the
    small set whose full history had to be rebuilt from corporate actions, a
    short request may look healthy and therefore return THS's incompatible
    adjusted series.  Evaluate both that series and raw OHLC against committed
    overlap closes, then use the lower-dispersion mapping for the new bar.
    """
    price_columns = ("open", "high", "low", "close")
    candidates: list[tuple[float, int, str, float]] = []

    def evaluate(remote_close_column: str, mode: str, preference: int) -> None:
        overlap = local[["date", "close"]].rename(
            columns={"close": "local_close"}
        ).merge(
            remote[["date", remote_close_column]].rename(
                columns={remote_close_column: "remote_close"}
            ),
            on="date",
            how="inner",
        )
        local_close = pd.to_numeric(overlap["local_close"], errors="coerce")
        remote_close = pd.to_numeric(overlap["remote_close"], errors="coerce")
        valid = (local_close > 0) & (remote_close > 0)
        if not valid.any():
            return
        ratios = local_close[valid] / remote_close[valid]
        scale = float(ratios.median())
        dispersion = float((ratios / scale - 1.0).abs().max())
        if math.isfinite(scale) and scale > 0 and math.isfinite(dispersion):
            candidates.append((dispersion, preference, mode, scale))

    evaluate("close", "adjusted", 0)
    raw_columns = tuple(f"{column}_raw" for column in price_columns)
    if set(raw_columns).issubset(remote.columns):
        evaluate("close_raw", "raw", 1)
    if not candidates:
        return remote
    dispersion, _, mode, scale = min(candidates)
    if dispersion > 0.01:
        raise THSDataSourceError(
            f"THS adjusted-price overlap cannot be rebased safely: dispersion={dispersion:.6f}"
        )
    result = remote.copy()
    for column in price_columns:
        source_column = column if mode == "adjusted" else f"{column}_raw"
        result[column] = pd.to_numeric(result[source_column], errors="coerce") * scale
    result.attrs.update(remote.attrs)
    result.attrs["price_rebase_mode"] = mode
    result.attrs["price_rebase_dispersion"] = dispersion
    return result


def _float(value: Any) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(parsed) else float(parsed)


def _is_valid_number(value: Any, positive: bool = False) -> bool:
    parsed = _float(value)
    return parsed is not None and math.isfinite(parsed) and (not positive or parsed > 0)


def _completed_bar_mask(frame: pd.DataFrame) -> pd.Series:
    """Identify actual traded daily bars, excluding suspension placeholders."""
    required = ("open", "high", "low", "close", "close_raw", "volume", "amount")
    if any(column not in frame.columns for column in required):
        return pd.Series(False, index=frame.index, dtype=bool)
    values = {
        column: pd.to_numeric(frame[column], errors="coerce").replace(
            [float("inf"), float("-inf")], pd.NA
        )
        for column in required
    }
    mask = pd.Series(True, index=frame.index, dtype=bool)
    for column in required:
        mask &= values[column].notna() & (values[column] > 0)
    mask &= values["high"] >= values["open"]
    mask &= values["high"] >= values["close"]
    mask &= values["low"] <= values["open"]
    mask &= values["low"] <= values["close"]
    return mask


def _latest_completed_data_date(rows: list[dict[str, Any]]) -> str | None:
    """Return the newest completed K-line date actually observed in this run."""
    dates: list[pd.Timestamp] = []
    for row in rows:
        if row.get("status") not in {"updated", "no_today_bar", "added"}:
            continue
        for field in ("local_last_date", "remote_last_date", "date_end"):
            parsed = pd.to_datetime(row.get(field), errors="coerce")
            if pd.notna(parsed):
                dates.append(pd.Timestamp(parsed).tz_localize(None))
    return max(dates).strftime("%Y-%m-%d") if dates else None


def _merge_one(
    path: Path,
    source: THSDataSource,
    quote: dict[str, Any] | None,
    *,
    data_dir: Path,
    lookback_days: int = 15,
) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    code = path.stem
    local = _normalise_dates(_read(path))
    if local.empty:
        return None, {"code": code, "status": "empty_or_bad_csv"}
    local_max = local["date"].max()
    local_last_date = local_max.strftime("%Y-%m-%d")
    start = (local_max.date() - timedelta(days=max(5, int(lookback_days)))).isoformat()
    end = TODAY_STR
    remote = source.fetch_klines(code, start, end)
    if remote.empty:
        return None, {
            "code": code,
            "status": "no_today_bar",
            "local_last_date": local_last_date,
        }
    remote = _normalise_dates(remote)
    remote = remote.loc[_completed_bar_mask(remote)].copy()
    if remote.empty:
        return None, {
            "code": code,
            "status": "no_today_bar",
            "local_last_date": local_last_date,
        }
    remote = _rebase_remote_adjusted_ohlc(local, remote)
    remote_max = remote["date"].max()
    new_dates = set(remote.loc[remote["date"] > local_max, "date"])
    if not new_dates:
        return None, {
            "code": code,
            "status": "no_today_bar",
            "local_last_date": local_last_date,
            "remote_last_date": remote_max.strftime("%Y-%m-%d"),
            "new_rows": 0,
            "touched_rows": 0,
        }

    # Normally the current snapshot supplies the one new bar's turnover.  A gap
    # of multiple bars requires the historical 212 endpoint; if the account is
    # a guest this fails closed instead of importing a different provider.
    turnover_by_date: dict[pd.Timestamp, float] = {}
    used_historical_turnover = False
    if len(new_dates) > 1:
        historical = source.fetch_turnover_history(code, start, end)
        used_historical_turnover = True
        turnover_by_date = {
            pd.Timestamp(row.date): float(row.turnover)
            for row in historical.itertuples(index=False)
            if _is_valid_number(row.turnover, positive=True)
        }
    if quote is None:
        # Batch snapshots can omit an obsolete/delisted symbol.  A single
        # per-code request is still THS-only and keeps active symbols updating.
        try:
            quote = source.fetch_realtime(code)
        except THSDataSourceError:
            quote = None
    if quote and _is_valid_number(quote.get("turnover"), positive=True):
        turnover_by_date[pd.Timestamp(remote_max)] = float(quote["turnover"])
    missing_new = [d.strftime("%Y-%m-%d") for d in sorted(new_dates) if d not in turnover_by_date]
    if missing_new:
        raise THSDataSourceError(f"THSDK turnover unavailable for new dates: {missing_new}")

    original_columns = list(local.columns)
    if "close_raw" not in original_columns:
        insert_at = original_columns.index("close") + 1 if "close" in original_columns else len(original_columns)
        original_columns.insert(insert_at, "close_raw")
    for column in ("market_cap", "turnover", "pe_dynamic", "pb", "ps", "pcf"):
        if column not in original_columns:
            original_columns.append(column)

    # Materialize newly introduced schema columns before row-wise ``.loc``
    # assignment; older pandas versions otherwise drop labels such as
    # ``close_raw`` when the target row already exists.
    for column in original_columns:
        if column not in local.columns:
            local[column] = pd.NA

    # Index existing rows by normalized date.  Only THS-overlap rows are
    # corrected; unrelated optional fields (PE/PB/flow) are retained.
    local = local.drop_duplicates("date", keep="last").set_index("date", drop=False)
    touched_dates: set[pd.Timestamp] = set()
    cap_errors: list[float] = []
    for row in remote.itertuples(index=False):
        day = pd.Timestamp(row.date)
        if day not in new_dates:
            continue
        values = row._asdict()
        if day in local.index:
            target = local.loc[day].copy()
        else:
            target = pd.Series({column: pd.NA for column in original_columns}, dtype="object")
            target["date"] = day
        for field in ("open", "high", "low", "close", "volume", "amount", "close_raw"):
            if field in values:
                target[field] = values[field]
        turnover = turnover_by_date.get(day)
        if turnover is not None:
            target["turnover"] = turnover
            raw_close = _float(target.get("close_raw"))
            volume = _float(target.get("volume"))
            if raw_close is not None and volume is not None and turnover > 0:
                target["market_cap"] = raw_close * volume * 100.0 / turnover
        # THS exposes PE(TTM), PB and PS(TTM) only as a point-in-time quote.
        # Persist them solely on the latest completed bar; applying today's
        # snapshot to earlier gap dates would introduce look-ahead data.
        if quote and day == remote_max:
            for field in ("pe_dynamic", "pb", "ps"):
                value = quote.get(field)
                if _is_valid_number(value):
                    target[field] = float(value)
        # Point-in-time fields from old sources are intentionally untouched if
        # THS did not provide a replacement for that date.
        local.loc[day] = target
        touched_dates.add(day)

    # Validate the current snapshot against the formula when the latest bar is
    # available.  Small differences are expected from rounded volume/turnover.
    latest = remote.loc[remote["date"] == remote_max].iloc[0]
    if quote and _is_valid_number(quote.get("market_cap"), positive=True):
        raw_close = _float(latest.get("close_raw"))
        volume = _float(latest.get("volume"))
        turnover = _float(quote.get("turnover"))
        if raw_close and volume and turnover and turnover > 0:
            derived = raw_close * volume * 100.0 / turnover
            error_pct = abs(derived / float(quote["market_cap"]) - 1.0) * 100.0
            cap_errors.append(error_pct)
            if error_pct > 1.0:
                raise THSDataSourceError(f"THS cap cross-check exceeds 1%: {error_pct:.4f}%")

    result = local.reset_index(drop=True)
    result = result.sort_values("date", ascending=False)
    # Recompute derived daily change fields only for rows whose adjusted prices
    # came from THS; this prevents old-source optional fields being fabricated.
    result_asc = result.sort_values("date").reset_index(drop=True)
    previous = result_asc["close"].shift(1).map(_float)
    current = result_asc["close"].map(_float)
    for column, values in {
        "change": current - previous,
        "change_pct": (current / previous - 1.0) * 100.0,
        "amplitude": (result_asc["high"].map(_float) - result_asc["low"].map(_float)) / previous * 100.0,
    }.items():
        if column in result_asc.columns:
            for idx, day in enumerate(result_asc["date"]):
                if day in touched_dates and pd.notna(values.iloc[idx]):
                    result_asc.at[idx, column] = values.iloc[idx]
    result = result_asc.sort_values("date", ascending=False)
    result["date"] = result["date"].dt.strftime("%Y-%m-%d")
    result = result[[column for column in original_columns if column in result.columns]]
    return result, {
        "code": code,
        "status": "updated",
        "remote_last_date": remote_max.strftime("%Y-%m-%d"),
        "new_rows": len(new_dates),
        "touched_rows": len(touched_dates),
        "historical_turnover": used_historical_turnover,
        "cap_error_pct": max(cap_errors) if cap_errors else None,
        "valuation_source": "ths_realtime" if quote else None,
    }


def _run_unlocked(
    data_dir: str | Path = DATA_DIR,
    max_stocks: int | None = None,
    source: THSDataSource | None = None,
    *,
    require_ths_manifest: bool = True,
) -> int:
    data_dir = Path(data_dir)
    manifest: dict[str, Any] | None = None
    if require_ths_manifest:
        manifest = validate_dataset_manifest(
            data_dir,
            recover_interrupted_new_history=max_stocks is None,
        )
    else:
        # Test/repair callers may deliberately run without a baseline gate.
        # If a manifest is present, still load it so a successful unbounded
        # reconciliation can keep its no-history and stock-count metadata.
        manifest_path = data_dir / DATASET_MANIFEST
        if manifest_path.exists():
            try:
                candidate = json.loads(manifest_path.read_text(encoding="utf-8"))
                if isinstance(candidate, dict):
                    manifest = candidate
            except (OSError, json.JSONDecodeError):
                manifest = None
    files = iter_stock_files(data_dir, max_stocks=max_stocks)
    checkpoint_dir = data_dir / "_daily_updates" / TODAY_STR
    backup_dir = checkpoint_dir / "backup"
    report_path = checkpoint_dir / "ths_update_report.csv"
    validation_path = checkpoint_dir / "checkpoint_validation.json"
    cache_path = data_dir / ".update_cache.json"
    rows: list[dict[str, Any]] = []
    own_source = source is None
    source = source or THSDataSource()
    try:
        # Reconcile newly listed THS symbols only for an unbounded production
        # run.  A bounded smoke test must remain a strict subset operation: it
        # must neither expand the stock tree nor mutate the full-universe
        # manifest/no-history list.
        added_rows: list[dict[str, Any]] = []
        no_history_codes = _manifest_no_history_codes(manifest)
        pending_inventory_additions = _manifest_pending_inventory_additions(manifest)
        added_codes: set[str] = set()
        current_codes: set[str] = set()
        if max_stocks is None and hasattr(source, "fetch_stock_universe"):
            current_codes = _universe_codes(source.fetch_stock_universe())
            local_codes = {path.stem for path in iter_stock_files(data_dir)}
            (
                added_rows,
                no_history_codes,
                added_codes,
                pending_inventory_additions,
            ) = _add_new_history_files(
                data_dir=data_dir,
                source=source,
                current_codes=current_codes,
                local_codes=local_codes,
                manifest=manifest,
            )
            # Include newly added files in the expected-file count, but do not
            # merge them a second time: their full THS history already reaches
            # TODAY_STR and has passed the rebuild validator.
            if added_codes:
                files = iter_stock_files(data_dir, max_stocks=None)
                files = [path for path in files if path.stem not in added_codes]
        if not files:
            # A baseline with only no-history listings is still a valid THS
            # dataset.  It simply has nothing to increment today.  If a run
            # started from an empty tree and added a valid listing, that is
            # also a successful reconciliation even though there is no old
            # file left to merge below.
            if not added_rows:
                raise RuntimeError(f"no stock CSV files found under {data_dir}")

        # One batched snapshot request supplies the latest turnover/caps.
        codes = [path.stem for path in files]
        try:
            quotes = _fetch_realtime_batch_or_empty(source, codes)
        except Exception as exc:
            print(f"[WARN] THS 批量快照失败，将逐股尝试: {exc}")
            quotes = {}
        rows.extend(added_rows)
        for index, path in enumerate(files, len(rows) + 1):
            code = path.stem
            try:
                frame, row = _merge_one(path, source, quotes.get(code), data_dir=data_dir)
                if frame is not None:
                    _backup(path, data_dir, backup_dir)
                    _atomic_csv(frame, path)
                    _invalidate_indicator_cache(code, data_dir)
                rows.append(row)
            except Exception as exc:
                rows.append({"code": code, "status": f"failed:{type(exc).__name__}", "error": str(exc)})
            if index == 1 or index % 100 == 0 or str(rows[-1].get("status", "")).startswith("failed"):
                print(f"{index}/{len(files) + len(added_rows)} status={rows[-1].get('status')}", flush=True)
    finally:
        if own_source:
            source.close()

    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(report_path, index=False, encoding="utf-8-sig")
    updated_count = sum(row.get("status") == "updated" for row in rows)
    added_count = sum(row.get("status") == "added" for row in rows)
    successful = updated_count + added_count
    unchanged = sum(row.get("status") == "no_today_bar" for row in rows)
    legal_no_history = sum(row.get("status") == "no_history_yet" for row in rows)
    failed = len(rows) - successful - unchanged - legal_no_history
    validation = {
        "valid": failed == 0,
        "source": "thsdk",
        # A bounded run reports only its selected slice; advertising the
        # entire live tree here would make a smoke test look like a full
        # production reconciliation.
        "expected_files": (
            len(iter_stock_files(data_dir))
            if max_stocks is None
            else len(files)
        ),
        "updated": updated_count,
        "added": added_count,
        "no_today_bar": unchanged,
        "no_history": legal_no_history,
        "failed": failed,
        "failed_codes": [row.get("code") for row in rows if str(row.get("status", "")).startswith("failed")],
    }
    latest_completed_date = _latest_completed_data_date(rows)
    validation["latest_completed_data_date"] = latest_completed_date
    validation_path.write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
    # A bounded smoke-test run must never mark the whole production dataset as
    # current; otherwise the next unbounded daily run would trust this cache and
    # skip stocks that were not part of the sample.
    if max_stocks is None:
        # Reconcile the manifest after the new-file attempt (valid files are
        # already committed; invalid files are never written).
        # ``stock_count`` is derived from the live tree rather than from the
        # number of successful rows, so a partial run cannot leave the next
        # invocation blocked by a stale count.  No manifest is synthesized for
        # explicitly un-gated test/repair runs.
        if manifest is not None:
            _sync_manifest_inventory(
                data_dir,
                manifest,
                no_history_codes,
                pending_inventory_additions=pending_inventory_additions,
                mark_daily_update=(
                    validation["valid"] and latest_completed_date is not None
                ),
                completed_data_date=latest_completed_date,
            )
        if validation["valid"]:
            cache = {}
            if cache_path.exists():
                try:
                    cache = json.loads(cache_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    cache = {}
            # Cache the newest bar THS actually returned, not the wall-clock
            # date.  Before 15:00 today's incomplete candle is excluded, so a
            # post-close run on the same day must not be skipped.
            if latest_completed_date is not None:
                cache["last_update_date"] = latest_completed_date
                cache["last_update_completed_date"] = latest_completed_date
            cache["last_update_attempt_date"] = TODAY_STR
            cache["last_update_source"] = "thsdk"
            cache_path.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
            if require_ths_manifest:
                cleanup = prune_checkpoint_history(
                    data_dir / "_daily_updates",
                    checkpoint_dir,
                )
                cleanup_path = checkpoint_dir / "checkpoint_cleanup.json"
                _atomic_json(cleanup, cleanup_path)
                print(
                    f"checkpoint_cleanup={cleanup_path} "
                    f"removed_files={cleanup['removed_files']} "
                    f"removed_bytes={cleanup['removed_bytes']}",
                    flush=True,
                )
    print(f"THS daily update: updated={successful} unchanged={unchanged} failed={failed}")
    print(f"report={report_path}")
    return 0 if validation["valid"] else 2


def run(
    data_dir: str | Path = DATA_DIR,
    max_stocks: int | None = None,
    source: THSDataSource | None = None,
    *,
    require_ths_manifest: bool = True,
) -> int:
    """Serialize one dataset's incremental writes across daily-update entry points."""
    data_dir = Path(data_dir)
    with process_lock(data_dir / UPDATE_LOCK_FILENAME, "THS daily update"):
        return _run_unlocked(
            data_dir=data_dir,
            max_stocks=max_stocks,
            source=source,
            require_ths_manifest=require_ths_manifest,
        )


def main() -> int:
    max_stocks = os.environ.get("THS_MAX_STOCKS")
    return run(max_stocks=int(max_stocks) if max_stocks else None)


if __name__ == "__main__":
    raise SystemExit(main())
