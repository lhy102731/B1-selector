"""Backfill valuation columns without changing the THS price/volume bars.

The repository's historical column contract is:

* ``pe_dynamic`` = PE(TTM), despite the legacy column name;
* ``pb`` = PB(MRQ);
* ``ps`` = PS(TTM);
* ``pcf`` = PCF based on net cash flow (BaoStock ``pcfNcfTTM``).

THS historical valuation queries are not used here because they backfill
later-published financial reports into earlier report-period dates.  The
``legacy`` command migrates only the independently-audited BaoStock history
through 2026-05-29.  The ``baostock`` command re-queries the later contaminated
incremental period using the same four field definitions.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
import time
from contextlib import redirect_stdout
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from utils.baostock_lock import serialized_baostock


ROOT = Path(__file__).resolve().parents[1]
STOCK_PREFIXES = ("00", "30", "60", "68")
VALUATION_COLUMNS = ("pe_dynamic", "pb", "ps", "pcf")
PKL_COLUMN_MAP = {"peTTM": "pe_dynamic", "pbMRQ": "pb"}
BAOSTOCK_COLUMN_MAP = {
    "peTTM": "pe_dynamic",
    "pbMRQ": "pb",
    "psTTM": "ps",
    "pcfNcfTTM": "pcf",
}
COMMON_SENTINELS = frozenset(
    {2147483647.0, 2147483648.0, 4294967295.0, 999999999.0}
)
PS_SENTINEL = 99_999_999.999999
DEFAULT_SAFE_CUTOFF = "2026-05-29"
COMPLETED_PROGRESS_STATUSES = frozenset(
    {"updated", "unchanged", "no_target_rows"}
)


def stock_files(data_dir: Path) -> list[Path]:
    files: list[Path] = []
    for prefix in STOCK_PREFIXES:
        files.extend(sorted((data_dir / prefix).glob("*.csv")))
    return files


def code_path(root: Path, code: str) -> Path:
    return root / code[:2] / f"{code}.csv"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        # ``round_trip`` is required because every successful fill rewrites the
        # full CSV.  The default parser can change unrelated float values by one
        # ULP (for example market_cap) when they are serialized again.
        return pd.read_csv(
            path,
            encoding="gbk",
            dtype={"date": str},
            float_precision="round_trip",
        )
    except (pd.errors.EmptyDataError, UnicodeDecodeError):
        return pd.DataFrame()


def _date_keys(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, errors="coerce").dt.strftime("%Y-%m-%d")


def valid_valuation(values: pd.Series, field: str) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    finite = numeric.map(lambda value: pd.notna(value) and math.isfinite(float(value)))
    valid = finite & numeric.ne(0) & ~numeric.isin(COMMON_SENTINELS)
    if field == "ps":
        valid &= (numeric - PS_SENTINEL).abs() > 1e-3
    return valid


def _source_frame(frame: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    if frame is None or frame.empty or "date" not in frame.columns:
        return pd.DataFrame(columns=("date", *mapping.values()))
    available = {source: target for source, target in mapping.items() if source in frame.columns}
    result = pd.DataFrame({"date": _date_keys(frame["date"])})
    for source, target in available.items():
        result[target] = pd.to_numeric(frame[source], errors="coerce")
    result = result.dropna(subset=["date"]).drop_duplicates("date", keep="last")
    return result


def _legacy_frame(frame: pd.DataFrame) -> pd.DataFrame:
    mapping = {column: column for column in VALUATION_COLUMNS if column in frame.columns}
    return _source_frame(frame, mapping)


def _apply_source(
    target: pd.DataFrame,
    source: pd.DataFrame,
    *,
    cutoff: str | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    result = target.copy()
    for column in VALUATION_COLUMNS:
        if column not in result.columns:
            result[column] = pd.NA
    stats = {f"filled_{column}": 0 for column in VALUATION_COLUMNS}
    if result.empty or source.empty:
        return result, stats

    target_dates = _date_keys(result["date"])
    source_indexed = source.set_index("date")
    allowed_date = pd.Series(True, index=result.index)
    if cutoff is not None:
        allowed_date = target_dates.le(str(cutoff))

    for column in VALUATION_COLUMNS:
        if column not in source_indexed.columns:
            continue
        mapped = target_dates.map(source_indexed[column])
        source_valid = valid_valuation(mapped, column)
        target_valid = valid_valuation(result[column], column)
        fill = allowed_date & source_valid & ~target_valid
        if fill.any():
            result.loc[fill, column] = mapped.loc[fill].astype(float)
            stats[f"filled_{column}"] = int(fill.sum())
    return result, stats


def merge_legacy_sources(
    target: pd.DataFrame,
    legacy: pd.DataFrame,
    pepb_cache: pd.DataFrame | None,
    *,
    cutoff: str = DEFAULT_SAFE_CUTOFF,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Merge audited sources in priority order without forward-filling."""
    result = target.copy()
    totals = {f"filled_{column}": 0 for column in VALUATION_COLUMNS}

    if pepb_cache is not None and not pepb_cache.empty:
        result, stats = _apply_source(
            result,
            _source_frame(pepb_cache, PKL_COLUMN_MAP),
            cutoff=cutoff,
        )
        for key, value in stats.items():
            totals[key] += value

    result, stats = _apply_source(result, _legacy_frame(legacy), cutoff=cutoff)
    for key, value in stats.items():
        totals[key] += value
    return result, totals


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="gbk",
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


def _atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    os.replace(temporary, path)


def _load_progress(
    progress_path: Path,
) -> tuple[set[str], dict[str, int]]:
    if not progress_path.exists():
        return set(), {f"filled_{column}": 0 for column in VALUATION_COLUMNS}
    latest_by_code: dict[str, dict[str, Any]] = {}
    with progress_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            code = str(row.get("code", ""))
            if code:
                latest_by_code[code] = row
    completed = {
        code
        for code, row in latest_by_code.items()
        if row.get("status") in COMPLETED_PROGRESS_STATUSES
    }
    aggregate = {f"filled_{column}": 0 for column in VALUATION_COLUMNS}
    for code in completed:
        row = latest_by_code[code]
        for key in aggregate:
            aggregate[key] += int(row.get(key, 0) or 0)
    return completed, aggregate


def _append_progress(progress_path: Path, row: dict[str, Any]) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    with progress_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _update_manifest(data_dir: Path, key: str, value: dict[str, Any]) -> None:
    path = data_dir / ".ths_dataset_manifest.json"
    if not path.exists():
        return
    manifest = json.loads(path.read_text(encoding="utf-8"))
    valuation = dict(manifest.get("valuation_provenance") or {})
    valuation[key] = value
    manifest["valuation_provenance"] = valuation
    manifest["updated_at"] = datetime.now().isoformat(timespec="seconds")
    _atomic_json(manifest, path)


def run_legacy(
    data_dir: Path,
    legacy_dir: Path,
    pepb_pickle: Path,
    *,
    cutoff: str,
    apply: bool,
    progress_path: Path,
    report_path: Path,
) -> int:
    files = stock_files(data_dir)
    cache: dict[str, pd.DataFrame] = {}
    if pepb_pickle.exists():
        loaded = pd.read_pickle(pepb_pickle)
        if isinstance(loaded, dict):
            cache = loaded
    if apply:
        completed, aggregate = _load_progress(progress_path)
    else:
        completed = set()
        aggregate = {f"filled_{column}": 0 for column in VALUATION_COLUMNS}
    failed = 0
    started = time.time()

    for index, path in enumerate(files, 1):
        code = path.stem
        if code in completed:
            continue
        row: dict[str, Any] = {"code": code}
        try:
            target = _read_csv(path)
            legacy = _read_csv(code_path(legacy_dir, code))
            merged, stats = merge_legacy_sources(
                target, legacy, cache.get(code), cutoff=cutoff
            )
            changed = any(stats.values())
            if apply and changed:
                _atomic_csv(merged, path)
            row.update(stats)
            row["status"] = "updated" if changed else "unchanged"
            for key, value in stats.items():
                aggregate[key] += value
        except Exception as exc:
            failed += 1
            row.update(
                status="failed",
                error_type=type(exc).__name__,
                error=str(exc)[:500],
            )
        if apply:
            _append_progress(progress_path, row)
        if index == 1 or index % 100 == 0 or row["status"] == "failed":
            print(
                f"legacy {index}/{len(files)} {code} {row['status']} "
                f"elapsed={time.time() - started:.1f}s",
                flush=True,
            )

    report = {
        "status": "COMPLETED" if failed == 0 else "FAILED",
        "mode": "legacy",
        "applied": apply,
        "data_dir": str(data_dir),
        "legacy_dir": str(legacy_dir),
        "pepb_pickle": str(pepb_pickle),
        "cutoff": cutoff,
        "files": len(files),
        "failed": failed,
        "filled": aggregate,
        "elapsed_seconds": round(time.time() - started, 3),
        "semantics": {
            "pe_dynamic": "peTTM",
            "pb": "pbMRQ",
            "ps": "psTTM",
            "pcf": "pcfNcfTTM",
        },
    }
    _atomic_json(report, report_path)
    if apply and failed == 0:
        _update_manifest(
            data_dir,
            "historical_legacy_baostock",
            {
                "source": str(legacy_dir),
                "pe_pb_priority_source": str(pepb_pickle),
                "through": cutoff,
                "asof_mode": "point_in_time_original_daily_values",
                "zero_values": "treated_as_missing",
                "ps_sentinel": PS_SENTINEL,
                "filled": aggregate,
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"filled={aggregate} failed={failed}", flush=True)
    return 0 if failed == 0 else 2


def _bs_code(code: str) -> str:
    return f"sh.{code}" if code.startswith(("6", "9")) else f"sz.{code}"


def _fetch_baostock(
    bs: Any,
    code: str,
    start: str,
    end: str,
    *,
    attempts: int = 3,
) -> pd.DataFrame:
    fields = "date,code,peTTM,pbMRQ,psTTM,pcfNcfTTM"
    last_error = "unknown"
    for attempt in range(attempts):
        rs = bs.query_history_k_data_plus(
            _bs_code(code),
            fields,
            start_date=start,
            end_date=end,
            frequency="d",
            adjustflag="1",
        )
        if rs.error_code == "0":
            rows: list[list[str]] = []
            while (rs.error_code == "0") & rs.next():
                rows.append(rs.get_row_data())
            frame = pd.DataFrame(rows, columns=fields.split(","))
            return _source_frame(frame, BAOSTOCK_COLUMN_MAP)
        last_error = f"{rs.error_code} {rs.error_msg}"
        time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"BaoStock query failed for {code}: {last_error}")


def _legacy_missing_codes(data_dir: Path, legacy_dir: Path) -> set[str]:
    missing: set[str] = set()
    for path in stock_files(data_dir):
        legacy = _read_csv(code_path(legacy_dir, path.stem))
        if legacy.empty or "date" not in legacy.columns:
            missing.add(path.stem)
    return missing


@serialized_baostock
def run_baostock(
    data_dir: Path,
    legacy_dir: Path,
    *,
    start: str,
    end: str,
    apply: bool,
    progress_path: Path,
    report_path: Path,
    codes: set[str] | None = None,
) -> int:
    import baostock as bs

    files = stock_files(data_dir)
    if codes:
        files = [path for path in files if path.stem in codes]
    if apply:
        completed, aggregate = _load_progress(progress_path)
    else:
        completed = set()
        aggregate = {f"filled_{column}": 0 for column in VALUATION_COLUMNS}
    full_history_codes = _legacy_missing_codes(data_dir, legacy_dir)
    if codes:
        full_history_codes &= {path.stem for path in files}
    failed = 0
    started = time.time()
    with redirect_stdout(open(os.devnull, "w")):
        login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"BaoStock login failed: {login.error_code} {login.error_msg}")
    try:
        for index, path in enumerate(files, 1):
            code = path.stem
            if code in completed:
                continue
            row: dict[str, Any] = {"code": code}
            try:
                target = _read_csv(path)
                query_start = start
                if code in full_history_codes and not target.empty:
                    local_start = _date_keys(target["date"]).dropna().min()
                    if local_start:
                        query_start = min(query_start, local_start)
                remote = _fetch_baostock(bs, code, query_start, end)
                if remote.empty:
                    stats = {f"filled_{column}": 0 for column in VALUATION_COLUMNS}
                    row.update(stats)
                    target_dates = _date_keys(target["date"])
                    has_target_rows = target_dates.between(query_start, end).any()
                    if has_target_rows:
                        raise RuntimeError(
                            "BaoStock returned no rows although local trading rows "
                            f"exist in {query_start}..{end}"
                        )
                    row["status"] = "no_target_rows"
                else:
                    merged, stats = _apply_source(target, remote)
                    changed = any(stats.values())
                    if apply and changed:
                        _atomic_csv(merged, path)
                    row.update(stats)
                    row["status"] = "updated" if changed else "unchanged"
                    for key, value in stats.items():
                        aggregate[key] += value
                row["query_start"] = query_start
                row["full_history_fallback"] = code in full_history_codes
            except Exception as exc:
                failed += 1
                row.update(
                    status="failed",
                    error_type=type(exc).__name__,
                    error=str(exc)[:500],
                )
            if apply:
                _append_progress(progress_path, row)
            if index == 1 or index % 100 == 0 or row["status"] == "failed":
                print(
                    f"baostock {index}/{len(files)} {code} {row['status']} "
                    f"failed={failed} elapsed={time.time() - started:.1f}s",
                    flush=True,
                )
    finally:
        with redirect_stdout(open(os.devnull, "w")):
            bs.logout()

    report = {
        "status": "COMPLETED" if failed == 0 else "FAILED",
        "mode": "baostock",
        "applied": apply,
        "data_dir": str(data_dir),
        "start": start,
        "end": end,
        "full_history_codes": sorted(full_history_codes),
        "files": len(files),
        "requested_codes": sorted(codes) if codes else None,
        "failed": failed,
        "filled": aggregate,
        "elapsed_seconds": round(time.time() - started, 3),
        "semantics": {
            "pe_dynamic": "peTTM",
            "pb": "pbMRQ",
            "ps": "psTTM",
            "pcf": "pcfNcfTTM",
        },
    }
    _atomic_json(report, report_path)
    if apply and failed == 0:
        manifest_key = (
            "recent_baostock_requery"
            if start == "2026-05-30" and not codes
            else "targeted_baostock_missing_values"
        )
        _update_manifest(
            data_dir,
            manifest_key,
            {
                "source": "baostock query_history_k_data_plus",
                "start": start,
                "end": end,
                "asof_mode": "point_in_time_daily_values",
                "full_history_codes": sorted(full_history_codes),
                "requested_codes": sorted(codes) if codes else None,
                "filled": aggregate,
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"filled={aggregate} failed={failed}", flush=True)
    return 0 if failed == 0 else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    legacy = subparsers.add_parser("legacy")
    legacy.add_argument("--data-dir", default="data")
    legacy.add_argument(
        "--legacy-dir", default="data_pre_ths_backup_20260727_110350"
    )
    legacy.add_argument("--pepb-pickle", default="data/baostock_pepb_daily.pkl")
    legacy.add_argument("--cutoff", default=DEFAULT_SAFE_CUTOFF)
    legacy.add_argument("--apply", action="store_true")
    legacy.add_argument(
        "--progress-path",
        default="",
        help="Optional fresh progress JSONL path for a new repair pass.",
    )
    legacy.add_argument(
        "--report-path",
        default="",
        help="Optional report path paired with --progress-path.",
    )

    recent = subparsers.add_parser("baostock")
    recent.add_argument("--data-dir", default="data")
    recent.add_argument(
        "--legacy-dir", default="data_pre_ths_backup_20260727_110350"
    )
    recent.add_argument("--start", default="2026-05-30")
    recent.add_argument("--end", default=date.today().isoformat())
    recent.add_argument("--apply", action="store_true")
    recent.add_argument(
        "--codes",
        default="",
        help="Comma-separated stock codes to query; empty means the full dataset.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifact = ROOT / "artifacts" / "maintenance" / "valuation_backfill"
    if args.mode == "legacy":
        suffix = str(args.cutoff).replace("-", "")
        progress_path = (
            Path(args.progress_path).resolve()
            if args.progress_path
            else artifact / f"legacy_{suffix}_progress.jsonl"
        )
        report_path = (
            Path(args.report_path).resolve()
            if args.report_path
            else artifact / f"legacy_{suffix}_report.json"
        )
        return run_legacy(
            Path(args.data_dir).resolve(),
            Path(args.legacy_dir).resolve(),
            Path(args.pepb_pickle).resolve(),
            cutoff=args.cutoff,
            apply=args.apply,
            progress_path=progress_path,
            report_path=report_path,
        )
    requested_codes = {
        code.strip().zfill(6) for code in str(args.codes).split(",") if code.strip()
    }
    suffix = f"{args.start}_{args.end}".replace("-", "")
    if requested_codes:
        digest = hashlib.sha1(
            ",".join(sorted(requested_codes)).encode("ascii")
        ).hexdigest()[:8]
        suffix += f"_codes_{digest}"
    return run_baostock(
        Path(args.data_dir).resolve(),
        Path(args.legacy_dir).resolve(),
        start=args.start,
        end=args.end,
        apply=args.apply,
        progress_path=artifact / f"baostock_{suffix}_progress.jsonl",
        report_path=artifact / f"baostock_{suffix}_report.json",
        codes=requested_codes or None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
