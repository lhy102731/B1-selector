"""直接把统一 THS/Yuanhang 日线数据库写入一个全新的同结构目录。"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.rebuild_all_ths import (
    DATASET_MANIFEST,
    STOCK_PREFIXES,
    _atomic_write,
    _normalise_history,
    validate_history,
)
from utils.ths_data_source import THSDataSource, THSDataSourceError


REPORT_NAME = ".ths_fetch_report.json"
QUALITY_REPORT_NAME = ".ths_quality_report.json"
DATA_QUALITY_VERSION = 4
ARCHIVE_BOUNDARY_TOLERANCE_DAYS = 14
KNOWN_SOURCE_HISTORY_GAPS = {
    "000517": {"archive_start": "1993-08-06", "ths_start": "1996-04-16"},
    "000028": {"archive_start": "1993-08-09", "ths_start": "1996-04-16"},
    "600684": {"archive_start": "1993-10-28", "ths_start": "1996-04-16"},
    "000529": {"archive_start": "1993-11-18", "ths_start": "1996-04-16"},
    "600866": {"archive_start": "1994-08-18", "ths_start": "1996-03-12"},
    "000543": {"archive_start": "1993-12-20", "ths_start": "1995-04-10"},
    "000596": {"archive_start": "1996-09-27", "ths_start": "1996-12-06"},
    "000609": {"archive_start": "1996-10-10", "ths_start": "1996-11-15"},
}


def _archive_codes(data_dir: Path) -> set[str]:
    return {
        path.stem
        for prefix in STOCK_PREFIXES
        for path in (data_dir / prefix).glob("*.csv")
        if len(path.stem) == 6 and path.stem.isdigit()
    }


def _archive_date_bounds(data_dir: Path) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    """Read only legacy date columns to guard against truncated THS history."""
    bounds: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    for prefix in STOCK_PREFIXES:
        for path in (data_dir / prefix).glob("*.csv"):
            if len(path.stem) != 6 or not path.stem.isdigit():
                continue
            try:
                archive = pd.read_csv(
                    path, encoding="gbk", usecols=lambda column: column in {"date", "volume"}
                )
                dates = pd.to_datetime(archive["date"], errors="coerce")
                if "volume" in archive.columns:
                    positive = pd.to_numeric(archive["volume"], errors="coerce") > 0
                    traded_dates = dates[positive & dates.notna()]
                    if not traded_dates.empty:
                        dates = traded_dates
                dates = dates.dropna()
            except Exception:
                continue
            if not dates.empty:
                bounds[path.stem] = (
                    pd.Timestamp(dates.min()).tz_localize(None),
                    pd.Timestamp(dates.max()).tz_localize(None),
                )
    # A handful of legacy CSVs are all-NUL damaged, while their cleaned raw
    # parquet cache still has trustworthy date/volume columns. Use that only as
    # a boundary fallback; no old prices or fundamentals enter the THS output.
    for prefix in STOCK_PREFIXES:
        for path in (data_dir / "raw_parquet" / prefix).glob("*.parquet"):
            if path.stem in bounds or len(path.stem) != 6 or not path.stem.isdigit():
                continue
            try:
                archive = pd.read_parquet(path, columns=["date", "volume"])
                dates = pd.to_datetime(archive["date"], errors="coerce")
                positive = pd.to_numeric(archive["volume"], errors="coerce") > 0
                traded_dates = dates[positive & dates.notna()]
                dates = traded_dates if not traded_dates.empty else dates.dropna()
            except Exception:
                continue
            if not dates.empty:
                bounds[path.stem] = (
                    pd.Timestamp(dates.min()).tz_localize(None),
                    pd.Timestamp(dates.max()).tz_localize(None),
                )
    return bounds


def _effective_date_bounds(
    code: str,
    archive_bounds: dict[str, tuple[pd.Timestamp, pd.Timestamp]],
) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    bounds = archive_bounds.get(code)
    if bounds is None:
        return None
    gap = KNOWN_SOURCE_HISTORY_GAPS.get(code)
    if gap is None:
        return bounds
    return pd.Timestamp(gap["ths_start"]), bounds[1]


def _annotate_known_history_gap(validation: dict[str, Any], code: str) -> None:
    gap = KNOWN_SOURCE_HISTORY_GAPS.get(code)
    if gap is not None:
        validation["known_source_history_gap"] = {
            **gap,
            "reason": "THSDK and Yuanhang candle/history endpoints expose no earlier K-line",
        }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _existing_file_is_valid(
    path: Path,
    code: str,
    min_cap_coverage: float,
    expected_bounds: tuple[pd.Timestamp, pd.Timestamp] | None = None,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        frame = pd.read_csv(path, encoding="gbk", low_memory=False)
        if "date" not in frame.columns:
            return None
        raw_dates = pd.to_datetime(frame["date"], errors="coerce")
        if (
            raw_dates.isna().any()
            or raw_dates.duplicated().any()
            or not raw_dates.is_monotonic_decreasing
        ):
            return None
        validation = validate_history(
            _normalise_history(frame),
            code,
            min_cap_coverage=min_cap_coverage,
            expected_start=expected_bounds[0] if expected_bounds else None,
            expected_end=expected_bounds[1] if expected_bounds else None,
            boundary_tolerance_days=ARCHIVE_BOUNDARY_TOLERANCE_DAYS,
        )
    except Exception:
        return None
    return validation if validation.get("valid") else None


def fetch_all(
    *,
    archive_data_dir: str | Path = "data",
    output_dir: str | Path = "data_ths",
    start: str = "1990-01-01",
    end: str | None = None,
    max_stocks: int | None = None,
    resume: bool = True,
    min_cap_coverage: float = 0.90,
    source: THSDataSource | None = None,
) -> int:
    archive_data_dir = Path(archive_data_dir).resolve()
    output_dir = Path(output_dir).resolve()
    end = end or date.today().isoformat()
    if output_dir == archive_data_dir:
        raise ValueError("output_dir must be different from the old data directory")
    if output_dir in archive_data_dir.parents or archive_data_dir in output_dir.parents:
        raise ValueError("output_dir and archive_data_dir must be separate sibling-style trees")
    if not archive_data_dir.is_dir():
        raise FileNotFoundError(f"old data directory not found: {archive_data_dir}")
    for prefix in STOCK_PREFIXES:
        (output_dir / prefix).mkdir(parents=True, exist_ok=True)

    report_path = output_dir / REPORT_NAME
    manifest_path = output_dir / DATASET_MANIFEST
    try:
        prior_report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        prior_report = {}
    prior_matches = (
        prior_report.get("source") == "thsdk+yuanhang"
        and prior_report.get("start") == start
        and prior_report.get("end") == end
        and Path(str(prior_report.get("output_dir", ""))).resolve() == output_dir
    )
    prior_quality_version = int(prior_report.get("data_quality_version", 0) or 0)
    prior_by_code = {
        str(item.get("code")): item
        for item in prior_report.get("stocks", [])
        if prior_matches
        and item.get("valid") is True
        and (
            prior_quality_version == DATA_QUALITY_VERSION
            and item.get("status") in {"written", "skipped_valid"}
        )
    }
    manifest_quality_current = False
    if resume:
        try:
            existing_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing_manifest = {}
        try:
            manifest_schema_version = int(existing_manifest.get("schema_version", 0))
            manifest_quality_version = int(
                existing_manifest.get("data_quality_version", 0)
            )
        except (TypeError, ValueError):
            manifest_schema_version = 0
            manifest_quality_version = 0
        manifest_quality_current = (
            existing_manifest.get("status") == "COMPLETED"
            and existing_manifest.get("source") == "thsdk+yuanhang"
            and manifest_schema_version >= 3
            and manifest_quality_version == DATA_QUALITY_VERSION
            and existing_manifest.get("start") == start
            and existing_manifest.get("end") == end
        )
        if not manifest_quality_current and not prior_by_code:
            resume = False
            print("THS resume disabled: existing dataset predates the current quality policy", flush=True)
        elif not manifest_quality_current:
            print(
                f"THS interrupted-run resume: trusting {len(prior_by_code)} previously validated files",
                flush=True,
            )
    own_source = source is None
    source = source or THSDataSource()
    started = time.perf_counter()
    try:
        current_universe = source.fetch_stock_universe()
        archive_codes = _archive_codes(archive_data_dir)
        archive_bounds = _archive_date_bounds(archive_data_dir)
        codes = sorted(archive_codes | set(current_universe))
        if max_stocks is not None:
            codes = codes[:max_stocks]
        if not codes:
            raise RuntimeError("THS and the old archive returned an empty stock universe")

        report: dict[str, Any] = {
            "status": "RUNNING",
            "source": "thsdk+yuanhang",
            "data_quality_version": DATA_QUALITY_VERSION,
            "archive_data_dir": str(archive_data_dir),
            "output_dir": str(output_dir),
            "start": start,
            "end": end,
            "current_ths_codes": len(current_universe),
            "archive_codes": len(archive_codes),
            "requested_files": len(codes),
            "known_source_history_gaps": KNOWN_SOURCE_HISTORY_GAPS,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "stocks": [],
        }
        _write_json(report_path, report)

        failed = 0
        written = 0
        skipped = 0
        no_history = 0
        no_history_codes: list[str] = []
        quality_totals = {
            "volume_unit_repaired_rows": 0,
            "ohlc_envelope_repaired_rows": 0,
            "amount_sentinel_rows": 0,
            "amount_invalid_rows": 0,
            "price_adjustment_reconstructed_files": 0,
            "share_action_realigned_events": 0,
        }
        quality_stocks: list[dict[str, Any]] = []
        for index, code in enumerate(codes, 1):
            target = output_dir / code[:2] / f"{code}.csv"
            validation: dict[str, Any]
            trusted_prior = prior_by_code.get(code)
            may_resume = resume and (manifest_quality_current or trusted_prior is not None)
            existing = (
                _existing_file_is_valid(
                    target,
                    code,
                    min_cap_coverage,
                    _effective_date_bounds(code, archive_bounds),
                )
                if may_resume
                else None
            )
            if existing is not None:
                validation = existing
                _annotate_known_history_gap(validation, code)
                quality = dict((trusted_prior or {}).get("quality", {}))
                if quality:
                    validation["quality"] = quality
                    for key in (
                        "volume_unit_repaired_rows", "ohlc_envelope_repaired_rows", "amount_sentinel_rows",
                        "amount_invalid_rows", "share_action_realigned_events",
                    ):
                        quality_totals[key] += int(quality.get(key, 0) or 0)
                    quality_totals["price_adjustment_reconstructed_files"] += int(
                        bool(quality.get("price_adjustment_reconstructed"))
                    )
                    if any(
                        quality.get(key)
                        for key in (
                            "volume_unit_repaired_rows", "ohlc_envelope_repaired_rows", "amount_sentinel_rows",
                            "amount_invalid_rows", "price_adjustment_reconstructed",
                            "share_action_realigned_events",
                        )
                    ):
                        quality_stocks.append({"code": code, **quality})
                validation["status"] = "skipped_valid"
                skipped += 1
            else:
                item_started = time.perf_counter()
                try:
                    fetched = source.fetch_history(code, start, end)
                    quality = dict(fetched.attrs.get("quality_audit", {}))
                    history = _normalise_history(fetched)
                    if history.empty and code not in archive_codes:
                        raise THSDataSourceError("not data: empty THS history")
                    expected_bounds = _effective_date_bounds(code, archive_bounds)
                    validation = validate_history(
                        history,
                        code,
                        min_cap_coverage=min_cap_coverage,
                        expected_start=expected_bounds[0] if expected_bounds else None,
                        expected_end=expected_bounds[1] if expected_bounds else None,
                        boundary_tolerance_days=ARCHIVE_BOUNDARY_TOLERANCE_DAYS,
                    )
                    _annotate_known_history_gap(validation, code)
                    validation["quality"] = quality
                    for key in (
                        "volume_unit_repaired_rows", "ohlc_envelope_repaired_rows", "amount_sentinel_rows",
                        "amount_invalid_rows", "share_action_realigned_events",
                    ):
                        quality_totals[key] += int(quality.get(key, 0) or 0)
                    quality_totals["price_adjustment_reconstructed_files"] += int(
                        bool(quality.get("price_adjustment_reconstructed"))
                    )
                    if any(
                        quality.get(key)
                        for key in (
                            "volume_unit_repaired_rows", "ohlc_envelope_repaired_rows", "amount_sentinel_rows",
                            "amount_invalid_rows", "price_adjustment_reconstructed",
                            "share_action_realigned_events",
                        )
                    ):
                        quality_stocks.append({"code": code, **quality})
                    validation["elapsed_seconds"] = round(time.perf_counter() - item_started, 4)
                    if validation["valid"]:
                        _atomic_write(history, target)
                        validation["status"] = "written"
                        written += 1
                    else:
                        validation["status"] = "failed_validation"
                        failed += 1
                except Exception as exc:
                    if "not data" in str(exc).lower() and code not in archive_codes:
                        no_history += 1
                        no_history_codes.append(code)
                        validation = {
                            "code": code,
                            "name": current_universe.get(code, {}).get("name", ""),
                            "status": "no_history_yet",
                            "reason": "THS current universe contains the code but no K-line exists through end date",
                            "elapsed_seconds": round(time.perf_counter() - item_started, 4),
                        }
                    else:
                        failed += 1
                        validation = {
                            "code": code,
                            "status": "failed",
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                            "elapsed_seconds": round(time.perf_counter() - item_started, 4),
                        }
            report["stocks"].append(validation)

            elapsed = time.perf_counter() - started
            rate = index / max(elapsed, 1e-9)
            eta_seconds = (len(codes) - index) / max(rate, 1e-9)
            if index == 1 or index % 25 == 0 or validation["status"].startswith("failed") or validation["status"] == "no_history_yet":
                print(
                    f"{index}/{len(codes)} {code} {validation['status']} "
                    f"elapsed={elapsed / 60:.1f}m eta={eta_seconds / 60:.1f}m",
                    flush=True,
                )
            if index % 25 == 0 or validation["status"].startswith("failed") or validation["status"] == "no_history_yet":
                report.update(
                    {
                        "written": written,
                        "skipped": skipped,
                        "failed": failed,
                        "no_history": no_history,
                        "no_history_codes": no_history_codes,
                        "processed": index,
                        "elapsed_seconds": round(elapsed, 3),
                        "quality_totals": quality_totals,
                    }
                )
                _write_json(report_path, report)

        elapsed = time.perf_counter() - started
        report.update(
            {
                "status": "COMPLETED" if failed == 0 else "COMPLETED_WITH_FAILURES",
                "written": written,
                "skipped": skipped,
                "failed": failed,
                "no_history": no_history,
                "no_history_codes": no_history_codes,
                "processed": len(codes),
                "elapsed_seconds": round(elapsed, 3),
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "quality_totals": quality_totals,
            }
        )
        _write_json(report_path, report)
        quality_report = {
            "status": report["status"],
            "data_quality_version": DATA_QUALITY_VERSION,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "totals": quality_totals,
            "known_source_history_gaps": KNOWN_SOURCE_HISTORY_GAPS,
            "stocks": quality_stocks,
        }
        _write_json(output_dir / QUALITY_REPORT_NAME, quality_report)
        if failed == 0:
            manifest = {
                "status": "COMPLETED",
                "source": "thsdk+yuanhang",
                "schema_version": 3,
                "data_quality_version": DATA_QUALITY_VERSION,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "start": start,
                "end": end,
                "stock_count": written + skipped,
                "requested_codes": len(codes),
                "no_history_codes": no_history_codes,
                "known_source_history_gaps": KNOWN_SOURCE_HISTORY_GAPS,
                "price_adjustment": "backward",
                "raw_close_column": "close_raw",
                "turnover_semantics": "volume * 100 / effective outstanding shares; direct 212 fallback",
                "market_cap_semantics": "close_raw * effective outstanding shares",
                "quality_report": QUALITY_REPORT_NAME,
                "archive_data_dir": str(archive_data_dir),
            }
            _write_json(output_dir / DATASET_MANIFEST, manifest)
        print(
            f"THS fetch finished: status={report['status']} files={len(codes)} "
            f"written={written} skipped={skipped} no_history={no_history} "
            f"failed={failed} elapsed={elapsed / 60:.1f}m",
            flush=True,
        )
        print(f"output={output_dir}", flush=True)
        print(f"report={report_path}", flush=True)
        return 0 if failed == 0 else 2
    finally:
        if own_source:
            source.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="直接拉取统一 THS 数据到新的同结构目录")
    parser.add_argument("--archive-data-dir", default="data")
    parser.add_argument("--output-dir", default="data_ths")
    parser.add_argument("--start", default="1990-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--max-stocks", type=int, default=None)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--min-cap-coverage", type=float, default=0.90)
    args = parser.parse_args()
    return fetch_all(
        archive_data_dir=args.archive_data_dir,
        output_dir=args.output_dir,
        start=args.start,
        end=args.end,
        max_stocks=args.max_stocks,
        resume=not args.no_resume,
        min_cap_coverage=args.min_cap_coverage,
    )


if __name__ == "__main__":
    raise SystemExit(main())
