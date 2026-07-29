"""Read-only field and date coverage audit for the THS and legacy stock CSVs."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


PREFIXES = ("00", "30", "60", "68")
VALUATIONS = {"pe_dynamic", "pb", "ps", "pcf", "pe_static"}
POSITIVE = {"open", "high", "low", "close", "close_raw", "market_cap", "total_cap", "circ_cap"}
NONNEGATIVE = {"volume", "amount", "turnover", "volume_ratio", "amplitude"}
COMMON_SENTINELS = frozenset(
    {2_147_483_647.0, 2_147_483_648.0, 4_294_967_295.0, 999_999_999.0}
)
APPROVED_LEGACY_ONLY_NON_TRADING = frozenset(
    {
        ("000016", "1992-10-04"),
        ("000020", "1992-10-04"),
        ("000529", "2001-01-01"),
        ("000681", "2001-01-01"),
        ("002500", "2020-06-17"),
        ("600602", "1990-12-21"),
    }
)


def valid_mask(series: pd.Series, field: str) -> tuple[pd.Series, pd.Series]:
    values = pd.to_numeric(series, errors="coerce")
    finite = pd.Series(np.isfinite(values.to_numpy(dtype=float, na_value=np.nan)), index=series.index)
    valid = finite.copy()
    if field in VALUATIONS:
        valid &= values.ne(0) & values.abs().lt(1_000_000)
    elif field in POSITIVE:
        valid &= values.gt(0)
    elif field in NONNEGATIVE:
        valid &= values.ge(0)
    if field in {"volume", "amount"}:
        valid &= ~values.isin(COMMON_SENTINELS)
    return finite, valid


def frame_stats(frame: pd.DataFrame) -> dict:
    result = {"rows": int(len(frame)), "fields": {}}
    volume = pd.to_numeric(frame.get("volume", pd.Series(index=frame.index, dtype=float)), errors="coerce")
    positive_volume = volume.gt(0) & ~volume.isin(COMMON_SENTINELS)
    suspended = volume.fillna(-1).eq(0)
    result["zero_volume_rows"] = int(suspended.sum())
    for field in frame.columns:
        if field == "date":
            nonempty = frame[field].notna() & frame[field].astype(str).str.strip().ne("")
            result["fields"][field] = {"nonempty": int(nonempty.sum())}
            continue
        finite, valid = valid_mask(frame[field], field)
        values = pd.to_numeric(frame[field], errors="coerce")
        result["fields"][field] = {
            "finite": int(finite.sum()),
            "valid": int(valid.sum()),
            "missing_or_invalid": int((~valid).sum()),
            "missing_or_invalid_positive_volume": int((~valid & positive_volume).sum()),
            "missing_or_invalid_zero_volume": int((~valid & suspended).sum()),
            "zero": int((finite & values.eq(0)).sum()),
            "negative": int((finite & values.lt(0)).sum()),
        }
    return result


def read_frame(path: Path | None) -> tuple[pd.DataFrame | None, str | None]:
    if path is None or not path.exists():
        return None, None
    try:
        frame = pd.read_csv(path, encoding="gbk", low_memory=False)
        if "date" not in frame.columns:
            return None, "missing date header"
        frame["date"] = frame["date"].astype(str).str[:10]
        return frame, None
    except Exception as exc:  # read-only audit must retain every damaged-file error
        return None, f"{type(exc).__name__}: {exc}"


def audit_pair(args: tuple[str, str | None, str | None]) -> dict:
    code, current_name, legacy_name = args
    current_path = Path(current_name) if current_name else None
    legacy_path = Path(legacy_name) if legacy_name else None
    current, current_error = read_frame(current_path)
    legacy, legacy_error = read_frame(legacy_path)
    out = {
        "code": code,
        "current_error": current_error,
        "legacy_error": legacy_error,
        "current": frame_stats(current) if current is not None else None,
        "legacy": frame_stats(legacy) if legacy is not None else None,
        "matched_rows": 0,
        "current_only_dates": Counter(),
        "legacy_only_dates": Counter(),
        "date_gap_classes": Counter(),
        "positive_date_gaps": {"current_only": Counter(), "legacy_only": Counter()},
        "current_invalid_positive_volume_by_year": {},
        "candidate_by_year": {},
        "amount_candidate_quality": Counter(),
        "field_match": {},
        "samples": {},
    }
    if current is None or legacy is None:
        return out
    c = current.drop_duplicates("date", keep="first").set_index("date")
    l = legacy.drop_duplicates("date", keep="first").set_index("date")
    common_dates = c.index.intersection(l.index)
    current_only = c.index.difference(l.index)
    legacy_only = l.index.difference(c.index)
    out["matched_rows"] = int(len(common_dates))
    out["current_only_dates"] = Counter(str(x)[:4] for x in current_only)
    out["legacy_only_dates"] = Counter(str(x)[:4] for x in legacy_only)
    current_gap_volume = pd.to_numeric(c.loc[current_only, "volume"], errors="coerce")
    legacy_gap_volume = pd.to_numeric(l.loc[legacy_only, "volume"], errors="coerce")
    current_gap_positive = current_gap_volume.gt(0) & ~current_gap_volume.isin(COMMON_SENTINELS)
    legacy_gap_positive = legacy_gap_volume.gt(0) & ~legacy_gap_volume.isin(COMMON_SENTINELS)
    approved_legacy = pd.Series(
        [(code, str(day)) in APPROVED_LEGACY_ONLY_NON_TRADING for day in legacy_only],
        index=legacy_only,
        dtype=bool,
    )
    out["date_gap_classes"] = Counter({
        "current_only_zero_volume": int(current_gap_volume.eq(0).sum()),
        "current_only_positive_volume": int(current_gap_positive.sum()),
        "legacy_only_zero_volume": int(legacy_gap_volume.eq(0).sum()),
        "legacy_only_positive_volume": int((legacy_gap_positive & ~approved_legacy).sum()),
        "approved_legacy_only_non_trading": int(approved_legacy.sum()),
    })
    out["positive_date_gaps"] = {
        "current_only": Counter(str(x) for x in current_only[current_gap_positive.to_numpy()]),
        "legacy_only": Counter(
            str(x) for x in legacy_only[(legacy_gap_positive & ~approved_legacy).to_numpy()]
        ),
    }
    current_volume = pd.to_numeric(current["volume"], errors="coerce")
    current_positive_volume = current_volume.gt(0) & ~current_volume.isin(COMMON_SENTINELS)
    for field in current.columns:
        if field == "date":
            continue
        _, current_valid = valid_mask(current[field], field)
        mask = ~current_valid & current_positive_volume
        if mask.any():
            out["current_invalid_positive_volume_by_year"][field] = Counter(
                current.loc[mask, "date"].astype(str).str[:4]
            )
    if len(current_only):
        out["samples"]["current_only_dates"] = [f"{code}:{x}" for x in current_only[:2]]
    if len(legacy_only):
        out["samples"]["legacy_only_dates"] = [f"{code}:{x}" for x in legacy_only[:2]]
    approved_dates = [str(day) for day in legacy_only if (code, str(day)) in APPROVED_LEGACY_ONLY_NON_TRADING]
    if approved_dates:
        out["samples"]["approved_legacy_only_non_trading"] = [
            f"{code}:{day}" for day in approved_dates[:2]
        ]
    for field in sorted(set(c.columns).union(l.columns)):
        if field not in c.columns:
            _, lv = valid_mask(l.loc[common_dates, field], field)
            out["field_match"][field] = {"legacy_valid_no_current_column": int(lv.sum())}
            continue
        if field not in l.columns:
            _, cv = valid_mask(c.loc[common_dates, field], field)
            out["field_match"][field] = {"current_valid_no_legacy_column": int(cv.sum())}
            continue
        cv_num = pd.to_numeric(c.loc[common_dates, field], errors="coerce")
        lv_num = pd.to_numeric(l.loc[common_dates, field], errors="coerce")
        _, cv = valid_mask(c.loc[common_dates, field], field)
        _, lv = valid_mask(l.loc[common_dates, field], field)
        both = cv & lv
        equal = both & np.isclose(cv_num, lv_num, rtol=1e-7, atol=1e-9, equal_nan=False)
        stats = {
            "current_missing_legacy_valid": int((~cv & lv).sum()),
            "current_valid_legacy_missing": int((cv & ~lv).sum()),
            "both_valid": int(both.sum()),
            "equal_tight": int(equal.sum()),
        }
        out["field_match"][field] = stats
        missing = common_dates[(~cv & lv).to_numpy()]
        if len(missing):
            out["candidate_by_year"][field] = Counter(str(x)[:4] for x in missing)
            out["samples"][f"{field}:current_missing_legacy_valid"] = [f"{code}:{x}" for x in missing[:2]]
            if field == "amount":
                dates = missing
                old_amount = lv_num.loc[dates]
                current_volume = pd.to_numeric(c.loc[dates, "volume"], errors="coerce")
                legacy_volume = pd.to_numeric(l.loc[dates, "volume"], errors="coerce")
                raw_close = pd.to_numeric(c.loc[dates, "close_raw"], errors="coerce")
                factor = pd.to_numeric(c.loc[dates, "close"], errors="coerce") / raw_close
                raw_low = pd.to_numeric(c.loc[dates, "low"], errors="coerce") / factor
                raw_high = pd.to_numeric(c.loc[dates, "high"], errors="coerce") / factor
                vwap = old_amount / current_volume
                out["amount_candidate_quality"].update({
                    "rows": len(dates),
                    "volume_equal_tight": int(np.isclose(current_volume, legacy_volume, rtol=1e-7, atol=1e-9).sum()),
                    "vwap_inside_raw_ohlc": int((vwap.ge(raw_low * 0.999) & vwap.le(raw_high * 1.001)).sum()),
                    "vwap_within_20pct_raw_ohlc": int((vwap.ge(raw_low * 0.8) & vwap.le(raw_high * 1.2)).sum()),
                    "candidate_finite_positive": int((np.isfinite(vwap) & vwap.gt(0)).sum()),
                    "volume_relative_diff_le_0.01pct": int(((current_volume - legacy_volume).abs() / current_volume).le(0.0001).sum()),
                    "volume_relative_diff_le_0.1pct": int(((current_volume - legacy_volume).abs() / current_volume).le(0.001).sum()),
                    "volume_relative_diff_le_1pct": int(((current_volume - legacy_volume).abs() / current_volume).le(0.01).sum()),
                    "volume_relative_diff_le_5pct": int(((current_volume - legacy_volume).abs() / current_volume).le(0.05).sum()),
                })
    return out


def merge_field_stats(target: dict, source: dict) -> None:
    target["rows"] += source["rows"]
    target["zero_volume_rows"] += source["zero_volume_rows"]
    for field, stats in source["fields"].items():
        dest = target["fields"][field]
        dest["files_with_column"] += 1
        for key, value in stats.items():
            dest[key] += value


def inventory(root: Path) -> dict[str, Path]:
    return {path.stem: path for prefix in PREFIXES for path in (root / prefix).glob("*.csv")}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", default="data")
    parser.add_argument("--legacy", default="data_pre_ths_backup_20260727_110350")
    parser.add_argument("--output", default="artifacts/maintenance/all_data_gap_audit.json")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    current_root, legacy_root = Path(args.current), Path(args.legacy)
    current_files, legacy_files = inventory(current_root), inventory(legacy_root)
    codes = sorted(set(current_files).union(legacy_files))
    tasks = [(code, str(current_files[code]) if code in current_files else None,
              str(legacy_files[code]) if code in legacy_files else None) for code in codes]
    dataset = lambda: {"rows": 0, "zero_volume_rows": 0, "fields": defaultdict(lambda: defaultdict(int))}
    report = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "roots": {"current": str(current_root.resolve()), "legacy": str(legacy_root.resolve())},
        "files": {
            "current": len(current_files), "legacy": len(legacy_files),
            "current_only_codes": sorted(set(current_files) - set(legacy_files)),
            "legacy_only_codes": sorted(set(legacy_files) - set(current_files)),
        },
        "datasets": {"current": dataset(), "legacy": dataset()},
        "matched_rows": 0,
        "date_gaps_by_year": {"current_only": Counter(), "legacy_only": Counter()},
        "date_gap_classes": Counter(),
        "positive_date_gaps": {"current_only": Counter(), "legacy_only": Counter()},
        "current_invalid_positive_volume_by_year": defaultdict(Counter),
        "current_invalid_positive_volume_by_code": defaultdict(Counter),
        "candidate_by_year": defaultdict(Counter),
        "amount_candidate_quality": Counter(),
        "field_match": defaultdict(lambda: defaultdict(int)),
        "read_errors": [],
        "samples": defaultdict(list),
    }
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        for index, item in enumerate(pool.map(audit_pair, tasks, chunksize=4), 1):
            if item["current_error"] or item["legacy_error"]:
                report["read_errors"].append({k: item[k] for k in ("code", "current_error", "legacy_error")})
            for name in ("current", "legacy"):
                if item[name] is not None:
                    merge_field_stats(report["datasets"][name], item[name])
            if item["current"] is not None:
                for field, stats in item["current"]["fields"].items():
                    count = stats.get("missing_or_invalid_positive_volume", 0)
                    if count:
                        report["current_invalid_positive_volume_by_code"][field][item["code"]] = count
            report["matched_rows"] += item["matched_rows"]
            report["date_gaps_by_year"]["current_only"].update(item["current_only_dates"])
            report["date_gaps_by_year"]["legacy_only"].update(item["legacy_only_dates"])
            report["date_gap_classes"].update(item["date_gap_classes"])
            for side in ("current_only", "legacy_only"):
                report["positive_date_gaps"][side].update(item["positive_date_gaps"][side])
            for field, counts in item["current_invalid_positive_volume_by_year"].items():
                report["current_invalid_positive_volume_by_year"][field].update(counts)
            for field, counts in item["candidate_by_year"].items():
                report["candidate_by_year"][field].update(counts)
            report["amount_candidate_quality"].update(item["amount_candidate_quality"])
            for field, stats in item["field_match"].items():
                for key, value in stats.items():
                    report["field_match"][field][key] += value
            for key, values in item["samples"].items():
                if len(report["samples"][key]) < 12:
                    report["samples"][key].extend(values[: 12 - len(report["samples"][key])])
            if index % 500 == 0:
                print(f"audited {index}/{len(tasks)}", flush=True)
    for name in ("current", "legacy"):
        report["datasets"][name]["fields"] = {k: dict(v) for k, v in report["datasets"][name]["fields"].items()}
    report["date_gaps_by_year"] = {k: dict(sorted(v.items())) for k, v in report["date_gaps_by_year"].items()}
    report["date_gap_classes"] = dict(report["date_gap_classes"])
    report["positive_date_gap_top_dates"] = {
        side: report["positive_date_gaps"][side].most_common(50)
        for side in ("current_only", "legacy_only")
    }
    report["positive_date_gap_by_year"] = {
        side: dict(sorted(Counter({
            year: sum(count for date, count in report["positive_date_gaps"][side].items() if date[:4] == year)
            for year in {date[:4] for date in report["positive_date_gaps"][side]}
        }).items()))
        for side in ("current_only", "legacy_only")
    }
    del report["positive_date_gaps"]
    report["current_invalid_positive_volume_by_year"] = {
        k: dict(sorted(v.items())) for k, v in report["current_invalid_positive_volume_by_year"].items()
    }
    report["current_invalid_positive_volume_top_codes"] = {
        k: {"stock_count": len(v), "top": v.most_common(50)}
        for k, v in report["current_invalid_positive_volume_by_code"].items()
    }
    del report["current_invalid_positive_volume_by_code"]
    report["candidate_by_year"] = {k: dict(sorted(v.items())) for k, v in report["candidate_by_year"].items()}
    report["amount_candidate_quality"] = dict(report["amount_candidate_quality"])
    report["field_match"] = {k: dict(v) for k, v in report["field_match"].items()}
    report["samples"] = dict(report["samples"])
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
