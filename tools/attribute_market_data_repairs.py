"""Read-only provenance attribution for recent market-data scale breaks.

The command compares rows already identified by the semantic audit with
historical repair backups.  It never repairs, moves, or deletes source data.
Ambiguous provenance is preserved: a row that appears in several snapshots is
reported with every matching source rather than assigned to one by guesswork.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]


OHLC_FIELDS = ("open", "high", "low", "close")
READ_FIELDS = ("date", *OHLC_FIELDS, "change_pct")
DEFAULT_DATES = ("2026-06-24", "2026-06-25")
DEFAULT_HEAD_ROWS = 128
DEFAULT_MIN_SCALE_BREAK = 0.0025
DEFAULT_SCALE_RATIO_TOLERANCE = 0.0025

KNOWN_EM_BACKUPS = (
    "backup",
    "bulk_backup",
    "market_cap_backup",
    "recent_factor_backup",
    "remaining_factor_backup",
    "baostock_turnover_backup",
    "today_baostock_backup",
    "today_suspended_backup",
    "today_update_backup",
)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _same_number(left: Any, right: Any) -> bool:
    left_number = _number(left)
    right_number = _number(right)
    if left_number is None or right_number is None:
        return False
    return math.isclose(left_number, right_number, rel_tol=1e-9, abs_tol=1e-8)


def matching_fields(
    current: dict[str, Any], candidate: dict[str, Any]
) -> tuple[bool, bool]:
    """Return ``(full_ohlc_match, close_match)`` for two daily rows."""

    close_match = _same_number(current.get("close"), candidate.get("close"))
    full_match = close_match and all(
        _same_number(current.get(field), candidate.get(field))
        for field in OHLC_FIELDS
    )
    return full_match, close_match


def classify_source_matches(
    full_matches: Sequence[str], close_matches: Sequence[str]
) -> str:
    """Classify provenance without discarding multi-snapshot ambiguity."""

    if len(full_matches) > 1:
        return "MULTI_SOURCE_FULL_MATCH"
    if len(full_matches) == 1:
        return "UNIQUE_FULL_MATCH"
    if close_matches:
        return "CLOSE_ONLY_MATCH"
    return "UNATTRIBUTED"


def detect_scale_sandwich(
    rows: Iterable[dict[str, Any]],
    middle_date: str,
    right_date: str,
    *,
    min_scale_break: float = DEFAULT_MIN_SCALE_BREAK,
    scale_ratio_tolerance: float = DEFAULT_SCALE_RATIO_TOLERANCE,
) -> dict[str, Any]:
    """Detect a new-scale -> old-scale -> new-scale three-bar sequence.

    The saved percentage return on the middle bar implies what the left close
    would have been on the middle bar's scale.  The saved return on the right
    bar independently implies what the middle close would have been on the
    right bar's scale.  A sandwich exists when both implied scale ratios are on
    the same side of one, materially non-unit, and agree within tolerance.
    """

    result: dict[str, Any] = {
        "eligible": False,
        "detected": False,
        "reason": "missing_rows",
        "left_date": None,
        "middle_date": middle_date,
        "right_date": right_date,
        "left_to_middle_scale_ratio": None,
        "right_to_middle_scale_ratio": None,
        "ratio_gap": None,
    }
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        raw_date = row.get("date")
        try:
            date = pd.Timestamp(raw_date).strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            continue
        if date in seen:
            result["reason"] = "duplicate_dates"
            return result
        seen.add(date)
        normalized.append({**row, "date": date})

    normalized.sort(key=lambda item: item["date"])
    positions = {row["date"]: index for index, row in enumerate(normalized)}
    if middle_date not in positions or right_date not in positions:
        return result
    middle_index = positions[middle_date]
    right_index = positions[right_date]
    if middle_index == 0 or right_index != middle_index + 1:
        result["reason"] = "non_adjacent_trading_rows"
        return result

    left = normalized[middle_index - 1]
    middle = normalized[middle_index]
    right = normalized[right_index]
    left_close = _number(left.get("close"))
    middle_close = _number(middle.get("close"))
    right_close = _number(right.get("close"))
    middle_return = _number(middle.get("change_pct"))
    right_return = _number(right.get("change_pct"))
    values = (left_close, middle_close, right_close, middle_return, right_return)
    if any(value is None for value in values):
        result["reason"] = "missing_numeric_values"
        return result
    assert left_close is not None
    assert middle_close is not None
    assert right_close is not None
    assert middle_return is not None
    assert right_return is not None
    middle_multiplier = 1.0 + middle_return / 100.0
    right_multiplier = 1.0 + right_return / 100.0
    if min(left_close, middle_close, right_close) <= 0 or min(
        middle_multiplier, right_multiplier
    ) <= 0:
        result["reason"] = "non_positive_values"
        return result

    expected_left_on_middle_scale = middle_close / middle_multiplier
    expected_middle_on_right_scale = right_close / right_multiplier
    left_ratio = left_close / expected_left_on_middle_scale
    right_ratio = expected_middle_on_right_scale / middle_close
    ratio_gap = abs(left_ratio - right_ratio) / max(abs(left_ratio), abs(right_ratio))
    same_side = (left_ratio > 1.0 and right_ratio > 1.0) or (
        left_ratio < 1.0 and right_ratio < 1.0
    )
    materially_non_unit = (
        abs(left_ratio - 1.0) >= float(min_scale_break)
        and abs(right_ratio - 1.0) >= float(min_scale_break)
    )

    if not materially_non_unit:
        reason = "below_min_scale_break"
    elif not same_side:
        reason = "ratios_opposite_sides"
    elif ratio_gap > float(scale_ratio_tolerance):
        reason = "ratio_disagreement"
    else:
        reason = "scale_sandwich"

    result.update(
        {
            "eligible": True,
            "detected": bool(
                same_side
                and materially_non_unit
                and ratio_gap <= float(scale_ratio_tolerance)
            ),
            "reason": reason,
            "left_date": left["date"],
            "left_to_middle_scale_ratio": left_ratio,
            "right_to_middle_scale_ratio": right_ratio,
            "ratio_gap": ratio_gap,
        }
    )
    return result


def discover_candidate_roots(
    tencent_repair_dir: str | Path,
    em_repair_dir: str | Path,
) -> dict[str, Path]:
    """Return every known repair backup root in deterministic stage order."""

    candidates: dict[str, Path] = {}
    tencent_backup = Path(tencent_repair_dir).resolve() / "backup"
    if tencent_backup.is_dir():
        candidates["tencent_backup"] = tencent_backup

    em_root = Path(em_repair_dir).resolve()
    added: set[str] = set()
    for directory_name in KNOWN_EM_BACKUPS:
        path = em_root / directory_name
        if path.is_dir():
            candidates[f"em_{directory_name}"] = path
            added.add(directory_name)
    if em_root.is_dir():
        for path in sorted(em_root.iterdir(), key=lambda item: item.name):
            if (
                path.is_dir()
                and path.name.endswith("backup")
                and path.name not in added
            ):
                candidates[f"em_{path.name}"] = path
    return candidates


def _stock_path(root: Path, code: str) -> Path:
    return root / code[:2] / f"{code}.csv"


def _read_recent_rows(path: Path, head_rows: int) -> list[dict[str, Any]]:
    frame = pd.read_csv(
        path,
        encoding="gbk",
        nrows=max(3, int(head_rows)),
        usecols=lambda name: name in READ_FIELDS,
    )
    if "date" not in frame.columns:
        raise ValueError("missing date column")
    records: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        try:
            record["date"] = pd.Timestamp(record["date"]).strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            continue
        records.append(record)
    return records


def _rows_by_date(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    duplicates: set[str] = set()
    for row in rows:
        date = str(row.get("date") or "")
        if not date:
            continue
        if date in result:
            duplicates.add(date)
        result[date] = row
    for date in duplicates:
        result.pop(date, None)
    return result


def _load_targets(
    issues_path: str | Path, target_dates: Sequence[str]
) -> list[dict[str, str]]:
    dates = set(target_dates)
    unique: dict[tuple[str, str], dict[str, str]] = {}
    with Path(issues_path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"code", "date", "issue"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError(f"issues CSV must contain {sorted(required)}")
        for row in reader:
            code = str(row.get("code") or "").zfill(6)
            date = str(row.get("date") or "")
            if row.get("issue") != "RETURN_MISMATCH" or date not in dates:
                continue
            unique[(code, date)] = {
                "code": code,
                "date": date,
                "issue": "RETURN_MISMATCH",
            }
    return [unique[key] for key in sorted(unique)]


def attribute_repairs(
    *,
    issues_path: str | Path,
    data_dir: str | Path,
    tencent_repair_dir: str | Path,
    em_repair_dir: str | Path,
    target_dates: Sequence[str] = DEFAULT_DATES,
    head_rows: int = DEFAULT_HEAD_ROWS,
    min_scale_break: float = DEFAULT_MIN_SCALE_BREAK,
    scale_ratio_tolerance: float = DEFAULT_SCALE_RATIO_TOLERANCE,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Attribute all selected semantic issues to available repair snapshots."""

    targets = _load_targets(issues_path, target_dates)
    targets_by_code: dict[str, list[dict[str, str]]] = defaultdict(list)
    for target in targets:
        targets_by_code[target["code"]].append(target)

    current_root = Path(data_dir).resolve()
    candidates = discover_candidate_roots(tencent_repair_dir, em_repair_dir)
    read_counts: Counter[str] = Counter()
    read_failures: list[dict[str, str]] = []
    attribution_rows: list[dict[str, Any]] = []
    pattern_rows: list[dict[str, Any]] = []

    for code in sorted(targets_by_code):
        current_path = _stock_path(current_root, code)
        try:
            current_rows = _read_recent_rows(current_path, head_rows)
            read_counts["current"] += 1
        except Exception as error:  # noqa: BLE001 - every unreadable source is audited.
            read_failures.append(
                {
                    "source": "current",
                    "path": str(current_path),
                    "error": f"{type(error).__name__}: {error}",
                }
            )
            current_rows = []
        current_by_date = _rows_by_date(current_rows)

        candidate_rows: dict[str, list[dict[str, Any]]] = {}
        candidate_by_date: dict[str, dict[str, dict[str, Any]]] = {}
        for source, root in candidates.items():
            path = _stock_path(root, code)
            if not path.is_file():
                candidate_rows[source] = []
                candidate_by_date[source] = {}
                continue
            try:
                rows = _read_recent_rows(path, head_rows)
                read_counts[source] += 1
            except Exception as error:  # noqa: BLE001 - failures cannot be silently absent.
                read_failures.append(
                    {
                        "source": source,
                        "path": str(path),
                        "error": f"{type(error).__name__}: {error}",
                    }
                )
                rows = []
            candidate_rows[source] = rows
            candidate_by_date[source] = _rows_by_date(rows)

        sorted_dates = sorted(target_dates)
        if len(sorted_dates) == 2:
            pattern = detect_scale_sandwich(
                current_rows,
                sorted_dates[0],
                sorted_dates[1],
                min_scale_break=min_scale_break,
                scale_ratio_tolerance=scale_ratio_tolerance,
            )
        else:
            pattern = {
                "eligible": False,
                "detected": False,
                "reason": "requires_two_target_dates",
                "left_date": None,
                "middle_date": None,
                "right_date": None,
                "left_to_middle_scale_ratio": None,
                "right_to_middle_scale_ratio": None,
                "ratio_gap": None,
            }

        left_date = pattern.get("left_date")
        tencent = candidate_by_date.get("tencent_backup", {})
        tencent_center_match = False
        tencent_left_match = False
        tencent_right_match = False
        if len(sorted_dates) == 2:
            middle_date, right_date = sorted_dates
            if current_by_date.get(middle_date) and tencent.get(middle_date):
                tencent_center_match = matching_fields(
                    current_by_date[middle_date], tencent[middle_date]
                )[0]
            if left_date and current_by_date.get(left_date) and tencent.get(left_date):
                tencent_left_match = matching_fields(
                    current_by_date[left_date], tencent[left_date]
                )[0]
            if current_by_date.get(right_date) and tencent.get(right_date):
                tencent_right_match = matching_fields(
                    current_by_date[right_date], tencent[right_date]
                )[0]
        source_backed_sandwich = bool(
            pattern.get("detected")
            and tencent_center_match
            and not tencent_left_match
            and not tencent_right_match
        )
        pattern_row = {
            "code": code,
            **pattern,
            "tencent_left_full_ohlc_match": tencent_left_match,
            "tencent_middle_full_ohlc_match": tencent_center_match,
            "tencent_right_full_ohlc_match": tencent_right_match,
            "tencent_source_backed_sandwich": source_backed_sandwich,
        }
        pattern_rows.append(pattern_row)

        for target in targets_by_code[code]:
            date = target["date"]
            current = current_by_date.get(date)
            if current is None:
                attribution_rows.append(
                    {
                        **target,
                        "current_open": None,
                        "current_high": None,
                        "current_low": None,
                        "current_close": None,
                        "current_change_pct": None,
                        "sources_with_date": "",
                        "full_ohlc_match_sources": "",
                        "close_match_sources": "",
                        "attribution_class": "CURRENT_ROW_MISSING",
                        "scale_sandwich_detected": bool(pattern.get("detected")),
                        "tencent_source_backed_sandwich": source_backed_sandwich,
                    }
                )
                continue

            sources_with_date: list[str] = []
            full_matches: list[str] = []
            close_matches: list[str] = []
            for source in candidates:
                candidate = candidate_by_date[source].get(date)
                if candidate is None:
                    continue
                sources_with_date.append(source)
                full_match, close_match = matching_fields(current, candidate)
                if full_match:
                    full_matches.append(source)
                if close_match:
                    close_matches.append(source)
            attribution_rows.append(
                {
                    **target,
                    "current_open": _number(current.get("open")),
                    "current_high": _number(current.get("high")),
                    "current_low": _number(current.get("low")),
                    "current_close": _number(current.get("close")),
                    "current_change_pct": _number(current.get("change_pct")),
                    "sources_with_date": ";".join(sources_with_date),
                    "full_ohlc_match_sources": ";".join(full_matches),
                    "close_match_sources": ";".join(close_matches),
                    "attribution_class": classify_source_matches(
                        full_matches, close_matches
                    ),
                    "scale_sandwich_detected": bool(pattern.get("detected")),
                    "tencent_source_backed_sandwich": source_backed_sandwich,
                }
            )

    class_counts = Counter(row["attribution_class"] for row in attribution_rows)
    date_class_counts: dict[str, Counter[str]] = defaultdict(Counter)
    source_presence = Counter()
    source_full_matches = Counter()
    source_close_matches = Counter()
    source_date_presence: dict[str, Counter[str]] = defaultdict(Counter)
    source_date_full_matches: dict[str, Counter[str]] = defaultdict(Counter)
    source_date_close_matches: dict[str, Counter[str]] = defaultdict(Counter)
    match_sets = Counter()
    for row in attribution_rows:
        date_class_counts[row["date"]][row["attribution_class"]] += 1
        full_set = row["full_ohlc_match_sources"] or "<none>"
        match_sets[full_set] += 1
        for source in filter(None, row["sources_with_date"].split(";")):
            source_presence[source] += 1
            source_date_presence[source][row["date"]] += 1
        for source in filter(None, row["full_ohlc_match_sources"].split(";")):
            source_full_matches[source] += 1
            source_date_full_matches[source][row["date"]] += 1
        for source in filter(None, row["close_match_sources"].split(";")):
            source_close_matches[source] += 1
            source_date_close_matches[source][row["date"]] += 1

    summary: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "INCOMPLETE" if read_failures else "COMPLETE",
        "issues_path": str(Path(issues_path).resolve()),
        "data_dir": str(current_root),
        "target_dates": list(target_dates),
        "target_rows": len(targets),
        "target_codes": len(targets_by_code),
        "candidate_sources": {
            source: str(root) for source, root in candidates.items()
        },
        "head_rows": int(head_rows),
        "min_scale_break": float(min_scale_break),
        "scale_ratio_tolerance": float(scale_ratio_tolerance),
        "attribution_class_counts": dict(sorted(class_counts.items())),
        "unattributed_rows": int(class_counts.get("UNATTRIBUTED", 0)),
        "current_row_missing": int(class_counts.get("CURRENT_ROW_MISSING", 0)),
        "per_date_class_counts": {
            date: dict(sorted(counts.items()))
            for date, counts in sorted(date_class_counts.items())
        },
        "full_ohlc_match_set_counts": dict(
            sorted(match_sets.items(), key=lambda item: (-item[1], item[0]))
        ),
        "source_stats": {
            source: {
                "files_read": int(read_counts[source]),
                "rows_with_date": int(source_presence[source]),
                "full_ohlc_matches": int(source_full_matches[source]),
                "close_matches": int(source_close_matches[source]),
                "per_date": {
                    date: {
                        "rows_with_date": int(source_date_presence[source][date]),
                        "full_ohlc_matches": int(
                            source_date_full_matches[source][date]
                        ),
                        "close_matches": int(
                            source_date_close_matches[source][date]
                        ),
                    }
                    for date in target_dates
                },
            }
            for source in candidates
        },
        "scale_sandwich": {
            "codes_evaluated": len(pattern_rows),
            "eligible_codes": sum(bool(row["eligible"]) for row in pattern_rows),
            "detected_codes": sum(bool(row["detected"]) for row in pattern_rows),
            "tencent_source_backed_codes": sum(
                bool(row["tencent_source_backed_sandwich"])
                for row in pattern_rows
            ),
        },
        "read_failure_count": len(read_failures),
        "read_failures": read_failures[:500],
    }
    return summary, attribution_rows, pattern_rows


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
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


def _csv_text(rows: Sequence[dict[str, Any]]) -> str:
    if not rows:
        return ""
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        handle.seek(0)
        return handle.read()


def write_reports(
    output_dir: str | Path,
    summary: dict[str, Any],
    attribution_rows: Sequence[dict[str, Any]],
    pattern_rows: Sequence[dict[str, Any]],
) -> tuple[Path, Path, Path]:
    destination = Path(output_dir).resolve()
    summary_path = destination / "repair_attribution_summary.json"
    attribution_path = destination / "repair_attribution.csv"
    pattern_path = destination / "scale_sandwich_codes.csv"
    _write_text_atomic(
        summary_path,
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    _write_text_atomic(attribution_path, _csv_text(attribution_rows))
    _write_text_atomic(pattern_path, _csv_text(pattern_rows))
    return summary_path, attribution_path, pattern_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issues", required=True)
    parser.add_argument("--data-dir", default=str(PROJECT_ROOT / "data"))
    parser.add_argument(
        "--tencent-repair-dir",
        default=str(
            PROJECT_ROOT / "artifacts" / "maintenance" / "tencent_20260625"
        ),
    )
    parser.add_argument(
        "--em-repair-dir",
        default=str(
            PROJECT_ROOT / "artifacts" / "maintenance" / "em_anchor_20260626"
        ),
    )
    parser.add_argument("--dates", default=",".join(DEFAULT_DATES))
    parser.add_argument("--head-rows", type=int, default=DEFAULT_HEAD_ROWS)
    parser.add_argument("--min-scale-break", type=float, default=DEFAULT_MIN_SCALE_BREAK)
    parser.add_argument(
        "--scale-ratio-tolerance",
        type=float,
        default=DEFAULT_SCALE_RATIO_TOLERANCE,
    )
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    target_dates = tuple(
        date.strip() for date in args.dates.split(",") if date.strip()
    )
    summary, attribution_rows, pattern_rows = attribute_repairs(
        issues_path=args.issues,
        data_dir=args.data_dir,
        tencent_repair_dir=args.tencent_repair_dir,
        em_repair_dir=args.em_repair_dir,
        target_dates=target_dates,
        head_rows=args.head_rows,
        min_scale_break=args.min_scale_break,
        scale_ratio_tolerance=args.scale_ratio_tolerance,
    )
    summary_path, attribution_path, pattern_path = write_reports(
        args.output_dir, summary, attribution_rows, pattern_rows
    )
    print(f"status={summary['status']}")
    print(f"target_rows={summary['target_rows']} target_codes={summary['target_codes']}")
    print(f"attribution_class_counts={summary['attribution_class_counts']}")
    print(f"scale_sandwich={summary['scale_sandwich']}")
    print(f"summary={summary_path}")
    print(f"attribution={attribution_path}")
    print(f"scale_sandwich_codes={pattern_path}")
    return 2 if summary["status"] == "INCOMPLETE" else 0


if __name__ == "__main__":
    raise SystemExit(main())
