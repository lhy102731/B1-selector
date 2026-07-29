"""Read-only cross-sectional semantic audit for source stock CSV files.

This command never repairs, moves, or deletes source data.  It distinguishes a
byte-readable snapshot from a research-eligible snapshot by looking for
same-date market-wide return and amplitude inconsistencies.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.market_data_semantics import (
    DEFAULT_AMPLITUDE_ERROR_PP,
    DEFAULT_CROSS_SECTIONAL_SPIKE_RATIO,
    DEFAULT_MIN_ELIGIBLE,
    DEFAULT_RETURN_ERROR_PP,
    audit_frame,
    summarize_checks,
)


SOURCE_PREFIXES = ("00", "30", "60", "68")
AUDIT_COLUMNS = {
    "date",
    "open",
    "high",
    "low",
    "close",
    "change_pct",
    "amplitude",
}


def iter_stock_files(data_dir: Path) -> list[Path]:
    files: list[Path] = []
    for prefix in SOURCE_PREFIXES:
        source_dir = data_dir / prefix
        if source_dir.is_dir():
            files.extend(sorted(source_dir.glob("*.csv")))
    return files


def _issue_rows(row: dict[str, Any]) -> Iterator[dict[str, Any]]:
    if row.get("return_bad"):
        yield {
            "code": row["code"],
            "date": row["date"],
            "issue": "RETURN_MISMATCH",
            "stored": row.get("stored_change_pct"),
            "calculated": row.get("calculated_change_pct"),
            "error_pp": row.get("return_error_pp"),
        }
    if row.get("amplitude_bad"):
        yield {
            "code": row["code"],
            "date": row["date"],
            "issue": "AMPLITUDE_MISMATCH",
            "stored": row.get("stored_amplitude_pct"),
            "calculated": row.get("calculated_amplitude_pct"),
            "error_pp": row.get("amplitude_error_pp"),
        }


def scan_data_dir(
    data_dir: str | Path,
    *,
    overlay_dir: str | Path | None = None,
    recent_rows: int = 160,
    return_error_pp: float = DEFAULT_RETURN_ERROR_PP,
    amplitude_error_pp: float = DEFAULT_AMPLITUDE_ERROR_PP,
    cross_sectional_spike_ratio: float = DEFAULT_CROSS_SECTIONAL_SPIKE_RATIO,
    min_eligible: int = DEFAULT_MIN_ELIGIBLE,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Scan source CSV files and return an aggregate summary plus issue rows."""

    root = Path(data_dir).resolve()
    overlay_root = Path(overlay_dir).resolve() if overlay_dir is not None else None
    files = iter_stock_files(root)
    state: dict[str, Any] = {
        "files_scanned": 0,
        "files_failed": 0,
        "overlay_files_used": 0,
        "failures": [],
    }
    bad_rows: list[dict[str, Any]] = []

    def all_checks() -> Iterator[dict[str, Any]]:
        for source_path in files:
            state["files_scanned"] += 1
            path = source_path
            if overlay_root is not None:
                replacement = overlay_root / source_path.relative_to(root)
                if replacement.is_file():
                    path = replacement
                    state["overlay_files_used"] += 1
            try:
                frame = pd.read_csv(
                    path,
                    encoding="gbk",
                    nrows=max(2, int(recent_rows)),
                    usecols=lambda name: name in AUDIT_COLUMNS,
                )
                checks = audit_frame(
                    frame,
                    code=source_path.stem,
                    return_error_pp=return_error_pp,
                    amplitude_error_pp=amplitude_error_pp,
                )
            except Exception as error:  # noqa: BLE001 - every unreadable source must be counted.
                state["files_failed"] += 1
                if len(state["failures"]) < 200:
                    state["failures"].append(
                        {
                            "path": str(path),
                            "error": f"{type(error).__name__}: {error}",
                        }
                    )
                continue
            for row in checks:
                bad_rows.extend(_issue_rows(row))
                yield row

    summary = summarize_checks(
        all_checks(),
        cross_sectional_spike_ratio=cross_sectional_spike_ratio,
        min_eligible=min_eligible,
    )
    summary.update(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "data_dir": str(root),
            "overlay_dir": str(overlay_root) if overlay_root is not None else None,
            "overlay_files_used": int(state["overlay_files_used"]),
            "recent_rows": int(recent_rows),
            "return_error_pp": float(return_error_pp),
            "amplitude_error_pp": float(amplitude_error_pp),
            "files_scanned": int(state["files_scanned"]),
            "files_failed": int(state["files_failed"]),
            "bad_row_count": len(bad_rows),
            "failures": state["failures"],
        }
    )
    if state["files_failed"]:
        summary["status"] = "SEMANTIC_QUARANTINE"
    return summary, bad_rows


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


def write_reports(
    output_dir: str | Path,
    summary: dict[str, Any],
    bad_rows: list[dict[str, Any]],
) -> tuple[Path, Path]:
    destination = Path(output_dir).resolve()
    summary_path = destination / "semantic_summary.json"
    issues_path = destination / "semantic_issues.csv"
    _write_text_atomic(
        summary_path,
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )

    fieldnames = ["code", "date", "issue", "stored", "calculated", "error_pp"]
    lines: list[str] = []
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(bad_rows)
        handle.seek(0)
        lines.append(handle.read())
    _write_text_atomic(issues_path, "".join(lines))
    return summary_path, issues_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=str(PROJECT_ROOT / "data"))
    parser.add_argument("--overlay-dir")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--recent-rows", type=int, default=160)
    parser.add_argument("--return-error-pp", type=float, default=DEFAULT_RETURN_ERROR_PP)
    parser.add_argument("--amplitude-error-pp", type=float, default=DEFAULT_AMPLITUDE_ERROR_PP)
    parser.add_argument(
        "--cross-sectional-spike-ratio",
        type=float,
        default=DEFAULT_CROSS_SECTIONAL_SPIKE_RATIO,
    )
    parser.add_argument("--min-eligible", type=int, default=DEFAULT_MIN_ELIGIBLE)
    parser.add_argument("--fail-on-quarantine", action="store_true")
    args = parser.parse_args()

    summary, bad_rows = scan_data_dir(
        args.data_dir,
        overlay_dir=args.overlay_dir,
        recent_rows=args.recent_rows,
        return_error_pp=args.return_error_pp,
        amplitude_error_pp=args.amplitude_error_pp,
        cross_sectional_spike_ratio=args.cross_sectional_spike_ratio,
        min_eligible=args.min_eligible,
    )
    summary_path, issues_path = write_reports(args.output_dir, summary, bad_rows)
    print(f"status={summary['status']}")
    print(f"files_scanned={summary['files_scanned']} files_failed={summary['files_failed']}")
    print(f"bad_row_count={summary['bad_row_count']}")
    print(f"summary={summary_path}")
    print(f"issues={issues_path}")
    if args.fail_on_quarantine and summary["status"] == "SEMANTIC_QUARANTINE":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
