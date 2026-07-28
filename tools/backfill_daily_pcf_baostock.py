"""Fill the daily ``pcfNcfTTM`` gap after the THS market-data update.

THS Wencai exposes an operating-cash-flow PCF, which is not the repository's
BaoStock-compatible net-cash-flow PCF contract.  This exact-day fallback runs
only after THS has written the daily bar and changes only a missing ``pcf``.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from contextlib import redirect_stdout
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_valuation_fields import (
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _fetch_baostock,
    _read_csv,
    _update_manifest,
    stock_files,
    valid_valuation,
)
from utils.baostock_lock import serialized_baostock


def merge_daily_pcf(
    frame: pd.DataFrame,
    remote: pd.DataFrame,
    day: str,
) -> tuple[pd.DataFrame, bool]:
    result = frame.copy()
    if result.empty or remote.empty or "date" not in result or "date" not in remote:
        return result, False
    if "pcf" not in result:
        result["pcf"] = pd.NA
    target_dates = _date_keys(result["date"])
    mask = target_dates.eq(day)
    if not mask.any() or valid_valuation(result.loc[mask, "pcf"], "pcf").iloc[0]:
        return result, False
    source = remote.assign(_date=_date_keys(remote["date"]))
    source = source.loc[source["_date"].eq(day)]
    if source.empty or "pcf" not in source:
        return result, False
    value = pd.to_numeric(source["pcf"], errors="coerce").iloc[-1]
    if not valid_valuation(pd.Series([value]), "pcf").iloc[0]:
        return result, False
    result.loc[mask, "pcf"] = float(value)
    return result, True


@serialized_baostock
def run(
    data_dir: Path,
    *,
    day: str,
    apply: bool,
    max_stocks: int | None,
    start_index: int = 0,
) -> int:
    import baostock as bs

    all_files = stock_files(data_dir)
    start_index = max(0, int(start_index))
    files = all_files[start_index:]
    if max_stocks is not None:
        files = files[: max(0, int(max_stocks))]
    checkpoint = data_dir / "_daily_updates" / day
    backup_dir = checkpoint / "pcf_backup"
    bounded = start_index > 0 or max_stocks is not None
    report_path = (
        checkpoint / f"pcf_fallback_report_{start_index}_{start_index + len(files)}.json"
        if bounded
        else checkpoint / "pcf_fallback_report.json"
    )
    counts = {"target_files": 0, "filled": 0, "unavailable": 0, "failed": 0}
    failures: list[dict[str, str]] = []
    started = time.time()
    with redirect_stdout(open(os.devnull, "w")):
        login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"BaoStock login failed: {login.error_code} {login.error_msg}")
    try:
        for index, path in enumerate(files, 1):
            frame = _read_csv(path)
            dates = _date_keys(frame.get("date", pd.Series(dtype=object)))
            mask = dates.eq(day)
            if not mask.any():
                continue
            pcf = frame.get("pcf", pd.Series(pd.NA, index=frame.index))
            if valid_valuation(pcf.loc[mask], "pcf").iloc[0]:
                continue
            counts["target_files"] += 1
            try:
                remote = _fetch_baostock(bs, path.stem, day, day)
                merged, changed = merge_daily_pcf(frame, remote, day)
                if changed:
                    counts["filled"] += 1
                    if apply:
                        backup = backup_dir / path.relative_to(data_dir)
                        backup.parent.mkdir(parents=True, exist_ok=True)
                        if not backup.exists():
                            shutil.copy2(path, backup)
                        _atomic_csv(merged, path)
                else:
                    counts["unavailable"] += 1
            except Exception as exc:
                counts["failed"] += 1
                failures.append(
                    {"code": path.stem, "type": type(exc).__name__, "error": str(exc)[:500]}
                )
            if index == 1 or index % 250 == 0:
                print(
                    f"daily-pcf {index}/{len(files)} targets={counts['target_files']} "
                    f"filled={counts['filled']} failed={counts['failed']}",
                    flush=True,
                )
    finally:
        with redirect_stdout(open(os.devnull, "w")):
            bs.logout()

    report = {
        "status": "COMPLETED" if not failures else "FAILED",
        "applied": bool(apply),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "date": day,
        "source": "baostock pcfNcfTTM exact-day fallback after THS",
        "data_dir": str(data_dir),
        "start_index": start_index,
        "files": len(files),
        "counts": counts,
        "failures": failures,
        "elapsed_seconds": round(time.time() - started, 3),
    }
    _atomic_json(report, report_path)
    if apply and not failures and not bounded:
        _update_manifest(
            data_dir,
            "daily_pcf_baostock_fallback",
            {
                "source": "baostock pcfNcfTTM",
                "date": day,
                "filled": counts["filled"],
                "unavailable": counts["unavailable"],
                "report": str(report_path),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"counts={counts}", flush=True)
    return 0 if not failures else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--max-stocks", type=int, default=None)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(
        run(
            Path(args.data_dir).resolve(),
            day=args.date,
            apply=args.apply,
            max_stocks=args.max_stocks,
            start_index=args.start_index,
        )
    )
