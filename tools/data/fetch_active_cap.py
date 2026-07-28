# -*- coding: utf-8 -*-
"""Extract the active-market-cap series from Compass day.vdat."""
from __future__ import annotations

import argparse
import struct
from datetime import datetime
from pathlib import Path
from typing import Sequence

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve()
while not (PROJECT_ROOT / "AGENTS.md").exists() and PROJECT_ROOT != PROJECT_ROOT.parent:
    PROJECT_ROOT = PROJECT_ROOT.parent

VDAT_PATH = Path(r"D:\Compass\WavMain\ANALYSE\Data\ChinaStk\Z_SK\day.vdat")
CSV_PATH = PROJECT_ROOT / "data" / "market" / "active_cap.csv"


def extract_all_records(filepath: str | Path) -> list[dict[str, object]]:
    """Extract plausible date and OHLC records from the binary source."""
    with Path(filepath).open("rb") as handle:
        data = handle.read()

    records: list[dict[str, object]] = []
    record_size = 28
    for offset in range(0, len(data) - record_size):
        date_int = struct.unpack("<I", data[offset : offset + 4])[0]
        if not 19900101 <= date_int <= 20351231:
            continue
        year = date_int // 10000
        month = (date_int // 100) % 100
        day = date_int % 100
        if not (1 <= month <= 12 and 1 <= day <= 31):
            continue

        open_value = struct.unpack("<f", data[offset + 4 : offset + 8])[0]
        high = struct.unpack("<f", data[offset + 8 : offset + 12])[0]
        low = struct.unpack("<f", data[offset + 12 : offset + 16])[0]
        close = struct.unpack("<f", data[offset + 16 : offset + 20])[0]
        if not (100 < close < 500000 and 100 < open_value < 500000):
            continue
        if high < low:
            continue

        records.append(
            {
                "date": datetime(year, month, day),
                "active_cap": round(close, 4),
                "open": round(open_value, 2),
                "high": round(high, 2),
                "low": round(low, 2),
            }
        )
    return records


def update_csv(records: Sequence[dict[str, object]]) -> None:
    """Append records not already present in the production CSV."""
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if CSV_PATH.exists():
        existing = pd.read_csv(CSV_PATH)
        existing["date"] = pd.to_datetime(existing["date"])
        existing_dates = set(existing["date"])
    else:
        existing = pd.DataFrame(columns=["date", "active_cap"])
        existing_dates = set()

    new_records = [
        record for record in records if record["date"] not in existing_dates
    ]
    if not new_records:
        print("No new active-cap records to append.")
        return

    new_frame = pd.DataFrame(new_records)[["date", "active_cap"]]
    combined = pd.concat([existing, new_frame], ignore_index=True)
    combined = (
        combined.sort_values("date")
        .drop_duplicates("date")
        .reset_index(drop=True)
    )
    combined.to_csv(CSV_PATH, index=False)

    print(f"Appended {len(new_records)} active-cap records.")
    print("\nLatest 5 records:")
    for _, row in combined.tail(5).iterrows():
        print(f"  {row['date'].strftime('%Y-%m-%d')}: {row['active_cap']:.2f}")

    if len(combined) >= 2:
        last = combined.iloc[-1]["active_cap"]
        previous = combined.iloc[-2]["active_cap"]
        percent = (last - previous) / previous * 100
        latest_date = combined.iloc[-1]["date"].strftime("%Y-%m-%d")
        print(f"\nLatest change: {percent:+.2f}% ({latest_date})")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract active-cap series from Compass day.vdat"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Rebuild the output CSV from scratch",
    )
    parser.add_argument(
        "--last",
        type=int,
        metavar="N",
        help="Display the latest N extracted records only",
    )
    args = parser.parse_args(argv)

    if not VDAT_PATH.exists():
        print(f"Source file not found: {VDAT_PATH}")
        return 1

    print(f"Reading: {VDAT_PATH}")
    records = extract_all_records(VDAT_PATH)
    print(f"Extracted {len(records)} active-cap records.")
    if records:
        first_date = records[0]["date"].strftime("%Y-%m-%d")
        last_date = records[-1]["date"].strftime("%Y-%m-%d")
        print(f"Date range: {first_date} ~ {last_date}")

    if args.last is not None:
        print(f"\nLatest {args.last} records:")
        for record in records[-args.last :]:
            date_text = record["date"].strftime("%Y-%m-%d")
            print(
                f"  {date_text}: C={record['active_cap']:.2f} "
                f"(O={record['open']:.2f} H={record['high']:.2f} "
                f"L={record['low']:.2f})"
            )
    elif args.full:
        if CSV_PATH.exists():
            CSV_PATH.unlink()
        update_csv(records)
    else:
        update_csv(records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
