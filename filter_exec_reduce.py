# -*- coding: utf-8 -*-
"""
Filter out Brick signals with same-day executive share reduction.

Usage:
    python filter_exec_reduce.py --signals artifacts/daily/brick/signals_today.csv [--date YYYY-MM-DD]
"""

import argparse
import time
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")


def get_reduce_codes(date_str):
    """Fetch stocks with executive reduction ending on date_str.

    Returns:
        set[str]: fetched reduction codes, possibly empty.
        None: upstream fetch failed; caller should skip filtering explicitly.
    """
    import akshare as ak

    last_error = None
    for attempt in range(1, 4):
        try:
            df = ak.stock_ggcg_em(symbol="全部")
            break
        except Exception as exc:
            last_error = exc
            print(f"[reduction] fetch failed attempt {attempt}/3: {exc}")
            if attempt < 3:
                time.sleep(2 * attempt)
    else:
        print(f"[reduction] fetch unavailable, skip reduction filter: {last_error}")
        return None

    if df is None or df.empty:
        print(f"[reduction] {date_str}: upstream returned empty table")
        return set()

    df["code"] = df.iloc[:, 0].astype(str).str.zfill(6)
    change_col = df.columns[5]
    end_col = df.columns[14]
    df = df[df[change_col] == "减持"]
    df[end_col] = pd.to_datetime(df[end_col], errors="coerce")
    target = pd.to_datetime(date_str).date()
    codes = set(df[df[end_col].dt.date == target]["code"])
    print(f"[reduction] {date_str}: {len(codes)} stocks with executive reduction")
    return codes


def main():
    parser = argparse.ArgumentParser(description="Filter executive reduction signals")
    parser.add_argument("--signals", type=str, default="artifacts/daily/brick/signals_today.csv")
    parser.add_argument("--date", type=str, default=None, help="Signal date YYYY-MM-DD")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    signals_path = Path(args.signals)
    if not signals_path.exists():
        print(f"[reduction] signals file not found: {signals_path}, skip")
        return

    df = pd.read_csv(signals_path, encoding="gbk")
    if "code" in df.columns:
        df["code"] = (
            df["code"]
            .astype("string")
            .str.replace(r"\.0$", "", regex=True)
            .str.zfill(6)
        )
    if df.empty:
        print("[reduction] signal file is empty, no filtering needed")
        return

    if args.date:
        date_str = args.date
    elif "entry_date" in df.columns:
        date_str = str(df["entry_date"].iloc[0])[:10]
    elif "signal_date" in df.columns:
        date_str = str(df["signal_date"].iloc[0])[:10]
    else:
        from datetime import datetime, timedelta

        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    reduce_codes = get_reduce_codes(date_str)
    if reduce_codes is None:
        print("[reduction] upstream unavailable, keeping original signals unchanged")
        return
    if not reduce_codes:
        print("[reduction] no reductions today, no filtering needed")
        return

    before = len(df)
    df = df[~df["code"].astype(str).str.zfill(6).isin(reduce_codes)]
    after = len(df)
    removed = before - after
    print(f"[reduction] removed {removed} signals with executive reduction ({removed / before * 100:.1f}%)")

    out_path = Path(args.output) if args.output else signals_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="gbk")
    print(f"[reduction] saved {after} signals to {out_path}")


if __name__ == "__main__":
    main()
