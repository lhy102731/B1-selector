"""Incrementally rebuild the daily-return cache when invoked explicitly."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).parent
CACHE_DIR = ROOT / "data" / "indicators_cache"
OUT = ROOT / "data" / "_allpeers_daily_ret.parquet"


def main() -> int:
    """Preserve the legacy CLI workflow without performing work on import."""
    cutoff = datetime.now() - timedelta(days=60)
    rows: list[dict[str, object]] = []
    failures: list[tuple[Path, str]] = []
    paths = sorted(CACHE_DIR.glob("*.parquet"))
    if not paths:
        print(f"[daily_ret] no indicator parquet files found under {CACHE_DIR}")
        return 2
    for path in paths:
        code = path.stem
        try:
            frame = pd.read_parquet(path, columns=["date", "close"])
            frame["date"] = pd.to_datetime(frame["date"])
            frame = frame[frame["date"] >= cutoff].sort_values("date")
            if len(frame) < 2:
                continue
            frame["ret"] = frame["close"].pct_change()
            for _, row in frame.dropna(subset=["ret"]).iterrows():
                rows.append(
                    {
                        "date": row["date"],
                        "code": code,
                        "ret": float(row["ret"]),
                    }
                )
        except Exception as exc:
            failures.append((path, str(exc)))

    if failures:
        print(f"[daily_ret] failed to read {len(failures)} indicator files")
        for path, error in failures[:20]:
            print(f"  FAIL {path.name}: {error}")
        return 2
    if not rows:
        print("[daily_ret] no recent return rows were produced")
        return 2

    output = pd.DataFrame(rows)
    if OUT.exists():
        old = pd.read_parquet(OUT)
        old["date"] = pd.to_datetime(old["date"])
        old = old[old["date"] < cutoff]
        output = pd.concat([old, output], ignore_index=True).drop_duplicates(
            subset=["date", "code"]
        )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUT.with_suffix(OUT.suffix + ".tmp")
    try:
        output.to_parquet(temporary, index=False)
        os.replace(temporary, OUT)
    finally:
        if temporary.exists():
            temporary.unlink()
    if rows:
        print(
            f"[daily_ret] added {len(rows)} rows "
            f"(dates: {output['date'].min().date()} ~ "
            f"{output['date'].max().date()}), "
            f"{output['code'].nunique()} stocks -> {OUT}"
        )
    else:
        print("[daily_ret] up-to-date, nothing to add")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
