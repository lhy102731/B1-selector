"""Generate a typed, research-only ETF technical candidate pool."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategy.unified_b1_strategy import UnifiedB1Strategy
from utils.asset_policy import AssetPolicy
from utils.selection_universe import SelectionUniverse


OUTPUT_COLUMNS = (
    "instrument_id",
    "asset_type",
    "code",
    "name",
    "signal_date",
    "close",
    "J",
    "reasons",
    "validation_status",
)


def scan_etf_candidates(
    *,
    data_dir: str | Path = "data",
    signal_date: str,
    strategy: Any | None = None,
    max_etfs: int | None = None,
) -> list[dict[str, Any]]:
    """Return ETF signals separately from all stock-trained ranking models."""
    data_dir = Path(data_dir)
    target = pd.Timestamp(signal_date).normalize()
    strategy = strategy or UnifiedB1Strategy()
    policy = AssetPolicy.for_asset_type("etf")
    assets = [
        asset
        for asset in SelectionUniverse(data_dir).list_assets(include_etfs=True)
        if asset.asset_type == "etf"
    ]
    if max_etfs is not None:
        assets = assets[:max_etfs]

    candidates: list[dict[str, Any]] = []
    for asset in assets:
        cache_path = data_dir / "indicators_cache" / "etf" / f"{asset.code}.parquet"
        if not cache_path.is_file():
            continue
        try:
            frame = pd.read_parquet(cache_path)
            if frame.empty or "date" not in frame.columns:
                continue
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
            frame = frame.dropna(subset=["date"]).sort_values("date", ascending=False).reset_index(drop=True)
            if frame.empty or frame.iloc[0]["date"].normalize() != target:
                continue
            signals = strategy.select_stocks(frame, asset.name, asset_type="etf")
            if not signals:
                continue
            signal = dict(signals[0])
            reasons = signal.get("reasons", [])
            candidates.append(
                {
                    "instrument_id": f"etf:{asset.code}",
                    "asset_type": "etf",
                    "code": asset.code,
                    "name": asset.name,
                    "signal_date": target.strftime("%Y-%m-%d"),
                    "close": signal.get("close", frame.iloc[0].get("close")),
                    "J": signal.get("J", frame.iloc[0].get("J")),
                    "reasons": json.dumps(reasons, ensure_ascii=False),
                    "validation_status": policy.validation_status,
                }
            )
        except Exception as exc:
            print(f"[WARN] ETF candidate scan failed for {asset.code}: {type(exc).__name__}: {exc}")
    return candidates


def write_candidates(candidates: list[dict[str, Any]], output: str | Path) -> Path:
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(candidates, columns=OUTPUT_COLUMNS)
    if "code" in frame.columns and not frame.empty:
        frame["code"] = frame["code"].astype(str).str.zfill(6)
    frame.to_csv(path, index=False, encoding="gbk")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--output", default="artifacts/daily/etf/signals_today.csv")
    parser.add_argument("--max-etfs", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    candidates = scan_etf_candidates(
        data_dir=args.data_dir,
        signal_date=args.date,
        max_etfs=args.max_etfs,
    )
    output = write_candidates(candidates, args.output)
    print(f"ETF research-only candidates: {len(candidates)} -> {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
