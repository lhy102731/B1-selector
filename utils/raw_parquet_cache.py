"""Raw OHLCV parquet cache for stock CSV data.

The CSV files remain the source of truth. This cache stores a cleaned,
date-ascending parquet copy so indicator precompute can avoid repeatedly
parsing GBK CSV files.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.csv_manager import CSVManager


REQUIRED_COLUMNS = ("date", "open", "high", "low", "close", "volume")
NUMERIC_COLUMNS = (
    "open", "high", "low", "close", "close_raw", "volume", "amount", "turnover",
    "turnover_rate", "pct_chg", "change", "amplitude",
)


def normalize_raw_stock_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw stock bars for cache storage and indicator calculation."""
    if df is None or df.empty:
        return pd.DataFrame()
    result = df.copy()
    missing = [column for column in REQUIRED_COLUMNS if column not in result.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")

    result["date"] = pd.to_datetime(result["date"], errors="coerce")
    result = result.dropna(subset=["date"])
    for column in NUMERIC_COLUMNS:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    result = result.dropna(subset=["open", "high", "low", "close", "volume"])
    result = result[result["volume"] > 0]
    result = result.sort_values("date").reset_index(drop=True)
    return result


class RawParquetCache:
    """Maintains data/raw_parquet/{prefix}/{code}.parquet files."""

    def __init__(self, data_dir: str | Path = "data", cache_name: str = "raw_parquet"):
        self.data_dir = Path(data_dir)
        self.cache_dir = self.data_dir / cache_name
        self.csv_manager = CSVManager(self.data_dir)

    def csv_path(self, code: str) -> Path:
        return self.data_dir / code[:2] / f"{code}.csv"

    def parquet_path(self, code: str) -> Path:
        return self.cache_dir / code[:2] / f"{code}.parquet"

    def is_current(self, code: str) -> bool:
        csv_path = self.csv_path(code)
        parquet_path = self.parquet_path(code)
        return (
            csv_path.exists()
            and parquet_path.exists()
            and parquet_path.stat().st_mtime >= csv_path.stat().st_mtime
        )

    def read_stock(self, code: str, refresh: bool = False) -> pd.DataFrame:
        """Read a stock from raw parquet, rebuilding from CSV when stale."""
        parquet_path = self.parquet_path(code)
        if not refresh and self.is_current(code):
            return pd.read_parquet(parquet_path)
        return self.build_stock(code)

    def build_stock(self, code: str) -> pd.DataFrame:
        """Build one raw parquet cache file and return the normalized frame."""
        csv_path = self.csv_path(code)
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return pd.DataFrame()
        raw = self.csv_manager.read_stock(code)
        normalized = normalize_raw_stock_frame(raw)
        if normalized.empty:
            return normalized
        parquet_path = self.parquet_path(code)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        normalized.to_parquet(parquet_path, index=False)
        return normalized
