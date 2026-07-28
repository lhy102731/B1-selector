"""L2 analysis feature cache using Parquet.

Caches computed tick-level and order-book-level features for fast retrieval.
Follows the same pattern as build_indicators_cache.py in the existing codebase.

Storage: data/l2/features/year=YYYY/month=MM/day=DD/stock=XXXXXX.parquet
"""

from pathlib import Path
from datetime import datetime, timedelta
import logging

import pandas as pd
import pyarrow.dataset as ds

from l2.data.config import L2Config

logger = logging.getLogger(__name__)


class FeatureCache:
    """Cache for L2 analysis features.

    Saves daily feature dicts to partitioned Parquet for efficient
    historical lookups needed by SignalEngine (trend context).
    """

    def __init__(self, config: L2Config | None = None, base_dir: str | None = None):
        self.config = config or L2Config()
        self.base_dir = Path(base_dir or self.config.STORAGE_BASE) / "features"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _feature_path(self, stock_code: str, date_str: str) -> Path:
        """Build path for a feature file."""
        date_str = date_str.replace("-", "")
        y, m, d = date_str[:4], date_str[4:6], date_str[6:8]
        part_dir = self.base_dir / f"year={y}" / f"month={m}" / f"day={d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        return part_dir / f"{stock_code}.parquet"

    def save(self, stock_code: str, date_str: str, features: dict) -> Path:
        """Save features dict as a single-row Parquet file."""
        file_path = self._feature_path(stock_code, date_str)
        df = pd.DataFrame([features])
        df.to_parquet(file_path, index=False, compression=self.config.PARQUET_COMPRESSION)
        return file_path

    def load(self, stock_code: str, date_str: str) -> dict | None:
        """Load features for a single stock-date."""
        file_path = self._feature_path(stock_code, date_str)
        if not file_path.exists():
            return None
        df = pd.read_parquet(file_path)
        return df.iloc[0].to_dict() if len(df) > 0 else None

    def get_history(self, stock_code: str, days: int = 5) -> list[dict]:
        """Get most recent N trading days of features for a stock.

        Returns list of feature dicts sorted by date ascending.
        """
        results = []
        if not self.base_dir.exists():
            return results

        # Find all feature files for this stock
        pattern = f"*/year=*/month=*/day=*/{stock_code}.parquet"
        files = sorted(self.base_dir.glob(pattern))

        # Take last N files
        for f in files[-days:]:
            try:
                df = pd.read_parquet(f)
                for _, row in df.iterrows():
                    results.append(row.to_dict())
            except Exception as e:
                logger.warning(f"Error reading {f}: {e}")
        return results

    def get_history_range(self, stock_code: str, start_date: str, end_date: str) -> list[dict]:
        """Get features for a date range."""
        results = []
        if not self.base_dir.exists():
            return results

        s = start_date.replace("-", "")
        e = end_date.replace("-", "")

        for f in sorted(self.base_dir.glob(f"*/year=*/month=*/day=*/{stock_code}.parquet")):
            try:
                # Extract date from path: .../year=YYYY/month=MM/day=DD/stock.parquet
                parts = f.parts
                year_part = [p for p in parts if p.startswith("year=")][0]
                month_part = [p for p in parts if p.startswith("month=")][0]
                day_part = [p for p in parts if p.startswith("day=")][0]
                date_key = year_part[5:] + month_part[6:] + day_part[4:]
                if s <= date_key <= e:
                    df = pd.read_parquet(f)
                    for _, row in df.iterrows():
                        results.append(row.to_dict())
            except (ValueError, IndexError, Exception):
                continue
        return results

    def get_available_dates(self, stock_code: str) -> list[str]:
        """List dates with cached features for a stock."""
        dates = []
        for f in sorted(self.base_dir.glob(f"*/year=*/month=*/day=*/{stock_code}.parquet")):
            try:
                parts = f.parts
                y = [p for p in parts if p.startswith("year=")][0][5:]
                m = [p for p in parts if p.startswith("month=")][0][6:]
                d = [p for p in parts if p.startswith("day=")][0][4:]
                dates.append(f"{y}-{m}-{d}")
            except (ValueError, IndexError):
                continue
        return dates

    def has_data(self, stock_code: str, date_str: str) -> bool:
        """Check if features are cached for a stock-date."""
        return self._feature_path(stock_code, date_str).exists()

    def cleanup(self, retention_days: int | None = None):
        """Remove feature files older than retention_days."""
        days = retention_days or self.config.FEATURE_RETENTION_DAYS
        cutoff = datetime.now() - timedelta(days=days)
        for day_dir in self.base_dir.rglob("day=*"):
            day_str = day_dir.name[4:]
            try:
                date = datetime.strptime(day_str, "%Y%m%d")
                if date < cutoff:
                    import shutil
                    shutil.rmtree(day_dir, ignore_errors=True)
            except ValueError:
                continue

    def get_stats(self) -> dict:
        """Get cache statistics."""
        if not self.base_dir.exists():
            return {"total_files": 0, "size_mb": 0, "stock_count": 0}

        files = list(self.base_dir.glob("*/year=*/month=*/day=*/*.parquet"))
        total_size = sum(f.stat().st_size for f in files)
        stocks = set(f.stem for f in files)

        return {
            "total_files": len(files),
            "size_mb": round(total_size / (1024 * 1024), 2),
            "stock_count": len(stocks),
        }
