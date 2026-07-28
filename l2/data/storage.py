"""L2 tick data storage using Parquet with Hive-style partitioning.

Directory structure:
  data/l2/
    transactions/         # tick-by-tick data
      year=YYYY/month=MM/day=DD/stock=XXXXXX/part-0.parquet
    orderbook/            # order book snapshots
      year=YYYY/month=MM/day=DD/stock=XXXXXX.parquet
    features/             # daily feature cache
      year=YYYY/month=MM/day=DD/stock=XXXXXX.parquet
    daily_summary/        # daily aggregated stats
      year=YYYY/month=MM/stock=XXXXXX.parquet
"""

import shutil
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from l2.data.config import L2Config


TRANSACTION_SCHEMA = pa.schema([
    ("time", pa.timestamp("ms")),
    ("price", pa.float32()),
    ("volume", pa.int32()),
    ("amount", pa.float64()),
    ("direction", pa.int8()),
    ("order_type", pa.int8()),
    ("seq", pa.int64()),
    ("stock_code", pa.string()),
])

ORDERBOOK_SCHEMA = pa.schema([
    ("time", pa.timestamp("ms")),
    ("bid_price_01", pa.float32()), ("bid_volume_01", pa.int32()),
    ("bid_price_02", pa.float32()), ("bid_volume_02", pa.int32()),
    ("bid_price_03", pa.float32()), ("bid_volume_03", pa.int32()),
    ("bid_price_04", pa.float32()), ("bid_volume_04", pa.int32()),
    ("bid_price_05", pa.float32()), ("bid_volume_05", pa.int32()),
    ("bid_price_06", pa.float32()), ("bid_volume_06", pa.int32()),
    ("bid_price_07", pa.float32()), ("bid_volume_07", pa.int32()),
    ("bid_price_08", pa.float32()), ("bid_volume_08", pa.int32()),
    ("bid_price_09", pa.float32()), ("bid_volume_09", pa.int32()),
    ("bid_price_10", pa.float32()), ("bid_volume_10", pa.int32()),
    ("ask_price_01", pa.float32()), ("ask_volume_01", pa.int32()),
    ("ask_price_02", pa.float32()), ("ask_volume_02", pa.int32()),
    ("ask_price_03", pa.float32()), ("ask_volume_03", pa.int32()),
    ("ask_price_04", pa.float32()), ("ask_volume_04", pa.int32()),
    ("ask_price_05", pa.float32()), ("ask_volume_05", pa.int32()),
    ("ask_price_06", pa.float32()), ("ask_volume_06", pa.int32()),
    ("ask_price_07", pa.float32()), ("ask_volume_07", pa.int32()),
    ("ask_price_08", pa.float32()), ("ask_volume_08", pa.int32()),
    ("ask_price_09", pa.float32()), ("ask_volume_09", pa.int32()),
    ("ask_price_10", pa.float32()), ("ask_volume_10", pa.int32()),
    ("spread", pa.float32()),
    ("stock_code", pa.string()),
])


class TickStorage:
    """L2 tick data storage with Parquet partitioning."""

    def __init__(self, config: L2Config | None = None, base_dir: str | None = None):
        self.config = config or L2Config()
        self.base_dir = Path(base_dir or self.config.STORAGE_BASE)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    # ---- Path builders ----

    def _date_parts(self, date_str: str) -> tuple[str, str, str]:
        """Split 'YYYY-MM-DD' or 'YYYYMMDD' into (year, month, day)."""
        s = date_str.replace("-", "")
        return s[:4], s[4:6], s[6:8]

    def _transactions_dir(self, date_str: str, stock_code: str) -> Path:
        y, m, d = self._date_parts(date_str)
        return self.base_dir / "transactions" / f"year={y}" / f"month={m}" / f"day={d}" / f"stock={stock_code}"

    def _orderbook_dir(self, date_str: str) -> Path:
        y, m, d = self._date_parts(date_str)
        return self.base_dir / "orderbook" / f"year={y}" / f"month={m}" / f"day={d}"

    def _features_dir(self, date_str: str) -> Path:
        y, m, d = self._date_parts(date_str)
        return self.base_dir / "features" / f"year={y}" / f"month={m}" / f"day={d}"

    def _daily_summary_dir(self, date_str: str) -> Path:
        y, m, d = self._date_parts(date_str)
        return self.base_dir / "daily_summary" / f"year={y}" / f"month={m}"

    # ---- Save ----

    def save_transactions(self, stock_code: str, date_str: str, df: pd.DataFrame) -> Path:
        """Save tick-by-tick data as partitioned Parquet."""
        if df.empty:
            return None
        part_dir = self._transactions_dir(date_str, stock_code)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / f"{stock_code}_{date_str}.parquet"
        # Ensure stock_code column
        if "stock_code" not in df.columns:
            df["stock_code"] = stock_code
        df.to_parquet(file_path, index=False, compression=self.config.PARQUET_COMPRESSION)
        return file_path

    def save_orderbook_snapshot(self, stock_code: str, date_str: str, df: pd.DataFrame) -> Path:
        """Save order book depth snapshot."""
        if df.empty:
            return None
        part_dir = self._orderbook_dir(date_str)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / f"{stock_code}.parquet"
        if "stock_code" not in df.columns:
            df["stock_code"] = stock_code
        # Append if file exists (multiple snapshots per day)
        if file_path.exists():
            existing = pd.read_parquet(file_path)
            df = pd.concat([existing, df], ignore_index=True)
        df.to_parquet(file_path, index=False, compression=self.config.PARQUET_COMPRESSION)
        return file_path

    def save_features(self, stock_code: str, date_str: str, features: dict) -> Path:
        """Save daily features dict as single-row Parquet."""
        part_dir = self._features_dir(date_str)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / f"{stock_code}.parquet"
        df = pd.DataFrame([features])
        df.to_parquet(file_path, index=False, compression=self.config.PARQUET_COMPRESSION)
        return file_path

    def save_daily_summary(self, stock_code: str, date_str: str, summary: dict) -> Path:
        """Save daily aggregated L2 summary."""
        part_dir = self._daily_summary_dir(date_str)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / f"{stock_code}.parquet"
        df = pd.DataFrame([summary])
        df.to_parquet(file_path, index=False, compression=self.config.PARQUET_COMPRESSION)
        return file_path

    # ---- Load ----

    def load_transactions(self, stock_code: str, date_str: str) -> pd.DataFrame:
        """Load tick data for a stock on a specific date."""
        part_dir = self._transactions_dir(date_str, stock_code)
        if not part_dir.exists():
            return pd.DataFrame()
        files = list(part_dir.glob("*.parquet"))
        if not files:
            return pd.DataFrame()
        return pd.read_parquet(files[0])

    def load_transactions_range(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Load tick data for a date range using PyArrow dataset API."""
        base = self.base_dir / "transactions"
        if not base.exists():
            return pd.DataFrame()
        try:
            dataset = ds.dataset(str(base), format="parquet", partitioning="hive")
            y1, m1, d1 = self._date_parts(start_date)
            y2, m2, d2 = self._date_parts(end_date)
            expr = (
                (ds.field("stock") == stock_code)
                & (ds.field("year") >= y1)
                & (ds.field("year") <= y2)
            )
            table = dataset.to_table(filter=expr)
            return table.to_pandas() if table.num_rows > 0 else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def load_orderbook(self, stock_code: str, date_str: str) -> pd.DataFrame:
        """Load order book snapshots for a stock-date."""
        file_path = self._orderbook_dir(date_str) / f"{stock_code}.parquet"
        if not file_path.exists():
            return pd.DataFrame()
        return pd.read_parquet(file_path)

    def load_features(self, stock_code: str, date_str: str) -> dict | None:
        """Load features for a single stock-date."""
        file_path = self._features_dir(date_str) / f"{stock_code}.parquet"
        if not file_path.exists():
            return None
        df = pd.read_parquet(file_path)
        return df.iloc[0].to_dict() if len(df) > 0 else None

    def load_features_range(self, stock_code: str, start_date: str, end_date: str) -> list[dict]:
        """Load feature history for a stock over a date range."""
        results = []
        base = self.base_dir / "features"
        if not base.exists():
            return results
        y1, m1, d1 = self._date_parts(start_date)
        y2, m2, d2 = self._date_parts(end_date)
        for part_dir in sorted(base.rglob(f"stock={stock_code}")):
            parts = part_dir.parts
            try:
                year_idx = parts.index([p for p in parts if p.startswith("year=")][0])
                month_idx = parts.index([p for p in parts if p.startswith("month=")][0])
                day_idx = parts.index([p for p in parts if p.startswith("day=")][0])
                y, m, d = parts[year_idx][5:], parts[month_idx][6:], parts[day_idx][4:]
                date_key = f"{y}{m}{d}"
                if f"{y1}{m1}{d1}" <= date_key <= f"{y2}{m2}{d2}":
                    for f in part_dir.glob("*.parquet"):
                        df = pd.read_parquet(f)
                        for _, row in df.iterrows():
                            results.append(row.to_dict())
            except (ValueError, IndexError):
                continue
        return results

    def load_daily_summary(self, stock_code: str, date_str: str) -> dict | None:
        """Load daily summary for a stock-date."""
        y, m, _ = self._date_parts(date_str)
        file_path = self.base_dir / "daily_summary" / f"year={y}" / f"month={m}" / f"{stock_code}.parquet"
        if not file_path.exists():
            return None
        df = pd.read_parquet(file_path)
        return df.iloc[0].to_dict() if len(df) > 0 else None

    # ---- Query helpers ----

    def get_available_dates(self, stock_code: str) -> list[str]:
        """Get list of dates with tick data for a stock."""
        dates = []
        base = self.base_dir / "transactions"
        if not base.exists():
            return dates
        for stock_dir in sorted(base.rglob(f"stock={stock_code}")):
            # stock_dir is like .../day=YYYYMMDD/stock=XXXXXX
            day_dir = stock_dir.parent
            day_part = day_dir.name  # e.g., "day=20260522"
            if day_part.startswith("day="):
                d = day_part[4:]
                dates.append(f"{d[:4]}-{d[4:6]}-{d[6:8]}")
        return dates

    def get_available_stocks(self, date_str: str) -> list[str]:
        """Get stocks that have tick data on a given date."""
        y, m, d = self._date_parts(date_str)
        day_dir = self.base_dir / "transactions" / f"year={y}" / f"month={m}" / f"day={d}"
        if not day_dir.exists():
            return []
        stocks = []
        for stock_dir in day_dir.iterdir():
            if stock_dir.name.startswith("stock="):
                stocks.append(stock_dir.name[6:])
        return stocks

    # ---- Maintenance ----

    def cleanup_raw_ticks(self, retention_days: int | None = None):
        """Remove raw tick data older than retention_days."""
        days = retention_days or self.config.RAW_TICK_RETENTION_DAYS
        cutoff = datetime.now() - timedelta(days=days)
        base = self.base_dir / "transactions"
        if not base.exists():
            return
        for day_dir in base.rglob("day=*"):
            day_str = day_dir.name[4:]  # day=20260522
            try:
                date = datetime.strptime(day_str, "%Y%m%d")
                if date < cutoff:
                    shutil.rmtree(day_dir, ignore_errors=True)
            except ValueError:
                continue

    def cleanup_old_features(self, retention_days: int | None = None):
        """Remove feature cache older than retention_days."""
        days = retention_days or self.config.FEATURE_RETENTION_DAYS
        cutoff = datetime.now() - timedelta(days=days)
        base = self.base_dir / "features"
        if not base.exists():
            return
        for day_dir in base.rglob("day=*"):
            day_str = day_dir.name[4:]
            try:
                date = datetime.strptime(day_str, "%Y%m%d")
                if date < cutoff:
                    shutil.rmtree(day_dir, ignore_errors=True)
            except ValueError:
                continue

    def get_storage_stats(self) -> dict:
        """Get storage statistics."""
        stats = {"transactions": {}, "orderbook": {}, "features": {}, "daily_summary": {}}
        for dtype in ["transactions", "orderbook", "features", "daily_summary"]:
            base = self.base_dir / dtype
            if base.exists():
                total_size = sum(f.stat().st_size for f in base.rglob("*.parquet"))
                file_count = sum(1 for _ in base.rglob("*.parquet"))
                stats[dtype] = {
                    "size_mb": round(total_size / (1024 * 1024), 2),
                    "file_count": file_count,
                }
        return stats
