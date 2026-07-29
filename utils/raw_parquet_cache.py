"""Raw OHLCV parquet cache for stock CSV data.

The CSV files remain the source of truth. This cache stores a cleaned,
date-ascending parquet copy so indicator precompute can avoid repeatedly
parsing GBK CSV files.
"""
from __future__ import annotations

import hashlib
import io
from pathlib import Path
from typing import Literal

import pandas as pd

from research_automation.data_generation.cache_identity import (
    build_cache_identity,
    load_verified_cache_identity,
    write_cache_identity_sidecar,
)
from research_automation.data_generation.generation import GenerationPin
from utils.csv_manager import CSVManager


REQUIRED_COLUMNS = ("date", "open", "high", "low", "close", "volume")
NUMERIC_COLUMNS = (
    "open", "high", "low", "close", "close_raw", "volume", "amount", "turnover",
    "turnover_rate", "pct_chg", "change", "amplitude",
)
RAW_PARQUET_FEATURE_CONTRACT_ID = hashlib.sha256(
    b"a-share.raw-parquet.normalized-ascending.v1"
).hexdigest()
PINNED_RAW_CACHE_NAMES = {
    "production": "raw_parquet",
    "research": "research_raw_parquet",
}


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


def read_stock_csv_bytes(content: bytes) -> pd.DataFrame:
    """Parse exact pinned CSV bytes without reopening the mutable path."""
    if not isinstance(content, bytes) or not content:
        raise ValueError("stock CSV content must be non-empty bytes")
    for encoding in ("gbk", "utf-8"):
        try:
            return pd.read_csv(
                io.BytesIO(content),
                parse_dates=["date"],
                encoding=encoding,
            )
        except UnicodeDecodeError:
            continue
        except Exception as error:
            raise ValueError("unable to parse pinned stock CSV") from error
    raise ValueError("pinned stock CSV is neither GBK nor UTF-8")


class RawParquetCache:
    """Maintains data/raw_parquet/{prefix}/{code}.parquet files."""

    def __init__(
        self,
        data_dir: str | Path = "data",
        cache_name: str = "raw_parquet",
        *,
        generation_pin: GenerationPin | None = None,
        cache_namespace: Literal["production", "research"] = "production",
    ):
        self.data_dir = Path(data_dir)
        self.cache_dir = self.data_dir / cache_name
        self.csv_manager = CSVManager(self.data_dir)
        self.generation_pin = generation_pin
        if cache_namespace not in {"production", "research"}:
            raise ValueError("unsupported cache namespace")
        self.cache_namespace = cache_namespace
        if (
            generation_pin is not None
            and self.data_dir.resolve(strict=True) != generation_pin.data_root
        ):
            raise ValueError("raw cache data_dir must equal the generation pin root")
        if (
            generation_pin is not None
            and cache_name != PINNED_RAW_CACHE_NAMES[cache_namespace]
        ):
            raise ValueError("pinned cache name does not match its namespace")

    def csv_path(self, code: str) -> Path:
        return self.data_dir / code[:2] / f"{code}.csv"

    def parquet_path(self, code: str) -> Path:
        return self.cache_dir / code[:2] / f"{code}.parquet"

    def is_current(self, code: str) -> bool:
        csv_path = self.csv_path(code)
        parquet_path = self.parquet_path(code)
        if self.generation_pin is not None:
            if not csv_path.exists() or not parquet_path.exists():
                return False
            self._load_pinned_identity(code)
            return True
        return (
            csv_path.exists()
            and parquet_path.exists()
            and parquet_path.stat().st_mtime >= csv_path.stat().st_mtime
        )

    def read_stock(self, code: str, refresh: bool = False) -> pd.DataFrame:
        """Read a stock from raw parquet, rebuilding from CSV when stale."""
        parquet_path = self.parquet_path(code)
        if self.generation_pin is not None:
            if not refresh and parquet_path.exists():
                identity = self._load_pinned_identity(code)
                content = self.generation_pin.read_verified_bytes(
                    parquet_path.relative_to(self.data_dir).as_posix(),
                    identity.artifact,
                )
                frame = pd.read_parquet(io.BytesIO(content))
                self._verify_pinned_source(code)
                return frame
            return self.build_stock(code)
        if not refresh and self.is_current(code):
            return pd.read_parquet(parquet_path)
        return self.build_stock(code)

    def _load_pinned_identity(self, code: str):
        if self.generation_pin is None:
            raise RuntimeError("pinned cache identity requires a generation pin")
        source = self._verify_pinned_source(code)
        return load_verified_cache_identity(
            self.generation_pin,
            **self._pinned_identity_arguments(code, source.artifact_id),
        )

    def verified_identity(self, code: str):
        """Return a strictly verified identity for a pinned raw cache."""
        return self._load_pinned_identity(code)

    def _verify_pinned_source(self, code: str):
        if self.generation_pin is None:
            raise RuntimeError("pinned source identity requires a generation pin")
        return self.generation_pin.verify_artifact(
            self.csv_path(code).relative_to(self.data_dir).as_posix(),
            content_schema="a-share.gbk_csv.v1",
            kind="source_csv",
            logical_role="raw_stock_bars",
        )

    def _pinned_identity_arguments(
        self,
        code: str,
        source_artifact_id: str,
    ) -> dict[str, object]:
        return {
            "relative_path": self.parquet_path(code).relative_to(
                self.data_dir
            ).as_posix(),
            "cache_namespace": self.cache_namespace,
            "cache_kind": "raw_parquet",
            "source_artifact_ids": (source_artifact_id,),
            "feature_contract_id": RAW_PARQUET_FEATURE_CONTRACT_ID,
            "content_schema": "parquet.normalized_stock_bars.v1",
            "producer": "utils.raw_parquet_cache.RawParquetCache",
            "logical_role": "ascending_raw_bars",
        }

    def build_stock(self, code: str) -> pd.DataFrame:
        """Build one raw parquet cache file and return the normalized frame."""
        csv_path = self.csv_path(code)
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return pd.DataFrame()
        source_identity = (
            self._verify_pinned_source(code)
            if self.generation_pin is not None
            else None
        )
        if self.generation_pin is not None and source_identity is not None:
            raw = read_stock_csv_bytes(
                self.generation_pin.read_verified_bytes(
                    csv_path.relative_to(self.data_dir).as_posix(),
                    source_identity,
                )
            )
        else:
            raw = self.csv_manager.read_stock(code)
        normalized = normalize_raw_stock_frame(raw)
        if normalized.empty:
            return normalized
        parquet_path = self.parquet_path(code)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        normalized.to_parquet(parquet_path, index=False)
        if self.generation_pin is not None and source_identity is not None:
            identity = build_cache_identity(
                self.generation_pin,
                **self._pinned_identity_arguments(
                    code,
                    source_identity.artifact_id,
                ),
            )
            write_cache_identity_sidecar(
                self.generation_pin,
                relative_path=self.parquet_path(code).relative_to(
                    self.data_dir
                ).as_posix(),
                identity=identity,
            )
        return normalized
