"""Storage contract for native THS indices and exchange-traded funds."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


class MarketAssetStore:
    """Keep non-stock assets isolated from the A-share CSV tree."""

    ASSET_TYPES = frozenset({"etf", "industry", "concept", "index"})
    REQUIRED_BAR_COLUMNS = frozenset(
        {"date", "open", "high", "low", "close", "close_raw", "volume", "amount"}
    )

    def __init__(self, data_dir: str | Path = "data") -> None:
        self.data_dir = Path(data_dir)

    @classmethod
    def _asset_type(cls, value: str) -> str:
        asset_type = str(value).strip().lower()
        if asset_type not in cls.ASSET_TYPES:
            raise ValueError(f"unsupported asset type: {value!r}")
        return asset_type

    def asset_root(self, asset_type: str) -> Path:
        asset_type = self._asset_type(asset_type)
        if asset_type == "etf":
            return self.data_dir / "etf"
        return self.data_dir / "indices" / asset_type

    def catalog_path(self, asset_type: str) -> Path:
        return self.asset_root(asset_type) / "metadata.json"

    def history_path(self, asset_type: str, code: str) -> Path:
        asset_type = self._asset_type(asset_type)
        code = str(code).strip()
        if len(code) != 6 or not code.isdigit():
            raise ValueError("asset code must contain exactly six digits")
        root = self.asset_root(asset_type)
        return (root / code[:2] / f"{code}.csv") if asset_type == "etf" else (root / f"{code}.csv")

    @staticmethod
    def _atomic_json(payload: Any, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_name, path)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass
            raise

    @staticmethod
    def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        os.close(handle)
        try:
            frame.to_csv(temporary_name, index=False, encoding="gbk")
            os.replace(temporary_name, path)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass
            raise

    def write_catalog(self, asset_type: str, catalog: dict[str, dict[str, Any]]) -> Path:
        asset_type = self._asset_type(asset_type)
        normalized: dict[str, dict[str, Any]] = {}
        for raw_code, raw_metadata in catalog.items():
            code = str(raw_code).strip()
            if len(code) != 6 or not code.isdigit() or not isinstance(raw_metadata, dict):
                raise ValueError(f"invalid {asset_type} catalog entry: {raw_code!r}")
            metadata = dict(raw_metadata)
            metadata["code"] = code
            metadata["asset_type"] = asset_type
            normalized[code] = metadata
        path = self.catalog_path(asset_type)
        self._atomic_json(normalized, path)
        return path

    def read_catalog(self, asset_type: str) -> dict[str, dict[str, Any]]:
        path = self.catalog_path(asset_type)
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"invalid catalog payload: {path}")
        return payload

    def read_history(self, asset_type: str, code: str) -> pd.DataFrame:
        path = self.history_path(asset_type, code)
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame()
        for encoding in ("gbk", "utf-8"):
            try:
                frame = pd.read_csv(path, encoding=encoding)
                if "date" in frame.columns:
                    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
                return frame
            except UnicodeDecodeError:
                continue
        raise UnicodeError(f"history is neither GBK nor UTF-8: {path}")

    def write_history(self, asset_type: str, code: str, frame: pd.DataFrame) -> Path:
        asset_type = self._asset_type(asset_type)
        required = set(self.REQUIRED_BAR_COLUMNS)
        if asset_type == "etf":
            required.update({"open_raw", "high_raw", "low_raw"})
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(f"history is missing required columns: {sorted(missing)}")
        result = frame.copy()
        result["date"] = pd.to_datetime(result["date"], errors="coerce")
        if result["date"].isna().any() or result["date"].duplicated().any():
            raise ValueError("history contains invalid or duplicate dates")
        numeric_columns = ["open", "high", "low", "close", "close_raw", "volume", "amount"]
        numeric_columns.extend(
            column for column in ("open_raw", "high_raw", "low_raw") if column in result.columns
        )
        for column in numeric_columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
        price_columns = ["open", "high", "low", "close", "close_raw"]
        price_columns.extend(
            column for column in ("open_raw", "high_raw", "low_raw") if column in result.columns
        )
        prices = result[price_columns].to_numpy(dtype=float)
        if not np.isfinite(prices).all() or (prices <= 0).any():
            raise ValueError("history contains invalid prices")
        if (result["high"] < result[["open", "close", "low"]].max(axis=1)).any():
            raise ValueError("history contains a high-price envelope violation")
        if (result["low"] > result[["open", "close", "high"]].min(axis=1)).any():
            raise ValueError("history contains a low-price envelope violation")
        if {"open_raw", "high_raw", "low_raw"}.issubset(result.columns):
            if (result["high_raw"] < result[["open_raw", "close_raw", "low_raw"]].max(axis=1)).any():
                raise ValueError("history contains a raw high-price envelope violation")
            if (result["low_raw"] > result[["open_raw", "close_raw", "high_raw"]].min(axis=1)).any():
                raise ValueError("history contains a raw low-price envelope violation")

        sentinels = {2147483647.0, 2147483648.0, 4294967295.0}
        for column in ("volume", "amount"):
            finite_values = result[column].dropna().astype(float)
            if (finite_values < 0).any() or (~np.isfinite(finite_values)).any():
                raise ValueError(f"history contains invalid {column}")
            if finite_values.isin(sentinels).any():
                raise ValueError(f"history contains a {column} sentinel")

        if asset_type == "etf":
            if result["volume"].isna().any():
                raise ValueError("ETF history requires finite volume")
            positive_volume = result["volume"] > 0
            known_amount = result["amount"].notna()
            if (result.loc[positive_volume & known_amount, "amount"] <= 0).any():
                raise ValueError("ETF positive-volume bars require a positive amount")
            if (result.loc[~positive_volume & known_amount, "amount"] != 0).any():
                raise ValueError("ETF zero-volume bars require a zero amount")
            traded = result.loc[positive_volume & known_amount]
            if not traded.empty:
                # THS archives amount as whole yuan.  On one- or two-share
                # historical ETF prints, dividing the rounded yuan amount by
                # volume can sit visibly outside a mill-price OHLC range even
                # though the aggregate amount is correct.  Validate in amount
                # space with a one-yuan rounding allowance instead.
                minimum_amount = traded["low_raw"] * traded["volume"]
                maximum_amount = traded["high_raw"] * traded["volume"]
                rounding_allowance = np.maximum(1.0, traded["volume"] * 0.001)
                outside = (traded["amount"] < minimum_amount - rounding_allowance) | (
                    traded["amount"] > maximum_amount + rounding_allowance
                )
                if outside.any():
                    raise ValueError("ETF VWAP is outside the raw low/high range")
        result["asset_type"] = asset_type
        result["date"] = result["date"].dt.strftime("%Y-%m-%d")
        result = result.sort_values("date", ascending=False).reset_index(drop=True)
        path = self.history_path(asset_type, code)
        self._atomic_csv(result, path)
        return path


__all__ = ["MarketAssetStore"]
