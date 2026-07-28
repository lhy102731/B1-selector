"""Typed selection universe spanning A-shares and independently stored ETFs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from utils.csv_manager import CSVManager
from utils.market_asset_store import MarketAssetStore


@dataclass(frozen=True)
class AssetRef:
    code: str
    asset_type: str
    path: Path
    name: str = ""
    ths_code: str = ""
    exchange: str = ""
    t0: bool = False


class SelectionUniverse:
    """Expose eligible local assets without conflating their storage schemas."""

    def __init__(self, data_dir: str | Path = "data") -> None:
        self.data_dir = Path(data_dir)
        self.stocks = CSVManager(self.data_dir)
        self.market_assets = MarketAssetStore(self.data_dir)

    @staticmethod
    def _exchange_from_ths_code(ths_code: str) -> str:
        market = str(ths_code).upper()[:4]
        if market in {"USHA", "USHJ", "USHI", "USHT"}:
            return "sh"
        if market in {"USZA", "USZJ", "USZI", "USZT"}:
            return "sz"
        return ""

    def list_assets(self, *, include_etfs: bool = True) -> list[AssetRef]:
        assets = [
            AssetRef(
                code=code,
                asset_type="stock",
                path=self.stocks.get_stock_path(code),
                exchange="sh" if code.startswith(("60", "68")) else "sz",
            )
            for code in self.stocks.list_all_stocks()
        ]
        if include_etfs:
            for code, metadata in sorted(self.market_assets.read_catalog("etf").items()):
                if not bool(metadata.get("selection_eligible", False)):
                    continue
                path = self.market_assets.history_path("etf", code)
                if not path.is_file() or path.stat().st_size == 0:
                    continue
                ths_code = str(metadata.get("ths_code", ""))
                exchange = self._exchange_from_ths_code(ths_code)
                if not exchange:
                    continue
                assets.append(
                    AssetRef(
                        code=code,
                        asset_type="etf",
                        path=path,
                        name=str(metadata.get("name", "")),
                        ths_code=ths_code,
                        exchange=exchange,
                        t0=bool(metadata.get("t0", False)),
                    )
                )
        return assets


__all__ = ["AssetRef", "SelectionUniverse"]
