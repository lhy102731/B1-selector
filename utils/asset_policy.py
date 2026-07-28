"""Central rules that prevent company-specific filters leaking into ETFs."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AssetPolicy:
    asset_type: str
    apply_market_cap: bool
    apply_valuation: bool
    apply_stock_name_filter: bool
    apply_stock_price_limits: bool
    apply_company_actions: bool
    liquidity_field: str
    validation_status: str

    @classmethod
    def for_asset_type(cls, asset_type: str) -> "AssetPolicy":
        value = str(asset_type).strip().lower()
        if value == "stock":
            return cls(
                asset_type="stock",
                apply_market_cap=True,
                apply_valuation=True,
                apply_stock_name_filter=True,
                apply_stock_price_limits=True,
                apply_company_actions=True,
                liquidity_field="market_cap",
                validation_status="production",
            )
        if value == "etf":
            return cls(
                asset_type="etf",
                apply_market_cap=False,
                apply_valuation=False,
                apply_stock_name_filter=False,
                apply_stock_price_limits=False,
                apply_company_actions=False,
                liquidity_field="amount",
                validation_status="research_only",
            )
        raise ValueError(f"unsupported selection asset type: {asset_type!r}")


__all__ = ["AssetPolicy"]
