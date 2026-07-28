from __future__ import annotations

import unittest

from utils.asset_policy import AssetPolicy


class AssetPolicyTests(unittest.TestCase):
    def test_etf_policy_disables_company_filters_and_uses_traded_amount(self):
        stock = AssetPolicy.for_asset_type("stock")
        etf = AssetPolicy.for_asset_type("etf")

        self.assertTrue(stock.apply_market_cap)
        self.assertTrue(stock.apply_valuation)
        self.assertTrue(stock.apply_stock_name_filter)
        self.assertEqual("market_cap", stock.liquidity_field)

        self.assertFalse(etf.apply_market_cap)
        self.assertFalse(etf.apply_valuation)
        self.assertFalse(etf.apply_stock_name_filter)
        self.assertFalse(etf.apply_stock_price_limits)
        self.assertEqual("amount", etf.liquidity_field)
        self.assertEqual("research_only", etf.validation_status)


if __name__ == "__main__":
    unittest.main()
