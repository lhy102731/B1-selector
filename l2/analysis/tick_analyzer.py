"""Tick-by-tick transaction analyzer for L2 data.

Core feature extraction from raw tick data:
  1. Big/huge order detection (dynamic percentile threshold)
  2. Active buy/sell direction classification
  3. Institutional vs retail trader classification
  4. Order flow imbalance (OFI)
  5. Tick intensity and clustering analysis
  6. Session phase breakdown
  7. Full feature vector computation
"""

import numpy as np
import pandas as pd

from l2.data.config import L2Config


class TickAnalyzer:
    """逐笔成交分析器 - extracts features from tick-by-tick data."""

    # Order size classification by amount (yuan)
    ORDER_CLASS_THRESHOLDS = {
        "retail": 20_000,       # < 2万
        "small": 100_000,       # 2万-10万
        "medium": 500_000,      # 10万-50万
        "large": 1_000_000,     # 50万-100万
        # >= 1,000,000 = huge (特大单)
    }

    def __init__(self, config: L2Config | None = None):
        self.config = config or L2Config()

    # ---- Trade size classification ----

    def classify_trade_size(self, volume: int, price: float) -> str:
        """Classify a single trade by its amount (volume * price)."""
        amount = volume * price
        if amount < self.ORDER_CLASS_THRESHOLDS["retail"]:
            return "retail"
        elif amount < self.ORDER_CLASS_THRESHOLDS["small"]:
            return "small"
        elif amount < self.ORDER_CLASS_THRESHOLDS["medium"]:
            return "medium"
        elif amount < self.ORDER_CLASS_THRESHOLDS["large"]:
            return "large"
        return "huge"

    def classify_trades(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add trade size classification to tick DataFrame."""
        result = df.copy()
        if "amount" not in result.columns:
            result["amount"] = result["price"] * result["volume"]
        result["order_class"] = result["amount"].apply(
            lambda amt: (
                "retail" if amt < 20000
                else "small" if amt < 100000
                else "medium" if amt < 500000
                else "large" if amt < 1000000
                else "huge"
            )
        )
        return result

    # ---- Big order detection ----

    def detect_big_orders(self, df: pd.DataFrame, dynamic_threshold: bool = True) -> pd.DataFrame:
        """Detect big and huge orders using dynamic or fixed thresholds.

        Adds columns:
          - amount: trade amount (price * volume)
          - is_big_order: amount >= big threshold (config percentile or fixed)
          - is_huge_order: amount >= huge threshold
          - order_class: retail/small/medium/large/huge
        """
        result = df.copy()
        if "amount" not in result.columns:
            result["amount"] = result["price"] * result["volume"]

        amounts = result["amount"]

        if dynamic_threshold and len(amounts) > 10:
            big_pct = self.config.BIG_ORDER_PERCENTILE
            huge_pct = self.config.HUGE_ORDER_PERCENTILE
            big_threshold = np.percentile(amounts, big_pct)
            huge_threshold = np.percentile(amounts, huge_pct)
        else:
            big_threshold = self.config.INSTITUTIONAL_MIN_AMOUNT
            huge_threshold = self.config.INSTITUTIONAL_MIN_AMOUNT * 5

        result["is_big_order"] = amounts >= big_threshold
        result["is_huge_order"] = amounts >= huge_threshold
        result["order_class"] = result["amount"].apply(
            lambda amt: (
                "retail" if amt < 20000
                else "small" if amt < 100000
                else "medium" if amt < 500000
                else "large" if amt < 1000000
                else "huge"
            )
        )
        return result

    def get_big_order_stats(self, df: pd.DataFrame) -> dict:
        """Compute summary statistics for big/huge orders."""
        total = len(df)
        if total == 0:
            return {}
        df = self.detect_big_orders(df)
        big = df["is_big_order"].sum()
        huge = df["is_huge_order"].sum()

        big_buy = ((df["is_big_order"]) & (df["direction"] == 1)).sum()
        big_sell = ((df["is_big_order"]) & (df["direction"] == -1)).sum()

        return {
            "big_order_count": int(big),
            "big_order_pct": round(big / total * 100, 2),
            "big_order_buy_ratio": round(big_buy / big * 100, 2) if big > 0 else 0,
            "huge_order_count": int(huge),
            "huge_order_pct": round(huge / total * 100, 2),
        }

    # ---- Order flow imbalance ----

    def compute_order_flow_imbalance(self, df: pd.DataFrame, window: int = 50) -> pd.Series:
        """Compute Order Flow Imbalance (OFI) over a sliding window.

        OFI = (buy_volume - sell_volume) / (buy_volume + sell_volume)
        Range: [-1, 1], positive = buy pressure
        """
        if df.empty:
            return pd.Series(dtype=float)

        buy_mask = df["direction"] == 1
        sell_mask = df["direction"] == -1

        buy_vol = pd.Series(0.0, index=df.index)
        sell_vol = pd.Series(0.0, index=df.index)
        buy_vol[buy_mask] = df.loc[buy_mask, "volume"].astype(float)
        sell_vol[sell_mask] = df.loc[sell_mask, "volume"].astype(float)

        buy_cum = buy_vol.rolling(window, min_periods=1).sum()
        sell_cum = sell_vol.rolling(window, min_periods=1).sum()
        denom = buy_cum + sell_cum + 1e-8

        return (buy_cum - sell_cum) / denom

    def compute_cumulative_delta(self, df: pd.DataFrame) -> pd.Series:
        """Compute cumulative volume delta (buy_vol - sell_vol)."""
        if df.empty:
            return pd.Series(dtype=float)

        delta = pd.Series(0.0, index=df.index)
        buy_mask = df["direction"] == 1
        sell_mask = df["direction"] == -1
        delta[buy_mask] = df.loc[buy_mask, "volume"].astype(float)
        delta[sell_mask] = -df.loc[sell_mask, "volume"].astype(float)
        return delta.cumsum()

    # ---- Institutional vs retail classification ----

    def classify_trader_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """Classify each tick as institutional or retail based on trade size.

        Institutional trades are those with amount >= configured threshold.
        """
        result = df.copy()
        if "amount" not in result.columns:
            result["amount"] = result["price"] * result["volume"]

        threshold = self.config.INSTITUTIONAL_MIN_AMOUNT
        result["trader_type"] = result["amount"].apply(
            lambda a: "institutional" if a >= threshold else "retail"
        )
        return result

    def get_trader_flow_stats(self, df: pd.DataFrame) -> dict:
        """Compute institutional vs retail flow statistics."""
        if df.empty:
            return {}
        df = self.classify_trader_type(df)

        inst = df[df["trader_type"] == "institutional"]
        retail = df[df["trader_type"] == "retail"]

        inst_buy = inst[inst["direction"] == 1]["volume"].sum()
        inst_sell = inst[inst["direction"] == -1]["volume"].sum()
        retail_buy = retail[retail["direction"] == 1]["volume"].sum()
        retail_sell = retail[retail["direction"] == -1]["volume"].sum()

        return {
            "institutional_net_flow": int(inst_buy - inst_sell),
            "institutional_buy_vol": int(inst_buy),
            "institutional_sell_vol": int(inst_sell),
            "retail_net_flow": int(retail_buy - retail_sell),
            "retail_buy_vol": int(retail_buy),
            "retail_sell_vol": int(retail_sell),
        }

    # ---- Tick intensity ----

    def compute_tick_intensity(self, df: pd.DataFrame, window: str = "1min") -> pd.Series:
        """Compute tick count per time window (ticks per minute)."""
        if df.empty or "time" not in df.columns:
            return pd.Series(dtype=float)
        df_t = df.set_index("time")
        return df_t.resample(window)["price"].count()

    def detect_tick_surge(self, df: pd.DataFrame) -> list[dict]:
        """Detect periods of abnormally high tick activity.

        Returns list of surge periods with start time and intensity multiplier.
        """
        if len(df) < 100:
            return []

        intensity = self.compute_tick_intensity(df)
        if intensity.empty:
            return []

        mean_intensity = intensity.mean()
        std_intensity = intensity.std()
        if std_intensity == 0:
            return []

        surges = []
        multiplier = self.config.TICK_SURGE_MULTIPLIER
        for ts, val in intensity.items():
            if val > mean_intensity + multiplier * std_intensity:
                surges.append({
                    "time": str(ts),
                    "tick_count": int(val),
                    "multiplier": round(val / mean_intensity, 1),
                })

        return surges

    # ---- Session phases ----

    def analyze_session_phases(self, df: pd.DataFrame) -> dict:
        """Break trading day into phases and compute per-phase stats.

        Phases:
          - opening_call: 09:15 - 09:25
          - early:        09:30 - 10:30
          - mid_morning:  10:30 - 11:30
          - early_after:  13:00 - 14:00
          - late:         14:00 - 14:55
          - closing:      14:55 - 15:00
        """
        if df.empty or "time" not in df.columns:
            return {}

        phases = {
            "opening_call": ("09:15", "09:25"),
            "early": ("09:30", "10:30"),
            "mid_morning": ("10:30", "11:30"),
            "early_after": ("13:00", "14:00"),
            "late": ("14:00", "14:55"),
            "closing": ("14:55", "15:00"),
        }

        result = {}
        for phase_name, (start, end) in phases.items():
            mask = (df["time"].dt.strftime("%H:%M") >= start) & (df["time"].dt.strftime("%H:%M") <= end)
            phase_df = df[mask]
            if phase_df.empty:
                result[phase_name] = {"tick_count": 0, "volume": 0, "amount": 0}
                continue

            result[phase_name] = {
                "tick_count": len(phase_df),
                "volume": int(phase_df["volume"].sum()),
                "amount": float(phase_df["amount"].sum() if "amount" in phase_df.columns
                              else (phase_df["price"] * phase_df["volume"]).sum()),
                "buy_pct": round((phase_df["direction"] == 1).sum() / len(phase_df) * 100, 1),
            }
        return result

    # ---- Full feature extraction ----

    def compute_full_features(self, df: pd.DataFrame, stock_code: str = "", date: str = "") -> dict:
        """Compute comprehensive tick-level features for a stock-date.

        Returns a dict that can be saved to Parquet via FeatureCache.
        """
        if df.empty:
            return {"stock_code": stock_code, "date": date, "total_ticks": 0}

        df = self.detect_big_orders(df)

        total_buy_vol = int(df[df["direction"] == 1]["volume"].sum())
        total_sell_vol = int(df[df["direction"] == -1]["volume"].sum())
        net_buy_vol = total_buy_vol - total_sell_vol
        total_vol = total_buy_vol + total_sell_vol

        ofi = self.compute_order_flow_imbalance(df)
        trader = self.get_trader_flow_stats(df)
        big_stats = self.get_big_order_stats(df)

        # Price stats
        prices = df["price"].dropna()
        amounts = df["amount"] if "amount" in df.columns else df["price"] * df["volume"]

        features = {
            "stock_code": stock_code,
            "date": date,
            "total_ticks": len(df),
            "total_volume": int(df["volume"].sum()),
            "total_amount": float(amounts.sum()),
            "total_buy_vol": total_buy_vol,
            "total_sell_vol": total_sell_vol,
            "net_buy_vol": net_buy_vol,
            "buy_sell_ratio": round(total_buy_vol / total_sell_vol, 4) if total_sell_vol > 0 else None,
            "net_flow_pct": round(net_buy_vol / total_vol * 100, 2) if total_vol > 0 else 0,
            "buy_tick_pct": round((df["direction"] == 1).sum() / len(df) * 100, 2),
            "ofi_mean": round(float(ofi.mean()), 4),
            "ofi_std": round(float(ofi.std()), 4),
            "ofi_last_20": round(float(ofi.tail(20).mean()), 4),
            "avg_trade_size": round(float(amounts.mean()), 2),
            "median_trade_size": round(float(amounts.median()), 2),
            "max_trade_size": round(float(amounts.max()), 2),
            "trade_size_std": round(float(amounts.std()), 2),
            "trade_size_85pct": round(float(np.percentile(amounts, 85)), 2),
            "trade_size_97pct": round(float(np.percentile(amounts, 97)), 2),
            "price_vwap": round(float((prices * df["volume"]).sum() / df["volume"].sum()), 4) if df["volume"].sum() > 0 else 0,
            "price_high": round(float(prices.max()), 4),
            "price_low": round(float(prices.min()), 4),
            "price_range_pct": round(float((prices.max() - prices.min()) / prices.median() * 100), 4) if prices.median() > 0 else 0,
            **big_stats,
            **trader,
            "tick_surge_count": len(self.detect_tick_surge(df)),
        }
        return features
