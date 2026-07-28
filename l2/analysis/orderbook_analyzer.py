"""Order book depth analyzer for L2 data.

Analysis on N-level market depth data:
  1. Order wall detection (托单/压单)
  2. Depth imbalance (买卖盘力度对比)
  3. Spread analysis (价差分析)
  4. Weighted mid-price computation
  5. Depth spoofing detection
"""

import numpy as np
import pandas as pd

from l2.data.config import L2Config


class OrderBookAnalyzer:
    """订单簿分析器 - analyzes L2 market depth snapshots."""

    def __init__(self, config: L2Config | None = None):
        self.config = config or L2Config()

    # ---- Depth imbalance ----

    def compute_depth_imbalance(self, ob: dict) -> float:
        """Compute buy/sell depth imbalance ratio.

        imbalance = (total_bid_vol - total_ask_vol) / (total_bid_vol + total_ask_vol)
        > 0 = buy pressure, < 0 = sell pressure
        """
        levels = ob.get("levels", 10)
        total_bid = sum(ob.get(f"bid_volume_{i:02d}", 0) or 0 for i in range(1, levels + 1))
        total_ask = sum(ob.get(f"ask_volume_{i:02d}", 0) or 0 for i in range(1, levels + 1))
        return (total_bid - total_ask) / (total_bid + total_ask + 1e-8)

    def compute_weighted_imbalance(self, ob: dict) -> float:
        """Compute volume-weighted depth imbalance.

        Nearer price levels get higher weight (closer to mid = more impact).
        """
        levels = ob.get("levels", 10)
        weights = np.arange(levels, 0, -1)  # level 1 weight=10, level 10 weight=1
        total_bid = sum(
            (ob.get(f"bid_volume_{i:02d}", 0) or 0) * weights[i - 1]
            for i in range(1, levels + 1)
        )
        total_ask = sum(
            (ob.get(f"ask_volume_{i:02d}", 0) or 0) * weights[i - 1]
            for i in range(1, levels + 1)
        )
        return (total_bid - total_ask) / (total_bid + total_ask + 1e-8)

    # ---- Order wall detection ----

    def detect_order_walls(self, ob: dict) -> list[dict]:
        """Detect large pending orders (order walls).

        An order wall exists when a single price level's volume
        exceeds wall_threshold_ratio * average level volume.

        Returns list of wall dicts sorted by wall_ratio descending.
        """
        levels = ob.get("levels", 10)
        walls = []

        for side in ["bid", "ask"]:
            vols = np.array([
                ob.get(f"{side}_volume_{i:02d}", 0) or 0
                for i in range(1, levels + 1)
            ], dtype=float)

            prices = np.array([
                ob.get(f"{side}_price_{i:02d}", 0) or 0
                for i in range(1, levels + 1)
            ], dtype=float)

            avg_vol = vols.mean()
            if avg_vol <= 0:
                continue

            ratio = self.config.ORDER_WALL_THRESHOLD_RATIO
            for i in range(levels):
                wall_ratio = vols[i] / avg_vol
                if wall_ratio >= ratio:
                    walls.append({
                        "side": side,
                        "level": i + 1,
                        "price": float(prices[i]),
                        "volume": int(vols[i]),
                        "wall_ratio": round(wall_ratio, 1),
                    })

        return sorted(walls, key=lambda w: w["wall_ratio"], reverse=True)

    def has_strong_wall(self, ob: dict) -> dict | None:
        """Check if there's a very strong wall (>= strong_ratio threshold)."""
        walls = self.detect_order_walls(ob)
        strong_ratio = self.config.ORDER_WALL_STRONG_RATIO
        for w in walls:
            if w["wall_ratio"] >= strong_ratio:
                return w
        return None

    # ---- Weighted mid-price ----

    def compute_weighted_mid_price(self, ob: dict) -> float:
        """Compute volume-weighted mid price.

        Closer bid/ask levels receive higher weights.
        """
        levels = ob.get("levels", 10)
        best_bid = ob.get("bid_price_01", 0) or 0
        best_ask = ob.get("ask_price_01", 0) or 0

        if best_bid <= 0 or best_ask <= 0:
            return (best_bid + best_ask) / 2 if best_ask > 0 else best_bid

        total_bid_vol = sum(ob.get(f"bid_volume_{i:02d}", 0) or 0 for i in range(1, levels + 1))
        total_ask_vol = sum(ob.get(f"ask_volume_{i:02d}", 0) or 0 for i in range(1, levels + 1))

        return (best_bid * total_ask_vol + best_ask * total_bid_vol) / (total_bid_vol + total_ask_vol + 1e-8)

    # ---- Spread analysis ----

    def compute_spread_stats(self, obs: list[dict]) -> dict:
        """Compute spread statistics across multiple order book snapshots."""
        if not obs:
            return {}

        spreads = []
        for ob in obs:
            bid = ob.get("bid_price_01", 0) or 0
            ask = ob.get("ask_price_01", 0) or 0
            if bid > 0 and ask > 0:
                spread_bps = (ask - bid) / bid * 10000
                spreads.append(spread_bps)

        if not spreads:
            return {}

        return {
            "mean_spread_bps": round(float(np.mean(spreads)), 2),
            "max_spread_bps": round(float(np.max(spreads)), 2),
            "min_spread_bps": round(float(np.min(spreads)), 2),
            "spread_volatility_bps": round(float(np.std(spreads)), 2),
            "snapshot_count": len(spreads),
        }

    # ---- Depth pressure ----

    def compute_depth_pressure(self, ob: dict) -> dict:
        """Compute buy/sell pressure at different depth levels.

        Returns pressure ratios for:
          - near (levels 1-3): immediate pressure
          - mid (levels 4-7):  medium-range pressure
          - far (levels 8-10): deep support/resistance
        """
        levels = ob.get("levels", 10)
        ranges = {"near": (1, 3), "mid": (4, 7), "far": (8, min(10, levels))}

        result = {}
        for name, (start, end) in ranges.items():
            bid_vol = sum(ob.get(f"bid_volume_{i:02d}", 0) or 0 for i in range(start, end + 1))
            ask_vol = sum(ob.get(f"ask_volume_{i:02d}", 0) or 0 for i in range(start, end + 1))
            total = bid_vol + ask_vol
            result[f"{name}_pressure"] = round((bid_vol - ask_vol) / (total + 1e-8), 4)

        return result

    # ---- Full order book feature extraction ----

    def compute_full_features(self, obs: list[dict]) -> dict:
        """Compute comprehensive order book features from a list of snapshots."""
        if not obs:
            return {}

        latest = obs[-1]

        # Aggregate across snapshots
        depth_imbalances = [self.compute_depth_imbalance(ob) for ob in obs]
        weighted_imbalances = [self.compute_weighted_imbalance(ob) for ob in obs]
        spread_stats = self.compute_spread_stats(obs)
        walls = self.detect_order_walls(latest)
        pressure = self.compute_depth_pressure(latest)
        mid_price = self.compute_weighted_mid_price(latest)

        return {
            "depth_imbalance_mean": round(float(np.mean(depth_imbalances)), 4),
            "depth_imbalance_std": round(float(np.std(depth_imbalances)), 4),
            "weighted_imbalance_mean": round(float(np.mean(weighted_imbalances)), 4),
            "mid_price": round(mid_price, 4),
            "best_bid": float(latest.get("bid_price_01", 0) or 0),
            "best_ask": float(latest.get("ask_price_01", 0) or 0),
            "spread": float(latest.get("spread", 0) or 0),
            **spread_stats,
            "wall_count": len(walls),
            "strongest_wall_ratio": walls[0]["wall_ratio"] if walls else 0,
            "strongest_wall_side": walls[0]["side"] if walls else None,
            **pressure,
        }
