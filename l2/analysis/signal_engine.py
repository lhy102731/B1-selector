"""L2 trading signal generation engine.

Combines tick analysis and order book features to detect:
  1. whale_accumulation   - 鲸鱼吸筹
  2. whale_distribution   - 鲸鱼出货
  3. wash_trading         - 对倒/洗售
  4. order_wall           - 委托墙
  5. anomaly_trade        - 异常成交
  6. tick_surge           - 成交放量
  7. depth_imbalance      - 深度失衡
  8. spoofing             - 虚假委托

Signal priority: info < warning < alert < critical
"""

from datetime import datetime
from dataclasses import dataclass, field, asdict
import logging

import numpy as np
import pandas as pd

from l2.data.config import L2Config
from l2.analysis.tick_analyzer import TickAnalyzer
from l2.analysis.orderbook_analyzer import OrderBookAnalyzer

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    """L2 trading signal emitted by SignalEngine."""

    signal_type: str           # whale_accumulation, wash_trading, etc.
    stock_code: str
    timestamp: datetime = field(default_factory=datetime.now)
    severity: str = "info"     # info / warning / alert / critical
    title: str = ""
    detail: dict = field(default_factory=dict)
    is_bullish: bool = False
    confidence: float = 0.0    # 0-100

    SEVERITY_ORDER = {"info": 0, "warning": 1, "alert": 2, "critical": 3}

    @property
    def severity_rank(self) -> int:
        return self.SEVERITY_ORDER.get(self.severity, 0)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d


class SignalEngine:
    """L2交易信号生成引擎.

    Combines TickAnalyzer and OrderBookAnalyzer outputs to generate
    actionable trading signals with severity and confidence scores.
    """

    def __init__(self, config: L2Config | None = None):
        self.config = config or L2Config()
        self.tick_analyzer = TickAnalyzer(config)
        self.ob_analyzer = OrderBookAnalyzer(config)

    # ---- Signal detection methods ----

    def detect_whale_accumulation(
        self,
        features: dict,
        feature_history: list[dict] | None = None,
    ) -> Signal | None:
        """Detect whale/institutional accumulation.

        Criteria:
          1. Big order buy ratio > 60% (大单买方占优)
          2. Net buy volume positive in 3+ of last 5 periods
          3. Price not rising sharply (absorption behavior)
          4. Institutional flow positive
        """
        if not features or features.get("total_ticks", 0) < 50:
            return None

        history = feature_history or []
        recent = history[-4:] + [features] if history else [features]

        # Big order buy dominance
        big_ratio = features.get("big_order_buy_ratio", 50)
        if big_ratio < 60:
            return None

        # Consecutive net buying
        net_buys = [(f.get("net_buy_vol", 0) or 0) > 0 for f in recent if f]
        if sum(net_buys) < 2:
            return None

        # Price absorption check - volume in but price not surging
        price_changes = []
        for f in recent:
            if f and f.get("price_vwap", 0) and recent[0].get("price_vwap", 0):
                base = recent[0].get("price_vwap", 0)
                if base > 0:
                    price_changes.append((f["price_vwap"] - base) / base)
        if price_changes and max(price_changes, default=0) > 0.03:
            return None  # Price up >3% means markup, not absorption

        # Institutional net flow
        inst_flow = features.get("institutional_net_flow", 0) or 0
        if inst_flow <= 0:
            return None

        # Confidence scoring
        confidence = min(100, (
            (big_ratio - 50) * 1.5
            + sum(net_buys) * 8
            + min(inst_flow / 10000, 20)
        ))

        return Signal(
            signal_type="whale_accumulation",
            stock_code=features.get("stock_code", ""),
            severity="alert" if confidence > 75 else "warning",
            title=f"鲸鱼吸筹 - 大单买入占比{big_ratio:.0f}%",
            detail={
                "big_order_buy_ratio": big_ratio,
                "net_buy_vol": features.get("net_buy_vol", 0),
                "institutional_net_flow": inst_flow,
                "consecutive_net_buy": sum(net_buys),
                "confidence": round(confidence, 1),
            },
            is_bullish=True,
            confidence=round(confidence, 1),
        )

    def detect_whale_distribution(
        self,
        features: dict,
        feature_history: list[dict] | None = None,
    ) -> Signal | None:
        """Detect institutional distribution (出货).

        Criteria:
          1. Big order sell ratio > 60%
          2. Net sell volume in 3+ of last 5 periods
          3. Price not declining sharply (distribution, not crash)
          4. Institutional flow negative
        """
        if not features or features.get("total_ticks", 0) < 50:
            return None

        history = feature_history or []
        recent = history[-4:] + [features] if history else [features]

        big_ratio = features.get("big_order_buy_ratio", 50)
        if big_ratio > 40:  # sell ratio = 100 - buy_ratio > 60%
            return None

        net_sells = [(f.get("net_buy_vol", 0) or 0) < 0 for f in recent if f]
        if sum(net_sells) < 2:
            return None

        inst_flow = features.get("institutional_net_flow", 0) or 0
        if inst_flow >= 0:
            return None

        sell_ratio = 100 - big_ratio
        confidence = min(100, (
            (sell_ratio - 50) * 1.5
            + sum(net_sells) * 8
            + min(abs(inst_flow) / 10000, 20)
        ))

        return Signal(
            signal_type="whale_distribution",
            stock_code=features.get("stock_code", ""),
            severity="alert" if confidence > 75 else "warning",
            title=f"鲸鱼出货 - 大单卖出占比{sell_ratio:.0f}%",
            detail={
                "big_order_sell_ratio": sell_ratio,
                "net_sell_vol": abs(features.get("net_buy_vol", 0) or 0),
                "institutional_net_flow": inst_flow,
                "consecutive_net_sell": sum(net_sells),
                "confidence": round(confidence, 1),
            },
            is_bullish=False,
            confidence=round(confidence, 1),
        )

    def detect_wash_trading(self, df: pd.DataFrame) -> Signal | None:
        """Detect wash trading (对倒/洗售).

        Patterns:
          1. Near-equal buy and sell volumes in short windows
          2. High direction alternation frequency
          3. Volume spikes with little net change
        """
        if df.empty or len(df) < 50:
            return None

        window = self.config.WASH_TRADE_WINDOW
        wash_scores = []
        stock_code = df.iloc[0].get("stock_code", "")

        for i in range(0, len(df) - window, window // 2):
            chunk = df.iloc[i:i + window]
            buy_vol = chunk[chunk["direction"] == 1]["volume"].sum()
            sell_vol = chunk[chunk["direction"] == -1]["volume"].sum()

            if max(buy_vol, sell_vol) < 1000:
                continue

            vol_symmetry = min(buy_vol, sell_vol) / max(buy_vol, sell_vol + 1)

            direction_changes = (chunk["direction"].diff().fillna(0) != 0).sum()
            alt_freq = direction_changes / max(len(chunk), 1)

            score = vol_symmetry * alt_freq
            if score > 0.5:
                wash_scores.append(score)

        if not wash_scores:
            return None

        max_score = max(wash_scores)
        if max_score < 0.6:
            return None

        confidence = min(100, max_score * 100)

        return Signal(
            signal_type="wash_trading",
            stock_code=stock_code,
            severity="warning" if confidence < 85 else "alert",
            title=f"疑似对倒交易 - 洗售评分{max_score:.0%}",
            detail={
                "wash_score": round(max_score, 3),
                "wash_windows_detected": len(wash_scores),
                "avg_wash_score": round(np.mean(wash_scores), 3),
                "confidence": round(confidence, 1),
            },
            is_bullish=False,
            confidence=round(confidence, 1),
        )

    def detect_anomaly_trade(self, df: pd.DataFrame) -> Signal | None:
        """Detect anomalous trades (price/volume outliers).

        Uses z-score on trade amount distribution.
        Trades with z-score > config threshold are flagged.
        """
        if df.empty or len(df) < 20:
            return None

        amounts = df["price"] * df["volume"]
        mean_amt = amounts.mean()
        std_amt = amounts.std()
        if std_amt == 0:
            return None

        z_scores = (amounts - mean_amt) / std_amt
        threshold = self.config.ANOMALY_ZSCORE_THRESHOLD
        anomalies = df[z_scores > threshold]

        if len(anomalies) == 0:
            return None

        stock_code = df.iloc[0].get("stock_code", "")
        max_z = float(z_scores.max())
        anomaly_count = len(anomalies)
        total = len(df)

        confidence = min(100, anomaly_count / total * 1000 + max_z * 5)

        return Signal(
            signal_type="anomaly_trade",
            stock_code=stock_code,
            severity="warning" if anomaly_count < total * 0.02 else "alert",
            title=f"异常成交 - {anomaly_count}笔超出{threshold}z",
            detail={
                "anomaly_count": anomaly_count,
                "anomaly_pct": round(anomaly_count / total * 100, 2),
                "max_z_score": round(max_z, 2),
                "total_amount": float(anomalies["price"].sum() * anomalies["volume"].sum()),
                "confidence": round(confidence, 1),
            },
            is_bullish=False,
            confidence=round(confidence, 1),
        )

    def detect_order_wall_signal(self, obs: list[dict]) -> Signal | None:
        """Detect significant order wall events."""
        if not obs:
            return None

        all_walls = []
        for ob in obs:
            walls = self.ob_analyzer.detect_order_walls(ob)
            all_walls.extend(walls)

        if not all_walls:
            return None

        strongest = max(all_walls, key=lambda w: w["wall_ratio"])
        stock_code = obs[-1].get("stock_code", "")

        if strongest["wall_ratio"] >= self.config.ORDER_WALL_STRONG_RATIO:
            side_label = "买方托单" if strongest["side"] == "bid" else "卖方压单"
            is_bullish = strongest["side"] == "bid"
            confidence = min(100, strongest["wall_ratio"] * 15)

            return Signal(
                signal_type="order_wall",
                stock_code=stock_code,
                severity="warning" if not is_bullish else "info",
                title=f"{side_label}墙 - 深度{strongest['level']}档 x{strongest['wall_ratio']:.1f}",
                detail={
                    **strongest,
                    "confidence": round(confidence, 1),
                },
                is_bullish=is_bullish,
                confidence=round(confidence, 1),
            )
        return None

    def detect_depth_imbalance_signal(self, ob: dict) -> Signal | None:
        """Detect extreme depth imbalance."""
        if not ob:
            return None

        imbalance = self.ob_analyzer.compute_depth_imbalance(ob)
        extreme = self.config.DEPTH_IMBALANCE_EXTREME

        if abs(imbalance) < extreme:
            return None

        is_bullish = imbalance > 0
        confidence = min(100, abs(imbalance) * 120)

        return Signal(
            signal_type="depth_imbalance",
            stock_code=ob.get("stock_code", ""),
            severity="warning" if abs(imbalance) < 0.85 else "alert",
            title=f"深度{('买盘' if is_bullish else '卖盘')}失衡 - {abs(imbalance):.0%}",
            detail={
                "imbalance": round(imbalance, 4),
                "bid_vol": ob.get("bid_volume_01", 0),
                "ask_vol": ob.get("ask_volume_01", 0),
                "confidence": round(confidence, 1),
            },
            is_bullish=is_bullish,
            confidence=round(confidence, 1),
        )

    def detect_tick_surge_signal(self, df: pd.DataFrame) -> Signal | None:
        """Detect abnormal tick activity surge."""
        if df.empty:
            return None

        surges = self.tick_analyzer.detect_tick_surge(df)
        if not surges:
            return None

        stock_code = df.iloc[0].get("stock_code", "")
        max_surge = max(s["multiplier"] for s in surges)

        confidence = min(100, max_surge * 25)

        return Signal(
            signal_type="tick_surge",
            stock_code=stock_code,
            severity="info" if max_surge < 5 else "warning",
            title=f"成交放量 - {len(surges)}个时段 x{max_surge:.1f}倍",
            detail={
                "surge_periods": len(surges),
                "max_multiplier": max_surge,
                "surge_times": [s["time"] for s in surges[:5]],
                "confidence": round(confidence, 1),
            },
            is_bullish=True,  # Surge can be either, default to bullish
            confidence=round(confidence, 1),
        )

    # ---- Main analysis pipeline ----

    def analyze_all(
        self,
        stock_code: str,
        df: pd.DataFrame,
        obs: list[dict] | None = None,
        features: dict | None = None,
        feature_history: list[dict] | None = None,
    ) -> list[Signal]:
        """Run all detection algorithms and return active signals.

        Args:
            stock_code: Stock code
            df: Tick-by-tick DataFrame
            obs: Order book snapshots (list of dicts)
            features: Pre-computed tick features (optional; computed if None)
            feature_history: Historical features for trend context

        Returns:
            List of Signal objects, sorted by severity desc then confidence desc
        """
        if features is None:
            features = self.tick_analyzer.compute_full_features(df, stock_code)

        signals: list[Signal] = []

        # Whale accumulation
        acc = self.detect_whale_accumulation(features, feature_history)
        if acc:
            signals.append(acc)

        # Whale distribution
        dist = self.detect_whale_distribution(features, feature_history)
        if dist:
            signals.append(dist)

        # Wash trading
        wash = self.detect_wash_trading(df)
        if wash:
            signals.append(wash)

        # Anomaly trades
        anomaly = self.detect_anomaly_trade(df)
        if anomaly:
            signals.append(anomaly)

        # Tick surge
        surge = self.detect_tick_surge_signal(df)
        if surge:
            signals.append(surge)

        # Order book signals
        if obs:
            wall = self.detect_order_wall_signal(obs)
            if wall:
                signals.append(wall)

            if obs:
                imbalance = self.detect_depth_imbalance_signal(obs[-1])
                if imbalance:
                    signals.append(imbalance)

        # Sort by severity desc, then confidence desc
        signals.sort(key=lambda s: (s.severity_rank, s.confidence), reverse=True)
        return signals

    def analyze_single_stock(
        self,
        stock_code: str,
        df: pd.DataFrame,
        obs: list[dict] | None = None,
        history: list[dict] | None = None,
    ) -> dict:
        """Convenience method: run full analysis and return summary dict."""
        signals = self.analyze_all(stock_code, df, obs, history=history)
        features = self.tick_analyzer.compute_full_features(df, stock_code)

        return {
            "stock_code": stock_code,
            "features": features,
            "signals": signals,
            "signal_count": len(signals),
            "has_alert": any(s.severity in ("alert", "critical") for s in signals),
            "has_warning": any(s.severity == "warning" for s in signals),
            "top_signal": signals[0].to_dict() if signals else None,
        }
