"""Tick flow chart panel — cumulative delta and trade markers.

Shows cumulative volume delta (buy - sell) over time with
big order markers and trade direction coloring.
"""

from PyQt5.QtWidgets import QWidget, QVBoxLayout
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure
import pandas as pd
import numpy as np


class TickFlowChart(QWidget):
    """Tick-by-Tick Flow Chart (逐笔成交流量图).

    Shows cumulative volume delta and trade intensity over time.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.figure = Figure(figsize=(6, 4), dpi=100, facecolor="#12131c")
        self.canvas = FigureCanvasQTAgg(self.figure)
        self.ax = self.figure.add_subplot(111)
        self._setup_style()
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.canvas)
        self.setLayout(layout)

    def _setup_style(self):
        self.ax.set_facecolor("#12131c")
        self.figure.patch.set_facecolor("#12131c")
        self.ax.tick_params(colors="#8e91a8", labelsize=8)
        self.ax.spines["bottom"].set_color("#2a2b3d")
        self.ax.spines["left"].set_color("#2a2b3d")
        self.ax.spines["top"].set_visible(False)
        self.ax.spines["right"].set_visible(False)
        self.ax.axhline(0, color="#2a2b3d", linewidth=0.5)

    def clear(self):
        """Reset chart to empty state."""
        self.ax.clear()
        self._setup_style()
        self.ax.axhline(0, color="#45475a", linewidth=0.5)
        self.canvas.draw()

    def update_ticks(self, df: pd.DataFrame, features: dict | None = None):
        """Redraw with latest tick data."""
        self.ax.clear()
        self._setup_style()

        if df.empty:
            self.canvas.draw()
            return

        # Compute cumulative delta
        buy_mask = df["direction"] == 1
        sell_mask = df["direction"] == -1

        delta = pd.Series(0.0, index=df.index)
        delta[buy_mask] = df.loc[buy_mask, "volume"].astype(float)
        delta[sell_mask] = -df.loc[sell_mask, "volume"].astype(float)
        cum_delta = delta.cumsum()

        # Time axis
        if "time" in df.columns:
            x = df["time"]
        else:
            x = range(len(df))

        # Plot cumulative delta
        self.ax.plot(x, cum_delta.values, color="#89b4fa", linewidth=1.2, alpha=0.8)
        self.ax.fill_between(
            x, cum_delta.values, 0,
            where=(cum_delta.values >= 0),
            color="#a6e3a1", alpha=0.15
        )
        self.ax.fill_between(
            x, cum_delta.values, 0,
            where=(cum_delta.values < 0),
            color="#f38ba8", alpha=0.15
        )

        # Big order markers (if feature data available)
        if features and features.get("big_order_count", 0) > 0:
            threshold = features.get("trade_size_85pct", 0)
            if threshold > 0:
                big_idx = df[df["amount"] >= threshold].index
                if len(big_idx) > 0:
                    big_x = [x[i] if isinstance(x, pd.Index) else big_idx for i in range(len(big_idx))]
                    self.ax.scatter(
                        [df.index.get_loc(i) if isinstance(i, (int, np.integer)) else i for i in big_idx],
                        cum_delta.iloc[[df.index.get_loc(i) if isinstance(i, (int, np.integer)) else i for i in big_idx]],
                        color="#f9e2af", s=10, alpha=0.6, marker="o", label="Big orders"
                    )

        self.ax.set_xlabel("Tick Sequence", color="#cdd6f4", fontsize=8)
        self.ax.set_ylabel("Cumulative Delta (vol)", color="#cdd6f4", fontsize=8)
        stock = features.get("stock_code", "") if features else ""
        net = features.get("net_buy_vol", 0) if features else 0
        self.ax.set_title(
            f"Tick Flow - {stock} (net: {net:+,})",
            color="#cdd6f4", fontsize=10, fontweight="bold"
        )
        if len(self.ax.get_legend_handles_labels()[0]) > 0:
            self.ax.legend(loc="upper left", fontsize=7)

        self.figure.tight_layout()
        self.canvas.draw()
