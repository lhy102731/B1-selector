"""Depth chart panel — L2 order book depth visualization.

Red/Green cumulative volume chart with order wall annotations.
Embedded matplotlib in PyQt5 QWidget.
"""

from PyQt5.QtWidgets import QWidget, QVBoxLayout
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure
import numpy as np


class DepthChart(QWidget):
    """L2 Order Book Depth Chart (买卖盘深度图).

    Shows cumulative bid (red) and ask (green) volumes at each price level.
    Annotates order walls and mid-price.
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
        """Dark theme styling for depth chart."""
        self.ax.set_facecolor("#12131c")
        self.figure.patch.set_facecolor("#12131c")
        self.ax.tick_params(colors="#8e91a8", labelsize=8)
        self.ax.spines["bottom"].set_color("#2a2b3d")
        self.ax.spines["left"].set_color("#2a2b3d")
        self.ax.spines["top"].set_visible(False)
        self.ax.spines["right"].set_visible(False)

    def clear(self):
        """Reset chart to empty state."""
        self.ax.clear()
        self._setup_style()
        self.canvas.draw()

    def update_depth(self, ob: dict):
        """Redraw depth chart with latest order book snapshot."""
        self.ax.clear()
        self._setup_style()

        levels = ob.get("levels", 10)

        bid_prices = np.array([ob.get(f"bid_price_{i:02d}", 0) or 0 for i in range(1, levels + 1)])
        bid_volumes = np.array([ob.get(f"bid_volume_{i:02d}", 0) or 0 for i in range(1, levels + 1)])
        ask_prices = np.array([ob.get(f"ask_price_{i:02d}", 0) or 0 for i in range(1, levels + 1)])
        ask_volumes = np.array([ob.get(f"ask_volume_{i:02d}", 0) or 0 for i in range(1, levels + 1)])

        # Filter zero prices
        bid_mask = bid_prices > 0
        ask_mask = ask_prices > 0
        bid_prices = bid_prices[bid_mask]
        bid_volumes = bid_volumes[bid_mask]
        ask_prices = ask_prices[ask_mask]
        ask_volumes = ask_volumes[ask_mask]

        if len(bid_prices) == 0 and len(ask_prices) == 0:
            self.canvas.draw()
            return

        # Cumulative volumes
        cum_bid = np.cumsum(bid_volumes[::-1])[::-1]
        cum_ask = np.cumsum(ask_volumes)

        # Plot
        self.ax.fill_between(bid_prices, cum_bid, alpha=0.4, color="#f38ba8", step="post")
        self.ax.step(bid_prices, cum_bid, color="#f38ba8", linewidth=1.5, where="post")
        self.ax.fill_between(ask_prices, cum_ask, alpha=0.4, color="#a6e3a1", step="post")
        self.ax.step(ask_prices, cum_ask, color="#a6e3a1", linewidth=1.5, where="post")

        # Mid-price line
        if len(bid_prices) > 0 and len(ask_prices) > 0:
            mid = (bid_prices[0] + ask_prices[0]) / 2
            self.ax.axvline(mid, color="#89b4fa", linestyle="--", alpha=0.5, linewidth=1)

        # Annotations
        self.ax.set_xlabel("Price", color="#cdd6f4", fontsize=8)
        self.ax.set_ylabel("Cum Vol", color="#cdd6f4", fontsize=8)
        self.ax.set_title(
            f"L2 Depth - {ob.get('stock_code', '')} (spread: {ob.get('spread', 0):.3f})",
            color="#cdd6f4", fontsize=10, fontweight="bold"
        )

        self.figure.tight_layout()
        self.canvas.draw()
