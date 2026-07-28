"""Capital flow chart panel — institutional vs retail flow.

Shows net capital flow breakdown by trader type over time.
"""

from PyQt5.QtWidgets import QWidget, QVBoxLayout
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure
import numpy as np


class CapitalFlowChart(QWidget):
    """Capital Flow Chart (资金流向图).

    Shows institutional vs retail net flows and big order net flow.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.figure = Figure(figsize=(6, 4), dpi=100, facecolor="#12131c")
        self.canvas = FigureCanvasQTAgg(self.figure)
        self.ax = self.figure.add_subplot(111)
        self._setup_style()

        # Historical data for trend line
        self.history: list[dict] = []

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

    def update_flow(self, features: dict):
        """Redraw with latest feature data."""
        self.ax.clear()
        self._setup_style()

        if not features:
            self.canvas.draw()
            return

        # Accumulate history for trend
        self.history.append(features)
        if len(self.history) > 20:
            self.history = self.history[-20:]

        # Plot current breakdown as bar chart
        categories = ["Institutional", "Retail", "Big Orders"]
        inst_flow = features.get("institutional_net_flow", 0) or 0
        retail_flow = features.get("retail_net_flow", 0) or 0
        # Net big order flow (estimated from big_order_buy_ratio)
        net_vol = features.get("net_buy_vol", 0) or 0
        values = [inst_flow, retail_flow, net_vol - inst_flow - retail_flow]

        colors = ["#cba6f7" if v > 0 else "#f38ba8" for v in values]
        bars = self.ax.bar(categories, values, color=colors, alpha=0.7)

        # Trend line from history
        if len(self.history) > 1:
            hist_net = [(h.get("institutional_net_flow", 0) or 0) for h in self.history]
            self.ax_twin = self.ax.twinx()
            self.ax_twin.plot(
                range(len(hist_net)), hist_net,
                color="#89b4fa", linewidth=1.5, marker="o", markersize=3
            )
            self.ax_twin.set_ylabel("Inst Net Flow Trend", color="#89b4fa", fontsize=8)
            self.ax_twin.tick_params(colors="#3b82f6", labelsize=7)
            self.ax_twin.set_facecolor("#12131c")
            self.ax_twin.spines["top"].set_visible(False)

        for bar, val in zip(bars, values):
            self.ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + (max(values) * 0.02),
                f"{val:+,.0f}",
                ha="center", va="bottom" if val >= 0 else "top",
                fontsize=7, color="#e2e4f0"
            )

        self.ax.set_ylabel("Net Flow (shares)", color="#8e91a8", fontsize=8)
        stock = features.get("stock_code", "")
        self.ax.set_title(f"Capital Flow - {stock}", color="#e2e4f0", fontsize=10, fontweight="bold")

        self.figure.tight_layout()
        self.canvas.draw()

    def clear_history(self):
        """Reset history."""
        self.history.clear()
