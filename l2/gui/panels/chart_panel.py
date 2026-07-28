"""Chart panel — QTabWidget aggregating all chart types."""

from PyQt5.QtWidgets import QTabWidget

from l2.gui.panels.depth_chart import DepthChart
from l2.gui.panels.tick_chart import TickFlowChart
from l2.gui.panels.capital_flow import CapitalFlowChart


class ChartPanel(QTabWidget):
    """Central chart panel with tabs for all visualization types."""

    def __init__(self, parent=None):
        super().__init__(parent)

        self.depth_chart = DepthChart()
        self.tick_chart = TickFlowChart()
        self.flow_chart = CapitalFlowChart()

        self.addTab(self.depth_chart, "Depth (深度图)")
        self.addTab(self.tick_chart, "Tick Flow (逐笔成交)")
        self.addTab(self.flow_chart, "Capital Flow (资金流向)")

        self.setTabPosition(QTabWidget.North)
        self.setStyleSheet("""
            QTabWidget::pane { border: 1px solid #2a2b3d; background: #12131c; }
            QTabBar::tab { background: #1a1b26; color: #8e91a8; padding: 6px 16px;
                           border: none; border-bottom: 2px solid transparent;
                           font-size: 11px; margin-right: 2px; }
            QTabBar::tab:selected { color: #e2e4f0; border-bottom: 2px solid #3b82f6; }
            QTabBar::tab:hover { color: #e2e4f0; background: #242540; }
        """)

    def update_for_stock(self, stock_code: str, df, ob: dict | None, features: dict | None):
        """Update all charts for the currently selected stock."""
        if ob:
            self.depth_chart.update_depth(ob)
        if df is not None:
            self.tick_chart.update_ticks(df, features)
        if features:
            self.flow_chart.update_flow(features)
            current_idx = self.currentIndex()
            if current_idx == 0 and ob is None:
                self.setCurrentIndex(1)

    def clear_data(self):
        """Clear all chart data."""
        self.depth_chart.clear()
        self.tick_chart.clear()
        self.flow_chart.clear_history()
