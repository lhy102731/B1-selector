"""Multi-stock monitor panel — QTableView with real-time stock stats."""

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QTableView, QLineEdit, QHeaderView, QAbstractItemView
)
from PyQt5.QtCore import Qt

from l2.gui.models.stock_model import StockTableModel


class MonitorPanel(QWidget):
    """Multi-stock monitoring table.

    Columns: code | name | price | change% | net_vol | big_order% | alert_count | score
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)

        # Search bar
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search stock code/name...")
        self.search_input.setStyleSheet(
            "QLineEdit { background: #1a1b26; color: #e2e4f0; border: 1px solid #2a2b3d;"
            " padding: 5px 8px; border-radius: 4px; }"
            "QLineEdit:focus { border-color: #3b82f6; }"
        )
        layout.addWidget(self.search_input)

        # Table
        self.table = QTableView()
        self.model = StockTableModel()
        self.table.setModel(self.model)
        self._setup_table()

        layout.addWidget(self.table)
        self.setLayout(layout)

        # Connect search
        self.search_input.textChanged.connect(self._on_search)

    def _setup_table(self):
        """Configure table appearance."""
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setStyleSheet("""
            QTableView {
                background: #12131c; color: #e2e4f0;
                gridline-color: #2a2b3d; border: none;
                font-size: 11px;
            }
            QTableView::item { padding: 2px 6px; }
            QTableView::item:selected { background: #1e3a5f; color: #e2e4f0; }
            QTableView::item:hover { background: #1a1b26; }
            QHeaderView::section {
                background: #1a1b26; color: #8e91a8; font-weight: bold;
                border: none; border-bottom: 2px solid #2a2b3d;
                padding: 5px 6px; font-size: 11px;
            }
        """)
        # Fixed column widths + stretch last section for remaining space
        self.table.horizontalHeader().setStretchLastSection(True)
        col_widths = [70, 75, 65, 65, 85, 55, 50, 60]
        for i, w in enumerate(col_widths):
            self.table.setColumnWidth(i, w)
            self.table.horizontalHeader().setSectionResizeMode(i, QHeaderView.Interactive)

    def _on_search(self, text: str):
        """Filter table by search text."""
        if not text.strip():
            self.model.reset_filter()
            return
        # Filter rows where code or name contains search text
        for row in range(self.model.rowCount(None)):
            code = self.model.get_row_data(row).get("code", "")
            name = self.model.get_row_data(row).get("name", "")
            match = text.strip().lower() in code.lower() or text.strip() in name
            self.table.setRowHidden(row, not match)

    def update_stock(self, stock_code: str, data: dict):
        """Update or insert a stock row."""
        self.model.update_stock(stock_code, data)

    def update_signal_badge(self, stock_code: str, severity: str):
        """Update alert count badge for a stock."""
        self.model.update_alert(stock_code, severity)

    def on_stock_selected(self):
        """Get currently selected stock code."""
        idx = self.table.currentIndex()
        if not idx.isValid():
            return None
        row_data = self.model.get_row_data(idx.row())
        return row_data.get("code") if row_data else None

    def get_watchlist(self) -> list[str]:
        """Get list of currently monitored stocks."""
        return self.model.get_all_codes()
