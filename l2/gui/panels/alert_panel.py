"""Alert/signal log panel — displays generated L2 signals."""

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QTableView, QLabel, QHeaderView, QAbstractItemView
)
from PyQt5.QtCore import Qt, pyqtSignal

from l2.gui.models.signal_model import SignalTableModel


class AlertPanel(QWidget):
    """Signal/Alert log panel.

    Shows all generated signals in a color-coded table:
    | time | stock | type | severity | title | confidence |

    Severity colors:
      - info:     gray
      - warning:  yellow
      - alert:    orange
      - critical: red
    """

    signal_clicked = pyqtSignal(str)  # emits stock_code on click

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)

        # Header
        self.header = QLabel("Signals (信号日志)")
        self.header.setStyleSheet("color: #8e91a8; font-weight: bold; padding: 2px 4px; font-size: 10px;")
        layout.addWidget(self.header)

        # Table
        self.table = QTableView()
        self.model = SignalTableModel()
        self.table.setModel(self.model)
        self._setup_table()

        layout.addWidget(self.table)
        self.setLayout(layout)

        # Click handler
        self.table.clicked.connect(self._on_click)

    def _setup_table(self):
        """Configure table appearance."""
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setAlternatingRowColors(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setStyleSheet("""
            QTableView {
                background: #12131c; color: #e2e4f0;
                gridline-color: #2a2b3d; border: none;
                font-size: 11px;
            }
            QTableView::item:selected { background: #1e3a5f; }
            QHeaderView::section {
                background: #1a1b26; color: #8e91a8; font-weight: bold;
                border: none; border-bottom: 2px solid #2a2b3d;
                padding: 4px 6px; font-size: 11px;
            }
        """)
        # Column widths: time, stock, type, severity, title, confidence
        col_widths = [150, 70, 80, 65, 160, 70]
        for i, w in enumerate(col_widths):
            self.table.setColumnWidth(i, w)
            self.table.horizontalHeader().setSectionResizeMode(i, QHeaderView.Interactive)

    def _on_click(self, index):
        """Handle click — emit stock_code for navigation."""
        row_data = self.model.get_signal(index.row())
        if row_data:
            code = row_data.get("stock_code", "")
            if code:
                self.signal_clicked.emit(code)

    def add_signal(self, signal):
        """Append a signal to the log table."""
        self.model.append_signal(signal)

    def clear_signals(self):
        """Clear all signals."""
        self.model.clear()

    def get_signal_count(self) -> int:
        """Get total signal count."""
        return self.model.rowCount(None)
