"""B1 result panel — scan controls + progress + result table + actions."""

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QSpinBox, QDoubleSpinBox, QProgressBar, QTableView, QLineEdit,
    QHeaderView, QAbstractItemView,
)
from PyQt5.QtCore import pyqtSignal, Qt

from l2.gui.models.b1_result_model import B1ResultTableModel


class B1ResultPanel(QWidget):
    """Panel with B1 scan controls and a sortable result table.

    Layout (top to bottom):
      1. Control bar: [Start] [Cancel]  Max: [spin]  MinSim: [spin]  Lookback: [spin]
      2. Progress bar + status labels
      3. Result table (QTableView)
      4. Action bar: [Add Selected to Monitor] [Clear]  Search: [____]
    """

    start_scan_signal = pyqtSignal()
    cancel_scan_signal = pyqtSignal()
    add_to_monitor_requested = pyqtSignal(str, str, float)  # code, name, close

    def __init__(self, parent=None):
        super().__init__(parent)
        self.model = B1ResultTableModel(self)
        self._scanning = False
        self._found_count = 0
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout()
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)

        # ---- 1. Control bar ----
        ctrl = QHBoxLayout()
        ctrl.setSpacing(8)

        self.btn_start = QPushButton("Start Scan")
        self.btn_start.setStyleSheet(
            "QPushButton { background: #22c55e; color: #12131c; padding: 5px 14px; "
            "border: none; border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background: #16a34a; }"
            "QPushButton:disabled { background: #2a2b3d; color: #5b5e76; }"
        )
        self.btn_start.clicked.connect(self._on_start)
        ctrl.addWidget(self.btn_start)

        self.btn_cancel = QPushButton("Cancel")
        self.btn_cancel.setStyleSheet(
            "QPushButton { background: #ef4444; color: white; padding: 5px 14px; "
            "border: none; border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background: #dc2626; }"
            "QPushButton:disabled { background: #2a2b3d; color: #5b5e76; }"
        )
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.clicked.connect(self._on_cancel)
        ctrl.addWidget(self.btn_cancel)

        ctrl.addSpacing(12)

        ctrl.addWidget(QLabel("Max Stocks"))
        self.spin_max = QSpinBox()
        self.spin_max.setRange(0, 10000)
        self.spin_max.setValue(500)
        self.spin_max.setSpecialValueText("All")
        self.spin_max.setToolTip("0 = all stocks")
        self.spin_max.setFixedWidth(80)
        ctrl.addWidget(self.spin_max)

        ctrl.addWidget(QLabel("Min Sim"))
        self.spin_sim = QDoubleSpinBox()
        self.spin_sim.setRange(0, 100)
        self.spin_sim.setValue(60)
        self.spin_sim.setDecimals(0)
        self.spin_sim.setSuffix("%")
        self.spin_sim.setFixedWidth(80)
        ctrl.addWidget(self.spin_sim)

        ctrl.addWidget(QLabel("Lookback"))
        self.spin_lookback = QSpinBox()
        self.spin_lookback.setRange(5, 120)
        self.spin_lookback.setValue(35)
        self.spin_lookback.setSuffix("d")
        self.spin_lookback.setFixedWidth(80)
        ctrl.addWidget(self.spin_lookback)

        ctrl.addStretch()
        layout.addLayout(ctrl)

        # ---- 2. Progress bar + status ----
        progress_row = QHBoxLayout()
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setFormat("%p%")
        progress_row.addWidget(self.progress_bar, 3)

        self.label_status = QLabel("Ready")
        self.label_status.setStyleSheet("color: #8e91a8;")
        progress_row.addWidget(self.label_status, 1)

        self.label_found = QLabel("Found: 0")
        self.label_found.setStyleSheet("color: #a6e3a1; font-weight: bold;")
        progress_row.addWidget(self.label_found)

        layout.addLayout(progress_row)

        # ---- 3. Result table ----
        self.table = QTableView()
        self.table.setModel(self.model)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.setStyleSheet("""
            QTableView { background: #12131c; color: #e2e4f0; gridline-color: #2a2b3d;
                         border: 1px solid #2a2b3d; font-size: 11px; }
            QTableView::item:selected { background: #1e3a5f; }
            QTableView::item:hover { background: #1a1b26; }
            QHeaderView::section { background: #1a1b26; color: #8e91a8; font-weight: bold;
                                   padding: 4px; border: none; border-bottom: 2px solid #2a2b3d;
                                   font-size: 11px; }
        """)

        # Default column widths
        col_widths = [55, 65, 70, 55, 45, 70, 70, 90, 160]
        for i, w in enumerate(col_widths):
            self.table.setColumnWidth(i, w)

        self.table.doubleClicked.connect(self._on_double_click)
        self.table.selectionModel().selectionChanged.connect(self._on_selection_changed)

        layout.addWidget(self.table, 1)

        # ---- 4. Action bar ----
        action_row = QHBoxLayout()
        action_row.setSpacing(8)

        self.btn_add_monitor = QPushButton("Add Selected to L2 Monitor")
        self.btn_add_monitor.setEnabled(False)
        self.btn_add_monitor.setStyleSheet(
            "QPushButton { background: #3b82f6; color: white; padding: 5px 12px; "
            "border: none; border-radius: 4px; }"
            "QPushButton:hover { background: #2563eb; }"
            "QPushButton:disabled { background: #2a2b3d; color: #5b5e76; }"
        )
        self.btn_add_monitor.clicked.connect(self._on_add_to_monitor)
        action_row.addWidget(self.btn_add_monitor)

        self.btn_clear = QPushButton("Clear Results")
        self.btn_clear.setStyleSheet(
            "QPushButton { background: #2a2b3d; color: #e2e4f0; padding: 5px 12px; "
            "border: none; border-radius: 4px; }"
            "QPushButton:hover { background: #3b3d54; }"
        )
        self.btn_clear.clicked.connect(self.clear_results)
        action_row.addWidget(self.btn_clear)

        action_row.addStretch()

        action_row.addWidget(QLabel("Filter:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("code or name...")
        self.search_input.setFixedWidth(140)
        self.search_input.setStyleSheet(
            "QLineEdit { background: #1a1b26; color: #e2e4f0; border: 1px solid #2a2b3d; "
            "padding: 3px 6px; border-radius: 3px; }"
            "QLineEdit:focus { border-color: #3b82f6; }"
        )
        self.search_input.textChanged.connect(self._on_filter)
        action_row.addWidget(self.search_input)

        layout.addLayout(action_row)
        self.setLayout(layout)

    # ---- Public API ----

    def get_scan_params(self) -> dict:
        """Return scan parameters from control widgets."""
        max_stocks = self.spin_max.value()
        return {
            "max_stocks": max_stocks if max_stocks > 0 else None,
            "min_similarity": self.spin_sim.value(),
            "lookback_days": self.spin_lookback.value(),
        }

    def set_scanning(self, scanning: bool):
        """Update UI state for scan start/stop."""
        self._scanning = scanning
        self.btn_start.setEnabled(not scanning)
        self.btn_cancel.setEnabled(scanning)
        self.spin_max.setEnabled(not scanning)
        self.spin_sim.setEnabled(not scanning)
        self.spin_lookback.setEnabled(not scanning)

    def update_progress(self, current: int, total: int, code: str):
        """Update progress bar and status label."""
        if total > 0:
            pct = int(current / total * 100)
            self.progress_bar.setValue(pct)
            self.progress_bar.setFormat(f"%p% ({current}/{total})")
        self.label_status.setText(f"Processing {code}" if code else "Complete")
        self.label_found.setText(f"Found: {self._found_count}")

    def add_result(self, result: dict):
        """Add a single B1 result to the table (real-time)."""
        self.model.add_result(result)
        self._found_count += 1
        self.label_found.setText(f"Found: {self._found_count}")

    def set_results(self, results: list[dict]):
        """Batch set all results (for final sort)."""
        self.model.set_results(results)
        self._found_count = len(results)
        self.label_found.setText(f"Found: {self._found_count}")

    def set_status(self, msg: str):
        """Set status label text."""
        self.label_status.setText(msg)

    def show_error(self, msg: str):
        """Show error in status label."""
        self.label_status.setText(msg)
        self.label_status.setStyleSheet("color: #f38ba8; font-weight: bold;")

    def clear_results(self):
        """Clear all results and reset counters."""
        self.model.clear()
        self._found_count = 0
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("%p%")
        self.label_status.setText("Ready")
        self.label_found.setText("Found: 0")
        self.label_status.setStyleSheet("color: #a6adc8;")

    def get_selected_for_monitor(self) -> list[tuple[str, str, float]]:
        """Get (code, name, close) for all selected rows."""
        selected = []
        for idx in self.table.selectionModel().selectedRows():
            result = self.model.get_result(idx.row())
            if result:
                selected.append((
                    result["code"],
                    result.get("name", ""),
                    result.get("close", 0.0),
                ))
        return selected

    # ---- Internal slots ----

    def _on_start(self):
        self.start_scan_signal.emit()

    def _on_cancel(self):
        self.cancel_scan_signal.emit()

    def _on_add_to_monitor(self):
        stocks = self.get_selected_for_monitor()
        for code, name, close in stocks:
            self.add_to_monitor_requested.emit(code, name, close)

    def _on_double_click(self, index):
        """Double-click a row to add that single stock to monitor."""
        result = self.model.get_result(index.row())
        if result:
            self.add_to_monitor_requested.emit(
                result["code"], result.get("name", ""), result.get("close", 0.0)
            )

    def _on_selection_changed(self):
        """Enable/disable Add button based on selection."""
        has_selection = len(self.table.selectionModel().selectedRows()) > 0
        self.btn_add_monitor.setEnabled(has_selection and not self._scanning)

    def _on_filter(self, text: str):
        """Filter table rows by code or name."""
        for row in range(self.model.rowCount()):
            match = False
            result = self.model.get_result(row)
            if result:
                t = text.lower()
                match = t in result.get("code", "").lower() or t in result.get("name", "").lower()
            self.table.setRowHidden(row, not match)
