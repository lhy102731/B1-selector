"""Compact stock selector panel — search, add, and select stocks."""

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLineEdit,
    QPushButton, QListWidget, QListWidgetItem, QLabel,
)
from PyQt5.QtCore import pyqtSignal, Qt


class StockSelector(QWidget):
    """Compact left panel for selecting stocks from the watchlist.

    Layout:
      - Search input + Add button
      - Watchlist (QListWidget)
    """

    stock_selected = pyqtSignal(str)   # emits stock_code
    stock_added = pyqtSignal(str)      # emits stock_code

    def __init__(self, parent=None):
        super().__init__(parent)

        layout = QVBoxLayout()
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        # Header
        header = QLabel("Watchlist")
        header.setStyleSheet(
            "color: #8e91a8; font-weight: bold; font-size: 10px; padding: 2px 4px;"
        )
        layout.addWidget(header)

        # Search + Add row
        row = QHBoxLayout()
        row.setSpacing(4)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Stock code...")
        self.search_input.setStyleSheet(
            "QLineEdit { background: #1a1b26; color: #e2e4f0; border: 1px solid #2a2b3d;"
            " padding: 5px 6px; font-size: 11px; border-radius: 4px; }"
            "QLineEdit:focus { border-color: #3b82f6; }"
        )
        self.search_input.returnPressed.connect(self._on_add)
        row.addWidget(self.search_input)

        self.btn_add = QPushButton("Add")
        self.btn_add.setFixedWidth(44)
        self.btn_add.setStyleSheet(
            "QPushButton { background: #22c55e; color: #12131c; border: none;"
            " padding: 4px 10px; font-size: 10px; border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background: #16a34a; }"
        )
        self.btn_add.clicked.connect(self._on_add)
        row.addWidget(self.btn_add)

        layout.addLayout(row)

        # Watchlist
        self.list_widget = QListWidget()
        self.list_widget.setStyleSheet(
            "QListWidget { background: #12131c; color: #e2e4f0; border: 1px solid #2a2b3d;"
            " font-size: 11px; border-radius: 4px; }"
            "QListWidget::item { padding: 4px 8px; }"
            "QListWidget::item:selected { background: #1e3a5f; color: #e2e4f0; }"
            "QListWidget::item:hover { background: #1a1b26; }"
        )
        self.list_widget.itemClicked.connect(self._on_item_clicked)
        layout.addWidget(self.list_widget)

        self.setLayout(layout)

    def _on_add(self):
        code = self.search_input.text().strip()
        if code:
            code = code.zfill(6)
            self.add_stock(code)
            self.stock_added.emit(code)
            self.search_input.clear()

    def _on_item_clicked(self, item: QListWidgetItem):
        code = item.data(Qt.UserRole)
        if code:
            self.stock_selected.emit(code)

    def add_stock(self, code: str, name: str = ""):
        """Add a stock code to the list (dedup)."""
        # Check if already exists
        for i in range(self.list_widget.count()):
            if self.list_widget.item(i).data(Qt.UserRole) == code:
                return

        label = f"{code}" if not name else f"{code}  {name}"
        item = QListWidgetItem(label)
        item.setData(Qt.UserRole, code)
        self.list_widget.addItem(item)

    def remove_stock(self, code: str):
        """Remove a stock from the list."""
        for i in range(self.list_widget.count()):
            if self.list_widget.item(i).data(Qt.UserRole) == code:
                self.list_widget.takeItem(i)
                return

    def get_codes(self) -> list[str]:
        """Get all stock codes in the list."""
        return [
            self.list_widget.item(i).data(Qt.UserRole)
            for i in range(self.list_widget.count())
        ]

    def select_stock(self, code: str):
        """Programmatically select a stock in the list."""
        for i in range(self.list_widget.count()):
            if self.list_widget.item(i).data(Qt.UserRole) == code:
                self.list_widget.setCurrentRow(i)
                return
