"""Stock table model for monitor panel."""

from PyQt5.QtCore import QAbstractTableModel, Qt
from PyQt5.QtGui import QColor


class StockTableModel(QAbstractTableModel):
    """QAbstractTableModel for multi-stock monitoring table.

    Columns: code | name | price | change% | net_vol | big_order% | alert_count | score
    """

    COLUMNS = ["Code", "Name", "Price", "Chg%", "Net Vol", "Big%", "Alerts", "Score"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._data: list[dict] = []
        self._code_index: dict[str, int] = {}

    def rowCount(self, parent=None):
        return len(self._data)

    def columnCount(self, parent=None):
        return len(self.COLUMNS)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        row = self._data[index.row()]
        col = self.COLUMNS[index.column()].lower().replace("%", "").replace(" ", "_")

        if role == Qt.DisplayRole:
            val = row.get(col, "")
            if col == "chg%" and isinstance(val, (int, float)):
                return f"{val:+.2f}%"
            elif col == "price" and isinstance(val, (int, float)):
                return f"{val:.2f}"
            elif col == "net_vol" and isinstance(val, (int, float)):
                return f"{val:+,}"
            elif col == "big%" and isinstance(val, (int, float)):
                return f"{val:.1f}%"
            elif col == "score" and isinstance(val, (int, float)):
                return f"{val:.1f}"
            return str(val)

        elif role == Qt.ForegroundRole:
            if col == "chg%":
                val = row.get(col, 0) or 0
                return QColor("#22c55e") if val >= 0 else QColor("#ef4444")
            elif col == "net_vol":
                val = row.get(col, 0) or 0
                return QColor("#22c55e") if val >= 0 else QColor("#ef4444")
            elif col == "alerts":
                val = row.get(col, 0) or 0
                if val >= 3:
                    return QColor("#ef4444")
                elif val >= 1:
                    return QColor("#eab308")
            elif col == "score":
                val = row.get(col, 0) or 0
                if val >= 80:
                    return QColor("#22c55e")
                elif val >= 60:
                    return QColor("#3b82f6")

        elif role == Qt.TextAlignmentRole:
            if col in ("code", "name"):
                return Qt.AlignLeft | Qt.AlignVCenter
            return Qt.AlignRight | Qt.AlignVCenter

        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.COLUMNS[section]
        return None

    def update_stock(self, stock_code: str, data: dict):
        """Update or insert a stock row."""
        data["code"] = stock_code
        if stock_code in self._code_index:
            row = self._code_index[stock_code]
            self._data[row].update(data)
            self.dataChanged.emit(
                self.index(row, 0), self.index(row, len(self.COLUMNS) - 1)
            )
        else:
            self.beginInsertRows(self.index(self.rowCount(), 0), self.rowCount(), self.rowCount())
            # Ensure all columns exist
            row_data = {"code": stock_code, "name": "", "price": 0, "chg%": 0,
                        "net_vol": 0, "big%": 0, "alerts": 0, "score": 0}
            row_data.update(data)
            self._data.append(row_data)
            self._code_index[stock_code] = len(self._data) - 1
            self.endInsertRows()

    def update_alert(self, stock_code: str, severity: str):
        """Increment alert count for a stock."""
        if stock_code in self._code_index:
            row = self._code_index[stock_code]
            self._data[row]["alerts"] = self._data[row].get("alerts", 0) + 1
            self.dataChanged.emit(
                self.index(row, 6), self.index(row, 6)
            )

    def get_row_data(self, row: int) -> dict | None:
        """Get data dict for a row."""
        if 0 <= row < len(self._data):
            return self._data[row]
        return None

    def get_all_codes(self) -> list[str]:
        """Get list of all stock codes in the model."""
        return [d["code"] for d in self._data]

    def reset_filter(self):
        """Reset any active filter (placeholder for external use)."""
        pass

    def clear(self):
        """Clear all data."""
        self.beginResetModel()
        self._data.clear()
        self._code_index.clear()
        self.endResetModel()
