"""Signal table model for alert panel."""

from datetime import datetime

from PyQt5.QtCore import QAbstractTableModel, Qt
from PyQt5.QtGui import QColor


class SignalTableModel(QAbstractTableModel):
    """QAbstractTableModel for signal/alert log table.

    Columns: time | stock | type | severity | title | confidence
    """

    COLUMNS = ["Time", "Stock", "Type", "Severity", "Title", "Conf"]
    SEVERITY_COLORS = {
        "critical": QColor("#ef4444"),
        "alert": QColor("#f97316"),
        "warning": QColor("#eab308"),
        "info": QColor("#a6adc8"),
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self._signals: list[dict] = []

    def rowCount(self, parent=None):
        return len(self._signals)

    def columnCount(self, parent=None):
        return len(self.COLUMNS)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        sig = self._signals[index.row()]
        col = self.COLUMNS[index.column()]

        if role == Qt.DisplayRole:
            if col == "Time":
                ts = sig.get("timestamp")
                if isinstance(ts, datetime):
                    return ts.strftime("%H:%M:%S")
                return str(ts)[-8:]
            elif col == "Stock":
                return sig.get("stock_code", "")
            elif col == "Type":
                return sig.get("signal_type", "")
            elif col == "Severity":
                return sig.get("severity", "")
            elif col == "Title":
                return sig.get("title", "")
            elif col == "Conf":
                conf = sig.get("confidence", 0)
                return f"{conf:.0f}%"

        elif role == Qt.ForegroundRole:
            if col == "Severity":
                severity = sig.get("severity", "info")
                return self.SEVERITY_COLORS.get(severity, QColor("#8e91a8"))
            elif col == "Conf":
                conf = sig.get("confidence", 0)
                if conf >= 80:
                    return QColor("#a6e3a1")
                elif conf >= 60:
                    return QColor("#f9e2af")

        elif role == Qt.TextAlignmentRole:
            if col in ("Conf",):
                return Qt.AlignCenter
            return Qt.AlignLeft | Qt.AlignVCenter

        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.COLUMNS[section]
        return None

    def append_signal(self, signal):
        """Append a new signal to the table."""
        row = self.rowCount()
        self.beginInsertRows(self.index(row, 0), row, row)
        sig_dict = signal.to_dict() if hasattr(signal, "to_dict") else signal
        self._signals.append(sig_dict)
        self.endInsertRows()

    def get_signal(self, row: int) -> dict | None:
        """Get signal dict for a row."""
        if 0 <= row < len(self._signals):
            return self._signals[row]
        return None

    def clear(self):
        """Clear all signals."""
        self.beginResetModel()
        self._signals.clear()
        self.endResetModel()
