"""B1 result table model for displaying selection results."""

from PyQt5.QtCore import QAbstractTableModel, Qt
from PyQt5.QtGui import QColor


class B1ResultTableModel(QAbstractTableModel):
    """Table model for B1 selection results.

    Columns: Score, Code, Name, Close, J, Build Gain, Surge Turn,
             Matched Case, Reasons
    """

    COLUMNS = [
        "Score", "Code", "Name", "Close", "J",
        "Build Gain%", "Surge Turn%", "Matched Case", "Reasons",
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._results: list[dict] = []

    def rowCount(self, parent=None):
        return len(self._results)

    def columnCount(self, parent=None):
        return len(self.COLUMNS)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.COLUMNS[section]
        return None

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()
        r = self._results[row]

        if role == Qt.DisplayRole:
            return self._format_value(col, r)

        if role == Qt.ForegroundRole:
            if col == 0:
                score = r.get("b1_score", 0)
                if score >= 80:
                    return QColor("#22c55e")
                elif score >= 65:
                    return QColor("#3b82f6")
                else:
                    return QColor("#8e91a8")
            if col == 4 and r.get("J", 0) < 0:
                return QColor("#ef4444")
            return None

        if role == Qt.TextAlignmentRole:
            if col in (0, 3, 4, 5, 6):
                return Qt.AlignRight | Qt.AlignVCenter
            return Qt.AlignLeft | Qt.AlignVCenter

        if role == Qt.ToolTipRole:
            r = self._results[row]
            bd = r.get("breakdown", {})
            parts = [
                f"Score: {r.get('b1_score', 0):.1f}",
                f"Matched: {r.get('matched_case', '')} {r.get('matched_date', '')}",
            ]
            if bd:
                parts.append(" | ".join(f"{k}: {v:.0f}" for k, v in bd.items()))
            if r.get("hist_bonus", 0):
                parts.append(f"Hist bonus: {r['hist_bonus']:+.0f}")
            tags = r.get("tags", [])
            if tags:
                parts.append("Tags: " + ", ".join(tags))
            return "\n".join(parts)

        return None

    def _format_value(self, col: int, r: dict) -> str:
        """Format a cell value for display."""
        key_map = {
            0: ("b1_score", ".1f"),
            1: ("code", "s"),
            2: ("name", "s"),
            3: ("close", ".2f"),
            4: ("J", ".1f"),
            5: ("build_gain", ".1f"),
            6: ("surge_turnover", ".1f"),
            7: ("matched_case", "s"),
            8: ("reasons", "s"),
        }
        key, fmt = key_map.get(col, ("", "s"))
        val = r.get(key, "")

        if isinstance(val, float):
            if fmt == ".1f":
                return f"{val:.1f}"
            elif fmt == ".2f":
                return f"{val:.2f}"
            return str(val)
        if isinstance(val, list):
            return ", ".join(str(v) for v in val[:3])
        return str(val) if val else ""

    def add_result(self, result: dict):
        """Append a single result (real-time insert during scan)."""
        row = len(self._results)
        self.beginInsertRows(self.index(-1, -1) if self._results else self.createIndex(0, 0), row, row)
        self._results.append(result)
        self.endInsertRows()

    def set_results(self, results: list[dict]):
        """Batch replace all results."""
        self.beginResetModel()
        self._results = list(results)
        self.endResetModel()

    def get_result(self, row: int) -> dict | None:
        """Get result dict for a row index."""
        if 0 <= row < len(self._results):
            return dict(self._results[row])
        return None

    def get_all_results(self) -> list[dict]:
        """Get all results as a list."""
        return list(self._results)

    def clear(self):
        """Remove all rows."""
        self.beginResetModel()
        self._results.clear()
        self.endResetModel()
