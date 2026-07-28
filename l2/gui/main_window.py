"""Main window for DeepTrade L2 desktop application.

QDockWidget layout:
  - Left: Monitor panel (stock watchlist)
  - Center: Chart panel (depth / tick flow / capital flow tabs)
  - Bottom: Alert panel (signal log)
  - Right: Config panel (analyzer toggles, thresholds)
  - Right/Bottom: B1 Result panel (B1 selection scan + results)
"""

import time
import pandas as pd

from PyQt5.QtWidgets import (
    QMainWindow, QDockWidget, QAction, QToolBar,
    QStatusBar, QLabel, QWidget, QVBoxLayout, QSplitter
)
from PyQt5.QtCore import Qt, QTimer

from l2.data.config import L2Config
from l2.data.collector import L2DataCollector
from l2.analysis.tick_analyzer import TickAnalyzer
from l2.analysis.signal_engine import SignalEngine
from l2.analysis.features import FeatureCache

from l2.gui.panels.monitor_panel import MonitorPanel
from l2.gui.panels.chart_panel import ChartPanel
from l2.gui.panels.alert_panel import AlertPanel
from l2.gui.panels.config_panel import ConfigPanel
from l2.gui.panels.b1_result_panel import B1ResultPanel
from l2.gui.panels.stock_selector import StockSelector
from l2.gui.workers.data_worker import L2DataWorker
from l2.gui.workers.analysis_worker import AnalysisWorker
from l2.gui.workers.b1_worker import B1Worker


class MainWindow(QMainWindow):
    """DeepTrade L2 desktop application main window."""

    def __init__(self, config: L2Config | None = None):
        super().__init__()
        self.config = config or L2Config()

        self.setWindowTitle("DeepTrade L2 - A股Level2量化分析系统")
        self.resize(1600, 1000)
        self.setMinimumSize(1024, 600)

        # Core components
        self.tick_analyzer = TickAnalyzer(self.config)
        self.signal_engine = SignalEngine(self.config)
        self.feature_cache = FeatureCache(self.config)

        # Data buffers per stock
        self._tick_buffers: dict[str, list] = {}
        self._ob_buffers: dict[str, list] = {}
        self._latest_features: dict[str, dict] = {}
        self._current_stock: str = ""

        # Thread workers (started in setup_workers)
        self.data_worker: L2DataWorker | None = None
        self.analysis_worker: AnalysisWorker | None = None
        self.b1_worker: B1Worker | None = None

        # Build UI
        self._setup_ui()
        self._setup_workers()
        self._connect_signals()

        # Status
        self.statusBar().showMessage("Ready - Add stocks to begin monitoring")

    # ---- UI Setup ----

    def _setup_ui(self):
        """Build the dock widget layout.

        Layout: Top=monitor, Left=selector, Center=charts, Bottom=signals, Right=config
        Design: see app.py stylesheet for full design system.
        """

        # Panel instances
        self.monitor_panel = MonitorPanel()
        self.chart_panel = ChartPanel()
        self.alert_panel = AlertPanel()
        self.config_panel = ConfigPanel()
        self.stock_selector = StockSelector()

        # Top dock: Stock monitor (full width, no column truncation)
        top_dock = QDockWidget("Market Monitor (行情监控)", self)
        top_dock.setWidget(self.monitor_panel)
        top_dock.setFeatures(QDockWidget.DockWidgetMovable | QDockWidget.DockWidgetFloatable)
        top_dock.setMinimumHeight(150)
        self.addDockWidget(Qt.TopDockWidgetArea, top_dock)
        self._top_dock = top_dock

        # Left dock: Stock selector (compact)
        left_dock = QDockWidget("Watchlist (自选)", self)
        left_dock.setWidget(self.stock_selector)
        left_dock.setFeatures(QDockWidget.DockWidgetMovable | QDockWidget.DockWidgetFloatable)
        left_dock.setMinimumWidth(160)
        self.addDockWidget(Qt.LeftDockWidgetArea, left_dock)
        self._left_dock = left_dock

        # Center: Chart panel
        self.setCentralWidget(self.chart_panel)

        # Bottom dock: Alert log
        bottom_dock = QDockWidget("Signals (信号日志)", self)
        bottom_dock.setWidget(self.alert_panel)
        bottom_dock.setFeatures(QDockWidget.DockWidgetMovable | QDockWidget.DockWidgetFloatable)
        bottom_dock.setMinimumHeight(100)
        self.addDockWidget(Qt.BottomDockWidgetArea, bottom_dock)
        self._bottom_dock = bottom_dock

        # Right dock: Config
        right_dock = QDockWidget("Config (分析配置)", self)
        right_dock.setWidget(self.config_panel)
        right_dock.setFeatures(QDockWidget.DockWidgetMovable | QDockWidget.DockWidgetFloatable)
        right_dock.setMinimumWidth(220)
        self.addDockWidget(Qt.RightDockWidgetArea, right_dock)
        self._right_dock = right_dock

        # B1 result panel dock (hidden by default)
        self.b1_panel = B1ResultPanel()
        b1_dock = QDockWidget("B1 Selection (B1选股)", self)
        b1_dock.setWidget(self.b1_panel)
        b1_dock.setFeatures(
            QDockWidget.DockWidgetMovable | QDockWidget.DockWidgetFloatable |
            QDockWidget.DockWidgetClosable
        )
        b1_dock.setVisible(False)
        self.addDockWidget(Qt.BottomDockWidgetArea, b1_dock)
        self.b1_dock = b1_dock

        # Toolbar (global stylesheet handles styling)
        toolbar = QToolBar("Controls")
        self.btn_start = QAction("Start (启动)", self)
        self.btn_start.triggered.connect(self._start_monitoring)
        toolbar.addAction(self.btn_start)

        self.btn_stop = QAction("Stop (停止)", self)
        self.btn_stop.triggered.connect(self._stop_monitoring)
        self.btn_stop.setEnabled(False)
        toolbar.addAction(self.btn_stop)

        toolbar.addSeparator()
        self.btn_add = QAction("Add Stock", self)
        self.btn_add.triggered.connect(self._add_stock_dialog)
        toolbar.addAction(self.btn_add)

        self.btn_clear = QAction("Clear", self)
        self.btn_clear.triggered.connect(self._clear_all)
        toolbar.addAction(self.btn_clear)

        toolbar.addSeparator()

        self.btn_b1 = QAction("B1 Scan", self)
        self.btn_b1.setCheckable(True)
        self.btn_b1.triggered.connect(self._toggle_b1_panel)
        toolbar.addAction(self.btn_b1)

        self.addToolBar(toolbar)

        # Status bar
        self.status_label = QLabel("Disconnected")
        self.status_label.setStyleSheet("color: #a6adc8;")
        self.statusBar().addPermanentWidget(self.status_label)

        # Dock proportions
        self.resizeDocks(
            [self._left_dock, self._right_dock],
            [180, 260],
            Qt.Horizontal,
        )
        self.resizeDocks(
            [self._top_dock, self._bottom_dock, self.b1_dock],
            [170, 120, 200],
            Qt.Vertical,
        )

    def _setup_workers(self):
        """Initialize background worker threads."""
        self.data_worker = L2DataWorker(self.config)
        self.analysis_worker = AnalysisWorker(self.config)
        self.b1_worker = B1Worker(data_dir="data")

    def _connect_signals(self):
        """Connect signals between workers and UI panels."""
        # Data worker signals
        self.data_worker.tick_data_ready.connect(self._on_tick_data)
        self.data_worker.orderbook_ready.connect(self._on_orderbook)
        self.data_worker.quotes_ready.connect(self._on_quotes)
        self.data_worker.status_update.connect(self._on_status)
        self.data_worker.connection_changed.connect(self._on_connection)

        # Analysis worker signals
        self.analysis_worker.analysis_complete.connect(self._on_analysis_complete)
        self.analysis_worker.signal_generated.connect(self._on_signal)
        self.analysis_worker.status_update.connect(self._on_status)

        # B1 worker signals
        self.b1_worker.progress_updated.connect(self._on_b1_progress)
        self.b1_worker.stock_processed.connect(self._on_b1_stock_result)
        self.b1_worker.scan_completed.connect(self._on_b1_scan_complete)
        self.b1_worker.status_update.connect(self._on_status)
        self.b1_worker.scan_error.connect(self._on_b1_error)

        # B1 panel signals
        self.b1_panel.start_scan_signal.connect(self._start_b1_scan)
        self.b1_panel.cancel_scan_signal.connect(self._cancel_b1_scan)
        self.b1_panel.add_to_monitor_requested.connect(self._add_b1_to_monitor)

        # Panel signals
        self.stock_selector.stock_selected.connect(self._on_stock_selected)
        self.stock_selector.stock_added.connect(self._on_stock_added)
        self.alert_panel.signal_clicked.connect(self._on_signal_navigate)
        self.config_panel.config_changed.connect(self._on_config_changed)

    # ---- Slots ----

    # B1 scan slots

    def _toggle_b1_panel(self, visible: bool):
        """Show/hide the B1 selection panel."""
        self.b1_dock.setVisible(visible)
        if visible:
            self.b1_dock.raise_()

    def _start_b1_scan(self):
        """Configure and start the B1 worker thread."""
        if self.b1_worker and self.b1_worker.isRunning():
            self.statusBar().showMessage("B1 scan already running", 3000)
            return
        params = self.b1_panel.get_scan_params()
        self.b1_worker.configure(**params)
        self.b1_panel.clear_results()
        self.b1_panel.set_scanning(True)
        self._b1_start_time = time.time()
        self.b1_worker.start()
        self.statusBar().showMessage("B1 scan started...")

    def _cancel_b1_scan(self):
        """Cancel a running B1 scan."""
        if self.b1_worker and self.b1_worker.isRunning():
            self.b1_worker.stop()
            self.b1_panel.set_scanning(False)
            self.b1_panel.set_status("Scan cancelled by user")
            self.statusBar().showMessage("B1 scan cancelled", 3000)

    def _on_b1_progress(self, current: int, total: int, code: str):
        """Update B1 progress bar."""
        self.b1_panel.update_progress(current, total, code)

    def _on_b1_stock_result(self, result: dict):
        """Add a single B1 result to the panel in real-time."""
        self.b1_panel.add_result(result)

    def _on_b1_scan_complete(self, results: list):
        """Handle B1 scan completion."""
        self.b1_panel.set_scanning(False)
        if results:
            elapsed = time.time() - getattr(self, "_b1_start_time", time.time())
            self.statusBar().showMessage(
                f"B1 scan complete: {len(results)} stocks found in {elapsed:.0f}s",
                15000,
            )
        else:
            self.statusBar().showMessage("B1 scan complete: no stocks matched", 5000)

    def _on_b1_error(self, msg: str):
        """Handle B1 scan error."""
        self.b1_panel.set_scanning(False)
        self.b1_panel.show_error(msg)
        self.statusBar().showMessage(f"B1 scan error: {msg}", 10000)

    def _add_b1_to_monitor(self, code: str, name: str, close: float):
        """Add a B1-selected stock to the L2 monitor panel."""
        if self.data_worker:
            self.data_worker.add_stock(code)
        self.monitor_panel.update_stock(code, {
            "code": code, "name": name, "price": close, "score": 80.0
        })
        self.stock_selector.add_stock(code, name)
        self.statusBar().showMessage(f"Added {code} {name} to L2 monitor", 3000)

    def _on_stock_added(self, code: str):
        """Handle stock added via selector."""
        if self.data_worker:
            self.data_worker.add_stock(code)
        self.monitor_panel.update_stock(code, {"code": code, "name": ""})

    # L2 data slots

    def _on_tick_data(self, stock_code: str, df):
        """Handle new tick data from data worker."""
        # Buffer ticks for this stock
        if stock_code not in self._tick_buffers:
            self._tick_buffers[stock_code] = []
        self._tick_buffers[stock_code].append(df)

        # Queue analysis
        self.analysis_worker.queue_analysis(stock_code, df)

        # Update chart if this is the selected stock
        if stock_code == self._current_stock:
            combined = pd.concat(self._tick_buffers[stock_code][-5:], ignore_index=True)
            features = self._latest_features.get(stock_code)
            self.chart_panel.tick_chart.update_ticks(combined, features)

        # Update monitor stats
        net_buy = df[df["direction"] == 1]["volume"].sum() - df[df["direction"] == -1]["volume"].sum()
        price = df["price"].iloc[-1] if len(df) > 0 else 0
        self.monitor_panel.update_stock(stock_code, {
            "price": float(price),
            "net_vol": int(net_buy) + (self._latest_features.get(stock_code, {}).get("net_buy_vol", 0) or 0),
        })

    def _on_orderbook(self, stock_code: str, ob: dict):
        """Handle new order book snapshot."""
        if stock_code not in self._ob_buffers:
            self._ob_buffers[stock_code] = []
        self._ob_buffers[stock_code].append(ob)
        # Keep last 20 snapshots
        if len(self._ob_buffers[stock_code]) > 20:
            self._ob_buffers[stock_code] = self._ob_buffers[stock_code][-20:]

        if stock_code == self._current_stock:
            self.chart_panel.depth_chart.update_depth(ob)
            features = self._latest_features.get(stock_code)
            if features:
                self.chart_panel.flow_chart.update_flow(features)

    def _on_quotes(self, df):
        """Handle batch quotes update."""
        for _, row in df.iterrows():
            code = str(row.get("code", row.get("market", "")))
            if not code:
                continue
            price = float(row.get("price", row.get("last", 0)) or 0)
            change = float(row.get("change", row.get("pctChg", 0)) or 0)
            name = str(row.get("name", ""))
            self.monitor_panel.update_stock(code, {
                "price": price, "chg%": change, "name": name
            })

    def _on_analysis_complete(self, stock_code: str, features: dict):
        """Handle completed analysis."""
        self._latest_features[stock_code] = features

        # Update monitor score
        score = 50 + (features.get("net_buy_vol", 0) or 0) / 10000
        score += (features.get("big_order_buy_ratio", 50) or 50) / 3
        score = max(0, min(100, score))
        self.monitor_panel.update_stock(stock_code, {"score": score})

        if stock_code == self._current_stock:
            self.chart_panel.flow_chart.update_flow(features)

    def _on_signal(self, signal):
        """Handle generated trading signal."""
        self.alert_panel.add_signal(signal)
        self.monitor_panel.update_signal_badge(signal.stock_code, signal.severity)

        # Critical signals: show in status bar
        if signal.severity in ("alert", "critical"):
            self.statusBar().showMessage(
                f"[{signal.severity.upper()}] {signal.stock_code}: {signal.title}", 10000
            )

    def _on_stock_selected(self, code: str):
        """Handle stock selection from selector panel."""
        if code and code != self._current_stock:
            self._current_stock = code
            # Update charts for newly selected stock
            ticks = self._tick_buffers.get(code, [])
            tick_df = pd.concat(ticks[-5:], ignore_index=True) if ticks else None
            obs = self._ob_buffers.get(code, [])
            ob = obs[-1] if obs else None
            features = self._latest_features.get(code)
            self.chart_panel.update_for_stock(code, tick_df, ob, features)

    def _on_signal_navigate(self, stock_code: str):
        """Navigate to stock when signal clicked."""
        self.stock_selector.select_stock(stock_code)
        self._on_stock_selected(stock_code)

    def _on_status(self, msg: str):
        """Handle status messages."""
        self.status_label.setText(msg)

    def _on_connection(self, ext_ok: bool, std_ok: bool):
        """Handle connection status changes."""
        if ext_ok:
            self.statusBar().showMessage("L2 Connected - Full depth + transactions available")
        elif std_ok:
            self.statusBar().showMessage("Standard Connected - Basic quotes only (5-level)")
        else:
            self.statusBar().showMessage("Not connected - Check TDX server")

    def _on_config_changed(self, config: dict):
        """Handle config changes from config panel."""
        # Update L2Config with new values
        if "big_order_pct" in config:
            self.config.BIG_ORDER_PERCENTILE = config["big_order_pct"]
        if "wall_ratio" in config:
            self.config.ORDER_WALL_THRESHOLD_RATIO = config["wall_ratio"]
        if "anomaly_zscore" in config:
            self.config.ANOMALY_ZSCORE_THRESHOLD = config["anomaly_zscore"]
        self.statusBar().showMessage("Configuration applied", 3000)

    # ---- Actions ----

    def _start_monitoring(self):
        """Start data collection and analysis."""
        watchlist = self.stock_selector.get_codes()
        if not watchlist:
            self.statusBar().showMessage("Add stocks to watchlist first", 5000)
            return

        self.data_worker.set_watchlist(watchlist)
        self.data_worker.start()
        self.analysis_worker.start()

        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.statusBar().showMessage(f"Monitoring {len(watchlist)} stocks...")

    def _stop_monitoring(self):
        """Stop data collection and analysis."""
        if self.data_worker:
            self.data_worker.stop()
        if self.analysis_worker:
            self.analysis_worker.stop()
        if self.b1_worker and self.b1_worker.isRunning():
            self.b1_worker.stop()

        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.statusBar().showMessage("Monitoring stopped")

    def _add_stock_dialog(self):
        """Simple input dialog to add a stock."""
        from PyQt5.QtWidgets import QInputDialog
        code, ok = QInputDialog.getText(
            self, "Add Stock", "Enter stock code (e.g. 600366):"
        )
        if ok and code.strip():
            code = code.strip().zfill(6)
            self.data_worker.add_stock(code)
            self.monitor_panel.update_stock(code, {"code": code, "name": ""})
            self.stock_selector.add_stock(code)
            self.statusBar().showMessage(f"Added {code} to watchlist", 3000)

    def _clear_all(self):
        """Clear all data."""
        self._tick_buffers.clear()
        self._ob_buffers.clear()
        self._latest_features.clear()
        self._current_stock = ""
        self.monitor_panel.model.clear()
        self.stock_selector.list_widget.clear()
        self.alert_panel.clear_signals()
        self.chart_panel.clear_data()
        self.statusBar().showMessage("All data cleared")

    # ---- Lifecycle ----

    def closeEvent(self, event):
        """Clean shutdown on window close."""
        self._stop_monitoring()
        event.accept()
