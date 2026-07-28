"""L2 data collection worker thread.

Polls TDX L2 server for real-time data via L2DataCollector.
Runs in a background QThread; emits data via pyqtSignal.
"""

import time
import logging
from datetime import datetime

from PyQt5.QtCore import QThread, pyqtSignal

from l2.data.config import L2Config
from l2.data.collector import L2DataCollector

logger = logging.getLogger(__name__)


class L2DataWorker(QThread):
    """Background worker for polling L2 data.

    Polls TDX server at configurable intervals for:
      - Tick-by-tick data (every TICK_POLL_INTERVAL_MS)
      - Order book snapshots (every ORDERBOOK_POLL_INTERVAL_MS)
      - Market quotes (every QUOTES_POLL_INTERVAL_MS)

    Emits data to main thread via signals.
    """

    tick_data_ready = pyqtSignal(str, object)        # stock_code, DataFrame
    orderbook_ready = pyqtSignal(str, object)        # stock_code, dict
    quotes_ready = pyqtSignal(object)                # DataFrame (all stocks)
    status_update = pyqtSignal(str)                  # status message
    connection_changed = pyqtSignal(bool, bool)      # ext_connected, std_connected

    def __init__(self, config: L2Config | None = None):
        super().__init__()
        self.config = config or L2Config()
        self.collector: L2DataCollector | None = None
        self._running = False
        self._watchlist: list[str] = []
        self._last_tick_positions: dict[str, int] = {}

    def set_watchlist(self, codes: list[str]):
        """Update the stock watchlist for polling."""
        self._watchlist = codes
        for code in codes:
            if code not in self._last_tick_positions:
                self._last_tick_positions[code] = 0

    def add_stock(self, code: str):
        """Add a single stock to the watchlist."""
        if code not in self._watchlist:
            self._watchlist.append(code)
            self._last_tick_positions[code] = 0

    def remove_stock(self, code: str):
        """Remove a stock from the watchlist."""
        if code in self._watchlist:
            self._watchlist.remove(code)
            self._last_tick_positions.pop(code, None)

    def run(self):
        """Main polling loop."""
        self._init_collector()

        # Report initial connection status
        status = self.collector.get_connection_status()
        self.connection_changed.emit(status["l2_connected"], status["std_connected"])
        self.status_update.emit(
            f"Connected (L2={'yes' if status['has_l2'] else 'no'}, "
            f"depth={status['l2_levels']} levels)"
        )

        self._running = True
        last_tick_poll = 0
        last_ob_poll = 0
        last_quotes_poll = 0

        while self._running:
            now = time.time() * 1000  # ms

            # Poll tick data
            if now - last_tick_poll >= self.config.TICK_POLL_INTERVAL_MS:
                self._poll_ticks()
                last_tick_poll = now

            # Poll order book
            if now - last_ob_poll >= self.config.ORDERBOOK_POLL_INTERVAL_MS:
                self._poll_orderbooks()
                last_ob_poll = now

            # Poll quotes
            if now - last_quotes_poll >= self.config.QUOTES_POLL_INTERVAL_MS:
                self._poll_quotes()
                last_quotes_poll = now

            self.msleep(200)  # Small sleep to prevent CPU spin

    def _init_collector(self):
        """Initialize the L2 data collector."""
        try:
            self.collector = L2DataCollector(config=self.config)
        except Exception as e:
            logger.error(f"Failed to initialize collector: {e}")
            self.status_update.emit(f"Connection error: {e}")
            self.collector = L2DataCollector(config=self.config)

    def _poll_ticks(self):
        """Poll for new tick data."""
        if not self.collector or not self.collector.has_l2:
            return

        for code in self._watchlist:
            try:
                start = self._last_tick_positions.get(code, 0)
                df = self.collector.get_realtime_transactions(code, start, 800)
                if df is not None and not df.empty:
                    self._last_tick_positions[code] = start + len(df)
                    self.tick_data_ready.emit(code, df)
            except Exception as e:
                logger.debug(f"Tick poll error for {code}: {e}")

    def _poll_orderbooks(self):
        """Poll for order book snapshots."""
        if not self.collector:
            return

        for code in self._watchlist:
            try:
                ob = self.collector.get_order_book_snapshot(code)
                if ob:
                    self.orderbook_ready.emit(code, ob)
            except Exception as e:
                logger.debug(f"Orderbook poll error for {code}: {e}")

    def _poll_quotes(self):
        """Poll for market quotes."""
        if not self.collector or not self._watchlist:
            return

        try:
            df = self.collector.get_realtime_quotes(self._watchlist)
            if df is not None and not df.empty:
                self.quotes_ready.emit(df)
        except Exception as e:
            logger.debug(f"Quotes poll error: {e}")

    def stop(self):
        """Stop the worker thread."""
        self._running = False
        self.wait(3000)
        if self.collector:
            try:
                self.collector.close()
            except Exception:
                pass

    def restart_collector(self):
        """Restart the collector (after config change)."""
        if self.collector:
            try:
                self.collector.close()
            except Exception:
                pass
        self._init_collector()
        for code in self._watchlist:
            self._last_tick_positions[code] = 0
