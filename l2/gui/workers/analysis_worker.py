"""L2 analysis worker thread.

Runs tick analysis + signal detection in a background QThread.
Emits results via pyqtSignal to the GUI main thread.
"""

import queue
import logging

import pandas as pd
from PyQt5.QtCore import QThread, pyqtSignal

from l2.data.config import L2Config
from l2.analysis.tick_analyzer import TickAnalyzer
from l2.analysis.orderbook_analyzer import OrderBookAnalyzer
from l2.analysis.signal_engine import SignalEngine, Signal
from l2.analysis.features import FeatureCache

logger = logging.getLogger(__name__)


class AnalysisWorker(QThread):
    """Background worker for L2 analysis pipeline.

    Consumes raw tick data from a queue, runs the full analysis
    pipeline, and emits results back to the main thread.
    """

    analysis_complete = pyqtSignal(str, dict)   # stock_code, features
    signal_generated = pyqtSignal(object)       # Signal
    status_update = pyqtSignal(str)             # status message

    def __init__(self, config: L2Config | None = None):
        super().__init__()
        self.config = config or L2Config()
        self.tick_analyzer = TickAnalyzer(self.config)
        self.ob_analyzer = OrderBookAnalyzer(self.config)
        self.signal_engine = SignalEngine(self.config)
        self.feature_cache = FeatureCache(self.config)

        self._queue: queue.Queue = queue.Queue()
        self._feature_history: dict[str, list[dict]] = {}
        self._running = False
        self._max_history = 5

    def queue_analysis(
        self,
        stock_code: str,
        ticks_df: pd.DataFrame,
        orderbooks: list[dict] | None = None,
    ):
        """Add an analysis job to the queue."""
        self._queue.put((stock_code, ticks_df, orderbooks or []))

    def run(self):
        """Main loop - process queued analysis jobs."""
        self._running = True
        while self._running:
            try:
                stock_code, ticks_df, orderbooks = self._queue.get(timeout=1)
            except queue.Empty:
                continue

            if ticks_df is None or ticks_df.empty:
                continue

            try:
                # Compute features
                today = pd.Timestamp.now().strftime("%Y-%m-%d")
                features = self.tick_analyzer.compute_full_features(
                    ticks_df, stock_code, today
                )

                # Get historical features for trend context
                history = self._feature_history.get(stock_code, [])
                if not history:
                    history = self.feature_cache.get_history(stock_code, self._max_history)

                # Order book features
                ob_features = {}
                if orderbooks:
                    ob_features = self.ob_analyzer.compute_full_features(orderbooks)

                # Merge features
                full_features = {**features, **ob_features}

                # Detect signals
                signals = self.signal_engine.analyze_all(
                    stock_code, ticks_df, orderbooks, full_features, history
                )

                # Emit results
                self.analysis_complete.emit(stock_code, full_features)
                for signal in signals:
                    self.signal_generated.emit(signal)

                # Update feature history (in-memory)
                if stock_code not in self._feature_history:
                    self._feature_history[stock_code] = []
                self._feature_history[stock_code].append(full_features)
                if len(self._feature_history[stock_code]) > self._max_history:
                    self._feature_history[stock_code] = self._feature_history[stock_code][-self._max_history:]

                # Persist to cache
                self.feature_cache.save(stock_code, today, full_features)

            except Exception as e:
                logger.error(f"Analysis error for {stock_code}: {e}")
                self.status_update.emit(f"Analysis error: {e}")

    def get_feature_history(self, stock_code: str) -> list[dict]:
        """Get cached feature history for a stock."""
        return self._feature_history.get(stock_code, [])

    def stop(self):
        """Stop the worker thread."""
        self._running = False
        self.wait(5000)
