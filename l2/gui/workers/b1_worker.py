"""B1 stock selection worker thread.

Runs the full B1 selection pipeline in the background:
  CSVManager → UnifiedB1Strategy → StockScorer → emit results

Mirrors the QThread + pyqtSignal pattern of L2DataWorker/AnalysisWorker.
"""

import sys
import json
import logging
from pathlib import Path

from PyQt5.QtCore import QThread, pyqtSignal

logger = logging.getLogger(__name__)


class B1Worker(QThread):
    """Background worker for B1 stock selection scanning."""

    progress_updated = pyqtSignal(int, int, str)   # current, total, stock_code
    stock_processed = pyqtSignal(dict)             # single B1 result dict
    scan_completed = pyqtSignal(list)              # all results (sorted)
    status_update = pyqtSignal(str)                # status message
    scan_error = pyqtSignal(str)                   # fatal error message

    def __init__(self, data_dir: str = "data"):
        super().__init__()
        self._data_dir = data_dir
        self._cancelled = False
        self._max_stocks = None
        self._min_similarity = 60.0
        self._lookback_days = 35

    def configure(self, max_stocks=None, min_similarity=60.0, lookback_days=35):
        """Set scan parameters before starting."""
        self._max_stocks = max_stocks
        self._min_similarity = min_similarity
        self._lookback_days = lookback_days

    def run(self):
        """Main B1 scanning loop."""
        self._cancelled = False

        try:
            self._ensure_project_path()
            self._run_scan()
        except Exception as e:
            logger.exception("B1 scan crashed")
            self.scan_error.emit(str(e))

    def _ensure_project_path(self):
        """Add project root to sys.path so strategy/utils imports work."""
        project_root = str(Path(__file__).parent.parent.parent.parent)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)

    def _run_scan(self):
        """Execute the full B1 selection pipeline."""
        from utils.csv_manager import CSVManager
        from strategy.strategy_registry import get_registry
        from utils.stock_scorer import StockScorer

        # ---- Phase 1: Load infrastructure ----
        self.status_update.emit("Loading stock list...")
        csv_manager = CSVManager(self._data_dir)
        stock_codes = csv_manager.list_all_stocks()

        if self._max_stocks:
            stock_codes = stock_codes[:self._max_stocks]

        total = len(stock_codes)
        self.status_update.emit(f"Scanning {total} stocks...")

        # Load stock names
        stock_names = self._load_stock_names()

        # Load history bonus
        bonus_lookup = self._load_bonus_lookup()

        # Initialize strategy
        registry = get_registry("config/strategy_params.yaml")
        registry.auto_register_from_directory("strategy")
        strategy = registry.get_strategy("UnifiedB1Strategy")
        if not strategy:
            from strategy.unified_b1_strategy import UnifiedB1Strategy
            strategy = registry.register(UnifiedB1Strategy, name="UnifiedB1Strategy")

        # Initialize scorer (expensive: loads 27 perfect cases + cache)
        self.status_update.emit("Initializing pattern scorer...")
        scorer = StockScorer(csv_manager, registry)

        # ---- Phase 2: Scan all stocks ----
        selected = []
        filtered_count = 0

        for i, code in enumerate(stock_codes, 1):
            if self._cancelled:
                self.status_update.emit("Scan cancelled by user")
                return

            df = csv_manager.read_stock(code)
            name = stock_names.get(code, "")

            # Pre-filters
            if df.empty or len(df) < 60:
                filtered_count += 1
                continue
            if any(kw in name for kw in ["退", "ST", "*ST"]):
                filtered_count += 1
                continue

            try:
                df_indicators = strategy.calculate_indicators(df)
                signals = strategy.select_stocks(df_indicators, name)

                if not signals:
                    continue

                signal = signals[0]
                surge = signal.get("surge_start_date")
                score_info = scorer.score_stock(
                    code, df_indicators, self._lookback_days, start_date=surge
                )

                # Rank penalty
                rank_penalty = 3 if signal.get("max_high_vol_rank") == 2 else 0

                # History bonus
                hist_bonus = bonus_lookup.get(code, 0)

                b1_score = score_info["b1_score"] - rank_penalty + hist_bonus

                if b1_score >= self._min_similarity:
                    result = {
                        "code": code,
                        "name": name,
                        "b1_score": round(b1_score, 1),
                        "matched_case": score_info["matched_case"],
                        "matched_date": score_info["matched_date"],
                        "breakdown": score_info["breakdown"],
                        "tags": score_info.get("tags", []),
                        "is_washout": signal.get("is_washout", False),
                        "is_super_b1": signal.get("is_super_b1", False),
                        "build_gain": signal.get("build_gain", 0),
                        "surge_turnover": signal.get("surge_turnover", 0),
                        "close": signal["close"],
                        "J": round(signal["J"], 1),
                        "max_high_vol_rank": signal.get("max_high_vol_rank", 0),
                        "vol_resonance_score": signal.get("vol_resonance_score", 0),
                        "hist_bonus": hist_bonus,
                        "reasons": signal["reasons"],
                    }
                    selected.append(result)
                    self.stock_processed.emit(result)

            except Exception as e:
                logger.debug(f"Error processing {code}: {e}")
                continue

            if i % 100 == 0:
                self.progress_updated.emit(i, total, code)
                self.status_update.emit(
                    f"Processed {i}/{total}, found {len(selected)}"
                )

        # ---- Phase 3: Finalize ----
        selected.sort(key=lambda x: x["b1_score"], reverse=True)
        self.progress_updated.emit(total, total, "")
        self.status_update.emit(
            f"B1 scan complete: {len(selected)} selected from {total} stocks "
            f"({filtered_count} filtered by data/ST)"
        )
        self.scan_completed.emit(selected)

    def _load_stock_names(self) -> dict:
        """Load stock name mapping from cache file."""
        names_file = Path(self._data_dir) / "stock_names.json"
        if names_file.exists():
            try:
                with open(names_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _load_bonus_lookup(self) -> dict:
        """Load history fitness bonus lookup."""
        bonus_file = (
            Path(self._data_dir) / "stock_scoring" / "bonus_lookup.json"
        )
        if bonus_file.exists():
            try:
                with open(bonus_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def stop(self):
        """Request cancellation and wait for thread to finish."""
        self._cancelled = True
        self.wait(5000)
