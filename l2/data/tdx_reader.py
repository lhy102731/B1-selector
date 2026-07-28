"""TDX local file reader via mootdx ExtReader.

Reads L2 data from local TDX vipdoc directory:
  - .lc1 files: L2 daily data (逐笔成交汇总)
  - .lc5 files: L2 5-minute / fzline data
  - .dat files: standard daily data

Uses mootdx.reader.ExtReader internally.
"""

import os
from pathlib import Path
from functools import lru_cache

import pandas as pd

from l2.data.config import L2Config


class TDXLocalReader:
    """Read TDX local L2 data files via mootdx ExtReader.

    File locations (under TDX_DIR/vipdoc/):
      sh/lday/  -> sh600366.day (standard daily), sh600366.lc1 (L2 daily)
      sz/lday/  -> sz000001.day, sz000001.lc1
      sh/fzline/ -> sh600366.lc5 (L2 5-minute)
      sz/fzline/ -> sz000001.lc5
    """

    def __init__(self, config: L2Config | None = None, tdx_dir: str | None = None):
        self.config = config or L2Config()
        self.tdxdir = Path(tdx_dir or self.config.TDX_DIR)
        self._ext_reader = None
        self._std_reader = None

    @property
    def ext_reader(self):
        if self._ext_reader is None:
            from mootdx.reader import Reader
            self._ext_reader = Reader.factory(market="ext", tdxdir=str(self.tdxdir))
        return self._ext_reader

    @property
    def std_reader(self):
        if self._std_reader is None:
            from mootdx.reader import Reader
            self._std_reader = Reader.factory(market="std", tdxdir=str(self.tdxdir))
        return self._std_reader

    @property
    def is_available(self) -> bool:
        """Check if TDX local data directory exists and is accessible."""
        return self.tdxdir.exists() and (self.tdxdir / "vipdoc").exists()

    def _resolve_symbol(self, code: str) -> tuple[str, str]:
        """Resolve stock code to market prefix and TDX file symbol.

        Returns (market, tdx_symbol) e.g., ('sh', 'sh600366') or ('sz', 'sz000001').
        """
        if code.startswith("sh") or code.startswith("sz"):
            market = code[:2]
            return market, code
        if code.startswith("6") or code.startswith("68"):
            return "sh", f"sh{code}"
        return "sz", f"sz{code}"

    # ---- L2 Daily Data (.lc1) ----

    def get_l2_daily(self, code: str) -> pd.DataFrame:
        """Read L2 daily data from .lc1 file.

        Returns DataFrame with L2 daily aggregated data.
        The exact columns depend on TDX version.
        """
        if not self.is_available:
            raise FileNotFoundError(f"TDX directory not found: {self.tdxdir}")
        market, symbol = self._resolve_symbol(code)
        return self.ext_reader.daily(symbol=symbol)

    # ---- L2 Minute Data ----

    def get_l2_minute(self, code: str) -> pd.DataFrame:
        """Read L2 1-minute data from local file.

        Returns DataFrame with minute-level L2 data.
        """
        if not self.is_available:
            raise FileNotFoundError(f"TDX directory not found: {self.tdxdir}")
        market, symbol = self._resolve_symbol(code)
        return self.ext_reader.minute(symbol=symbol)

    # ---- L2 FZLine Data (.lc5) ----

    def get_l2_fzline(self, code: str) -> pd.DataFrame:
        """Read L2 5-minute data from .lc5 fzline file.

        Returns DataFrame with 5-minute aggregated L2 stats.
        """
        if not self.is_available:
            raise FileNotFoundError(f"TDX directory not found: {self.tdxdir}")
        market, symbol = self._resolve_symbol(code)
        return self.ext_reader.fzline(symbol=symbol)

    # ---- Standard Daily Data ----

    def get_std_daily(self, code: str) -> pd.DataFrame:
        """Read standard daily data (non-L2) from .day file."""
        if not self.is_available:
            raise FileNotFoundError(f"TDX directory not found: {self.tdxdir}")
        market, symbol = self._resolve_symbol(code)
        return self.std_reader.daily(symbol=symbol)

    # ---- Stock discovery ----

    @lru_cache(maxsize=1)
    def list_local_stocks(self, market: str = "all") -> list[str]:
        """Scan vipdoc for stocks with L2 data (.lc1 files).

        Args:
            market: 'sh', 'sz', or 'all'
        """
        if not self.is_available:
            return []
        stocks = []
        vipdoc = self.tdxdir / "vipdoc"
        for mkt in (["sh", "sz"] if market == "all" else [market]):
            lday_dir = vipdoc / mkt / "lday"
            if lday_dir.exists():
                for f in lday_dir.glob(f"{mkt}*.lc1"):
                    code = f.stem.replace(mkt, "")
                    if code.isdigit() and len(code) == 6:
                        stocks.append(code)
        return sorted(stocks)

    @lru_cache(maxsize=1)
    def list_all_local_stocks(self) -> list[str]:
        """Scan vipdoc for all stocks with any TDX data files."""
        if not self.is_available:
            return []
        stocks = set()
        vipdoc = self.tdxdir / "vipdoc"
        for mkt in ["sh", "sz"]:
            lday_dir = vipdoc / mkt / "lday"
            if lday_dir.exists():
                for f in lday_dir.glob(f"{mkt}*"):
                    if f.suffix in (".day", ".lc1"):
                        code = f.stem.replace(mkt, "")
                        if code.isdigit() and len(code) == 6:
                            stocks.add(code)
        return sorted(stocks)
