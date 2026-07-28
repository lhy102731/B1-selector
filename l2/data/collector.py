"""L2 data collector via tdxpy (primary) + mootdx std (fallback).

L2 connection: tdxpy.TdxExHq_API -> port 7727 (DSHOST from TDX connect.cfg)
Std fallback:    mootdx Quotes(market='std') -> port 7709 (HQHOST)

Local files:     tdxpy.TdxLCMinBarReader -> .lc1 L2 minute bars
                 mootdx Reader(market='std') -> .day daily bars

Primary data sources (L2 via tdxpy):
  - get_transaction_data(market, code)       -> real-time tick-by-tick
  - get_history_transaction_data(m, code, d) -> historical tick-by-tick
  - get_instrument_quote(market, code)       -> real-time L2 quote (10-level)
  - get_minute_time_data(market, code)        -> real-time minute K-line
"""

import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

from l2.data.config import L2Config
from l2.data.tdx_reader import TDXLocalReader

logger = logging.getLogger(__name__)

# L2 server port (from TDX connect.cfg [DSHOST] section)
L2_PORT = 7727


def discover_l2_server() -> tuple[str, str, int] | None:
    """Dynamically discover the L2 server from running TDX client.

    Finds tdxw.exe's network connections and returns the one
    connected to port 7727 (L2 ext market).

    Returns (name, ip, port) or None if TDX is not running.
    """
    try:
        # Find TDX process
        result = subprocess.run(
            ['tasklist', '/FI', 'IMAGENAME eq tdxw.exe', '/FO', 'CSV'],
            capture_output=True, text=True, timeout=5
        )
        pids = []
        for line in result.stdout.strip().split('\n'):
            parts = line.split(',')
            if len(parts) >= 2:
                try:
                    pids.append(int(parts[1].strip('"')))
                except ValueError:
                    pass

        if not pids:
            return None

        # Find L2 connection (port 7727)
        netstat = subprocess.run(['netstat', '-ano'], capture_output=True, text=True, timeout=5)
        for pid in pids:
            for line in netstat.stdout.split('\n'):
                if str(pid) not in line:
                    continue
                if 'ESTABLISHED' not in line:
                    continue
                parts = line.split()
                if len(parts) < 3:
                    continue
                remote = parts[2]  # e.g., 112.74.214.43:7727
                if f':{L2_PORT}' in remote:
                    ip = remote.split(':')[0]
                    return (f"TDX-L2-{ip}", ip, L2_PORT)
    except Exception as e:
        logger.debug(f"L2 server discovery failed: {e}")

    return None


def discover_l2_servers_from_config(tdx_dir: str = "D:/TDX") -> list[tuple[str, str, int]]:
    """Parse L2 servers from TDX connect.cfg [DSHOST] section as fallback."""
    cfg_path = Path(tdx_dir) / "connect.cfg"
    if not cfg_path.exists():
        return []

    try:
        text = cfg_path.read_bytes().decode("gbk", errors="replace")
    except Exception:
        return []

    servers = []
    in_dshost = False
    for line in text.split('\n'):
        line = line.strip()
        if line.startswith('[DSHOST]'):
            in_dshost = True
            continue
        if line.startswith('[') and in_dshost:
            break
        if in_dshost and line.startswith('IPAddress'):
            # e.g., IPAddress01=112.74.214.43
            idx = line.split('=')[0].replace('IPAddress', '')
            ip = line.split('=')[1]
            # Find corresponding port
            servers.append((f"cfg-dshost-{idx}", ip, L2_PORT))

    return servers


class L2DataCollector:
    """TDX L2 data collector.

    Uses tdxpy for L2 (ext market, port 7727) as primary backend.
    Falls back to mootdx std (port 7709) for basic quotes.
    Uses local TDX files for offline data.

    Market codes: 1=Shanghai, 0=Shenzhen

    Usage:
        collector = L2DataCollector()
        collector.connect()
        ticks = collector.get_historical_transactions('600366', '20260522')
        quote = collector.get_l2_quote('600366')
        minute = collector.get_minute_data('600366')
    """

    MARKET_SH = 1
    MARKET_SZ = 0

    def __init__(self, config: L2Config | None = None, tdx_dir: str | None = None):
        self.config = config or L2Config()
        self._api = None           # tdxpy TdxExHq_API (L2)
        self._std_quotes = None    # mootdx StdQuotes (fallback)
        self._local_reader = None
        self._lc_reader = None     # TdxLCMinBarReader
        self._connected_l2 = False
        self._connected_std = False

    # ---- Connection ----

    @property
    def api(self):
        """Lazy-init tdxpy L2 connection."""
        if self._api is None:
            self.connect()
        return self._api

    @property
    def std(self):
        """Lazy-init mootdx std connection."""
        if self._std_quotes is None:
            self._connect_std()
        return self._std_quotes

    def connect(self) -> bool:
        """Connect to L2 server via tdxpy.

        Discovers the active L2 server dynamically:
          1. Check running TDX process (tdxw.exe) for active L2 connection
          2. Fall back to TDX connect.cfg [DSHOST] servers
          3. Fall back to user-configured CUSTOM_EX_HOSTS

        Returns True on success.
        """
        from tdxpy.exhq import TdxExHq_API

        self._api = TdxExHq_API()

        # Priority 1: Discover from running TDX process
        live_server = discover_l2_server()
        if live_server:
            name, ip, port = live_server
            try:
                self._api.connect(ip, port)
                self._connected_l2 = True
                logger.info(f"L2 connected via TDX: {name} ({ip}:{port})")
                return True
            except Exception as e:
                logger.debug(f"L2 via TDX {ip}:{port} failed: {e}")

        # Priority 2: Parse TDX connect.cfg
        cfg_servers = discover_l2_servers_from_config(str(self.config.TDX_DIR))
        for name, ip, port in cfg_servers:
            try:
                self._api.connect(ip, port)
                self._connected_l2 = True
                logger.info(f"L2 connected via cfg: {name} ({ip}:{port})")
                return True
            except Exception as e:
                logger.debug(f"L2 {ip}:{port} from cfg failed: {e}")

        # Priority 3: User-configured custom hosts
        if self.config.CUSTOM_EX_HOSTS:
            for name, ip, port in self.config.CUSTOM_EX_HOSTS:
                try:
                    self._api.connect(ip, port)
                    self._connected_l2 = True
                    logger.info(f"L2 connected via custom: {name} ({ip}:{port})")
                    return True
                except Exception as e:
                    logger.debug(f"L2 custom {ip}:{port} failed: {e}")

        logger.warning("All L2 servers failed, L2 features unavailable")
        self._connected_l2 = False
        return False

    def _connect_std(self):
        """Establish mootdx std connection as fallback."""
        from mootdx.quotes import Quotes
        try:
            self._std_quotes = Quotes.factory(market="std", best=True, timeout=10)
            self._connected_std = True
        except Exception as e:
            logger.warning(f"Std connection failed: {e}")
            self._connected_std = False

    @property
    def has_l2(self) -> bool:
        if self._api is None:
            self.connect()
        return self._connected_l2

    @property
    def is_connected(self) -> bool:
        return self._connected_l2 or self._connected_std

    def close(self):
        for obj in [self._api, self._std_quotes]:
            if obj:
                try:
                    obj.disconnect() if hasattr(obj, 'disconnect') else obj.close()
                except Exception:
                    pass
        self._api = None
        self._std_quotes = None
        self._connected_l2 = False
        self._connected_std = False

    def _market(self, code: str) -> int:
        return self.MARKET_SH if (code.startswith("6") or code.startswith("68")) else self.MARKET_SZ

    # ---- L2 Transaction Data (逐笔成交) ----

    def get_historical_transactions(
        self, symbol: str, date: str, start: int = 0, count: int = 800
    ) -> pd.DataFrame:
        """Get historical tick-by-tick transactions via tdxpy L2.

        Args:
            symbol: Stock code e.g. '600366'
            date: Trading date '20260522' or '2026-05-22'
            start: Starting position
            count: Number of ticks (max 1800 per call)

        Returns DataFrame with tick data.
        """
        if not self.has_l2:
            logger.warning("L2 not connected, returning empty")
            return pd.DataFrame()

        date_str = date.replace("-", "")
        market = self._market(symbol)

        try:
            data = self.api.get_history_transaction_data(
                market, symbol, date_str, start=start, count=count
            )
            if data is None or (hasattr(data, '__len__') and len(data) == 0):
                return pd.DataFrame()
            df = self.api.to_df(data)
            return self._normalize_transactions(df, symbol)
        except Exception as e:
            logger.error(f"Historical transactions failed for {symbol}: {e}")
            return pd.DataFrame()

    def get_realtime_transactions(
        self, symbol: str, start: int = 0, count: int = 800
    ) -> pd.DataFrame:
        """Get real-time tick-by-tick transactions (current trading day)."""
        if not self.has_l2:
            return pd.DataFrame()

        market = self._market(symbol)
        try:
            data = self.api.get_transaction_data(market, symbol, start=start, count=count)
            if data is None or (hasattr(data, '__len__') and len(data) == 0):
                return pd.DataFrame()
            df = self.api.to_df(data)
            return self._normalize_transactions(df, symbol)
        except Exception as e:
            logger.debug(f"Realtime transactions failed for {symbol}: {e}")
            return pd.DataFrame()

    def get_historical_transactions_full(self, symbol: str, date: str) -> pd.DataFrame:
        """Fetch ALL tick-by-tick transactions via pagination."""
        if not self.has_l2:
            return pd.DataFrame()

        date_str = date.replace("-", "")
        market = self._market(symbol)
        all_ticks = []
        start = 0
        batch = 1800  # tdxpy max

        while True:
            try:
                data = self.api.get_history_transaction_data(
                    market, symbol, date_str, start=start, count=batch
                )
            except Exception as e:
                logger.warning(f"Error at offset {start}: {e}")
                break
            if data is None or len(data) == 0:
                break
            df = self.api.to_df(data)
            df = self._normalize_transactions(df, symbol)
            all_ticks.append(df)
            if len(df) < batch:
                break
            start += batch

        if not all_ticks:
            return pd.DataFrame()
        result = pd.concat(all_ticks, ignore_index=True)
        return result.drop_duplicates().sort_values("time").reset_index(drop=True)

    # ---- L2 Quotes (十档盘口) ----

    def get_l2_quote(self, symbol: str) -> dict | None:
        """Get real-time L2 quote with 10-level depth via tdxpy."""
        if not self.has_l2:
            return None

        market = self._market(symbol)
        try:
            data = self.api.get_instrument_quote(market, symbol)
            if data is None:
                return None
            if hasattr(self.api, 'to_df'):
                df = self.api.to_df(data)
                if df is not None and not df.empty:
                    return self._parse_quote_df(df, symbol)
            return self._parse_quote_raw(data, symbol)
        except Exception as e:
            logger.debug(f"L2 quote failed for {symbol}: {e}")
            return None

    def get_l2_quote_batch(self, symbols: list[str]) -> pd.DataFrame:
        """Get L2 quotes for multiple stocks."""
        if not self.has_l2:
            return self._fallback_quotes(symbols)

        results = []
        for sym in symbols:
            q = self.get_l2_quote(sym)
            if q:
                results.append(q)
        if results:
            return pd.DataFrame(results)
        return pd.DataFrame()

    def get_realtime_quotes(self, symbols: list[str]) -> pd.DataFrame:
        """Get real-time quotes for multiple stocks. L2 first, std fallback."""
        if self.has_l2:
            df = self.get_l2_quote_batch(symbols)
            if not df.empty:
                return df
        return self._fallback_quotes(symbols)

    def get_order_book_snapshot(self, symbol: str) -> dict | None:
        """Get L2 order book depth. Uses L2 if available, falls back to std 5-level."""
        if self.has_l2:
            ob = self.get_l2_quote(symbol)
            if ob:
                return ob
        return self._get_std_orderbook(symbol)

    # ---- Minute Data ----

    def get_minute_data(self, symbol: str) -> pd.DataFrame:
        """Get real-time minute data via L2."""
        if not self.has_l2:
            return pd.DataFrame()
        market = self._market(symbol)
        try:
            data = self.api.get_minute_time_data(market, symbol)
            if data is not None and len(data) > 0:
                return self.api.to_df(data)
        except Exception:
            pass
        return pd.DataFrame()

    def get_history_minute_data(self, symbol: str, date: str) -> pd.DataFrame:
        """Get historical minute data via L2."""
        if not self.has_l2:
            return pd.DataFrame()
        market = self._market(symbol)
        date_str = date.replace("-", "")
        try:
            data = self.api.get_history_minute_time_data(market, symbol, date_str)
            if data is not None and len(data) > 0:
                return self.api.to_df(data)
        except Exception:
            pass
        return pd.DataFrame()

    # ---- Local L2 Minute Data (.lc1 files) ----

    @property
    def lc_reader(self):
        """Lazy-init TdxLCMinBarReader for local .lc1 files."""
        if self._lc_reader is None:
            from tdxpy.reader.lc_min_bar_reader import TdxLCMinBarReader
            self._lc_reader = TdxLCMinBarReader(str(self.config.TDX_DIR))
        return self._lc_reader

    def get_local_l2_minute(self, symbol: str) -> pd.DataFrame:
        """Read local .lc1 L2 minute bar file.

        File location: {TDX_DIR}/vipdoc/{sh|sz}/minline/{sh|sz}{code}.lc1
        """
        market = "sh" if self._market(symbol) == self.MARKET_SH else "sz"
        tdx_dir = Path(self.config.TDX_DIR)
        lc1_path = tdx_dir / "vipdoc" / market / "minline" / f"{market}{symbol}.lc1"

        if not lc1_path.exists():
            logger.warning(f"Local .lc1 not found: {lc1_path}")
            return pd.DataFrame()

        try:
            return self.lc_reader.get_df(str(lc1_path))
        except Exception as e:
            logger.error(f"Failed to read {lc1_path}: {e}")
            return pd.DataFrame()

    # ---- Fallback: Std Market Quotes ----

    def _fallback_quotes(self, symbols: list[str]) -> pd.DataFrame:
        """Get quotes via mootdx std (5-level)."""
        if not self._connected_std:
            self._connect_std()
        if not self._connected_std:
            return pd.DataFrame()
        try:
            return self.std.quotes(symbol=symbols)
        except Exception:
            return pd.DataFrame()

    def _get_std_orderbook(self, symbol: str) -> dict | None:
        """Build order book from std (5-level) quote."""
        df = self._fallback_quotes([symbol])
        if df is None or df.empty:
            return None

        row = df.iloc[0]
        ob = {"time": datetime.now(), "stock_code": symbol, "levels": 5}

        all_cols = list(df.columns)
        for i in range(1, 6):
            for key, patterns in [
                ("bid_price", [f"bid{i}", f"买{i}价", f"买{i}"]),
                ("bid_volume", [f"bid_vol{i}", f"bidv{i}", f"买{i}量", f"买量{i}"]),
                ("ask_price", [f"ask{i}", f"卖{i}价", f"卖{i}"]),
                ("ask_volume", [f"ask_vol{i}", f"askv{i}", f"卖{i}量", f"卖量{i}"]),
            ]:
                col = self._find_col(all_cols, patterns)
                val = float(row[col]) if col and pd.notna(row[col]) else 0.0
                ob[f"{key}_{i:02d}"] = int(val) if "volume" in key else val

        for i in range(6, 11):
            for k in [f"bid_price_{i:02d}", f"bid_volume_{i:02d}",
                      f"ask_price_{i:02d}", f"ask_volume_{i:02d}"]:
                ob[k] = 0

        ob["spread"] = ob["ask_price_01"] - ob["bid_price_01"] if ob["bid_price_01"] > 0 else 0
        return ob

    # ---- Instrument / Market Info ----

    def get_instruments(self) -> pd.DataFrame:
        """Get instrument list. Uses L2 if available, else std."""
        if self.has_l2:
            try:
                data = self.api.get_instrument_info(0, 100)
                if data is not None:
                    return self.api.to_df(data)
            except Exception:
                pass
        return self._fallback_instruments()

    def _fallback_instruments(self) -> pd.DataFrame:
        if not self._connected_std:
            self._connect_std()
        if not self._connected_std:
            return pd.DataFrame()
        try:
            dfs = []
            for m in [self.MARKET_SH, self.MARKET_SZ]:
                df = self.std.stocks(market=m)
                if df is not None and not df.empty:
                    dfs.append(df)
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_stock_count(self) -> int:
        if self.has_l2:
            try:
                return self.api.get_instrument_count() or 0
            except Exception:
                pass
        return 0

    def get_connection_status(self) -> dict:
        """Return connection status for diagnostics."""
        if self._api is None:
            self.connect()
        return {
            "l2_connected": self._connected_l2,
            "std_connected": self._connected_std,
            "has_l2": self._connected_l2,
            "l2_levels": 10 if self._connected_l2 else 5 if self._connected_std else 0,
            "transactions_available": self._connected_l2,
        }

    # ---- Helpers ----

    def _normalize_transactions(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Standardize transaction column names."""
        if df is None or df.empty:
            return pd.DataFrame()

        column_map = {
            "time": "time", "price": "price", "vol": "volume", "volume": "volume",
            "num": "volume", "amount": "amount", "buyorsell": "direction",
            "type": "order_type", "seq": "seq", "serial": "seq",
        }
        rename = {}
        for col in df.columns:
            cl = str(col).lower().strip()
            if cl in column_map:
                target = column_map[cl]
                if target != cl and target not in df.columns:
                    rename[col] = target
        if rename:
            df = df.rename(columns=rename)

        if "direction" in df.columns:
            df["direction"] = df["direction"].map({0: -1, 1: 1, 2: 0}).fillna(0).astype("int8")
        else:
            df["direction"] = 0
        for col in ["price", "volume", "amount"]:
            if col not in df.columns:
                df[col] = 0.0
        if "order_type" not in df.columns:
            df["order_type"] = 0
        if "seq" not in df.columns:
            df["seq"] = range(len(df))
        df["stock_code"] = symbol
        return df[["time", "price", "volume", "amount", "direction", "order_type", "seq", "stock_code"]]

    def _parse_quote_df(self, df: pd.DataFrame, symbol: str) -> dict:
        """Parse L2 quote from DataFrame row."""
        row = df.iloc[0]
        ob = {"time": datetime.now(), "stock_code": symbol, "levels": 10}
        cols = list(df.columns)
        for i in range(1, 11):
            for key, patterns in [
                ("bid_price", [f"bid_price_{i}", f"bid{i}_price", f"买{i}价"]),
                ("bid_volume", [f"bid_volume_{i}", f"bid{i}_volume", f"买{i}量"]),
                ("ask_price", [f"ask_price_{i}", f"ask{i}_price", f"卖{i}价"]),
                ("ask_volume", [f"ask_volume_{i}", f"ask{i}_volume", f"卖{i}量"]),
            ]:
                col = self._find_col(cols, [str(p) for p in patterns])
                ob[f"{key}_{i:02d}"] = float(row[col]) if col and pd.notna(row[col]) else 0.0
        ob["spread"] = ob["ask_price_01"] - ob["bid_price_01"] if ob["bid_price_01"] > 0 else 0
        return ob

    def _parse_quote_raw(self, data, symbol: str) -> dict | None:
        """Try to parse raw L2 quote data into order book dict."""
        if data is None:
            return None
        if hasattr(data, '__len__') and len(data) == 0:
            return None
        return {"time": datetime.now(), "stock_code": symbol, "levels": 10}

    @staticmethod
    def _find_col(columns: list[str], candidates: list[str]) -> str | None:
        for c in candidates:
            for col in columns:
                if c.lower() in str(col).lower():
                    return col
        return None

    @staticmethod
    def is_trading_hours(dt: datetime | None = None) -> bool:
        if dt is None:
            dt = datetime.now()
        t = dt.time()
        from datetime import time
        return time(9, 30) <= t <= time(11, 30) or time(13, 0) <= t <= time(15, 0)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
