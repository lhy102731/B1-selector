import os
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class L2Config:
    """L2 system configuration.

    Sources:
      - mootdx remote: ExtQuotes (market='ext') for real-time L2 data
      - TDX local files: ExtReader for .lc1/.lc5 offline data
    """

    # ---- TDX paths ----
    TDX_DIR: str = field(default_factory=lambda: os.environ.get(
        "TDX_DIR", "D:/TDX"
    ))

    # ---- mootdx remote server ----
    # Market: 'ext'=L2 (protocol not supported by current mootdx/pytdx)
    #         'std'=standard (works reliably, 5-level depth)
    # Note: TDX L2 servers use port 7727 with an updated protocol that
    # mootdx/pytdx cannot parse. L2 transaction data requires either:
    #   a) A custom protocol adapter for the new L2 binary format
    #   b) Using akshare's stock_fund_flow_big_deal() for big-order data
    #   c) A third-party TDX L2 SDK
    MOOTDX_MARKET: str = "std"         # default to working std market
    MOOTDX_BEST_IP: bool = True
    MOOTDX_TIMEOUT: int = 10
    MOOTDX_RETRY: int = 3
    # Custom L2 server(s). Format: [("name", "ip", port), ...]
    # Found in D:/TDX/connect.cfg [DSHOST] section - 16 servers on port 7727
    CUSTOM_EX_HOSTS: list | None = None
    FALLBACK_TO_STD: bool = True

    # ---- Data collection intervals (ms) ----
    TICK_POLL_INTERVAL_MS: int = 3000       # poll every 3s for new ticks
    ORDERBOOK_POLL_INTERVAL_MS: int = 1000  # poll depth every 1s
    QUOTES_POLL_INTERVAL_MS: int = 5000     # poll market quotes every 5s
    MAX_TICK_BATCH: int = 800               # mootdx max offset per call

    # ---- Storage ----
    STORAGE_BASE: str = "data/l2"
    PARQUET_COMPRESSION: str = "zstd"
    RAW_TICK_RETENTION_DAYS: int = 30
    FEATURE_RETENTION_DAYS: int = 90

    # ---- Analysis thresholds ----
    BIG_ORDER_PERCENTILE: float = 85.0      # percentile for big order threshold
    HUGE_ORDER_PERCENTILE: float = 97.0     # percentile for huge order threshold
    INSTITUTIONAL_MIN_AMOUNT: float = 100000  # min amount for institutional trade
    ORDER_WALL_THRESHOLD_RATIO: float = 3.0   # N x average depth = order wall
    ORDER_WALL_STRONG_RATIO: float = 5.0      # N x average depth = strong wall
    WASH_TRADE_WINDOW: int = 20              # sliding window for wash detection
    ANOMALY_ZSCORE_THRESHOLD: float = 4.0    # z-score for anomaly detection
    TICK_SURGE_MULTIPLIER: float = 3.0       # N x normal tick rate = surge
    DEPTH_IMBALANCE_EXTREME: float = 0.7     # imbalance > |0.7| = extreme

    # ---- L2 order size classification (yuan) ----
    ORDER_CLASS_RETAIL: tuple = (0, 20000)         # < 2万
    ORDER_CLASS_SMALL: tuple = (20000, 100000)     # 2万-10万
    ORDER_CLASS_MEDIUM: tuple = (100000, 500000)   # 10万-50万
    ORDER_CLASS_LARGE: tuple = (500000, 1000000)   # 50万-100万
    # > 1,000,000 = huge (特大单)

    # ---- Market hours (A-share) ----
    MARKET_OPEN: str = "09:30"
    MARKET_CLOSE: str = "15:00"
    MORNING_SESSION_END: str = "11:30"
    AFTERNOON_SESSION_START: str = "13:00"

    # ---- GUI defaults ----
    DEFAULT_WATCHLIST: list = field(default_factory=list)
    MAX_WATCHLIST_SIZE: int = 50
    GUI_REFRESH_INTERVAL_MS: int = 100     # chart redraw throttle

    @property
    def storage_path(self) -> Path:
        return Path(self.STORAGE_BASE)

    @property
    def tdx_path(self) -> Path:
        return Path(self.TDX_DIR)
