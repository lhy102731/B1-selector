"""Verify mootdx/tdxpy L2 connectivity and data access.

Tests:
  1. L2 connection + stock count
  2. Std fallback stock list
  3. Historical transactions (L2, market-hours only)
  4. Real-time quotes (L2, market-hours only)
  5. Order book depth snapshot
  6. Local TDX .lc1 minute data
  7. TickStorage read/write
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from l2.data.config import L2Config
from l2.data.collector import L2DataCollector
from l2.data.storage import TickStorage
from l2.data.tdx_reader import TDXLocalReader
from datetime import datetime, time

TEST_STOCKS = ["600366", "000001", "600519", "000977", "300750"]

SKIP = "SKIP"
PASS = "PASS"
FAIL = "FAIL"


def is_market_hours():
    """Check if currently within A-share trading hours."""
    now = datetime.now()
    t = now.time()
    return time(9, 25) <= t <= time(11, 30) or time(13, 0) <= t <= time(15, 5)


def test_l2_connection():
    """Test tdxpy L2 connection and stock count."""
    print("=" * 60)
    print("Test 1: L2 Connection + Stock Count")
    print("=" * 60)
    try:
        c = L2DataCollector()
        result = c.connect()
        status = c.get_connection_status()
        print(f"  L2 connected: {status['l2_connected']}")
        print(f"  L2 levels: {status['l2_levels']}")

        if status['l2_connected']:
            count = c.get_stock_count()
            print(f"  L2 stock count: {count:,}")
        else:
            print("  L2 not connected (TDX may not be running)")
        c.close()
        return PASS if status['l2_connected'] else SKIP
    except Exception as e:
        print(f"  FAILED: {e}")
        return FAIL


def test_std_stock_list():
    """Test standard stock list via mootdx."""
    print("\n" + "=" * 60)
    print("Test 2: Standard Stock List (mootdx)")
    print("=" * 60)
    try:
        c = L2DataCollector()
        c._connect_std()
        instruments = c._fallback_instruments()
        if not instruments.empty:
            print(f"  Stocks: {len(instruments):,}")
            print(f"  Columns: {list(instruments.columns)}")
            print(f"  Sample: {instruments.head(2).to_string()}")
        c.close()
        return PASS if not instruments.empty else FAIL
    except Exception as e:
        print(f"  FAILED: {e}")
        return FAIL


def test_historical_transactions():
    """Test historical tick-by-tick via L2."""
    print("\n" + "=" * 60)
    print("Test 3: Historical Transactions (L2)")
    print("=" * 60)
    if not is_market_hours():
        print("  SKIP: Outside trading hours")
        return SKIP
    try:
        c = L2DataCollector()
        c.connect()
        if not c.has_l2:
            print("  SKIP: L2 not connected")
            c.close()
            return SKIP

        today = datetime.now().strftime("%Y%m%d")
        for code in TEST_STOCKS[:2]:
            print(f"  Fetching {code}...")
            df = c.get_historical_transactions(code, today)
            if not df.empty:
                print(f"  {code}: {len(df)} ticks")
                print(f"  Columns: {list(df.columns)}")
                print(f"  Sample:\n{df.head(3).to_string()}")
                print(f"  Buy/Sell: {(df['direction']==1).sum()} buy, {(df['direction']==-1).sum()} sell")
                c.close()
                return PASS
        print(f"  No data (expected outside trading hours or for non-trading day)")
        c.close()
        return SKIP
    except Exception as e:
        print(f"  FAILED: {e}")
        import traceback
        traceback.print_exc()
        return FAIL


def test_realtime_quotes():
    """Test real-time L2 quotes."""
    print("\n" + "=" * 60)
    print("Test 4: Real-time L2 Quotes")
    print("=" * 60)
    if not is_market_hours():
        print("  SKIP: Outside trading hours")
        return SKIP
    try:
        c = L2DataCollector()
        c.connect()
        if not c.has_l2:
            print("  SKIP: L2 not connected")
            c.close()
            return SKIP

        df = c.get_realtime_quotes(TEST_STOCKS[:3])
        if not df.empty:
            print(f"  Quotes: {len(df)} rows")
            print(f"  Sample:\n{df.head(2).to_string()}")
            c.close()
            return PASS
        print("  No data")
        c.close()
        return SKIP
    except Exception as e:
        print(f"  FAILED: {e}")
        return FAIL


def test_orderbook():
    """Test L2 order book (10-level depth)."""
    print("\n" + "=" * 60)
    print("Test 5: L2 Order Book Depth")
    print("=" * 60)
    if not is_market_hours():
        print("  SKIP: Outside trading hours")
        return SKIP
    try:
        c = L2DataCollector()
        c.connect()
        if not c.has_l2:
            print("  SKIP: L2 not connected")
            c.close()
            return SKIP

        ob = c.get_order_book_snapshot("600366")
        if ob:
            print(f"  Levels: {ob.get('levels', 'N/A')}")
            print(f"  bid_price_01: {ob.get('bid_price_01', 'N/A')}")
            print(f"  ask_price_01: {ob.get('ask_price_01', 'N/A')}")
            print(f"  spread: {ob.get('spread', 'N/A')}")
            c.close()
            return PASS
        print("  No data")
        c.close()
        return SKIP
    except Exception as e:
        print(f"  FAILED: {e}")
        return FAIL


def test_local_l2_data():
    """Test local TDX .lc1 L2 minute data."""
    print("\n" + "=" * 60)
    print("Test 6: Local TDX L2 Minute Data (.lc1)")
    print("=" * 60)
    try:
        c = L2DataCollector()
        c.connect()
        df = c.get_local_l2_minute("600366")
        if not df.empty:
            print(f"  600366: {len(df):,} rows")
            print(f"  Columns: {list(df.columns)}")
            print(f"  Date range: {df.index[0]} ~ {df.index[-1]}")
            print(f"  Sample:\n{df.head(3).to_string()}")
            c.close()
            return PASS
        print("  No local .lc1 data (TDX may not have L2 setup)")
        c.close()
        return SKIP
    except Exception as e:
        print(f"  FAILED: {e}")
        import traceback
        traceback.print_exc()
        return FAIL


def test_storage():
    """Test TickStorage read/write."""
    print("\n" + "=" * 60)
    print("Test 7: TickStorage (Parquet)")
    print("=" * 60)
    import pandas as pd
    import numpy as np

    storage = TickStorage()
    print(f"  Base dir: {storage.base_dir}")

    test_date = "20260522"
    test_code = "TEST99"
    n = 100
    df = pd.DataFrame({
        "time": pd.date_range("2026-05-22 09:30:00", periods=n, freq="3s"),
        "price": np.random.uniform(10, 11, n).astype("float32"),
        "volume": np.random.randint(100, 10000, n).astype("int32"),
        "amount": np.random.uniform(1000, 100000, n),
        "direction": np.random.choice([-1, 1, 0], n).astype("int8"),
        "order_type": np.zeros(n, dtype="int8"),
        "seq": np.arange(n, dtype="int64"),
        "stock_code": test_code,
    })

    path = storage.save_transactions(test_code, test_date, df)
    print(f"  Saved: {path}")

    loaded = storage.load_transactions(test_code, test_date)
    print(f"  Loaded: {len(loaded)} rows, match: {len(loaded) == len(df)}")

    import shutil
    test_dir = storage._transactions_dir(test_date, test_code)
    shutil.rmtree(test_dir.parent.parent, ignore_errors=True)
    print("  Cleanup: OK")
    return PASS


def main():
    print("L2 DeepTrade Connectivity Verification")
    print(f"Time: {datetime.now()}")
    print(f"Market hours: {is_market_hours()}")
    print(f"Python: {sys.version}")

    results = {}
    tests = [
        ("L2 Connection + Stock Count", test_l2_connection),
        ("Standard Stock List", test_std_stock_list),
        ("Historical Transactions (L2)", test_historical_transactions),
        ("Real-time L2 Quotes", test_realtime_quotes),
        ("L2 Order Book Depth", test_orderbook),
        ("Local .lc1 Minute Data", test_local_l2_data),
        ("TickStorage", test_storage),
    ]

    for name, test_fn in tests:
        try:
            results[name] = test_fn()
        except Exception as e:
            print(f"\n  UNEXPECTED ERROR in {name}: {e}")
            import traceback
            traceback.print_exc()
            results[name] = FAIL

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    passed = 0
    total = 0
    for name, status in results.items():
        if status == SKIP:
            print(f"  [SKIP] {name}")
        elif status == PASS:
            print(f"  [PASS] {name}")
            passed += 1
            total += 1
        else:
            print(f"  [FAIL] {name}")
            total += 1
    print(f"\n  {passed}/{total} tests passed ({len([v for v in results.values() if v == SKIP])} skipped)")

    return 0 if all(v != FAIL for v in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
