from __future__ import annotations

import unittest
from datetime import datetime
from types import SimpleNamespace

import pandas as pd

from utils.ths_data_source import THSDataSource


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class _FakeClient:
    def __init__(self) -> None:
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.kline_calls: list[dict] = []
        self.query_calls: list[dict] = []

    def connect(self):
        self.connect_calls += 1
        return SimpleNamespace(success=True, error="", data=[])

    def disconnect(self):
        self.disconnect_calls += 1

    def complete_ths_code(self, code):
        # Include an index result to verify that code completion filters it.
        return SimpleNamespace(
            success=True,
            error="",
            data=[{"代码": "USZD000013"}, {"代码": "UZZI000013"}],
        )

    def klines(self, ths_code, **kwargs):
        self.kline_calls.append({"ths_code": ths_code, **kwargs})
        adjusted = [
            {1: 20240102, 7: 49.0, 8: 52.0, 9: 48.0, 11: 50.0, 13: 1000, 19: 50000.0},
            {1: 20240103, 7: 50.0, 8: 55.0, 9: 49.0, 11: 54.0, 13: 2000, 19: 108000.0},
        ]
        raw = [
            {1: 20240102, 7: 4.9, 8: 5.2, 9: 4.8, 11: 5.0, 13: 1000, 19: 5000.0},
            {1: 20240103, 7: 5.0, 8: 6.2, 9: 4.9, 11: 6.0, 13: 2000, 19: 12000.0},
        ]
        return SimpleNamespace(success=True, error="", data=adjusted if kwargs["adjust"] == "backward" else raw)

    def corporate_action(self, ths_code):
        return SimpleNamespace(success=True, error="", data=[])

    def query_data(self, params):
        self.query_calls.append(params.copy())
        if params["id"] == 202:
            rows = [{
                5: "USZD000013",
                1968584: 1.25,
                3475914: 4_200_000_000,
                3541450: 5_000_000_000,
                3153: 12.5,
                2947: 1.75,
                134071: 2.25,
            }]
        elif params["id"] == 212:
            rows = [{1: 20240102, 1968584: 2.0}, {1: 20240103, 1968584: 4.0}]
        elif params["id"] == 211:
            rows = [{1: 20240102, 407: 50_000}, {1: 20240103, 407: 50_000}]
        else:
            raise AssertionError(params)
        return SimpleNamespace(success=True, error="", data=rows)


class _FlakyKlineClient(_FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self.timeout_failures = 0

    def klines(self, ths_code, **kwargs):
        if self.timeout_failures < 2:
            self.timeout_failures += 1
            self.kline_calls.append({"ths_code": ths_code, **kwargs})
            return SimpleNamespace(success=False, error="[thsdk]请求超时，超过 30 秒", data=[])
        return super().klines(ths_code, **kwargs)


class _FakeYuanhangBridge:
    def __init__(self) -> None:
        self.requests: list[str] = []
        self.closed = False

    def query(self, request: str):
        self.requests.append(request)
        if request.startswith("id=212&"):
            return [
                {"1": "20240102", "1968584": None},
                {"1": "20240103", "1968584": None},
            ]
        if request.startswith("id=211&"):
            return [{"1": "20240102", "407": "50000"}]
        raise AssertionError(request)

    def close(self):
        self.closed = True


class _FlakyYuanhangBridge(_FakeYuanhangBridge):
    def __init__(self) -> None:
        super().__init__()
        self.failures = 0

    def query(self, request: str):
        if request.startswith("id=212&") and self.failures < 2:
            self.failures += 1
            raise RuntimeError("System.NullReferenceException: transient parser state")
        return super().query(request)


class _DuplicateYuanhangBridge(_FakeYuanhangBridge):
    def query(self, request: str):
        if request.startswith("id=212&"):
            return [
                {"1": "20240102", "1968584": 1.0},
                {"1": "20240102", "1968584": 2.0},
                {"1": "20240103", "1968584": 3.0},
            ]
        return super().query(request)


class THSDataSourceTests(unittest.TestCase):
    def make_source(self):
        client = _FakeClient()
        clock = _Clock()
        source = THSDataSource(
            client_factory=lambda: client,
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
        )
        return source, client, clock

    def test_lazy_single_connection_and_rate_limit(self):
        source, client, clock = self.make_source()
        self.assertEqual("USZD000013", source.complete_code("000013"))
        self.assertEqual("USZD000013", source.complete_code("000013"))
        source.fetch_realtime("000013")
        self.assertEqual(1, client.connect_calls)
        self.assertEqual(1, len(clock.sleeps))
        self.assertGreaterEqual(clock.sleeps[0], 0.025)
        source.close()
        source.close()
        self.assertEqual(1, client.disconnect_calls)

    def test_dual_kline_merge_parses_yyyymmdd_and_close_raw(self):
        source, client, _ = self.make_source()
        result = source.fetch_klines("000013", "2024-01-01", "2024-01-03")
        self.assertEqual([50.0, 54.0], result["close"].tolist())
        self.assertEqual([5.0, 6.0], result["close_raw"].tolist())
        self.assertEqual(
            [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            [call["start_time"] for call in client.kline_calls],
        )

    def test_retries_transient_thsdk_timeout(self):
        client = _FlakyKlineClient()
        clock = _Clock()
        source = THSDataSource(
            client_factory=lambda: client,
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
        )

        result = source.fetch_klines("000013", "2024-01-01", "2024-01-03")

        self.assertEqual(2, client.timeout_failures)
        self.assertEqual([50.0, 54.0], result["close"].tolist())
        self.assertGreaterEqual(sum(clock.sleeps), 0.3)
        self.assertEqual(3, client.connect_calls)
        self.assertEqual(2, client.disconnect_calls)

    def test_realtime_fields_and_formula(self):
        source, _, _ = self.make_source()
        snapshot = source.fetch_realtime("000013")
        self.assertEqual(1.25, snapshot["turnover"])
        self.assertEqual(4_200_000_000, snapshot["market_cap"])
        self.assertEqual(12.5, snapshot["pe_dynamic"])
        self.assertEqual(1.75, snapshot["pb"])
        self.assertEqual(2.25, snapshot["ps"])
        history = source.fetch_history("000013", "2024-01-01", "2024-01-03")
        self.assertEqual([250_000.0, 300_000.0], history["market_cap"].tolist())
        self.assertEqual([5.0, 6.0], history["close_raw"].tolist())

    def test_realtime_batch_includes_valuation_fields(self):
        source, _, _ = self.make_source()

        quote = source.fetch_realtime_batch(["000013"])["000013"]

        self.assertEqual(12.5, quote["pe_dynamic"])
        self.assertEqual(1.75, quote["pb"])
        self.assertEqual(2.25, quote["ps"])

    def test_historical_field_queries_include_protocol_ids(self):
        source, client, _ = self.make_source()
        source.fetch_historical_fields("000013", "2024-01-01", "2024-01-03")
        self.assertEqual([212, 211], [call["id"] for call in client.query_calls])
        self.assertEqual(["1,1968584", "1,407"], [call["datatype"] for call in client.query_calls])

    def test_yuanhang_history_derives_delisted_turnover_from_effective_shares(self):
        client = _FakeClient()
        clock = _Clock()
        bridge = _FakeYuanhangBridge()
        source = THSDataSource(
            client_factory=lambda: client,
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
            history_bridge_factory=lambda: bridge,
        )

        history = source.fetch_history("000013", "2024-01-01", "2024-01-03")

        self.assertEqual([2.0, 4.0], history["turnover"].tolist())
        self.assertEqual([250_000.0, 300_000.0], history["market_cap"].tolist())
        self.assertTrue(all("market=USZD" in request for request in bridge.requests))
        self.assertEqual(2, len(bridge.requests))
        source.close()
        self.assertTrue(bridge.closed)

    def test_retries_transient_yuanhang_null_reference(self):
        client = _FakeClient()
        clock = _Clock()
        bridge = _FlakyYuanhangBridge()
        source = THSDataSource(
            client_factory=lambda: client,
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
            history_bridge_factory=lambda: bridge,
        )

        turnover = source.fetch_turnover_history("000013", "2024-01-01", "2024-01-03")

        self.assertEqual(2, bridge.failures)
        self.assertEqual(2, len(turnover))

    def test_historical_field_keeps_last_same_day_snapshot(self):
        client = _FakeClient()
        clock = _Clock()
        bridge = _DuplicateYuanhangBridge()
        source = THSDataSource(
            client_factory=lambda: client,
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
            history_bridge_factory=lambda: bridge,
        )

        turnover = source.fetch_turnover_history(
            "000013", "2024-01-01", "2024-01-03"
        )

        self.assertEqual([2.0, 3.0], turnover["turnover"].tolist())

    def test_rebuilds_backward_prices_from_corporate_actions(self):
        raw = pd.DataFrame(
            {
                "date": pd.to_datetime(["2012-05-04", "2012-05-07"]),
                "open": [14.49, 10.20],
                "high": [15.71, 10.60],
                "low": [14.47, 10.04],
                "close": [15.47, 10.15],
            }
        )
        actions = pd.DataFrame(
            {
                "date": pd.to_datetime(["2012-05-07"]),
                "bonus_ratio": [0.5],
                "cash_per_share": [0.06],
                "rights_ratio": [0.0],
                "rights_price": [0.0],
            }
        )

        rebuilt = THSDataSource._apply_backward_adjustment(raw, actions)

        self.assertAlmostEqual(15.47, rebuilt.iloc[0]["close"], places=8)
        event_factor = 15.47 * 1.5 / (15.47 - 0.06)
        self.assertAlmostEqual(10.15 * event_factor, rebuilt.iloc[1]["close"], places=8)
        self.assertAlmostEqual(10.20 * event_factor, rebuilt.iloc[1]["open"], places=8)
        self.assertTrue((rebuilt[["open", "high", "low", "close"]] > 0).all().all())

    def test_repairs_only_a_confirmed_persistent_volume_unit_regime(self):
        dates = pd.date_range("1991-01-01", periods=25, freq="D")
        frame = pd.DataFrame(
            {
                "date": dates,
                "open_raw": [14.8] * 25,
                "high_raw": [15.5] * 25,
                "low_raw": [14.5] * 25,
                "close_raw": [15.0] * 25,
                "volume": [300.0] * 25,
                "amount": [22_500.0] * 25,
            }
        )
        # An isolated bad amount must be nulled, not used to invent a volume
        # correction for an otherwise healthy regime.
        isolated = pd.DataFrame(
            {
                "date": pd.to_datetime(["2005-01-05"]),
                "open_raw": [5.3],
                "high_raw": [5.6],
                "low_raw": [5.2],
                "close_raw": [5.46],
                "volume": [17_589_854.0],
                "amount": [2_147_483_648.0],
            }
        )
        frame = pd.concat([frame, isolated], ignore_index=True)

        repaired, audit = THSDataSource._repair_trade_units(frame)

        self.assertTrue((repaired.iloc[:25]["volume"] == 1500.0).all())
        self.assertTrue(pd.isna(repaired.iloc[-1]["amount"]))
        self.assertEqual(25, audit["volume_unit_repaired_rows"])
        self.assertEqual(1, audit["amount_sentinel_rows"])

    def test_volume_regime_does_not_cross_normal_counterevidence(self):
        dates = pd.date_range("1991-01-01", periods=24, freq="D")
        volumes = [300.0] * 10 + [1500.0] * 4 + [300.0] * 10
        frame = pd.DataFrame(
            {
                "date": dates,
                "open_raw": [14.8] * 24,
                "high_raw": [15.5] * 24,
                "low_raw": [14.5] * 24,
                "close_raw": [15.0] * 24,
                "volume": volumes,
                "amount": [22_500.0] * 24,
            }
        )

        repaired, audit = THSDataSource._repair_trade_units(frame)

        self.assertEqual(volumes, repaired["volume"].tolist())
        self.assertEqual(0, audit["volume_unit_repaired_rows"])
        self.assertEqual(20, audit["amount_invalid_rows"])
        self.assertTrue(repaired.iloc[10:14]["amount"].notna().all())

    def test_repairs_volume_regime_when_source_volume_is_too_large(self):
        frame = pd.DataFrame(
            {
                "date": pd.date_range("1991-01-01", periods=25, freq="D"),
                "open_raw": [14.8] * 25,
                "high_raw": [15.5] * 25,
                "low_raw": [14.5] * 25,
                "close_raw": [15.0] * 25,
                # The archive is 100x too large; the economically compatible
                # traded volume is 1,500 shares.
                "volume": [150_000.0] * 25,
                "amount": [22_500.0] * 25,
            }
        )

        repaired, audit = THSDataSource._repair_trade_units(frame)

        self.assertTrue((repaired["volume"] == 1_500.0).all())
        self.assertTrue(repaired["amount"].notna().all())
        self.assertEqual(25, audit["volume_unit_repaired_rows"])
        self.assertEqual(0.01, audit["volume_unit_regimes"][0]["factor"])

    def test_repairs_unique_isolated_tenfold_volume_error(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["1992-10-28"]),
                "open_raw": [68.2],
                "high_raw": [71.5],
                "low_raw": [67.1],
                "close_raw": [70.0],
                "volume": [276_400.0],
                "amount": [193_221_800.0],
            }
        )

        repaired, audit = THSDataSource._repair_trade_units(frame)

        self.assertEqual(2_764_000.0, repaired.iloc[0]["volume"])
        self.assertEqual(193_221_800.0, repaired.iloc[0]["amount"])
        self.assertEqual(1, audit["volume_unit_isolated_rows"])
        self.assertEqual(10.0, audit["volume_unit_isolated_repairs"][0]["factor"])

    def test_isolated_non_tenfold_error_remains_fail_closed(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["1992-10-28"]),
                "open_raw": [68.2],
                "high_raw": [71.5],
                "low_raw": [67.1],
                "close_raw": [70.0],
                "volume": [552_800.0],
                "amount": [193_221_800.0],
            }
        )

        repaired, audit = THSDataSource._repair_trade_units(frame)

        self.assertTrue(pd.isna(repaired.iloc[0]["amount"]))
        self.assertEqual(0, audit["volume_unit_isolated_rows"])
        self.assertEqual(1, audit["amount_invalid_rows"])

    def test_volume_sentinel_is_never_used_for_vwap_repair(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2006-12-06"]),
                "open_raw": [3.44],
                "high_raw": [3.47],
                "low_raw": [3.30],
                "close_raw": [3.33],
                "volume": [2_147_483_648.0],
                "amount": [740_495_000.0],
            }
        )

        repaired, audit = THSDataSource._repair_trade_units(frame)

        self.assertTrue(pd.isna(repaired.iloc[0]["volume"]))
        self.assertTrue(pd.isna(repaired.iloc[0]["amount"]))
        self.assertEqual(1, audit["volume_sentinel_rows"])

    def test_repairs_reciprocal_twenty_five_volume_regime(self):
        frame = pd.DataFrame(
            {
                "date": pd.date_range("1991-01-01", periods=20, freq="D"),
                "open_raw": [14.8] * 20,
                "high_raw": [15.5] * 20,
                "low_raw": [14.5] * 20,
                "close_raw": [15.0] * 20,
                "volume": [37_500.0] * 20,
                "amount": [22_500.0] * 20,
            }
        )

        repaired, audit = THSDataSource._repair_trade_units(frame)

        self.assertTrue((repaired["volume"] == 1_500.0).all())
        self.assertEqual(20, audit["volume_unit_repaired_rows"])
        self.assertEqual(0.04, audit["volume_unit_regimes"][0]["factor"])

    def test_repairs_only_minor_ohlc_envelope_errors(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2000-09-05", "2000-09-06"]),
                "open": [35.10, 10.0],
                "high": [35.75, 10.1],
                "low": [34.58, 9.9],
                "close": [34.463, 10.5],
                "open_raw": [26.70, 10.0],
                "high_raw": [27.00, 10.1],
                "low_raw": [26.60, 9.9],
                "close_raw": [26.51, 10.5],
            }
        )

        repaired, audit = THSDataSource._repair_minor_ohlc_envelope(frame)

        self.assertEqual(34.463, repaired.iloc[0]["low"])
        self.assertEqual(26.51, repaired.iloc[0]["low_raw"])
        self.assertEqual(10.1, repaired.iloc[1]["high"])
        self.assertEqual(10.1, repaired.iloc[1]["high_raw"])
        self.assertEqual(1, audit["ohlc_envelope_repaired_rows"])
        self.assertEqual(2, len(audit["ohlc_envelope_repairs"]))

    def test_leaves_material_ohlc_envelope_error_for_validation_gate(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2000-09-06"]),
                "open": [10.0], "high": [10.1], "low": [9.9], "close": [10.5],
            }
        )

        repaired, audit = THSDataSource._repair_minor_ohlc_envelope(frame)

        self.assertEqual(10.1, repaired.iloc[0]["high"])
        self.assertEqual(0, audit["ohlc_envelope_repaired_rows"])

    def test_backward_adjustment_treats_missing_action_numbers_as_zero(self):
        raw = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-02", "2020-01-03"]),
                "open": [10.0, 11.0], "high": [11.0, 12.0],
                "low": [9.0, 10.0], "close": [10.5, 11.5],
            }
        )
        actions = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-03"]),
                "bonus_ratio": [pd.NA], "cash_per_share": [pd.NA],
                "rights_ratio": [pd.NA], "rights_price": [pd.NA],
                "consideration_stock_ratio": [pd.NA],
                "consideration_cash_per_share": [pd.NA],
            }
        )

        adjusted = THSDataSource._apply_backward_adjustment(raw, actions)

        pd.testing.assert_frame_equal(adjusted, raw)

    def test_drops_only_the_still_forming_current_daily_bar(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-24", "2026-07-27"]),
                "close": [10.0, 10.5],
            }
        )

        before_close = THSDataSource._drop_incomplete_daily_bar(
            frame, datetime(2026, 7, 27, 9, 38)
        )
        after_close = THSDataSource._drop_incomplete_daily_bar(
            frame, datetime(2026, 7, 27, 15, 1)
        )

        self.assertEqual(["2026-07-24"], before_close["date"].dt.strftime("%Y-%m-%d").tolist())
        self.assertEqual(2, len(after_close))

    def test_aligns_share_change_to_the_corporate_action_trading_day(self):
        trading_dates = pd.DataFrame(
            {"date": pd.to_datetime(["1996-05-23", "1996-05-24", "1996-05-27"])}
        )
        shares = pd.DataFrame(
            {
                "date": pd.to_datetime(["1996-05-23", "1996-05-24"]),
                "outstanding_shares": [357_211_478.0, 714_422_956.0],
            }
        )
        actions = pd.DataFrame(
            {
                "date": pd.to_datetime(["1996-05-27"]),
                "bonus_ratio": [1.0],
                "cash_per_share": [0.0],
                "rights_ratio": [0.0],
                "rights_price": [0.0],
            }
        )

        aligned, audit = THSDataSource._align_outstanding_shares(
            trading_dates, shares, actions
        )

        self.assertEqual(
            [357_211_478.0, 357_211_478.0, 714_422_956.0],
            aligned["outstanding_shares"].tolist(),
        )
        self.assertEqual(1, audit["share_action_realigned_events"])

    def test_consideration_stock_ratio_aligns_share_change_to_reform_day(self):
        trading_dates = pd.DataFrame(
            {"date": pd.to_datetime(["2006-05-24", "2006-05-25", "2006-05-26"])}
        )
        shares = pd.DataFrame(
            {
                "date": pd.to_datetime(["2006-05-24", "2006-05-25", "2006-05-26"]),
                "outstanding_shares": [100_000_000.0, 100_000_000.0, 150_000_000.0],
            }
        )
        actions = pd.DataFrame(
            {
                "date": pd.to_datetime(["2006-05-25"]),
                "bonus_ratio": [0.0], "rights_ratio": [0.0],
                "consideration_stock_ratio": [0.5],
            }
        )

        aligned, audit = THSDataSource._align_outstanding_shares(
            trading_dates, shares, actions
        )

        self.assertEqual(
            [100_000_000.0, 150_000_000.0, 150_000_000.0],
            aligned["outstanding_shares"].tolist(),
        )
        self.assertEqual(1, audit["share_action_realigned_events"])

    def test_aligns_share_change_from_adjusted_vs_raw_price_discontinuity(self):
        trading_dates = pd.DataFrame(
            {
                "date": pd.to_datetime(["1996-05-23", "1996-05-24", "1996-05-27"]),
                "close": [146.271, 148.622, 150.244],
                "close_raw": [18.03, 18.32, 9.26],
            }
        )
        shares = pd.DataFrame(
            {
                "date": pd.to_datetime(["1996-05-23", "1996-05-24"]),
                "outstanding_shares": [357_211_478.0, 714_422_956.0],
            }
        )

        aligned, audit = THSDataSource._align_outstanding_shares(
            trading_dates, shares, pd.DataFrame()
        )

        self.assertEqual(
            [357_211_478.0, 357_211_478.0, 714_422_956.0],
            aligned["outstanding_shares"].tolist(),
        )
        self.assertEqual("price_discontinuity", audit["share_action_realignments"][0]["evidence"])

    def test_does_not_realign_small_ordinary_float_share_changes(self):
        trading_dates = pd.DataFrame(
            {
                "date": pd.to_datetime(["2018-01-02", "2018-01-03", "2018-01-04"]),
                "close": [10.0, 11.0, 11.077],
                "close_raw": [10.0, 10.0, 10.0],
            }
        )
        shares = pd.DataFrame(
            {
                "date": pd.to_datetime(["2018-01-02", "2018-01-03"]),
                "outstanding_shares": [100_000_000.0, 100_700_000.0],
            }
        )

        aligned, audit = THSDataSource._align_outstanding_shares(
            trading_dates, shares, pd.DataFrame()
        )

        self.assertEqual(
            [100_000_000.0, 100_700_000.0, 100_700_000.0],
            aligned["outstanding_shares"].tolist(),
        )
        self.assertEqual(0, audit["share_action_realigned_events"])

    def test_aligns_late_share_change_back_to_ex_rights_day(self):
        trading_dates = pd.DataFrame(
            {"date": pd.to_datetime(["2012-05-04", "2012-05-07", "2012-05-08"])}
        )
        shares = pd.DataFrame(
            {
                "date": pd.to_datetime(["2012-05-04", "2012-05-07", "2012-05-08"]),
                "outstanding_shares": [218_140_000.0, 218_140_000.0, 327_210_000.0],
            }
        )
        actions = pd.DataFrame(
            {
                "date": pd.to_datetime(["2012-05-07"]),
                "bonus_ratio": [0.5],
                "cash_per_share": [0.06],
                "rights_ratio": [0.0],
                "rights_price": [0.0],
            }
        )

        aligned, audit = THSDataSource._align_outstanding_shares(
            trading_dates, shares, actions
        )

        self.assertEqual(
            [218_140_000.0, 327_210_000.0, 327_210_000.0],
            aligned["outstanding_shares"].tolist(),
        )
        self.assertEqual(1, audit["share_action_realigned_events"])

    def test_partial_history_does_not_match_an_old_action_to_a_new_transition(self):
        trading_dates = pd.DataFrame(
            {"date": pd.to_datetime(["1996-05-23", "1996-05-24", "1996-05-27"])}
        )
        shares = pd.DataFrame(
            {
                "date": pd.to_datetime(["1996-05-23", "1996-05-24"]),
                "outstanding_shares": [357_211_478.0, 714_422_956.0],
            }
        )
        actions = pd.DataFrame(
            {
                "date": pd.to_datetime(["1993-05-24", "1996-05-27"]),
                "bonus_ratio": [0.95, 1.0],
                "cash_per_share": [0.03, 0.0],
                "rights_ratio": [0.0, 0.0],
                "rights_price": [0.0, 0.0],
            }
        )

        aligned, audit = THSDataSource._align_outstanding_shares(
            trading_dates, shares, actions
        )

        self.assertEqual(
            [357_211_478.0, 357_211_478.0, 714_422_956.0],
            aligned["outstanding_shares"].tolist(),
        )
        self.assertEqual("1996-05-27", audit["share_action_realignments"][0]["event_date"])

    def test_parses_bonus_rights_and_share_reform_actions(self):
        response = SimpleNamespace(
            data=[
                {
                    1: 20060525,
                    471: (
                        "2006-05-25(  \u6bcf10\u80a1\u5bf9\u4ef7\u73b0\u91d141.3200\u5143 ,"
                        "\u6bcf10\u80a1\u5bf9\u4ef7\u80a1\u796812.4000\u80a1)$"
                    ),
                },
                {
                    1: 20120507,
                    471: "2012-05-07(\u6bcf\u5341\u80a1 \u90015.00\u80a1 \u7ea2\u52290.60\u5143)$",
                },
            ]
        )

        actions = THSDataSource._parse_corporate_actions(response)

        self.assertAlmostEqual(1.24, actions.iloc[0]["consideration_stock_ratio"])
        self.assertAlmostEqual(4.132, actions.iloc[0]["consideration_cash_per_share"])
        self.assertAlmostEqual(0.5, actions.iloc[1]["bonus_ratio"])
        self.assertAlmostEqual(0.06, actions.iloc[1]["cash_per_share"])

    def test_corporate_action_with_missing_description_does_not_crash(self):
        response = SimpleNamespace(data=[{1: 20200102, 471: pd.NA}])

        actions = THSDataSource._parse_corporate_actions(response)

        self.assertEqual(1, len(actions))
        self.assertEqual("", actions.iloc[0]["description"])
        self.assertEqual(0.0, actions.iloc[0]["bonus_ratio"])


if __name__ == "__main__":
    unittest.main()
