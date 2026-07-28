"""同花顺 THSDK 数据源适配器。

这个模块只在第一次请求时导入 ``thsdk``，因此没有安装原生行情库的机器仍
可以导入项目其余模块。所有请求复用一个 TCP 连接，并遵守官方 20ms 限频。

数据约定：``close`` 是后复权价，``close_raw`` 是未复权价；``market_cap``
统一表示普通流通市值（元），优先由未复权收盘价和远航版历史流通股本推导。
"""

from __future__ import annotations

import importlib
import json
import logging
import math
import re
import threading
import time
from collections.abc import Callable, Iterable
from datetime import date, datetime
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd

from utils.ths_yuanhang_bridge import hydrate_ths_process_environment


_AUTO_BRIDGE = object()


class THSDataSourceError(RuntimeError):
    """THSDK 请求或返回数据不满足适配器契约。"""


class THSHistoryPermissionError(THSDataSourceError):
    """历史字段接口不可用（常见于游客账号）。"""


class THSDataSource:
    """惰性、可复用、限频的 THSDK 客户端。"""

    # ``pe_dynamic`` historically stores PE(TTM) in this repository.  THS
    # field 2942 is a different dynamic-PE definition, so the realtime path
    # must use field 3153 to preserve the existing column contract.
    PE_TTM_FIELD_ID = 3153
    PB_FIELD_ID = 2947
    PS_TTM_FIELD_ID = 134071
    CURRENT_FIELD_IDS = (
        1968584,
        3475914,
        3541450,
        PE_TTM_FIELD_ID,
        PB_FIELD_ID,
        PS_TTM_FIELD_ID,
    )
    TURNOVER_FIELD_ID = 1968584
    OUTSTANDING_SHARES_FIELD_ID = 407
    # 桌面端 RequestCandleV2 使用的日线周期；历史字段接口需要这个值。
    DAILY_PERIOD = 0x4000
    STOCK_MARKETS = frozenset(
        {"USHA", "USZA", "USHT", "USZT", "USHP", "USZP", "USHD", "USZD", "USTM"}
    )
    ACTIVE_MARKETS = frozenset({"USHA", "USZA", "USHT", "USZT", "USTM"})
    MISSING_SENTINELS = frozenset({2147483647.0, 2147483648.0, 4294967295.0})
    # Historical THS archives contain both directions of volume-unit drift:
    # some intervals need the reported volume multiplied by a lot-size factor,
    # while old Shanghai intervals can be too large and need division.  Keep
    # the human-readable basis factors here and evaluate their reciprocals in
    # ``_repair_trade_units`` as volume multipliers.
    VOLUME_UNIT_CANDIDATES = (
        2.0,
        2.5,
        4.0,
        5.0,
        10.0,
        20.0,
        25.0,
        40.0,
        50.0,
        100.0,
        500.0,
    )
    MIN_VOLUME_REGIME_ROWS = 20
    MIN_VOLUME_REGIME_SHARE = 0.80
    # THS has a small number of old candles where close is one or a few ticks
    # outside the reported high/low.  Repair only a narrow envelope error;
    # larger discrepancies remain visible to validate_history and block release.
    MAX_OHLC_ENVELOPE_RELATIVE_GAP = 0.01

    def __init__(
        self,
        client_factory: Callable[[], Any] | None = None,
        monotonic: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
        min_interval: float = 0.025,
        history_bridge_factory: Callable[[], Any] | None | object = _AUTO_BRIDGE,
    ) -> None:
        if min_interval < 0.025:
            raise ValueError("min_interval must be at least 0.025 seconds")
        self._client_factory = client_factory or self._default_client_factory
        self._monotonic = monotonic or time.monotonic
        self._sleeper = sleeper or time.sleep
        self._min_interval = float(min_interval)
        self._client: Any | None = None
        if history_bridge_factory is _AUTO_BRIDGE:
            self._history_bridge_factory = self._default_history_bridge_factory if client_factory is None else None
        else:
            self._history_bridge_factory = history_bridge_factory
        self._history_bridge: Any | None = None
        self._last_request_at: float | None = None
        self._completed_codes: dict[str, str] = {}
        self._closed = False
        self._lock = threading.RLock()

    @staticmethod
    def _default_client_factory() -> Any:
        hydrate_ths_process_environment()
        module = importlib.import_module("thsdk")
        # The package logs the account and MAC on every connect/disconnect.
        # Credentials belong in process memory only, never in rebuild logs.
        logging.getLogger("thsdk").disabled = True
        logging.getLogger("thsdk.base").disabled = True
        return module.THS()

    @staticmethod
    def _default_history_bridge_factory() -> Any:
        module = importlib.import_module("utils.ths_yuanhang_bridge")
        return module.YuanhangHistoryBridge()

    def _history_bridge_or_none(self) -> Any | None:
        if self._history_bridge_factory is None:
            return None
        if self._history_bridge is None:
            self._history_bridge = self._history_bridge_factory()
        return self._history_bridge

    @staticmethod
    def _ok(response: Any, operation: str) -> Any:
        if response is None or not bool(getattr(response, "success", False)):
            error = str(getattr(response, "error", "empty response"))
            if operation.startswith("historical_") or "权限" in error or "非法请求" in error:
                raise THSHistoryPermissionError(f"THSDK {operation} failed: {error}")
            raise THSDataSourceError(f"THSDK {operation} failed: {error}")
        return response

    def _client_or_connect(self) -> Any:
        if self._closed:
            raise THSDataSourceError("THSDataSource is closed")
        if self._client is None:
            client = self._client_factory()
            self._ok(client.connect(), "connect")
            self._client = client
        return self._client

    def _request(self, method: str, *args: Any, operation: str | None = None, **kwargs: Any) -> Any:
        with self._lock:
            for attempt in range(3):
                client = self._client_or_connect()
                now = float(self._monotonic())
                required_interval = max(
                    self._min_interval,
                    0.050 if method == "corporate_action" else self._min_interval,
                )
                if self._last_request_at is not None:
                    wait = required_interval - (now - self._last_request_at)
                    if wait > 0:
                        self._sleeper(wait)
                        now = max(
                            float(self._monotonic()),
                            self._last_request_at + required_interval,
                        )
                self._last_request_at = now
                response = getattr(client, method)(*args, **kwargs)
                try:
                    return self._ok(response, operation or method)
                except THSDataSourceError as exc:
                    message = str(exc).lower()
                    transient = any(
                        token in message
                        for token in (
                            "请求超时", "timeout", "timed out",
                            "temporarily unavailable", "connection reset",
                        )
                    )
                    if not transient or attempt >= 2:
                        raise
                    # A timed-out THSDK socket can remain established but stop
                    # serving every subsequent request. Reconnect before the
                    # retry instead of spending another 30 seconds on the same
                    # dead session.
                    try:
                        client.disconnect()
                    finally:
                        self._client = None
                    self._sleeper(0.1 * (attempt + 1))
            raise AssertionError("unreachable THSDK retry loop")

    @staticmethod
    def _raw_rows(response: Any) -> list[dict[Any, Any]]:
        """Extract numeric protocol keys before THSDK's sometimes-garbled map."""
        raw = getattr(response, "_raw_json", None)
        if isinstance(raw, str):
            try:
                payload = json.loads(raw).get("payload", {})
                result = payload.get("result", []) if isinstance(payload, dict) else []
                if isinstance(result, dict):
                    result = [result]
                if isinstance(result, list) and all(isinstance(row, dict) for row in result):
                    converted: list[dict[Any, Any]] = []
                    for row in result:
                        converted.append({int(k) if str(k).isdigit() else k: v for k, v in row.items()})
                    return converted
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        data = getattr(response, "data", None)
        if isinstance(data, pd.DataFrame):
            return data.to_dict("records")
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        return []

    @staticmethod
    def _frame(response: Any) -> pd.DataFrame:
        return pd.DataFrame(THSDataSource._raw_rows(response))

    @staticmethod
    def _pick(frame: pd.DataFrame, candidates: Iterable[Any]) -> Any | None:
        """Pick a field by numeric id, normal name, or package mojibake name."""
        if frame.empty:
            return None
        expanded: list[Any] = []
        for candidate in candidates:
            expanded.append(candidate)
            if isinstance(candidate, str):
                try:
                    expanded.append(candidate.encode("latin1").decode("gbk"))
                except (UnicodeEncodeError, UnicodeDecodeError):
                    pass
        for candidate in expanded:
            if candidate in frame.columns:
                return candidate
        return None

    @staticmethod
    def _number(value: Any, name: str) -> float:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(parsed) or float(parsed) in THSDataSource.MISSING_SENTINELS:
            raise THSDataSourceError(f"THSDK returned invalid {name}")
        return float(parsed)

    @staticmethod
    def _number_or_zero(value: Any) -> float:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return 0.0 if pd.isna(parsed) else float(parsed)

    @staticmethod
    def _date(value: str | date | datetime) -> datetime:
        try:
            parsed = pd.Timestamp(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid date: {value!r}") from exc
        if pd.isna(parsed):
            raise ValueError(f"invalid date: {value!r}")
        # THSDK accepts a naive datetime and applies Asia/Shanghai internally.
        return parsed.to_pydatetime().replace(tzinfo=None)

    @classmethod
    def _date_key(cls, value: str | date | datetime) -> str:
        return cls._date(value).strftime("%Y%m%d")

    @classmethod
    def _select_completed_code(cls, value: str, candidates: Iterable[str]) -> str:
        candidates = list(dict.fromkeys(item.upper() for item in candidates))
        if not candidates:
            raise THSDataSourceError(f"THSDK did not return an A-share code for {value}")
        if len(candidates) == 1:
            return candidates[0]
        preferred = "USHA" if value.startswith(("6", "68")) else "USZA" if value.startswith(("0", "3")) else ""
        ranked = sorted(
            candidates,
            key=lambda item: (
                0 if item[:4] == preferred else 1,
                0 if item[:4] in cls.ACTIVE_MARKETS else 1,
                item,
            ),
        )
        if len(ranked) > 1 and ranked[0][:4] == ranked[1][:4]:
            raise THSDataSourceError(f"THSDK code completion for {value} is ambiguous: {candidates}")
        return ranked[0]

    def complete_code(self, code: str) -> str:
        value = str(code).strip().upper()
        if len(value) == 10 and value[-6:].isdigit():
            return value
        if len(value) != 6 or not value.isdigit():
            raise ValueError("A-share code must be six digits or a 10-character THSCODE")
        cached = self._completed_codes.get(value)
        if cached:
            return cached

        response = self._request("complete_ths_code", value)
        candidates: list[str] = []
        for row in self._raw_rows(response):
            candidate = row.get(5) or row.get("5") or row.get("THSCODE") or row.get("ths_code")
            if candidate is None:
                # Fallback for the decoded Response object.
                candidate = row.get("代码") or row.get("代碼")
            if isinstance(candidate, str) and len(candidate) == 10 and candidate[-6:] == value:
                market = candidate[:4].upper()
                if market in self.STOCK_MARKETS:
                    candidates.append(candidate.upper())
        selected = self._select_completed_code(value, candidates)
        self._completed_codes[value] = selected
        return selected

    def complete_codes(self, codes: Iterable[str], batch_size: int = 500) -> dict[str, str]:
        """Resolve many six-digit codes with batched THSDK completion requests."""
        values = list(dict.fromkeys(str(code).strip().upper() for code in codes))
        invalid = [value for value in values if len(value) != 6 or not value.isdigit()]
        if invalid:
            raise ValueError(f"invalid A-share codes: {invalid[:10]}")
        result = {value: self._completed_codes[value] for value in values if value in self._completed_codes}
        pending = [value for value in values if value not in result]
        for offset in range(0, len(pending), max(1, int(batch_size))):
            batch = pending[offset : offset + max(1, int(batch_size))]
            response = self._request("complete_ths_code", batch)
            grouped = {value: [] for value in batch}
            for row in self._raw_rows(response):
                candidate = row.get(5) or row.get("5") or row.get("THSCODE") or row.get("ths_code")
                if candidate is None:
                    candidate = row.get("代码") or row.get("代碼")
                if not isinstance(candidate, str) or len(candidate) != 10:
                    continue
                code = candidate[-6:]
                if code in grouped and candidate[:4].upper() in self.STOCK_MARKETS:
                    grouped[code].append(candidate.upper())
            for value in batch:
                selected = self._select_completed_code(value, grouped[value])
                result[value] = selected
                self._completed_codes[value] = selected
        return result

    @classmethod
    def _normalize_asset_catalog(
        cls,
        response: Any,
        *,
        asset_type: str,
        allowed_markets: frozenset[str],
    ) -> dict[str, dict[str, Any]]:
        catalog: dict[str, dict[str, Any]] = {}
        for row in cls._raw_rows(response):
            ths_code = row.get(5) or row.get("5") or row.get("THSCODE") or row.get("代码")
            name = row.get(55) or row.get("55") or row.get("name") or row.get("名称") or ""
            if not isinstance(ths_code, str) or len(ths_code) != 10:
                continue
            ths_code = ths_code.upper()
            market, code = ths_code[:4], ths_code[-6:]
            if market not in allowed_markets or not code.isdigit():
                continue
            catalog[code] = {
                "code": code,
                "ths_code": ths_code,
                "name": str(name),
                "asset_type": asset_type,
            }
        if not catalog:
            raise THSDataSourceError(f"THSDK returned an empty {asset_type} catalog")
        return catalog

    def fetch_index_catalog(self, kind: str) -> dict[str, dict[str, Any]]:
        """Return normalized THS industry or concept index metadata."""
        kind = str(kind).strip().lower()
        if kind not in {"industry", "concept"}:
            raise ValueError("index catalog kind must be 'industry' or 'concept'")
        response = self._request("ths_industry" if kind == "industry" else "ths_concept")
        return self._normalize_asset_catalog(
            response,
            asset_type=kind,
            allowed_markets=frozenset({"URFI"}),
        )

    def fetch_etf_universe(self) -> dict[str, dict[str, Any]]:
        """Return the THS ETF/LOF catalog with an explicit T+0 marker."""
        catalog = self._normalize_asset_catalog(
            self._request("fund_etf_lists"),
            asset_type="etf",
            allowed_markets=frozenset({"USHJ", "USZJ"}),
        )
        t0_catalog = self._normalize_asset_catalog(
            self._request("fund_etf_t0_lists"),
            asset_type="etf",
            allowed_markets=frozenset({"USHJ", "USZJ"}),
        )
        t0_codes = set(t0_catalog)
        for code, metadata in catalog.items():
            metadata["t0"] = code in t0_codes
            name_upper = str(metadata.get("name", "")).upper()
            market = str(metadata.get("ths_code", ""))[:4]
            exchange_etf_code = (
                (market == "USZJ" and code.startswith("159"))
                or (market == "USHJ" and code.startswith(("51", "52", "56", "58")))
            )
            if "LOF" in name_upper:
                subtype = "lof"
            elif "ETF" in name_upper or exchange_etf_code:
                subtype = "etf"
            else:
                subtype = "fund"
            metadata["subtype"] = subtype
            metadata["selection_eligible"] = subtype == "etf"
        return catalog

    def fetch_market_history(
        self,
        ths_code: str,
        start: str | date | datetime,
        end: str | date | datetime,
        *,
        asset_type: str,
    ) -> pd.DataFrame:
        """Fetch unadjusted daily bars for an ETF or a native THS index.

        These assets deliberately bypass the A-share completion, corporate
        action, valuation and circulating-capital paths.  Their ``close`` and
        ``close_raw`` columns share the same unadjusted price scale.
        """
        asset_type = str(asset_type).strip().lower()
        if asset_type not in {"etf", "industry", "concept", "index"}:
            raise ValueError("asset_type must be etf, industry, concept, or index")
        value = str(ths_code).strip().upper()
        allowed_markets = {
            "etf": {"USHJ", "USZJ"},
            "industry": {"URFI"},
            "concept": {"URFI"},
            "index": {"USHI", "USZI"},
        }[asset_type]
        if len(value) != 10 or value[:4] not in allowed_markets or not value[-6:].isdigit():
            raise ValueError(f"invalid {asset_type} THS code: {ths_code!r}")

        start_at = self._date(start)
        end_at = self._date(end)
        if start_at > end_at:
            raise ValueError("start date must not be after end date")
        params = {
            "code": value,
            "interval": "day",
            "start_time": start_at.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_at.strftime("%Y-%m-%d %H:%M:%S"),
        }
        provider = "thsdk"
        bridge = self._history_bridge_or_none() if asset_type in {"industry", "concept"} else None
        if bridge is not None:
            request = "&".join(
                (
                    "id=210",
                    f"market={value[:4]}",
                    f"code={value[4:]}",
                    f"start={start_at.strftime('%Y%m%d')}",
                    f"end={end_at.strftime('%Y%m%d')}",
                    "datatype=1,7,8,9,11,13,19",
                    f"period={self.DAILY_PERIOD}",
                )
            )
            rows: list[dict[str, Any]] = []
            for attempt in range(3):
                try:
                    rows = bridge.query(request)
                    break
                except Exception as exc:
                    transient = "NullReferenceException" in str(exc)
                    if transient and attempt < 2:
                        self._sleeper(0.1 * (attempt + 1))
                        continue
                    raise THSHistoryPermissionError(
                        f"Yuanhang {asset_type}_klines failed: {type(exc).__name__}: {exc}"
                    ) from exc
            raw = self._normalize_kline(SimpleNamespace(data=rows))
            provider = "yuanhang"
        else:
            raw_response = self._request(
                "call", "klines", {**params, "adjust": ""}, operation=f"{asset_type}_raw_klines"
            )
            raw = self._normalize_kline(raw_response)
        if asset_type == "etf":
            adjusted_response = self._request(
                "call",
                "klines",
                {**params, "adjust": "backward"},
                operation="etf_adjusted_klines",
            )
            adjusted = self._normalize_kline(adjusted_response)
            raw_dates = raw["date"].tolist()
            if adjusted["date"].tolist() != raw_dates:
                raise THSDataSourceError("THSDK ETF raw and adjusted dates do not align")
            result = adjusted[["date", "open", "high", "low", "close"]].copy()
            result["volume"] = raw["volume"].to_numpy()
            result["amount"] = raw["amount"].to_numpy()
        else:
            result = raw[["date", "open", "high", "low", "close", "volume", "amount"]].copy()
        for column in ("open", "high", "low", "close"):
            result[f"{column}_raw"] = raw[column].to_numpy()
        amount_values = pd.to_numeric(result["amount"], errors="coerce")
        amount_missing = amount_values.isna() | amount_values.isin(self.MISSING_SENTINELS)
        if asset_type == "etf":
            volume_values = pd.to_numeric(result["volume"], errors="coerce")
            amount_missing |= (volume_values > 0) & (amount_values <= 0)
        result.loc[amount_missing, "amount"] = float("nan")
        repaired_rows: set[int] = set()
        for suffix in ("", "_raw"):
            open_column, high_column = f"open{suffix}", f"high{suffix}"
            low_column, close_column = f"low{suffix}", f"close{suffix}"
            upper = result[[open_column, high_column, close_column]].max(axis=1)
            lower = result[[open_column, low_column, close_column]].min(axis=1)
            high_gap = upper - result[high_column]
            low_gap = result[low_column] - lower
            high_relative = high_gap / upper.abs().clip(lower=0.01)
            low_relative = low_gap / lower.abs().clip(lower=0.01)
            repair_high = (high_gap > 0) & ((high_relative <= 0.001) | (high_gap <= 0.005))
            repair_low = (low_gap > 0) & ((low_relative <= 0.001) | (low_gap <= 0.005))
            if repair_high.any():
                result.loc[repair_high, high_column] = upper.loc[repair_high]
                repaired_rows.update(result.index[repair_high].tolist())
            if repair_low.any():
                result.loc[repair_low, low_column] = lower.loc[repair_low]
                repaired_rows.update(result.index[repair_low].tolist())
        amount_vwap_rejected = pd.Series(False, index=result.index)
        if asset_type == "etf":
            volume_values = pd.to_numeric(result["volume"], errors="coerce")
            amount_values = pd.to_numeric(result["amount"], errors="coerce")
            known_trades = (volume_values > 0) & amount_values.notna()
            allowance = np.maximum(1.0, volume_values * 0.001)
            minimum_amount = result["low_raw"] * volume_values
            maximum_amount = result["high_raw"] * volume_values
            amount_vwap_rejected = known_trades & (
                (amount_values < minimum_amount - allowance)
                | (amount_values > maximum_amount + allowance)
            )
            if amount_vwap_rejected.any():
                result.loc[amount_vwap_rejected, "amount"] = float("nan")
                amount_missing |= amount_vwap_rejected
        result = self._drop_incomplete_daily_bar(result)
        for column in ("market_cap", "turnover", "pe_dynamic", "pb", "ps", "pcf"):
            result[column] = pd.NA
        result["asset_type"] = asset_type
        result["ths_code"] = value
        result = result.reset_index(drop=True)
        result.attrs["source"] = provider
        result.attrs["ohlc_envelope_repaired_rows"] = len(repaired_rows)
        result.attrs["amount_missing_rows"] = int(amount_missing.sum())
        result.attrs["amount_vwap_rejected_rows"] = int(amount_vwap_rejected.sum())
        return result

    def fetch_stock_universe(self) -> dict[str, dict[str, str]]:
        """Return the THS current Shanghai/Shenzhen A-share universe."""
        response = self._request("stock_cn_lists")
        universe: dict[str, dict[str, str]] = {}
        for row in self._raw_rows(response):
            ths_code = row.get(5) or row.get("5") or row.get("THSCODE") or row.get("代码")
            name = row.get(55) or row.get("55") or row.get("name") or row.get("名称") or ""
            if not isinstance(ths_code, str) or len(ths_code) != 10:
                continue
            market, code = ths_code[:4].upper(), ths_code[-6:]
            if market not in {"USHA", "USZA", "USHT", "USZT"}:
                continue
            if not code.startswith(("00", "30", "60", "68")):
                continue
            universe[code] = {"ths_code": ths_code.upper(), "name": str(name)}
            self._completed_codes[code] = ths_code.upper()
        if not universe:
            raise THSDataSourceError("THSDK returned an empty A-share universe")
        return universe

    @staticmethod
    def _parse_dates(values: pd.Series) -> pd.Series:
        """Parse both THSDK's YYYYMMDD integers and normal datetime values."""
        text = values.astype("string").str.strip()
        eight = text.str.fullmatch(r"\d{8}")
        parsed = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
        if eight.any():
            parsed.loc[eight] = pd.to_datetime(text.loc[eight], format="%Y%m%d", errors="coerce")
        if (~eight).any():
            parsed.loc[~eight] = pd.to_datetime(values.loc[~eight], errors="coerce")
        return parsed

    @classmethod
    def _normalize_kline(cls, response: Any, raw_only: bool = False) -> pd.DataFrame:
        frame = cls._frame(response)
        fields = {
            "date": (1, "1", "时间", "date", "Date"),
            "open": (7, "7", "开盘价", "open"),
            "high": (8, "8", "最高价", "high"),
            "low": (9, "9", "最低价", "low"),
            "close": (11, "11", "收盘价", "close"),
            "volume": (13, "13", "成交量", "volume"),
            "amount": (19, "19", "总金额", "amount"),
        }
        wanted = ("date", "close") if raw_only else tuple(fields)
        if frame.empty:
            return pd.DataFrame(columns=list(wanted))
        out = pd.DataFrame(index=frame.index)
        for name in wanted:
            source = cls._pick(frame, fields[name])
            if source is None and name == "amount" and not raw_only:
                out[name] = pd.NA
            elif source is None:
                raise THSDataSourceError(f"THSDK kline response is missing {name}")
            else:
                out[name] = frame[source]
        out["date"] = cls._parse_dates(out["date"])
        out["date"] = out["date"].dt.tz_localize(None)
        if out["date"].isna().any() or out["date"].duplicated().any():
            raise THSDataSourceError("THSDK kline response contains invalid or duplicate dates")
        for name in wanted:
            if name != "date":
                out[name] = pd.to_numeric(out[name], errors="coerce")
        return out.sort_values("date").reset_index(drop=True)

    @staticmethod
    def _action_number(text: str, label: str, negative_suffix: str | None = None) -> float:
        suffix = f"(?!{re.escape(negative_suffix)})" if negative_suffix else ""
        match = re.search(rf"{re.escape(label)}{suffix}\s*([0-9.]+)", text)
        return float(match.group(1)) if match else 0.0

    @classmethod
    def _parse_corporate_actions(cls, response: Any) -> pd.DataFrame:
        frame = cls._frame(response)
        columns = [
            "date", "bonus_ratio", "cash_per_share", "rights_ratio",
            "rights_price", "consideration_stock_ratio",
            "consideration_cash_per_share", "description",
        ]
        if frame.empty:
            return pd.DataFrame(columns=columns)
        date_column = cls._pick(frame, (1, "1", "date", "Date"))
        text_column = cls._pick(frame, (471, "471", "description"))
        if date_column is None or text_column is None:
            raise THSDataSourceError("THSDK corporate_action response is missing date or description")
        labels = {
            "bonus": "\u9001",
            "capitalized": "\u8f6c\u589e",
            "cash": "\u7ea2\u5229",
            "rights": "\u914d\u80a1",
            "price": "\u4ef7",
            "consideration": "\u5bf9\u4ef7",
            "stock": "\u80a1\u7968",
            "cash_word": "\u73b0\u91d1",
        }
        rows: list[dict[str, Any]] = []
        for _, row in frame.iterrows():
            raw_description = row[text_column]
            description = "" if pd.isna(raw_description) else str(raw_description)
            parsed_date = cls._parse_dates(pd.Series([row[date_column]])).iloc[0]
            if pd.isna(parsed_date):
                continue
            rows.append(
                {
                    "date": pd.Timestamp(parsed_date).tz_localize(None),
                    "bonus_ratio": (
                        cls._action_number(description, labels["bonus"])
                        + cls._action_number(description, labels["capitalized"])
                    ) / 10.0,
                    "cash_per_share": cls._action_number(description, labels["cash"]) / 10.0,
                    "rights_ratio": cls._action_number(
                        description, labels["rights"], labels["price"]
                    ) / 10.0,
                    "rights_price": cls._action_number(
                        description, labels["rights"] + labels["price"]
                    ),
                    "consideration_stock_ratio": cls._action_number(
                        description, labels["consideration"] + labels["stock"]
                    ) / 10.0,
                    "consideration_cash_per_share": cls._action_number(
                        description, labels["consideration"] + labels["cash_word"]
                    ) / 10.0,
                    "description": description,
                }
            )
        return pd.DataFrame(rows, columns=columns).sort_values("date", kind="stable").reset_index(drop=True)

    def fetch_corporate_actions(self, code: str) -> pd.DataFrame:
        ths_code = self.complete_code(code)
        try:
            response = self._request("corporate_action", ths_code)
        except THSDataSourceError as exc:
            if "not data" in str(exc).lower():
                return self._parse_corporate_actions(None)
            raise
        return self._parse_corporate_actions(response)

    @staticmethod
    def _apply_backward_adjustment(raw: pd.DataFrame, actions: pd.DataFrame) -> pd.DataFrame:
        result = raw.copy().sort_values("date", kind="stable").reset_index(drop=True)
        if result.empty or actions is None or actions.empty:
            return result
        events = actions.copy().sort_values("date", kind="stable").reset_index(drop=True)
        multiplier = 1.0
        event_index = 0
        adjusted_rows: list[dict[str, float]] = []
        price_columns = [column for column in ("open", "high", "low", "close") if column in result]
        for row_index, row in enumerate(result.itertuples(index=False)):
            day = pd.Timestamp(row.date)
            while event_index < len(events) and pd.Timestamp(events.iloc[event_index]["date"]) <= day:
                event = events.iloc[event_index]
                capital_ratio = (
                    THSDataSource._number_or_zero(event.get("bonus_ratio", 0.0))
                    + THSDataSource._number_or_zero(event.get("rights_ratio", 0.0))
                    + THSDataSource._number_or_zero(
                        event.get("consideration_stock_ratio", 0.0)
                    )
                )
                cash = (
                    THSDataSource._number_or_zero(event.get("cash_per_share", 0.0))
                    + THSDataSource._number_or_zero(
                        event.get("consideration_cash_per_share", 0.0)
                    )
                )
                rights_ratio = THSDataSource._number_or_zero(
                    event.get("rights_ratio", 0.0)
                )
                rights_price = THSDataSource._number_or_zero(
                    event.get("rights_price", 0.0)
                )
                if row_index > 0:
                    previous_close = THSDataSource._number_or_zero(
                        result.iloc[row_index - 1].get("close")
                    )
                else:
                    previous_close = 0.0
                denominator = previous_close - cash + rights_ratio * rights_price
                if previous_close > 0 and denominator > 0:
                    event_factor = (
                        previous_close * (1.0 + capital_ratio) / denominator
                    )
                else:
                    # A partial request can begin after an old action and lack
                    # its prior close. Stock consideration still supplies a
                    # safe positive fallback; daily overlap rebasing restores
                    # the committed absolute scale.
                    event_factor = 1.0 + capital_ratio
                if not pd.notna(event_factor) or event_factor <= 0:
                    raise THSDataSourceError(
                        f"invalid corporate-action factor on {pd.Timestamp(event['date']).date()}"
                    )
                multiplier *= float(event_factor)
                event_index += 1
            adjusted_rows.append(
                {
                    column: multiplier * float(getattr(row, column))
                    for column in price_columns
                }
            )
        for column in price_columns:
            result[column] = [row[column] for row in adjusted_rows]
        return result

    @classmethod
    def _repair_minor_ohlc_envelope(
        cls, frame: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Clamp small source-side OHLC envelope errors and audit every row.

        The close/open fields are never changed.  A row whose required high or
        low correction exceeds one percent is deliberately left invalid so the
        dataset quality gate still catches material corruption.
        """
        result = frame.copy()
        repaired_dates: set[str] = set()
        details: list[dict[str, Any]] = []
        column_groups = (
            ("adjusted", ("open", "high", "low", "close")),
            ("raw", ("open_raw", "high_raw", "low_raw", "close_raw")),
        )
        for price_kind, columns in column_groups:
            if not set(columns).issubset(result.columns):
                continue
            open_col, high_col, low_col, close_col = columns
            prices = result[list(columns)].apply(pd.to_numeric, errors="coerce")
            required_high = prices[[open_col, high_col, low_col, close_col]].max(axis=1)
            required_low = prices[[open_col, high_col, low_col, close_col]].min(axis=1)
            high_gap = (required_high - prices[high_col]).clip(lower=0)
            low_gap = (prices[low_col] - required_low).clip(lower=0)
            scale = prices.abs().max(axis=1).clip(lower=1e-12)
            relative_gap = pd.concat([high_gap, low_gap], axis=1).max(axis=1) / scale
            violation = (high_gap > 0) | (low_gap > 0)
            repairable = (
                violation
                & prices.notna().all(axis=1)
                & (prices > 0).all(axis=1)
                & (relative_gap <= cls.MAX_OHLC_ENVELOPE_RELATIVE_GAP)
            )
            for index in result.index[repairable]:
                date_value = pd.Timestamp(result.loc[index, "date"]).strftime("%Y-%m-%d")
                old_high = float(prices.loc[index, high_col])
                old_low = float(prices.loc[index, low_col])
                new_high = float(required_high.loc[index])
                new_low = float(required_low.loc[index])
                result.loc[index, high_col] = new_high
                result.loc[index, low_col] = new_low
                repaired_dates.add(date_value)
                details.append(
                    {
                        "date": date_value,
                        "price_kind": price_kind,
                        "old_high": old_high,
                        "new_high": new_high,
                        "old_low": old_low,
                        "new_low": new_low,
                        "relative_gap": float(relative_gap.loc[index]),
                    }
                )
        return result, {
            "ohlc_envelope_repaired_rows": len(repaired_dates),
            "ohlc_envelope_repairs": details,
        }

    @staticmethod
    def _drop_incomplete_daily_bar(
        frame: pd.DataFrame,
        now: datetime | None = None,
    ) -> pd.DataFrame:
        """Exclude today's still-forming daily candle before the 15:00 close."""
        if frame.empty or "date" not in frame.columns:
            return frame
        current = now or datetime.now()
        if current.time() >= datetime.strptime("15:00", "%H:%M").time():
            return frame
        today = pd.Timestamp(current.date())
        dates = pd.to_datetime(frame["date"], errors="coerce").dt.tz_localize(None)
        return frame.loc[dates < today].reset_index(drop=True)

    @classmethod
    def _repair_trade_units(cls, frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
        result = frame.copy().sort_values("date", kind="stable").reset_index(drop=True)
        audit: dict[str, Any] = {
            "volume_unit_repaired_rows": 0,
            "volume_unit_regimes": [],
            "volume_unit_isolated_rows": 0,
            "volume_unit_isolated_repairs": [],
            "volume_sentinel_rows": 0,
            "amount_sentinel_rows": 0,
            "amount_invalid_rows": 0,
        }
        needed = {"date", "low_raw", "high_raw", "close_raw", "volume", "amount"}
        if result.empty or not needed.issubset(result.columns):
            return result, audit
        for column in ("low_raw", "high_raw", "close_raw", "volume", "amount"):
            result[column] = pd.to_numeric(result[column], errors="coerce")
        volume_sentinel = result["volume"].isin(cls.MISSING_SENTINELS)
        audit["volume_sentinel_rows"] = int(volume_sentinel.sum())
        result.loc[volume_sentinel, ["volume", "amount"]] = pd.NA
        sentinel = result["amount"].isin(cls.MISSING_SENTINELS)
        audit["amount_sentinel_rows"] = int(sentinel.sum())
        result.loc[sentinel, "amount"] = pd.NA
        nonpositive_amount = (result["volume"] > 0) & (
            result["amount"].isna() | (result["amount"] <= 0)
        )
        result.loc[nonpositive_amount, "amount"] = pd.NA

        def mismatch_mask() -> pd.Series:
            valid = (
                (result["volume"] > 0)
                & (result["amount"] > 0)
                & (result["low_raw"] > 0)
                & (result["high_raw"] >= result["low_raw"])
            )
            vwap = result["amount"] / result["volume"]
            return valid & (
                (vwap < result["low_raw"] * 0.98)
                | (vwap > result["high_raw"] * 1.02)
            )

        original_bad = mismatch_mask()
        original_vwap = result["amount"] / result["volume"]
        volume_multipliers = tuple(
            dict.fromkeys(
                (
                    *cls.VOLUME_UNIT_CANDIDATES,
                    *(1.0 / factor for factor in cls.VOLUME_UNIT_CANDIDATES),
                )
            )
        )
        candidate_masks: dict[float, pd.Series] = {}
        for multiplier in volume_multipliers:
            corrected_vwap = original_vwap / multiplier
            candidate_masks[multiplier] = original_bad & (
                (corrected_vwap >= result["low_raw"] * 0.98)
                & (corrected_vwap <= result["high_raw"] * 1.02)
            )

        occupied = pd.Series(False, index=result.index)
        comparable = (
            (result["volume"] > 0)
            & (result["amount"] > 0)
            & (result["low_raw"] > 0)
            & (result["high_raw"] >= result["low_raw"])
        )
        normal_counterevidence = comparable & ~original_bad
        ranked = sorted(
            volume_multipliers,
            key=lambda multiplier: int(candidate_masks[multiplier].sum()),
            reverse=True,
        )
        for multiplier in ranked:
            factor_evidence = candidate_masks[multiplier] & ~occupied
            if int(factor_evidence.sum()) < cls.MIN_VOLUME_REGIME_ROWS:
                continue
            # A valid row whose current VWAP is already inside raw low/high is
            # explicit counterevidence. Likewise, a mismatched row explained by
            # another factor must split the regime. This prevents two bad
            # clusters from swallowing a normal interval between them.
            barrier = (
                occupied
                | normal_counterevidence
                | (original_bad & ~candidate_masks[multiplier])
            )
            segment_ids = barrier.cumsum()
            for segment_id in segment_ids[factor_evidence].drop_duplicates():
                evidence = factor_evidence & (segment_ids == segment_id)
                if int(evidence.sum()) < cls.MIN_VOLUME_REGIME_ROWS:
                    continue
                first_index = int(evidence[evidence].index.min())
                last_index = int(evidence[evidence].index.max())
                window = (
                    (segment_ids == segment_id)
                    & (result.index >= first_index)
                    & (result.index <= last_index)
                    & ~occupied
                )
                bad_window = original_bad & window
                if not bad_window.any():
                    continue
                dominance = float(evidence.sum() / bad_window.sum())
                if dominance < cls.MIN_VOLUME_REGIME_SHARE:
                    continue
                repaired = window & (result["volume"] > 0)
                result.loc[repaired, "volume"] = (
                    result.loc[repaired, "volume"] * multiplier
                )
                occupied |= repaired
                count = int(repaired.sum())
                first = result.loc[first_index, "date"]
                last = result.loc[last_index, "date"]
                audit["volume_unit_repaired_rows"] += count
                audit["volume_unit_regimes"].append(
                    {
                        "start": pd.Timestamp(first).strftime("%Y-%m-%d"),
                        "end": pd.Timestamp(last).strftime("%Y-%m-%d"),
                        "factor": multiplier,
                        "rows": count,
                        "evidence_rows": int(evidence.sum()),
                    }
                )

        # An isolated tenfold archive-unit error cannot satisfy the persistent
        # regime gate above.  Repair it only when exactly one direction (x10
        # or /10) places the independently supplied amount/volume VWAP inside
        # the raw daily price envelope.  Ambiguous rows remain fail-closed.
        isolated_bad = mismatch_mask()
        if isolated_bad.any():
            isolated_vwap = result["amount"] / result["volume"]
            isolated_candidates: dict[float, pd.Series] = {}
            for multiplier in (10.0, 0.1):
                corrected_vwap = isolated_vwap / multiplier
                isolated_candidates[multiplier] = isolated_bad & (
                    (corrected_vwap >= result["low_raw"])
                    & (corrected_vwap <= result["high_raw"])
                )
            candidate_count = sum(
                mask.astype("int8") for mask in isolated_candidates.values()
            )
            for multiplier, candidate in isolated_candidates.items():
                repair = candidate & candidate_count.eq(1)
                if not repair.any():
                    continue
                result.loc[repair, "volume"] = (
                    result.loc[repair, "volume"] * multiplier
                )
                count = int(repair.sum())
                audit["volume_unit_repaired_rows"] += count
                audit["volume_unit_isolated_rows"] += count
                for row_index in result.index[repair]:
                    audit["volume_unit_isolated_repairs"].append(
                        {
                            "date": pd.Timestamp(result.at[row_index, "date"]).strftime(
                                "%Y-%m-%d"
                            ),
                            "factor": multiplier,
                        }
                    )

        unresolved = mismatch_mask()
        audit["amount_invalid_rows"] = int(unresolved.sum() + nonpositive_amount.sum())
        result.loc[unresolved, "amount"] = pd.NA
        return result, audit

    @staticmethod
    def _align_outstanding_shares(
        trading_dates: pd.DataFrame,
        shares: pd.DataFrame,
        actions: pd.DataFrame,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        price_evidence = trading_dates.copy()
        dates = trading_dates[["date"]].copy().sort_values("date", kind="stable").drop_duplicates("date")
        valid = shares[["date", "outstanding_shares"]].copy()
        valid["outstanding_shares"] = pd.to_numeric(valid["outstanding_shares"], errors="coerce")
        valid = valid.dropna().query("outstanding_shares > 0").sort_values("date", kind="stable")
        audit: dict[str, Any] = {
            "share_action_realigned_events": 0,
            "share_action_realignments": [],
        }
        if dates.empty:
            return dates.assign(outstanding_shares=pd.Series(dtype=float)), audit
        if valid.empty:
            return dates.assign(outstanding_shares=pd.NA), audit
        aligned = pd.merge_asof(
            dates,
            valid.drop_duplicates("date", keep="last"),
            on="date",
            direction="backward",
        ).reset_index(drop=True)
        previous = aligned["outstanding_shares"].shift(1)
        changed = (
            aligned["outstanding_shares"].notna()
            & previous.notna()
            & (aligned["outstanding_shares"] != previous)
        )
        transitions: list[dict[str, Any]] = []
        for index in aligned.index[changed]:
            old_value = float(previous.iloc[index])
            new_value = float(aligned.iloc[index]["outstanding_shares"])
            transitions.append(
                {
                    "index": int(index),
                    "date": pd.Timestamp(aligned.iloc[index]["date"]),
                    "old": old_value,
                    "new": new_value,
                    "ratio": new_value / old_value,
                }
            )

        used: set[int] = set()
        if {"close", "close_raw"}.issubset(price_evidence.columns):
            price_evidence = price_evidence[["date", "close", "close_raw"]].copy()
            price_evidence["close"] = pd.to_numeric(price_evidence["close"], errors="coerce")
            price_evidence["close_raw"] = pd.to_numeric(
                price_evidence["close_raw"], errors="coerce"
            )
            price_evidence = price_evidence.sort_values("date", kind="stable").drop_duplicates("date")
            price_evidence["adjusted_return"] = price_evidence["close"] / price_evidence["close"].shift(1)
            price_evidence["raw_return"] = price_evidence["close_raw"] / price_evidence["close_raw"].shift(1)
            price_evidence["mechanical_ratio"] = (
                price_evidence["adjusted_return"] / price_evidence["raw_return"]
            )
            mechanical = aligned[["date"]].merge(
                price_evidence[["date", "mechanical_ratio"]],
                on="date",
                how="left",
                validate="one_to_one",
            )
            for transition in transitions:
                expected = transition["ratio"]
                if not (expected > 0) or abs(expected - 1.0) < 0.03:
                    continue
                source_index = int(transition["index"])
                candidates: list[tuple[float, int, float]] = []
                for candidate_index in range(
                    max(1, source_index - 5), min(len(aligned), source_index + 6)
                ):
                    ratio = pd.to_numeric(
                        pd.Series([mechanical.iloc[candidate_index]["mechanical_ratio"]]),
                        errors="coerce",
                    ).iloc[0]
                    if (
                        pd.isna(ratio)
                        or float(ratio) <= 0
                        or abs(float(ratio) - 1.0) < 0.02
                    ):
                        continue
                    relative_error = abs(float(ratio) / expected - 1.0)
                    if relative_error <= 0.03:
                        candidates.append((relative_error, candidate_index, float(ratio)))
                if not candidates:
                    continue
                _, effective_index, observed = min(
                    candidates, key=lambda item: (item[0], abs(item[1] - source_index))
                )
                if source_index < effective_index:
                    aligned.loc[source_index : effective_index - 1, "outstanding_shares"] = transition["old"]
                elif source_index > effective_index:
                    aligned.loc[effective_index : source_index - 1, "outstanding_shares"] = transition["new"]
                else:
                    continue
                used.add(source_index)
                audit["share_action_realigned_events"] += 1
                audit["share_action_realignments"].append(
                    {
                        "event_date": pd.Timestamp(aligned.iloc[effective_index]["date"]).strftime(
                            "%Y-%m-%d"
                        ),
                        "effective_trading_date": pd.Timestamp(
                            aligned.iloc[effective_index]["date"]
                        ).strftime("%Y-%m-%d"),
                        "source_change_date": transition["date"].strftime("%Y-%m-%d"),
                        "expected_ratio": expected,
                        "observed_ratio": observed,
                        "evidence": "price_discontinuity",
                    }
                )

        events = (
            actions.copy().sort_values("date", kind="stable")
            if actions is not None and not actions.empty
            else pd.DataFrame(columns=["date"])
        )
        for event_date, group in events.groupby("date", sort=True):
            # A partial history request cannot safely match an old action to a
            # later, unrelated share transition merely because their ratios
            # happen to be similar.
            if pd.Timestamp(event_date) < pd.Timestamp(aligned.iloc[0]["date"]) - pd.Timedelta(days=7):
                continue
            expected = 1.0
            for _, event in group.iterrows():
                expected *= (
                    1.0
                    + THSDataSource._number_or_zero(event.get("bonus_ratio", 0.0))
                    + THSDataSource._number_or_zero(event.get("rights_ratio", 0.0))
                    + THSDataSource._number_or_zero(
                        event.get("consideration_stock_ratio", 0.0)
                    )
                )
            if abs(expected - 1.0) < 0.005:
                continue
            eligible_dates = aligned.index[aligned["date"] >= pd.Timestamp(event_date)]
            if len(eligible_dates) == 0:
                continue
            effective_index = int(eligible_dates[0])
            candidates = [
                transition
                for transition in transitions
                if transition["index"] not in used
                and abs(transition["index"] - effective_index) <= 5
                and abs(transition["ratio"] / expected - 1.0) <= 0.03
            ]
            if not candidates:
                continue
            selected = min(
                candidates,
                key=lambda transition: (
                    abs(transition["ratio"] / expected - 1.0),
                    abs(transition["index"] - effective_index),
                ),
            )
            used.add(selected["index"])
            source_index = int(selected["index"])
            if source_index < effective_index:
                aligned.loc[source_index : effective_index - 1, "outstanding_shares"] = selected["old"]
            elif source_index > effective_index:
                aligned.loc[effective_index : source_index - 1, "outstanding_shares"] = selected["new"]
            else:
                continue
            audit["share_action_realigned_events"] += 1
            audit["share_action_realignments"].append(
                {
                    "event_date": pd.Timestamp(event_date).strftime("%Y-%m-%d"),
                    "effective_trading_date": pd.Timestamp(
                        aligned.iloc[effective_index]["date"]
                    ).strftime("%Y-%m-%d"),
                    "source_change_date": selected["date"].strftime("%Y-%m-%d"),
                    "expected_ratio": expected,
                    "observed_ratio": selected["ratio"],
                    "evidence": "corporate_action",
                }
            )
        return aligned, audit

    def fetch_klines(
        self,
        code: str,
        start: str | date | datetime,
        end: str | date | datetime,
        *,
        corporate_actions: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        ths_code = self.complete_code(code)
        start_time, end_time = self._date(start), self._date(end)
        if start_time > end_time:
            raise ValueError("start must not be after end")
        kwargs = {"start_time": start_time, "end_time": end_time, "interval": "day"}
        adjusted = self._normalize_kline(self._request("klines", ths_code, adjust="backward", **kwargs))
        raw = self._normalize_kline(self._request("klines", ths_code, adjust="", **kwargs))
        raw = raw.rename(
            columns={
                "open": "open_raw",
                "high": "high_raw",
                "low": "low_raw",
                "close": "close_raw",
                "volume": "volume_raw",
                "amount": "amount_raw",
            }
        )
        if adjusted.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount", "close_raw"])
        if set(adjusted["date"]) != set(raw["date"]):
            raise THSDataSourceError("adjusted and unadjusted THSDK K-lines have different dates")
        result = adjusted.merge(raw, on="date", how="left", validate="one_to_one")
        # Raw volume/amount are the auditable exchange-unit fields.  THSDK's
        # adjusted response currently returns the same values, but selecting
        # them explicitly avoids a future volume-adjustment default change.
        result["volume"] = pd.to_numeric(result["volume_raw"], errors="coerce")
        result["amount"] = pd.to_numeric(result["amount_raw"], errors="coerce")

        market = ths_code[:4]
        comparable = result[["close", "close_raw"]].dropna()
        looks_unadjusted = (
            not comparable.empty
            and ((comparable["close"] - comparable["close_raw"]).abs() <= 1e-10).all()
        )
        adjusted_price_columns = [column for column in ("open", "high", "low", "close") if column in result]
        has_nonpositive_adjusted_price = bool(
            adjusted_price_columns
            and (result[adjusted_price_columns].apply(pd.to_numeric, errors="coerce") <= 0).any().any()
        )
        rebuilt = False
        actions = corporate_actions
        needs_reconstruction = (
            (market not in self.ACTIVE_MARKETS and looks_unadjusted)
            or has_nonpositive_adjusted_price
        )
        if needs_reconstruction:
            actions = (
                actions
                if actions is not None and not actions.empty
                else self.fetch_corporate_actions(ths_code)
            )
            if actions is not None and not actions.empty:
                raw_prices = result[["date", "open_raw", "high_raw", "low_raw", "close_raw"]].rename(
                    columns={
                        "open_raw": "open",
                        "high_raw": "high",
                        "low_raw": "low",
                        "close_raw": "close",
                    }
                )
                adjusted_prices = self._apply_backward_adjustment(raw_prices, actions)
                for column in ("open", "high", "low", "close"):
                    result[column] = adjusted_prices[column].to_numpy()
                rebuilt = True

        result, ohlc_audit = self._repair_minor_ohlc_envelope(result)
        result, trade_audit = self._repair_trade_units(result)
        result = self._drop_incomplete_daily_bar(result)
        result.attrs["quality_audit"] = {
            **trade_audit,
            **ohlc_audit,
            "price_adjustment_reconstructed": rebuilt,
            "corporate_action_count": int(len(actions)) if actions is not None else 0,
            "ths_code": ths_code,
        }
        return result

    def fetch_realtime(self, code: str) -> dict[str, Any]:
        ths_code = self.complete_code(code)
        params = {
            "id": 202,
            "codelist": ths_code[4:],
            "market": ths_code[:4],
            "datatype": ",".join(str(x) for x in self.CURRENT_FIELD_IDS),
            "service": "zhu",
        }
        frame = self._frame(self._request("query_data", params))
        if frame.empty:
            raise THSDataSourceError(f"THSDK returned no realtime row for {code}")
        row = frame.iloc[0]
        def field(ids: tuple[Any, ...], name: str, required: bool = True) -> float | None:
            column = self._pick(frame, ids)
            if column is None:
                if required:
                    raise THSDataSourceError(f"THSDK realtime response is missing {name}")
                return None
            try:
                return self._number(row[column], name)
            except THSDataSourceError:
                if required:
                    raise
                return None
        return {
            "ths_code": ths_code,
            "turnover": field((1968584, "1968584", "换手率", "turnover", "turnover_rate"), "turnover"),
            "market_cap": field((3475914, "3475914", "流通市值", "market_cap"), "circulating market cap"),
            "total_market_cap": field((3541450, "3541450", "总市值", "total_market_cap"), "total market cap", False),
            "pe_dynamic": field((self.PE_TTM_FIELD_ID, str(self.PE_TTM_FIELD_ID)), "PE TTM", False),
            "pb": field((self.PB_FIELD_ID, str(self.PB_FIELD_ID)), "PB", False),
            "ps": field((self.PS_TTM_FIELD_ID, str(self.PS_TTM_FIELD_ID)), "PS TTM", False),
        }

    def fetch_realtime_batch(self, codes: Iterable[str], batch_size: int = 100) -> dict[str, dict[str, Any]]:
        """Fetch current turnover/caps in market-homogeneous batches."""
        values = list(dict.fromkeys(str(code).strip().zfill(6) for code in codes))
        try:
            completed = self.complete_codes(values)
        except THSDataSourceError:
            # A single obsolete code must not suppress snapshots for the rest
            # of a batch. Resolve individually and retain auditable omissions.
            completed = {}
            for value in values:
                try:
                    completed[value] = self.complete_code(value)
                except THSDataSourceError:
                    continue
        groups: dict[str, list[str]] = {}
        for code, ths_code in completed.items():
            groups.setdefault(ths_code[:4], []).append(code)
        output: dict[str, dict[str, Any]] = {}
        for market, market_codes in groups.items():
            for offset in range(0, len(market_codes), max(1, int(batch_size))):
                batch = market_codes[offset : offset + max(1, int(batch_size))]
                params = {
                    "id": 202,
                    "codelist": ",".join(batch),
                    "market": market,
                    "datatype": ",".join(str(x) for x in self.CURRENT_FIELD_IDS),
                    "service": "zhu",
                }
                frame = self._frame(self._request("query_data", params))
                if frame.empty:
                    continue
                code_column = self._pick(frame, (5, "5", "代码", "code", "THSCODE"))
                turnover_column = self._pick(frame, (1968584, "1968584", "换手率", "turnover", "turnover_rate"))
                cap_column = self._pick(frame, (3475914, "3475914", "流通市值", "market_cap"))
                total_column = self._pick(frame, (3541450, "3541450", "总市值", "total_market_cap"))
                pe_column = self._pick(frame, (self.PE_TTM_FIELD_ID, str(self.PE_TTM_FIELD_ID)))
                pb_column = self._pick(frame, (self.PB_FIELD_ID, str(self.PB_FIELD_ID)))
                ps_column = self._pick(frame, (self.PS_TTM_FIELD_ID, str(self.PS_TTM_FIELD_ID)))
                if turnover_column is None or cap_column is None:
                    continue
                for _, row in frame.iterrows():
                    raw_code = str(row[code_column]) if code_column is not None else (batch[0] if len(batch) == 1 else "")
                    code = raw_code[-6:]
                    if code not in batch:
                        continue
                    turnover = pd.to_numeric(pd.Series([row[turnover_column]]), errors="coerce").iloc[0]
                    cap = pd.to_numeric(pd.Series([row[cap_column]]), errors="coerce").iloc[0]
                    total = pd.to_numeric(pd.Series([row[total_column]]), errors="coerce").iloc[0] if total_column is not None else pd.NA

                    def optional_number(column: Any | None) -> float | None:
                        if column is None:
                            return None
                        value = pd.to_numeric(pd.Series([row[column]]), errors="coerce").iloc[0]
                        if pd.isna(value):
                            return None
                        result = float(value)
                        if result in self.MISSING_SENTINELS or not math.isfinite(result):
                            return None
                        return result

                    if (
                        pd.isna(turnover)
                        or pd.isna(cap)
                        or float(turnover) in self.MISSING_SENTINELS
                        or float(cap) in self.MISSING_SENTINELS
                        or float(turnover) < 0
                        or float(turnover) > 1000
                    ):
                        continue
                    output[code] = {
                        "ths_code": completed[code],
                        "turnover": float(turnover),
                        "market_cap": float(cap),
                        "total_market_cap": None if pd.isna(total) or float(total) in self.MISSING_SENTINELS else float(total),
                        "pe_dynamic": optional_number(pe_column),
                        "pb": optional_number(pb_column),
                        "ps": optional_number(ps_column),
                    }
        return output

    def _historical_field(
        self,
        code: str,
        start: str | date | datetime,
        end: str | date | datetime,
        request_id: int,
        datatype: int,
        output_name: str,
        candidates: tuple[Any, ...],
    ) -> pd.DataFrame:
        ths_code = self.complete_code(code)
        start_key, end_key = self._date_key(start), self._date_key(end)
        if start_key > end_key:
            raise ValueError("start must not be after end")
        params = {
            "id": request_id,
            "market": ths_code[:4],
            "code": ths_code[4:],
            "start": start_key,
            "end": end_key,
            "datatype": f"1,{datatype}",
            "period": self.DAILY_PERIOD,
            "service": "zhu",
        }
        bridge = self._history_bridge_or_none()
        if bridge is None:
            frame = self._frame(self._request("query_data", params, operation=f"historical_{output_name}"))
        else:
            request = "&".join(
                (
                    f"id={request_id}",
                    f"market={ths_code[:4]}",
                    f"code={ths_code[4:]}",
                    f"start={start_key}",
                    f"end={end_key}",
                    f"datatype=1,{datatype}",
                    f"period={self.DAILY_PERIOD}",
                )
            )
            frame = pd.DataFrame()
            for attempt in range(3):
                try:
                    frame = pd.DataFrame(bridge.query(request))
                    break
                except Exception as exc:
                    message = str(exc)
                    if "EmptyResponse" in message:
                        frame = pd.DataFrame()
                        break
                    transient = "NullReferenceException" in message
                    if transient and attempt < 2:
                        self._sleeper(0.1 * (attempt + 1))
                        continue
                    raise THSHistoryPermissionError(
                        f"Yuanhang historical_{output_name} failed: {type(exc).__name__}: {exc}"
                    ) from exc
        if frame.empty:
            return pd.DataFrame(columns=["date", output_name])
        date_column = self._pick(frame, (1, "1", "时间", "date", "Date"))
        value_column = self._pick(frame, (datatype, str(datatype), *candidates))
        if date_column is None or value_column is None:
            raise THSDataSourceError(f"THSDK historical response is missing {output_name}")
        out = pd.DataFrame({
            "date": self._parse_dates(frame[date_column]),
            output_name: pd.to_numeric(frame[value_column], errors="coerce"),
        })
        if output_name == "turnover":
            invalid = out[output_name].isin(self.MISSING_SENTINELS) | (out[output_name] < 0) | (out[output_name] > 1000)
            out.loc[invalid, output_name] = pd.NA
        elif output_name == "outstanding_shares":
            invalid = out[output_name].isin(self.MISSING_SENTINELS) | (out[output_name] <= 0)
            out.loc[invalid, output_name] = pd.NA
        out["date"] = out["date"].dt.tz_localize(None)
        if out["date"].isna().any():
            raise THSDataSourceError("THSDK historical response contains invalid dates")
        # Yuanhang can transiently emit multiple field snapshots for one day.
        # Preserve response order and use the last snapshot deterministically;
        # K-line duplicates remain a hard error in _normalize_kline.
        return (
            out.sort_values("date", kind="stable")
            .drop_duplicates("date", keep="last")
            .reset_index(drop=True)
        )

    def fetch_turnover_history(self, code: str, start: str | date | datetime, end: str | date | datetime) -> pd.DataFrame:
        return self._historical_field(code, start, end, 212, self.TURNOVER_FIELD_ID, "turnover", ("换手率", "turnover", "turnover_rate"))

    def fetch_outstanding_shares_history(self, code: str, start: str | date | datetime, end: str | date | datetime) -> pd.DataFrame:
        return self._historical_field(code, start, end, 211, self.OUTSTANDING_SHARES_FIELD_ID, "outstanding_shares", ("流通股本", "outstanding_shares"))

    def fetch_historical_fields(self, code: str, start: str | date | datetime, end: str | date | datetime) -> pd.DataFrame:
        turnover = self.fetch_turnover_history(code, start, end)
        shares = self.fetch_outstanding_shares_history(code, start, end)
        return turnover.merge(shares, on="date", how="outer", validate="one_to_one").sort_values("date").reset_index(drop=True)

    def fetch_history(self, code: str, start: str | date | datetime, end: str | date | datetime) -> pd.DataFrame:
        ths_code = self.complete_code(code)
        actions = (
            self.fetch_corporate_actions(ths_code)
            if ths_code[:4] not in self.ACTIVE_MARKETS
            else self._parse_corporate_actions(None)
        )
        prices = self.fetch_klines(ths_code, start, end, corporate_actions=actions)
        if prices.empty:
            return prices.assign(turnover=pd.Series(dtype=float), market_cap=pd.Series(dtype=float))
        turnover = self.fetch_turnover_history(code, start, end)
        shares = self.fetch_outstanding_shares_history(code, start, end)
        result = prices.merge(turnover, on="date", how="left", validate="one_to_one")

        valid_shares = shares.dropna(subset=["date", "outstanding_shares"]).copy()
        valid_shares = valid_shares[valid_shares["outstanding_shares"] > 0]
        valid_shares = valid_shares.sort_values("date").drop_duplicates("date", keep="last")
        aligned, share_audit = self._align_outstanding_shares(
            result[["date", "close", "close_raw"]], valid_shares, actions
        )
        result = result.merge(aligned, on="date", how="left", validate="one_to_one")

        direct_turnover = pd.to_numeric(result["turnover"], errors="coerce")
        outstanding = pd.to_numeric(result["outstanding_shares"], errors="coerce")
        volume = pd.to_numeric(result["volume"], errors="coerce")
        valid_outstanding = outstanding.notna() & (outstanding > 0) & volume.notna() & (volume >= 0)
        derived_turnover = volume * 100.0 / outstanding
        result["turnover"] = derived_turnover.where(valid_outstanding, direct_turnover)

        positive_volume = volume > 0
        missing_turnover = positive_volume & (
            pd.to_numeric(result["turnover"], errors="coerce").isna()
            | (pd.to_numeric(result["turnover"], errors="coerce") <= 0)
        )
        if missing_turnover.any():
            missing = result.loc[missing_turnover, "date"].dt.strftime("%Y-%m-%d").tolist()
            raise THSDataSourceError(f"THS turnover/share history is missing for dates: {missing[:10]}")

        result["market_cap"] = float("nan")
        close_raw = pd.to_numeric(result["close_raw"], errors="coerce")
        result.loc[valid_outstanding & (close_raw > 0), "market_cap"] = (
            close_raw[valid_outstanding & (close_raw > 0)]
            * outstanding[valid_outstanding & (close_raw > 0)]
        )
        fallback = (
            result["market_cap"].isna()
            & (close_raw > 0)
            & positive_volume
            & (pd.to_numeric(result["turnover"], errors="coerce") > 0)
        )
        result.loc[fallback, "market_cap"] = (
            close_raw[fallback]
            * volume[fallback]
            * 100.0
            / pd.to_numeric(result.loc[fallback, "turnover"], errors="coerce")
        )
        result = result.drop(
            columns=[
                "outstanding_shares", "open_raw", "high_raw", "low_raw",
                "volume_raw", "amount_raw",
            ],
            errors="ignore",
        )
        previous = result["close"].shift(1)
        result["change"] = result["close"] - previous
        result["change_pct"] = (result["close"] / previous - 1.0) * 100.0
        result["amplitude"] = (result["high"] - result["low"]) / previous * 100.0
        result.loc[previous.isna() | (previous <= 0), ["change", "change_pct", "amplitude"]] = pd.NA
        result.attrs["quality_audit"] = {
            **prices.attrs.get("quality_audit", {}),
            **share_audit,
            "direct_turnover_rows": int(direct_turnover.notna().sum()),
            "derived_turnover_rows": int(valid_outstanding.sum()),
        }
        return result

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._client is not None:
                self._client.disconnect()
            if self._history_bridge is not None:
                self._history_bridge.close()

    def __enter__(self) -> "THSDataSource":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()


__all__ = ["THSDataSource", "THSDataSourceError", "THSHistoryPermissionError"]
