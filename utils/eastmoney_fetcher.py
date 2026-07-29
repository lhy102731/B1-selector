"""
Eastmoney public market-data helper.

This module intentionally uses public HTTP endpoints only. It does not load or
execute the Eastmoney desktop client binaries.

Verified kline fields:
    f51 date
    f52 open
    f53 close
    f54 high
    f55 low
    f56 volume in lots
    f57 amount in yuan
    f58 amplitude percent
    f59 change percent
    f60 change amount
    f61 turnover percent
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd
import requests


REQUEST_TIMEOUT = 15


def secid_for_code(code: str) -> str:
    code = str(code).zfill(6)
    market = "1" if code.startswith("6") else "0"
    return f"{market}.{code}"


@dataclass
class EastmoneyQuote:
    code: str
    name: str | None = None
    market_cap: int = 0
    circ_market_cap: int = 0
    pe_dynamic: float | None = None
    pb: float | None = None
    latest_price: float | None = None
    previous_close: float | None = None


class EastmoneyFetcher:
    def __init__(self):
        self.session = requests.Session()
        # Windows desktop proxy settings often point at an optional local
        # proxy process.  Scheduled market-data jobs must use the public
        # domestic endpoints directly instead of depending on that process.
        self.session.trust_env = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "application/json,text/plain,*/*",
        })

    def fetch_kline(
        self,
        code: str,
        start: str,
        end: str | None = None,
        adjust: str = "hfq",
    ) -> pd.DataFrame:
        """
        Fetch daily K-line data.

        adjust:
            "none" -> fqt=0
            "qfq"  -> fqt=1
            "hfq"  -> fqt=2
        """
        fqt_map = {"none": "0", "qfq": "1", "hfq": "2"}
        if adjust not in fqt_map:
            raise ValueError(f"unsupported adjust={adjust!r}")
        end = end or datetime.now().strftime("%Y%m%d")
        params = {
            "secid": secid_for_code(code),
            "klt": "101",
            "fqt": fqt_map[adjust],
            "beg": start.replace("-", ""),
            "end": end.replace("-", ""),
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        }
        resp = self.session.get(
            "https://push2his.eastmoney.com/api/qt/stock/kline/get",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json().get("data") or {}
        rows = data.get("klines") or []
        parsed = []
        for row in rows:
            parts = row.split(",")
            if len(parts) < 11:
                continue
            try:
                parsed.append({
                    "date": parts[0],
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "volume": int(float(parts[5]) * 100),
                    "amount": float(parts[6]),
                    "amplitude": float(parts[7]),
                    "change_pct": float(parts[8]),
                    "change": float(parts[9]),
                    "turnover": float(parts[10]),
                })
            except ValueError:
                continue
        if not parsed:
            return pd.DataFrame()
        df = pd.DataFrame(parsed)
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date", ascending=False)

    def fetch_recent_update(self, code: str, days: int = 10, adjust: str = "hfq") -> pd.DataFrame:
        end = datetime.now()
        start = end - timedelta(days=days + 10)
        return self.fetch_kline(
            code,
            start=start.strftime("%Y%m%d"),
            end=end.strftime("%Y%m%d"),
            adjust=adjust,
        ).head(days)

    def fetch_quote(self, code: str) -> EastmoneyQuote | None:
        params = {
            "secid": secid_for_code(code),
            "fields": ",".join([
                "f43", "f57", "f58", "f60", "f84", "f85", "f116", "f117",
                "f162", "f167", "f152",
            ]),
        }
        resp = self.session.get(
            "https://push2.eastmoney.com/api/qt/stock/get",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json().get("data") or {}
        if not data:
            return None

        scale = data.get("f152") or 2

        def scaled(v):
            if v in ("", None):
                return None
            try:
                return float(v) / (10 ** int(scale))
            except Exception:
                return None

        def scaled_2(v):
            if v in ("", None):
                return None
            try:
                return float(v) / 100.0
            except Exception:
                return None

        return EastmoneyQuote(
            code=str(data.get("f57") or code).zfill(6),
            name=data.get("f58"),
            market_cap=int(float(data.get("f116") or 0)),
            circ_market_cap=int(float(data.get("f117") or 0)),
            pe_dynamic=scaled_2(data.get("f162")),
            pb=scaled_2(data.get("f167")),
            latest_price=scaled(data.get("f43")),
            previous_close=scaled(data.get("f60")),
        )

    @staticmethod
    def rescale_ohlc_to_anchor(df: pd.DataFrame, anchor_date: str, anchor_close: float) -> tuple[pd.DataFrame, float | None]:
        """
        Scale Eastmoney adjusted OHLC to the current local adjusted baseline.

        Eastmoney and baostock both provide adjusted prices, but their adjustment
        bases differ. If the local CSV already has a clean adjusted close on an
        overlapping date, multiply Eastmoney OHLC by:

            local_close(anchor_date) / eastmoney_close(anchor_date)
        """
        if df.empty:
            return df, None
        anchor_ts = pd.to_datetime(anchor_date)
        match = df[pd.to_datetime(df["date"]).dt.normalize() == anchor_ts.normalize()]
        if match.empty:
            return df, None
        em_close = float(match.iloc[0]["close"])
        if em_close <= 0:
            return df, None
        factor = float(anchor_close) / em_close
        out = df.copy()
        for col in ["open", "high", "low", "close", "change"]:
            if col in out.columns:
                out[col] = out[col].astype(float) * factor
        return out, factor


def fetch_many_klines(codes: Iterable[str], days: int = 10, adjust: str = "hfq") -> dict[str, pd.DataFrame]:
    fetcher = EastmoneyFetcher()
    result = {}
    for code in codes:
        result[str(code).zfill(6)] = fetcher.fetch_recent_update(str(code), days=days, adjust=adjust)
    return result
