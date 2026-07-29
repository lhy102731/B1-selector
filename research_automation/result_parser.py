"""result_parser.py -- Phase 5 standardized backtest result parser.

Reads result/metrics.json, report.md, equity.csv, trades.csv in priority order and
produces a normalized StandardMetrics object. Pure stdlib (csv/json/re); no pandas.
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from .experiment import StandardMetrics

# Canonical metric -> list of accepted aliases found in json/report files.
_ALIASES = {
    "sharpe": ["sharpe", "sharpe_ratio", "夏普"],
    "cagr": ["cagr", "annual_return", "annualized_return", "年化", "年化收益"],
    "win_rate": ["win_rate", "winrate", "胜率"],
    "max_drawdown": ["max_drawdown", "mdd", "maxdd", "最大回撤"],
    "ndcg": ["ndcg"],
    "ic": ["ic", "information_coefficient"],
    "rank_ic": ["rank_ic", "rankic"],
    "turnover": ["turnover", "换手", "换手率"],
    "trades": ["trades", "num_trades", "trade_count", "交易数", "交易次数"],
}


class BacktestResultParser:
    def parse(self, result_dir: str | Path) -> StandardMetrics:
        rd = Path(result_dir)
        m = self._from_json(rd / "metrics.json")
        if m:
            m.source = "metrics_json"
            return self._normalize(m)
        m = self._from_report(rd / "report.md")
        if m:
            m.source = "report_md"
            return self._normalize(m)
        m = self._from_equity(rd / "equity.csv", rd / "trades.csv")
        if m:
            m.source = "equity_csv"
            return self._normalize(m)
        return StandardMetrics(source="none")

    # ---- sources ----------------------------------------------------------
    def _from_json(self, path: Path) -> StandardMetrics | None:
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        return self._map_dict(data)

    def _from_report(self, path: Path) -> StandardMetrics | None:
        if not path.exists():
            return None
        text = path.read_text(encoding="utf-8", errors="ignore")
        found: dict = {}
        for canon, aliases in _ALIASES.items():
            for a in aliases:
                m = re.search(rf"{re.escape(a)}\s*[:=：]\s*(-?\d+(?:\.\d+)?)%?", text, re.IGNORECASE)
                if m:
                    found[canon] = float(m.group(1))
                    break
        return self._map_dict(found) if found else None

    def _from_equity(self, equity: Path, trades: Path) -> StandardMetrics | None:
        if not equity.exists():
            return None
        rows = self._read_csv(equity)
        col = None
        for c in ("equity", "nav", "value", "净值"):
            if rows and c in rows[0]:
                col = c
                break
        sm = StandardMetrics()
        if col:
            try:
                series = [float(r[col]) for r in rows if r.get(col) not in (None, "")]
                if len(series) >= 2 and series[0] > 0:
                    total = series[-1] / series[0]
                    sm.cagr = round(total - 1.0, 6)  # total return as a fallback proxy
                    peak = series[0]
                    mdd = 0.0
                    for v in series:
                        peak = max(peak, v)
                        mdd = min(mdd, v / peak - 1.0)
                    sm.max_drawdown = round(mdd, 6)
            except Exception:
                pass
        if trades.exists():
            try:
                trows = self._read_csv(trades)
                sm.trades = len(trows)
                pnls = [float(r["pnl"]) for r in trows if r.get("pnl") not in (None, "")]
                if pnls:
                    sm.win_rate = round(sum(1 for x in pnls if x > 0) / len(pnls), 6)
            except Exception:
                pass
        return sm

    # ---- helpers ----------------------------------------------------------
    @staticmethod
    def _read_csv(path: Path) -> list[dict]:
        with open(path, encoding="utf-8", errors="ignore", newline="") as f:
            return list(csv.DictReader(f))

    def _map_dict(self, data: dict) -> StandardMetrics:
        sm = StandardMetrics()
        lowered = {str(k).lower(): v for k, v in data.items()}
        used = set()
        for canon, aliases in _ALIASES.items():
            for a in aliases:
                if a.lower() in lowered:
                    val = lowered[a.lower()]
                    used.add(a.lower())
                    try:
                        setattr(sm, canon, int(val) if canon == "trades" else float(val))
                    except (TypeError, ValueError):
                        pass
                    break
        # keep anything we did not recognize
        sm.extra = {k: v for k, v in data.items() if str(k).lower() not in used}
        return sm

    @staticmethod
    def _normalize(sm: StandardMetrics) -> StandardMetrics:
        # normalize max_drawdown to a magnitude in [0,1] if given as percent or negative
        if sm.max_drawdown is not None:
            mdd = abs(sm.max_drawdown)
            if mdd > 1.0:  # looked like a percentage e.g. 14.4
                mdd = mdd / 100.0
            sm.max_drawdown = round(mdd, 6)
        if sm.win_rate is not None and sm.win_rate > 1.0:
            sm.win_rate = round(sm.win_rate / 100.0, 6)
        return sm
