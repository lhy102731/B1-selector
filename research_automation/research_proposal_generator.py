"""research_proposal_generator.py — Rule-based research proposal generator.

Analyses past experiment results to:
  - Detect parameter exhaustion (diminishing / negative returns)
  - Avoid repeating exhausted directions
  - Propose new research directions targeting untried parameters

Phase 5: Auto-discovers parameters from strategy/brick_chart_strategy.py's
default_params dict.  Supports sequential sweep (multiple values per parameter)
and two-parameter combination exploration when single-param catalogs are
exhausted.  No LLM, no ClaudeCodeExecutor, no strategy logic changes.
"""
from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ============================================================
# Auto-discover parameters from strategy/brick_chart_strategy.py
# ============================================================
_DISCOVERED_PARAMS: dict[str, dict] = {}

try:
    _src_path = Path(__file__).resolve().parent.parent / "strategy" / "brick_chart_strategy.py"
    if _src_path.exists():
        _text = _src_path.read_text(encoding="utf-8")
        # Search for default_params dict in __init__
        _match = re.search(r"default_params\s*=\s*\{([^}]+)\}", _text, re.DOTALL)
        if _match:
            _body = _match.group(1)
            for _line in _body.split(","):
                _line = _line.strip()
                if not _line:
                    continue
                _kv = re.match(r"'([^']+)'\s*:\s*(.+)$", _line)
                if _kv:
                    _key = _kv.group(1)
                    _val_str = _kv.group(2).strip()
                    # Skip height_ratio (handled manually with non-linear range)
                    if _key == "height_ratio":
                        continue
                    # Try to evaluate as a Python expression
                    try:
                        _val = ast.literal_eval(_val_str)
                        if isinstance(_val, (int, float)):
                            _lo = int(_val * 0.3) if isinstance(_val, int) else _val * 0.3
                            _hi = int(_val * 3.0) if isinstance(_val, int) else min(_val * 3.0, _val + 200)
                            _DISCOVERED_PARAMS[_key] = {
                                "default": _val,
                                "range": (_lo, _hi),
                                "description": f"Auto-discovered: {_key} (default={_val})",
                                "category": "default_params",
                            }
                    except Exception:
                        pass
except Exception:
    pass

# ============================================================
# Static parameter registry (manually curated ranges)
# ============================================================
_PARAMETER_REGISTRY: dict[str, dict] = {
    "height_ratio": {
        "default": 2.0 / 3.0,
        "range": (0.3, 2.0),
        "description": "Red/green brick height ratio threshold (red_height >= green_height * ratio)",
        "category": "signal_quality",
        "sweep": [0.5, 0.67, 0.8, 1.0, 1.2, 1.5],
    },
    "M1": {
        "default": 14,
        "range": (5, 30),
        "description": "Yellow line MA period 1 (shortest)",
        "category": "yellow_line",
        "sweep": [5, 7, 10, 14, 21, 30],
    },
    "M2": {
        "default": 28,
        "range": (10, 60),
        "description": "Yellow line MA period 2",
        "category": "yellow_line",
        "sweep": [10, 14, 21, 28, 42, 60],
    },
    "M3": {
        "default": 57,
        "range": (20, 120),
        "description": "Yellow line MA period 3",
        "category": "yellow_line",
        "sweep": [21, 34, 57, 89, 120],
    },
    "M4": {
        "default": 114,
        "range": (40, 250),
        "description": "Yellow line MA period 4 (longest)",
        "category": "yellow_line",
        "sweep": [55, 89, 114, 144, 233],
    },
}

# Merge discovered params into registry (avoiding duplicates)
for _k, _v in _DISCOVERED_PARAMS.items():
    if _k not in _PARAMETER_REGISTRY:
        _v["sweep"] = _v.get("sweep") or None
        _PARAMETER_REGISTRY[_k] = _v

# Sweep generation for default_params without explicit sweep
for _k, _v in list(_PARAMETER_REGISTRY.items()):
    if _v.get("sweep") is None and _v.get("category") == "default_params":
        _lo, _hi = _v["range"]
        _default = _v["default"]
        if isinstance(_default, int):
            _v["sweep"] = [max(_lo, int(_default * f)) for f in [0.5, 0.7, 1.0, 1.5, 2.0]
                          if max(_lo, int(_default * f)) <= _hi]
        else:
            _v["sweep"] = [max(_lo, _default * f) for f in [0.5, 0.7, 1.0, 1.5, 2.0]
                          if max(_lo, _default * f) <= _hi]


@dataclass
class ResearchProposal:
    """A structured research proposal generated from historical analysis."""
    hypothesis: str
    rationale: str
    target_file: str = "strategy/brick_chart_strategy.py"
    code_change: dict = field(default_factory=dict)
    priority: float = 0.5       # 0.0 = lowest, 1.0 = highest
    exhausted_params: list[str] = field(default_factory=list)
    suggested_params: list[str] = field(default_factory=list)


def _score_trend(values: list[float]) -> str:
    """Classify a trend from a list of metric values (oldest first)."""
    if len(values) < 2:
        return "insufficient_data"
    # simple linear regression slope
    n = len(values)
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    num = sum((xs[i] - mean_x) * (values[i] - mean_y) for i in range(n))
    den = sum((x - mean_x) ** 2 for x in xs)
    if den == 0:
        return "flat"
    slope = num / den
    if slope > 0.01:
        return "improving"
    elif slope < -0.01:
        return "declining"
    return "flat"


def _build_symbol_index(experiments: list[dict]) -> dict[str, list[dict]]:
    """Group experiments by the symbol they modified (from code_change or hypothesis)."""
    index: dict[str, list[dict]] = {}
    for exp in experiments:
        cc = exp.get("code_change") or {}
        symbol = cc.get("symbol")
        if not symbol:
            # try to infer from hypothesis
            hyp = exp.get("hypothesis", "")
            for param in _PARAMETER_REGISTRY:
                if param in hyp:
                    symbol = param
                    break
        if not symbol:
            symbol = "_unknown"
        index.setdefault(symbol, []).append(exp)
    return index


def _detect_exhausted(experiments: list[dict]) -> list[dict]:
    """Return params that show clear exhaustion (diminishing/negative returns).

    Each entry: {symbol, trend, values, recommendation}
    """
    exhausted = []
    by_symbol = _build_symbol_index(experiments)

    for symbol, exps in by_symbol.items():
        if len(exps) < 2:
            continue
        # sort by the parameter value (ascending)
        exps_sorted = sorted(exps, key=lambda e: e.get("param_value", 0))
        returns = [e.get("total_return", 0) or 0 for e in exps_sorted]
        trend = _score_trend(returns)

        if trend == "declining" or (trend == "flat" and max(returns) <= returns[0] * 0.1 + returns[0]
                                     and len(exps) >= 3):
            # first value is the best and everything else is same or worse → exhausted
            exhausted.append({
                "symbol": symbol,
                "trend": trend,
                "param_values": [e.get("param_value") for e in exps_sorted],
                "returns": returns,
                "best_value": exps_sorted[0].get("param_value"),
                "best_return": returns[0],
                "recommendation": f"ABANDON {symbol}: {trend} trend, best={exps_sorted[0].get('param_value')} "
                                  f"(return={returns[0]:.2f}). Try alternative parameter category.",
            })
    return exhausted


# ============================================================
# Main generator
# ============================================================

class ResearchProposalGenerator:
    """Rule-based generator: analyses past experiments and produces ResearchProposals.

    Usage:
        gen = ResearchProposalGenerator()
        proposals = gen.generate(experiments, max_proposals=3)
    """

    def __init__(self, parameter_registry: dict | None = None):
        self.registry = parameter_registry or _PARAMETER_REGISTRY

    def generate(self, experiments: list[dict],
                 max_proposals: int = 3) -> list[ResearchProposal]:
        """Analyse experiments and return ranked proposals.

        Phase 5: sequential sweep exploration + two-parameter combinations.
        Each parameter has a ``sweep`` sequence.  For partially-explored
        parameters the generator proposes the NEXT untried sweep value, not
        a one-off guess.  When single-parameter catalogs are fully exhausted,
        two-parameter combinations are generated.
        """
        exhausted = _detect_exhausted(experiments)
        exhausted_symbols = {e["symbol"] for e in exhausted}
        by_symbol = _build_symbol_index(experiments)

        # Track which values have been tried for each parameter
        tried_values: dict[str, set] = {}
        for symbol, exps in by_symbol.items():
            vals = set()
            for e in exps:
                pv = e.get("param_value")
                if pv is not None:
                    vals.add(pv)
                cc = e.get("code_change") or {}
                if cc.get("value") is not None:
                    vals.add(cc["value"])
            tried_values[symbol] = vals

        proposals = []

        # 1. Sequential sweep: for parameters with sweep sequences,
        #    propose the NEXT untried value.
        for symbol, info in self.registry.items():
            sweep = info.get("sweep")
            if not sweep:
                continue
            if symbol in exhausted_symbols and not self._has_untried_sweep(symbol, sweep, tried_values):
                continue
            # Find next untried sweep value
            trial_indices = [sweep.index(v) for v in tried_values.get(symbol, set()) if v in sweep]
            next_idx = max(trial_indices) + 1 if trial_indices else 0
            if next_idx < len(sweep):
                next_val = sweep[next_idx]
                proposals.append(ResearchProposal(
                    hypothesis=f"{symbol}={next_val} (sweep {next_idx+1}/{len(sweep)})",
                    rationale=(f"Sequential sweep of {symbol} ({info.get('description','')}). "
                               f"Default={info['default']}, range={info['range']}. "
                               f"Tried {len(tried_values.get(symbol,set()))} "
                               f"{'values: '+','.join(str(v) for v in tried_values.get(symbol,set())) if tried_values.get(symbol) else 'none'}."),
                    code_change={"change_type": "modify_constant",
                                 "file": "strategy/brick_chart_strategy.py",
                                 "symbol": symbol,
                                 "value": next_val,
                                 "old_value": str(info["default"])},
                    priority=0.9 if next_idx <= 2 else 0.7,
                    exhausted_params=[],
                    suggested_params=[symbol],
                ))

        # 2. Untried parameters (no sweep defined)
        tried_symbols = set(by_symbol.keys())
        all_untried = [s for s in self.registry if s not in tried_symbols
                       and self.registry[s].get("sweep") is None]
        for symbol in all_untried:
            info = self.registry[symbol]
            lo, hi = info["range"]
            default = info["default"]
            span = max(1, hi - lo)
            offset = int(span * 0.2) if isinstance(default, int) else span * 0.2
            explore_val = default + offset
            if explore_val > hi or explore_val == default:
                explore_val = max(lo, default - offset)
            proposals.append(ResearchProposal(
                hypothesis=f"Explore {symbol}={explore_val} ({info.get('description','')})",
                rationale=(f"{symbol} has not been tested yet. Default={default}, "
                           f"range={info['range']}. Category: {info.get('category','?')}."),
                code_change={"change_type": "modify_constant",
                             "file": "strategy/brick_chart_strategy.py",
                             "symbol": symbol,
                             "value": explore_val,
                             "old_value": str(default)},
                priority=0.5,
                exhausted_params=[],
                suggested_params=[symbol],
            ))

        # 3. Two-parameter combination (single-param catalog nearly done)
        if len(proposals) < max_proposals * 0.5:
            exhausted_params = [s for s in self.registry
                                if not self._has_untried_sweep(s, self.registry[s].get("sweep", []), tried_values)
                                and len(tried_values.get(s, set())) > 0]
            tried_params = [s for s in self.registry if tried_values.get(s)]
            if len(exhausted_params) >= 2:
                p1, p2 = exhausted_params[:2]
                info1, info2 = self.registry[p1], self.registry[p2]
                p1_val = (self.registry[p1].get("sweep") or [info1["default"]])[0]
                p2_val = (self.registry[p2].get("sweep") or [info2["default"]])[0]
                proposals.append(ResearchProposal(
                    hypothesis=f"Combination: {p1}={p1_val} + {p2}={p2_val} (all single-params exhausted)",
                    rationale=f"Both {p1} and {p2} show declining/flat returns individually. "
                              f"A two-parameter combination may uncover interaction effects.",
                    code_change={"change_type": "modify_constant",
                                 "file": "strategy/brick_chart_strategy.py",
                                 "symbol": f"{p1}+{p2}",
                                 "value": f"{p1_val},{p2_val}",
                                 "old_value": "?"},
                    priority=0.6,
                    exhausted_params=exhausted_params,
                    suggested_params=[p1, p2],
                ))
            elif len(tried_params) >= 2:
                p1, p2 = tried_params[:2]
                vals1 = tried_values.get(p1, set())
                vals2 = tried_values.get(p2, set())
                p1_val = next(iter(vals1)) if vals1 else self.registry[p1]["default"]
                p2_val = next(iter(vals2)) if vals2 else self.registry[p2]["default"]
                proposals.append(ResearchProposal(
                    hypothesis=f"Combination: {p1}={p1_val} + {p2}={p2_val}",
                    rationale=f"Explore interaction between {p1} and {p2}.",
                    code_change={"change_type": "modify_constant",
                                 "file": "strategy/brick_chart_strategy.py",
                                 "symbol": f"{p1}+{p2}",
                                 "value": f"{p1_val},{p2_val}",
                                 "old_value": "?"},
                    priority=0.4,
                    exhausted_params=[],
                    suggested_params=[p1, p2],
                ))

        # sort by priority descending
        proposals.sort(key=lambda p: p.priority, reverse=True)
        return proposals[:max_proposals]

    @staticmethod
    def _has_untried_sweep(symbol: str, sweep: list, tried_values: dict[str, set]) -> bool:
        """True if there's at least one sweep value for this symbol that hasn't been tried."""
        if not sweep:
            return True
        tried = tried_values.get(symbol, set())
        sweep_set = set(sweep)
        # If sweep is ints but tried has floats, fuzzy match
        if all(isinstance(v, float) for v in sweep):
            sweep_set = set(sweep)
        return len(sweep_set - tried) > 0 or len(tried & sweep_set) < len(sweep)

        # sort by priority descending
        proposals.sort(key=lambda p: p.priority, reverse=True)
        return proposals[:max_proposals]
