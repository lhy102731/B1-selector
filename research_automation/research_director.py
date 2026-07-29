"""research_director.py — Rule-based Research Director (Reasoning Framework).

Sits above the Proposal Generator. Analyses historical experiment results to:
  - Classify parameter dimensions (exhausted / promising / unstable)
  - Generate structured findings (rule-based inference)
  - Formulate hypotheses from observed patterns
  - Rank proposals by exploration value

Phase 3B: STRICTLY rule-driven. NO Claude, NO OpenAI, NO GLM, NO AG2, NO LLM.
Pure heuristic reasoning on structured experiment data.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .research_proposal_generator import (
    ResearchProposalGenerator, ResearchProposal,
    _detect_exhausted, _build_symbol_index, _score_trend,
    _PARAMETER_REGISTRY,
)


# ============================================================
# Output structures
# ============================================================

@dataclass
class DimensionClassification:
    """A parameter dimension classified by observed trend."""
    symbol: str
    category: str                          # exhausted / promising / unstable
    trend: str                              # declining / improving / flat / insufficient_data
    confidence: float                       # 0.0–1.0
    evidence: list[float] = field(default_factory=list)
    description: str = ""


@dataclass
class Finding:
    """A structured finding inferred from experiment data."""
    id: str
    statement: str
    evidence: str                           # which data supports it
    confidence: float


@dataclass
class Hypothesis:
    """A testable hypothesis generated from findings."""
    id: str
    statement: str
    rationale: str
    suggested_test: str                     # what experiment would validate it
    confidence: float


@dataclass
class RankedProposal:
    """A proposal with priority, score, and reasoning."""
    proposal_id: str
    reason: str
    score: float
    confidence: float
    priority: float
    code_change: dict = field(default_factory=dict)


@dataclass
class ResearchAnalysis:
    """Complete research analysis output."""
    findings: list[Finding] = field(default_factory=list)
    exhausted_dimensions: list[DimensionClassification] = field(default_factory=list)
    promising_dimensions: list[DimensionClassification] = field(default_factory=list)
    unstable_dimensions: list[DimensionClassification] = field(default_factory=list)
    hypotheses: list[Hypothesis] = field(default_factory=list)
    ranked_proposals: list[RankedProposal] = field(default_factory=list)


# ============================================================
# Finding templates (rule-based)
# ============================================================

def _generate_findings(experiments: list[dict]) -> list[Finding]:
    """Infer structured findings from experiment data using rule templates."""
    findings = []
    fid = 0

    by_symbol = _build_symbol_index(experiments)
    exhausted = _detect_exhausted(experiments)

    # Finding 1: exhausted parameter impact
    for exh in exhausted:
        fid += 1
        rets = exh["returns"]
        pvs = exh["param_values"]
        findings.append(Finding(
            id=f"F{fid:02d}",
            statement=(f"Increasing {exh['symbol']} from {pvs[0]:.2f} to {pvs[-1]:.2f} "
                       f"reduced return from {rets[0]:.2f} to {rets[-1]:.2f}"),
            evidence=f"{len(rets)} experiments, {exh['trend']} trend",
            confidence=min(0.95, 0.5 + 0.1 * len(rets)),
        ))

    # Finding 2: trade count stability
    trades = [e.get("metrics", {}).get("trades") or e.get("total_return", 0) * 100
              for e in experiments]
    trade_values = {e.get("trades", "?") for e in experiments
                    if "trades" in (e.get("metrics", {}) or {}) or "trades" in e}
    if len(trade_values) <= 1 and len(experiments) >= 3:
        fid += 1
        findings.append(Finding(
            id=f"F{fid:02d}",
            statement=f"Trade count unchanged ({next(iter(trade_values))} trades) despite parameter variation",
            evidence=f"{len(experiments)} experiments, all showing same trade count",
            confidence=0.85,
        ))

    # Finding 3: win_rate trend
    wr_values = [e.get("metrics", {}).get("win_rate") or e.get("win_rate")
                 for e in experiments]
    wr_values = [w for w in wr_values if w is not None]
    if len(wr_values) >= 3:
        wr_trend = _score_trend(wr_values)
        fid += 1
        if wr_trend == "flat":
            findings.append(Finding(
                id=f"F{fid:02d}", statement="Win rate remains stable despite parameter changes",
                evidence=f"wr values: {[f'{w:.3f}' for w in wr_values]}, trend={wr_trend}",
                confidence=0.7,
            ))
        elif wr_trend == "improving":
            findings.append(Finding(
                id=f"F{fid:02d}", statement="Win rate improves with parameter adjustment",
                evidence=f"wr trend: {wr_trend}", confidence=0.75,
            ))
        else:
            findings.append(Finding(
                id=f"F{fid:02d}", statement=f"Win rate shows {wr_trend} trend — parameter impact is unclear",
                evidence=f"wr values: {[f'{w:.3f}' for w in wr_values]}", confidence=0.6,
            ))

    # Finding 4: diminishing returns pattern
    ret_values = [e.get("total_return", 0) or 0 for e in experiments]
    if len(ret_values) >= 3 and all(ret_values[i] >= ret_values[i+1] - 0.01
                                    for i in range(len(ret_values) - 1)):
        fid += 1
        findings.append(Finding(
            id=f"F{fid:02d}", statement="Monotonic return degradation — parameter is a primary driver",
            evidence=f"returns: {[f'{r:.2f}' for r in ret_values]}",
            confidence=0.9,
        ))

    return findings


def _generate_hypotheses(dimensions: list[DimensionClassification],
                         findings: list[Finding]) -> list[Hypothesis]:
    """Generate hypotheses from dimension classifications and findings."""
    hyps = []
    hid = 0

    # Hypothesis A: exhausted parameter isn't the right lever
    exhausted = [d for d in dimensions if d.category == "exhausted"]
    if exhausted:
        hid += 1
        exh_names = ", ".join(d.symbol for d in exhausted)
        hyps.append(Hypothesis(
            id=f"H{hid:02d}",
            statement=f"{exh_names} does not control signal quality — look at other dimensions",
            rationale=f"All {exh_names} experiments show trade count is stable; "
                      f"the parameter only tightens/loosens filtering, not signal generation.",
            suggested_test=f"Vary yellow_line parameters (M1-M4) to test if entry timing dominates "
                           f"over signal filtering.",
            confidence=0.8,
        ))

    # Hypothesis B: yellow_line family may have larger influence
    hid += 1
    hyps.append(Hypothesis(
        id=f"H{hid:02d}",
        statement="Yellow line parameters (M1-M4) may have larger influence on entry timing",
        rationale="The yellow line defines the multi-MA fair value. Adjusting its periods "
                  "changes which pullbacks qualify as 'above yellow' — this is upstream of "
                  "brick signal filtering.",
        suggested_test="Run a grid sweep on M1 (shortest MA) first — vary from 7 to 21.",
        confidence=0.7,
    ))

    # Hypothesis C: brick qualification may dominate
    hid += 1
    hyps.append(Hypothesis(
        id=f"H{hid:02d}",
        statement="Brick qualification thresholds (VAR6A > 4, height_ratio) may have "
                  "nonlinear interactions — need multi-parameter exploration",
        rationale="height_ratio alone shows monotonic decline, but in combination with "
                  "yellow_line adjustment, the optimal region may shift.",
        suggested_test="Co-optimize height_ratio (0.5–0.9) with M1 (7–21) in a 2D grid.",
        confidence=0.6,
    ))

    return hyps


# ============================================================
# Dimension classifier
# ============================================================

def _classify_dimensions(experiments: list[dict]) -> list[DimensionClassification]:
    """Classify each tested parameter into exhausted / promising / unstable."""
    dims = []
    by_symbol = _build_symbol_index(experiments)
    exhausted_list = _detect_exhausted(experiments)
    exhausted_symbols = {e["symbol"] for e in exhausted_list}

    for symbol, exps in by_symbol.items():
        exps_sorted = sorted(exps, key=lambda e: e.get("param_value", 0))
        rets = [e.get("total_return", 0) or 0 for e in exps_sorted]
        trend = _score_trend(rets)

        if symbol in exhausted_symbols:
            dims.append(DimensionClassification(
                symbol=symbol, category="exhausted", trend=trend,
                confidence=min(0.95, 0.5 + 0.1 * len(rets)),
                evidence=rets,
                description=f"{symbol}: {trend} trend, best at {exps_sorted[0].get('param_value')}",
            ))
        elif trend == "improving":
            dims.append(DimensionClassification(
                symbol=symbol, category="promising", trend=trend,
                confidence=0.6 + 0.1 * len(rets),
                evidence=rets,
                description=f"{symbol}: {trend} — continue exploring",
            ))
        elif trend == "declining" and len(rets) < 3:
            dims.append(DimensionClassification(
                symbol=symbol, category="unstable", trend=trend,
                confidence=0.4,
                evidence=rets,
                description=f"{symbol}: {trend} but insufficient data (n={len(rets)})",
            ))
        else:
            dims.append(DimensionClassification(
                symbol=symbol, category="unstable" if len(rets) < 3 else "exhausted",
                trend=trend, confidence=0.5, evidence=rets,
                description=f"{symbol}: {trend} (n={len(rets)})",
            ))

    return dims


# ============================================================
# Proposal ranker
# ============================================================

def _rank_proposals(proposals: list[ResearchProposal],
                    dimensions: list[DimensionClassification],
                    experiments: list[dict]) -> list[RankedProposal]:
    """Convert ResearchProposals into RankedProposals with scores."""
    ranked = []
    tried_symbols = set(_build_symbol_index(experiments).keys())
    exhausted_symbols = {d.symbol for d in dimensions if d.category == "exhausted"}

    for i, p in enumerate(proposals):
        symbol = p.code_change.get("symbol", "?")
        # score: untried > tried but not exhausted > anything else
        if symbol in exhausted_symbols:
            score = 0.1  # should not happen (generator filters these), but belt-and-suspenders
        elif symbol not in tried_symbols:
            score = 0.8  # untried parameter
        else:
            score = 0.4  # tried but not exhausted

        # boost for yellow_line category (hypothesized high-impact)
        param_info = _PARAMETER_REGISTRY.get(symbol, {})
        if param_info.get("category") == "yellow_line":
            score += 0.1

        ranked.append(RankedProposal(
            proposal_id=f"RP-{i+1:02d}",
            reason=p.rationale[:120],
            score=round(min(score, 1.0), 2),
            confidence=p.priority,
            priority=p.priority,
            code_change=p.code_change,
        ))

    ranked.sort(key=lambda r: (r.score, r.priority), reverse=True)
    return ranked


# ============================================================
# Research Director
# ============================================================

class ResearchDirector:
    """Rule-based research reasoning framework.

    Usage:
        director = ResearchDirector()
        analysis = director.analyze(experiments)
        # analysis.findings, .hypotheses, .ranked_proposals, ...
    """

    def __init__(self, parameter_registry: dict | None = None):
        self.registry = parameter_registry or _PARAMETER_REGISTRY
        self.proposal_gen = ResearchProposalGenerator(self.registry)

    def analyze(self, experiments: list[dict]) -> ResearchAnalysis:
        """Full analysis pipeline: classify → find → hypothesize → rank proposals.

        Each experiment dict: experiment_id, hypothesis, code_change,
        metrics (trades/win_rate/total_return), param_value, generation.
        """
        # 1. classify dimensions
        dimensions = _classify_dimensions(experiments)

        # 2. generate findings
        findings = _generate_findings(experiments)

        # 3. generate hypotheses
        hypotheses = _generate_hypotheses(dimensions, findings)

        # 4. generate proposals (delegate to ProposalGenerator)
        raw_proposals = self.proposal_gen.generate(experiments, max_proposals=5)

        # 5. rank proposals
        ranked = _rank_proposals(raw_proposals, dimensions, experiments)

        # split dimensions
        exhausted = [d for d in dimensions if d.category == "exhausted"]
        promising = [d for d in dimensions if d.category == "promising"]
        unstable = [d for d in dimensions if d.category == "unstable"]

        return ResearchAnalysis(
            findings=findings,
            exhausted_dimensions=exhausted,
            promising_dimensions=promising,
            unstable_dimensions=unstable,
            hypotheses=hypotheses,
            ranked_proposals=ranked,
        )
