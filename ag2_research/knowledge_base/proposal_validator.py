"""Validate an AG2-produced proposal against a strategy's hard constraints.

Used in two places:
  1) As an AG2 tool (ag2_research.tools.kb_validate_proposal) the agent can
     call BEFORE submitting a proposal.
  2) As a hard gate inside research_automation/ — every proposal MUST pass
     this validator before patch_executor is allowed to act on it.

Proposal schema (only fields the validator reads — others are ignored):

    {
      "subject": "b1_v3",
      "hypothesis": "...",
      "scope": {
        "params": {"j_max": 35, "turnover_max": 4.0},
        "code_change": {                 # optional
          "change_type": "modify_constant",
          "file": "strategy/b1_v3_config.py",
          "symbol": "wave_max_gain_pct",
          "value": 50,
          "old_value": "60"
        }
      },
      "measurement_plan": {              # optional
        "windows": ["A_2023", "B_2024H1", "C_2024H2_latest"],
        "reports": ["trades","total_return_pct","max_drawdown_pct",
                    "sharpe","profit_factor","jaccard_vs_baseline"]
      }
    }

Return value:

    {
      "verdict": "allow" | "reject" | "needs_evidence",
      "violations": [...],   # rule_id strings
      "warnings":   [...],
      "reasons":    [...],   # human-readable lines
      "kb_version": "...",
      "subject":    "..."
    }
"""
from __future__ import annotations

from typing import Any

from .loader import load


# Required measurement-plan keys per window
_REQ_PER_WINDOW = {
    "trades", "total_return_pct", "max_drawdown_pct",
    "sharpe", "profit_factor", "jaccard_vs_baseline",
}


def _collect_touched_params(proposal: dict) -> set[str]:
    """Return the set of B1V3Params field names the proposal would touch."""
    touched: set[str] = set()
    scope = proposal.get("scope", {})
    if isinstance(scope, dict):
        params = scope.get("params")
        if isinstance(params, dict):
            touched.update(params.keys())
        # code_change can target a B1V3Params field by symbol name
        cc = scope.get("code_change")
        if isinstance(cc, dict):
            sym = cc.get("symbol")
            if isinstance(sym, str):
                touched.add(sym)
    return touched


def _hypothesis_text(proposal: dict) -> str:
    return str(proposal.get("hypothesis", "")).lower()


def _proposal_text(proposal: dict) -> str:
    """Return lower-case proposal text for generic keyword rules."""
    parts = [str(proposal.get("hypothesis", ""))]
    for key in ("family", "mechanism", "expected_information_gain", "required_data"):
        if key in proposal:
            parts.append(str(proposal.get(key, "")))
    try:
        import json

        parts.append(json.dumps(proposal, ensure_ascii=False, sort_keys=True))
    except Exception:
        parts.append(str(proposal))
    return "\n".join(parts).lower()


def _check_forbidden_modifications(proposal: dict, hc: dict,
                                    violations: list, reasons: list) -> None:
    touched = _collect_touched_params(proposal)
    if not touched:
        return
    for rule in hc.get("forbidden_modifications", []) or []:
        rule_params = set(rule.get("params", []))
        hit = touched & rule_params
        if hit:
            violations.append(rule["id"])
            reasons.append(
                f"[{rule['id']}] proposal touches frozen param(s) "
                f"{sorted(hit)}: {rule.get('rationale','').strip().splitlines()[0]}"
            )


def _check_conditional_modifications(proposal: dict, hc: dict,
                                      warnings: list, reasons: list,
                                      needs_evidence: list) -> None:
    touched = _collect_touched_params(proposal)
    if not touched:
        return
    measurement = proposal.get("measurement_plan") or {}
    windows_in_plan = set(measurement.get("windows", []) or [])
    reports_in_plan = set(measurement.get("reports", []) or [])

    bar_windows = set(hc.get("acceptance_bar", {}).get("windows_required",
                                                       ["A_2023","B_2024H1","C_2024H2_latest"]))

    for rule in hc.get("conditional_modifications", []) or []:
        rule_params = set(rule.get("params", []))
        hit = touched & rule_params
        if not hit:
            continue
        missing = []
        if "remove-one in all 3 windows" in rule.get("required_evidence", []):
            if not bar_windows.issubset(windows_in_plan):
                missing.append("3-window remove-one test")
        if "remove-pair with A_J_range in all 3 windows" in rule.get("required_evidence", []):
            if "pair_removal_with_a_j_range" not in proposal.get("evidence_supplied", []):
                missing.append("pair-removal with A_J_range")
        if "acceptance bar satisfied in all 3 windows" in rule.get("required_evidence", []):
            if not _REQ_PER_WINDOW.issubset(reports_in_plan):
                missing.append("measurement of all 6 per-window metrics")
        if missing:
            needs_evidence.append(rule["id"])
            reasons.append(
                f"[{rule['id']}] conditional modification of {sorted(hit)} "
                f"requires: {', '.join(missing)}"
            )
        else:
            warnings.append(rule["id"])
            reasons.append(
                f"[{rule['id']}] conditional modification of {sorted(hit)} "
                f"requires the measurement plan you supplied to actually be run."
            )


def _check_forbidden_hypotheses(proposal: dict, hc: dict,
                                 violations: list, warnings: list,
                                 reasons: list) -> None:
    h = _proposal_text(proposal)
    if not h:
        return
    # Keyword patterns for each falsified hypothesis
    keyword_map = {
        "NO_ALPHA_ONLY_RECONSTRUCTION": [
            "only verified alpha", "alpha-only", "alpha only",
            "remove all concentrators", "minimum reconstruction",
        ],
        "NO_SECOND_GENERATOR_IN_PSM_DAILY": [
            "second generator", "second entry generator",
            "additional generator", "new entry generator",
        ],
        "NO_C_NO_WAVE_BREAK_REINFORCEMENT": [
            "require_no_wave_break", "tighten wave break",
            "strengthen wave break",
        ],
        "NO_SINGLE_ABLATION_REDUNDANCY_CLAIM": [
            "redundant on single removal", "single-ablation redundant",
        ],
        "NO_OPTIMIZATION_WITHOUT_FEATURE_EXTENSION": [
            "optimize threshold", "parameter sweep", "grid search",
            "fine-tune", "fine tune",
        ],
    }
    for rule in hc.get("forbidden_hypotheses", []) or []:
        keys = rule.get("keywords") or keyword_map.get(rule["id"], [])
        if any(k in h for k in keys):
            severity = rule.get("severity", "reject")
            if severity == "warn_not_reject":
                warnings.append(rule["id"])
            else:
                violations.append(rule["id"])
            message = rule.get("rejection_message") or rule.get("description") or ""
            reasons.append(
                f"[{rule['id']}] {message.strip().splitlines()[0]}"
            )


def _check_measurement_plan(proposal: dict, hc: dict,
                             needs_evidence: list, reasons: list) -> None:
    rmp = (hc.get("required_measurement_plan") or {})
    req_per = set(rmp.get("must_report_per_window", []))
    req_agg = set(rmp.get("must_report_aggregate", []))
    if not req_per and not req_agg:
        return

    measurement = proposal.get("measurement_plan") or {}
    reports = set(measurement.get("reports", []) or [])
    aggregates = set(measurement.get("aggregates", []) or [])
    windows = set(measurement.get("windows", []) or [])
    bar_windows = set(hc.get("acceptance_bar", {}).get("windows_required",
                                                       ["A_2023","B_2024H1","C_2024H2_latest"]))

    issues = []
    miss_per = req_per - reports
    if miss_per:
        issues.append(f"per-window reports missing: {sorted(miss_per)}")
    miss_agg = req_agg - aggregates
    if miss_agg:
        issues.append(f"aggregate reports missing: {sorted(miss_agg)}")
    miss_w = bar_windows - windows
    if miss_w:
        issues.append(f"required windows missing: {sorted(miss_w)}")

    if issues:
        needs_evidence.append("MEASUREMENT_PLAN_INCOMPLETE")
        reasons.append("[MEASUREMENT_PLAN_INCOMPLETE] " + "; ".join(issues))


# ---------------------------------------------------------------- public API

def validate_proposal(subject: str, proposal: dict) -> dict[str, Any]:
    """Run all hard-constraint checks against `proposal`.

    Returns a structured verdict (see module docstring).
    """
    kb = load(subject)
    hc = kb.hard_constraints or {}

    violations: list[str] = []
    warnings: list[str] = []
    needs_evidence: list[str] = []
    reasons: list[str] = []

    _check_forbidden_modifications(proposal, hc, violations, reasons)
    _check_conditional_modifications(proposal, hc, warnings, reasons, needs_evidence)
    _check_forbidden_hypotheses(proposal, hc, violations, warnings, reasons)
    _check_measurement_plan(proposal, hc, needs_evidence, reasons)

    if violations:
        verdict = "reject"
    elif needs_evidence:
        verdict = "needs_evidence"
    else:
        verdict = "allow"

    return {
        "verdict": verdict,
        "violations": violations,
        "warnings": warnings,
        "needs_evidence": needs_evidence,
        "reasons": reasons,
        "kb_version": kb.kb_version,
        "subject": kb.subject,
    }
