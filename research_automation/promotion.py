"""promotion.py -- three-layer status + candidate pool (Research-Branch).

Automation may only classify TESTED / VERIFIED / REJECTED. It can NEVER set PROMOTED
(that is a human-only gate). The candidate pool is append-only under the safe output root;
production Champion / Registry / Snapshot / Handoff are never touched.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import yaml

from .control_plane.provenance import stamp_legacy_result
from .experiment import Experiment, StandardMetrics
from .safety import assert_safe_path, output_root


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _score(m: StandardMetrics) -> tuple:
    """Comparable tuple: higher is better. Uses sharpe then total_return (extra)."""
    sharpe = m.sharpe if m.sharpe is not None else -1e9
    tr = m.extra.get("total_return") if isinstance(m.extra, dict) else None
    tr = tr if tr is not None else -1e9
    return (sharpe, tr)


class PromotionEvaluator:
    """TESTED = ran & improved vs baseline on primary window.
       VERIFIED = also improved on >=1 independent confirm window.
       REJECTED = failed / no metrics / not better than baseline.
       PROMOTED is NEVER returned (human-only)."""

    def evaluate(self, experiment: Experiment, baseline: StandardMetrics,
                 confirm: StandardMetrics | None = None) -> str:
        m = experiment.metrics
        if experiment.status.value in ("FAILED", "ESCALATED_TO_USER", "REJECTED"):
            return "rejected"
        if m is None or m.source == "none" or (m.sharpe is None and not (m.extra or {}).get("total_return")):
            return "rejected"
        if _score(m) <= _score(baseline):
            return "rejected"   # ran cleanly but not better than baseline (per Research-Branch rule)
        # improved on primary
        if confirm is not None and _score(confirm) > _score(baseline):
            return "verified"
        return "tested"


class CandidatePool:
    """Append-only candidate ledger. promotion_status in
    {candidate, tested, verified, rejected, promoted}; automation writes <= verified.
    Every entry is stamped as a legacy result (controller_created=False,
    trust_state=legacy_unaudited, promotion_eligible=False) before persisting;
    the stamp is non-overridable."""

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else (output_root() / "candidates" / "candidate_pool.yaml")

    def add(self, experiment: Experiment, promotion_status: str, baseline: StandardMetrics,
            delta: dict | None = None) -> dict:
        assert promotion_status in ("candidate", "tested", "verified", "rejected"), \
            f"automation cannot set promotion_status={promotion_status}"
        m = experiment.metrics
        scope = experiment.proposal.scope if isinstance(experiment.proposal.scope, dict) else {}
        code_change = scope.get("code_change")
        if not isinstance(code_change, dict):
            code_change = None
        entry = {
            "experiment_id": experiment.experiment_id,
            "strategy": experiment.strategy,
            "added_at": _now(),
            "promotion_status": promotion_status,   # never 'promoted' from automation
            "hypothesis": experiment.proposal.hypothesis,
            "params": scope.get("params"),
            "metrics": {"sharpe": m.sharpe, "max_drawdown": m.max_drawdown,
                        "win_rate": m.win_rate, "trades": m.trades,
                        "total_return": (m.extra or {}).get("total_return")},
            "delta_vs_baseline": delta or {},
            "report_path": experiment.report_path,
        }
        if code_change is not None:
            entry["code_change"] = dict(code_change)
        evidence_claim_ids = scope.get("evidence_claim_ids") or scope.get("knowledge_claims")
        if isinstance(evidence_claim_ids, (list, tuple)):
            entry["evidence_claim_ids"] = [str(item) for item in evidence_claim_ids if str(item).strip()]
        stamped = stamp_legacy_result(entry)   # non-overridable, applied before persisting
        self._append(stamped)
        return stamped

    def _append(self, entry: dict) -> None:
        path = assert_safe_path(self.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        pool = []
        if path.exists():
            pool = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("candidates", [])
        pool.append(entry)
        path.write_text(yaml.safe_dump({"candidates": pool}, allow_unicode=True, sort_keys=False),
                        encoding="utf-8")
