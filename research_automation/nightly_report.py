"""nightly_report.py -- human-facing summary of an autonomous cycle.

Writes _output/reports/<strategy>_r<rounds>_<date>_<time>.md: what ran, candidates
added (tested/verified), rejected, and suggested promotions (VERIFIED) for human
review. Never auto-promotes.

File-name carries the full identity (strategy + rounds + date + time, parsed from
cycle_id) so same-day multi-strategy cycles no longer overwrite each other.
"""
from __future__ import annotations

import re
from pathlib import Path

from .safety import assert_safe_path, output_root


class NightlyReport:
    def __init__(self, reports_dir: str | Path | None = None):
        self.dir = Path(reports_dir) if reports_dir else (output_root() / "reports")

    def generate(self, cycle_results: dict, date_str: str) -> Path:
        """cycle_results = {cycle_id, strategy, rounds, baseline, candidates:[...], counts:{...}}"""
        cands = cycle_results.get("candidates", [])
        verified = [c for c in cands if c.get("promotion_status") == "verified"]
        tested = [c for c in cands if c.get("promotion_status") == "tested"]
        rejected = [c for c in cands if c.get("promotion_status") == "rejected"]
        b = cycle_results.get("baseline", {})

        def line(c):
            m = c.get("metrics", {})
            return (f"- `{c['experiment_id']}` {c.get('hypothesis','')} "
                    f"| sharpe={m.get('sharpe')} ret={m.get('total_return')} dd={m.get('max_drawdown')} "
                    f"trades={m.get('trades')} | delta={c.get('delta_vs_baseline')}")

        md = f"""# Nightly Research Report — {date_str}

cycle_id: {cycle_results.get('cycle_id')}
strategy: {cycle_results.get('strategy')}
rounds: {cycle_results.get('rounds')}   experiments: {len(cands)}

## Baseline (champion, frozen)
- sharpe={b.get('sharpe')} total_return={b.get('total_return')} max_drawdown={b.get('max_drawdown')} trades={b.get('trades')}

## Suggested for promotion (VERIFIED — human decision required)
{chr(10).join(line(c) for c in verified) or '- (none)'}

## Tested (ran; not yet verified)
{chr(10).join(line(c) for c in tested) or '- (none)'}

## Rejected
{chr(10).join(line(c) for c in rejected) or '- (none)'}

---
Promotion is HUMAN-ONLY. This report changes nothing in Champion / Registry / Snapshot / Handoff.
Decide per candidate: Approve / Reject / Need-More-Evidence, then promote manually.
"""
        fname = self._build_filename(
            cycle_results.get("strategy"),
            cycle_results.get("cycle_id"),
            cycle_results.get("rounds"),
            date_str,
        )
        path = assert_safe_path(self.dir / fname)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(md, encoding="utf-8")
        return path

    @staticmethod
    def _build_filename(strategy, cycle_id, rounds, date_str: str) -> str:
        """<strategy>_r<rounds>_<date>_<time>.md, e.g. b1_r1_20260620_172214Z.md.

        date/time are parsed from cycle_id (UTC stamp 'YYYYMMDDTHHMMSSZ'); if that
        fails, date_str is the fallback. Each piece is filename-safe (alnum + '_').
        """
        def _safe(s, fallback=""):
            s = re.sub(r"[^A-Za-z0-9]+", "", str(s or fallback))
            return s or fallback

        strat = _safe(strategy, "unknown")
        rnd = _safe(rounds)
        rnd = f"r{rnd}" if rnd else "r0"

        date_part = _safe(date_str)
        time_part = ""
        m = re.match(r"(\d{8})T(\d{6}Z)", str(cycle_id or ""))
        if m:
            date_part = m.group(1)
            time_part = m.group(2)
        return f"{strat}_{rnd}_{date_part}_{time_part}.md" if time_part else f"{strat}_{rnd}_{date_part}.md"
