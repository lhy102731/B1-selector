"""report_generator.py -- Phase 5 experiment report generator.

Turns an Experiment + StandardMetrics into a markdown report written to the
staging dir. Generation only; it never mutates Research Memory.
"""
from __future__ import annotations

import json
from pathlib import Path

from .experiment import Experiment


class ReportGenerator:
    def generate(self, experiment: Experiment, out_dir: Path) -> Path:
        out_dir.mkdir(parents=True, exist_ok=True)
        m = experiment.metrics
        p = experiment.proposal
        rr = experiment.registry_reference

        def row(k, v):
            return f"| {k} | {v if v is not None else '-'} |"

        # ---- lineage (read from lineage.json if available) ----
        lineage_block = self._build_lineage_block(experiment, out_dir)

        md = f"""# Experiment Report: {experiment.experiment_id}

- strategy: {experiment.strategy}
- status: {experiment.status.value}
- git_commit: {experiment.git_commit}
- start: {experiment.start_time}  end: {experiment.end_time}

{lineage_block}
## Hypothesis
{p.hypothesis}

## Registry reference
- status: {rr.registry_status} (matched: {rr.matched_id}, overlap: {rr.overlap}, action: {rr.action})

## Standardized metrics (source: {m.source})
| metric | value |
|---|---|
{row("Sharpe", m.sharpe)}
{row("CAGR / total-return", m.cagr)}
{row("Win Rate", m.win_rate)}
{row("Max Drawdown", m.max_drawdown)}
{row("NDCG", m.ndcg)}
{row("IC", m.ic)}
{row("RankIC", m.rank_ic)}
{row("Turnover", m.turnover)}
{row("Trades", m.trades)}

## Changed files
{chr(10).join('- ' + f for f in experiment.changed_files) or '- (none)'}

## Notes
{"ESCALATED: " + "; ".join(experiment.escalation_reasons) if experiment.escalated else "ok"}
"""
        report_path = out_dir / "report.md"
        report_path.write_text(md, encoding="utf-8")
        experiment.report_path = str(report_path)
        return report_path

    @staticmethod
    def _build_lineage_block(experiment: Experiment, out_dir: Path) -> str:
        """Build the ## Lineage markdown block from experiment + lineage.json."""
        parent = experiment.parent_experiment_id
        gen = "?"
        change = "-"
        root = "-"

        lin_path = out_dir / "lineage.json"
        if lin_path.exists():
            try:
                lin = json.loads(lin_path.read_text(encoding="utf-8"))
                gen = str(lin.get("generation", "?"))
                change = lin.get("change_summary", "-")
                root = lin.get("root_id", "-")
            except Exception:
                pass

        return f"""## Lineage
- Experiment ID: {experiment.experiment_id}
- Parent: {parent or '(root)'}
- Root: {root}
- Generation: {gen}
- Change: {change}
"""
