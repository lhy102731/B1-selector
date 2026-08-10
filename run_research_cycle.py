#!/usr/bin/env python
"""run_research_cycle.py -- start one autonomous research cycle.

Current status: AutonomousRunnerV1 is retained for compatibility but is
legacy-unaudited. This entry point fails closed before runner construction
until an authorized control-plane campaign adapter replaces the legacy path.

Examples:
    # hybrid (default): run YOUR ideas first, then auto-continue
    python run_research_cycle.py --source hybrid --idea "pe_max=30,50,80" --idea "turnover_max" \
           --auto-source proposer --rounds 5 --per-round 4
    # unattended tonight (deterministic, most stable)
    python run_research_cycle.py --source proposer --rounds 5 --per-round 4
    # AG2 self-generated
    python run_research_cycle.py --source ag2 --rounds 5 --per-round 4

Stop anytime: double-click stop.bat, or create research_automation/_output/STOP, or Ctrl+C.
Safety: only writes staging deltas + candidate pool + reports under research_automation/_output/.
Never modifies Champion / Registry / Snapshot / Handoff / Memory. Promotion is human-only.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from research_automation.autonomous_runner import AutonomousRunnerV1
from research_automation.strategies import UnsupportedStrategyError, PROFILES
from research_automation.control_plane.campaign_preflight import (CampaignBoundaryError, require_campaign_boundary)


def main() -> int:
    ap = argparse.ArgumentParser(description="Autonomous research cycle (Research-Branch safe).")
    ap.add_argument("--strategy", default="b1",
                    help="b1 (full) | brick (experimental: V2 backtest only) | b3 (unsupported: no harness)")
    ap.add_argument("--source", choices=["hybrid", "proposer", "idea", "ag2"], default="hybrid")
    ap.add_argument("--auto-source", choices=["proposer", "ag2"], default="proposer",
                    help="phase-B generator after your ideas are drained")
    ap.add_argument("--idea", action="append", default=[],
                    help="your idea, e.g. 'pe_max=30,50,80' or 'turnover_max' (repeatable)")
    ap.add_argument("--ideas-file", default=None, help="text file, one idea per line")
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--per-round", type=int, default=4)
    ap.add_argument("--max-minutes", type=int, default=None)
    ap.add_argument("--recent-n", type=int, default=8, help="memory_packet recent-candidate cap")
    ap.add_argument("--keep-scratch", action="store_true", help="keep root backtest_*_v3.csv (debug)")
    ap.add_argument("--resume", action="store_true", help="continue the latest cycle's queue")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-workspace", action="store_true",
                    help="disable experiment workspace sandbox (run against production code)")
    ap.add_argument("--research-mode", type=str, default="parameter",
                    choices=["parameter", "code"],
                    help="parameter (default, unchanged): ParameterProposer + NoOpCodeChangeExecutor; "
                         "code: ResearchDirector + ProposalGenerator + ClaudePatchExecutor")
    args = ap.parse_args()

    try:
        require_campaign_boundary(surface="run_research_cycle.py:main")
    except CampaignBoundaryError as error:
        print(f"[run_research_cycle] blocked: {error}")
        return 3

    print(
        "[run_research_cycle] blocked: AutonomousRunnerV1 is legacy_unaudited and "
        "cannot run until a later control-plane campaign adapter is authorized."
    )
    return 3

    ideas = list(args.idea)
    if args.ideas_file and os.path.exists(args.ideas_file):
        ideas += [ln.strip() for ln in Path(args.ideas_file).read_text(encoding="utf-8").splitlines() if ln.strip()]

    try:
        runner = AutonomousRunnerV1(
            strategy=args.strategy, source=args.source, auto_source=args.auto_source,
            ideas=ideas, keep_scratch=args.keep_scratch, memory_packet_recent_n=args.recent_n,
            workspace_mode=not args.no_workspace,
            research_mode=args.research_mode,
        )
    except UnsupportedStrategyError as e:
        print(f"[run_research_cycle] cannot optimize '{args.strategy}': {e}")
        print(f"  selectable strategies: " +
              ", ".join(f"{k}({p.capability})" for k, p in PROFILES.items()))
        return 2
    result = runner.run(max_rounds=args.rounds, per_round=args.per_round,
                        max_minutes=args.max_minutes, resume=args.resume, dry_run=args.dry_run)
    print(f"\nRESULT: {result.get('cycle_id')} | experiments={len(result.get('candidates', result.get('tasks', [])))}"
          f" | report={result.get('report')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
