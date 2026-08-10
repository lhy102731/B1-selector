#!/usr/bin/env python
"""AG2 Multi-Agent Strategy Research — CLI Entry Point.

Quick start:
    # Fill in .env with your API keys, then:
    python run_research.py brainstorm --topic "How to improve B1 win rate?"
    python run_research.py brainstorm --profile deepseek --topic "..."
    python run_research.py list
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from research_automation.control_plane.cli_registry import (
    CliAuthorizationContext,
    authorize_cli_command,
)
from research_automation.control_plane.campaign_preflight import (CampaignBoundaryError, require_campaign_boundary)
from research_automation.control_plane.sink_guard import ExecutionAuthorizationError


# Lazy import seams preserve existing test patch points while ensuring an
# unauthorized CLI command is rejected before AG2/config construction.
Orchestrator = None
ResearchConfig = None
_save_discovery_handoff_impl = None


def _orchestrator_class():
    global Orchestrator
    if Orchestrator is None:
        from ag2_research import Orchestrator as implementation

        Orchestrator = implementation
    return Orchestrator


def _research_config_class():
    global ResearchConfig
    if ResearchConfig is None:
        from ag2_research import ResearchConfig as implementation

        ResearchConfig = implementation
    return ResearchConfig


def _handoff_writer():
    return save_discovery_handoff


def save_discovery_handoff(*args, **kwargs):
    """Lazy compatibility proxy for the protected KBase handoff writer."""
    global _save_discovery_handoff_impl
    if _save_discovery_handoff_impl is None:
        from ag2_research.discovery_handoff import save_discovery_handoff as implementation

        _save_discovery_handoff_impl = implementation
    return _save_discovery_handoff_impl(*args, **kwargs)


def _cli_preflight(
    args: argparse.Namespace,
    command: str,
    *,
    dry_run: bool = False,
) -> CliAuthorizationContext | None:
    argv = tuple(getattr(args, "_control_plane_argv", ()))
    if not argv:
        argv = ("run_research.py", command)
    return authorize_cli_command(
        getattr(args, "_control_plane_authorization", None),
        command=command,
        argv=argv,
        dry_run=dry_run,
    )


def _campaign_boundary(
    args: argparse.Namespace,
    command: str,
    dry_run: bool = False,
) -> None:
    """Route one legacy CLI command through the fail-closed Campaign boundary.

    execute-handoff --dry-run is read-only and remains exempt; every other
    legacy execution command fails closed until a control-plane Campaign
    execution context is attached.
    """
    if dry_run and command == "execute-handoff":
        return
    require_campaign_boundary(surface=f"run_research.py:{command}")


def _configure_stdio() -> None:
    """Prefer UTF-8 console streams on Windows shells."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except (OSError, ValueError):
                pass


_configure_stdio()


def cmd_list(_args: argparse.Namespace) -> None:
    cfg = _research_config_class()()
    print("\n=== LLM Profiles ===")
    for name in cfg.list_profiles():
        marker = " (default)" if name == cfg.default_profile else ""
        p = cfg.profiles[name]
        print(f"  {name}{marker}")
        print(f"      model={p.get('model','?')}  base_url={p.get('base_url','?')}")
    print("\n=== Agents ===")
    for a in cfg.list_agents():
        print(f"  {a['id']:<22s} {a['name']:<20s} {a['description']}")
    print("\n=== Workflows ===")
    for w in cfg.list_workflows():
        print(f"  {w['id']:<22s} {w['description']}")


def cmd_brainstorm(args: argparse.Namespace) -> None:
    _cli_preflight(args, "brainstorm")
    _campaign_boundary(args, "brainstorm")
    orch = _orchestrator_class()(profile=args.profile)
    ctx = args.context or ""
    if args.context_file:
        ctx = Path(args.context_file).read_text(encoding="utf-8")

    print(f"\n{'='*60}")
    print(f"Brainstorm: {args.topic}")
    print(f"Profile: {orch.profile}")
    print(f"{'='*60}")
    if ctx:
        print(f"Context: {len(ctx)} chars loaded")
    print("Workflow: brainstorm (configured sequential pipeline)")
    print()

    orch.run_workflow(
        "brainstorm",
        topic=args.topic,
        research_context=ctx,
    )


def cmd_discover(args: argparse.Namespace) -> Path:
    """Run the source-first KBase discovery workflow explicitly."""
    _cli_preflight(args, "discover")
    _campaign_boundary(args, "discover")
    orch = _orchestrator_class()(profile=args.profile)
    ctx = args.context or ""
    if args.context_file:
        ctx = Path(args.context_file).read_text(encoding="utf-8")
    if getattr(args, "sequential", False):
        workflow_id = "kbase_discovery"
    elif getattr(args, "roundtable_first", False):
        workflow_id = "kbase_roundtable_discovery"
    else:
        workflow_id = "kbase_source_first_discovery"
    result = orch.run_workflow(
        workflow_id,
        topic=args.topic,
        research_context=ctx,
        strategy_id=args.strategy,
    )
    saved = _handoff_writer()(
        result, topic=args.topic, strategy_id=args.strategy,
        output_dir=getattr(args, "output_dir", None),
    )
    print(f"KBase discovery ({workflow_id}): {result.get('status')} - {result.get('reason', '')}")
    print(f"Discovery handoff: {saved}")
    return saved


def cmd_resume_discover(args: argparse.Namespace) -> Path:
    """Resume the factor handoff from an archived source-first checkpoint."""
    _cli_preflight(args, "resume-discover")
    _campaign_boundary(args, "resume-discover")
    checkpoint = Path(args.handoff_path).resolve()
    document = yaml.safe_load(checkpoint.read_text(encoding="utf-8")) or {}
    strategy_id = str(document.get("strategy_id") or "")
    topic = str(document.get("topic") or "")
    orchestrator = _orchestrator_class()(profile=args.profile)
    result = orchestrator.resume_source_first_discovery(checkpoint)
    saved = _handoff_writer()(
        result,
        topic=topic,
        strategy_id=strategy_id,
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    print(f"KBase discovery resume: {result.get('status')} - {result.get('reason', '')}")
    print(f"Discovery handoff: {saved}")
    return saved


def cmd_execute_handoff(args: argparse.Namespace) -> dict:
    """Execute an APPROVED discovery handoff with a registered Phase 6 runner."""
    context = _cli_preflight(
        args,
        "execute-handoff",
        dry_run=bool(args.dry_run),
    )
    _campaign_boundary(args, "execute-handoff", dry_run=bool(args.dry_run))
    from ag2_research.discovery_handoff import load_latest_approved_discovery
    from research_automation.discovery_execution_bridge import (
        build_execution_plan,
        execute_plan,
    )

    handoff_path = args.handoff_path
    if not handoff_path:
        latest = load_latest_approved_discovery(args.strategy)
        if not latest:
            raise SystemExit(f"No APPROVED discovery handoff found for strategy={args.strategy}")
        handoff_path = latest["path"]

    plan = build_execution_plan(handoff_path, output_dir=args.output_dir)
    print("Discovery execution plan:")
    print(yaml.safe_dump(plan.to_dict(), allow_unicode=True, sort_keys=False))
    if args.dry_run:
        print("Dry run only; no runner executed.")
        return plan.to_dict()
    if context is None:
        raise RuntimeError("authorized execute-handoff context is unavailable")
    result = execute_plan(
        plan,
        dry_run=False,
        lease=context.lease,
        invocation=context.invocation,
        authority_reader=context.authority_reader,
        repository_root=context.repository_root,
    )
    return_code = result.returncode if result is not None else 0
    if return_code != 0:
        raise SystemExit(return_code)
    return plan.to_dict()


def cmd_full_cycle(args: argparse.Namespace) -> dict:
    """Run discovery, runner repair when needed, and archived research execution."""
    _cli_preflight(args, "full-cycle", dry_run=bool(args.dry_run))
    _campaign_boundary(args, "full-cycle", dry_run=bool(args.dry_run))
    from research_automation.kbase_ag2_full_cycle import run_kbase_ag2_full_cycle

    context = args.context or ""
    if args.context_file:
        context = Path(args.context_file).read_text(encoding="utf-8")
    workflow_id = (
        "kbase_discovery"
        if getattr(args, "sequential", False)
        else "kbase_roundtable_discovery"
    )
    result = run_kbase_ag2_full_cycle(
        topic=args.topic,
        strategy_id=args.strategy,
        profile=args.profile,
        research_context=context,
        handoff_path=args.handoff_path,
        output_dir=args.output_dir,
        workflow_id=workflow_id,
        auto_repair=not args.no_auto_repair,
        dry_run=args.dry_run,
        claude_binary=args.claude_binary,
        repair_timeout=args.repair_timeout,
    )
    print("KBase-AG2 full-cycle result:")
    print(json.dumps({
        "status": result.get("status"),
        "cycle_dir": result.get("cycle_dir"),
        "handoff_path": result.get("handoff_path"),
        "research_status": result.get("research_status"),
        "promotion_gate_passed": result.get("promotion_gate_passed", False),
        "production_boundary_unchanged": result.get("production_boundary_unchanged"),
        "reason": result.get("reason"),
    }, ensure_ascii=False, indent=2))
    return result


def cmd_repair_handoff_runner(args: argparse.Namespace) -> dict:
    """Auto-repair a missing registered Phase 6 runner for an APPROVED handoff."""
    _cli_preflight(args, "repair-handoff-runner", dry_run=bool(args.dry_run))
    _campaign_boundary(args, "repair-handoff-runner", dry_run=bool(args.dry_run))
    from research_automation.handoff_runner_repair import repair_handoff_runner

    result = repair_handoff_runner(
        handoff_path=args.handoff_path,
        output_dir=args.output_dir,
        failure_log_path=args.failure_log,
        claude_binary=args.claude_binary,
        timeout=args.timeout,
        dry_run=args.dry_run,
        skip_code_review=args.skip_code_review,
    )
    data = result.to_dict()
    print("Handoff runner repair result:")
    print(json.dumps(data, ensure_ascii=False, indent=2))
    if not result.ok:
        raise SystemExit(1)
    return data


def cmd_review(args: argparse.Namespace) -> None:
    _cli_preflight(args, "review")
    _campaign_boundary(args, "review")
    orch = _orchestrator_class()(profile=args.profile)

    if args.file:
        try:
            strategy_desc = Path(args.file).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            strategy_desc = Path(args.file).read_text(encoding="gbk")
    elif args.text:
        strategy_desc = args.text
    else:
        print("Error: --file or --text required for review")
        sys.exit(1)

    ctx = args.context or ""
    if args.context_file:
        ctx = Path(args.context_file).read_text(encoding="utf-8")

    print(f"\n{'='*60}")
    print(f"Strategy Review  |  Profile: {orch.profile}")
    print(f"{'='*60}\n")

    orch.run_review(strategy_description=strategy_desc, research_context=ctx)


def cmd_chat(args: argparse.Namespace) -> None:
    _cli_preflight(args, "chat")
    _campaign_boundary(args, "chat")
    orch = _orchestrator_class()(profile=args.profile)
    agent_ids = [a.strip() for a in args.agents.split(",")]

    ctx = args.context or ""
    if args.context_file:
        ctx = Path(args.context_file).read_text(encoding="utf-8")

    print(f"\n{'='*60}")
    print(f"Chat: {', '.join(agent_ids)}  |  Profile: {orch.profile}")
    print(f"{'='*60}\n")

    orch.run_chat(prompt=args.prompt, agent_ids=agent_ids, research_context=ctx, max_rounds=args.max_rounds)


def cmd_roundtable(args: argparse.Namespace) -> None:
    _cli_preflight(args, "roundtable")
    _campaign_boundary(args, "roundtable")
    orch = _orchestrator_class()()
    ctx = args.context or ""
    if args.context_file:
        ctx = Path(args.context_file).read_text(encoding="utf-8")

    # Allow overriding participants from CLI
    participants = None
    if args.models:
        participants = []
        for m in args.models.split(","):
            m = m.strip()
            # Support "profile:label" or just "profile"
            if ":" in m:
                profile, label = m.split(":", 1)
            else:
                profile = label = m
            participants.append({"profile": profile, "label": label})

    print(f"\n{'='*60}")
    print(f"Multi-LLM Roundtable: {args.topic}")
    print(f"{'='*60}")
    if participants:
        for p in participants:
            print(f"  {p['label']} ({p['profile']})")
    else:
        print("  Using default participants from config.yaml")
    print()

    orch.run_roundtable(
        topic=args.topic,
        research_context=ctx,
        participants=participants,
        max_rounds=args.max_rounds or None,
    )


def cmd_interactive(args: argparse.Namespace) -> None:
    _cli_preflight(args, "interactive")
    _campaign_boundary(args, "interactive")
    orch = _orchestrator_class()(profile=args.profile)
    print(f"\nAG2 Research Console — profile: {orch.profile}")
    print("Type /help for commands, /quit to exit\n")

    while True:
        try:
            cmd = input("research> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not cmd:
            continue
        if cmd in ("/quit", "/exit", "/q"):
            break
        if cmd == "/help":
            print("Commands:")
            print("  /agents              List available agents")
            print("  /workflows           List workflow presets")
            print("  /brainstorm <topic>  Run full brainstorm")
            print("  /roundtable <topic>  Multi-LLM roundtable discussion")
            print("  /review <text>       Quick strategy review")
            print("  /chat <msg>          Chat with Alpha Hunter + Risk Controller")
            print("  /quit                Exit")
            continue
        if cmd == "/agents":
            cfg = _research_config_class()()
            for a in cfg.list_agents():
                print(f"  {a['id']:<22s} {a['description']}")
            continue
        if cmd == "/workflows":
            cfg = _research_config_class()()
            for w in cfg.list_workflows():
                print(f"  {w['id']:<22s} {w['description']}")
            continue

        if cmd.startswith("/brainstorm "):
            topic = cmd[len("/brainstorm "):]
            orch.run_workflow("brainstorm", topic=topic)
        elif cmd.startswith("/roundtable "):
            topic = cmd[len("/roundtable "):]
            orch.run_roundtable(topic=topic)
        elif cmd.startswith("/review "):
            text = cmd[len("/review "):]
            orch.run_review(strategy_description=text)
        elif cmd.startswith("/chat "):
            msg = cmd[len("/chat "):]
            orch.run_chat(prompt=msg, agent_ids=["alpha_hunter", "risk_controller"], max_rounds=10)
        else:
            orch.run_chat(prompt=cmd, agent_ids=["alpha_hunter", "factor_engineer"], max_rounds=8)


def main(
    argv: list[str] | None = None,
    *,
    authorization: CliAuthorizationContext | None = None,
) -> int:
    parser = argparse.ArgumentParser(description="AG2 Multi-Agent Strategy Research")
    sub = parser.add_subparsers(dest="command")

    def _add_profile(p):
        p.add_argument("--profile", help="LLM profile to use (openai, deepseek, ollama, moonshot, zhipu, qwen)")

    # list
    sub.add_parser("list", help="List agents, workflows, and LLM profiles")

    # brainstorm
    p_brain = sub.add_parser("brainstorm", help="Run multi-agent brainstorm session")
    _add_profile(p_brain)
    p_brain.add_argument("--topic", required=True, help="Research question or topic")
    p_brain.add_argument("--context", help="Research context text")
    p_brain.add_argument("--context-file", help="Read research context from file")

    # source-first KBase discovery
    p_discover = sub.add_parser("discover", help="Browse KBase and produce a source-grounded factor handoff")
    _add_profile(p_discover)
    p_discover.add_argument("--topic", required=True, help="Research gap to investigate")
    p_discover.add_argument("--strategy", default="b1", help="Strategy memory scope")
    p_discover.add_argument("--context", help="Additional research context")
    p_discover.add_argument("--context-file", help="Read context from file")
    discover_mode = p_discover.add_mutually_exclusive_group()
    discover_mode.add_argument(
        "--sequential", action="store_true",
        help="Use the old single-pass KBase discovery workflow",
    )

    p_resume = sub.add_parser(
        "resume-discover",
        help="Resume only the factor handoff from a completed source-first checkpoint",
    )
    _add_profile(p_resume)
    p_resume.add_argument("--handoff-path", required=True, help="Source-first discovery checkpoint YAML")
    p_resume.add_argument("--output-dir", help="Override handoff output directory")
    discover_mode.add_argument(
        "--roundtable-first", action="store_true",
        help="Use the legacy roundtable-before-KBase compatibility workflow",
    )

    # discovery execution bridge
    p_exec = sub.add_parser("execute-handoff", help="Run a registered Phase 6 runner for an APPROVED discovery handoff")
    p_exec.add_argument("--strategy", default="brick", help="Strategy memory scope when --handoff-path is omitted")
    p_exec.add_argument("--handoff-path", help="Specific discovery handoff YAML to execute")
    p_exec.add_argument("--output-dir", help="Override output directory")
    p_exec.add_argument("--dry-run", action="store_true", help="Print the execution plan without running it")

    # complete KBase -> AG2 -> research execution cycle
    p_cycle = sub.add_parser(
        "full-cycle",
        help="Run and archive discovery, runner preparation, and research backtest",
    )
    _add_profile(p_cycle)
    cycle_source = p_cycle.add_mutually_exclusive_group(required=True)
    cycle_source.add_argument("--topic", help="Research gap to discover and execute")
    cycle_source.add_argument("--handoff-path", help="Reuse one archived discovery handoff")
    p_cycle.add_argument("--strategy", default="brick", help="Strategy research scope")
    p_cycle.add_argument("--context", help="Additional research context")
    p_cycle.add_argument("--context-file", help="Read research context from file")
    p_cycle.add_argument("--output-dir", help="Override the complete-cycle archive directory")
    p_cycle.add_argument(
        "--sequential",
        action="store_true",
        help="Use the old single-pass discovery path instead of the default roundtable",
    )
    p_cycle.add_argument(
        "--no-auto-repair",
        action="store_true",
        help="Fail closed instead of generating a missing research runner",
    )
    p_cycle.add_argument("--dry-run", action="store_true", help="Archive the plan without model repair or execution")
    p_cycle.add_argument("--claude-binary", default="claude")
    p_cycle.add_argument("--repair-timeout", type=int, default=900)

    # missing-runner repair
    p_repair = sub.add_parser(
        "repair-handoff-runner",
        help="Use the code-writing repair path to add/update a research-only Phase 6 runner",
    )
    p_repair.add_argument("--handoff-path", required=True, help="Specific APPROVED discovery handoff YAML")
    p_repair.add_argument("--output-dir", required=True, help="Directory for repair prompt, diff, and logs")
    p_repair.add_argument("--failure-log", help="execute.log from the failed handoff execution")
    p_repair.add_argument("--claude-binary", default="claude")
    p_repair.add_argument("--timeout", type=int, default=900)
    p_repair.add_argument("--dry-run", action="store_true", help="Write the repair prompt without calling Claude")
    p_repair.add_argument("--skip-code-review", action="store_true", help="Skip the Code_Reviewer gate")

    # review
    p_review = sub.add_parser("review", help="Quick strategy review")
    _add_profile(p_review)
    p_review.add_argument("--file", help="Read strategy description from file")
    p_review.add_argument("--text", help="Strategy description text")
    p_review.add_argument("--context", help="Additional research context")
    p_review.add_argument("--context-file", help="Read context from file")

    # chat
    p_chat = sub.add_parser("chat", help="Custom multi-agent chat")
    _add_profile(p_chat)
    p_chat.add_argument("--agents", required=True, help="Comma-separated agent IDs")
    p_chat.add_argument("--prompt", required=True, help="Initial message")
    p_chat.add_argument("--context", help="Research context")
    p_chat.add_argument("--context-file", help="Read context from file")
    p_chat.add_argument("--max-rounds", type=int, default=15)

    # roundtable
    p_rt = sub.add_parser("roundtable", help="Multi-LLM roundtable — different LLMs debate as equals")
    p_rt.add_argument("--topic", required=True, help="Research topic or question")
    p_rt.add_argument("--models", help="Override participants: 'openai:GPT-4o,deepseek:DeepSeek,zhipu:GLM-4'")
    p_rt.add_argument("--context", help="Research context text")
    p_rt.add_argument("--context-file", help="Read research context from file")
    p_rt.add_argument("--max-rounds", type=int, default=None)

    # interactive
    p_int = sub.add_parser("interactive", help="Interactive research console")
    _add_profile(p_int)

    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = parser.parse_args(raw_argv)
    args._control_plane_authorization = authorization
    args._control_plane_argv = tuple(["run_research.py", *raw_argv])

    if not args.command:
        parser.print_help()
        return 0

    commands = {
        "list": cmd_list,
        "brainstorm": cmd_brainstorm,
        "discover": cmd_discover,
        "resume-discover": cmd_resume_discover,
        "execute-handoff": cmd_execute_handoff,
        "full-cycle": cmd_full_cycle,
        "repair-handoff-runner": cmd_repair_handoff_runner,
        "roundtable": cmd_roundtable,
        "review": cmd_review,
        "chat": cmd_chat,
        "interactive": cmd_interactive,
    }
    try:
        result = commands[args.command](args)
    except (ExecutionAuthorizationError, CampaignBoundaryError) as error:
        print(f"[run_research] blocked: {error}", file=sys.stderr)
        return 3
    if args.command == "full-cycle":
        from research_automation.kbase_ag2_full_cycle import cycle_exit_code

        return cycle_exit_code(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
