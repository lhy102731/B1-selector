"""Build a research_context block to inject into AG2 agents' system_message.

The block is intentionally compact (so it does not eat the context window)
and deterministic (same KB version -> same string -> stable prompts).
"""
from __future__ import annotations

from .loader import KnowledgeBase, load


_HEADER = (
    "================================================================\n"
    "  KNOWLEDGE BASE: {fingerprint}\n"
    "================================================================\n"
)

_FOOTER = (
    "----------------------------------------------------------------\n"
    "  REMINDER: You MUST consult this knowledge base before proposing\n"
    "  any change. Hard rules are machine-enforced. Use the\n"
    "  kb_lookup_b1v3 and kb_validate_proposal tools when uncertain.\n"
    "================================================================\n"
)


def build_context(subject: str, *, mode: str = "brief") -> str:
    """Return a research_context string for the given strategy.

    mode:
        "brief"   - verdict_brief.md only (~120 lines). Default.
        "headers" - brief + alpha/exit/concentrator headers only (~200 lines).
        "full"    - brief + full verdict + interaction summary (~400 lines).

    The orchestrator should pass this string as the `research_context`
    argument of ag2_research.agents.create_agents().
    """
    kb = load(subject)
    parts: list[str] = [_HEADER.format(fingerprint=kb.fingerprint())]

    if mode in ("brief", "headers", "full"):
        parts.append(kb.read_text("brief"))

    if mode in ("headers", "full"):
        parts.append("\n## Alpha generators (headline)\n")
        for g in kb.alpha_generators():
            parts.append(f"- {g['id']}: {g.get('mechanism','')[:200]}\n")
        parts.append("\n## Exit alphas (headline)\n")
        for e in kb.exit_alphas():
            parts.append(f"- {e['id']}: {e.get('mechanism','')[:200]}\n")
        parts.append("\n## Concentrators (top 6)\n")
        for c in kb.concentrators()[:6]:
            parts.append(
                f"- {c['id']} ({c.get('type','?')}): "
                f"single_dPF={c.get('phase14b_single_d_pf_avg','-')}, "
                f"role={c.get('role','-')}\n"
            )

    if mode == "full":
        parts.append("\n## Full verdict\n")
        parts.append(kb.read_text("full_verdict"))

    parts.append(_FOOTER)
    return "".join(parts)


def build_context_for_agents(subject: str,
                             agent_modes: dict[str, str] | None = None) -> dict[str, str]:
    """Return per-agent research_context strings.

    Useful when different agents need different verbosity:
        {
            "alpha_researcher":   "full",
            "risk_manager":       "headers",
            "strategy_engineer":  "brief",
        }

    Returns a dict mapping agent_id -> context_string.
    """
    agent_modes = agent_modes or {}
    out = {}
    for agent_id, mode in agent_modes.items():
        out[agent_id] = build_context(subject, mode=mode)
    return out
