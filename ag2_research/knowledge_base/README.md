# Knowledge Base Packaging Guide

See [KNOWLEDGE_BRIDGE.md](KNOWLEDGE_BRIDGE.md) for the validated-claim intake and experiment writeback contract with `D:\KBase`.

This document explains how the B1 V3 research closure (Phase 9-15) is packaged
for the AG2 automated strategy optimization system.

## Layout

```
ag2_research/
├── knowledge_base/                    <- THIS PACKAGE
│   ├── __init__.py                    one-line public API
│   ├── b1_v3/                         strategy-scoped knowledge package
│   │   ├── manifest.yaml              canonical entry point + version
│   │   ├── verdict_brief.md           120-line agent-pasteable summary
│   │   ├── hard_constraints.yaml      machine-enforceable do/don't list
│   │   ├── alpha_generators.json      canonical machine-readable summary
│   │   ├── exit_generators.json
│   │   ├── concentrators.json
│   │   ├── dead_components.json
│   │   ├── redundant_components.json
│   │   ├── interaction_summary.json
│   │   ├── research_lessons.json
│   │   ├── architecture_full.md       full Phase 15 architecture
│   │   ├── constraint_geometry.md     full Phase 15 geometry report
│   │   ├── missing_universe_ranked.md Phase 14A research-gap ranking
│   │   └── dependency_graph.md        full Phase 15 graph report
│   ├── loader.py                      load + validate manifest, return KB object
│   ├── context_builder.py             builds an injectable research_context block
│   └── proposal_validator.py          rejects/warns AG2 proposals against KB
└── (existing ag2_research files)
```

## How AG2 consumes it

### 1. Static context injection at agent creation

`agents.create_agents(...)` already accepts a `research_context` string.
The knowledge base provides `context_builder.build_b1_v3_context()` which
returns a deterministic Markdown block. The orchestrator should pass this
as `research_context` for any agent whose role touches B1 V3.

This puts the strategy's verdict + hard constraints inside the agent's
system prompt. Every agent reasons with that ground truth without needing
to call tools.

### 2. On-demand tool lookup

`tools.py` gets two new tools:

- `kb_lookup_b1v3(question: str)` — agent calls this when uncertain about
  a specific component. Returns relevant JSON snippets + source phase
  citations.
- `kb_validate_proposal(proposal: dict)` — agent calls this BEFORE
  submitting a research proposal. Returns one of:
    - `{"verdict": "allowed"}`
    - `{"verdict": "violation", "reasons": [...], "violated_rules": [...]}`
    - `{"verdict": "needs_evidence", "missing": [...]}`

These tools are deterministic file-readers, not LLM calls. They are cheap
and produce auditable output.

### 3. Hard gate inside `research_automation.proposer` / `patch_executor`

`proposal_validator.validate()` is also wired into the proposal pipeline
in `research_automation/`. Any proposal that violates a hard constraint is
rejected at submission time, regardless of whether the agent followed the
soft instructions in its system prompt. This protects against agents that
hallucinate around the closure rules.

### 4. Versioning

`manifest.yaml` records:
- `kb_version` (semver-style)
- `subject` ("b1_v3")
- `as_of_phase` ("Phase 15")
- `evidence_universe` (60-code A-share, 2023-01-01 → 2026-06-22)
- `frozen_baseline` (the metric block agents must beat to claim improvement)

When the knowledge base is updated (e.g. Phase 16 invalidates a finding),
bump `kb_version`. Agents should refuse to act on stale knowledge by
checking `kb_version` on load.

## Hard rules baked into hard_constraints.yaml

These rules cannot be overridden by an agent:

1. Do not delete or relax `j_max` / `j_min` / `prefilter_j_max` (hub
   constraint).
2. Do not modify `t_plus_3_min_return` without re-running Phase 10/11
   equivalents.
3. Do not propose changes that affect the exit logic (yellow/white break,
   profit_25pct, stop_loss, max_hold) without explicit human authorization.
4. Do not propose changes to `C_wave_qualified` (require_wave_qualified=True
   stays True, wave_* thresholds frozen).
5. Any proposal must include a measurement plan that reports per-window
   Trades, Return, MaxDD, Sharpe, PF, Jaccard.
6. Acceptance bar: PF retention >= 80% AND Sharpe retention >= 80% AND
   Jaccard >= 0.50 in ALL three reference windows (A_2023 / B_2024H1 /
   C_2024H2_latest).
7. Single-component ablation evidence is NOT acceptable for redundancy
   claims. Must include pair-removal test.
8. Re-experimenting on already-falsified hypotheses (Phase 15 verdict §3)
   is forbidden unless the proposal contains a phase-specific argument
   why the prior finding does not apply.

## How to extend

For a new strategy (e.g. B2_v1):

1. Create `ag2_research/knowledge_base/b2_v1/`.
2. Author `manifest.yaml`, `verdict_brief.md`, `hard_constraints.yaml`.
3. Add JSON exports following the same schema as B1 V3.
4. Update `loader.py::list_subjects()` to include the new package.
5. `context_builder.build(subject)` and `proposal_validator.validate(subject, ...)`
   work without further changes (dispatch by `subject` field).
