# AG2 Integration Guide — How the B1 V3 Knowledge Base plugs in

This guide walks through every concrete change needed to make the
`ag2_research.knowledge_base` package an actively-used part of the AG2
research and proposal pipeline. Every step is reversible and isolated;
nothing here changes existing strategy or backtest logic.

## Step 1 — Verify the package loads

```bash
python -c "from ag2_research.knowledge_base import load, build_context, validate_proposal; \
           kb = load('b1_v3'); \
           print(kb.fingerprint()); \
           print(len(build_context('b1_v3', mode='brief'))); \
           print(validate_proposal('b1_v3', {'subject':'b1_v3','hypothesis':'test'}))"
```

Expected:
- prints `b1_v3@1.0.0 (Phase 15)`
- prints a length around 3500-5000
- prints a JSON dict with `verdict: allow` and `needs_evidence` non-empty
  (because the test proposal has no measurement plan)

## Step 2 — Inject context into AG2 agents

`ag2_research.agents.create_agents` already accepts a `research_context`
argument. The orchestrator that calls `create_agents` should be modified
to fetch the context for the strategy under research:

```python
# in ag2_research/orchestrator.py (or wherever create_agents is called)

from ag2_research.knowledge_base import build_context

ctx = build_context("b1_v3", mode="brief")
agents = create_agents(config, agent_ids, research_context=ctx)
```

For multi-strategy runs, dispatch by current subject:

```python
ctx = build_context(current_subject, mode="brief")
```

For agents that need richer context (e.g. an alpha_researcher role):

```python
from ag2_research.knowledge_base import build_context_for_agents

per_agent_ctx = build_context_for_agents(
    "b1_v3",
    agent_modes={
        "alpha_researcher": "full",
        "strategy_engineer": "headers",
        "risk_manager":      "headers",
        "evaluator":         "brief",
    },
)
# then loop and create one agent at a time with its own context
```

### Where to inject — system_message placeholder

`agents.create_agents` looks for `{research_context}` in the agent
template's system_message. Add this placeholder to the templates that
should consume the KB. Example (`ag2_research/config.yaml`):

```yaml
agents:
  alpha_researcher:
    name: AlphaResearcher
    description: "Searches for new alpha components"
    system_message: |
      You are AlphaResearcher.
      Before proposing any alpha, consult the strategy knowledge base:

      {research_context}

      Use the kb_lookup tool to fetch additional sections when needed.
      Use the kb_validate_proposal tool BEFORE submitting any proposal.
    tools:
      - kb_lookup
      - kb_validate_proposal
      - get_strategy_config
      - list_research_docs
      - read_research_doc
```

The two new tools (`kb_lookup`, `kb_validate_proposal`) are already
registered in `ag2_research/tools.py::TOOL_REGISTRY`. Just list them
under each agent's `tools:` key in the config.

## Step 3 — Wire the hard gate into research_automation

The `kb_gate.gate_proposal_kb` function returns one of three verdicts:
`allow`, `reject`, `needs_evidence`. Add it as the LAST check in the
existing capability gate.

### autonomous_runner.py (proposal filtering loop)

Current code at line ~332:

```python
if not self._is_supported_code_change(cc):
    print(f"    filtered unsupported code_change: ...")
    continue
```

Add right after that block:

```python
from research_automation.kb_gate import gate_proposal_kb

verdict = gate_proposal_kb(self.strategy, {
    "subject": self.strategy,
    "hypothesis": getattr(rp, "hypothesis", ""),
    "scope": {"code_change": cc, "params": task_scope.get("params", {})},
    "measurement_plan": getattr(rp, "measurement_plan", None) or {},
})
if verdict["verdict"] == "reject":
    print(f"    KB rejected: {verdict['violations']} -- {verdict['reasons'][:2]}")
    continue
if verdict["verdict"] == "needs_evidence":
    print(f"    KB needs_evidence: {verdict['needs_evidence']}")
    # Either skip, or stash a follow-up task that asks the agent to
    # supply the missing evidence on the next round.
    continue
```

### patch_executor.py (final write gate)

At the top of the function that performs the file modification, add:

```python
from research_automation.kb_gate import gate_proposal_kb

verdict = gate_proposal_kb(experiment.strategy, {
    "subject": experiment.strategy,
    "hypothesis": experiment.proposal.hypothesis,
    "scope": experiment.proposal.scope,
})
if verdict["verdict"] != "allow":
    raise RuntimeError(f"Patch rejected by KB: {verdict}")
```

This is defense in depth: even if an agent bypasses
`kb_validate_proposal` and gets through `autonomous_runner`, the patch
executor will refuse to write.

## Step 4 — Update agent config templates to reference KB

Recommended template additions (paste into `ag2_research/config.yaml`):

```yaml
agents:
  alpha_researcher:
    tools: [kb_lookup, kb_validate_proposal, ...existing tools...]
    system_message: |
      ...
      MANDATORY: Before proposing any change to a strategy, call
      kb_lookup(subject=<strategy_id>, section='brief') and read the
      verdict. If your hypothesis matches a phase that has been falsified
      in the closure, you must justify it with new evidence.

      Before submitting any proposal, call
      kb_validate_proposal(subject=<strategy_id>, proposal_json=<your proposal>)
      and STOP if the verdict is 'reject'. Address each 'needs_evidence'
      item before submitting.

      Knowledge base context for the current subject:
      {research_context}

  risk_manager:
    tools: [kb_lookup, ...existing tools...]
    system_message: |
      ...
      You may NOT approve proposals that would touch frozen parameters.
      Always confirm by calling kb_lookup(subject=<strategy_id>,
      section='hard_constraints').

      Knowledge base context:
      {research_context}
```

## Step 5 — Smoke test the full path

After Steps 2-4 are applied, run:

```bash
python - <<'PY'
from ag2_research.knowledge_base import validate_proposal

# This proposal touches the hub j_max — must be REJECTED
bad = {
    "subject": "b1_v3",
    "hypothesis": "tighten j_max to 25 for better signal quality",
    "scope": {"params": {"j_max": 25}},
}
print(validate_proposal("b1_v3", bad))

# This proposal proposes alpha-only reconstruction — must be REJECTED
falsified = {
    "subject": "b1_v3",
    "hypothesis": "keep alpha only, remove all concentrators",
    "scope": {"params": {}},
}
print(validate_proposal("b1_v3", falsified))

# Valid: touches a concentrator with proper plan
ok = {
    "subject": "b1_v3",
    "hypothesis": "test removing turnover_max in all windows",
    "scope": {"params": {"turnover_max": 1e9}},
    "measurement_plan": {
        "windows": ["A_2023", "B_2024H1", "C_2024H2_latest"],
        "reports": ["trades","total_return_pct","max_drawdown_pct",
                    "sharpe","profit_factor","jaccard_vs_baseline"],
        "aggregates": ["pf_retention","sharpe_retention","all_windows_pass_bar"],
    },
}
print(validate_proposal("b1_v3", ok))
PY
```

Expected:
- bad   -> `verdict: reject`,  violations include `FROZEN_HUB_A_J_RANGE`
- falsified -> `verdict: reject`, violations include `NO_ALPHA_ONLY_RECONSTRUCTION`
- ok    -> `verdict: allow` (warnings list may include the conditional rule)

## Step 6 — Version management

When you run a NEW research closure (e.g. Phase 16 modifies findings):

1. Update the JSON / Markdown artifacts under
   `ag2_research/knowledge_base/b1_v3/`.
2. Bump `kb_version` in `manifest.yaml` (semver):
   - patch increment for fact corrections that do not change rules
   - minor for new rules / new lessons
   - major for invalidating prior rules
3. The validator and context_builder pick up the new version
   automatically (loader is lru-cached per process; restart agents).
4. Update `as_of_phase` and `closed_at`.

## Step 7 — Adding a new strategy

To onboard `b2_v1`:

```bash
mkdir ag2_research/knowledge_base/b2_v1
# author manifest.yaml + verdict_brief.md + hard_constraints.yaml
# add JSON exports following the b1_v3 schema
```

The loader/validator/context_builder dispatch by subject argument — no
code changes needed. Run `list_subjects()` to verify:

```bash
python -c "from ag2_research.knowledge_base import list_subjects; print(list_subjects())"
```

## What this gives you

1. **Static context injection** — agents start every cycle with the
   verdict + rules baked into their system prompt.
2. **On-demand tool lookup** — agents can pull specific sections / JSON
   blobs without re-loading docs.
3. **Hard-rule enforcement at proposal time** — invalid proposals are
   rejected before backtest cycles are spent.
4. **Hard-rule enforcement at patch time** — even if an agent bypasses
   the proposal validator, the patch executor refuses to write files
   that would break frozen invariants.
5. **Versioned and auditable** — every reject carries a `kb_version`
   tag and a `rule_id`; every allow has the same provenance.
6. **Generalizable** — same machinery works for any future strategy
   by adding a new subdirectory.

## Knowledge Bridge v1

The integration now also reads validated external claims and writes experiment evidence back to the Obsidian vault. Use `ag2_research.knowledge_bridge.build_combined_research_context()` instead of injecting raw wiki pages. The project closure remains first in the authority order. Automated results are written as `output_only` and require independent review before they can become claims or change a project KB version.
