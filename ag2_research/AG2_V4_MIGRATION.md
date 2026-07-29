# AG2 v4.0 — Minimal-Change Migration Path

This guide rolls v4.0 out as 7 independent steps. Each step is
**rollback-safe**: if a step breaks, revert just that step and the prior
steps still work.

## Step 0 — Already done

The following are committed:

- `ag2_research/config.py` — added `get_agent_llm_config(agent_id)`
- `ag2_research/agents.py::create_agents` — per-agent profile dispatch
- `ag2_research/config.yaml` — `profile:` field on 6 existing agents +
  11 new agent templates added
- `ag2_research/AG2_V4_DESIGN.md` — full design

Verification:

```bash
python -c "
from ag2_research.config import ResearchConfig
cfg = ResearchConfig()
print(f'{len(cfg.agents)} agents wired')
for aid in cfg.agents:
    tpl = cfg.get_agent(aid)
    p = tpl.get('profile', 'DEFAULT')
    print(f'  {aid:<32} profile={p}')
"
```

After Step 0, your existing `run_research_cycle.py --source ag2` runs
unchanged — same 6-agent pipeline. The 11 new agents are loaded but
not invoked yet.

---

## Step 1 — Strategy state machine (P0, ~30 min)

Add a single yaml file per strategy:

```bash
mkdir -p research_state/b1_v3
```

Create `research_state/b1_v3/strategy_state.yaml`:

```yaml
schema_version: "1.0"
subject: "b1_v3"
state: "architecture_locked"
mode: "discovery"
last_transition: "2026-06-24T00:00:00Z"
transition_history:
  - {at: "2026-06-24", from: "exploring", to: "architecture_locked",
     reason: "Phase 14B constraint geometry audit closure"}
confidence:
  architecture_locked_confidence: "high"
  geometry_audited_at_phase: "14B"
  hub_node_known: "A_J_range"
allocation_target:
  architecture: 0.05
  factor_discovery: 0.25
  dimension_discovery: 0.40
  kgpr: 0.25
  maintenance: 0.05
discovery_debt:
  open_questions_count: 7
  open_questions_high_priority: 2
```

Add a reader in `research_automation/`:

```python
# research_automation/strategy_state.py  (new tiny file)
from pathlib import Path
import yaml

def load_state(subject: str) -> dict:
    p = Path("research_state") / subject / "strategy_state.yaml"
    if not p.exists():
        return {"state": "exploring", "mode": "discovery"}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
```

Now `autonomous_runner.py` and `orchestrator.py` can call
`load_state(self.strategy)` and decide channel allocation. Step 1 is
data-only; no behaviour change yet.

---

## Step 2 — Open Questions Registry (P1, ~30 min)

Seed `research_state/b1_v3/open_questions.yaml` from Phase 14A's
`ag2_research/knowledge_base/b1_v3/missing_universe_ranked.md`. Template in
`AG2_V4_DESIGN.md` section 5.

Initial content (one-time seed):

```yaml
schema_version: "1.0"
subject: "b1_v3"
updated_at: "2026-06-24T00:00:00Z"

questions:
  - id: "OQ-001"
    text: "Does a Multi-Timeframe re-aggregation expose a second entry generator?"
    status: "OPEN"
    opened_at_phase: "14A"
    priority: "high"
    blocking_for_mode: ["execution"]

  - id: "OQ-002"
    text: "Does Regime classification produce a generator independent of wave_qualified?"
    status: "OPEN"
    opened_at_phase: "14A"
    priority: "high"
    blocking_for_mode: ["execution"]

  - id: "OQ-003"
    text: "Is A_J_range's anti-alpha single-removal a hub artefact or a regime fit?"
    status: "PARTIALLY_EXPLORED"
    opened_at_phase: "14B"
    priority: "medium"
    decisive_tests_attempted: ["Phase 14B remove-three matrix"]
    related_evidence: ["Phase 14B: hub fingerprint confirmed"]

  - id: "OQ-004"
    text: "Will an alpha-only minimal reconstruction work under any param set?"
    status: "DISPROVEN"
    opened_at_phase: "12A"
    disproven_at_phase: "12B"
    related_evidence: ["Phase 12B: Jaccard 0.03-0.05, PF 1.10-1.16"]

  - id: "OQ-005"
    text: "Does Volatility-Contraction generator pass the 4-requirement bar in 2 of 3 windows?"
    status: "PARTIALLY_EXPLORED"
    opened_at_phase: "13"
    priority: "medium"
    related_evidence: ["Phase 13 G3: PF 2.77 in C window only"]

  - id: "OQ-006"
    text: "Does Cross-Sectional ranking add alpha when consumed in scoring layer?"
    status: "OPEN"
    opened_at_phase: "14A"
    priority: "medium"

  - id: "OQ-007"
    text: "Do unused parquet columns (ps, pcf, main_net_flow_*) carry alpha?"
    status: "OPEN"
    opened_at_phase: "14A"
    priority: "low"
```

After Step 2, Research_Historian (Kimi) has a registry to read. It
won't be invoked yet — Step 3 wires that in.

---

## Step 3 — Channel routing in System_Orchestrator (P0, ~1 hour)

Update System_Orchestrator's `system_message` in
`ag2_research/config.yaml`. Add a section after the existing
"SEQUENTIAL PIPELINE" block:

```
v4.0 CHANNEL ROUTING:
  At cycle start, read research_state/<subject>/strategy_state.yaml.
  Based on state + mode + allocation_target, select WHICH discovery
  agents participate this cycle. Defaults:

  state=architecture_locked, mode=discovery:
    every 4 cycles run Alpha_Hunter + Theory_Builder (Channel A/B)
    every 3 cycles run Data_Expansion_Researcher (Channel C)
    every 2 cycles run Parameter_Researcher (Channel D, KG sweep)
    every cycle:   Falsification_Officer
                   Constraint_Geometry_Auditor (audit role)
                   Statistician (prediction lock)
                   Research_Historian (post-cycle assessment)
    Code_Reviewer runs only on code-mode proposals.

  state=architecture_locked, mode=execution:
    Parameter_Researcher only (operating_point_search)
    Statistician (prediction lock + acceptance bar check)
    Code_Reviewer on any patch
    Falsification_Officer remains active

  state=maintenance:
    Parameter_Researcher (operating_point_search) every 5 cycles
    Otherwise idle. Trigger: drift detection.
```

After Step 3, AG2 actually uses the new agents. Test:

```bash
python run_research_cycle.py --source ag2 --rounds 1 --per-round 1
```

Observe `_ag2_round` output. New agents should appear in the pipeline.

---

## Step 4 — Cycle log + Surprise Engine (P1, ~1 hour)

Update `research_automation/autonomous_runner.py` to write
`research_state/<subject>/cycle_log_<cycle_id>.yaml` at the end of each
cycle.

Required fields (see AG2_V4_DESIGN.md section 7):
- `proposal` (already produced)
- `prediction` (Statistician must emit BEFORE Executor runs)
- `actual` (from Executor)
- `surprise.per_metric`, `surprise.max_surprise_score`
- `info_gain.score` (Historian writes post-cycle)

Anti-gaming guard:

```python
# in autonomous_runner — when committing the cycle log
if "prediction" in cycle_log and cycle_log["prediction"].get("locked_at"):
    raise RuntimeError(
        "prediction field is write-once; cannot be modified after lock"
    )
```

The orchestrator already serialises stages — Statistician runs before
Executor, so `prediction` is naturally locked first.

---

## Step 5 — Information Gain assessment (P1, ~2 hours)

Research_Historian's job. Already wired in Step 3. Step 5 is the
specific guidance for what it produces:

Each cycle, after Strategy_Synthesizer commits, Historian receives:
- The cycle's raw output
- The prior 20 cycle logs (via filesystem read)
- The current KB (via kb_lookup)

And outputs `info_gain_assessment` block in the cycle log (per
AG2_V4_DESIGN.md section 6).

Director (System_Orchestrator) reads `sum(info_gain_score)` over the
last 3 cycles. If zero, raise `hard_stop_pending` and either:
- transition `state` to `maintenance`, OR
- emit `ESCALATE_TO_USER`

This logic lives in `system_orchestrator.system_message` Step 0 of each
cycle.

---

## Step 6 — Research Capital tracking (P2, ~2 hours)

Each agent invocation: AG2 reports `tokens_in` and `tokens_out`. Capture
into `research_state/<subject>/research_capital_<cycle>.yaml`.

`research_automation/` already has token plumbing in
`AG2TaskAdapter`. Add a thin accumulator:

```python
# research_automation/capital_tracker.py  (new tiny file)
from pathlib import Path
import yaml, json

def record_agent_usage(subject: str, cycle_id: str,
                       agent_name: str, profile: str,
                       tokens_in: int, tokens_out: int,
                       channel: str, info_gain: int):
    p = Path("research_state") / subject / f"research_capital_{cycle_id}.yaml"
    data = yaml.safe_load(p.read_text(encoding="utf-8")) if p.exists() else {
        "cycle": cycle_id, "subject": subject,
        "agents": [], "channels": {},
    }
    data["agents"].append({
        "name": agent_name, "profile": profile,
        "tokens_in": tokens_in, "tokens_out": tokens_out,
        "info_gain": info_gain,
    })
    ch = data["channels"].setdefault(channel,
        {"tokens": 0, "proposals": 0, "info_gain": 0})
    ch["tokens"] += tokens_in + tokens_out
    ch["info_gain"] += info_gain
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
```

Then aggregate to `research_state/<subject>/agent_performance.json` on
some schedule (weekly).

---

## Step 7 — Code Pipeline with Code_Reviewer (P2, ~1 hour)

`ClaudePatchExecutor.apply()` already has a KB hard-constraint gate
(committed earlier). Add a Code_Reviewer step between Claude's diff
generation and `_apply_patch_to_workspace`:

```python
# in patch_executor.py, after KB gate, before _apply_patch_to_workspace

if experiment is not None:
    from ag2_research.config import ResearchConfig
    from ag2_research.agents import create_agents

    cfg = ResearchConfig()
    reviewer = create_agents(cfg, ["code_reviewer"], research_context="")
    reviewer_agent = next(iter(reviewer.values()))

    review_prompt = f"""Review this diff. APPROVE / REQUEST_CHANGES / REJECT.

Design: {experiment.proposal.hypothesis}

Diff:
{diff}
"""
    # synchronous one-turn invocation; output must be parseable JSON
    review_result = reviewer_agent.generate_reply(messages=[
        {"role": "user", "content": review_prompt}
    ])
    # parse JSON, check verdict
    if "REJECT" in review_result or '"verdict": "REJECT"' in review_result:
        return CodeChangeResult(
            ok=False,
            error=f"Code_Reviewer rejected: {review_result[:500]}",
            logs=[review_result],
        )
```

This makes Code_Reviewer mandatory for code-mode patches. Risk_Controller
remains as the post-execution audit.

---

## Quick-reference: what each step buys you

| Step | Time | Effect |
|---|---:|---|
| 0 | done | Per-agent LLM routing infrastructure |
| 1 | 30m | Strategy state machine readable from yaml |
| 2 | 30m | Open questions persisted; Historian has memory to work with |
| 3 | 1h | New agents actually invoked by Orchestrator |
| 4 | 1h | Surprise engine + cycle logs |
| 5 | 2h | Information gain ⇒ hard-stop rule activates |
| 6 | 2h | Cost tracking per channel/agent |
| 7 | 1h | Code_Reviewer enforces design fidelity on every patch |

**Total: ~7-8 hours of work, split across 7 commits. Each commit
keeps the system in a working state.**

---

## Validation checklist per step

Run after each step:

```bash
# Smoke test: AG2 path still works
python run_research_cycle.py --dry-run --rounds 1 --per-round 1 --source ag2

# Verify config loads
python -c "from ag2_research.config import ResearchConfig; \
           print(ResearchConfig().list_agents())"

# Verify per-agent LLM dispatch
python -c "
from ag2_research.config import ResearchConfig
c = ResearchConfig()
for aid in c.agents:
    p = c.get_agent(aid).get('profile') or c.default_profile
    print(f'{aid:<32} -> {p}')
"

# After Step 1+: verify state machine reads
python -c "
import sys; sys.path.insert(0, 'research_automation')
from strategy_state import load_state
print(load_state('b1_v3'))
"

# After Step 3: verify discovery agents fire (look for them in logs)
python run_research_cycle.py --source ag2 --rounds 1 --per-round 1
# grep logs for: Theory_Builder, Alpha_Hunter, ...
```

---

## Rollback (if any step breaks)

| Step | Rollback |
|---|---|
| 0 | `git checkout HEAD~ ag2_research/{config.py,agents.py,config.yaml}` |
| 1 | `rm -rf research_state/b1_v3/strategy_state.yaml research_automation/strategy_state.py` |
| 2 | `rm research_state/b1_v3/open_questions.yaml` |
| 3 | revert system_orchestrator system_message changes in config.yaml |
| 4 | revert autonomous_runner.py changes; cycle logs harmless to keep |
| 5 | unwire Research_Historian; hard-stop logic moot |
| 6 | revert capital_tracker.py + accumulator calls |
| 7 | revert patch_executor.py changes |

Step N only requires Steps 0..N-1 to be present. Steps 4-7 can be
applied in any order after Step 3.
