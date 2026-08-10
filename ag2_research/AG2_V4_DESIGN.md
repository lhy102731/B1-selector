# AG2 v4.0 — Research Operating System Design

This document covers the architectural design of v4.0. Items 2/3/9 are
implemented as concrete file changes; items 1/4/5/6/7/8/10 are designed
here.

## 0. Scope and constraints

**Inherits intact from v3.x**:
- `ag2_research/orchestrator.py` (SOLE controller pattern)
- `ag2_research/agents.py` (factory; will get one tiny edit to read per-agent profile)
- `ag2_research/config.py` (profile registry; will get one tiny method addition)
- `ag2_research/knowledge_base/*` (Phase 15 closure infra; unchanged)
- `research_automation/*` (autonomous_runner, patch_executor, kb_gate; unchanged
  except they will start reading `strategy_state`)

**Not changed in v4.0**:
- Strategy backtest code (`strategy/b1_v3_strategy.py` etc.)
- Indicator computation
- Memory priority (Snapshot > Handoff > Registry > Memory > Code)
- The control-layer rules in `CONTROL_LAYER_SPEC.yaml`

**v4.0 changes the prompts, the routing, and the operational state files.
It does not change the runtime control flow of the orchestrator.**

**P6 owner split.** The P6 control plane is the sole governance and persistence owner.
AG2 roles (including System_Orchestrator) produce draft decisions and deltas;
Capital Tracker / Coverage Map / Agent Performance files are analytics-only projections.

---

## 1. Architecture overview

```
                 +-----------------------------+
                 |     User / Operator         |
                 +--------------+--------------+
                                |
                                v
  +----------------------------------------------------+
  |        Director (System_Orchestrator)              |
  |  reads strategy_state, picks Mode + Channel mix    |
  +--+--------------+--------------+----------------+--+
     |              |              |                |
     v              v              v                v
  Mode:        Mode:           Mode:            Mode:
  Discovery    Discovery       Execution        Maintenance
  Channel A    Channel B/C/D   Channel D/E      Channel E only

  +--------+ +--------+ +-----------+ +-----------+ +---------+
  | Theory | | Alpha  | | Geometry  | | Falsif    | | Data    |
  | Builder| | Hunter | | Auditor   | | Officer   | | Expand  |
  +--------+ +--------+ +-----------+ +-----------+ +---------+
       |         |          |              |             |
       +----+----+----+-----+--------+-----+-------------+
            |              |          |
            v              v          v
       +--------+    +---------+ +---------+
       | Param  |    | Factor  | | Regime  |
       | Research|   | Engineer| | Research|
       +--------+    +---------+ +---------+
                                       |
                +----------------------+
                |
                v
       +-------------------------+
       | Statistician (lock      |
       | prediction; per-window  |
       | metrics; surprise)      |
       +-----------+-------------+
                   |
                   v
       +-------------------------+
       | Data_Validator + KB     |
       | hard_constraints gate   |
       +-----------+-------------+
                   |
                   v
       +-------------------------+
       | Experiment_Executor     |
       +-----------+-------------+
                   |
                   v
       +-------------------------+
       | Risk_Controller (diff   |
       | model family from exec) |
       +-----------+-------------+
                   |
                   v
       +-------------------------+
       | Code_Reviewer (only on  |
       | code-mode proposals)    |
       +-----------+-------------+
                   |
                   v
       +-------------------------+
       | Strategy_Synthesizer    |
       | + Research_Historian    |
       | (write deltas, info_gain|
       | assessment, surprise log)
       +-------------------------+
```

The pipeline is still **one pass per cycle**, controlled by
System_Orchestrator. Discovery agents (Theory / Alpha / Geometry /
Falsif / Data) feed proposals into the existing
proposer→validator→executor→risk→synth chain. They do not run on every
cycle — Director picks who participates per-cycle from the strategy
state.

---

## 2. Strategy lifecycle state machine

State file: `research_state/<subject>/strategy_state.yaml`

```yaml
# managed by Research_Historian; persisted only by the P6 control plane (AG2 roles emit drafts)
schema_version: "1.0"
subject: "b1_v3"
state: "architecture_locked"      # exploring | architecture_locked | maintenance
mode:  "discovery"                # discovery | execution | maintenance
last_transition: "2026-06-24T12:00:00Z"
transition_history:
  - {at: "2026-06-15", from: "exploring",
     to: "architecture_locked", reason: "Phase 14B closure"}

confidence:
  architecture_locked_confidence: high     # low | medium | high
  geometry_audited_at_phase: 14B
  hub_node_known: "A_J_range"

allocation_target:                  # active research budget allocation
  architecture: 0.05
  factor_discovery: 0.25
  dimension_discovery: 0.40
  kgpr: 0.25                        # knowledge-generating param research
  maintenance: 0.05
allocation_actual_last_5_cycles:    # filled by Director each cycle
  architecture: 0.04
  factor_discovery: 0.30
  dimension_discovery: 0.36
  kgpr: 0.25
  maintenance: 0.05

discovery_debt:                     # how many open questions remain
  open_questions_count: 7
  open_questions_high_priority: 2
```

### State definitions

| State | Trigger to enter | Trigger to leave |
|---|---|---|
| `exploring` | New strategy registered, or major architecture falsified | Architecture confidence reaches `high` AND open_questions_high_priority == 0 |
| `architecture_locked` | Closure phase completed (e.g. Phase 14B for B1 V3) | New Channel A architecture proposal accepted; or KB hard rule revoked |
| `maintenance` | All open_questions resolved AND 3 cycles no info_gain | Open question reopened by Falsification Officer |

### Default allocations per state (Director may dynamically adjust)

| Channel | exploring | architecture_locked | maintenance |
|---|---:|---:|---:|
| Architecture (A) | 40% | 5% | 0% |
| Factor (B) | 25% | 25% | 5% |
| Dimension (C) | 20% | 40% | 10% |
| KGPR (D) | 10% | 25% | 25% |
| Maintenance (E) | 5% | 5% | 60% |

These are budgets, not quotas. Director can deviate up to ±10pp per
channel before triggering DIVERSITY_WARNING.

### Diversity rules per state

| State | Max single-channel concentration over last 20 cycles |
|---|---:|
| exploring | 100% (no rule — let focus happen) |
| architecture_locked | 75% |
| maintenance | 50% |

---

## 3. Discovery Mode vs Execution Mode

`strategy_state.mode` is separate from `strategy_state.state`. The state
says "where the strategy is in its lifecycle". The mode says "what kind
of work the current cycle is doing".

| Mode | Allowed channels | Success metric | Output type |
|---|---|---|---|
| `discovery` | A, B, C, D (with sweep_intent=knowledge_generating or boundary_probing) | information_gain | hypotheses, theories, falsifications, new factor specs, new dimension proposals |
| `execution` | D (operating_point_search), E | acceptance_bar pass | code changes, validated configs, backtest reports |
| `maintenance` | E + drift detection | drift < threshold | regime refresh outputs, parameter updates within bar |

### Mode transitions

Director decides mode per cycle. Inputs:

```
allow_execution_mode = (
    state in {"architecture_locked","maintenance"}
    AND geometry_auditor.confidence in {"medium","high"}
    AND falsification_officer.last_5_decisive_tests_all_failed_to_overturn
    AND open_questions.high_priority_count == 0
    AND there_exists_a_validated_deployment_candidate
)

force_discovery_mode = (
    falsification_officer.produced_decisive_test_in_last_3_cycles
    OR alpha_hunter.produced_new_alpha_family_proposal_in_last_3_cycles
    OR statistician.surprise_score_max_last_3_cycles >= 0.5
)
```

Default: `discovery`. Execution is opt-in.

---

## 4. Research channels

Channels are mutually orthogonal sources of work. Director maintains the
per-cycle allocation by routing proposals.

### Channel A — Architecture Discovery

Agents involved: `Theory_Builder` (causal), `Alpha_Hunter` (ideation),
`Constraint_Geometry_Auditor` (structure), `Falsification_Officer`
(counter-proposals).

Proposal schema:

```yaml
proposal_kind: architecture
architecture_classification:
  current: "constraint_geometry"
  hypothesised_alternative: "regime_driven_basket_rotation"
  decisive_test: "<run plan that would distinguish them>"
expected_information_gain: "<what does the test resolve>"
```

### Channel B — Factor Discovery

Agents: `Alpha_Hunter`, `Factor_Engineer` (mass factor ideation; Seed),
`Statistician` (validates).

Proposal schema:

```yaml
proposal_kind: factor
factor_spec:
  name: "RS_zscore_over_ATR"
  expression: "(close / close.shift(60) - 1) / ATR(14)"
  family: "Relative Strength"
  polarity: positive | negative | anti_factor
expected_jaccard_vs_existing_factors: <float>
expected_information_gain: <string>
```

### Channel C — Dimension Discovery

Agents: `Data_Expansion_Researcher`, `Regime_Researcher`.

Proposal schema:

```yaml
proposal_kind: dimension
dimension:
  category: "Multi-Timeframe" | "Regime" | "Breadth" | "Cross-Sectional" | "Flow" | "Alternative"
  proposed_implementation: "<how to compute>"
  required_data: "<what data source, available?>"
expected_overlap_with_existing: <high|medium|low>
expected_information_gain: <string>
```

### Channel D — Knowledge-Generating Parameter Research

Agent: `Parameter_Researcher`.

Proposal schema (already designed in v3.1 critique; reproduced):

```yaml
proposal_kind: parameter_sweep
sweep_intent:
  one_of: [knowledge_generating, boundary_probing, operating_point_search]
parameter: <name>
values: [...]                       # at least 3 points
windows: [A_2023, B_2024H1, C_2024H2_latest]   # required
expected_knowledge: <non-empty string>
falsification_link: <open_question_id | null>
acceptance_bar_relevant: <bool>
```

KB gate rejects `operating_point_search` unless
`strategy_state.state == architecture_locked` AND
`mode == execution`.

### Channel E — Operating Point Search

Same as Channel D mode `operating_point_search` but accepted only in
Execution Mode and only when the search-space has been pre-cleared by KB.

---

## 5. Open Research Questions Registry

File: `research_state/<subject>/open_questions.yaml`

Owned by Research_Historian. Mutated only when System_Orchestrator
commits proposed transitions.

```yaml
schema_version: "1.0"
subject: "b1_v3"
updated_at: "2026-06-24T..."

questions:
  - id: "OQ-001"
    text: "Does a Multi-Timeframe re-aggregation expose a second entry generator?"
    status: "OPEN"            # OPEN | PARTIALLY_EXPLORED | CONFIRMED | DISPROVEN | ARCHIVED
    opened_at_phase: "14A"
    opened_by_agent: "Data_Expansion_Researcher"
    priority: "high"          # high | medium | low
    decisive_tests_attempted: []
    related_evidence: ["Phase 13: no 2nd generator in PSM daily"]
    blocking_for_mode: ["execution"]  # cannot enter execution while this is open AND high

  - id: "OQ-002"
    text: "Does Regime classification produce a generator independent of wave_qualified?"
    status: "OPEN"
    opened_at_phase: "14A"
    priority: "high"
    related_evidence: []

  - id: "OQ-003"
    text: "Is A_J_range's apparent anti-alpha single-removal a hub artefact or a regime fit?"
    status: "PARTIALLY_EXPLORED"
    opened_at_phase: "14B"
    priority: "medium"
    decisive_tests_attempted: ["Phase 14B remove-three matrix"]
    related_evidence: ["Phase 14B: hub fingerprint confirmed"]
    blocking_for_mode: []

  - id: "OQ-004"
    text: "Will alpha-only minimal reconstruction work under any param set?"
    status: "DISPROVEN"
    opened_at_phase: "12A"
    disproven_at_phase: "12B"
    related_evidence: ["Jaccard 0.03-0.05, PF 1.10-1.16"]
```

### Lifecycle of an OQ

```
new evidence (Falsification, Alpha_Hunter, Theory_Builder)
   -> Research_Historian drafts transition
       -> Director (System_Orchestrator) approves or rejects
           -> if approve: commit + bump kb_version if status reaches CONFIRMED/DISPROVEN
```

DISPROVEN questions feed `ag2_research/knowledge_base/<subject>/hard_constraints.yaml::forbidden_hypotheses`. So the OQ registry is the "live" view; KB is the "frozen" view.

---

## 6. Information Gain

`information_gain` is the v4.0 replacement for "new_knowledge_count".

### Definition

Research output `O` from cycle `C` is granted `information_gain > 0` iff
at least one of:

1. `O` is not present in any existing KB artifact AND is not a duplicate
   of any prior cycle's output.
2. `O` refines an existing KB entry with new evidence (e.g. tighter
   confidence bound, new window covered, new failure mode identified).
3. `O` overturns a previously CONFIRMED belief or moves a question from
   OPEN to DISPROVEN.

### Assessment

Research_Historian (Kimi, long context) performs the assessment at end
of each cycle. Output:

```yaml
info_gain_assessment:
  cycle_id: "..."
  produced_by_agent: <agent_name>
  raw_output_summary: "<one sentence>"
  comparison_basis: "kb_version + last_20_cycle_logs"
  novelty: novel | duplicate | refinement | overturn
  significance: trivial | minor | moderate | major
  info_gain_score: 0 | 1 | 2 | 3   # 0=duplicate, 1=trivial, 2=minor, 3=moderate, 4=major
  rationale: "<two sentences>"
```

### Hard-stop rule

`hard_stop_pending = (sum(info_gain_score over last 3 cycles) == 0)`

When `hard_stop_pending == True` for 3 cycles in a row, Director
either:
- Enters `maintenance` state (Soft Stop), OR
- Escalates to user (Hard Stop)

---

## 7. Surprise Engine

### Workflow

```
Director picks proposal
   -> Statistician produces baseline expectation:
       prediction_metrics: {return, dd, sharpe, pf, trades, jaccard}
       prediction_basis: "kb_baseline + last_5_cycle_drift"
       prediction_locked_at: <ts>
   -> [LOCK ENFORCED: cycle log writes prediction; no rewrite allowed]
   -> Experiment_Executor runs
   -> Statistician computes:
       actual_metrics: {...}
       surprise_score = max( |actual - prediction| / max(|prediction|, eps) ) over metrics
   -> Research_Historian writes surprise_memo if surprise_score >= 0.3
```

### Cycle log schema

File: `research_state/<subject>/cycle_log_<cycle_id>.yaml`

```yaml
schema_version: "1.0"
cycle_id: "..."
subject: "b1_v3"
strategy_state_at_start: {state: "architecture_locked", mode: "discovery"}
proposal:
  proposal_kind: "parameter_sweep"
  channel: "D"
  sweep_intent: "knowledge_generating"
  ...
prediction:
  locked_at: "..."
  by_agent: "Statistician"
  metrics: {pf: 2.30, sharpe: 1.30, return: 8.5, dd: -5.0, trades: 80, jaccard: 0.85}
  basis: "kb_baseline.A_2023 with -10% trade discount for tighter filter"
actual:
  metrics: {pf: 2.85, sharpe: 1.91, return: 18.5, dd: -8.0, trades: 95, jaccard: 0.62}
surprise:
  per_metric: {pf: 0.24, sharpe: 0.47, return: 1.18, dd: 0.60, trades: 0.19, jaccard: 0.27}
  max_surprise_score: 1.18         # return came in 118% above prediction
  surprise_metric: "return"
  surprise_memo_written: true
info_gain:
  novelty: refinement
  significance: moderate
  info_gain_score: 3
```

### Anti-gaming

- Prediction is locked at time T1, Executor runs at T2 > T1, Statistician
  receives actual at T3. Predictions are never revised post-hoc.
- Predictions must come from Statistician (Mimo). Agents cannot supply
  their own predictions for the surprise calculation.
- Eligible surprise metrics are fixed: `{return, dd, sharpe, pf, trades, jaccard}`.

---

## 8. Research Capital Allocation

### Tracked dimensions per cycle

File: `research_state/<subject>/research_capital.yaml`

```yaml
cycle: <id>
date: <yyyy-mm-dd>
total_tokens_in:  <int>
total_tokens_out: <int>
total_cost_usd:   <float>     # estimated from profile pricing
allocation_by_channel:
  A: {tokens: 1200, cost: 0.04, proposals: 1, info_gain_total: 3}
  B: {tokens: 8000, cost: 0.20, proposals: 4, info_gain_total: 5}
  C: {tokens: 2000, cost: 0.05, proposals: 1, info_gain_total: 2}
  D: {tokens: 5000, cost: 0.10, proposals: 3, info_gain_total: 6}
  E: {tokens: 0,    cost: 0.00, proposals: 0, info_gain_total: 0}
allocation_by_agent:
  GPT-5.5/Theory_Builder:        {in: 800, out: 400, cost: 0.06}
  Grok/Alpha_Hunter:             {in: 600, out: 300, cost: 0.03}
  ...
efficiency:
  info_gain_per_1k_tokens: 0.0625
  cost_per_info_gain_point: 0.027
```

### Rebalance trigger

Each 5 cycles, Director evaluates `efficiency.info_gain_per_1k_tokens` per
channel (analytics-only projections). If any channel produces 0 info_gain
over 5 cycles AND consumes >10% of total tokens, Director RECOMMENDS
cutting that channel's allocation by half for the next 5 cycles. If the
channel produces info_gain in any of those 5, Director recommends restore.
The P6 control plane is the sole governance and persistence owner and
applies allocation changes.

This prevents the system from spinning indefinitely on a sterile channel
while allowing recovery.

### Agent efficiency table

File: `research_state/<subject>/agent_performance.json`

```json
{
  "schema_version": "1.0",
  "subject": "b1_v3",
  "as_of_cycle": "...",
  "agents": [
    {
      "name": "Alpha_Hunter",
      "profile": "grok43",
      "tokens_in_total": 24000,
      "tokens_out_total": 12000,
      "cost_total_usd": 1.84,
      "accepted_proposals": 8,
      "rejected_proposals": 3,
      "confirmed_results": 2,
      "disproven_results": 1,
      "info_gain_total": 9,
      "info_gain_per_1k_tokens": 0.25,
      "last_10_cycles_active": 7
    },
    ...
  ]
}
```

This is the input to "is this agent worth its model tier?" decisions --
analytics-only; the P6 control plane is the sole governance and persistence
owner and decides enforcement.

---

## 9. LLM routing — implementation plan

### Per-agent profile in config (v4.1 — DeepSeek-first allocation)

| Agent | Profile | Justification |
|---|---|---|
| `system_orchestrator` | deepseekv4 | Workhorse model; runs every cycle multiple times. Most token-intensive role. |
| `experiment_executor` | deepseekv4 | "DeepSeek: Executor" per allocation table |
| `constraint_geometry_auditor` | deepseekv4 | "DeepSeek: Architecture Analysis" |
| `regime_researcher` | deepseekv4 | "DeepSeek: Regime Analysis" |
| `parameter_researcher` | glm51 | "GLM-5.2: Parameter Research / Sweep Design" |
| `research_proposer` (legacy) | glm51 | "GLM-5.2: Proposal Generation" |
| `data_validator` | glm51 | Structured field/schema checks; lightweight |
| `factor_engineer` | doubao (Seed 2.0 Pro) | "Seed: Factor Engineering / Interaction Discovery" |
| `strategy_synthesizer` | kimi | "Kimi: Knowledge Synthesis / Research Closure" |
| `research_historian` | kimi | "Kimi: Historian" |
| `risk_controller` | minimax | "Minimax: Risk Review"; different family from executor (deepseek) |
| `code_reviewer` | minimax | "Minimax: Code Review"; different family from code generator (deepseek) |
| `statistician` | mimo | "Mimo: Statistician / Prediction Lock / Surprise Engine" |
| `data_expansion_researcher` | gemini35flash | "Gemini Flash: Data Expansion / Alternative Data / External Research" |
| `theory_builder` | gpt55 | "GPT-5.5: Theory Builder / High-level Reasoning" (kept rare) |
| `alpha_hunter` | grok43 | "Grok: Alpha Hunter / New Mechanism Discovery" |
| `falsification_officer` | grok43 | "Grok: Contrarian Ideas" |

### Token share targets

| Model | Share | Agents | Note |
|---|---:|---|---|
| deepseekv4 | 30-35% | orchestrator + executor + geometry_auditor + regime_researcher | Primary workhorse |
| glm51 | 18-22% | parameter_researcher + data_validator + research_proposer (legacy) | Sweep + proposal layer |
| doubao (Seed) | 10-12% | factor_engineer | Mass factor ideation |
| kimi | 10% | synthesizer + historian | Long-context synthesis |
| minimax | 10% | risk_controller + code_reviewer | Adversarial audit + code review |
| mimo | 8-10% | statistician | Prediction lock + surprise |
| gemini35flash | 2-5% | data_expansion_researcher | External data scout |
| gpt55 | 2-4% | theory_builder | Reserved for high-leverage causal reasoning |
| grok43 | 2-4% | alpha_hunter + falsification_officer | Contrarian / lateral thinking |

### Critical pairings (must be different model families)

- `experiment_executor` (deepseek) ≠ `risk_controller` (minimax) ✓
- `experiment_executor` (deepseek) ≠ `code_reviewer` (minimax) ✓ (Code Pipeline)
- `alpha_hunter` (grok) ≠ `theory_builder` (gpt55) ✓
- `parameter_researcher` (glm51) ≠ `statistician` (mimo) ✓ (proposer ≠ prediction locker)

### Known tradeoff in v4.1

`system_orchestrator` is on deepseekv4 rather than gpt55. The orchestrator
is the SOLE controller — every `control_decision` flows through it. DeepSeek
is competent for structured control work but lacks gpt55's depth on
escalation / ambiguous-state handling. If observed misjudgements
accumulate (visible in cycle logs), promote orchestrator to gpt55 and
accept that gpt55's token share will rise to ~10-15%.

### Implementation (two small Python edits)

#### Edit 1 — `ag2_research/config.py` (add one method)

```python
def get_agent_llm_config(self, agent_id: str) -> dict:
    """Return the AG2 llm_config for an agent template. Falls back to
    default profile if the template has no 'profile' field."""
    tpl = self.get_agent(agent_id) or {}
    profile = tpl.get("profile") or self.default_profile
    return self.get_llm_config(profile=profile)
```

#### Edit 2 — `ag2_research/agents.py::create_agents` (one branch)

Inside the existing loop, replace:

```python
agent = autogen.AssistantAgent(
    name=template["name"],
    system_message=system_message.strip(),
    llm_config=llm_config,
    code_execution_config=False,
)
```

with:

```python
# per-agent override if config supplies one; else inherit caller's llm_config
per_agent_llm = config.get_agent_llm_config(agent_id) if llm_config is None else llm_config
agent = autogen.AssistantAgent(
    name=template["name"],
    system_message=system_message.strip(),
    llm_config=per_agent_llm,
    code_execution_config=False,
)
```

(If caller passes `llm_config`, we still honour it — orchestrator paths
that pass explicit overrides keep working.)

---

## 10. Migration path (see AG2_V4_MIGRATION.md for full steps)

Summary:

1. **Step 1 (config only)**: Add 11 new agent templates to config.yaml +
   add `profile:` field to existing 6. Apply two Python edits. Restart;
   nothing else changes (new agents simply aren't called yet).

2. **Step 2 (state machine)**: Add
   `research_state/b1_v3/strategy_state.yaml`. Orchestrator reads it at
   cycle start. Default mode `discovery`. Channels A/B/C unused until
   Step 3 wires the new agents in.

3. **Step 3 (channel routing)**: Update System_Orchestrator
   system_message to route proposals through new channels per
   allocation. autonomous_runner adjusts its task source dispatch.

4. **Step 4 (open questions)**: Seed
   `research_state/b1_v3/open_questions.yaml` from Phase 14A's
   `ag2_research/knowledge_base/b1_v3/missing_universe_ranked.md`. Research_Historian starts maintaining it.

5. **Step 5 (surprise engine)**: Statistician emits `prediction_locked`
   field; cycle log gains `surprise` block. Anti-gaming enforced by
   orchestrator (writes are write-once for the prediction field).

6. **Step 6 (information gain + capital)**: Research_Historian writes
   info_gain_assessment per cycle. agent_performance.json begins
   populating. Director starts using these analytics-only signals to recommend
rebalancing; the P6 control plane is the sole governance and persistence
owner and applies the changes.

7. **Step 7 (code pipeline)**: Insert Code_Reviewer as a hard step in
   `ClaudePatchExecutor.apply()` for any code-mode proposal. Risk
   Controller stays on a different model.

Each step is independent and rollback-safe.

---

## Anti-patterns explicitly forbidden in v4.0

1. **Blind parameter scans** — `Parameter_Researcher` proposals without
   `expected_knowledge` are rejected by `kb_validate_proposal`.

2. **Re-running disproven hypotheses** — `kb_validate_proposal`'s
   `forbidden_hypotheses` block + Research_Historian's OQ registry
   double-check.

3. **Same-model adversarial pair** — agents.py refuses to instantiate
   `risk_controller` and `experiment_executor` with the same profile
   (assertion in `create_agents`; documented in migration step 1).

4. **Surprise gaming** — `prediction` field is write-once; Statistician
   is the only role allowed to write it.

5. **Endless Discovery without info_gain** — Hard Stop or
   Maintenance Mode kicks in after 3 cycles of zero info_gain.

6. **Information silos** — Research_Historian reads ALL cycle logs and
   writes consolidated `research_evolution.md` every N cycles.

---

## Open design questions deferred to v4.1

1. **How to fund Channel B (Factor Discovery) when feature universe is
   audited closed?** Phase 14A identified missing dimensions, but no
   formal pipeline to acquire data exists yet.

2. **Cross-strategy knowledge sharing.** If `b2_v1` is onboarded, should
   Theory_Builder's b1_v3 lessons inform b2_v1's exploring stage?
   v4.0 keeps subjects isolated; v4.1 may relax this.

3. **Roundtable scheduling.** Mini-roundtable per cycle is feasible.
   Full-roundtable is expensive; v4.0 leaves the trigger condition
   as a Director judgement call. v4.1 may codify a quarterly schedule.

4. **Code-mode coverage.** Phase 15 hardened code modification for
   B1V3Params constants. Other change_types (function bodies, new
   imports, new files) are still ad-hoc. v4.1 should expand the
   change_type vocabulary.
