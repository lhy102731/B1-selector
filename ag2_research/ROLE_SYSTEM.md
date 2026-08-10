# AG2 Role System (Rewritten)

Refactor target: each role has **exactly one primary responsibility**, an explicit
**INPUT** and **OUTPUT**, and **no overlap** with any other role. The old Coordinator
is upgraded into the sole controller, `System_Orchestrator`.

> **v0.9 patch note.** The Registry Gate is now executed **only** by `System_Orchestrator`
> (PATCH #1). All non-orchestrator roles consume a single **memory_packet** instead of
> reading Snapshot/Handoff/Registry themselves (PATCH #2). The pipeline is **sequential
> single-pass** with bounded revisions (PATCH #3, #7). Data vs risk concerns are disjoint
> (PATCH #8). See `CONTROL_LAYER_SPEC.yaml` for the canonical rules.
>
> **P6 owner split.** The P6 control plane is the sole governance and persistence owner.
> AG2 System_Orchestrator's "commit" rights are AG2-internal: it emits commit-request
> drafts; it never persists Snapshot/Handoff/Registry itself. Capital Tracker / Coverage
> Map / Agent Performance projections are analytics-only and are never written by AG2 roles.

---

## 1. Why the old role system was broken

| Old role | Problem |
|---|---|
| `Alpha_Researcher` | Made strategy judgments (overlap with Architect/Analyst) |
| `Data_Analyst` | Also judged strategy quality, not just data |
| `Backtest_Engineer` | Ran + interpreted results (overlap with Risk_Manager) |
| `Risk_Manager` | Also ran backtests and validated (overlap with Backtest_Engineer) |
| `Strategy_Architect` | Made strategy judgments (overlap with Alpha_Researcher) |
| `Coordinator` | Neither a controller nor a judge; no final decision power |

Three groups did the same "judge the strategy" work (Alpha / Analyst / Architect),
two roles both did "validation" (Risk_Manager / Backtest_Engineer), and **no role
held final decision authority**.

---

## 2. The new role system (6 roles, single responsibility each)

| New role | id | ONE responsibility | Acts only when |
|---|---|---|---|
| Research Proposer | `research_proposer` | Emit ONE registry-clean, in-boundary proposal | Start of cycle |
| Data Validator | `data_validator` | Confirm fields are production-available, no leakage | After a proposal |
| Experiment Executor | `experiment_executor` | Run the approved test exactly, return raw metrics | After data PASS + approval |
| Risk Controller | `risk_controller` | Adversarially audit the result, emit ONE verdict | After execution |
| Strategy Synthesizer | `strategy_synthesizer` | Convert a cleared result into memory deltas | After a VALID/bounded verdict |
| System Orchestrator | `system_orchestrator` | SOLE control: sequence, gate, approve, commit | Every gate + commit |

### 2.1 INPUT / OUTPUT contract

**Research_Proposer**
- INPUT: `memory_packet` (from System_Orchestrator) — includes the `registry_verdict` and `registry_status`
- OUTPUT: `proposal{ hypothesis, alpha_source, scope, consumes_registry_verdict, novelty_justification, success_criteria }`
- MUST NOT: perform Registry lookup/classification (Orchestrator owns the gate — PATCH #1), run backtests, validate data, audit results, decide acceptance.

**Data_Validator**
- INPUT: `memory_packet` + `proposal`
- OUTPUT: `data_verdict{ fields_required, production_available, leakage_risk, data_consistency, verdict(PASS/FAIL) }`
- SOLE owner of: leakage, feature availability, production availability, data consistency (PATCH #8).
- MUST NOT: judge alpha quality, run backtests, interpret performance, or read memory sources directly.

**Experiment_Executor**
- INPUT: `memory_packet` + data-validated proposal + explicit orchestrator approval
- OUTPUT: `execution_record{ command, config, date_range, metrics, output_files, sanity_check, anomaly_flag }`
- MUST NOT: propose, decide accept/reject, interpret. Reports raw numbers only.

**Risk_Controller**
- INPUT: `memory_packet` + `execution_record` + `data_verdict` + proposal
- OUTPUT: `risk_verdict{ execution_risk, robustness_risk, regime_risk, deployment_risk, baseline_comparison, escalation_triggered, verdict(VALID/INVALID/INCONCLUSIVE) }`
- MUST NOT: re-judge leakage or feature/production availability (Data_Validator owns those — PATCH #8); re-run backtests; propose; or commit.

**Strategy_Synthesizer**
- INPUT: `memory_packet` + a cleared result + its `risk_verdict`
- OUTPUT: `synthesis{ registry_entry_delta, snapshot_delta, handoff_delta, recommended_next_priority }`
- MUST NOT: schedule next work, run tests, or write files; or read memory sources directly. Produces drafts only.

**System_Orchestrator** (upgraded Coordinator)
- INPUT: Snapshot/Handoff/Registry/Research Memory (it is the ONLY reader) + all role outputs
- STEP 0: reads memory once per cycle, runs the **Registry Gate**, emits the **memory_packet** (with `registry_verdict`) to the next role.
- OUTPUT: `control_decision{ current_state, stage, gate_results, revision_attempts, decision, approved_next_role, committed_deltas, reason }`
- EXCLUSIVE rights (AG2-internal): read memory, perform Registry Gate, advance stages, approve/deny gates, and emit commit-request drafts for Snapshot/Handoff/Registry. Persistence belongs to the P6 control plane: The P6 control plane is the sole governance and persistence owner. Capital Tracker / Coverage Map / Agent Performance projections are analytics-only and are never written by AG2 roles.

---

## 3. Old -> New mapping (how overlap was removed)

```
Alpha_Researcher  ─┐
Strategy_Architect ─┼─> split cleanly into:  Research_Proposer (propose only)
Data_Analyst (alpha part) ┘                  Strategy_Synthesizer (integrate only)

Data_Analyst (data part) ──────────────────> Data_Validator (availability/leakage only)

Backtest_Engineer ─────────────────────────> Experiment_Executor (run only, raw output)

Risk_Manager ──────────────────────────────> Risk_Controller (audit only, NO execution)

Coordinator ───────────────────────────────> System_Orchestrator (sole controller + judge + committer)
```

Key overlap kills:
- "Judge the strategy" was done by 3 roles -> now **Proposer proposes**, **Synthesizer integrates**; distinct stages.
- "Validation" was done by 2 roles -> now **Executor runs (raw)**, **Risk_Controller audits (no run)**, **Data_Validator checks data**. Three disjoint concerns.
- "Control" had no owner -> now **System_Orchestrator** is the single authority with final decision power.
- **Registry classification** was done by both Proposer and Orchestrator -> now **only the Orchestrator** runs the Registry Gate; the Proposer consumes its verdict (PATCH #1).
- **Memory reads** were spread across roles -> now **only the Orchestrator** reads memory and distributes a `memory_packet` (PATCH #2).
- **Leakage/availability** were judged by both Data and Risk -> now **Data_Validator is sole owner**; Risk only covers execution/robustness/regime/deployment (PATCH #8).

---

## 4. The gated pipeline (sequential single-pass)

```
   System_Orchestrator STEP 0: reads memory ONCE -> Registry Gate -> emits memory_packet
        │
        ▼
   Research_Proposer ─►[registry_gate]─► Data_Validator ─►[data_gate]─►
        │                                                              │
   [preflight_gate] ─► Experiment_Executor ─► Risk_Controller ─►[risk_gate]─►
                                                              │
                          Strategy_Synthesizer ─► System_Orchestrator [COMMIT]
```

- Each role runs **once per cycle**; there is no round-robin free discussion (PATCH #3).
  `max_rounds` is bounded in `config.yaml` so the system can never degrade into a group chat.
- All non-orchestrator roles read **only the memory_packet**; `System_Orchestrator` is the
  sole reader of Snapshot/Handoff/Registry/Research Memory (PATCH #2).
- Each `[gate]` is enforced by `System_Orchestrator` per `CONTROL_LAYER_SPEC.yaml`.
- `anomaly_flag` at execution short-circuits to `STOP_AND_VERIFY` from any state.
- **Loop termination (PATCH #7):** at most `max_revision_attempts = 2` REJECT→MODIFY
  cycles per proposal; on the third, the Orchestrator must `REJECT` or `ESCALATE_TO_USER`.
  A `risk_gate` failure either loops back (within the limit) or is committed as a recorded
  FAILED experiment so it is never silently retried later.

---

## 5. Runtime binding

The 6 roles are defined in `ag2_research/config.yaml` under `agents:`. Workflow ids
`brainstorm` / `review` / `solo` are preserved so `orchestrator.py` keeps running;
their agent lists reference the new roles, with `system_orchestrator` placed last
so the existing `agents.get("Coordinator") or list(agents.values())[-1]` fallback
selects it as the manager/initiator (it runs Step 0). Workflows are now `sequential`
with bounded `max_rounds`, so no workflow can degrade into an unbounded group chat.

Global control rules are **no longer duplicated** into every role prompt (PATCH #6):
they live only in `CONTROL_LAYER_SPEC.yaml` and the `System_Orchestrator` prompt. Each
other role keeps just its responsibility, INPUT, OUTPUT, and role-specific MUST-NOTs,
and is told to "follow the control packet provided by System_Orchestrator". Memory-reading
tools are assigned only to `system_orchestrator`, enforcing the memory_packet routing.
