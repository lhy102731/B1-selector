# CHANGELOG -- AG2 Structural Refactor (Research OS)

## v0.9 Patch (incremental, no rewrite)

Scope: only `config.yaml`, `ROLE_SYSTEM.md`, `CONTROL_LAYER_SPEC.yaml`,
`templates/snapshot_template.yaml`, `templates/handoff_template.yaml`. No strategy
logic, memory content, registry content, model config, backtest logic, or tool
definitions touched. Minimal, compatible, still runnable.

- **PATCH #1 Registry Gate centralization.** Removed registry lookup/classification
  from `Research_Proposer`; it now consumes a `registry_verdict` from the memory_packet
  and only drafts. `registry_gate.owner: system_orchestrator` added in `config.yaml` +
  `CONTROL_LAYER_SPEC.yaml`. Proposer tools reduced to `[]`.
- **PATCH #2 Memory Packet architecture.** Added `memory_packet` (owner: orchestrator)
  to `config.yaml` and `CONTROL_LAYER_SPEC.yaml` with the requested schema
  (`current_state, current_objective, registry_verdict, registry_status,
  active_constraints, allowed_actions, forbidden_actions, next_required_step`).
  Non-orchestrator roles read only the packet; memory-reading tools
  (`list_research_docs`, `read_research_doc`) now assigned ONLY to `system_orchestrator`.
- **PATCH #3 Round-robin -> sequential.** Workflows set to `type: sequential`,
  `speaker_selection: sequential`, `execution: one_pass_per_role`, `allow_repeat_speaker:
  false`, with `pipeline_order`. `brainstorm.max_rounds` cut 30 -> 8 to bound one pass.
  Runtime note: `orchestrator.py` still sets the AG2 speaker method itself; the bounded
  `max_rounds` is the safeguard until that code reads `speaker_selection` from config.
- **PATCH #4 Taxonomy unification.** Single vocabulary everywhere:
  `duplicate | partial_overlap | failed | verified | open | none`. Fixed
  `failed_match`/`verified_match`/`no_match` in `config.yaml` and `no_match` in the
  handoff template; added `open`. Verified identical across config / spec / handoff.
- **PATCH #5 Solo safety.** Plan B (minimal): `solo` now runs
  `[research_proposer, system_orchestrator-first via pipeline_order]`, marked
  `unsafe_for_production: true`. No longer bypasses control.
- **PATCH #6 Control-layer dedup.** Removed the repeated OPERATING CONTRACT block from
  the five non-orchestrator role prompts; each keeps only responsibility + INPUT +
  OUTPUT + role-specific MUST-NOTs and "follow the control packet". Full global contract
  retained only in `system_orchestrator` and `CONTROL_LAYER_SPEC.yaml`.
- **PATCH #7 Loop termination.** `max_revision_attempts: 2` added to `config.yaml`
  (`revision_limit`) and `CONTROL_LAYER_SPEC.yaml` (`pipeline.revision_limit`); on exceed
  -> `REJECT` or `ESCALATE_TO_USER`. `ESCALATE_TO_USER` added to the orchestrator's
  decision enum. `COMMITTED -> SNAPSHOT_READ` marked as a next-cycle, non-auto-loop transition.
- **PATCH #8 Data vs Risk boundary.** `Data_Validator` is sole owner of leakage /
  feature availability / production availability / data consistency. `Risk_Controller`
  output reworked to `execution_risk / robustness_risk / regime_risk / deployment_risk`
  and explicitly forbidden from re-judging leakage/availability. New `data_risk_boundary`
  section in `CONTROL_LAYER_SPEC.yaml`.

Verification (v0.9): all four YAML files pass `safe_load`; `config.yaml` loads via
`ResearchConfig`; all role tool refs resolve; only `system_orchestrator` holds memory
tools; the unified taxonomy is identical across config/spec/handoff; each workflow's
initiator resolves to `System_Orchestrator` (except diagnostic `solo`).

---

## v0.8 Structural Refactor

## Roles -- overlap eliminated (6 -> 6, single responsibility each)
- REMOVED overlapping roles: `Alpha_Researcher`, `Data_Analyst`, `Backtest_Engineer`,
  `Risk_Manager`, `Strategy_Architect` (three judged strategy; two both validated).
- ADDED single-responsibility roles, each with explicit INPUT/OUTPUT:
  `Research_Proposer`, `Data_Validator`, `Experiment_Executor`, `Risk_Controller`,
  `Strategy_Synthesizer`.
- UPGRADED `Coordinator` -> `System_Orchestrator`: the sole controller with
  exclusive scheduling, gating, and commit authority (the missing final-decision role).
- Risk no longer executes backtests (kills the old Risk/Backtest overlap); Executor
  returns raw metrics only; Synthesizer integrates only.

## Control layer -- added
- NEW `ag2_research/CONTROL_LAYER_SPEC.yaml` (canonical) + advisory mirror in `config.yaml`:
  authority, memory_priority, registry_gate, preflight_gate, forbidden_behaviors,
  pipeline state machine, success_criteria.
- Operating contract embedded in every role `system_message`, so rules are enforced
  at runtime in any workflow.

## Memory priority -- fixed ordering
- Enforced: Snapshot (1) > Handoff (2) > Registry (3) > Research Memory (4) > Code (5).
- Snapshot is authoritative current state; Code is last resort; no full-codebase first scan.

## Registry experiment gate -- added
- No proposal may exist without a Registry check.
- duplicate=REJECT, partial_overlap=MODIFY, failed=BLOCK unless reopen_condition,
  verified=reproduce-only, no_match=require novelty_justification.

## Workflows -- rewired (runtime-compatible)
- IDs `brainstorm` / `review` / `solo` kept so `orchestrator.py` keeps working; agent
  lists now point to the new roles, with `system_orchestrator` placed LAST so the
  `agents.get("Coordinator") or list(agents.values())[-1]` fallback selects it.
- ADDED `proposal_gate` workflow (proposal + registry + data validation only).

## Templates -- updated
- NEW `templates/snapshot_template.yaml` and `templates/handoff_template.yaml`:
  strategy-agnostic, with a non-removable `control:` block (memory_priority,
  registry_gate, forbidden_behaviors) wired to `CONTROL_LAYER_SPEC.yaml`.

## Preserved (unchanged)
- All 9 LLM profiles and the `roundtable` block in `config.yaml`.
- `config.py`, `agents.py`, `orchestrator.py`, `tools.py` (no code edits required).
- All B1/B3/Brick strategy logic, factors, and existing memory files.

## Verification
- `config.yaml` loads via `ResearchConfig`; all 6 role tool refs resolve in `TOOL_REGISTRY`;
  all workflow agent ids resolve; orchestrator fallback resolves to `System_Orchestrator`.
- All four new/modified YAML files pass `yaml.safe_load`.

## Follow-up (optional, not done -- would require Python edits beyond this refactor's scope)
- `orchestrator.run_review` prompt text still names legacy `Risk_Manager` /
  `Strategy_Architect` / `Coordinator` as plain instruction strings. It remains
  runnable; update those strings to the new role names for full coherence.
