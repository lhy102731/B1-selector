# P0 Change Request 001-A — Exact Allowlist Completion

```yaml
change_request_id: P0-CR-001-A
parent_change_request_id: P0-CR-001
status: PENDING_USER_APPROVAL
phase: P0
change_type: exact_file_allowlist_addendum
semantic_design_change: false
```

## Reason

The approved CR-001 requires complete import-active inventory, deep sink closure, a machine-computed P0 gate, and executable quarantine. The concurrent external panel and repository-level review confirmed that CR-001's explicit file list omitted a small number of concrete files needed to satisfy those already-approved requirements.

The master prompt requires every added or modified file to be approved explicitly. Therefore these files are not silently inserted into the P0 revision.

## Existing files added to the P0 modification allowlist

- `research_automation/evolution_loop.py` — additional automated-loop entry.
- `ag2_research/discovery_handoff.py` — direct discovery/handoff persistence seam.
- `build_daily_ret_cache.py` — import currently performs cache work; add a mechanical `main()` boundary only.
- `tools/diagnostics/test_llm_connectivity.py` — import currently calls configured LLM profiles; add a mechanical `main()` boundary only.
- `tools/data/fetch_active_cap.py` — import currently creates a production data directory; add a mechanical `main()` boundary only.

Current SHA-256 identities:

```text
research_automation/evolution_loop.py      a6fb936e9224711def6ccc6404546912d45c8bf81936fc92133984de48a14583
ag2_research/discovery_handoff.py          9ed97efa9c88326c9793bb88e63db576a7ebb3d42602dd6f627eb473b6a0a364
build_daily_ret_cache.py                   3c20b32874873dc7108842dccc3cf94843ea6ec0b7e279bebc27d653728f3938
tools/diagnostics/test_llm_connectivity.py b1dcc277176610d3b08fbbc442ea5d8b661974029cb1959f035f4171726d43d3
tools/data/fetch_active_cap.py             91796e25ee97ce1b61c7d3c2e3ee257a4ade0efb6873f3eb270872285ffb593f
```

## New files added to the P0 creation allowlist

- `research_automation/control_plane/entry_policy.json` — reviewed, hash-bound entry dispositions; scanner output cannot approve itself.
- `research_automation/control_plane/p0_gate.py` — deterministic P0 gate validator/writer.
- `tests/test_control_plane_p0_gate.py` — gate schema, self-hash, evidence, open-ticket, concurrency, and stale-attempt tests.
- `tests/test_web_entry_guards.py` — unauthorized config write, LLM thread, public debug binding, and context containment tests.
- `tests/test_import_side_effects.py` — import performs no write, network, cache build, or directory creation.
- `tests/test_legacy_campaign_guards.py` — backlog, autorun, evolution loop, executor, repair, and direct sink bypass tests.

## Frozen boundaries

- No P1 types or Protocol Compiler work.
- No strategy, signal, model, parameter, research date, roster, validation design, production config, data, cache, KBase, Registry, Snapshot, Handoff, Memory, or Git content change.
- The three import-active files receive only import-safety/main-guard refactoring; their algorithms and CLI behavior remain unchanged.
- Windows Task Scheduler and ACL mutation remain unauthorized.
- Approval only permits these paths to appear in the immutable P0 revision. Implementation still requires a fresh P0 authorization against the new hashes.

## Risk and rollback

Scientific risk is none because no research runs. Operational risk is limited to import behavior becoming side-effect free; explicit CLI execution remains available subject to its existing semantics and later P0 classification.

Rollback uses per-file reverse patches only. No existing file is deleted, moved, reset, checked out, staged, committed, or pushed.

## Approval command

```text
APPROVE_CHANGE_REQUEST id=P0-CR-001-A
```
