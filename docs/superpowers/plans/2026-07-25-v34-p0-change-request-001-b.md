# P0 Change Request 001-B — Control-Plane Cold-Start SLO

```yaml
change_request_id: P0-CR-001-B
parent_change_request_ids: [P0-CR-001, P0-CR-001-A]
status: PENDING_USER_APPROVAL
phase: P0
change_type: one_file_performance_allowlist_addendum
semantic_design_change: false
```

## Reason

The master plan defines a first-version control-path startup SLO of at most five seconds and requires optimization rather than silent scope reduction when the baseline exceeds it.

Three isolated cold imports of `research_automation.control_plane` took 7.262, 4.906, and 5.238 seconds. Python import-time profiling attributed approximately 4.85 seconds to `research_automation/__init__.py` eagerly importing `automation_controller -> ag2_research.orchestrator -> autogen`, before the lightweight control-plane package is available.

The parent package file was not listed in CR-001 or CR-001-A. It therefore cannot be modified without this exact allowlist addendum.

## Existing file added to the P0 modification allowlist

- `research_automation/__init__.py`

Current SHA-256:

```text
f0f866514d782ddebc04271a2c6206b279ae4d1c051b33624a15ea8f61ce8bdc
```

## Exact permitted change

Replace eager package-root imports with backward-compatible lazy exports. Importing `research_automation.control_plane` must not load AG2/autogen, start a network call, write a file, or initialize a research runner. Existing public names remain available on first access.

The already-approved `tests/test_import_side_effects.py` will verify:

- cold control-plane import does not load `autogen` or `ag2_research.orchestrator`;
- package-root public exports remain resolvable;
- three isolated cold-import samples meet the five-second SLO, with individual timings recorded rather than hidden behind an average;
- no import-time filesystem or network side effect occurs.

## Frozen boundaries

- No changes to AG2, research, strategy, signal, model, parameter, data, cache, KBase, Registry, Snapshot, Handoff, Memory, production configuration, scheduler, ACL, or Git behavior.
- This addendum permits one implementation file only; the test file is already approved by CR-001-A.
- Approval permits the path to enter the new P0 revision. It does not authorize implementation; a fresh `START_IMPLEMENTATION phase=P0` remains required after new hashes are published.

## Risk and rollback

Operational risk is compatibility of package-root imports. The test must enumerate and resolve every currently exported public name before the task passes. Rollback is a reverse patch of this one file only.

## Approval command

```text
APPROVE_CHANGE_REQUEST id=P0-CR-001-B
```
