# V3.4.2 Git Source Authority Transition

Status: `APPROVED_FOR_IMPLEMENTATION`

Date: 2026-07-29

Scope: source identity and release evidence only

## Decision

Git commit and tree identity are the authoritative source-code baseline for
normal development. The repository will no longer rebuild a whole-workspace,
per-file freeze after every ordinary change.

This transition changes the implementation of source identity. It does not
weaken or supersede the scientific, authorization, data-access, campaign, or
Final Holdout boundaries in the approved V3.4.2 plan. Existing P0 and P1
artifacts remain immutable historical evidence; they are not regenerated for
ordinary maintenance.

## Daily development contract

1. Adopt currently active source into Git in reviewed, exact-path batches.
2. Deliver one behavior at a time with targeted tests, then create one small
   Git commit for the verified logical stage.
3. Stage only named files. Broad staging, destructive cleanup, history
   rewriting, and incidental commits of user changes remain forbidden.
4. A formal control-plane release or campaign entry must resolve to a clean
   Git worktree and a recorded commit/tree identity.
5. Existing production scheduling is observed but not disabled or mutated
   during adoption. Any source in its transitive execution closure is treated
   as active and reviewed for Git adoption before a new formal gate relies on
   the tracked-source rule.

## Repository classifications

- Active production, database, operations, control-plane, and supporting test
  source is adopted into Git after review and secret scanning.
- Dormant or historical research source is not deleted merely because it is
  untracked. It remains outside formal release identity until explicitly
  reactivated and reviewed.
- Generated data, caches, model artifacts, reports, and runtime state are not
  adopted as source code.
- `.env`, Authority/Operational SQLite files, DPAPI capability material, and
  other credentials must never be staged or committed.

## Lightweight release manifest

A lightweight manifest is generated only at these boundaries:

1. P-phase closure;
2. formal Campaign launch;
3. model or production promotion;
4. Final Holdout authorization and access.

The manifest records the Git commit/tree, clean-worktree result, dependency
lock digest, and references/digests for the applicable data release, model,
external scheduler state, Authority/Operational state, and scientific
preregistration. It does not copy or hash every source file independently and
does not package the full repository.

## Gate migration acceptance behavior

The minimal compatible migration must prove through public behavior that:

- the gate accepts a clean repository whose expected commit/tree matches;
- it fails closed for dirty tracked source, untracked executable source in the
  formal execution scope, or an identity mismatch;
- external state and preregistration checks remain independently enforceable;
- historical P0 evidence remains readable but is not rebuilt;
- ordinary Git-tracked maintenance does not require a new P0 attempt or a
  whole-workspace freeze.

Tests use temporary Git repositories and must not run research, backtests,
data updates, cache rebuilds, LLM research calls, Campaigns, or Holdout access.

## Execution boundary

This approval covers source adoption and the minimal Git-identity migration,
followed by P1/T5-T6 verification. It does not authorize P2, a real Campaign,
production promotion, scheduler mutation, or Final Holdout access. Work stops
at the P1/P2 boundary unless the operator separately authorizes the next phase.
