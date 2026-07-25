# V3.4.2 P4: Safe Mutation, Evidence, and Learning Commit

## Mutation transaction

Deepen existing workspace and patch code into one trusted mutation transaction. `GitDiffAdapter` performs `git apply --check` then apply without `--unsafe-path`. A structured AST adapter may retain deterministic constant changes. Both share canonical path containment, file allowlists, before/after hashes, compile/selected-test gates, and actor audit.

Every mutation occurs in a disposable isolated workspace. Failure rolls back by discarding it, not by guessing original file text. Remove trusted reliance on the custom hunk fallback once deletion tests prove it is legacy-only.

## Runner artifacts and evidence

Each known Runner receives one strict versioned adapter. Loose stdout/Markdown/CSV parsing produces only `legacy_unaudited` raw evidence.

EvidenceAdapter independently verifies artifact hash/schema, label, exit, horizon, date coverage, data roles, rolling folds, purge/embargo, feature set, model artifact, threshold, generation, runner/code, rows/missingness, access/taint, and executed-versus-approved protocol. Runner-reported PASS or promotion booleans are ignored.

Evidence fields remain orthogonal: protocol conformance, evidence grade, scientific outcome, promotion eligibility, commit eligibility, and invalidation codes. `NO_MATERIAL_FINDING` is a round outcome rather than an empty claim.

## Learning Commit

A Learning Packet may represent POSITIVE, NEGATIVE, PARTIAL, anti-factor, or failed usage. It carries scope, evidence references, audit grade, taint, parent lineage, reopen predicate, and future-usage guidance.

Write packet bytes by content hash with exclusive create and durability, then append an idempotent commit event. A crash between them leaves an orphan that does not enter Ledger; reconciliation reports or safely adopts it without double count. Ledger projects incrementally by sequence.

Historical 2026-07-23/24 results may receive hash-bound audit addenda, never rewritten claims, unseen/OOS promotion language, or bulk ingestion.

## Gate

Test production byte identity, traversal/unsafe paths, compile and selected-test failure, workspace discard, unknown Runner/schema, semantic mismatch, tainted metrics, false PASS, packet tamper, crash windows, duplicate commit, projection rebuild, negative/PARTIAL scope, and raw-log exclusion. Then freeze, inventory, review policy, and close with the generic gate.

