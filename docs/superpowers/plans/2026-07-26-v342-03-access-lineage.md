# V3.4.2 P3: Access, Lineage, and Taint

## Objective

Record every access or derivation capable of affecting a scientific conclusion or model context, without creating a third store or persisting raw data in the journal.

## Operational events

Migrate the fixed OperationalJournal to add access, derivation, and projection tables. Reuse the shared sequence, idempotency, actor/invocation, CAS, and crash semantics while preserving distinct domain state machines.

Dataset roles are `TRAIN`, `VALIDATION`, `FOLD_TEST`, `FINAL_HOLDOUT`, and `LIVE_FORWARD`. Access operations are `READ`, `MATERIALIZE`, `DERIVE`, `DISPLAY`, `CONSUME`, and `EXPORT`. Events contain bounded metadata, references, and hashes, never DataFrames, raw logs, labels, or secrets.

## Lineage and taint

Adapt the existing `lineage.py` ancestry/cycle/root rules to journal derivation edges. TaintGraph is a projection, not a second durable graph. Taint includes `CLEAN`, `TEST_LABEL`, `TEST_DERIVED`, `FINAL_HOLDOUT`, and `INVALID`; display or consumption propagates taint through prompt, memory, artifacts, and claims.

Each frozen candidate/protocol/fold receives one FOLD_TEST attempt. Consumption is recorded before read. Results cannot select or revise a threshold, model, or variant for the same fold.

Final Holdout remains unavailable in P3. Only P8 may create and consume its authority attempt.

## Gate

Test concurrent append and sequence, idempotent replay, invalid reopen, lineage cycles, test-derived metrics, prompt exposure, fold-test replay, corruption, projection rebuild, and physical separation from AuthorityStore. Freeze, inventory, reviewed policy, and the generic gate follow implementation.

