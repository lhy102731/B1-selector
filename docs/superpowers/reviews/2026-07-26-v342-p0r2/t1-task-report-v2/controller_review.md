# Controller Review: TaskReport V2 Development Panel

## Result

Five requested profiles were invoked concurrently in attempt 001: four non-DeepSeek Volcengine profiles and the official DeepSeek profile. Attempt 001 produced one useful response, three billed empty responses, and one DeepSeek client serialization error. Retry 001 kept the exact providers/models, fixed recursive nested environment substitution, and increased reasoning allowance. It produced useful Minimax and official DeepSeek responses; GLM and Kimi were again billed but empty.

No response, error, or usage event was hidden. Known provider-reported usage is 25,788 tokens across both attempts; the failed first DeepSeek request has UNKNOWN usage and is not recorded as zero. GLM and Kimi stop after two empty attempts and are taken over by the primary Codex. No provider/model fallback occurred.

## Accepted findings

- A payload hash proves integrity only. Builder inputs exclude outcome/reason codes/hash, and validation mechanically re-derives outcome.
- PASS requires every frozen required receipt. Missing receipts BLOCK; failed receipts, unexpected changes, or unauthorized effects FAIL; IN_DOUBT remains the highest-precedence terminal uncertainty.
- P0R1 adoption uses exact raw-byte hashes and a separate adapter. V1 never enters the V2 validator.
- The future byte parser needs a hard input-size cap, duplicate-key rejection, and a deterministic nesting-depth cap.
- UNKNOWN usage remains null and budget-conservative.
- Changed-file truth must ultimately be derived from the bound baseline, not trusted from a worker list.

## Rejected or deferred findings

- No signature/RPC service is added. Mechanical derivation plus later AuthorityStore task-spec/receipt cross-check is sufficient for the current local threat model.
- Doubao's suggested `BLOCKED > FAIL > IN_DOUBT` ordering and treating UNKNOWN usage as automatically IN_DOUBT are rejected. The approved plan requires `IN_DOUBT > FAIL > BLOCKED > PASS`; UNKNOWN usage is allowed but keeps its budget reservation.
- No custom canonical JSON serializer, hash memoization, parallel hashing, or generic streaming parser is added. The report will be byte/depth bounded first and profiled later.
- No `changes_recorded` boolean is added. The trusted baseline-delta builder will own the file list.

## Required implementation sequence

1. Builder-owned mechanical outcome and reason codes.
2. Strict nested receipts, usage, paths, timestamps, changed-file semantics, and byte parser.
3. Exact-byte P0R1 adoption and ordered last-writer revalidation.
4. Gate-time AuthorityStore cross-check of task spec, receipt, actor/invocation, and identity hashes.
