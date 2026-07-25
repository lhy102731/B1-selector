# V3.4.2 P8: Trusted Final Evaluator and Irreversible Closure

## Entry boundary

P8 implementation tests never read the real Final Holdout. A real evaluation requires a separately authorized one-time attempt after the Campaign and candidate set are frozen and iterative research has stopped.

FinalEvalRequest binds campaign, candidate set, code, ExecutionSpec, features, model, threshold, roster, generation, holdout, actor, and all identity hashes.

AuthorityBroker consumes the attempt permanently before returning any holdout handle. Success, failure, timeout, and crash all consume it; retry needs a genuinely new operator decision and cannot reuse the dataset for the same plan.

TrustedEvaluator uses a separate low-privilege data-root adapter. Research Runners, AG2, prompts, memory, and general export code never receive raw paths, labels, or reconstructable holdout bytes.

The result contains only bounded structured metrics and evidence references. It cannot feed proposal selection, thresholds, model changes, or memory for the same research plan. Both success and failure append a terminal audit event and close the Campaign. Subsequent cycle requests are rejected.

Live-forward evaluation is a new non-promoted run and remains separate. Production promotion always requires manual approval.

## Gate

Test unfrozen candidates, wrong hashes/actors, path traversal and reparse escape, nonce replay, crash-after-consume, failed-attempt consumption, closed-campaign resume, prompt/LLM access denial, audit export redaction, and manual-only promotion. The implementation gate must positively demonstrate that real Final Holdout bytes were not opened.

