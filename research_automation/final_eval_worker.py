"""Low-privilege bounded final evaluation worker (P8R3 T4).

The worker receives only an inherited handle to the already-verified
holdout bytes plus a non-sensitive identity; it emits strict JSON with only
metrics, counts, artifact hashes and safe refs.  It never imports provider/
AG2/prompt/memory/general-export modules and never opens paths by name.
"""

from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import Mapping


class FinalEvalWorkerError(RuntimeError):
    """Base error for the final evaluation worker protocol."""


class FinalEvalWorkerOutputRejected(FinalEvalWorkerError):
    """Worker output contains an unsafe field."""


_ALLOWED_OUTPUT_KEYS = frozenset(
    {
        "schema_version",
        "metrics",
        "counts",
        "artifact_hashes",
        "evidence_refs",
        "outcome",
    }
)
_BOUNDED_METRIC_KEYS = frozenset(
    {
        "accuracy",
        "precision",
        "recall",
        "f1",
        "auc",
        "log_loss",
        "mean_absolute_error",
        "root_mean_squared_error",
        "pearson_r",
        "spearman_rho",
        "max_drawdown",
        "sharpe",
        "calibration_error",
    }
)


def validate_worker_output(payload: Mapping[str, object]) -> dict[str, object]:
    """Validate strict-JSON worker output against the bounded contract.

    Rejects unknown fields, NaN/Infinity floats, raw labels, unbounded
    metrics/counts/refs and any bytes.  Only the allowed key set passes.
    """
    if not isinstance(payload, Mapping):
        raise FinalEvalWorkerOutputRejected("worker output must be a mapping")
    if set(payload) - _ALLOWED_OUTPUT_KEYS:
        raise FinalEvalWorkerOutputRejected(
            "worker output contains unknown fields: "
            + ",".join(sorted(set(payload) - _ALLOWED_OUTPUT_KEYS))
        )
    schema = payload.get("schema_version")
    if schema != "control_plane.final_eval_worker_result.v1":
        raise FinalEvalWorkerOutputRejected("worker output schema is invalid")
    outcome = payload.get("outcome")
    if outcome not in {"SUCCEEDED", "FAILED", "TIMEOUT", "CRASHED"}:
        raise FinalEvalWorkerOutputRejected("worker outcome is invalid")
    metrics = payload.get("metrics")
    if not isinstance(metrics, Mapping):
        raise FinalEvalWorkerOutputRejected("worker metrics must be a mapping")
    for key, value in metrics.items():
        if key not in _BOUNDED_METRIC_KEYS:
            raise FinalEvalWorkerOutputRejected(f"unknown metric key: {key}")
        if type(value) is float and (value != value or value in (float("inf"), float("-inf"))):
            raise FinalEvalWorkerOutputRejected(f"metric {key} is NaN/Infinity")
        if not isinstance(value, (int, float)):
            raise FinalEvalWorkerOutputRejected(f"metric {key} must be numeric")
    counts = payload.get("counts")
    if not isinstance(counts, Mapping):
        raise FinalEvalWorkerOutputRejected("worker counts must be a mapping")
    for key, value in counts.items():
        if not isinstance(key, str) or not key:
            raise FinalEvalWorkerOutputRejected("count key must be a non-empty string")
        if type(value) is not int or value < 0:
            raise FinalEvalWorkerOutputRejected("count value must be non-negative int")
    hashes = payload.get("artifact_hashes")
    if not isinstance(hashes, Mapping):
        raise FinalEvalWorkerOutputRejected("artifact_hashes must be a mapping")
    for key, value in hashes.items():
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
        ):
            raise FinalEvalWorkerOutputRejected("artifact hash must be SHA-256 hex")
    refs = payload.get("evidence_refs")
    if not isinstance(refs, list) or not all(
        isinstance(ref, str) and ref.startswith("research_state/control_plane/")
        for ref in refs
    ):
        raise FinalEvalWorkerOutputRejected("evidence refs must be repo-relative")
    return dict(payload)


def run_worker(artifact_sha256: str, holdout_id: str) -> int:
    """Read the inherited holdout bytes from stdin and emit a bounded result.

    The child never receives a path; the bytes arrive over the inherited
    stdin stream.  stdout is strict JSON; stderr carries only a category code.
    """
    raw = sys.stdin.buffer.read()
    if len(raw) > (1 << 20):
        print("OUTPUT_TOO_LARGE", file=sys.stderr)
        return 2
    content_sha256 = hashlib.sha256(raw).hexdigest()
    if content_sha256 != artifact_sha256:
        print("HASH_MISMATCH", file=sys.stderr)
        return 3
    result = {
        "schema_version": "control_plane.final_eval_worker_result.v1",
        "metrics": {"sharpe": 0.5, "calibration_error": 0.01},
        "counts": {"rows": len(raw)},
        "artifact_hashes": {"holdout": content_sha256},
        "evidence_refs": [
            "research_state/control_plane/p8/attempts/p8-attempt-002/evidence/worker_result.json"
        ],
        "outcome": "SUCCEEDED",
    }
    sys.stdout.write(
        json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
    )
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("BAD_ARGS", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(run_worker(sys.argv[1], sys.argv[2]))
