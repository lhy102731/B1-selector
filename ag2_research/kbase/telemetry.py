"""Metadata-only local telemetry for demand-driven KBase maintenance."""
from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import json
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from .schemas import validate_usage_event


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_USAGE_ROOT = PROJECT_ROOT / "data" / "ag2_kbase" / "usage"


@contextlib.contextmanager
def _lock(path: Path, timeout: float = 5.0) -> Iterator[None]:
    deadline = time.monotonic() + timeout
    while True:
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            break
        except FileExistsError:
            try:
                if time.time() - path.stat().st_mtime > 60:
                    path.unlink()
                    continue
            except FileNotFoundError:
                continue
            if time.monotonic() >= deadline:
                raise TimeoutError(f"usage event lock timed out: {path}")
            time.sleep(0.05)
    try:
        yield
    finally:
        path.unlink(missing_ok=True)


def query_hash(query: str | None) -> str | None:
    if not query:
        return None
    return hashlib.sha256(str(query).encode("utf-8")).hexdigest()


def record_usage(event: dict[str, Any], *, usage_root: Path = DEFAULT_USAGE_ROOT) -> Path:
    validate_usage_event(event)
    usage_root.mkdir(parents=True, exist_ok=True)
    date = str(event["timestamp"])[:10]
    target = usage_root / f"{date}.jsonl"
    line = json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n"
    with _lock(usage_root / ".usage.lock"):
        with target.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
    return target


def new_event(
    *,
    event_type: str,
    tool: str,
    catalog_version: str | None,
    latency_ms: float,
    query: str | None = None,
    filters: dict[str, Any] | None = None,
    source_ids: list[str] | None = None,
    layer: str | None = None,
    result_count: int | None = None,
    outcome: str = "ok",
) -> dict[str, Any]:
    return {
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "event_type": event_type,
        "catalog_version": catalog_version,
        "tool": tool,
        "query_hash": query_hash(query),
        "filters": filters or {},
        "source_ids": source_ids or [],
        "layer": layer,
        "result_count": result_count,
        "latency_ms": round(max(0.0, float(latency_ms)), 3),
        "outcome": outcome,
    }


def aggregate_usage(*, usage_root: Path = DEFAULT_USAGE_ROOT) -> dict[str, Any]:
    events = []
    if usage_root.is_dir():
        for path in sorted(usage_root.glob("*.jsonl")):
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    event = json.loads(line)
                    validate_usage_event(event)
                    events.append(event)
                except (json.JSONDecodeError, ValueError):
                    continue
    source_counts = Counter(source_id for event in events for source_id in event["source_ids"])
    return {
        "event_count": len(events),
        "event_types": dict(Counter(event["event_type"] for event in events)),
        "outcomes": dict(Counter(event["outcome"] for event in events)),
        "top_source_ids": source_counts.most_common(50),
        "no_result_query_hashes": [event["query_hash"] for event in events if event["outcome"] == "no_result" and event["query_hash"]],
        "average_latency_ms": (
            sum(float(event["latency_ms"]) for event in events) / len(events) if events else 0.0
        ),
        "note": "Usage frequency indicates maintenance demand, not source correctness.",
    }
