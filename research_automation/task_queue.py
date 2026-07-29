"""task_queue.py -- experiment task queue (Phase 1).

A minimal, file-backed FIFO queue of pending experiment proposals plus a record
of in-flight / finished experiments. No external broker; JSON on disk so the loop
can survive a restart. Abstract enough to swap for Redis/SQS later.
"""
from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ExperimentTask:
    """A queued unit of work derived from an AG2 proposal."""

    task_id: str
    strategy: str
    proposal: dict  # {hypothesis, alpha_source, scope, success_criteria}
    source: str = "ag2"  # where the proposal came from
    priority: int = 100  # lower runs first


class QueuePersistenceError(RuntimeError):
    """Raised when persisted queue state cannot be trusted."""


class TaskQueue:
    """In-memory FIFO with optional JSON persistence.

    Persistence is opt-in (pass a path). The queue stores only proposals/tasks;
    full Experiment state is owned by the controller.
    """

    def __init__(self, persist_path: str | Path | None = None):
        self._q: deque[ExperimentTask] = deque()
        self._in_flight: dict[str, ExperimentTask] = {}
        self._done: list[str] = []
        self._failed: dict[str, str] = {}
        self.persist_path = Path(persist_path) if persist_path else None
        if self.persist_path and self.persist_path.exists():
            self._load()

    def enqueue(self, task: ExperimentTask) -> None:
        # priority insert (stable-ish): place before first lower-priority item
        inserted = False
        tmp = list(self._q)
        for i, t in enumerate(tmp):
            if task.priority < t.priority:
                tmp.insert(i, task)
                inserted = True
                break
        if not inserted:
            tmp.append(task)
        self._q = deque(tmp)
        self._save()

    def dequeue(self) -> ExperimentTask | None:
        if not self._q:
            return None
        task = self._q.popleft()
        self._in_flight[task.task_id] = task
        self._save()
        return task

    def mark_done(self, task_id: str) -> None:
        self._in_flight.pop(task_id, None)
        if task_id not in self._done:
            self._done.append(task_id)
        self._save()

    def mark_failed(self, task_id: str, error: str = "") -> None:
        self._in_flight.pop(task_id, None)
        self._failed[task_id] = str(error)
        self._save()

    def pending_count(self) -> int:
        return len(self._q)

    def __len__(self) -> int:
        return len(self._q)

    # ---- persistence ------------------------------------------------------
    def _save(self) -> None:
        if not self.persist_path:
            return
        self.persist_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "pending": [t.__dict__ for t in self._q],
            "in_flight": {k: v.__dict__ for k, v in self._in_flight.items()},
            "done": self._done,
            "failed": self._failed,
        }
        self.persist_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load(self) -> None:
        try:
            data = json.loads(self.persist_path.read_text(encoding="utf-8"))
        except Exception as exc:
            # Fail closed: treating corrupt durable state as an empty queue would
            # silently discard pending experiments.
            raise QueuePersistenceError(
                f"cannot load persisted task queue {self.persist_path}: {exc}"
            ) from exc
        if not isinstance(data, dict):
            raise QueuePersistenceError(
                f"persisted task queue {self.persist_path} must contain a JSON object"
            )
        pending = [ExperimentTask(**t) for t in data.get("pending", [])]
        # A persisted in-flight task has no live worker after process restart.
        # Put it back at the front so crashes cannot silently lose work.
        recovered = [ExperimentTask(**t) for t in data.get("in_flight", {}).values()]
        self._q = deque(recovered + pending)
        self._in_flight = {}
        self._done = data.get("done", [])
        self._failed = data.get("failed", {})
        if recovered:
            self._save()
