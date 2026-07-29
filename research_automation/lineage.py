"""lineage.py — Experiment Lineage System.

Tracks parent-child relationships between experiments so the full evolutionary
tree is traceable: which experiment a champion descends from, what was changed,
and which branch each experiment belongs to. Future Champion Pool reads the
lineage_tree.json produced here.

Rules:
- Root experiments have parent_experiment_id = None (generation = 0).
- Children auto-compute generation = parent.generation + 1.
- If workspace/code_change.json exists, its symbol/old/new become change_summary.
- lineage.json     → per-experiment (in experiment output dir)
- lineage_tree.json → per-cycle (in cycle dir, aggregates all candidates)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .safety import assert_safe_path


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_entry(entry: dict | object, key: str, default=None):
    if isinstance(entry, dict):
        return entry.get(key, default)
    return getattr(entry, key, default)


# ============================================================
# Per-experiment lineage.json
# ============================================================

def write_lineage_json(experiment, out_dir: Path, candidate_pool: list | None = None) -> Path:
    """Write ``lineage.json`` into the experiment's output directory.

    *experiment* can be an Experiment dataclass or a dict (from candidate_pool entries).
    *candidate_pool* is the full list of prior experiments (for generation + ancestry).
    """
    eid = _read_entry(experiment, "experiment_id")
    parent_id = _read_entry(experiment, "parent_experiment_id")
    generation, root_id = _compute_generation_and_root(parent_id, candidate_pool)

    # code_change summary if applicable
    change_summary = _extract_change_summary(experiment)

    lineage = {
        "experiment_id": eid,
        "parent_experiment_id": parent_id,
        "root_id": root_id or eid,
        "generation": generation,
        "created_at": _read_entry(experiment, "added_at", _now_iso()),
        "proposal_type": _read_entry(experiment, "strategy", "unknown"),
        "source": _read_entry(experiment, "source", "unknown"),
        "change_summary": change_summary,
    }
    path = assert_safe_path(Path(out_dir) / "lineage.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(lineage, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _compute_generation_and_root(parent_id, candidate_pool):
    """Walk parent chain to compute generation and root_id.

    Returns (generation, root_id). Root experiments have generation=0.
    A child with parent=root has generation=1.
    """
    if not parent_id or not candidate_pool:
        return 0, None
    gen = 1  # has a parent -> at least generation 1
    current = parent_id
    root = parent_id
    pool_map = {_read_entry(e, "experiment_id"): e for e in candidate_pool}
    visited = set()
    while current and current not in visited and gen < 1000:
        visited.add(current)
        root = current
        entry = pool_map.get(current)
        if entry is None:
            break
        current = _read_entry(entry, "parent_experiment_id")
        if current:
            gen += 1
    return gen, root


def _extract_change_summary(experiment) -> str:
    """Try to build a change summary from code_change.json in the workspace.
    Falls back to the experiment's hypothesis text."""
    ws = _read_workspace_path(experiment)
    if ws:
        cc_path = Path(ws) / "code_change.json"
        if cc_path.exists():
            try:
                cc = json.loads(cc_path.read_text(encoding="utf-8"))
                symbol = cc.get("symbol", "?")
                old = cc.get("old_value", "?")
                new = cc.get("new_value", "?")
                return f"modify_constant: {symbol} {old} -> {new}"
            except Exception:
                pass
    # fallback: use hypothesis
    hyp = _read_entry(experiment, "hypothesis", "")
    if not hyp:
        prop = _read_entry(experiment, "proposal", None)
        if prop:
            hyp = _read_entry(prop, "hypothesis", "")
    return hyp or "(no summary)"


def _read_workspace_path(experiment) -> str | None:
    """Best-effort workspace path: experiment.report_path -> parent dir -> workspace."""
    rp = _read_entry(experiment, "report_path")
    if rp:
        ws = Path(rp).parent / "workspace"
        if ws.is_dir():
            return str(ws)
    return None


# ============================================================
# Per-cycle lineage tree
# ============================================================

def build_lineage_tree(candidates: list, out_path: Path) -> Path:
    """Build ``lineage_tree.json`` from all candidate entries in a cycle.

    Each candidate must have at least ``experiment_id`` and ``parent_experiment_id``.
    """
    nodes = []
    root = None
    for c in candidates:
        eid = _read_entry(c, "experiment_id")
        parent = _read_entry(c, "parent_experiment_id")
        nodes.append({"id": eid, "parent": parent})
        if parent is None:
            root = eid
    tree = {"root": root or (nodes[0]["id"] if nodes else None), "nodes": nodes}
    path = assert_safe_path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(tree, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ============================================================
# Query interface (for future Champion Pool / AG2)
# ============================================================

def get_experiment_lineage(experiment_id: str, candidate_pool: list) -> list[str]:
    """Return the full ancestry chain from root -> ... -> experiment_id.

    Root is first in the returned list, experiment_id last.
    """
    pool_map = {_read_entry(e, "experiment_id"): e for e in candidate_pool}
    chain = []
    current = experiment_id
    visited = set()
    while current and current not in visited and len(chain) < 1000:
        visited.add(current)
        chain.append(current)
        entry = pool_map.get(current)
        if entry is None:
            break
        current = _read_entry(entry, "parent_experiment_id")
    chain.reverse()
    return chain
