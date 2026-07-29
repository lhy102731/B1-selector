"""proposer.py -- deterministic ParameterProposer (no LLM).

Reads search_space.yaml + (optionally) Snapshot.next_priority, and emits concrete
parameter-experiment proposals. This is the reliable, reproducible task source that
lets the autonomous runner work unattended without any API.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from ag2_research.orchestrator import MemoryRouter

_HERE = Path(__file__).resolve().parent


class ParameterProposer:
    def __init__(self, strategy: str = "b1", search_space_path: str | Path | None = None,
                 memory_router: MemoryRouter | None = None):
        self.strategy = strategy.lower()
        self.search_space_path = Path(search_space_path) if search_space_path else (_HERE / "search_space.yaml")
        self.router = memory_router or MemoryRouter(self.strategy)
        self._space = self._load_space()

    def _load_space(self) -> dict:
        with open(self.search_space_path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data.get(self.strategy, {})

    def _next_priority_text(self) -> str:
        try:
            snap = self.router.build_packet().get("snapshot", {}) or {}
            nxt = snap.get("next_priority") or {}
            return str(nxt).lower()
        except Exception:
            return ""

    def propose(self, max_experiments: int) -> list[dict]:
        """Return up to `max_experiments` concrete parameter proposals (OFAT or cartesian).
        Each candidate = champion baseline + ONE override, so a sweep isolates one variable."""
        scope = self._space.get("scope", {}) or {}
        grid = self._space.get("grid", {}) or {}
        champion = self._space.get("champion_params", {}) or {}
        mode = self._space.get("mode", "one_factor_at_a_time")

        # (param, value) pairs
        pairs: list[tuple[str, object]] = []
        if mode == "cartesian":
            import itertools
            keys = list(grid.keys())
            for combo in itertools.product(*[grid[k] for k in keys]):
                pairs.append(("+".join(keys), dict(zip(keys, combo))))
        else:  # one_factor_at_a_time
            npt = self._next_priority_text()
            ordered = sorted(grid.keys(), key=lambda k: (0 if k in npt else 1, k))
            for k in ordered:
                for v in grid[k]:
                    pairs.append((k, v))

        proposals = []
        for param, value in pairs[: max(0, max_experiments)]:
            if mode == "cartesian":
                override = dict(value)
            else:
                override = {param: value}
            params = {**champion, **override}        # champion baseline + one override
            desc = ", ".join(f"{k}={v}" for k, v in override.items())
            proposals.append({
                "hypothesis": f"{self.strategy.upper()} parameter sweep: {desc} (vs champion)",
                "alpha_source": f"{self.strategy.upper()} candidate ranking (parameter sweep)",
                "scope": {
                    "strategy": self.strategy.upper(),
                    "start": scope.get("start"),
                    "end": scope.get("end"),
                    "max_stocks": scope.get("max_stocks"),
                    "params": params,
                },
                "success_criteria": "account-level metrics vs champion baseline; bounded validation",
                "experiment_spec": {"override": override, "params": params},
                "source": "proposer",
            })
        return proposals
