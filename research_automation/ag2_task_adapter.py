"""ag2_task_adapter.py -- unified funnel: proposals/ideas/AG2 output -> ExperimentTask.

Three intakes, one output type (ExperimentTask):
  - from_proposer(proposals)        : deterministic ParameterProposer output
  - from_human_idea(idea)           : your inspiration (structured dict OR "param=v1,v2" text)
  - from_ag2_discussion(result)     : AG2 self-generated, parses research_proposer experiment_spec

All intakes run a registry preflight (duplicate/failed/verified are dropped) + dedup.
No LLM is required for proposer/human-structured paths.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import yaml

from .registry_updater import RegistryUpdater
from .task_queue import ExperimentTask

# Default B1 params the backtest entrypoint can vary via CLI (must match _B1_SPEC.param_flags).
_B1_CLI_PARAMS = {
    "j_max", "j_min", "vol_mode", "vol_peak", "vol_ma5", "turnover", "pe_max", "pb_max",
    "cs_shadow", "top_n", "wave_qual", "wave_health", "wave_break", "washout",
    "wave_max_gain", "wave_max_turnover", "wave_red_green_ratio", "wave_health_ratio",
    "surge_min_gain", "wave_break_width", "group_gap", "group_back_gap",
}
_B1_NORMALIZE = {"turnover_max": "turnover", "top_n_per_day": "top_n",
                 "vol_vs_wave_peak_max": "vol_peak", "vol_shrink_mode": "vol_mode"}


class AG2TaskAdapter:
    def __init__(self, strategy: str = "b1", registry_updater: RegistryUpdater | None = None,
                 default_scope: dict | None = None, cli_params: set | None = None,
                 normalize_map: dict | None = None, champion_params: dict | None = None):
        self.strategy = strategy.lower()
        self.registry_updater = registry_updater or RegistryUpdater(self.strategy)
        # used when a human idea / AG2 spec doesn't carry start/end/max_stocks
        self.default_scope = default_scope or {"start": "2024-01-01", "end": "2024-06-30", "max_stocks": 60}
        self.cli_params = cli_params or _B1_CLI_PARAMS
        self.normalize_map = normalize_map or _B1_NORMALIZE
        # champion baseline params merged under each candidate so a sweep isolates ONE override
        self.champion_params = champion_params or {}
        self._seen_keys: set[str] = set()

    # ---- intake 1: deterministic proposer ---------------------------------
    def from_proposer(self, proposals: list[dict], priority: int = 100, limit: int | None = None) -> list[ExperimentTask]:
        return self._to_tasks(proposals, priority=priority, source="proposer", limit=limit)

    # ---- intake 2: human idea ---------------------------------------------
    def from_human_idea(self, idea, priority: int = 0, fallback_grid: dict | None = None) -> list[ExperimentTask]:
        """Accept a structured dict, a list, or a 'param=v1,v2' / 'param' string.
        Multi-value ideas expand to ONE experiment per value. A bare 'param' uses
        fallback_grid[param] (the search-space grid) for its values."""
        proposals: list[dict] = []
        items = idea if isinstance(idea, list) else [idea]
        for it in items:
            for params in self._idea_to_param_dicts(it, fallback_grid or {}):
                proposals.append(self._proposal_from_params(params))
        return self._to_tasks(proposals, priority=priority, source="human_idea")

    # ---- intake 3: AG2 self-generated -------------------------------------
    def from_ag2_discussion(self, sequential_result: dict, priority: int = 100,
                            fallback_grid: dict | None = None) -> list[ExperimentTask]:
        """Parse run_sequential_workflow result transcript for research_proposer's
        machine-readable experiment_spec (param/values). Ignores free prose."""
        proposals: list[dict] = []
        transcript = (sequential_result or {}).get("transcript", []) or []
        for step in transcript:
            if step.get("stage") != "research_proposer":
                continue
            out = step.get("output", {}) or {}
            spec = out.get("experiment_spec") or out.get("proposal", {}).get("experiment_spec")
            if isinstance(spec, str):
                try:
                    spec = yaml.safe_load(spec)
                except Exception:
                    spec = None
            if isinstance(spec, dict):
                for params in self._idea_to_param_dicts(spec, fallback_grid or {}):
                    proposals.append(self._proposal_from_params(params))
        return self._to_tasks(proposals, priority=priority, source="ag2")

    # ---- helpers ----------------------------------------------------------
    def _parse_idea_text(self, text: str) -> dict | None:
        """'pe_max=30,50,80' or 'turnover_max' -> structured spec. NL without a known
        param is skipped (route such ideas via --source ag2)."""
        t = text.strip()
        if "=" in t:
            name, rhs = t.split("=", 1)
            name = name.strip()
            values = [self._coerce(v.strip()) for v in rhs.split(",") if v.strip()]
        else:
            name = t.split()[0] if t.split() else ""
            values = None
        name = self._normalize(name)
        if name not in self.cli_params:
            return None
        if not values:
            return {"param": name, "values": None}  # let search-space grid fill values later
        return {"param": name, "values": values}

    def _normalize(self, name: str) -> str:
        return self.normalize_map.get(name, name)

    @staticmethod
    def _coerce(v: str):
        for cast in (int, float):
            try:
                return cast(v)
            except ValueError:
                continue
        if v.lower() in ("true", "false"):
            return v.lower() == "true"
        return v

    def _idea_to_param_dicts(self, it, fallback_grid: dict) -> list[dict]:
        """Normalize one idea (dict/str) into a list of concrete params dicts (one per value)."""
        if isinstance(it, dict):
            if "params" in it and isinstance(it["params"], dict):
                return [dict(it["params"])]
            param = self._normalize(it.get("param", ""))
            vals = it.get("values")
            if vals is None and it.get("value") is not None:
                vals = [it["value"]]
        elif isinstance(it, str):
            spec = self._parse_idea_text(it)
            if not spec:
                return []
            param = spec["param"]
            vals = spec.get("values")
        else:
            return []
        if not param or param not in self.cli_params:
            return []
        if not vals:  # bare param -> use search-space grid
            vals = fallback_grid.get(param)
        if not vals:
            return []
        return [{param: v} for v in vals]

    def _proposal_from_params(self, override: dict) -> dict:
        # candidate = champion baseline + ONE override, so a sweep isolates a single variable
        params = {**self.champion_params, **override}
        desc = ", ".join(f"{k}={v}" for k, v in override.items())
        return {
            "hypothesis": f"{self.strategy.upper()} parameter experiment: {desc}",
            "alpha_source": f"{self.strategy.upper()} candidate ranking (parameter)",
            "scope": {"strategy": self.strategy.upper(), **self.default_scope, "params": params},
            "success_criteria": "account-level metrics vs champion baseline",
            "experiment_spec": {"override": override, "params": params},
        }

    def _to_tasks(self, proposals: list[dict], priority: int, source: str, limit: int | None = None) -> list[ExperimentTask]:
        tasks = []
        for prop in proposals:
            if limit is not None and len(tasks) >= limit:
                break
            if not prop:
                continue
            scope = prop.get("scope") or {}
            scope.setdefault("strategy", self.strategy.upper())
            for k in ("start", "end", "max_stocks"):
                scope.setdefault(k, self.default_scope.get(k))
            prop["scope"] = scope

            # dedup by full params signature (only survivors are marked seen)
            key = self._dedup_key(scope.get("params", {}))
            if key in self._seen_keys:
                continue

            # registry preflight: drop duplicate/failed/verified
            ref = self.registry_updater.classify(prop.get("hypothesis", ""))
            if ref.registry_status in ("duplicate", "failed", "verified"):
                continue

            self._seen_keys.add(key)
            # deterministic, collision-resistant id (stable across processes -> --resume safe)
            digest = hashlib.md5(f"{self.strategy}|{key}".encode()).hexdigest()[:10]
            tid = f"{self.strategy}-auto-{source}-{digest}"
            tasks.append(ExperimentTask(
                task_id=tid, strategy=self.strategy, proposal=prop,
                source=source, priority=priority,
            ))
        return tasks

    @staticmethod
    def _dedup_key(params: dict) -> str:
        return "|".join(f"{k}={params[k]}" for k in sorted(params))
