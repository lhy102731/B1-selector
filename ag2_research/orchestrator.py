"""Workflow orchestrator — runs sequential Research OS pipelines, GroupChat, and roundtables."""
from __future__ import annotations

import re
import sys
import time
import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import autogen
import yaml

from .agents import create_agents
from .config import ResearchConfig
from .deepseek_compat import create_profiled_assistant_agent
from .tools import get_tools_for_agent


def _configure_utf8_stdio() -> None:
    """Keep Windows redirected AG2 logs from crashing on Unicode source text."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")


_configure_utf8_stdio()


@dataclass
class ResearchSession:
    """Holds the state of a research session."""

    config: ResearchConfig
    agents: dict[str, autogen.AssistantAgent] = field(default_factory=dict)
    chat_history: list[dict] = field(default_factory=list)


# ============================================================
# Research OS Runtime v1.0 -- Memory Routing + Registry Gate
# ============================================================

_STOPWORDS = set(
    "a an the of to in on for and or is are be with without within into vs via as at by from "
    "this that these those it its their use using used only not no new full b1 b3 brick".split()
)

PHASE6_ROLLING_FORWARD_FOLDS = (
    ((2020, 2022), (2023, 2023), (2024, 2024)),
    ((2021, 2023), (2024, 2024), (2025, 2025)),
    ((2022, 2024), (2025, 2025), (2026, 2026)),
)


def _tokenize(text: Any) -> set[str]:
    """Tokenize Latin words and overlapping CJK n-grams."""
    if not text:
        return set()
    value = str(text).lower()
    tokens = {
        word for word in re.findall(r"[a-zA-Z_]+", value)
        if word not in _STOPWORDS and len(word) > 1
    }
    for sequence in re.findall(r"[\u4e00-\u9fff]+", value):
        if len(sequence) == 1:
            tokens.add(sequence)
        else:
            tokens.update(sequence[index:index + 2] for index in range(len(sequence) - 1))
    return tokens


def _coverage(query: set[str], target: set[str]) -> float:
    """Fraction of the query tokens covered by the target (overlap coefficient).

    Better than Jaccard for "is this hypothesis already described by an entry",
    because a long registry blob does not dilute the score.
    """
    if not query:
        return 0.0
    return len(query & target) / len(query)


class RegistryGate:
    """Runtime Registry Gate -- owned by System_Orchestrator (PATCH #1).

    Performs lookup + classification against a strategy's registry experiments
    using the unified taxonomy. No other role may call this.
    """

    TAXONOMY = ["duplicate", "partial_overlap", "failed", "verified", "open", "none"]
    DUP_THRESHOLD = 0.6
    PARTIAL_THRESHOLD = 0.3

    def __init__(self, entries: list[dict] | None = None):
        self.entries = entries or []

    def classify(self, hypothesis: str) -> dict:
        """Return {registry_status, matched_id, overlap, action}."""
        if not self.entries:
            return {"registry_status": "none", "matched_id": None, "overlap": 0.0,
                    "action": "pass"}
        h = _tokenize(hypothesis)
        best, best_ov = None, 0.0
        for e in self.entries:
            blob = f"{e.get('title', '')} {e.get('short_result', '')}"
            ov = _coverage(h, _tokenize(blob))
            if ov > best_ov:
                best_ov, best = ov, e
        status = self._status_for(best, best_ov)
        return {
            "registry_status": status,
            "matched_id": (best or {}).get("id"),
            "overlap": round(best_ov, 3),
            "action": self.action_for(status),
        }

    def _status_for(self, entry: dict | None, overlap: float) -> str:
        if entry is None or overlap < self.PARTIAL_THRESHOLD:
            return "none"
        if overlap < self.DUP_THRESHOLD:
            return "partial_overlap"
        st = (entry.get("status") or "").upper()
        if st == "VERIFIED":
            return "verified"
        if st in ("FAILED", "ABANDONED"):
            return "failed"
        if st == "OPEN":
            return "open"
        return "duplicate"

    @staticmethod
    def action_for(status: str) -> str:
        return {
            "open": "pass",
            "none": "pass",
            "partial_overlap": "modify",
            "duplicate": "reject",
            "failed": "reject",
            "verified": "reject",
        }.get(status, "pass")


class LegacyMemoryAdapter:
    """One-time adapter for legacy machine-control memory files.

    The packet is retained only for deterministic machine gates. V3.4 agent
    context is built exclusively through LearningContextRouter.
    """

    def __init__(self, strategy_id: str = "b1", root: str | Path | None = None):
        self.strategy_id = strategy_id.lower()
        self.root = Path(root) if root else Path(__file__).resolve().parent.parent
        self.registry_entries = self._load_registry_entries()
        self.registry_gate = RegistryGate(self.registry_entries)
        self._packet_cache: dict | None = None

    # ---- file discovery ----------------------------------------------------
    def _latest(self, pattern: str) -> Path | None:
        def version_key(path: Path) -> tuple[int, str]:
            match = re.search(r"_v(\d+)$", path.stem, re.IGNORECASE)
            return (int(match.group(1)) if match else -1, path.name)

        matches = sorted(self.root.glob(pattern), key=version_key)
        return matches[-1] if matches else None

    def _load(self, path: Path | None) -> dict:
        if not path or not path.exists():
            return {}
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except Exception as error:
            raise RuntimeError(f"cannot load required memory file '{path}': {error}") from error
        if not isinstance(data, dict):
            raise RuntimeError(f"required memory file '{path}' must contain a YAML mapping")
        return data

    def _load_registry_entries(self) -> list[dict]:
        path = self._latest(f"registry_{self.strategy_id}_v*.yaml") or \
            self._latest(f"registry_{self.strategy_id}.yaml")
        data = self._load(path)
        return (data.get("registry", {}) or {}).get("experiments", []) or []

    # ---- packet ------------------------------------------------------------
    def build_packet(self, objective: str = "") -> dict:
        if self._packet_cache is not None:
            packet = deepcopy(self._packet_cache)
            packet["current_objective"] = objective
            return packet
        snap = self._load(self._latest(f"snapshot_{self.strategy_id}.yaml")).get("snapshot", {})
        hand = self._load(self._latest(f"handoff_{self.strategy_id}_v*.yaml")).get("handoff", {})
        proj = self._load(self._latest(f"project_{self.strategy_id}_v*.yaml")).get("project", {})
        mem_path = self.root / f"{self.strategy_id}_memory.yaml"
        mem = self._load(mem_path if mem_path.exists() else None)

        self._packet_cache = {
            "strategy_id": self.strategy_id,
            "current_objective": "",
            "snapshot": {
                "current_champion": snap.get("current_champion"),
                "next_priority": snap.get("next_priority"),
                "frozen_directions": snap.get("frozen_directions"),
                "rejected_directions": snap.get("rejected_directions"),
            },
            "handoff": {
                "do_not_repeat": [d.get("item") for d in hand.get("do_not_repeat", []) if isinstance(d, dict)],
                "escalation_conditions": [c.get("condition") for c in hand.get("escalation_conditions", []) if isinstance(c, dict)],
                "active_focus": hand.get("active_focus"),
            },
            "registry_verdict": None,        # filled by the Registry Gate after the proposer
            "registry_status": None,
            "research_memory_summary": {k: (list(v) if isinstance(v, (list, tuple)) else v)
                                        for k, v in list(mem.items())[:6]} if mem else {},
            "project_context": {"name": proj.get("name"), "boundary": proj.get("boundary")},
            "active_constraints": (snap.get("frozen_directions") or []),
            "forbidden_actions": [d.get("item") for d in hand.get("do_not_repeat", []) if isinstance(d, dict)],
        }
        packet = deepcopy(self._packet_cache)
        packet["current_objective"] = objective
        return packet


# Backward-compatible import for external legacy tests and adapters.
MemoryRouter = LegacyMemoryAdapter


class Orchestrator:

    """Manages multi-agent research workflows."""

    def __init__(self, config_path: str | None = None, profile: str | None = None):
        self.config = ResearchConfig(config_path)
        self.profile = profile or self.config.default_profile
        self._session: ResearchSession | None = None

    @property
    def llm_config(self) -> dict:
        return self.config.get_llm_config(self.profile)

    # ---- Session management ------------------------------------------------

    def create_session(self, research_context: str = "") -> ResearchSession:
        """Create a new research session (does not create agents yet)."""
        self._session = ResearchSession(config=self.config)
        return self._session

    # ---- Workflow runners --------------------------------------------------

    def run_brainstorm(
        self,
        topic: str,
        research_context: str = "",
        agent_ids: list[str] | None = None,
        max_rounds: int = 25,
        llm_config: dict | None = None,
    ) -> dict:
        """Run a round-robin GroupChat brainstorm session.

        All agents debate the topic. The coordinator (last agent) synthesizes
        the final roadmap.

        Args:
            topic: The research question or topic to debate.
            research_context: Background info injected into agent system messages.
            agent_ids: Agent template IDs. Defaults to the 'brainstorm' workflow preset.
            max_rounds: Max GroupChat rounds.
            llm_config: LLM config override.

        Returns:
            Dict with chat_history and summary.
        """
        wf = self.config.get_workflow("brainstorm") or {}
        if agent_ids is None:
            agent_ids = wf.get("agents") or [
                "research_proposer", "data_validator", "statistician",
                "experiment_executor", "risk_controller",
                "strategy_synthesizer", "research_historian",
                "pipeline_controller",
            ]

        # ``None`` means every role selects the profile declared in config.
        # Only a caller-supplied override may force one model onto all roles.
        _llm = llm_config or self.llm_config
        trusted_contexts, untrusted_contexts = self._prepare_v342_agent_context(
            agent_ids, research_context, llm_config=llm_config
        )
        agents = create_agents(self.config, agent_ids, llm_config, trusted_contexts)

        # Phase 1: read runtime params from config instead of hard-coding.
        speaker = self._resolve_speaker_method(wf.get("speaker_selection", "round_robin"))
        allow_repeat = bool(wf.get("allow_repeat_speaker", True))
        rounds = wf.get("max_rounds", max_rounds)

        groupchat = autogen.GroupChat(
            agents=list(agents.values()),
            messages=list(next(iter(untrusted_contexts.values()), [])),
            max_round=rounds,
            speaker_selection_method=speaker,
            allow_repeat_speaker=allow_repeat,
        )
        manager = autogen.GroupChatManager(groupchat=groupchat, llm_config=_llm)

        # Coordinator initiates (config-driven coordinator field, PATCH-aligned).
        coordinator = self._resolve_coordinator(agents, wf)
        initial_message = self._build_brainstorm_prompt(topic, agent_ids, agents)

        result = coordinator.initiate_chat(manager, message=initial_message)

        self._session = ResearchSession(config=self.config, agents=agents)
        return {"status": "completed", "chat_history": getattr(result, "chat_history", [])}

    def run_review(
        self,
        strategy_description: str,
        research_context: str = "",
        llm_config: dict | None = None,
    ) -> dict:
        """Run a quick strategy review with Risk Manager + Strategy Architect.

        Args:
            strategy_description: Description of the strategy to review.
            research_context: Background context.
            llm_config: LLM config override.

        Returns:
            Dict with review verdict and identified issues.
        """
        wf = self.config.get_workflow("review")
        agent_ids = wf["agents"]

        trusted_contexts, untrusted_contexts = self._prepare_v342_agent_context(
            agent_ids, research_context, llm_config=llm_config
        )
        agents = create_agents(self.config, agent_ids, llm_config, trusted_contexts)

        # Phase 1: config-driven speaker selection / repeat / rounds.
        speaker = self._resolve_speaker_method(wf.get("speaker_selection", "auto"))
        allow_repeat = bool(wf.get("allow_repeat_speaker", True))

        groupchat = autogen.GroupChat(
            agents=list(agents.values()),
            messages=list(next(iter(untrusted_contexts.values()), [])),
            max_round=wf.get("max_rounds", 10),
            speaker_selection_method=speaker,
            allow_repeat_speaker=allow_repeat,
        )
        manager = autogen.GroupChatManager(
            groupchat=groupchat,
            llm_config=llm_config or self.llm_config,
        )

        coordinator = self._resolve_coordinator(agents, wf)
        prompt = f"""Review the following result/proposal critically:

{strategy_description}

Risk_Controller: Assess execution / robustness / regime / deployment risk and emit a verdict.
Strategy_Synthesizer: Draft the memory deltas implied by the result.
System_Orchestrator: Provide the final control_decision — APPROVE_NEXT / REJECT / ESCALATE_TO_USER / COMMIT — with rationale."""

        coordinator.initiate_chat(manager, message=prompt)
        return {"status": "completed"}

    def run_chat(
        self,
        prompt: str,
        agent_ids: list[str],
        research_context: str = "",
        max_rounds: int = 15,
        llm_config: dict | None = None,
    ) -> dict:
        """Run a custom GroupChat with arbitrary agent combination.

        Args:
            prompt: The initial message to kick off the discussion.
            agent_ids: Which agent templates to include.
            research_context: Injected into agent system messages.
            max_rounds: Max conversation rounds.
            llm_config: LLM config override.

        Returns:
            Dict with status.
        """
        trusted_contexts, untrusted_contexts = self._prepare_v342_agent_context(
            agent_ids, research_context, llm_config=llm_config
        )
        agents = create_agents(self.config, agent_ids, llm_config, trusted_contexts)

        groupchat = autogen.GroupChat(
            agents=list(agents.values()),
            messages=list(next(iter(untrusted_contexts.values()), [])),
            max_round=max_rounds,
            speaker_selection_method="round_robin",
            allow_repeat_speaker=True,
        )
        manager = autogen.GroupChatManager(
            groupchat=groupchat,
            llm_config=llm_config or self.llm_config,
        )

        first_agent = list(agents.values())[0]
        first_agent.initiate_chat(manager, message=prompt)
        return {"status": "completed"}

    # ---- Runtime v1.0: dispatcher + helpers --------------------------------

    @staticmethod
    def _resolve_speaker_method(value: str) -> str:
        """Map a config speaker_selection to an AG2-valid method.

        'sequential' is a Research OS concept AG2's GroupChat does not accept; if a
        group_chat path receives it, fall back to round_robin (bounded by max_rounds).
        """
        value = (value or "").lower()
        if value in ("auto", "round_robin", "random", "manual"):
            return value
        return "round_robin"

    def _resolve_coordinator(self, agents: dict, wf: dict):
        """Pick the initiator from the config `coordinator` field, else fallbacks."""
        coord_id = (wf or {}).get("coordinator")
        if coord_id:
            tmpl = self.config.get_agent(coord_id)
            if tmpl and tmpl.get("name") in agents:
                return agents[tmpl["name"]]
        return (agents.get("Pipeline_Controller") or agents.get("System_Orchestrator")
                or agents.get("Coordinator")
                or list(agents.values())[-1])

    def run_workflow(self, workflow_id: str, topic: str = "", **kwargs) -> dict:
        """Dispatch by workflow.type (Phase 6). Backward compatible.

        type == 'sequential'  -> run_sequential_workflow()
        type == 'group_chat'  -> config-driven GroupChat (run_brainstorm path)
        type == 'roundtable_discovery' -> legacy roundtable, then gated KBase discovery
        type == 'source_first_discovery' -> project audit, KBase brief, roundtable, factor handoff
        'roundtable'          -> run_roundtable()
        """
        if workflow_id == "roundtable":
            return self.run_roundtable(topic, kwargs.get("research_context", ""))
        wf = self.config.get_workflow(workflow_id)
        if wf is None:
            raise KeyError(f"workflow '{workflow_id}' not found")
        wtype = (wf.get("type") or "group_chat").lower()
        if wtype == "sequential":
            return self.run_sequential_workflow(workflow_id, topic, **kwargs)
        if wtype == "roundtable_discovery":
            return self.run_roundtable_discovery(topic, workflow_id=workflow_id, **kwargs)
        if wtype == "source_first_discovery":
            return self.run_source_first_discovery(topic, workflow_id=workflow_id, **kwargs)
        # group_chat (and any unknown type) -> bounded, config-driven GroupChat
        return self.run_brainstorm(
            topic,
            research_context=kwargs.get("research_context", ""),
            agent_ids=wf.get("agents"),
            max_rounds=wf.get("max_rounds", 25),
            llm_config=kwargs.get("llm_config"),
        )

    # ---- Runtime v1.0: Sequential Research OS pipeline ---------------------

    def run_sequential_workflow(
        self,
        workflow_id: str = "brainstorm",
        topic: str = "",
        strategy_id: str = "b1",
        research_context: str = "",
        agent_invoker: Callable | None = None,
        memory_router: LegacyMemoryAdapter | None = None,
        max_revision_attempts: int | None = None,
        llm_config: dict | None = None,
        initial_outputs: dict[str, Any] | None = None,
        memory_packet: dict[str, Any] | None = None,
        require_kbase_inspired: bool = False,
    ) -> dict:
        """Deterministic, gated, single-pass pipeline (Phases 2/3/4/5).

        Order comes strictly from config `pipeline_order` (orchestrator stripped).
        Python is the controller: each stage runs ONCE, the orchestrator gates the
        result (pass/modify/reject), revisions are bounded, and the Registry Gate +
        memory_packet are built here -- never by the worker agents.
        """
        wf = self.config.get_workflow(workflow_id) or {}
        order = wf.get("pipeline_order") or wf.get("agents") or []
        # v4.1: pipeline_controller is the canonical name; system_orchestrator is the legacy alias.
        _controller_ids = {"system_orchestrator", "pipeline_controller"}
        stages = [a for a in order if a not in _controller_ids]
        if not stages:
            return {"status": "ERROR", "reason": "no worker stages in pipeline_order"}

        if not research_context:
            try:
                from .knowledge_bridge import build_combined_research_context

                research_context = build_combined_research_context(
                    strategy_id,
                    query=topic,
                    project_mode="brief",
                )
            except Exception:
                research_context = ""

        cl = self.config._raw.get("control_layer", {})
        max_rev = (max_revision_attempts if max_revision_attempts is not None
                   else cl.get("revision_limit", {}).get("max_revision_attempts", 2))

        # STEP 0 -- System_Orchestrator builds the single memory_packet.
        router = memory_router or LegacyMemoryAdapter(strategy_id)
        packet = dict(memory_packet or router.build_packet(objective=topic))
        if require_kbase_inspired:
            packet["_workflow_constraints"] = {
                "require_kbase_inspired": True,
                "failure_rule": (
                    "Do not substitute a project-memory or independent proposal when the "
                    "approved source brief provides no support. Report the source mismatch."
                ),
            }
        invoker = agent_invoker or self._build_sequential_invoker(
            stages,
            research_context,
            llm_config,
        )

        transcript: list[dict] = []
        last_outputs: dict[str, Any] = dict(initial_outputs or {})
        stage_counts: dict[str, int] = {}
        revisions = 0
        i = 0
        while i < len(stages):
            stage = stages[i]
            output = invoker(stage, packet, last_outputs, topic)
            stage_counts[stage] = stage_counts.get(stage, 0) + 1
            if stage_counts[stage] > 1:
                # Preserve earlier passes (for example the Statistician's
                # pre-execution prediction) while keeping the plain key as the
                # latest value for downstream compatibility.
                previous = last_outputs.get(stage)
                if previous is not None and f"{stage}__1" not in last_outputs:
                    last_outputs[f"{stage}__1"] = previous
                last_outputs[f"{stage}__{stage_counts[stage]}"] = output
            last_outputs[stage] = output
            decision, reason, status = self._gate(
                stage,
                output,
                router,
                packet,
                last_outputs=last_outputs,
                require_kbase_inspired=require_kbase_inspired,
            )
            transcript.append({
                "stage": stage, "output": output,
                "gate": {"decision": decision, "reason": reason, "registry_status": status},
            })

            if decision == "pass":
                revision_context = last_outputs.get("__controller_revision__")
                if isinstance(revision_context, dict) and revision_context.get("stage") == stage:
                    last_outputs.pop("__controller_revision__", None)
                i += 1
            elif decision == "retry":
                revisions += 1
                if revisions > max_rev:
                    return self._finish(
                        "ESCALATE_TO_USER", transcript, packet,
                        reason=f"revision limit {max_rev} exceeded at stage '{stage}'",
                        revisions=revisions,
                    )
                last_outputs["__controller_revision__"] = {
                    "stage": stage,
                    "attempt": revisions,
                    "reason": reason,
                    "required_action": "rewrite this stage output without changing upstream outputs",
                }
            elif decision == "modify":
                revisions += 1
                if revisions > max_rev:
                    return self._finish("ESCALATE_TO_USER", transcript, packet,
                                        reason=f"revision limit {max_rev} exceeded at stage '{stage}'",
                                        revisions=revisions)
                if stage == "source_librarian":
                    from .kbase.citation_inventory import revision_citation_context

                    inventory = output.get("_citation_inventory")
                    issues = output.get("_citation_issues")
                    last_outputs["__controller_revision__"] = {
                        "stage": stage,
                        "attempt": revisions,
                        "reason": reason,
                        "citation_inventory": revision_citation_context(
                            inventory if isinstance(inventory, dict) else {},
                            issues if isinstance(issues, list) else [],
                        ),
                    }
                i = max(0, i - 1)  # return to the previous stage
            elif decision == "escalate":
                return self._finish(
                    "ESCALATE_TO_USER", transcript, packet,
                    reason=f"stage '{stage}': {reason}", revisions=revisions,
                )
            else:  # reject
                return self._finish("REJECTED", transcript, packet,
                                    reason=f"stage '{stage}': {reason}", revisions=revisions)

        # This runtime validates and drafts outputs but deliberately performs no
        # memory writes. Do not claim a commit that did not happen.
        return self._finish("APPROVED", transcript, packet,
                            reason="all gates passed; no memory writes performed",
                            revisions=revisions)

    @staticmethod
    def _finish(status: str, transcript: list, packet: dict, reason: str, revisions: int) -> dict:
        return {
            "status": status,
            "reason": reason,
            "revision_attempts": revisions,
            "registry_status": packet.get("registry_status"),
            "memory_packet": packet,
            "transcript": transcript,
            "control_decision": {
                "decision": {"APPROVED": "APPROVE_NEXT", "REJECTED": "REJECT",
                             "ESCALATE_TO_USER": "ESCALATE_TO_USER"}.get(status, status),
                "reason": reason,
            },
        }

    @staticmethod
    def _window_years(value: Any) -> tuple[int, int] | None:
        """Extract an inclusive year window from common YAML shapes."""
        if value is None:
            return None
        if isinstance(value, dict):
            start = (
                value.get("start") or value.get("from") or value.get("begin")
                or value.get("start_date")
            )
            end = value.get("end") or value.get("to") or value.get("end_date")
            if start is not None and end is not None:
                return Orchestrator._window_years(f"{start} {end}")
        if isinstance(value, (list, tuple)) and len(value) >= 2:
            return Orchestrator._window_years(f"{value[0]} {value[1]}")
        years = [int(year) for year in re.findall(r"\b(20\d{2})\b", str(value))]
        if not years:
            return None
        if len(years) == 1:
            return years[0], years[0]
        return years[0], years[-1]

    @staticmethod
    def _fold_window(fold: dict, *keys: str) -> tuple[int, int] | None:
        for key in keys:
            if key in fold:
                return Orchestrator._window_years(fold.get(key))
        return None

    @staticmethod
    def _validate_forward_validation(block: Any) -> tuple[bool, str]:
        """Enforce the Brick Phase 6 rolling forward protocol."""
        if not isinstance(block, dict):
            return False, "forward_validation must be a mapping"
        folds = block.get("folds")
        if not isinstance(folds, list) or len(folds) != len(PHASE6_ROLLING_FORWARD_FOLDS):
            return False, "forward_validation.folds must contain the three Phase 6 folds"

        actual = []
        for index, fold in enumerate(folds):
            if not isinstance(fold, dict):
                return False, f"forward_validation.folds[{index}] must be a mapping"
            train = Orchestrator._fold_window(fold, "train_window", "train", "training_window")
            validation = Orchestrator._fold_window(
                fold, "validation_window", "validation", "val_window", "validate_window"
            )
            test = Orchestrator._fold_window(
                fold, "unseen_test_window", "test_window", "unseen_test", "test"
            )
            if not train or not validation or not test:
                return False, f"forward_validation.folds[{index}] lacks train/validation/test windows"
            if not (train[1] < validation[0] <= validation[1] < test[0]):
                return False, f"forward_validation.folds[{index}] is not chronological"
            actual.append((train, validation, test))

        if tuple(actual) != PHASE6_ROLLING_FORWARD_FOLDS:
            return False, "forward_validation.folds do not match the fixed Phase 6 rolling windows"
        if "embargo_days" not in block and "purge_embargo_days" not in block:
            return False, "forward_validation must state embargo_days or purge_embargo_days"
        selection_rule = str(block.get("selection_rule") or "").lower()
        if not selection_rule:
            return False, "forward_validation.selection_rule is required"
        if "test" not in selection_rule or not (
            "train" in selection_rule or "training" in selection_rule
        ) or "validation" not in selection_rule:
            return False, "selection_rule must state train/validation selection and unseen test use"
        summary = block.get("test_summary") or block.get("summary")
        required_summary = (
            "average_test_metrics", "worst_fold_metrics",
            "fold_pass_rate", "dispersion",
        )
        if not isinstance(summary, dict) or any(key not in summary for key in required_summary):
            return False, "forward_validation.test_summary lacks average/worst/pass_rate/dispersion"
        return True, "forward_validation Phase 6 protocol valid"

    @staticmethod
    def _risk_forward_validation_status(value: Any) -> str:
        if isinstance(value, dict):
            raw = value.get("status") or value.get("verdict") or value.get("risk")
        else:
            raw = value
        text = str(raw or "").strip().lower()
        if text in {"pass", "passed", "valid"} or text.startswith("pass"):
            return "pass"
        if text in {"missing", "weak", "fail", "failed", "invalid"}:
            return text
        return text or "missing"

    @staticmethod
    def _validate_compute_acceleration(block: Any) -> tuple[bool, str]:
        """Require every execution to state GPU/CPU acceleration handling."""
        if not isinstance(block, dict):
            return False, "compute_acceleration must be a mapping"
        required = (
            "workload_type", "gpu_applicable", "gpu_available",
            "selected_backend", "fallback_backend", "reason",
        )
        if any(key not in block for key in required):
            return False, "compute_acceleration is incomplete"
        if not isinstance(block.get("gpu_applicable"), bool) or not isinstance(
            block.get("gpu_available"), bool
        ):
            return False, "compute_acceleration gpu flags must be booleans"
        if not str(block.get("workload_type") or "").strip():
            return False, "compute_acceleration.workload_type is required"
        selected = str(block.get("selected_backend") or "").strip().lower()
        fallback = str(block.get("fallback_backend") or "").strip().lower()
        if not selected or not fallback or not str(block.get("reason") or "").strip():
            return False, "compute_acceleration lacks backend/reason"
        if block.get("gpu_applicable") and not block.get("gpu_available") and selected != "cpu":
            return False, "gpu_applicable without gpu_available must fall back to cpu"
        return True, "compute_acceleration contract valid"

    @staticmethod
    def _validate_indicator_cache_isolation(block: Any) -> tuple[bool, str]:
        """Keep exploratory factor research off the production indicator cache."""
        if not isinstance(block, dict):
            return False, "execution_record must be a mapping"
        try:
            text = json.dumps(block, ensure_ascii=False, sort_keys=True).lower()
        except TypeError:
            text = str(block).lower()

        research_markers = (
            "research_indicators_cache",
            "--research-cache",
            "--research-indicators-cache",
        )
        if any(marker in text for marker in research_markers):
            return True, "research indicator cache isolation valid"

        production_exception_markers = (
            "production_reproduction",
            "production reproduction",
            "explicit production",
            "user explicitly requested production",
        )
        if "indicators_cache" in text and any(marker in text for marker in production_exception_markers):
            return True, "production indicator cache explicitly authorized"

        return False, "execution_record must state research_indicators_cache or an explicit production reproduction exception"

    @staticmethod
    def _falsification_protocol_issues(
        decisive_test: dict[str, Any],
        failure_conditions: list[Any],
    ) -> list[str]:
        """Detect internally contradictory fold counts without rewriting the protocol."""
        rendered = json.dumps(
            {
                "decisive_test": decisive_test,
                "failure_conditions": failure_conditions,
            },
            ensure_ascii=False,
            default=str,
        )
        fold_count_patterns = (
            r"\b(?:run|use|using|execute|apply)\s+(?:a\s+)?(\d+)\s*(?:-\s*)?folds?\b",
            r"(?<![\d/])(\d+)\s*-\s*folds?\b",
            r"(?:\u91c7\u7528|\u4f7f\u7528|\u8fd0\u884c)\s*(\d+)\s*\u6298",
        )
        fold_counts = {
            int(value)
            for pattern in fold_count_patterns
            for value in re.findall(pattern, rendered, flags=re.IGNORECASE)
        }
        ratio_pattern = (
            r"(?<![\d.])(\d+)\s*/\s*(\d+)"
            r"(?=\s*(?:PWF\s*)?(?:folds?\b|\u6298))"
        )
        ratios = [
            (int(numerator), int(denominator))
            for numerator, denominator in re.findall(
                ratio_pattern, rendered, flags=re.IGNORECASE
            )
            if int(numerator) <= 100 and int(denominator) <= 100
        ]
        issues: list[str] = []
        if len(fold_counts) > 1:
            issues.append(f"conflicting declared fold counts: {sorted(fold_counts)}")
        if len(fold_counts) == 1:
            declared = next(iter(fold_counts))
            mismatched = sorted({ratio for ratio in ratios if ratio[1] != declared})
            if mismatched:
                issues.append(
                    f"declared {declared} folds but pass/fail ratios use {mismatched}"
                )
        invalid_ratios = sorted({ratio for ratio in ratios if ratio[0] > ratio[1]})
        if invalid_ratios:
            issues.append(f"invalid fold ratios: {invalid_ratios}")
        return issues

    def _gate(self, stage: str, output: dict, router: LegacyMemoryAdapter, packet: dict,
              last_outputs: dict | None = None, *, require_kbase_inspired: bool = False):
        """Orchestrator gate decision per stage. Returns (decision, reason, registry_status)."""
        last_outputs = last_outputs or {}
        stage_error = str(output.get("error") or "").strip() if isinstance(output, dict) else ""
        if stage_error:
            return "escalate", f"stage execution failed without fallback: {stage_error}", None
        if stage == "source_librarian":
            try:
                from .kbase.repository import KBaseRepository
                from .kbase.schemas import (
                    ContractValidationError,
                    validate_source_brief,
                    validate_source_brief_semantics,
                )
                from .kbase.citation_inventory import citation_inventory_issues

                payload = self._normalize_source_brief_payload({
                    key: value for key, value in output.items() if not str(key).startswith("_")
                })
                try:
                    validate_source_brief(payload)
                except ContractValidationError as error:
                    if "project-derivation fields are forbidden" in str(error):
                        raise
                    issues = [{
                        "code": "source_brief_contract_invalid",
                        "detail": str(error),
                    }]
                    output["_citation_issues"] = issues
                    return (
                        "modify",
                        "source brief requires a complete replacement: "
                        + json.dumps(issues, ensure_ascii=False, sort_keys=True),
                        None,
                    )
                inventory = output.get("_citation_inventory")
                if isinstance(inventory, dict):
                    issues = citation_inventory_issues(payload, inventory)
                    if issues:
                        output["_citation_issues"] = issues
                        return (
                            "modify",
                            "source citation inventory mismatch: "
                            + json.dumps(issues, ensure_ascii=False, sort_keys=True),
                            None,
                        )
                repo = KBaseRepository()
                evidence_refs_by_source = {}
                for source in payload.get("sources_consulted", []):
                    source_id = str(source.get("source_id") or "")
                    entry = repo.get(source_id)
                    allowed = set()
                    if entry:
                        packet_document = repo.read_packet(entry)
                        record = packet_document.get("record") if isinstance(packet_document.get("record"), dict) else {}
                        for field in ("methods", "claims", "risks", "contradictions", "definitions", "examples"):
                            values = record.get(field, []) if isinstance(record.get(field), list) else []
                            if values:
                                allowed.add(f"{source_id}#{field}")
                            allowed.update(f"{source_id}#{field}[{index}]" for index in range(len(values)))
                        for layer in ("raw", "wiki"):
                            if entry.get("paths", {}).get(layer):
                                allowed.add(f"{source_id}#{layer}")
                        if entry.get("summary") or "summary" in (entry.get("available_layers") or []):
                            allowed.add(f"{source_id}#summary")
                    evidence_refs_by_source[source_id] = allowed
                try:
                    validate_source_brief_semantics(
                        payload,
                        known_source_ids={str(entry["source_id"]) for entry in repo.entries()},
                        catalog_version=str(repo.manifest.get("catalog_version")),
                        evidence_refs_by_source=evidence_refs_by_source,
                    )
                except ContractValidationError as error:
                    issues = [{
                        "code": "source_brief_semantics_invalid",
                        "detail": str(error),
                    }]
                    output["_citation_issues"] = issues
                    return (
                        "modify",
                        "source brief requires a source-only semantic revision: "
                        + json.dumps(issues, ensure_ascii=False, sort_keys=True),
                        None,
                    )
            except Exception as error:
                return "reject", f"invalid source_brief: {type(error).__name__}: {error}", None
            output.update(payload)
            return "pass", "source_brief contract and catalog references valid", None
        if stage in {"alpha_hunter", "falsification_officer", "factor_engineer"}:
            self._recover_scattered_source_boundary(stage, output, last_outputs)
            boundary = output.get("source_boundary")
            if not isinstance(boundary, dict):
                return "reject", "missing source_boundary", None
            if "source_supported" in boundary:
                boundary["source_supported"] = self._normalize_source_supported(
                    boundary.get("source_supported")
                )
            channel = str(boundary.get("research_channel") or "")
            if channel not in {"kbase_inspired", "independent"}:
                return "reject", "invalid research_channel", None
            if require_kbase_inspired and stage == "alpha_hunter" and channel != "kbase_inspired":
                return (
                    "reject",
                    "source-first discovery cannot substitute an independent or project-memory "
                    "proposal for a KBase-supported candidate",
                    None,
                )
            if channel == "kbase_inspired":
                if stage == "falsification_officer":
                    upstream_boundary = (last_outputs.get("alpha_hunter") or {}).get("source_boundary") or {}
                    boundary.setdefault("source_brief_id", upstream_boundary.get("source_brief_id"))
                    boundary.setdefault("source_supported", upstream_boundary.get("source_supported"))
                    if not boundary.get("source_supported"):
                        boundary["source_supported"] = upstream_boundary.get("source_supported")
                elif stage == "factor_engineer":
                    upstream_boundary = (last_outputs.get("falsification_officer") or {}).get("source_boundary") or {}
                    boundary.setdefault("source_brief_id", upstream_boundary.get("source_brief_id"))
                    boundary.setdefault("source_supported", upstream_boundary.get("source_supported"))
                    if not boundary.get("source_supported"):
                        boundary["source_supported"] = upstream_boundary.get("source_supported")
                if not boundary.get("source_brief_id") or not boundary.get("source_supported"):
                    return "reject", "kbase_inspired output lacks source_brief_id/source_supported", None
                if stage == "alpha_hunter":
                    upstream = last_outputs.get("source_librarian") or {}
                    expected_brief = upstream.get("brief_id")
                    consulted = {
                        str(item.get("source_id"))
                        for item in upstream.get("sources_consulted", [])
                        if isinstance(item, dict) and item.get("source_id")
                    }
                    if not expected_brief or boundary.get("source_brief_id") != expected_brief:
                        return "reject", "source_brief_id does not match Source Librarian output", None
                    unsupported = set(map(str, boundary.get("source_supported", []))) - consulted
                    if unsupported:
                        return "reject", f"source_supported not consulted: {sorted(unsupported)}", None
                elif stage == "factor_engineer":
                    upstream = (last_outputs.get("falsification_officer") or {}).get("source_boundary") or {}
                    if not upstream:
                        return "reject", "missing Falsification Officer source boundary", None
                    if boundary.get("source_brief_id") != upstream.get("source_brief_id"):
                        return "reject", "source_brief_id does not match Falsification Officer output", None
                    if set(map(str, boundary.get("source_supported", []))) != set(
                        map(str, upstream.get("source_supported", []))
                    ):
                        return "reject", "source_supported does not match Falsification Officer output", None
                else:
                    upstream = (last_outputs.get("alpha_hunter") or {}).get("source_boundary") or {}
                    if upstream.get("research_channel") != "kbase_inspired":
                        return "reject", "research_channel does not match Alpha Hunter output", None
                    if boundary.get("source_brief_id") != upstream.get("source_brief_id"):
                        return "reject", "source_brief_id does not match Alpha Hunter output", None
                    if set(map(str, boundary.get("source_supported", []))) != set(
                        map(str, upstream.get("source_supported", []))
                    ):
                        return "reject", "source_supported does not match Alpha Hunter output", None
            else:
                if boundary.get("source_supported") or boundary.get("source_brief_id"):
                    return "reject", "independent output cannot claim source support or a source brief", None
                if stage == "factor_engineer":
                    upstream = (last_outputs.get("falsification_officer") or {}).get("source_boundary") or {}
                    if upstream.get("research_channel") != "independent":
                        return "reject", "research_channel does not match Falsification Officer output", None
                elif stage == "falsification_officer":
                    upstream = (last_outputs.get("alpha_hunter") or {}).get("source_boundary") or {}
                    if upstream.get("research_channel") != "independent":
                        return "reject", "research_channel does not match Alpha Hunter output", None

            if stage == "alpha_hunter":
                gap = output.get("alpha_family_gap")
                generator = output.get("proposed_generator")
                if not isinstance(gap, dict) or not isinstance(generator, dict):
                    return "reject", "missing alpha_family_gap/proposed_generator", None
                gap_required = ("existing_families", "missing_families", "highest_potential")
                generator_required = (
                    "family", "mechanism", "required_data",
                    "expected_jaccard_vs_wave_qualified", "expected_information_gain",
                )
                if any(key not in gap for key in gap_required):
                    return "reject", "incomplete alpha_family_gap", None
                if not isinstance(gap.get("existing_families"), list) or not isinstance(
                    gap.get("missing_families"), list
                ) or not gap.get("missing_families") or not str(
                    gap.get("highest_potential") or ""
                ).strip():
                    return "reject", "alpha_family_gap lacks a substantive gap", None
                if any(not str(generator.get(key) or "").strip() for key in generator_required):
                    return "reject", "incomplete proposed_generator", None
                if channel == "kbase_inspired":
                    bias = output.get("kbase_bias_check")
                    bias_required = (
                        "source_density_bias",
                        "why_not_source_abundance",
                        "underexplored_alternative_considered",
                        "novelty_or_reopen_reason",
                    )
                    if not isinstance(bias, dict):
                        return "reject", "missing kbase_bias_check", None
                    density = str(bias.get("source_density_bias") or "").lower()
                    if density not in {"low", "medium", "high"}:
                        return "reject", "invalid kbase_bias_check.source_density_bias", None
                    if any(not str(bias.get(key) or "").strip() for key in bias_required[1:]):
                        return "reject", "incomplete kbase_bias_check", None
            elif stage == "falsification_officer":
                alpha = last_outputs.get("alpha_hunter") or {}
                generator = alpha.get("proposed_generator") or {}
                mechanism_ref = output.get("alpha_mechanism")
                expected_mechanism = {
                    "family": generator.get("family"),
                    "mechanism": generator.get("mechanism"),
                }
                if not isinstance(mechanism_ref, dict):
                    output["alpha_mechanism"] = expected_mechanism
                    output["alpha_mechanism_binding_note"] = (
                        "inserted from the exact Alpha Hunter proposed_generator; "
                        "Falsification Officer critique remains in counter_hypothesis/"
                        "decisive_test/failure_conditions"
                    )
                    mechanism_ref = expected_mechanism
                if str(mechanism_ref.get("family") or "").strip() != str(
                    expected_mechanism.get("family") or ""
                ).strip():
                    return "reject", "alpha_mechanism family does not match Alpha Hunter output", None
                if mechanism_ref != expected_mechanism:
                    output["alpha_mechanism_raw"] = mechanism_ref
                    output["alpha_mechanism"] = expected_mechanism
                    output["alpha_mechanism_binding_note"] = (
                        "normalized to the exact Alpha Hunter proposed_generator; "
                        "Falsification Officer critique remains in counter_hypothesis/"
                        "decisive_test/failure_conditions"
                    )
                counter = output.get("counter_hypothesis")
                test = output.get("decisive_test")
                failures = output.get("failure_conditions")
                verdict = str(output.get("verdict") or "").upper()
                test_required = (
                    "method", "discriminating_observation",
                    "expected_if_alpha_holds", "expected_if_counter_holds",
                )
                if not str(counter or "").strip():
                    return "reject", "missing substantive counter_hypothesis", None
                if not isinstance(test, dict) or any(
                    not str(test.get(key) or "").strip() for key in test_required
                ):
                    return "reject", "decisive_test is incomplete", None
                if test.get("expected_if_alpha_holds") == test.get("expected_if_counter_holds"):
                    return "reject", "decisive_test outcomes are not discriminating", None
                if not isinstance(failures, list) or not any(str(item).strip() for item in failures):
                    return "reject", "failure_conditions must be a non-empty list", None
                protocol_issues = self._falsification_protocol_issues(test, failures)
                if protocol_issues:
                    output["_protocol_issues"] = protocol_issues
                    return (
                        "retry",
                        "falsification protocol is internally inconsistent: "
                        + "; ".join(protocol_issues),
                        None,
                    )
                if verdict == "REVISE":
                    if not str(output.get("revision_guidance") or "").strip():
                        return "reject", "REVISE requires revision_guidance", None
                    return "modify", "falsification requires bounded Alpha revision", None
                if verdict == "REJECT":
                    return "reject", "falsification rejected the proposed mechanism", None
                if verdict != "PROCEED":
                    return "reject", "verdict must be PROCEED, REVISE, or REJECT", None
            else:
                falsification = last_outputs.get("falsification_officer") or {}
                consumed = output.get("falsification_consumed")
                expected_consumed = {
                    "verdict": "PROCEED",
                    "counter_hypothesis": falsification.get("counter_hypothesis"),
                    "decisive_test": falsification.get("decisive_test"),
                    "failure_conditions": falsification.get("failure_conditions"),
                }
                if falsification.get("verdict") != "PROCEED":
                    return "reject", "factor output did not consume the passed falsification result", None
                if not isinstance(consumed, dict):
                    if not output.get("_falsification_consumed_declared"):
                        return "reject", "missing falsification_consumed", None
                    output["falsification_consumed_raw"] = consumed
                    output["falsification_consumed"] = expected_consumed
                    output["falsification_consumed_binding_note"] = (
                        "recovered declared PROCEED consumption and bound it to the exact "
                        "passed Falsification Officer review"
                    )
                    consumed = expected_consumed
                consumed_required = (
                    "verdict", "counter_hypothesis", "decisive_test", "failure_conditions"
                )
                if any(key not in consumed for key in consumed_required):
                    if not output.get("_falsification_consumed_declared"):
                        return "reject", "incomplete falsification_consumed", None
                    output["falsification_consumed_raw"] = consumed
                    output["falsification_consumed"] = expected_consumed
                    output["falsification_consumed_binding_note"] = (
                        "recovered declared PROCEED consumption and bound it to the exact "
                        "passed Falsification Officer review"
                    )
                    consumed = expected_consumed
                if str(consumed.get("verdict") or "").upper() != "PROCEED":
                    return "reject", "factor output did not consume the passed falsification result", None
                if consumed != expected_consumed:
                    output["falsification_consumed_raw"] = consumed
                    output["falsification_consumed"] = expected_consumed
                    output["falsification_consumed_binding_note"] = (
                        "normalized to the exact passed Falsification Officer review"
                    )
                factors = output.get("factor_batch")
                research_mechanism = output.get("research_mechanism")
                required = (
                    "name", "expression", "family", "polarity",
                    "transformation_type", "data_requirements",
                )
                if isinstance(factors, list) and factors:
                    for index, factor in enumerate(factors):
                        if not isinstance(factor, dict) or any(key not in factor for key in required):
                            return "reject", f"factor_batch[{index}] is incomplete", None
                        if any(not str(factor.get(key) or "").strip() for key in required[:-1]):
                            return "reject", f"factor_batch[{index}] has empty fields", None
                        if not isinstance(factor.get("data_requirements"), list) or not factor["data_requirements"]:
                            return "reject", f"factor_batch[{index}] lacks data_requirements", None
                    unsupported = self._unsupported_factor_data_requirements(factors)
                    if unsupported:
                        preview = "; ".join(unsupported[:3])
                        if len(unsupported) > 3:
                            preview += f"; +{len(unsupported) - 3} more"
                        return "reject", f"factor_batch uses unavailable data requirements: {preview}", None
                else:
                    mechanism_required = (
                        "name",
                        "family",
                        "mechanism",
                        "runner_id",
                        "validation_plan",
                        "stop_conditions",
                    )
                    if not isinstance(research_mechanism, dict):
                        return "reject", "factor_batch must be non-empty unless research_mechanism is provided", None
                    if any(key not in research_mechanism for key in mechanism_required):
                        return "reject", "research_mechanism is incomplete", None
                    if any(not str(research_mechanism.get(key) or "").strip() for key in mechanism_required[:4]):
                        return "reject", "research_mechanism has empty fields", None
                    if not research_mechanism.get("validation_plan"):
                        return "reject", "research_mechanism lacks validation_plan", None
                    if not isinstance(research_mechanism.get("stop_conditions"), list) or not research_mechanism["stop_conditions"]:
                        return "reject", "research_mechanism lacks stop_conditions", None
            return "pass", f"source boundary valid ({channel})", None
        if stage == "research_proposer":
            proposal = output.get("proposal") if isinstance(output.get("proposal"), dict) else {}
            required = (
                "hypothesis", "alpha_source", "scope", "novelty_justification",
                "success_criteria", "experiment_spec", "requested_next_role",
            )
            if any(key not in proposal for key in required):
                return "reject", "incomplete proposal contract", None
            if (not str(proposal.get("hypothesis") or "").strip()
                    or not str(proposal.get("alpha_source") or "").strip()
                    or not isinstance(proposal.get("scope"), dict)
                    or not str(proposal.get("success_criteria") or "").strip()
                    or not isinstance(proposal.get("experiment_spec"), dict)
                    or not proposal["experiment_spec"]):
                return "reject", "proposal lacks substantive experiment fields", None
            hypo = (output.get("raw_hypothesis") or output.get("hypothesis")
                    or proposal.get("hypothesis") or output.get("_raw", ""))
            verdict = router.registry_gate.classify(hypo)
            packet["registry_status"] = verdict["registry_status"]
            packet["registry_verdict"] = verdict
            return verdict["action"], f"registry_status={verdict['registry_status']} (match={verdict['matched_id']}, overlap={verdict['overlap']})", verdict["registry_status"]
        if stage == "data_validator":
            block = output.get("data_verdict") if isinstance(output.get("data_verdict"), dict) else output
            required = (
                "fields_required", "production_available", "leakage_risk",
                "data_consistency", "forward_validation_design", "verdict",
                "blocking_reasons", "next_role_if_pass",
            )
            if any(key not in block for key in required):
                return "reject", "incomplete data_verdict", None
            if (not isinstance(block.get("fields_required"), list)
                    or not isinstance(block.get("production_available"), (list, dict))
                    or not isinstance(block.get("blocking_reasons"), list)
                    or not str(block.get("data_consistency") or "").strip()):
                return "reject", "data_verdict has invalid field structure", None
            design = block.get("forward_validation_design")
            if isinstance(design, dict):
                ok, reason = self._validate_forward_validation(design)
                if not ok:
                    return "reject", reason, None
            elif not str(design or "").strip():
                return "reject", "missing forward_validation_design", None
            v = str(block.get("verdict", "")).upper()
            return ("pass" if v == "PASS" else "reject"), f"data_verdict={v or 'MISSING'}", None
        if stage == "experiment_executor":
            block = output.get("execution_record") if isinstance(output.get("execution_record"), dict) else output
            required = (
                "command", "config", "date_range", "forward_validation",
                "compute_acceleration", "metrics", "output_files",
                "sanity_check", "anomaly_flag",
            )
            if any(key not in block for key in required):
                return "reject", "incomplete execution_record", None
            if not str(block.get("command") or "").strip() or not isinstance(block.get("config"), dict):
                return "reject", "execution_record lacks command/config", None
            if not str(block.get("date_range") or "").strip() or not isinstance(block.get("metrics"), dict) or not block["metrics"]:
                return "reject", "execution_record lacks date_range/metrics", None
            if not isinstance(block.get("output_files"), list) or not str(block.get("sanity_check") or "").strip():
                return "reject", "execution_record lacks output_files/sanity_check", None
            ok, reason = self._validate_forward_validation(block.get("forward_validation"))
            if not ok:
                return "reject", reason, None
            ok, reason = self._validate_compute_acceleration(block.get("compute_acceleration"))
            if not ok:
                return "reject", reason, None
            ok, reason = self._validate_indicator_cache_isolation(block)
            if not ok:
                return "reject", reason, None
            anomaly = str(block.get("anomaly_flag", "none")).strip().lower()
            if anomaly and anomaly not in ("none", "false", ""):
                return "reject", "anomaly_flag set -> STOP_AND_VERIFY", None
            return "pass", "execution ok", None
        if stage == "risk_controller":
            block = output.get("risk_verdict") if isinstance(output.get("risk_verdict"), dict) else output
            required = (
                "execution_risk", "robustness_risk", "forward_validation_risk",
                "regime_risk", "deployment_risk", "baseline_comparison",
                "escalation_triggered", "verdict", "rationale",
            )
            if any(key not in block for key in required):
                return "reject", "incomplete risk_verdict", None
            if (not isinstance(block.get("escalation_triggered"), list)
                    or not str(block.get("rationale") or "").strip()):
                return "reject", "risk_verdict lacks rationale/escalation structure", None
            v = str(block.get("verdict", "")).upper()
            forward_status = self._risk_forward_validation_status(block.get("forward_validation_risk"))
            if v == "VALID":
                if forward_status != "pass":
                    return "reject", "VALID risk verdict requires forward_validation_risk=pass", None
                return "pass", "risk VALID", None
            if v == "INVALID":
                return "reject", "risk INVALID", None
            # Re-running the same upstream outputs cannot resolve missing or
            # contradictory evidence. Escalate instead of looping pointlessly.
            return "escalate", f"risk {v or 'INCONCLUSIVE'} requires new evidence", None
        if stage == "strategy_synthesizer":
            block = output.get("synthesis") if isinstance(output.get("synthesis"), dict) else None
            required = ("registry_entry_delta", "snapshot_delta", "handoff_delta", "recommended_next_priority")
            if not block or any(key not in block for key in required):
                return "reject", "incomplete synthesis", None
            if not isinstance(block.get("registry_entry_delta"), dict) or not block["registry_entry_delta"]:
                return "reject", "synthesis lacks registry_entry_delta", None
            if not str(block.get("recommended_next_priority") or "").strip():
                return "reject", "synthesis lacks recommended_next_priority", None
            return "pass", "synthesis drafted", None
        if stage == "theory_builder":
            block = output.get("theory_hypothesis") if isinstance(output.get("theory_hypothesis"), dict) else None
            required = ("mechanism", "expected_market", "failure_mode", "observable_signature")
            if not block or any(not str(block.get(key) or "").strip() for key in required):
                return "reject", "incomplete theory_hypothesis", None
            if "falsification_link" not in block:
                return "reject", "theory_hypothesis lacks falsification_link", None
            return "pass", "theory hypothesis contract valid", None
        if stage == "statistician":
            prediction = output.get("prediction")
            if isinstance(prediction, dict):
                if (not isinstance(prediction.get("metrics"), dict) or not prediction["metrics"]
                        or not str(prediction.get("basis") or "").strip()
                        or not str(prediction.get("locked_at") or "").strip()):
                    return "reject", "incomplete locked prediction", None
                return "pass", "prediction contract valid", None
            actual = output.get("actual_metrics")
            surprise = output.get("surprise")
            robustness = output.get("robustness_verdict")
            if not isinstance(actual, dict) or not actual or not isinstance(surprise, dict) or not isinstance(robustness, dict):
                return "reject", "missing statistician prediction or completed assessment", None
            if not isinstance(surprise.get("per_metric"), dict) or "max_surprise_score" not in surprise or not surprise.get("surprise_metric"):
                return "reject", "incomplete surprise assessment", None
            return "pass", "statistical assessment contract valid", None
        if stage == "research_historian":
            assessment = output.get("info_gain_assessment")
            transitions = output.get("open_questions")
            evolution = output.get("research_evolution_update")
            if not isinstance(assessment, dict) or any(
                key not in assessment for key in ("novelty", "significance", "info_gain_score", "rationale")
            ):
                return "reject", "incomplete info_gain_assessment", None
            if not isinstance(transitions, list) or evolution is None:
                return "reject", "historian output lacks open_questions/research_evolution_update", None
            return "pass", "historian contract valid", None
        return "pass", "", None

    @staticmethod
    def _normalize_source_supported(value: Any) -> list[str]:
        """Extract stable 64-hex source ids from model-formatted support lists."""
        found: list[str] = []
        seen: set[str] = set()

        def visit(item: Any) -> None:
            if item is None:
                return
            if isinstance(item, dict):
                for key, val in item.items():
                    visit(key)
                    visit(val)
                return
            if isinstance(item, (list, tuple, set)):
                for subitem in item:
                    visit(subitem)
                return
            text = str(item).strip()
            matched = False
            for match in re.finditer(r"\b[0-9a-fA-F]{64}\b", text):
                matched = True
                source_id = match.group(0).lower()
                if source_id not in seen:
                    seen.add(source_id)
                    found.append(source_id)
            if not matched and re.fullmatch(r"[A-Za-z0-9_-]+", text):
                if text not in seen:
                    seen.add(text)
                    found.append(text)

        visit(value)
        return found

    @staticmethod
    def _unsupported_factor_data_requirements(factors: list[dict]) -> list[str]:
        """Reject data modalities not present in the current research indicator cache."""
        forbidden_markers = (
            "l2", "level2", "tick", "orderbook", "order_book",
            "bid_depth", "ask_depth", "auction", "minute", "1min",
            "big_order", "signal_trigger_timestamp", "future", "lookahead",
            "look-ahead", "realized", "return_pct", "outcome",
            "post-signal", "post signal", "post-entry", "post entry",
            "after signal", "after entry", "after trigger", "holding-period",
            "centered on signal", "centered on the signal",
            "around signal day", "around each signal", "around the signal",
            "entry_date high", "entry_date low", "entry_date close",
            "entry day high", "entry day low", "entry day close",
            "entry_high", "entry_low", "entry_close",
            "t+1 high", "t+1 low", "t+1 close", "t1 high", "t1 low", "t1 close",
            "same-day high", "same-day low", "same-day close", "intraday",
            "exit_date", "exit return", "future return", "future_ret",
            "未来", "事后", "信号日后", "触发日后", "signal day后",
            "入场后", "实现收益", "实际收益", "收益标签",
            "以信号日为中心", "围绕信号日",
        )
        unsupported: list[str] = []
        for factor in factors:
            name = str(factor.get("name") or "<unnamed>")
            for requirement in factor.get("data_requirements") or []:
                text = str(requirement).lower()
                # A boundary note such as "no future data" is evidence of
                # compliance, not a request for a forbidden future field.
                boundary_text = re.sub(
                    r"\b(?:no|without(?:\s+any)?)\s+(?:future\s+data|look-?ahead)\b",
                    "",
                    text,
                )
                has_future_window = bool(re.search(r"后\s*\d+\s*[日天d]", str(requirement)))
                if any(marker in boundary_text for marker in forbidden_markers) or has_future_window:
                    unsupported.append(f"{name}: {requirement}")
        return unsupported

    @staticmethod
    def _recover_scattered_source_boundary(stage: str, output: dict, last_outputs: dict) -> None:
        """Recover downstream source_boundary fields scattered by Markdown/YAML drift."""
        if not isinstance(output, dict) or isinstance(output.get("source_boundary"), dict):
            return
        if stage == "alpha_hunter":
            top_level_brief = output.get("source_brief_id") or output.get("brief_id")
            top_level_sources = output.get("source_supported")
            top_level_channel = output.get("research_channel")
            if top_level_brief or top_level_sources or top_level_channel:
                channel = Orchestrator._clean_markdown_field(
                    "research_channel", str(top_level_channel or "kbase_inspired")
                )
                output["source_boundary"] = {
                    "research_channel": channel,
                    "source_brief_id": top_level_brief,
                    "source_supported": top_level_sources or [],
                    "agent_inference": output.get("agent_inference"),
                }
                return
            output["source_boundary"] = {
                "research_channel": "independent",
                "source_brief_id": None,
                "source_supported": [],
                "agent_inference": (
                    "source_boundary inserted by orchestrator because Alpha Hunter "
                    "made no explicit source support claim"
                ),
            }
            return
        if stage == "falsification_officer":
            upstream = (last_outputs.get("alpha_hunter") or {}).get("source_boundary") or {}
        elif stage == "factor_engineer":
            upstream = (last_outputs.get("falsification_officer") or {}).get("source_boundary") or {}
        else:
            return
        if not isinstance(upstream, dict) or not upstream:
            return
        boundary = dict(upstream)
        top_level_brief = output.get("source_brief_id") or output.get("brief_id")
        if top_level_brief:
            boundary["source_brief_id"] = top_level_brief
        top_level_sources = output.get("source_supported")
        if top_level_sources:
            boundary["source_supported"] = top_level_sources
        top_level_channel = output.get("research_channel")
        if top_level_channel:
            boundary["research_channel"] = Orchestrator._clean_markdown_field(
                "research_channel", str(top_level_channel)
            )
        top_level_inference = output.get("agent_inference")
        if top_level_inference:
            boundary["agent_inference"] = top_level_inference
        output["source_boundary"] = boundary

    @staticmethod
    def _public_stage_output(output: dict[str, Any]) -> dict[str, Any]:
        """Remove controller-owned audit fields before an output reaches another agent."""
        return {
            key: value for key, value in output.items() if not str(key).startswith("_")
        }

    @staticmethod
    def _normalize_source_brief_payload(payload: dict) -> dict:
        """Normalize source_brief syntax without inventing source evidence."""
        if not isinstance(payload, dict):
            return payload
        normalized = dict(payload)
        for source in normalized.get("sources_consulted", []) or []:
            if isinstance(source, dict):
                date_value = source.get("date")
                if hasattr(date_value, "isoformat"):
                    source["date"] = date_value.isoformat()
        consulted_ids = [
            str(source.get("source_id"))
            for source in normalized.get("sources_consulted", []) or []
            if isinstance(source, dict) and source.get("source_id")
        ]
        for source in normalized.get("sources_consulted", []) or []:
            if not isinstance(source, dict):
                continue
            expanded_refs = []
            for evidence_ref in source.get("evidence_refs", []) or []:
                ref = str(evidence_ref)
                if "#" in ref:
                    prefix, suffix = ref.split("#", 1)
                    prefix = prefix.replace("…", "").rstrip(".")
                    if len(prefix) < 64:
                        matches = [source_id for source_id in consulted_ids if source_id.startswith(prefix)]
                        if len(matches) == 1:
                            ref = f"{matches[0]}#{suffix}"
                expanded_refs.append(ref)
            source["evidence_refs"] = expanded_refs
        return normalized

    # ---- Runtime v1.0: stage invocation ------------------------------------

    @staticmethod
    def _tool_round_limit(stage: str) -> int:
        """Return a bounded tool budget matched to each stage's audit workload."""
        return {
            "source_librarian": 40,
            "alpha_hunter": 24,
        }.get(stage, 8)

    @staticmethod
    def _context_role(stage: str) -> str:
        return {
            "source_librarian": "source_librarian",
            "alpha_hunter": "alpha_hunter",
            "research_proposer": "alpha_hunter",
            "falsification_officer": "falsification_officer",
            "risk_controller": "falsification_officer",
            "data_validator": "falsification_officer",
            "statistician": "falsification_officer",
        }.get(stage, "factor_engineer")

    def _prepare_v342_agent_context(
        self,
        stages: list[str],
        research_context: str,
        *,
        llm_config: dict | None = None,
        tokenizer_names: dict[str, str] | None = None,
    ) -> tuple[dict[str, str], dict[str, list[dict[str, str]]]]:
        from research_automation.control_plane.memory import (
            CommittedLearningLedgerReader,
            LearningContextRouter,
        )

        trusted_contexts: dict[str, str] = {}
        untrusted_contexts: dict[str, list[dict[str, str]]] = {}
        committed_claims = CommittedLearningLedgerReader(
            Path(__file__).resolve().parent.parent
        ).read_claims()
        sources = (
            [{"source_ref": "legacy-research-context", "content": research_context}]
            if research_context
            else None
        )
        for stage in stages:
            tokenizer_name = (
                (tokenizer_names or {}).get(stage)
                or self._llm_config_model_name(llm_config)
                or self._configured_stage_model_name(stage)
            )
            context_router = (
                LearningContextRouter(
                    tokenizer_kind="AG2", tokenizer_name=tokenizer_name
                )
                if tokenizer_name is not None
                and self._has_exact_tokenizer(tokenizer_name)
                else LearningContextRouter()
            )
            context_messages = context_router.build_messages(
                committed_claims,
                role=self._context_role(stage),
                untrusted_sources=sources,
            )
            if context_messages["status"] != "OK":
                raise RuntimeError(
                    f"V3.4 learning context unavailable for stage '{stage}': "
                    f"{context_messages['status']}"
                )
            trusted_contexts[stage] = context_messages["system_message"]["content"]
            untrusted_contexts[stage] = context_messages["untrusted_messages"]
        return trusted_contexts, untrusted_contexts

    @staticmethod
    def _llm_config_model_name(llm_config: object) -> str | None:
        if not isinstance(llm_config, dict):
            return None
        config_list = llm_config.get("config_list")
        if not isinstance(config_list, list) or not config_list:
            return None
        first = config_list[0]
        if not isinstance(first, dict):
            return None
        model = first.get("model")
        if not isinstance(model, str) or not model or model != model.strip():
            return None
        return model

    def _configured_stage_model_name(self, stage: str) -> str | None:
        try:
            llm_config = self.config.get_agent_llm_config(stage)
        except (KeyError, TypeError, ValueError):
            return None
        return self._llm_config_model_name(llm_config)

    @staticmethod
    def _has_exact_tokenizer(model_name: str) -> bool:
        import tiktoken

        try:
            tiktoken.encoding_for_model(model_name)
        except KeyError:
            return False
        return True

    def _build_sequential_invoker(
        self,
        stages: list[str],
        research_context: str,
        llm_config: dict | None,
    ) -> Callable:
        """Default invoker: one bounded tool-capable conversation per stage agent."""
        # Preserve per-agent model routing unless the caller explicitly asks
        # for a workflow-wide override.  The orchestrator default is for
        # manager duties, not an implicit override for every specialist.
        trusted_contexts, untrusted_contexts = self._prepare_v342_agent_context(
            stages, research_context, llm_config=llm_config
        )
        agents = create_agents(self.config, stages, llm_config, trusted_contexts)
        id_to_name = {sid: (self.config.get_agent(sid) or {}).get("name") for sid in stages}
        source_tool_audit: list[dict[str, Any]] = []
        source_conversation_history: list[dict[str, Any]] = []

        def invoker(stage: str, packet: dict, last_outputs: dict, topic: str) -> dict:
            agent = agents.get(id_to_name.get(stage))
            if agent is None:
                return {"_raw": "", "error": f"agent for stage '{stage}' not created"}
            message = self._build_stage_message(stage, packet, last_outputs, topic)
            try:
                reply = self._generate_reply_with_tools(
                    agent,
                    message,
                    max_tool_rounds=self._tool_round_limit(stage),
                    tool_audit=source_tool_audit if stage == "source_librarian" else None,
                    conversation_history=(
                        source_conversation_history if stage == "source_librarian" else None
                    ),
                    initial_messages=untrusted_contexts[stage],
                )
            except Exception as exc:
                output = {
                    "_raw": "",
                    "error": f"stage '{stage}' reply failed: {type(exc).__name__}: {exc}",
                }
            else:
                text = reply if isinstance(reply, str) else (reply or {}).get("content", "")
                output = self._parse_stage_output(stage, text)
            if stage == "source_librarian":
                from .kbase.citation_inventory import build_citation_inventory

                output["_citation_inventory"] = build_citation_inventory(source_tool_audit)
            return output

        return invoker

    @staticmethod
    def _generate_reply_with_tools(agent: Any, message: str,
                                   max_tool_rounds: int = 8,
                                   tool_audit: list[dict[str, Any]] | None = None,
                                   conversation_history: list[dict[str, Any]] | None = None,
                                   initial_messages: list[dict[str, str]] | None = None,
                                   ) -> str | dict | None:
        """Run one stage through AG2's tool-call cycle with a hard safety bound.

        ``ConversableAgent.generate_reply`` executes tools only when a tool-call
        message is present at the end of the supplied history.  A single call
        therefore returns the request without executing it.  Keep the complete
        local history until the model produces a final assistant response.
        """
        if max_tool_rounds < 1:
            raise ValueError("max_tool_rounds must be at least 1")

        messages = conversation_history if conversation_history is not None else []
        if not messages and initial_messages:
            messages.extend(dict(item) for item in initial_messages)
        messages.append({"role": "user", "content": message})
        tool_rounds = 0
        pending_tool_calls: dict[str, dict[str, Any]] = {}
        while True:
            reply = agent.generate_reply(messages=messages)
            if reply is None:
                return reply
            if isinstance(reply, str):
                messages.append({"role": "assistant", "content": reply})
                return reply
            if not isinstance(reply, dict):
                raise TypeError(f"unsupported AG2 reply type: {type(reply).__name__}")

            normalized = dict(reply)
            role = normalized.get("role")
            has_tool_request = bool(
                normalized.get("tool_calls") or normalized.get("function_call")
            )
            is_tool_result = role in ("tool", "function") or bool(
                normalized.get("tool_responses")
            )

            if has_tool_request:
                tool_rounds += 1
                if tool_rounds > max_tool_rounds:
                    raise RuntimeError(
                        f"tool-call round limit exceeded ({max_tool_rounds})"
                    )
                normalized.setdefault("role", "assistant")
                tool_calls = normalized.get("tool_calls") or []
                if normalized.get("function_call"):
                    tool_calls = list(tool_calls) + [{
                        "id": f"legacy-tool-call-{tool_rounds}",
                        "function": normalized["function_call"],
                    }]
                for index, tool_call in enumerate(tool_calls):
                    if not isinstance(tool_call, dict):
                        continue
                    function = tool_call.get("function") if isinstance(tool_call.get("function"), dict) else {}
                    call_id = str(tool_call.get("id") or f"tool-call-{tool_rounds}-{index}")
                    raw_arguments = function.get("arguments")
                    if isinstance(raw_arguments, str):
                        try:
                            arguments = json.loads(raw_arguments)
                        except json.JSONDecodeError:
                            arguments = {}
                    else:
                        arguments = raw_arguments if isinstance(raw_arguments, dict) else {}
                    pending_tool_calls[call_id] = {
                        "tool_name": str(function.get("name") or tool_call.get("name") or ""),
                        "arguments": arguments,
                    }
                messages.append(normalized)
                continue

            if is_tool_result:
                if tool_audit is not None:
                    from .kbase.citation_inventory import summarize_tool_exchange

                    responses = normalized.get("tool_responses")
                    if not isinstance(responses, list) or not responses:
                        responses = [normalized]
                    for response in responses:
                        if not isinstance(response, dict):
                            continue
                        call_id = str(
                            response.get("tool_call_id")
                            or normalized.get("tool_call_id")
                            or ""
                        )
                        pending = pending_tool_calls.pop(call_id, None)
                        if pending is None and len(pending_tool_calls) == 1:
                            _, pending = pending_tool_calls.popitem()
                        pending = pending or {}
                        event = summarize_tool_exchange(
                            sequence=len(tool_audit) + 1,
                            tool_call_id=call_id or None,
                            tool_name=str(pending.get("tool_name") or response.get("name") or ""),
                            arguments=pending.get("arguments"),
                            content=response.get("content", normalized.get("content")),
                        )
                        if event is not None:
                            tool_audit.append(event)
                messages.append(normalized)
                continue

            messages.append(normalized)
            return reply

    def _build_stage_message(self, stage: str, packet: dict, last_outputs: dict, topic: str) -> str:
        revision_context = last_outputs.get("__controller_revision__")
        if (
            stage == "source_librarian"
            and isinstance(revision_context, dict)
            and revision_context.get("stage") == stage
        ):
            return "\n".join((
                "SOURCE_BRIEF_REVISION_ONLY",
                f"Objective: {topic}",
                "Your prior tool evidence and draft remain in this conversation. Do not restart "
                "catalog research. Call a tool only when the controller issue explicitly says a "
                "source was not opened or not traced. Otherwise, rewrite the complete source_brief "
                "from the existing evidence and exact citation inventory.",
                "CONTROLLER_REVISION_REQUIRED:",
                yaml.safe_dump(revision_context, allow_unicode=True, sort_keys=False),
                self._stage_output_instruction(stage),
            ))
        parts = [
            f"Sequential Research OS pipeline. You are the '{stage}' role.",
            f"Objective: {topic}",
            "Use only the V3.4 trusted learning system context and explicit upstream "
            "stage outputs; do not read legacy memory files directly.",
        ]
        public_outputs = {
            key: self._public_stage_output(value) if isinstance(value, dict) else value
            for key, value in last_outputs.items()
            if not str(key).startswith("__")
        }
        if public_outputs:
            parts += ["Upstream stage outputs:",
                      yaml.safe_dump(public_outputs, allow_unicode=True, sort_keys=False)]
        revision_context = last_outputs.get("__controller_revision__")
        if isinstance(revision_context, dict) and revision_context.get("stage") == stage:
            parts += [
                "CONTROLLER_REVISION_REQUIRED:",
                "The prior output failed a deterministic gate. Follow this controller-owned "
                "record exactly; do not silently drop or substitute a citation.",
                yaml.safe_dump(revision_context, allow_unicode=True, sort_keys=False),
            ]
        if stage == "alpha_hunter":
            falsification = last_outputs.get("falsification_officer") or {}
            if str(falsification.get("verdict") or "").upper() == "REVISE":
                parts += [
                    "REVISION REQUIRED:",
                    "Falsification_Officer returned REVISE. You must materially apply "
                    "revision_guidance below, preserve the exact source_boundary, and "
                    "return a revised alpha_family_gap/proposed_generator. Do not repeat "
                    "the previous proposed_generator unchanged.",
                    yaml.safe_dump({
                        "revision_guidance": falsification.get("revision_guidance"),
                        "failure_conditions": falsification.get("failure_conditions"),
                    }, allow_unicode=True, sort_keys=False),
                ]
        elif stage == "falsification_officer":
            alpha = last_outputs.get("alpha_hunter") or {}
            generator = alpha.get("proposed_generator") or {}
            if isinstance(generator, dict) and generator:
                parts += [
                    "CURRENT_ALPHA_BINDING:",
                    "Your alpha_mechanism MUST exactly equal this latest Alpha Hunter "
                    "family/mechanism pair. Ignore older alpha_hunter__N records for "
                    "alpha_mechanism binding.",
                    "Copy this mapping verbatim into alpha_mechanism. Put any formulas, "
                    "elaboration, objections, or sharper wording only in counter_hypothesis, "
                    "decisive_test, failure_conditions, or revision_guidance.",
                    yaml.safe_dump({
                        "alpha_mechanism": {
                            "family": generator.get("family"),
                            "mechanism": generator.get("mechanism"),
                        }
                    }, allow_unicode=True, sort_keys=False),
                ]
        parts.append(self._stage_output_instruction(stage))
        return "\n".join(parts)

    @staticmethod
    def _stage_output_instruction(stage: str) -> str:
        return {
            "source_librarian": (
                "Output only one complete source_brief YAML object, no process notes, "
                "no markdown headings, no prose before or after. Required top-level "
                "fields: brief_id, catalog_version, research_gap, sources_consulted, "
                "source_observations, disagreements_and_limits, missing_evidence, "
                "agent_inference_boundary, handoff_questions. source_observations "
                "must be a non-empty list of source-only observations from consulted "
                "sources. There is no fixed source count: follow distinct evidence and "
                "independent voices, deduplicate semantic repeats, and stop only when "
                "additional browsing no longer adds material information. Use quoted "
                "strings or YAML block scalars for any "
                "source_says, context, or research_gap value containing ':' or other "
                "punctuation. Copy every source_id and evidence_ref exactly from KBase "
                "tool results. A source must be successfully opened with kbase_open before "
                "it may appear in sources_consulted, and every consulted source must also "
                "pass kbase_trace. Include one complete source_observation with non-empty "
                "context for every consulted source, plus explicit limits and downstream "
                "handoff questions. Never splice, autocomplete, reconstruct, or retype an "
                "id from memory. No project inference fields."
            ),
            "alpha_hunter": (
                "Tool calls are not a final answer. After any kb_lookup/kb_validate_proposal call, "
                "continue and output exactly one YAML object with these top-level keys: "
                "source_boundary, alpha_family_gap, kbase_bias_check, proposed_generator. "
                "Do not output a preliminary proposal only. Do not omit alpha_family_gap or "
                "proposed_generator. kbase_inspired requires the upstream source_brief id, cited "
                "source ids, and a bias check proving the direction was not chosen merely because "
                "KBase has more material there. proposed_generator.required_data must use only "
                "pre-trade information available on or before the signal day, or explicitly "
                "audited next-open entry fields. For Brick V2, those audited fields are only "
                "overnight_gap_pct, entry_open_to_yellow_pct, and entry_open_to_ma5_pct under "
                "daily_select.py semantics: entry_date open versus signal-day known close/yellow/MA5. "
                "Never use entry_date high/low/close, intraday future data, T+1 close-derived "
                "MA/yellow, post-signal windows, post-entry windows, centered/around-signal "
                "windows, future returns, realized outcomes, labels, or exit results. If a lookback window is needed, state that it "
                "ends at the signal day. Minimal skeleton:\n"
                "source_boundary:\n"
                "  research_channel: independent\n"
                "  source_brief_id: null\n"
                "  source_supported: []\n"
                "  agent_inference: <project-side inference only>\n"
                "alpha_family_gap:\n"
                "  existing_families: [<...>]\n"
                "  missing_families: [<...>]\n"
                "  highest_potential: <one family and rationale>\n"
                "kbase_bias_check:\n"
                "  source_density_bias: low\n"
                "  why_not_source_abundance: <...>\n"
                "  underexplored_alternative_considered: <...>\n"
                "  novelty_or_reopen_reason: <...>\n"
                "proposed_generator:\n"
                "  family: <...>\n"
                "  mechanism: <one sentence>\n"
                "  required_data: <data source and availability>\n"
                "  expected_jaccard_vs_wave_qualified: low\n"
                "  expected_information_gain: <...>"
            ),
            "falsification_officer": (
                "Tool calls are not a final answer. Output exactly one YAML object. "
                "Copy CURRENT_ALPHA_BINDING.alpha_mechanism verbatim; do not paraphrase, "
                "expand, formula-ize, translate, or normalize it. Preserve source_boundary "
                "exactly from Alpha Hunter. Put all critique and formulas outside "
                "alpha_mechanism. Do not output a 'Current Alpha Binding' section, "
                "placeholder values, or a partial skeleton; every field below must "
                "be filled with substantive content. If the current alpha is executable, "
                "uses only available pre-signal data, has a clear decisive test, and has "
                "materially applied your prior revision guidance, choose PROCEED and put "
                "remaining skepticism in counter_hypothesis and failure_conditions. Do not "
                "return REVISE only to make the mechanism simpler, more parsimonious, or "
                "stylistically cleaner; REVISE is only for a new blocking defect such as "
                "leakage, unavailable data, ambiguity that prevents implementation, or a "
                "missing discriminating test. Required skeleton:\n"
                "source_boundary:\n"
                "  research_channel: <exact upstream value>\n"
                "  source_brief_id: <exact upstream value>\n"
                "  source_supported: <exact upstream list>\n"
                "  agent_inference: <exact upstream value>\n"
                "alpha_mechanism:\n"
                "  family: <verbatim CURRENT_ALPHA_BINDING value>\n"
                "  mechanism: <verbatim CURRENT_ALPHA_BINDING value>\n"
                "counter_hypothesis: <substantive alternative explanation>\n"
                "decisive_test:\n"
                "  method: parameter_sweep | code_change | data_extension | manual_audit\n"
                "  discriminating_observation: <what separates alpha from counter>\n"
                "  expected_if_alpha_holds: <observable outcome>\n"
                "  expected_if_counter_holds: <different observable outcome>\n"
                "failure_conditions: [<condition>, <condition>]\n"
                "verdict: PROCEED | REVISE | REJECT\n"
                "revision_guidance: <required when verdict is REVISE; optional otherwise>"
            ),
            "factor_engineer": (
                "Output source_boundary, falsification_consumed, and exactly one of: "
                "factor_batch (for new inference factors) OR research_mechanism (for no-new-factor "
                "training-objective, label, weighting, abstention, or execution-mechanism changes). "
                "You may proceed only from the upstream PROCEED verdict and must preserve its exact "
                "review fields. factor_batch.data_requirements must name only pre-trade "
                "fields available on or before the signal day, or explicitly audited "
                "next-open entry fields. research_mechanism must include name, family, mechanism, "
                "runner_id, validation_plan, and stop_conditions. For Brick V2, the only audited "
                "next-open fields are overnight_gap_pct, entry_open_to_yellow_pct, and "
                "entry_open_to_ma5_pct under daily_select.py semantics: entry_date open versus "
                "signal-day known close/yellow/MA5. entry_date high/low/close, intraday future "
                "data, T+1 close-derived MA/yellow, post-signal/post-entry windows, future "
                "returns, realized outcomes used as inference features, exit results used as "
                "inference features, and centered/around-signal windows are invalid. Labels and "
                "hold_days may be used only inside train folds as labels/sample weights and must "
                "not enter inference features. Any rolling window must explicitly end at the signal day. "
                "When kb_validate_proposal returns needs_evidence, add the missing measurement plan "
                "before final output or reject the proposal; do not treat needs_evidence as approval."
            ),
            "theory_builder": (
                "Output exactly one YAML object with one top-level key, theory_hypothesis. "
                "Required fields are mechanism, expected_market, failure_mode, "
                "observable_signature, and falsification_link. Explain a causal mechanism "
                "for the current strategy or observed result; do not propose parameter "
                "changes, factor formulas, code edits, or backtests. Set falsification_link "
                "to an existing open-question id or null."
            ),
            "research_proposer": "Output one proposal YAML block matching your system contract, including hypothesis and experiment_spec. Do NOT classify the registry.",
            "data_validator": "Output one data_verdict YAML block matching your system contract, including forward_validation_design for the fixed Phase 6 rolling folds and the Brick V2 entry-open boundary when applicable.",
            "experiment_executor": "Output one execution_record YAML block matching your system contract, including forward_validation with the fixed Phase 6 folds, compute_acceleration GPU/CPU handling, indicator_cache=research_indicators_cache unless explicitly authorized for production reproduction, script_boundary for backtest_brick_v2.py vs backtest_brick_v2_research.py, and test_summary average/worst/pass_rate/dispersion.",
            "risk_controller": "Output one risk_verdict YAML block matching your system contract, including forward_validation_risk. Do NOT re-judge leakage.",
            "strategy_synthesizer": "Output a 'synthesis' block with registry_entry_delta / snapshot_delta / handoff_delta.",
        }.get(stage, "Output a structured result for your role.")

    @staticmethod
    def _parse_stage_output(stage: str, text: str) -> dict:
        """Best-effort structured parse of an agent reply."""
        out: dict[str, Any] = {"_raw": text}
        if not text:
            return out
        # Models commonly wrap otherwise valid YAML/JSON in Markdown fences,
        # sometimes with a short sentence before or after the block. Try the
        # narrowest structured candidates before falling back to regex crumbs.
        structured_candidates = [text.strip()]
        structured_candidates.extend(
            match.group(1).strip()
            for match in re.finditer(
                r"```(?:yaml|yml|json)?\s*\n?(.*?)\n?```",
                text,
                flags=re.IGNORECASE | re.DOTALL,
            )
        )
        for marker in (
            "brief_id:",
            "source_boundary:",
            "factor_batch:",
            "research_mechanism:",
            "experiment_spec:",
            "data_verdict:",
            "alpha_mechanism:",
            "synthesis:",
        ):
            index = text.find(marker)
            if index >= 0:
                structured_candidates.append(text[index:].strip())

        # YAML is a superset of JSON, so one safe loader handles both.
        for structured_text in structured_candidates:
            for candidate in (
                structured_text,
                Orchestrator._quote_scalar_yaml_bullets(structured_text),
                Orchestrator._quote_source_brief_yaml_scalars(structured_text),
                Orchestrator._quote_source_brief_yaml_scalars(
                    Orchestrator._quote_scalar_yaml_bullets(structured_text)
                ),
            ):
                try:
                    data = yaml.safe_load(candidate)
                    if isinstance(data, dict):
                        out.update(data)
                        break
                except Exception:
                    continue
            if len(out) > 1:
                break
        markdown_sections = Orchestrator._parse_markdown_section_output(text)
        for key, value in markdown_sections.items():
            out.setdefault(key, value)
        if stage == "falsification_officer":
            for key in (
                "counter_hypothesis",
                "decisive_test",
                "failure_conditions",
                "verdict",
                "revision_guidance",
            ):
                value = out.get(key)
                top_level_occurrences = len(re.findall(
                    rf"(?m)^{re.escape(key)}:\s*",
                    text,
                ))
                needs_recovery = key not in out or top_level_occurrences > 1
                if key == "decisive_test":
                    required = (
                        "method",
                        "discriminating_observation",
                        "expected_if_alpha_holds",
                        "expected_if_counter_holds",
                    )
                    needs_recovery = needs_recovery or not isinstance(value, dict) or any(
                        not str(value.get(field) or "").strip() for field in required
                    )
                elif key == "failure_conditions":
                    needs_recovery = needs_recovery or not isinstance(value, list) or not any(
                        str(item).strip() for item in value
                    )
                elif key == "verdict":
                    needs_recovery = needs_recovery or str(value or "").strip(" :").upper() not in {
                        "PROCEED", "REVISE", "REJECT",
                    }
                else:
                    needs_recovery = needs_recovery or not str(value or "").strip(" :")
                if needs_recovery:
                    if key == "decisive_test":
                        extracted = Orchestrator._extract_loose_mapping_field(
                            text,
                            key,
                            required,
                        )
                        if not extracted:
                            extracted = Orchestrator._extract_loose_top_level_field(text, key)
                    else:
                        extracted = Orchestrator._extract_loose_top_level_field(text, key)
                    if extracted is not None:
                        out[key] = extracted
        if stage == "alpha_hunter":
            for key in (
                "source_boundary",
                "alpha_family_gap",
                "kbase_bias_check",
                "proposed_generator",
            ):
                if key not in out:
                    extracted = Orchestrator._extract_loose_top_level_field(text, key)
                    if extracted is not None:
                        out[key] = extracted
        if stage == "factor_engineer":
            consumed_fields = (
                "verdict",
                "counter_hypothesis",
                "decisive_test",
                "failure_conditions",
            )
            recovered_consumed = Orchestrator._extract_loose_mapping_field(
                text,
                "falsification_consumed",
                consumed_fields,
                list_fields={"failure_conditions"},
            )
            if recovered_consumed:
                out["_falsification_consumed_declared"] = (
                    str(recovered_consumed.get("verdict") or "").strip().upper()
                    == "PROCEED"
                )
                consumed = out.get("falsification_consumed")
                if not isinstance(consumed, dict) or any(
                    key not in consumed for key in consumed_fields
                ):
                    merged = dict(consumed) if isinstance(consumed, dict) else {}
                    merged.update(recovered_consumed)
                    out["falsification_consumed"] = merged

            mechanism_fields = (
                "name",
                "family",
                "mechanism",
                "runner_id",
                "validation_plan",
                "stop_conditions",
            )
            research_mechanism = out.get("research_mechanism")
            if not isinstance(research_mechanism, dict) or any(
                key not in research_mechanism for key in mechanism_fields
            ):
                recovered_mechanism = Orchestrator._extract_loose_mapping_field(
                    text,
                    "research_mechanism",
                    mechanism_fields,
                    list_fields={"validation_plan", "stop_conditions"},
                )
                if recovered_mechanism:
                    merged = (
                        dict(research_mechanism)
                        if isinstance(research_mechanism, dict)
                        else {}
                    )
                    merged.update(recovered_mechanism)
                    out["research_mechanism"] = merged
        m = re.search(r"raw_hypothesis\s*[:=]\s*(.+)", text, re.IGNORECASE)
        if m and "raw_hypothesis" not in out:
            out["raw_hypothesis"] = m.group(1).strip()
        m = re.search(r"verdict\s*[:=]\s*(PASS|FAIL|VALID|INVALID|INCONCLUSIVE)", text, re.IGNORECASE)
        if m:
            out["verdict"] = m.group(1).upper()
        m = re.search(r"anomaly_flag\s*[:=]\s*([^\n]+)", text, re.IGNORECASE)
        if m:
            out["anomaly_flag"] = m.group(1).strip()
        return out

    @staticmethod
    def _extract_loose_top_level_field(text: str, key: str) -> Any:
        """Extract one top-level YAML-like field from malformed model output."""
        top_level_keys = (
            "source_boundary", "alpha_family_gap", "kbase_bias_check",
            "proposed_generator", "alpha_mechanism", "counter_hypothesis",
            "decisive_test", "failure_conditions", "verdict",
            "revision_guidance", "factor_batch", "research_mechanism",
            "falsification_consumed",
        )
        next_keys = "|".join(re.escape(item) for item in top_level_keys if item != key)
        pattern = rf"(?ms)^{re.escape(key)}:[^\S\r\n]*(.*?)(?=^(?:{next_keys}):\s|\Z)"
        matches = list(re.finditer(pattern, text))
        if not matches:
            return None
        fallback_body = ""
        for match in reversed(matches):
            body = "\n".join(
                line for line in match.group(1).splitlines()
                if not line.strip().startswith("```")
            ).rstrip()
            if not fallback_body:
                fallback_body = body
            snippet = f"{key}: {body}\n"
            for candidate in (snippet, Orchestrator._quote_scalar_yaml_bullets(snippet)):
                try:
                    parsed = yaml.safe_load(candidate)
                except Exception:
                    continue
                if isinstance(parsed, dict) and key in parsed:
                    return parsed[key]
            if key == "failure_conditions":
                stripped = body.strip()
                if stripped.startswith("[") and stripped.endswith("]"):
                    inner = stripped[1:-1].strip()
                    items = [item.strip().strip("'\"") for item in inner.split(",") if item.strip()]
                else:
                    items = [
                        Orchestrator._clean_markdown_value(line.strip()[2:])
                        for line in body.splitlines()
                        if line.strip().startswith("- ")
                    ]
                if items:
                    return items
        return Orchestrator._clean_markdown_value(fallback_body.strip())

    @staticmethod
    def _extract_loose_mapping_field(
        text: str,
        key: str,
        fields: tuple[str, ...],
        *,
        list_fields: set[str] | None = None,
    ) -> dict[str, Any] | None:
        """Recover a known mapping even when prose or anonymous nested lists break YAML."""
        top_level_keys = (
            "source_boundary", "alpha_family_gap", "kbase_bias_check",
            "proposed_generator", "alpha_mechanism", "counter_hypothesis",
            "decisive_test", "failure_conditions", "verdict",
            "revision_guidance", "factor_batch", "research_mechanism",
            "falsification_consumed",
        )
        next_keys = "|".join(re.escape(item) for item in top_level_keys if item != key)
        pattern = rf"(?ms)^{re.escape(key)}:[^\S\r\n]*(.*?)(?=^(?:{next_keys}):\s|\Z)"
        matches = list(re.finditer(pattern, text))
        list_fields = set(list_fields or ())

        for match in reversed(matches):
            lines = [
                line for line in match.group(1).splitlines()
                if not line.strip().startswith("```")
            ]
            field_pattern = re.compile(
                rf"^(?P<indent>[ \t]+)(?P<field>{'|'.join(map(re.escape, fields))}):"
                rf"[^\S\r\n]*(?P<inline>.*)$"
            )
            locations: list[tuple[int, int, str]] = []
            for index, line in enumerate(lines):
                field_match = field_pattern.match(line)
                if field_match:
                    locations.append((
                        index,
                        len(field_match.group("indent").expandtabs(8)),
                        field_match.group("field"),
                    ))
            if not locations:
                continue

            mapping_indent = min(indent for _, indent, _ in locations)
            locations = [item for item in locations if item[1] == mapping_indent]
            recovered: dict[str, Any] = {}
            for location_index, (start, _, field) in enumerate(locations):
                end = locations[location_index + 1][0] if location_index + 1 < len(locations) else len(lines)
                segment = lines[start:end]
                indent_text = re.match(r"^[ \t]+", segment[0]).group(0)
                snippet = "\n".join(
                    line[len(indent_text):] if line.startswith(indent_text) else line.lstrip()
                    for line in segment
                ).rstrip()
                parsed_value: Any = None
                parsed = False
                for candidate in (snippet, Orchestrator._quote_scalar_yaml_bullets(snippet)):
                    try:
                        document = yaml.safe_load(candidate)
                    except Exception:
                        continue
                    if isinstance(document, dict) and field in document:
                        parsed_value = document[field]
                        parsed = True
                        break

                if field in list_fields and (not parsed or not isinstance(parsed_value, list)):
                    items = []
                    for line in segment[1:]:
                        bullet = re.match(r"^\s*-\s+(.+)$", line)
                        if bullet:
                            items.append(Orchestrator._clean_markdown_value(bullet.group(1).strip()))
                    if items:
                        parsed_value = items
                        parsed = True
                elif not parsed:
                    first_value = segment[0].split(":", 1)[1].strip()
                    continuation = [line.strip() for line in segment[1:] if line.strip()]
                    raw_value = "\n".join([first_value, *continuation]).strip()
                    if raw_value:
                        parsed_value = Orchestrator._clean_markdown_value(raw_value)
                        parsed = True

                if parsed:
                    recovered[field] = parsed_value
            if recovered:
                return recovered
        return None

    @staticmethod
    def _parse_markdown_section_output(text: str) -> dict:
        """Parse simple **section:** blocks emitted instead of YAML."""
        sections: dict[str, Any] = {}
        for match in re.finditer(
            r"(?m)^#{1,6}\s*([A-Za-z0-9_]+)\s*\n+\s*```(?:yaml|yml|json)?\s*\n(.*?)\n```",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            key = match.group(1).strip()
            payload = match.group(2).strip()
            for candidate in (payload, Orchestrator._quote_scalar_yaml_bullets(payload)):
                try:
                    parsed = yaml.safe_load(candidate)
                except Exception:
                    continue
                if parsed is not None:
                    sections[key] = parsed
                    break

        current: str | None = None
        current_key: str | None = None
        for raw_line in text.splitlines():
            line = raw_line.strip().rstrip()
            if not line:
                current_key = None
                continue
            if line.startswith("```"):
                continue
            section = re.fullmatch(r"\*\*([A-Za-z0-9_]+):?\*\*\s*(.*)", line)
            if section:
                current = section.group(1)
                inline = section.group(2).strip()
                sections[current] = Orchestrator._clean_markdown_value(inline) if inline else {}
                current_key = None
                continue
            if current and isinstance(sections.get(current), dict) and line.startswith("- ") and current_key:
                block = sections[current]
                if not isinstance(block.get(current_key), list):
                    block[current_key] = []
                block[current_key].append(Orchestrator._clean_markdown_value(line[2:].strip()))
                continue
            if current and sections.get(current) == {} and line.startswith("- "):
                sections[current] = [Orchestrator._clean_markdown_value(line[2:].strip())]
                current_key = current
                continue
            if current and isinstance(sections.get(current), list) and line.startswith("- "):
                sections[current].append(Orchestrator._clean_markdown_value(line[2:].strip()))
                current_key = current
                continue
            if current == "decisive_test" and sections.get(current) == {} and ":" not in line:
                sections[current] = {"method": Orchestrator._clean_markdown_value(line)}
                current_key = None
                continue
            if current and isinstance(sections.get(current), dict) and ":" in line:
                key, value = line.split(":", 1)
                key = key.strip()
                cleaned = Orchestrator._clean_markdown_field(key, value.strip())
                sections[current][key] = cleaned
                current_key = key if cleaned == "" else None
            elif current and sections.get(current) == {}:
                sections[current] = Orchestrator._clean_markdown_value(line)
        return sections

    @staticmethod
    def _quote_scalar_yaml_bullets(text: str) -> str:
        """Quote scalar list items that contain punctuation confusing YAML."""
        lines: list[str] = []
        for raw_line in text.splitlines():
            match = re.match(r"^(\s*-\s+)(.+)$", raw_line)
            if not match:
                lines.append(raw_line)
                continue
            prefix, value = match.groups()
            stripped = value.strip()
            if re.match(r"^[A-Za-z0-9_]+:\s*", stripped):
                lines.append(raw_line)
                continue
            if stripped in {"[]", "{}"} or stripped.startswith(("{", "[")):
                lines.append(raw_line)
                continue
            quoted = yaml.safe_dump(
                stripped, allow_unicode=True, default_style='"', width=100000
            ).strip()
            lines.append(prefix + quoted)
        return "\n".join(lines)

    @staticmethod
    def _quote_source_brief_yaml_scalars(text: str) -> str:
        """Quote source_brief scalar fields that often contain unescaped colons."""
        scalar_keys = {
            "research_gap",
            "source_says",
            "context",
            "agent_inference_boundary",
        }
        lines: list[str] = []
        pattern = re.compile(r"^(\s*)([A-Za-z_][A-Za-z0-9_]*):\s+(.+)$")
        for raw_line in text.splitlines():
            match = pattern.match(raw_line)
            if not match:
                lines.append(raw_line)
                continue
            indent, key, value = match.groups()
            stripped = value.strip()
            if key not in scalar_keys or stripped.startswith(("|", ">", "'", '"', "{", "[")):
                lines.append(raw_line)
                continue
            quoted = yaml.safe_dump(
                stripped, allow_unicode=True, default_style='"', width=100000
            ).strip()
            lines.append(f"{indent}{key}: {quoted}")
        return "\n".join(lines)

    @staticmethod
    def _clean_markdown_value(value: str) -> Any:
        value = value.rstrip("  ")
        if "__LT__" in value and "__GT__" in value:
            value = value.split("__LT__", 1)[0].strip()
        if "__SEMI__" in value:
            value = value.split("__SEMI__", 1)[0].strip()
        value = value.replace("__LT__", "<").replace("__GT__", ">")
        if value in {"null", "None"}:
            return None
        if value.startswith("[") and value.endswith("]"):
            inner = value[1:-1].strip()
            if not inner:
                return []
            if "__PIPE__" in inner and "," not in inner:
                return [item.strip() for item in inner.split("__PIPE__") if item.strip()]
            return [item.strip().strip("'\"") for item in inner.split(",") if item.strip()]
        if "__PIPE__" in value:
            return value.split("__PIPE__", 1)[0].strip()
        return value.strip("'\"")

    @staticmethod
    def _clean_markdown_field(key: str, value: str) -> Any:
        cleaned = Orchestrator._clean_markdown_value(value)
        if key == "research_channel" and isinstance(cleaned, str) and "|" in cleaned:
            for option in (part.strip().strip("'\"") for part in cleaned.split("|")):
                if option in {"kbase_inspired", "independent"}:
                    return option
        if key == "source_brief_id" and isinstance(cleaned, str):
            cleaned = re.sub(r"\s*\((?:null|none)\s+for\s+independent\)\s*$", "", cleaned, flags=re.IGNORECASE)
        return cleaned

    @staticmethod
    def _result_stage_output(result: dict[str, Any], stage: str) -> dict[str, Any] | None:
        transcript = result.get("transcript", []) if isinstance(result, dict) else []
        for step in reversed(transcript):
            if (
                isinstance(step, dict)
                and step.get("stage") == stage
                and isinstance(step.get("output"), dict)
                and (step.get("gate") or {}).get("decision") == "pass"
            ):
                return Orchestrator._public_stage_output(step["output"])
        for step in reversed(transcript):
            if (
                isinstance(step, dict)
                and step.get("stage") == stage
                and isinstance(step.get("output"), dict)
            ):
                return Orchestrator._public_stage_output(step["output"])
        return None

    def run_source_first_discovery(
        self,
        topic: str,
        workflow_id: str = "kbase_source_first_discovery",
        strategy_id: str = "b1",
        research_context: str = "",
        llm_config: dict | None = None,
        **kwargs,
    ) -> dict:
        """Audit project state and build a KBase brief before mechanism debate."""
        from .project_state import compile_project_state
        from .research_gap import build_research_gap_request

        wf = self.config.get_workflow(workflow_id) or {}
        project_state = compile_project_state(strategy_id, topic)
        gap_request = build_research_gap_request(project_state)
        release_status = str((project_state.get("kbase_release") or {}).get("status") or "")
        if release_status != "READY":
            return {
                "status": "ESCALATE_TO_USER",
                "reason": f"KBase semantic release bundle is not READY: {release_status or 'UNKNOWN'}",
                "workflow_order": "source_first",
                "project_state_packet": project_state,
                "research_gap_request": gap_request,
                "transcript": [],
                "control_decision": {
                    "decision": "ESCALATE_TO_USER",
                    "reason": "catalog, semantic release, gate, and runtime must agree before discovery",
                },
            }

        source_context = "\n".join(part for part in (
            research_context.strip(),
            "PROJECT_STATE_PACKET:",
            yaml.safe_dump(project_state, allow_unicode=True, sort_keys=False),
            "RESEARCH_GAP_REQUEST:",
            yaml.safe_dump(gap_request, allow_unicode=True, sort_keys=False),
            "The Source Librarian must investigate the request above before any agent proposes a mechanism.",
        ) if part)
        brief_workflow = wf.get("source_brief_workflow", "kbase_source_brief")
        brief_result = self.run_sequential_workflow(
            brief_workflow,
            topic=topic,
            strategy_id=strategy_id,
            research_context=source_context,
            llm_config=llm_config,
        )
        brief = self._result_stage_output(brief_result, "source_librarian")
        if str(brief_result.get("status") or "") != "APPROVED" or not brief:
            return {
                "status": brief_result.get("status", "REJECTED"),
                "reason": f"source brief did not pass: {brief_result.get('reason', '')}",
                "workflow_order": "source_first",
                "project_state_packet": project_state,
                "research_gap_request": gap_request,
                "source_brief_result": brief_result,
                "transcript": brief_result.get("transcript", []),
                "control_decision": brief_result.get("control_decision"),
            }

        approved_context = "\n".join((
            source_context,
            "APPROVED_SOURCE_BRIEF:",
            yaml.safe_dump(brief, allow_unicode=True, sort_keys=False),
            "The brief is source material, not a factor proposal. Project-side agents must label every inference beyond it.",
        ))
        rt_cfg = wf.get("roundtable", {}) if isinstance(wf.get("roundtable"), dict) else {}
        roundtable_topic = (
            f"{topic}\n\n"
            "The project audit and approved KBase source brief are already in context. "
            "Now propose distinct project-side mechanisms, challenge source-abundance bias, "
            "data availability, leakage, overfit, and execution risk, and identify candidates "
            "worthy of strict rolling forward validation."
        )
        roundtable_result = self.run_roundtable(
            roundtable_topic,
            research_context=approved_context,
            participants=rt_cfg.get("participants"),
            max_rounds=rt_cfg.get("max_rounds"),
            roundtable_config=rt_cfg,
        )
        if str(roundtable_result.get("status", "")).lower() not in {"completed", "approved"}:
            return {
                "status": "ESCALATE_TO_USER",
                "reason": f"roundtable failed after approved source brief: {roundtable_result.get('message', '')}",
                "workflow_order": "source_first",
                "project_state_packet": project_state,
                "research_gap_request": gap_request,
                "source_brief_result": brief_result,
                "roundtable": roundtable_result,
                "transcript": brief_result.get("transcript", []),
                "control_decision": {
                    "decision": "ESCALATE_TO_USER",
                    "reason": "no downgrade is allowed after roundtable failure",
                },
            }

        log_text = ""
        log_file = roundtable_result.get("log_file")
        if log_file:
            try:
                log_text = Path(log_file).read_text(encoding="utf-8")
            except OSError:
                log_text = ""
        memory_digest = self._build_roundtable_memory_digest(
            topic=topic,
            strategy_id=strategy_id,
            roundtable_result=roundtable_result,
            log_text=log_text,
        )
        try:
            digest_path = self._write_roundtable_memory_digest(memory_digest, strategy_id)
            roundtable_result["memory_digest_path"] = str(digest_path)
        except OSError as error:
            roundtable_result["memory_digest_error"] = f"{type(error).__name__}: {error}"
        roundtable_result["memory_digest"] = memory_digest

        downstream_context = "\n".join((
            approved_context,
            "ROUND_TABLE_MEMORY_DIGEST:",
            yaml.safe_dump(memory_digest, allow_unicode=True, sort_keys=False),
            "ROUND_TABLE_LOG_PATH:",
            str(log_file or ""),
        ))
        factor_workflow = wf.get("factor_workflow", "kbase_factor_handoff")
        factor_result = self.run_sequential_workflow(
            factor_workflow,
            topic=topic,
            strategy_id=strategy_id,
            research_context=downstream_context,
            llm_config=llm_config,
            initial_outputs={"source_librarian": brief},
            memory_packet=brief_result.get("memory_packet"),
            require_kbase_inspired=True,
        )
        transcript = list(brief_result.get("transcript", [])) + list(
            factor_result.get("transcript", [])
        )
        return {
            "status": factor_result.get("status"),
            "reason": factor_result.get("reason"),
            "workflow_order": "source_first",
            "project_state_packet": project_state,
            "research_gap_request": gap_request,
            "source_brief": brief,
            "source_brief_result": brief_result,
            "roundtable": roundtable_result,
            "discovery": factor_result,
            "transcript": transcript,
            "control_decision": factor_result.get("control_decision"),
        }

    def resume_source_first_discovery(
        self,
        handoff_path: str | Path,
        *,
        llm_config: dict | None = None,
    ) -> dict[str, Any]:
        """Resume only the gated factor handoff from a completed source-first checkpoint."""
        checkpoint_path = Path(handoff_path).resolve()
        try:
            document = yaml.safe_load(checkpoint_path.read_text(encoding="utf-8")) or {}
        except (OSError, UnicodeError, yaml.YAMLError) as error:
            return {
                "status": "ESCALATE_TO_USER",
                "reason": f"cannot read discovery checkpoint: {type(error).__name__}: {error}",
                "workflow_order": "source_first_resume",
                "resumed_from": str(checkpoint_path),
                "transcript": [],
            }
        result = document.get("result") if isinstance(document, dict) else None
        if (
            document.get("handoff_type") != "kbase_discovery"
            or not isinstance(result, dict)
            or result.get("workflow_order") != "source_first"
        ):
            return {
                "status": "ESCALATE_TO_USER",
                "reason": "checkpoint is not a source-first KBase discovery handoff",
                "workflow_order": "source_first_resume",
                "resumed_from": str(checkpoint_path),
                "transcript": [],
            }

        strategy_id = str(document.get("strategy_id") or "")
        topic = str(document.get("topic") or "")
        project_state = result.get("project_state_packet") or {}
        gap_request = result.get("research_gap_request") or {}
        brief_result = result.get("source_brief_result") or {}
        roundtable_result = result.get("roundtable") or {}
        coverage = roundtable_result.get("coverage") or {}
        if (
            brief_result.get("status") != "APPROVED"
            or str(roundtable_result.get("status") or "").lower() not in {"completed", "approved"}
            or coverage.get("covered") is not True
        ):
            return {
                "status": "ESCALATE_TO_USER",
                "reason": "checkpoint lacks an approved source brief or complete roundtable coverage",
                "workflow_order": "source_first_resume",
                "resumed_from": str(checkpoint_path),
                "project_state_packet": project_state,
                "research_gap_request": gap_request,
                "source_brief_result": brief_result,
                "roundtable": roundtable_result,
                "transcript": brief_result.get("transcript", []),
            }

        from .kbase.release_bundle import inspect_semantic_release_bundle

        current_release = inspect_semantic_release_bundle()
        expected_release = project_state.get("kbase_release") or {}
        release_mismatches = []
        for key in ("catalog_version", "bundle_fingerprint"):
            expected = str(expected_release.get(key) or "")
            current = str(current_release.get(key) or "")
            if not expected or expected != current:
                release_mismatches.append(f"{key}: checkpoint={expected or 'missing'} current={current or 'missing'}")
        if current_release.get("status") != "READY" or release_mismatches:
            return {
                "status": "ESCALATE_TO_USER",
                "reason": "KBase release changed or is not READY: " + "; ".join(release_mismatches),
                "workflow_order": "source_first_resume",
                "resumed_from": str(checkpoint_path),
                "current_kbase_release": current_release,
                "transcript": brief_result.get("transcript", []),
            }

        source_step = None
        for step in reversed(brief_result.get("transcript", [])):
            if (
                isinstance(step, dict)
                and step.get("stage") == "source_librarian"
                and isinstance(step.get("output"), dict)
                and (step.get("gate") or {}).get("decision") == "pass"
            ):
                source_step = step
                break
        if source_step is None:
            return {
                "status": "ESCALATE_TO_USER",
                "reason": "checkpoint has no passing Source Librarian output",
                "workflow_order": "source_first_resume",
                "resumed_from": str(checkpoint_path),
                "transcript": brief_result.get("transcript", []),
            }
        source_output = dict(source_step["output"])
        source_decision, source_reason, _ = self._gate(
            "source_librarian", source_output, None, {}, {}
        )
        if source_decision != "pass":
            return {
                "status": "ESCALATE_TO_USER",
                "reason": f"archived source brief no longer passes current gates: {source_reason}",
                "workflow_order": "source_first_resume",
                "resumed_from": str(checkpoint_path),
                "transcript": brief_result.get("transcript", []),
            }
        brief = self._public_stage_output(source_output)

        source_context = "\n".join((
            "PROJECT_STATE_PACKET:",
            yaml.safe_dump(project_state, allow_unicode=True, sort_keys=False),
            "RESEARCH_GAP_REQUEST:",
            yaml.safe_dump(gap_request, allow_unicode=True, sort_keys=False),
            "APPROVED_SOURCE_BRIEF:",
            yaml.safe_dump(brief, allow_unicode=True, sort_keys=False),
            "The brief is source material, not a factor proposal. Project-side agents must label every inference beyond it.",
        ))
        memory_digest = roundtable_result.get("memory_digest") or {}
        downstream_context = "\n".join((
            source_context,
            "ROUND_TABLE_MEMORY_DIGEST:",
            yaml.safe_dump(memory_digest, allow_unicode=True, sort_keys=False),
            "ROUND_TABLE_LOG_PATH:",
            str(roundtable_result.get("log_file") or ""),
        ))
        workflow = self.config.get_workflow("kbase_source_first_discovery") or {}
        factor_result = self.run_sequential_workflow(
            workflow.get("factor_workflow", "kbase_factor_handoff"),
            topic=topic,
            strategy_id=strategy_id,
            research_context=downstream_context,
            llm_config=llm_config,
            initial_outputs={"source_librarian": brief},
            memory_packet=brief_result.get("memory_packet"),
            require_kbase_inspired=True,
        )
        transcript = list(brief_result.get("transcript", [])) + list(
            factor_result.get("transcript", [])
        )
        return {
            "status": factor_result.get("status"),
            "reason": factor_result.get("reason"),
            "workflow_order": "source_first",
            "resume_stage": "factor_handoff",
            "resumed_from": str(checkpoint_path),
            "project_state_packet": project_state,
            "research_gap_request": gap_request,
            "source_brief": brief,
            "source_brief_result": brief_result,
            "roundtable": roundtable_result,
            "discovery": factor_result,
            "transcript": transcript,
            "control_decision": factor_result.get("control_decision"),
        }

    # ---- Multi-LLM Roundtable ------------------------------------------------

    def run_roundtable_discovery(
        self,
        topic: str,
        workflow_id: str = "kbase_roundtable_discovery",
        strategy_id: str = "b1",
        research_context: str = "",
        llm_config: dict | None = None,
        **kwargs,
    ) -> dict:
        """Run KBase discovery through a roundtable before the gated handoff.

        The roundtable explores multiple mechanisms; the normal sequential
        KBase workflow then turns the discussion into source-bound, falsified
        factor candidates under the same gates as ``kbase_discovery``.
        """
        wf = self.config.get_workflow(workflow_id) or {}
        rt_cfg = wf.get("roundtable", {}) if isinstance(wf.get("roundtable"), dict) else {}
        context_parts = [research_context] if research_context else []
        try:
            router = LegacyMemoryAdapter(strategy_id)
            packet = router.build_packet(objective=topic)
            context_parts += [
                "memory_packet:",
                yaml.safe_dump(packet, allow_unicode=True, sort_keys=False),
            ]
        except Exception as error:
            context_parts.append(f"memory_packet_unavailable: {type(error).__name__}: {error}")
        try:
            from .knowledge_bridge import build_combined_research_context

            context_parts += [
                "kbase_source_brief_context:",
                build_combined_research_context(strategy_id, query=topic, project_mode="brief"),
            ]
        except Exception as error:
            context_parts.append(f"kbase_context_unavailable: {type(error).__name__}: {error}")

        roundtable_topic = (
            f"{topic}\n\n"
            "Roundtable objective: propose diverse Brick factor mechanisms, challenge each "
            "other for leakage/overfit/source-abundance bias, and vote for candidates that "
            "deserve strict Phase 6 rolling forward validation."
        )
        roundtable_result = self.run_roundtable(
            roundtable_topic,
            research_context="\n".join(context_parts),
            participants=rt_cfg.get("participants"),
            max_rounds=rt_cfg.get("max_rounds"),
            roundtable_config=rt_cfg,
        )
        if str(roundtable_result.get("status", "")).lower() not in {"completed", "approved"}:
            return {
                "status": "ESCALATE_TO_USER",
                "reason": f"roundtable failed: {roundtable_result.get('message', '')}",
                "roundtable": roundtable_result,
            }

        log_text = ""
        log_file = roundtable_result.get("log_file")
        if log_file:
            try:
                log_text = Path(log_file).read_text(encoding="utf-8")
            except OSError:
                log_text = ""
        memory_digest = self._build_roundtable_memory_digest(
            topic=topic,
            strategy_id=strategy_id,
            roundtable_result=roundtable_result,
            log_text=log_text,
        )
        try:
            digest_path = self._write_roundtable_memory_digest(memory_digest, strategy_id)
            roundtable_result["memory_digest_path"] = str(digest_path)
        except OSError as error:
            roundtable_result["memory_digest_error"] = f"{type(error).__name__}: {error}"
        roundtable_result["memory_digest"] = memory_digest
        synthesis_context = "\n".join([
            research_context,
            "ROUND_TABLE_MEMORY_DIGEST:",
            yaml.safe_dump(memory_digest, allow_unicode=True, sort_keys=False),
            "ROUND_TABLE_LOG_PATH:",
            str(log_file or ""),
        ]).strip()

        sequential_workflow = wf.get("sequential_workflow", "kbase_discovery")
        discovery_result = self.run_sequential_workflow(
            sequential_workflow,
            topic=topic,
            strategy_id=strategy_id,
            research_context=synthesis_context,
            llm_config=llm_config,
        )
        return {
            "status": discovery_result.get("status"),
            "reason": discovery_result.get("reason"),
            "roundtable": roundtable_result,
            "discovery": discovery_result,
            "control_decision": discovery_result.get("control_decision"),
        }

    @staticmethod
    def _split_roundtable_log(log_text: str) -> list[dict[str, str]]:
        sections: list[dict[str, str]] = []
        current_name: str | None = None
        current_lines: list[str] = []
        for line in (log_text or "").splitlines():
            match = re.match(r"^###\s+(.+?)\s*$", line)
            if match:
                if current_name:
                    sections.append({
                        "participant": current_name,
                        "content": "\n".join(current_lines).strip(),
                    })
                current_name = match.group(1).strip()
                current_lines = []
                continue
            if current_name:
                current_lines.append(line)
        if current_name:
            sections.append({
                "participant": current_name,
                "content": "\n".join(current_lines).strip(),
            })
        return sections

    @staticmethod
    def _select_digest_lines(content: str, max_lines: int = 5) -> list[str]:
        keywords = (
            "factor", "mechanism", "candidate", "vote", "risk", "leak", "overfit",
            "phase 6", "validation", "因子", "机制", "候选", "投票", "风险",
            "过拟合", "泄漏", "验证", "反驳", "质疑",
        )
        selected: list[str] = []
        for raw_line in content.splitlines():
            line = raw_line.strip("- \t")
            if not line:
                continue
            lowered = line.lower()
            if any(keyword in lowered for keyword in keywords):
                selected.append(line[:240])
            if len(selected) >= max_lines:
                break
        if selected:
            return selected
        fallback = [line.strip()[:240] for line in content.splitlines() if line.strip()]
        return fallback[:max_lines]

    @staticmethod
    def _build_roundtable_memory_digest(
        topic: str,
        strategy_id: str,
        roundtable_result: dict,
        log_text: str,
    ) -> dict:
        """Create deterministic project-side memory from a roundtable log."""
        sections = Orchestrator._split_roundtable_log(log_text)
        participants = [section["participant"] for section in sections]
        participant_positions = [
            {
                "participant": section["participant"],
                "digest_lines": Orchestrator._select_digest_lines(section["content"]),
            }
            for section in sections
        ]
        combined_lines = [
            line
            for section in participant_positions
            for line in section["digest_lines"]
        ]
        candidate_keywords = ("factor", "mechanism", "candidate", "因子", "机制", "候选")
        critique_keywords = ("risk", "leak", "overfit", "反驳", "质疑", "风险", "泄漏", "过拟合")
        return {
            "digest_type": "roundtable_memory_digest",
            "schema_version": 1,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "strategy_id": strategy_id,
            "topic": topic,
            "source_log_file": roundtable_result.get("log_file"),
            "participants": participants or [
                item.get("label") for item in roundtable_result.get("participants", [])
                if isinstance(item, dict) and item.get("label")
            ],
            "participant_positions": participant_positions,
            "candidate_mechanism_lines": [
                line for line in combined_lines
                if any(keyword in line.lower() for keyword in candidate_keywords)
            ][:12],
            "critique_lines": [
                line for line in combined_lines
                if any(keyword in line.lower() for keyword in critique_keywords)
            ][:12],
            "phase6_validation_protocol": {
                "folds": [
                    {
                        "train_window": "2020-2022",
                        "validation_window": "2023",
                        "unseen_test_window": "2024",
                    },
                    {
                        "train_window": "2021-2023",
                        "validation_window": "2024",
                        "unseen_test_window": "2025",
                    },
                    {
                        "train_window": "2022-2024",
                        "validation_window": "2025",
                        "unseen_test_window": "2026",
                    },
                ],
                "selection_rule": "Select factors/thresholds on train+validation only; use each test year once.",
                "required_report": [
                    "average_test_metrics",
                    "worst_fold_metrics",
                    "fold_pass_rate",
                    "dispersion",
                ],
            },
            "memory_boundary": (
                "Project-side AG2 digest only. It is not KBase source evidence and must not be written to D:\\KBase."
            ),
        }

    @staticmethod
    def _write_roundtable_memory_digest(digest: dict, strategy_id: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", strategy_id or ""):
            raise OSError("unsafe strategy_id for roundtable memory digest")
        root = Path(__file__).resolve().parent.parent / "research_state" / strategy_id
        root.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        digest_id = hashlib.sha256(
            yaml.safe_dump(digest, allow_unicode=True, sort_keys=True).encode("utf-8")
        ).hexdigest()[:10]
        path = root / f"roundtable_memory_digest_{timestamp}_{digest_id}.yaml"
        path.write_text(
            yaml.safe_dump(digest, allow_unicode=True, sort_keys=False, width=100),
            encoding="utf-8",
        )
        return path

    @staticmethod
    def _autogen_safe_agent_name(label: str, index: int = 0) -> str:
        """Convert a display label to an AutoGen-safe internal agent name."""
        safe = re.sub(r"[^A-Za-z0-9_]+", "_", str(label or "")).strip("_")
        if not safe:
            safe = f"Roundtable_Participant_{index + 1}"
        if safe[0].isdigit():
            safe = f"Participant_{safe}"
        return safe

    @staticmethod
    def _positive_int(value: Any, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed > 0 else default

    @staticmethod
    def _roundtable_coverage(
        messages: list[dict],
        display_labels_by_name: dict[str, str],
        required_labels: list[str],
        min_messages_per_participant: int,
    ) -> dict:
        """Count substantive roundtable turns per required display label."""
        min_messages = max(1, int(min_messages_per_participant or 1))
        counts = {label: 0 for label in required_labels}
        for msg in messages:
            internal_name = str(msg.get("name") or "")
            label = display_labels_by_name.get(internal_name, internal_name)
            content = str(msg.get("content") or "").strip()
            if label in counts and content:
                counts[label] += 1
        missing = [label for label, count in counts.items() if count < min_messages]
        return {
            "covered": not missing,
            "required_labels": list(required_labels),
            "min_messages_per_participant": min_messages,
            "message_counts": counts,
            "missing_labels": missing,
        }

    def run_roundtable(
        self,
        topic: str,
        research_context: str = "",
        participants: list[dict] | None = None,
        max_rounds: int | None = None,
        roundtable_config: dict | None = None,
        _coverage_retry_attempted: bool = False,
        _connection_retry_attempted: bool = False,
        _connection_retry_count: int = 0,
    ) -> dict:
        """Run a multi-LLM roundtable — each LLM is an equal peer, no preset roles.

        All participants receive the same system prompt. The coordinator LLM
        (defined in config) synthesizes the final summary.

        Args:
            topic: The research question or topic.
            research_context: Background info appended to the initial message.
            participants: Override config participants. List of {profile, label} dicts.
            max_rounds: Override config max_rounds.

        Returns:
            Dict with status.
        """
        base_rt = self.config._raw.get("roundtable", {})
        rt = dict(base_rt)
        if isinstance(roundtable_config, dict):
            rt.update(roundtable_config)
        participants_overridden = participants is not None
        if participants is None:
            participants = rt.get("participants", [])
        if not participants:
            return {"status": "error", "message": "No roundtable participants configured"}

        coordinator_profile = rt.get("coordinator", participants[0]["profile"])
        configured_max_rounds = max_rounds if max_rounds is not None else rt.get("max_rounds", 20)
        max_rounds = self._positive_int(configured_max_rounds, 20)
        system_template = rt.get("system_prompt", "You are {label}, an AI participating in a research roundtable.")
        coverage_cfg = rt.get("coverage_gate", {}) if isinstance(rt.get("coverage_gate"), dict) else {}
        coverage_enabled = bool(coverage_cfg.get("enabled", True))
        min_messages = self._positive_int(coverage_cfg.get("min_messages_per_participant", 1), 1)
        retry_on_coverage_failure = bool(coverage_cfg.get("retry_on_coverage_failure", False))

        context_keys = [
            f"roundtable_peer_{index}" for index in range(len(participants))
        ]
        tokenizer_names: dict[str, str] = {}
        for index, participant in enumerate(participants):
            try:
                participant_llm = self.config.get_llm_config(
                    participant.get("profile", "aggregator"),
                    model=participant.get("model"),
                )
            except KeyError:
                continue
            participant_model = self._llm_config_model_name(participant_llm)
            if participant_model is not None:
                tokenizer_names[context_keys[index]] = participant_model
        trusted_contexts, untrusted_contexts = self._prepare_v342_agent_context(
            context_keys,
            research_context,
            tokenizer_names=tokenizer_names,
        )

        # Create one agent per LLM, each with its own model name
        agents: dict[str, autogen.AssistantAgent] = {}
        display_labels_by_name: dict[str, str] = {}
        active_participants: list[dict] = []
        skipped_participants: list[dict] = []
        active_context_keys: list[str] = []
        used_agent_names: set[str] = set()
        for index, p in enumerate(participants):
            label = p["label"]
            profile_name = p.get("profile", "aggregator")
            model_name = p.get("model")  # per-participant model override
            base_agent_name = self._autogen_safe_agent_name(label, index)
            agent_name = base_agent_name
            suffix = 2
            while agent_name in used_agent_names:
                agent_name = f"{base_agent_name}_{suffix}"
                suffix += 1
            used_agent_names.add(agent_name)
            display_labels_by_name[agent_name] = label

            try:
                llm_cfg = self.config.get_llm_config(profile_name, model=model_name)
            except KeyError:
                print(f"Warning: LLM profile '{profile_name}' not found, skipping {label}")
                skipped_participants.append({"label": label, "profile": profile_name})
                continue

            system_msg = system_template.format(label=label)
            system_msg += (
                "\n\nV3.4 TRUSTED LEARNING SYSTEM CONTEXT:\n"
                f"{trusted_contexts[context_keys[index]]}"
            )

            agent = create_profiled_assistant_agent(
                profile_name,
                name=agent_name,
                system_message=system_msg.strip(),
                llm_config=llm_cfg,
                code_execution_config=False,
            )

            # Register read-only tools so agents can explore code and docs
            for tool_func in get_tools_for_agent(["list_code", "read_code", "list_research_docs", "read_research_doc", "get_strategy_config", "list_available_data"]):
                agent.register_for_llm()(tool_func)
                agent.register_for_execution()(tool_func)
            agents[agent_name] = agent
            active_participants.append(p)
            active_context_keys.append(context_keys[index])

        if skipped_participants and bool(coverage_cfg.get("fail_on_missing_participants", True)):
            return {
                "status": "error",
                "message": "Roundtable participant profile missing; refusing to silently shrink the roster",
                "skipped_participants": skipped_participants,
            }

        if len(agents) < 2:
            return {"status": "error", "message": f"Need at least 2 LLMs, got {len(agents)}"}

        configured_required_labels = coverage_cfg.get("required_labels")
        if configured_required_labels and not participants_overridden:
            required_labels = [str(label) for label in configured_required_labels]
        else:
            required_labels = [str(p["label"]) for p in active_participants]
        if coverage_enabled:
            min_required_rounds = len(required_labels) * min_messages + 1
            if max_rounds < min_required_rounds:
                max_rounds = min_required_rounds

        # Manager uses coordinator's profile + model
        manager_llm = self.config.get_llm_config(coordinator_profile)
        groupchat = autogen.GroupChat(
            agents=list(agents.values()),
            messages=list(untrusted_contexts[active_context_keys[0]]),
            max_round=max_rounds,
            speaker_selection_method="round_robin",
            allow_repeat_speaker=bool(rt.get("allow_repeat_speaker", False)),
        )
        manager = autogen.GroupChatManager(groupchat=groupchat, llm_config=manager_llm)

        # Build the initial message
        participant_list = "\n".join(f"  - {p['label']} (via {p['profile']})" for p in participants)
        initial_message = f"""研究课题：{topic}

参与模型：
{participant_list}

讨论形式：
- 第一轮：每位参与者用2-3句话陈述自己的初步分析。
- 第二轮：自由辩论 — 挑战、质疑、拓展彼此的观点。
- 第三轮：每位参与者指出讨论中最有价值的观点（不一定出自自己）。
- 最后一轮：总结共识，提出可落地的行动建议。

背景资料：
背景资料已作为独立 UNTRUSTED_DATA 用户消息提供。

{list(agents.values())[0].name}，你先开始。"""

        participant_list = "\n".join(f"  - {p['label']} (via {p['profile']})" for p in active_participants)
        coverage_rule = ""
        if coverage_enabled:
            coverage_rule = (
                "\n圆桌覆盖率硬规则：\n"
                f"- 必须让以下参与者每人至少发言 {min_messages} 次：{', '.join(required_labels)}。\n"
                "- 如果覆盖率失败，系统会用同一完整圆桌重试；不得自动缩成小桌、单模型或弱化讨论目标。\n"
            )
        initial_message = f"""研究课题：{topic}

参与模型：
{participant_list}
{coverage_rule}
讨论形式：
- 第一轮：每位参与者用 2-3 句话给出自己的初步分析。
- 第二轮：互相挑战、质疑、扩展彼此观点，重点寻找新机制和反证。
- 第三轮：每位参与者指出最有价值的观点、最大风险、下一步实验。
- 最后一轮：总结共识、分歧、可执行行动和必须保留的反对意见。

故障处理原则：
- 如果圆桌没有充分发挥作用，先修复圆桌覆盖率、模型连通性、调度或提示问题。
- 不要用小桌、单模型、缩小范围、弱化验证或人工替代来获得妥协结果，除非用户明确授权。

背景资料：
背景资料已作为独立 UNTRUSTED_DATA 用户消息提供。

{list(agents.values())[0].name}，你先开始。"""

        first_agent = list(agents.values())[0]
        log_dir = Path(__file__).resolve().parent.parent / "discussions"
        log_dir.mkdir(exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"roundtable_{timestamp}.md"
        roundtable_error: Exception | None = None
        try:
            first_agent.initiate_chat(manager, message=initial_message)
        except Exception as exc:
            roundtable_error = exc

        # Save discussion to file, even if a provider/network error interrupted
        # the chat before the normal persistence point.

        with open(log_file, "w", encoding="utf-8") as f:
            f.write(f"# Roundtable Discussion\n\n")
            f.write(f"**Topic:** {topic}\n\n")
            f.write(f"**Participants:** {', '.join(p['label'] for p in active_participants)}\n\n")
            f.write(f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            coverage = self._roundtable_coverage(
                groupchat.messages,
                display_labels_by_name,
                required_labels,
                min_messages,
            ) if coverage_enabled else {
                "covered": True,
                "required_labels": [],
                "min_messages_per_participant": 0,
                "message_counts": {},
                "missing_labels": [],
            }
            coverage["configured_max_rounds"] = configured_max_rounds
            coverage["effective_max_rounds"] = max_rounds
            f.write(f"**Coverage:** {'PASS' if coverage['covered'] else 'FAIL'}\n\n")
            if coverage_enabled:
                f.write("**Roundtable Coverage Counts:**\n\n")
                for label, count in coverage["message_counts"].items():
                    f.write(f"- {label}: {count}\n")
                if coverage["missing_labels"]:
                    f.write(f"\n**Missing Coverage:** {', '.join(coverage['missing_labels'])}\n")
                f.write("\n")
            if roundtable_error is not None:
                f.write("**Interrupted:** YES\n\n")
                f.write(f"**Interrupt Type:** {type(roundtable_error).__name__}\n\n")
                f.write(f"**Interrupt Message:** {roundtable_error}\n\n")
            f.write(f"---\n\n")
            for msg in groupchat.messages:
                name = msg.get("name", "System")
                name = display_labels_by_name.get(name, name)
                content = msg.get("content", "")
                if content:
                    f.write(f"### {name}\n\n{content}\n\n")

        print(f"\n讨论已保存至: {log_file}")
        if roundtable_error is not None:
            retry_limit = self._positive_int(coverage_cfg.get("connection_retry_max_attempts"), 3)
            if bool(coverage_cfg.get("retry_on_connection_failure", True)) and _connection_retry_count < retry_limit:
                repair_context = "\n\n".join([
                    research_context,
                    "ROUND_TABLE_CONNECTION_REPAIR:",
                    f"Previous full-roster attempt was interrupted by {type(roundtable_error).__name__}: {roundtable_error}",
                    f"Partial log: {log_file}",
                    f"Retry {int(_connection_retry_count) + 1}/{retry_limit} with the same full roster and the same validation scope. Do not downgrade.",
                ]).strip()
                retry_result = self.run_roundtable(
                    topic=topic,
                    research_context=repair_context,
                    participants=active_participants,
                    max_rounds=max_rounds,
                    roundtable_config=rt,
                    _coverage_retry_attempted=_coverage_retry_attempted,
                    _connection_retry_attempted=True,
                    _connection_retry_count=int(_connection_retry_count) + 1,
                )
                retry_result["connection_retry"] = {
                    "attempted": True,
                    "attempt": int(_connection_retry_count) + 1,
                    "max_attempts": retry_limit,
                    "previous_log_file": str(log_file),
                    "previous_error_type": type(roundtable_error).__name__,
                    "previous_error": str(roundtable_error),
                    "previous_coverage": coverage,
                }
                return retry_result
            return {
                "status": "connection_failed",
                "message": (
                    "Roundtable interrupted by provider/network error after partial log recovery; "
                    "refusing to downgrade to a smaller workaround"
                ),
                "log_file": str(log_file),
                "participants": active_participants,
                "coverage": coverage,
                "error_type": type(roundtable_error).__name__,
                "error": str(roundtable_error),
            }
        if coverage_enabled and not coverage["covered"]:
            if retry_on_coverage_failure and not _coverage_retry_attempted:
                retry_default = max_rounds + max(1, len(required_labels) * min_messages)
                retry_max_rounds = max(
                    self._positive_int(coverage_cfg.get("retry_max_rounds"), retry_default),
                    retry_default,
                )
                repair_context = "\n\n".join([
                    research_context,
                    "ROUND_TABLE_COVERAGE_REPAIR:",
                    f"Previous full-roster attempt failed coverage. Missing labels: {', '.join(coverage['missing_labels'])}.",
                    f"Previous log: {log_file}",
                    "Retry with the same full roster. Do not downgrade to a small table or single model.",
                ]).strip()
                retry_result = self.run_roundtable(
                    topic=topic,
                    research_context=repair_context,
                    participants=active_participants,
                    max_rounds=retry_max_rounds,
                    roundtable_config=rt,
                    _coverage_retry_attempted=True,
                    _connection_retry_attempted=_connection_retry_attempted,
                    _connection_retry_count=_connection_retry_count,
                )
                retry_result["coverage_retry"] = {
                    "attempted": True,
                    "previous_log_file": str(log_file),
                    "previous_coverage": coverage,
                }
                return retry_result
            return {
                "status": "coverage_failed",
                "message": "Roundtable coverage gate failed; refusing to downgrade to a smaller workaround",
                "log_file": str(log_file),
                "participants": active_participants,
                "coverage": coverage,
            }
        return {
            "status": "completed",
            "log_file": str(log_file),
            "participants": active_participants,
            "coverage": coverage,
        }

    # ---- Helpers -----------------------------------------------------------

    @staticmethod
    def _build_brainstorm_prompt(topic: str, agent_ids: list[str], agents: dict) -> str:
        agent_names = [a.name for a in agents.values()]
        agent_list = "\n".join(f"  - {name}" for name in agent_names)

        return f"""Research Topic: {topic}

This is a sequential Research OS pipeline (each role acts once, in order):
1. System_Orchestrator: state from Snapshot + Registry verdict + memory_packet.
2. Research_Proposer: draft ONE raw_hypothesis (no registry classification).
3. Data_Validator: PASS/FAIL on leakage and production availability.
4. Experiment_Executor: run exactly as scoped; report raw metrics + anomaly_flag.
5. Risk_Controller: execution/robustness/regime/deployment risk verdict.
6. Strategy_Synthesizer: draft registry/snapshot/handoff deltas.
7. System_Orchestrator: final control_decision (COMMIT / REJECT / ESCALATE_TO_USER).

Participants:
{agent_list}

System_Orchestrator, begin with Step 0."""
