from __future__ import annotations

from datetime import date
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from ag2_research.config import ResearchConfig
from ag2_research.kbase.schemas import ContractValidationError, validate_source_brief_semantics
from ag2_research.orchestrator import Orchestrator


SOURCE_ID = "a" * 64


def valid_brief() -> dict:
    return {
        "brief_id": "brief-test",
        "catalog_version": "catalog-test",
        "research_gap": "理解来源差异。",
        "sources_consulted": [{
            "source_id": SOURCE_ID,
            "voice_role": "primary_direct",
            "date": None,
            "reliability": "medium",
            "evidence_refs": [f"{SOURCE_ID}#raw"],
        }],
        "source_observations": [{
            "source_id": SOURCE_ID,
            "source_says": "来源陈述。",
            "context": "来源上下文。",
        }],
        "disagreements_and_limits": [],
        "missing_evidence": [],
        "agent_inference_boundary": "以下内容尚未进行AG2推断",
        "handoff_questions": ["下游需要判断什么？"],
    }


class DummyRepository:
    manifest = {"catalog_version": "catalog-test"}

    def entries(self):
        return [{"source_id": SOURCE_ID}]

    def get(self, source_id):
        return {
            "source_id": SOURCE_ID,
            "summary": "example summary",
            "available_layers": ["summary", "raw"],
            "paths": {"raw": "raw/example.txt"},
        } if source_id == SOURCE_ID else None

    def read_packet(self, entry):
        return {}


class KBaseWorkflowTests(unittest.TestCase):
    @staticmethod
    def _credentialed_config() -> ResearchConfig:
        return ResearchConfig(
            environ={
                "AG2_Kimi_API_KEY": "test-kimi-key",
                "AG2_Kimi_BASE_URL": "https://example.invalid/kimi/v1",
                "AG2_ZHIPU_API_KEY": "test-zhipu-key",
                "AG2_ZHIPU_BASE_URL": "https://example.invalid/zhipu/v1",
            }
        )

    def test_semantic_contract_rejects_unknown_and_unconsulted_sources(self) -> None:
        brief = valid_brief()
        brief["source_observations"][0]["source_id"] = "b" * 64
        with self.assertRaises(ContractValidationError):
            validate_source_brief_semantics(brief, known_source_ids={SOURCE_ID}, catalog_version="catalog-test")

    def test_semantic_contract_requires_an_observation_for_every_consulted_source(self) -> None:
        brief = valid_brief()
        second_id = "b" * 64
        brief["sources_consulted"].append({
            "source_id": second_id,
            "voice_role": "secondary_commentary",
            "date": None,
            "reliability": "medium",
            "evidence_refs": [f"{second_id}#summary"],
        })
        with self.assertRaisesRegex(ContractValidationError, "lack source_observations"):
            validate_source_brief_semantics(
                brief,
                known_source_ids={SOURCE_ID, second_id},
                catalog_version="catalog-test",
            )

    def test_semantic_contract_rejects_factor_ideas_hidden_as_handoff_questions(self) -> None:
        brief = valid_brief()
        brief["handoff_questions"] = [
            "市场广度是否可作为 Brick 候选排序特征输入？"
        ]
        with self.assertRaisesRegex(ContractValidationError, "project-derivation language"):
            validate_source_brief_semantics(
                brief,
                known_source_ids={SOURCE_ID},
                catalog_version="catalog-test",
            )

    def test_source_librarian_gate_validates_catalog_references(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        with patch("ag2_research.kbase.repository.KBaseRepository", DummyRepository):
            decision, reason, _ = orchestrator._gate("source_librarian", valid_brief(), None, {})
        self.assertEqual(decision, "pass", reason)

        invalid = valid_brief()
        invalid["factor_spec"] = {"expression": "forbidden"}
        with patch("ag2_research.kbase.repository.KBaseRepository", DummyRepository):
            decision, reason, _ = orchestrator._gate("source_librarian", invalid, None, {})
        self.assertEqual(decision, "reject")
        self.assertIn("forbidden", reason)

    def test_gate_escalates_agent_errors_without_inserting_a_fallback_boundary(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        decision, reason, _ = orchestrator._gate(
            "alpha_hunter",
            {"error": "RateLimitError: quota exceeded"},
            None,
            {},
        )
        self.assertEqual("escalate", decision)
        self.assertIn("quota exceeded", reason)

    def test_source_librarian_gate_revises_handoff_question_pollution(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        brief = valid_brief()
        brief["handoff_questions"] = ["市场广度是否可作为排序特征输入？"]
        with patch("ag2_research.kbase.repository.KBaseRepository", DummyRepository):
            decision, reason, _ = orchestrator._gate(
                "source_librarian", brief, None, {}, {}
            )
        self.assertEqual("modify", decision)
        self.assertIn("source-only semantic revision", reason)

    def test_roundtable_discovery_workflow_is_configured(self) -> None:
        workflow = ResearchConfig().get_workflow("kbase_roundtable_discovery")
        self.assertEqual("roundtable_discovery", workflow["type"])
        self.assertEqual("kbase_discovery", workflow["sequential_workflow"])
        self.assertGreaterEqual(len(workflow["roundtable"]["participants"]), 4)
        self.assertGreaterEqual(workflow["roundtable"]["max_rounds"], 40)
        coverage_gate = workflow["roundtable"]["coverage_gate"]
        self.assertTrue(coverage_gate["enabled"])
        self.assertGreaterEqual(coverage_gate["min_messages_per_participant"], 2)
        self.assertTrue(coverage_gate["retry_on_coverage_failure"])
        self.assertTrue(coverage_gate["retry_on_connection_failure"])
        self.assertGreaterEqual(coverage_gate["connection_retry_max_attempts"], 2)
        self.assertTrue(coverage_gate["no_small_table_fallback"])

    def test_source_first_discovery_workflow_is_configured(self) -> None:
        workflow = ResearchConfig().get_workflow("kbase_source_first_discovery")
        self.assertEqual("source_first_discovery", workflow["type"])
        self.assertEqual("kbase_source_brief", workflow["source_brief_workflow"])
        self.assertEqual("kbase_factor_handoff", workflow["factor_workflow"])
        self.assertGreaterEqual(len(workflow["roundtable"]["participants"]), 4)

    def test_llm_config_sets_provider_retry_budget(self) -> None:
        llm_config = self._credentialed_config().get_llm_config("kimi")
        self.assertGreaterEqual(llm_config["config_list"][0]["max_retries"], 3)
        self.assertNotIn("max_retries", llm_config)

    def test_source_librarian_profile_has_long_structured_output_budget(self) -> None:
        llm_config = self._credentialed_config().get_agent_llm_config("source_librarian")
        self.assertGreaterEqual(llm_config["max_tokens"], 32000)

    def test_roundtable_labels_are_converted_to_autogen_safe_names(self) -> None:
        self.assertEqual(
            "DeepSeek_V4_Alpha_Hunter",
            Orchestrator._autogen_safe_agent_name("DeepSeek-V4 Alpha Hunter"),
        )
        self.assertEqual(
            "Participant_123_Model",
            Orchestrator._autogen_safe_agent_name("123 Model"),
        )

    def test_roundtable_coverage_detects_missing_participants(self) -> None:
        labels_by_name = {
            "DeepSeek_V4_Alpha_Hunter": "DeepSeek-V4 Alpha Hunter",
            "Kimi_Falsifier": "Kimi Falsifier",
        }
        messages = [
            {"name": "DeepSeek_V4_Alpha_Hunter", "content": "mechanism"},
            {"name": "Kimi_Falsifier", "content": "risk"},
            {"name": "DeepSeek_V4_Alpha_Hunter", "content": "follow-up"},
        ]
        coverage = Orchestrator._roundtable_coverage(
            messages,
            labels_by_name,
            ["DeepSeek-V4 Alpha Hunter", "Kimi Falsifier"],
            min_messages_per_participant=2,
        )
        self.assertFalse(coverage["covered"])
        self.assertEqual(["Kimi Falsifier"], coverage["missing_labels"])

        messages.append({"name": "Kimi_Falsifier", "content": "decisive falsification"})
        coverage = Orchestrator._roundtable_coverage(
            messages,
            labels_by_name,
            ["DeepSeek-V4 Alpha Hunter", "Kimi Falsifier"],
            min_messages_per_participant=2,
        )
        self.assertTrue(coverage["covered"])

    def test_source_supported_normalizes_model_annotations(self) -> None:
        source_a = "a" * 64
        source_b = "b" * 64
        self.assertEqual(
            [source_a, source_b],
            Orchestrator._normalize_source_supported([
                f"{source_a} (quant textbook: alpha purification)",
                {f"{source_b}  (volume authenticity)": "shrinkage note"},
            ]),
        )

    def test_roundtable_discovery_dispatches_to_dedicated_runner(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator.config = ResearchConfig()
        with patch.object(orchestrator, "run_roundtable_discovery", return_value={"status": "APPROVED"}) as runner:
            result = orchestrator.run_workflow(
                "kbase_roundtable_discovery", topic="brick factor diversity", strategy_id="brick"
            )
        self.assertEqual("APPROVED", result["status"])
        runner.assert_called_once()

    def test_source_first_discovery_dispatches_to_dedicated_runner(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator.config = ResearchConfig()
        with patch.object(
            orchestrator, "run_source_first_discovery", return_value={"status": "APPROVED"}
        ) as runner:
            result = orchestrator.run_workflow(
                "kbase_source_first_discovery",
                topic="优化当前选股系统",
                strategy_id="brick",
            )
        self.assertEqual("APPROVED", result["status"])
        runner.assert_called_once()

    def test_roundtable_memory_digest_is_structured(self) -> None:
        log = """# Roundtable Discussion

### DeepSeek-V4 Alpha Hunter

候选机制：测试成交额冲击后的砖型延续因子。

### Grok Falsifier

风险：这个机制可能只是 source abundance bias，并且需要 Phase 6 validation。
"""
        digest = Orchestrator._build_roundtable_memory_digest(
            topic="brick factor diversity",
            strategy_id="brick",
            roundtable_result={"log_file": "roundtable.md"},
            log_text=log,
        )

        self.assertEqual("roundtable_memory_digest", digest["digest_type"])
        self.assertEqual(["DeepSeek-V4 Alpha Hunter", "Grok Falsifier"], digest["participants"])
        self.assertTrue(digest["candidate_mechanism_lines"])
        self.assertTrue(digest["critique_lines"])
        self.assertEqual(3, len(digest["phase6_validation_protocol"]["folds"]))

    def test_roundtable_discovery_writes_digest_and_passes_it_downstream(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator.config = ResearchConfig()
        with tempfile.TemporaryDirectory() as directory:
            log_path = f"{directory}/roundtable.md"
            with open(log_path, "w", encoding="utf-8") as handle:
                handle.write("### DeepSeek\n\n候选机制：new factor\n")
            with (
                patch.object(orchestrator, "run_roundtable", return_value={
                    "status": "completed",
                    "log_file": log_path,
                }),
                patch.object(orchestrator, "_write_roundtable_memory_digest", return_value=f"{directory}/digest.yaml"),
                patch("ag2_research.knowledge_bridge.build_combined_research_context", return_value="KB"),
                patch.object(orchestrator, "run_sequential_workflow", return_value={
                    "status": "APPROVED",
                    "reason": "ok",
                    "control_decision": {"decision": "APPROVE_NEXT"},
                }) as sequential,
            ):
                result = orchestrator.run_roundtable_discovery(
                    "brick factor diversity",
                    strategy_id="brick",
                    research_context="CTX",
                )

        self.assertEqual("APPROVED", result["status"])
        self.assertIn("memory_digest", result["roundtable"])
        self.assertIn("memory_digest_path", result["roundtable"])
        context = sequential.call_args.kwargs["research_context"]
        self.assertIn("ROUND_TABLE_MEMORY_DIGEST", context)
        self.assertIn("phase6_validation_protocol", context)

    def test_source_first_discovery_searches_before_roundtable_and_factor_handoff(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator.config = ResearchConfig()
        events = []
        brief = valid_brief()

        def sequential(workflow_id, **kwargs):
            events.append(workflow_id)
            if workflow_id == "kbase_source_brief":
                return {
                    "status": "APPROVED",
                    "reason": "brief valid",
                    "transcript": [{"stage": "source_librarian", "output": brief}],
                    "control_decision": {"decision": "APPROVE_NEXT"},
                }
            self.assertEqual({"source_librarian": brief}, kwargs["initial_outputs"])
            self.assertIn("APPROVED_SOURCE_BRIEF", kwargs["research_context"])
            self.assertTrue(kwargs["require_kbase_inspired"])
            return {
                "status": "APPROVED",
                "reason": "factor handoff valid",
                "transcript": [
                    {"stage": "alpha_hunter", "output": {}},
                    {"stage": "falsification_officer", "output": {}},
                    {"stage": "factor_engineer", "output": {}},
                ],
                "control_decision": {"decision": "APPROVE_NEXT"},
            }

        def roundtable(*args, **kwargs):
            events.append("roundtable")
            self.assertIn("APPROVED_SOURCE_BRIEF", kwargs["research_context"])
            return {"status": "completed", "messages": [], "participants": []}

        project_state = {
            "schema_version": "ag2.project_state_packet.v1",
            "strategy_id": "brick",
            "optimization_request": "优化当前选股系统",
            "project_state_fingerprint": "p" * 64,
            "kbase_release": {"status": "READY", "catalog_version": "catalog-test"},
        }
        gap_request = {
            "schema_version": "ag2.research_gap_request.v1",
            "request_id": "g" * 64,
        }
        with (
            patch("ag2_research.project_state.compile_project_state", return_value=project_state),
            patch("ag2_research.research_gap.build_research_gap_request", return_value=gap_request),
            patch.object(orchestrator, "run_sequential_workflow", side_effect=sequential),
            patch.object(orchestrator, "run_roundtable", side_effect=roundtable),
            patch.object(orchestrator, "_write_roundtable_memory_digest", return_value="digest.yaml"),
        ):
            result = orchestrator.run_source_first_discovery(
                "优化当前选股系统", strategy_id="brick", research_context="USER_CONTEXT"
            )

        self.assertEqual(
            ["kbase_source_brief", "roundtable", "kbase_factor_handoff"], events
        )
        self.assertEqual("APPROVED", result["status"])
        self.assertEqual(
            ["source_librarian", "alpha_hunter", "falsification_officer", "factor_engineer"],
            [step["stage"] for step in result["transcript"]],
        )
        self.assertEqual("source_first", result["workflow_order"])

    def test_stage_execution_error_escalates_before_source_boundary_gate(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        decision, reason, registry_status = orchestrator._gate(
            "alpha_hunter",
            {
                "_raw": "",
                "error": "stage 'alpha_hunter' reply failed: RateLimitError: quota exceeded",
                "source_boundary": {
                    "research_channel": "independent",
                    "source_supported": [],
                },
            },
            None,
            {},
            require_kbase_inspired=True,
        )

        self.assertEqual("escalate", decision)
        self.assertIn("stage execution failed without fallback", reason)
        self.assertIsNone(registry_status)

    def test_source_librarian_runtime_instruction_has_no_fixed_source_cap(self) -> None:
        instruction = Orchestrator._stage_output_instruction("source_librarian")
        self.assertNotIn("at most 5 sources", instruction)
        self.assertNotIn("at most 2 evidence_refs", instruction)
        self.assertIn("no fixed source count", instruction)
        self.assertIn("successfully opened with kbase_open", instruction)
        self.assertIn("pass kbase_trace", instruction)
        self.assertIn("Never splice", instruction)

    def test_source_librarian_revision_message_does_not_restart_research(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        message = orchestrator._build_stage_message(
            "source_librarian",
            {"large": "memory packet"},
            {"__controller_revision__": {
                "stage": "source_librarian",
                "reason": "missing handoff questions",
                "citation_inventory": {"eligible_sources": []},
            }},
            "optimize Brick",
        )
        self.assertIn("SOURCE_BRIEF_REVISION_ONLY", message)
        self.assertIn("Do not restart catalog research", message)
        self.assertNotIn("memory_packet:", message)

    def test_resume_source_first_runs_only_factor_handoff(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator.config = ResearchConfig()
        brief = valid_brief()
        project_state = {
            "kbase_release": {
                "status": "READY",
                "catalog_version": "catalog-test",
                "bundle_fingerprint": "f" * 64,
            }
        }
        checkpoint = {
            "handoff_type": "kbase_discovery",
            "strategy_id": "brick",
            "topic": "resume test",
            "result": {
                "workflow_order": "source_first",
                "project_state_packet": project_state,
                "research_gap_request": {"request_id": "g" * 64},
                "source_brief_result": {
                    "status": "APPROVED",
                    "memory_packet": {"registry_status": "none"},
                    "transcript": [{
                        "stage": "source_librarian",
                        "output": brief,
                        "gate": {"decision": "pass"},
                    }],
                },
                "roundtable": {
                    "status": "completed",
                    "coverage": {"covered": True},
                    "memory_digest": {"core_consensus": ["resume"]},
                    "log_file": "roundtable.log",
                },
            },
        }
        factor_result = {
            "status": "APPROVED",
            "reason": "factor handoff valid",
            "transcript": [{"stage": "alpha_hunter", "output": {}}],
            "control_decision": {"decision": "APPROVE_NEXT"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "checkpoint.yaml"
            path.write_text(yaml.safe_dump(checkpoint, allow_unicode=True), encoding="utf-8")
            with (
                patch(
                    "ag2_research.kbase.release_bundle.inspect_semantic_release_bundle",
                    return_value={
                        "status": "READY",
                        "catalog_version": "catalog-test",
                        "bundle_fingerprint": "f" * 64,
                    },
                ),
                patch.object(orchestrator, "_gate", return_value=("pass", "ok", None)),
                patch.object(orchestrator, "run_sequential_workflow", return_value=factor_result) as run,
            ):
                result = orchestrator.resume_source_first_discovery(path)

        self.assertEqual("APPROVED", result["status"])
        self.assertEqual("factor_handoff", result["resume_stage"])
        self.assertEqual(1, run.call_count)
        self.assertEqual("kbase_factor_handoff", run.call_args.args[0])
        self.assertTrue(run.call_args.kwargs["require_kbase_inspired"])

    def test_stage_parser_extracts_fenced_source_brief_from_mixed_reply(self) -> None:
        text = "I now have enough material.\n\n```yaml\n" + yaml.safe_dump(valid_brief()) + "```\n"
        parsed = Orchestrator._parse_stage_output("source_librarian", text)
        self.assertEqual(parsed["brief_id"], "brief-test")
        self.assertEqual(parsed["catalog_version"], "catalog-test")

    def test_falsification_parser_replaces_thinking_placeholders_with_yaml_fields(self) -> None:
        parsed = Orchestrator._parse_stage_output(
            "falsification_officer",
            """<think>
**decisive_test:**
**failure_conditions:**
**verdict:** I need to decide.
</think>

```yaml
decisive_test:
  method: manual_audit
  discriminating_observation: draft
  expected_if_alpha_holds: same placeholder
  expected_if_counter_holds: same placeholder
failure_conditions:
  - draft placeholder
verdict: PROCEED
```

decisive_test:
  method: code_change
  discriminating_observation: fold-stable account lift
  expected_if_alpha_holds: all unseen folds improve
  expected_if_counter_holds: lift disappears out of sample
failure_conditions:
  - no unseen-fold improvement
verdict: PROCEED
""",
        )

        self.assertEqual("code_change", parsed["decisive_test"]["method"])
        self.assertEqual(["no unseen-fold improvement"], parsed["failure_conditions"])
        self.assertEqual("PROCEED", parsed["verdict"])

    def test_stage_parser_repairs_source_brief_scalars_with_colons(self) -> None:
        text = f"""I will compile the source brief.

```yaml
brief_id: brief-test
catalog_version: catalog-test
research_gap: Brick next factor: after sector-relative failure
sources_consulted:
  - source_id: {SOURCE_ID}
    voice_role: primary_direct
    date: null
    reliability: medium
    evidence_refs:
      - {SOURCE_ID[:8]}#summary
source_observations:
  - source_id: {SOURCE_ID}
    source_says: Proposes a five-question framework: identify main sector and profit effect.
    context: Practitioner source: qualitative framework, no formal backtest.
disagreements_and_limits:
  - Qualitative source: no direct evidence.
missing_evidence:
  - No A-share daily-horizon test.
agent_inference_boundary: 以下内容尚未进行AG2推断
handoff_questions:
  - Can the idea be tested without leakage?
```
"""
        parsed = Orchestrator._parse_stage_output("source_librarian", text)
        self.assertEqual(parsed["brief_id"], "brief-test")
        self.assertEqual(parsed["research_gap"], "Brick next factor: after sector-relative failure")
        self.assertIn("framework: identify", parsed["source_observations"][0]["source_says"])

    def test_stage_parser_extracts_heading_fenced_alpha_sections(self) -> None:
        text = f"""### source_boundary

```yaml
research_channel: independent
source_brief_id: null
source_supported: []
agent_inference: structural gap analysis only
```

### alpha_family_gap

```yaml
existing_families: [Entry-Day Pricing]
missing_families: [Multi-Timeframe]
highest_potential: Multi-Timeframe relational structure
```

### proposed_generator

```yaml
family: Multi-Timeframe
mechanism: MA5 and MA20 disagreement scores pullback quality
required_data: OHLCV and entry_open, all available at decision time
expected_jaccard_vs_wave_qualified: low
expected_information_gain: cross-timeframe structure absent from V2
```
"""
        parsed = Orchestrator._parse_stage_output("alpha_hunter", text)
        self.assertEqual("independent", parsed["source_boundary"]["research_channel"])
        self.assertEqual([], parsed["source_boundary"]["source_supported"])
        self.assertEqual("Multi-Timeframe", parsed["proposed_generator"]["family"])
        self.assertIn("Multi-Timeframe", parsed["alpha_family_gap"]["missing_families"])

    def test_source_brief_gate_revises_semantic_omissions_instead_of_inventing_defaults(self) -> None:
        brief = valid_brief()
        brief["sources_consulted"][0]["date"] = date(2024, 1, 2)
        del brief["source_observations"][0]["context"]
        del brief["disagreements_and_limits"]
        del brief["missing_evidence"]
        del brief["agent_inference_boundary"]
        del brief["handoff_questions"]

        orchestrator = Orchestrator.__new__(Orchestrator)
        with patch("ag2_research.kbase.repository.KBaseRepository", DummyRepository):
            decision, reason, _ = orchestrator._gate("source_librarian", brief, None, {})
        self.assertEqual(decision, "modify", reason)
        self.assertIn("complete replacement", reason)

    def test_source_brief_gate_expands_unique_short_evidence_refs(self) -> None:
        brief = valid_brief()
        brief["sources_consulted"][0]["evidence_refs"] = [
            f"{SOURCE_ID[:8]}#raw",
            f"{SOURCE_ID[:8]}#summary",
            f"{SOURCE_ID[:6]}...#raw",
        ]

        orchestrator = Orchestrator.__new__(Orchestrator)
        with patch("ag2_research.kbase.repository.KBaseRepository", DummyRepository):
            decision, reason, _ = orchestrator._gate("source_librarian", brief, None, {})
        self.assertEqual(decision, "pass", reason)

    def test_source_brief_gate_accepts_section_level_evidence_refs(self) -> None:
        brief = valid_brief()
        brief["sources_consulted"][0]["evidence_refs"] = [
            f"{SOURCE_ID}#claims",
            f"{SOURCE_ID}#methods",
        ]

        class SectionRepository(DummyRepository):
            def read_packet(self, entry):
                return {
                    "record": {
                        "claims": [{"text": "claim"}],
                        "methods": [{"text": "method"}],
                    }
                }

        orchestrator = Orchestrator.__new__(Orchestrator)
        with patch("ag2_research.kbase.repository.KBaseRepository", SectionRepository):
            decision, reason, _ = orchestrator._gate("source_librarian", brief, None, {})
        self.assertEqual(decision, "pass", reason)

    def test_stage_parser_handles_markdown_section_output(self) -> None:
        text = f"""**source_boundary:**
research_channel: kbase_inspired__PIPE__independent
source_brief_id: brief-test__LT__required__GT__
source_supported: [{SOURCE_ID}]
agent_inference: project-side inference

**alpha_family_gap:**
existing_families: []
missing_families: [Regime__PIPE__Tail__PIPE__Risk]
highest_potential: Regime because market state may alter Brick risk.

**proposed_generator:**
family: Regime
mechanism: market state changes Brick payoff asymmetry
required_data: index breadth, available
expected_jaccard_vs_wave_qualified: low
expected_information_gain: non-empty
"""
        parsed = Orchestrator._parse_stage_output("alpha_hunter", text)
        self.assertEqual(parsed["source_boundary"]["research_channel"], "kbase_inspired")
        self.assertEqual(parsed["source_boundary"]["source_brief_id"], "brief-test")
        self.assertEqual(parsed["alpha_family_gap"]["missing_families"], ["Regime", "Tail", "Risk"])
        self.assertEqual(parsed["proposed_generator"]["family"], "Regime")

    def test_stage_parser_handles_markdown_section_without_colon(self) -> None:
        text = f"""**source_boundary**
research_channel: kbase_inspired
source_brief_id: brief-test
source_supported: [{SOURCE_ID}]

**alpha_family_gap**
existing_families: []
missing_families: [Flow]
highest_potential: Flow

**proposed_generator**
family: Flow
mechanism: volume pressure
required_data: daily bars
expected_jaccard_vs_wave_qualified: low
expected_information_gain: distinct flow score
"""
        parsed = Orchestrator._parse_stage_output("alpha_hunter", text)
        self.assertEqual(parsed["source_boundary"]["source_brief_id"], "brief-test")
        self.assertEqual(parsed["proposed_generator"]["family"], "Flow")

    def test_stage_parser_normalizes_research_channel_alternatives(self) -> None:
        text = f"""**source_boundary:**
research_channel: kbase_inspired | independent
source_brief_id: brief-test (null for independent)
source_supported: [{SOURCE_ID}]

**alpha_family_gap:**
existing_families: []
missing_families: [Flow]
highest_potential: Flow

**proposed_generator:**
family: Flow
mechanism: volume pressure
required_data: daily bars
expected_jaccard_vs_wave_qualified: low
expected_information_gain: distinct flow score
"""
        parsed = Orchestrator._parse_stage_output("alpha_hunter", text)
        self.assertEqual(parsed["source_boundary"]["research_channel"], "kbase_inspired")
        self.assertEqual(parsed["source_boundary"]["source_brief_id"], "brief-test")

    def test_stage_parser_handles_fenced_markdown_sections(self) -> None:
        text = f"""**source_boundary:**
```
research_channel: kbase_inspired
source_brief_id: brief-test
source_supported: [{SOURCE_ID}]
```

**alpha_family_gap:**
```
existing_families: []
missing_families: [Flow]
highest_potential: Flow
```

**proposed_generator:**
```
family: Flow
mechanism: volume pressure
required_data: daily bars
expected_jaccard_vs_wave_qualified: low
expected_information_gain: distinct flow score
```
"""
        parsed = Orchestrator._parse_stage_output("alpha_hunter", text)
        self.assertEqual(parsed["source_boundary"]["source_brief_id"], "brief-test")
        self.assertEqual(parsed["proposed_generator"]["mechanism"], "volume pressure")

    def test_stage_parser_recovers_alpha_sections_from_malformed_fenced_yaml(self) -> None:
        text = """The proposal follows.

```yaml
source_boundary:
  research_channel: independent
  source_brief_id: null
  source_supported: []
  agent_inference: roundtable structural gap analysis

alpha_family_gap:
  existing_families:
    - Entry-Day Price Position
  missing_families:
    - Cross-Sectional Path Structure
  highest_potential: path texture rather than endpoint distance

kbase_bias_check:
  source_density_bias: low
  novelty_or_reopen_reason: >
    This long scalar is valid until the next line.
   This under-indented continuation makes the whole YAML block malformed.

proposed_generator:
  family: Cross-Sectional Path Structure
  mechanism: pre-signal decline texture using only daily OHLCV
  required_data: daily OHLCV from research_indicators_cache
  expected_jaccard_vs_wave_qualified: low
  expected_information_gain: residual RankIC after controlling V2
```
"""
        parsed = Orchestrator._parse_stage_output("alpha_hunter", text)
        self.assertEqual("independent", parsed["source_boundary"]["research_channel"])
        self.assertIn("Cross-Sectional Path Structure", parsed["alpha_family_gap"]["missing_families"])
        self.assertEqual("Cross-Sectional Path Structure", parsed["proposed_generator"]["family"])

    def test_stage_parser_handles_markdown_lists_and_lt_tokens(self) -> None:
        text = f"""**source_boundary:**
research_channel: kbase_inspired
source_brief_id: brief-test
source_supported:
- {SOURCE_ID}

**alpha_mechanism:**
family: Regime__PIPE__Flow
mechanism: layered position cuts (首仓__LT__30 %) stay inside Brick.

**counter_hypothesis:**
Regime overlay may add only noise.

**verdict:** REVISE
"""
        parsed = Orchestrator._parse_stage_output("falsification_officer", text)
        self.assertEqual(parsed["source_boundary"]["source_supported"], [SOURCE_ID])
        self.assertIn("<30", parsed["alpha_mechanism"]["mechanism"])
        self.assertEqual(parsed["counter_hypothesis"], "Regime overlay may add only noise.")
        self.assertEqual(parsed["verdict"], "REVISE")

    def test_stage_parser_handles_section_level_bullet_list(self) -> None:
        text = """**failure_conditions:**
- Only internal ranking improves.
- TopN boundary changes.

**verdict:** PROCEED
"""
        parsed = Orchestrator._parse_stage_output("falsification_officer", text)
        self.assertEqual(
            parsed["failure_conditions"],
            ["Only internal ranking improves.", "TopN boundary changes."],
        )
        self.assertEqual(parsed["verdict"], "PROCEED")

    def test_stage_parser_handles_decisive_test_method_then_fields(self) -> None:
        text = """**decisive_test:**
parameter_sweep
discriminating_observation: account-level lift over baseline
expected_if_alpha_holds: better Sharpe with lower drawdown
expected_if_counter_holds: no lift or worse drawdown
"""
        parsed = Orchestrator._parse_stage_output("falsification_officer", text)
        self.assertEqual(parsed["decisive_test"]["method"], "parameter_sweep")
        self.assertEqual(
            parsed["decisive_test"]["discriminating_observation"],
            "account-level lift over baseline",
        )
        self.assertEqual(
            parsed["decisive_test"]["expected_if_counter_holds"],
            "no lift or worse drawdown",
        )

    def test_falsification_parser_recovers_decisive_test_with_unquoted_colon(self) -> None:
        text = """<think>
decisive_test: comprehensive PWF with two baselines
failure_conditions: draft
verdict: PROCEED
</think>
```yaml
counter_hypothesis: the overlay is only a regime proxy
decisive_test:
  method: code_change
  discriminating_observation: The triple signature: beat V2 and the regime control.
  expected_if_alpha_holds: Both account baselines improve in 2 of 3 folds.
  expected_if_counter_holds: The control comparison fails: no incremental lift remains.
failure_conditions:
  - no account-level lift
verdict: PROCEED
```
"""

        parsed = Orchestrator._parse_stage_output("falsification_officer", text)

        self.assertEqual("code_change", parsed["decisive_test"]["method"])
        self.assertIn("triple signature", parsed["decisive_test"]["discriminating_observation"])
        self.assertIn("control comparison fails", parsed["decisive_test"]["expected_if_counter_holds"])
        self.assertEqual("PROCEED", parsed["verdict"])

    def test_stage_parser_recovers_falsification_fields_from_malformed_yaml(self) -> None:
        text = """source_boundary:
  research_channel: kbase_inspired
  source_brief_id: brief-test
  source_supported:
    - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
alpha_mechanism:
  family: Volume-Structure Anchoring
  mechanism: Rank by distance to the most recent high-volume bar.
counter_hypothesis: The observed ranking power is only a restatement of V2's entry-day price position and short-term return path.
decisive_test:
  method: residual_ablation
  discriminating_observation: incremental R2 after regressing V2 features
  expected_if_alpha_holds: incremental R2 remains non-zero
  expected_if_counter_holds: incremental R2 is statistically zero
failure_conditions: [lookback window >60 days causes stale anchor, any leakage beyond T-1 close]
verdict: REVISE
revision_guidance: Tighten the lookback and add an orthogonality audit.
"""
        parsed = Orchestrator._parse_stage_output("falsification_officer", text)
        self.assertIn("restatement of V2", parsed["counter_hypothesis"])
        self.assertEqual("residual_ablation", parsed["decisive_test"]["method"])
        self.assertEqual("REVISE", parsed["verdict"])
        self.assertIn("orthogonality", parsed["revision_guidance"])

    def test_stage_parser_handles_quoted_text_inside_yaml_bullet(self) -> None:
        text = """```yaml
brief_id: brief-test
catalog_version: catalog-test
research_gap: gap
sources_consulted:
  - source_id: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    voice_role: primary_direct
    date: null
    reliability: medium
    evidence_refs:
      - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#summary
source_observations:
  - source_id: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    source_says: source says x
    context: context
disagreements_and_limits:
  - "后量超前量、底量超顶量"量能形态是否可转化为特征
missing_evidence: []
agent_inference_boundary: 以下内容尚未进行AG2推断
handoff_questions:
  - "换手率 15-30%"是否正交？
```"""
        parsed = Orchestrator._parse_stage_output("source_librarian", text)
        self.assertEqual(parsed["brief_id"], "brief-test")
        self.assertIn("底量超顶量", parsed["disagreements_and_limits"][0])
        self.assertIn("换手率", parsed["handoff_questions"][0])

    def test_downstream_boundary_gate_blocks_false_source_support(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        invalid = {"source_boundary": {"research_channel": "independent", "source_supported": [SOURCE_ID]}}
        decision, reason, _ = orchestrator._gate("alpha_hunter", invalid, None, {})
        self.assertEqual(decision, "reject")
        self.assertIn("cannot claim", reason)

        valid = {
            "source_boundary": {
                "research_channel": "kbase_inspired", "source_brief_id": "brief-test",
                "source_supported": [SOURCE_ID],
            },
            "factor_batch": [{
                "name": "flow_pressure", "expression": "volume / ma(volume, 20)",
                "family": "flow", "polarity": "positive",
                "transformation_type": "ratio", "data_requirements": ["volume"],
            }],
        }
        falsification = {
            "source_boundary": valid["source_boundary"],
            "alpha_mechanism": {"family": "flow", "mechanism": "pressure persists"},
            "counter_hypothesis": "the volume is only transient attention",
            "decisive_test": {
                "method": "manual_audit",
                "discriminating_observation": "subsequent demand persistence",
                "expected_if_alpha_holds": "demand persists",
                "expected_if_counter_holds": "demand vanishes",
            },
            "failure_conditions": ["no persistence"],
            "verdict": "PROCEED",
        }
        valid["falsification_consumed"] = {
            key: falsification[key]
            for key in ("verdict", "counter_hypothesis", "decisive_test", "failure_conditions")
        }
        decision, _, _ = orchestrator._gate(
            "factor_engineer", valid, None, {},
            {"falsification_officer": falsification},
        )
        self.assertEqual(decision, "pass")

    def test_factor_gate_normalizes_falsification_consumption(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        boundary = {
            "research_channel": "independent",
            "source_brief_id": None,
            "source_supported": [],
        }
        falsification = {
            "source_boundary": boundary,
            "counter_hypothesis": "feature duplicates V2 volume state",
            "decisive_test": {
                "method": "code_change",
                "discriminating_observation": "ablation loses fold-stable lift",
                "expected_if_alpha_holds": "new factor survives ablation",
                "expected_if_counter_holds": "new factor equals baseline",
            },
            "failure_conditions": ["no fold-stable lift"],
            "verdict": "PROCEED",
        }
        output = {
            "source_boundary": boundary,
            "falsification_consumed": {
                "verdict": "PROCEED",
                "counter_hypothesis": "same idea, paraphrased",
                "decisive_test": falsification["decisive_test"],
                "failure_conditions": falsification["failure_conditions"],
            },
            "factor_batch": [{
                "name": "daily_volume_pressure",
                "expression": "volume / mean(volume, 20)",
                "family": "volume",
                "polarity": "positive",
                "transformation_type": "zscore",
                "data_requirements": ["volume"],
            }],
        }
        decision, reason, _ = orchestrator._gate(
            "factor_engineer", output, None, {},
            {"falsification_officer": falsification},
        )
        self.assertEqual("pass", decision, reason)
        self.assertEqual(
            "feature duplicates V2 volume state",
            output["falsification_consumed"]["counter_hypothesis"],
        )
        self.assertEqual("same idea, paraphrased", output["falsification_consumed_raw"]["counter_hypothesis"])

    def test_factor_parser_recovers_malformed_research_mechanism(self) -> None:
        text = """source_boundary:
  research_channel: independent
  source_brief_id: null
  source_supported: []

falsification_consumed:
  verdict: PROCEED
  counter_hypothesis: baseline trees already condition volume: no residual signal
  decisive_test:
    method: code_change
    discriminating_observation: Fold 1: train 2020-2022, test 2024
    expected_if_alpha_holds: stable lift
    expected_if_counter_holds: no lift
  failure_conditions:
    - no fold-stable lift

research_mechanism:
  name: Cross-Level Signal Fidelity Ensemble
  family: Cross-Level Signal Fidelity
  mechanism: Train a volume sub-model: combine it with the full model at inference.
  runner_id: backtest_brick_v2_research.py
  validation_plan:
    - Pre-screen gate
      - Compare against V5 regime labels
      - Archive when correlation exceeds the bound
    - 3-Fold Purged Walk Forward
      - Fold 1: Train 2020-2022, Val 2023, Test 2024
  stop_conditions:
    - V5 correlation exceeds the bound
    - Ensemble fails average test CAGR improvement: 3 percent
"""
        parsed = Orchestrator._parse_stage_output("factor_engineer", text)

        self.assertTrue(parsed["_falsification_consumed_declared"])
        mechanism = parsed["research_mechanism"]
        self.assertEqual("Cross-Level Signal Fidelity", mechanism["family"])
        self.assertEqual("backtest_brick_v2_research.py", mechanism["runner_id"])
        self.assertIn("Compare against V5 regime labels", mechanism["validation_plan"])
        self.assertEqual(2, len(mechanism["stop_conditions"]))

    def test_factor_gate_binds_declared_malformed_falsification_to_upstream(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        boundary = {
            "research_channel": "independent",
            "source_brief_id": None,
            "source_supported": [],
        }
        falsification = {
            "source_boundary": boundary,
            "counter_hypothesis": "baseline already conditions volume",
            "decisive_test": {
                "method": "code_change",
                "discriminating_observation": "fold-stable account lift",
                "expected_if_alpha_holds": "stable lift",
                "expected_if_counter_holds": "no lift",
            },
            "failure_conditions": ["no fold-stable lift"],
            "verdict": "PROCEED",
        }
        output = {
            "source_boundary": boundary,
            "falsification_consumed": {"verdict": "PROCEED"},
            "_falsification_consumed_declared": True,
            "research_mechanism": {
                "name": "Cross-Level Signal Fidelity Ensemble",
                "family": "Cross-Level Signal Fidelity",
                "mechanism": "conditionally combine two pre-trade model scores",
                "runner_id": "backtest_brick_v2_research.py",
                "validation_plan": ["three fixed rolling folds"],
                "stop_conditions": ["no fold-stable lift"],
            },
        }

        decision, reason, _ = orchestrator._gate(
            "factor_engineer",
            output,
            None,
            {},
            {"falsification_officer": falsification},
        )

        self.assertEqual("pass", decision, reason)
        self.assertEqual(falsification["decisive_test"], output["falsification_consumed"]["decisive_test"])
        self.assertEqual({"verdict": "PROCEED"}, output["falsification_consumed_raw"])

    def test_factor_gate_rejects_unavailable_l2_requirements(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        boundary = {
            "research_channel": "independent",
            "source_brief_id": None,
            "source_supported": [],
        }
        falsification = {
            "source_boundary": boundary,
            "counter_hypothesis": "L2 flow may be redundant",
            "decisive_test": {
                "method": "data_extension",
                "discriminating_observation": "coverage and account lift",
                "expected_if_alpha_holds": "coverage passes and lift appears",
                "expected_if_counter_holds": "coverage fails or no lift",
            },
            "failure_conditions": ["L2 data unavailable"],
            "verdict": "PROCEED",
        }
        output = {
            "source_boundary": boundary,
            "falsification_consumed": {
                key: falsification[key]
                for key in ("verdict", "counter_hypothesis", "decisive_test", "failure_conditions")
            },
            "factor_batch": [{
                "name": "pre_signal_imbalance",
                "expression": "big_buy_volume - big_sell_volume",
                "family": "flow",
                "polarity": "positive",
                "transformation_type": "zscore",
                "data_requirements": [
                    "research_indicators_cache.l2_tick.big_order_buy_volume",
                    "research_indicators_cache.l2_tick.big_order_sell_volume",
                ],
            }],
        }
        decision, reason, _ = orchestrator._gate(
            "factor_engineer", output, None, {},
            {"falsification_officer": falsification},
        )
        self.assertEqual("reject", decision)
        self.assertIn("unavailable data requirements", reason)

    def test_factor_gate_rejects_post_signal_future_requirements(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        boundary = {
            "research_channel": "independent",
            "source_brief_id": None,
            "source_supported": [],
        }
        falsification = {
            "source_boundary": boundary,
            "counter_hypothesis": "post-signal smoothness is just label leakage",
            "decisive_test": {
                "method": "code_change",
                "discriminating_observation": "pre-trade availability",
                "expected_if_alpha_holds": "all fields exist before entry",
                "expected_if_counter_holds": "factor uses future bars",
            },
            "failure_conditions": ["uses future bars"],
            "verdict": "PROCEED",
        }
        output = {
            "source_boundary": boundary,
            "falsification_consumed": {
                key: falsification[key]
                for key in ("verdict", "counter_hypothesis", "decisive_test", "failure_conditions")
            },
            "factor_batch": [{
                "name": "post_signal_path_smoothness",
                "expression": "smoothness(close[t+1:t+5])",
                "family": "path",
                "polarity": "positive",
                "transformation_type": "zscore",
                "data_requirements": ["signal day后5日 OHLCV path"],
            }],
        }
        decision, reason, _ = orchestrator._gate(
            "factor_engineer", output, None, {},
            {"falsification_officer": falsification},
        )
        self.assertEqual("reject", decision)
        self.assertIn("unavailable data requirements", reason)

    def test_factor_data_gate_accepts_explicit_no_future_boundary_note(self) -> None:
        factors = [{
            "name": "signal_day_volume_divergence",
            "data_requirements": [
                "signal_day_stock_turnover (available at signal day close, no future data)",
                "signal_day_index_amount (available at signal day close, without any lookahead)",
                "past_19d_stock_turnover (all available before signal day)",
            ],
        }]

        self.assertEqual([], Orchestrator._unsupported_factor_data_requirements(factors))

    def test_factor_gate_rejects_ambiguous_centered_signal_window(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        boundary = {
            "research_channel": "independent",
            "source_brief_id": None,
            "source_supported": [],
        }
        falsification = {
            "source_boundary": boundary,
            "counter_hypothesis": "centered windows can include future bars",
            "decisive_test": {
                "method": "code_change",
                "discriminating_observation": "feature timestamp boundary",
                "expected_if_alpha_holds": "window ends at signal day",
                "expected_if_counter_holds": "window reaches after signal day",
            },
            "failure_conditions": ["window includes future bars"],
            "verdict": "PROCEED",
        }
        output = {
            "source_boundary": boundary,
            "falsification_consumed": {
                key: falsification[key]
                for key in ("verdict", "counter_hypothesis", "decisive_test", "failure_conditions")
            },
            "factor_batch": [{
                "name": "w_bottom_volume_contraction",
                "expression": "second_leg_volume / first_leg_volume",
                "family": "path_volume",
                "polarity": "negative",
                "transformation_type": "rank_zscore",
                "data_requirements": ["5-day window centered on signal day daily OHLCV"],
            }],
        }
        decision, reason, _ = orchestrator._gate(
            "factor_engineer", output, None, {},
            {"falsification_officer": falsification},
        )
        self.assertEqual("reject", decision)
        self.assertIn("unavailable data requirements", reason)

    def test_falsification_gate_recovers_scattered_source_boundary(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        boundary = {
            "research_channel": "kbase_inspired",
            "source_brief_id": "brief-test",
            "source_supported": [SOURCE_ID],
            "agent_inference": "project-side inference",
        }
        alpha = {
            "source_boundary": boundary,
            "proposed_generator": {"family": "Position", "mechanism": "risk sized cap"},
        }
        output = {
            "brief_id": "brief-test",
            "source_supported": [SOURCE_ID],
            "agent_inference": "project-side inference",
            "alpha_mechanism": {"family": "Position", "mechanism": "risk sized cap"},
            "counter_hypothesis": "position caps add no account-level edge",
            "decisive_test": {
                "method": "manual_audit",
                "discriminating_observation": "account-level lift over baseline",
                "expected_if_alpha_holds": "lower drawdown at same pool",
                "expected_if_counter_holds": "no lift or worse drawdown",
            },
            "failure_conditions": ["no account-level lift"],
            "verdict": "REVISE",
            "revision_guidance": "reduce to one position-sizing overlay",
        }
        decision, reason, _ = orchestrator._gate(
            "falsification_officer", output, None, {},
            {"alpha_hunter": alpha},
        )
        self.assertEqual(decision, "modify", reason)
        self.assertEqual(output["source_boundary"]["source_brief_id"], "brief-test")

    def test_falsification_prompt_binds_latest_alpha_generator(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        message = orchestrator._build_stage_message(
            "falsification_officer",
            {"objective": "test"},
            {
                "alpha_hunter__1": {
                    "proposed_generator": {
                        "family": "Old",
                        "mechanism": "old mechanism",
                    }
                },
                "alpha_hunter": {
                    "proposed_generator": {
                        "family": "Regime",
                        "mechanism": "latest mechanism",
                    }
                },
            },
            "topic",
        )
        self.assertIn("CURRENT_ALPHA_BINDING", message)
        self.assertIn("family: Regime", message)
        self.assertIn("mechanism: latest mechanism", message)
        self.assertIn("Do not return REVISE only to make the mechanism simpler", message)
        self.assertIn("REVISE is only for a new blocking defect", message)

    def test_falsification_gate_normalizes_mechanism_binding(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        boundary = {
            "research_channel": "independent",
            "source_brief_id": None,
            "source_supported": [],
        }
        alpha = {
            "source_boundary": boundary,
            "proposed_generator": {
                "family": "flow",
                "mechanism": "pressure persists",
            },
        }
        output = {
            "source_boundary": boundary,
            "alpha_mechanism": {
                "family": "flow",
                "mechanism": "pressure persists with an added formula",
            },
            "counter_hypothesis": "the signal is transient attention",
            "decisive_test": {
                "method": "manual_audit",
                "discriminating_observation": "persistence after the signal",
                "expected_if_alpha_holds": "pressure persists",
                "expected_if_counter_holds": "pressure disappears",
            },
            "failure_conditions": ["no persistence after the signal"],
            "verdict": "PROCEED",
        }
        decision, reason, _ = orchestrator._gate(
            "falsification_officer", output, None, {}, {"alpha_hunter": alpha}
        )
        self.assertEqual("pass", decision, reason)
        self.assertEqual(
            {"family": "flow", "mechanism": "pressure persists"},
            output["alpha_mechanism"],
        )
        self.assertEqual(
            "pressure persists with an added formula",
            output["alpha_mechanism_raw"]["mechanism"],
        )

    def test_falsification_gate_inserts_missing_mechanism_binding(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        boundary = {
            "research_channel": "independent",
            "source_brief_id": None,
            "source_supported": [],
        }
        alpha = {
            "source_boundary": boundary,
            "proposed_generator": {
                "family": "compression",
                "mechanism": "pre-signal compression is credible",
            },
        }
        output = {
            "source_boundary": boundary,
            "counter_hypothesis": "compression duplicates existing path features",
            "decisive_test": {
                "method": "code_change",
                "discriminating_observation": "fold-stable lift after ablation",
                "expected_if_alpha_holds": "ablation loses account-level lift",
                "expected_if_counter_holds": "ablation is indistinguishable",
            },
            "failure_conditions": ["no lift after ablation"],
            "verdict": "PROCEED",
        }
        decision, reason, _ = orchestrator._gate(
            "falsification_officer", output, None, {}, {"alpha_hunter": alpha}
        )
        self.assertEqual("pass", decision, reason)
        self.assertEqual(
            {
                "family": "compression",
                "mechanism": "pre-signal compression is credible",
            },
            output["alpha_mechanism"],
        )

    def test_alpha_gate_inserts_independent_boundary_only_without_source_claims(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        alpha = {
            "alpha_family_gap": {
                "existing_families": ["entry price"],
                "missing_families": ["multi-timeframe"],
                "highest_potential": "weekly context adds orthogonal information",
            },
            "proposed_generator": {
                "family": "multi-timeframe",
                "mechanism": "weekly trend state modifies Brick candidate ranking",
                "required_data": "daily OHLCV aggregated to weekly bars",
                "expected_jaccard_vs_wave_qualified": "low",
                "expected_information_gain": "tests a higher-timeframe context",
            },
        }
        decision, reason, _ = orchestrator._gate(
            "alpha_hunter", alpha, None, {}, {"source_librarian": valid_brief()}
        )
        self.assertEqual("pass", decision, reason)
        self.assertEqual("independent", alpha["source_boundary"]["research_channel"])

        strict_alpha = {
            key: value for key, value in alpha.items() if key != "source_boundary"
        }
        decision, reason, _ = orchestrator._gate(
            "alpha_hunter",
            strict_alpha,
            None,
            {},
            {"source_librarian": valid_brief()},
            require_kbase_inspired=True,
        )
        self.assertEqual("reject", decision)
        self.assertIn("cannot substitute", reason)

        claimed = dict(alpha)
        claimed.pop("source_boundary", None)
        claimed["source_supported"] = [SOURCE_ID]
        decision, reason, _ = orchestrator._gate(
            "alpha_hunter", claimed, None, {}, {"source_librarian": valid_brief()}
        )
        self.assertEqual("reject", decision)
        self.assertIn("source_brief_id", reason)

    def test_kbase_workflow_keeps_source_librarian_first(self) -> None:
        config = ResearchConfig()
        workflow = config.get_workflow("kbase_discovery")
        self.assertEqual(workflow["pipeline_order"], [
            "source_librarian", "alpha_hunter", "falsification_officer", "factor_engineer",
        ])
        self.assertEqual(
            config.get_agent("source_librarian")["tools"],
            ["kbase_overview", "kbase_browse", "kbase_search", "kbase_open", "kbase_trace"],
        )
        self.assertEqual(config.get_agent("alpha_hunter")["profile"], "deepseekv4")
        for agent_id in ("alpha_hunter", "falsification_officer", "factor_engineer"):
            self.assertFalse({"book_index", "book_open", "book_search"} & set(config.get_agent(agent_id)["tools"]))

    def test_falsification_gate_controls_factor_handoff(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        boundary = {
            "research_channel": "independent", "source_brief_id": None,
            "source_supported": [],
        }
        alpha = {
            "source_boundary": boundary,
            "proposed_generator": {"family": "flow", "mechanism": "pressure persists"},
        }

        def review(verdict: str) -> dict:
            result = {
                "source_boundary": boundary,
                "alpha_mechanism": {"family": "flow", "mechanism": "pressure persists"},
                "counter_hypothesis": "the signal is transient attention",
                "decisive_test": {
                    "method": "manual_audit",
                    "discriminating_observation": "persistence after the signal",
                    "expected_if_alpha_holds": "pressure persists",
                    "expected_if_counter_holds": "pressure disappears",
                },
                "failure_conditions": ["no persistence after the signal"],
                "verdict": verdict,
            }
            if verdict == "REVISE":
                result["revision_guidance"] = "separate persistent from one-day volume"
            return result

        self.assertEqual("pass", orchestrator._gate(
            "falsification_officer", review("PROCEED"), None, {}, {"alpha_hunter": alpha}
        )[0])
        self.assertEqual("modify", orchestrator._gate(
            "falsification_officer", review("REVISE"), None, {}, {"alpha_hunter": alpha}
        )[0])
        self.assertEqual("reject", orchestrator._gate(
            "falsification_officer", review("REJECT"), None, {}, {"alpha_hunter": alpha}
        )[0])

    def test_falsification_gate_retries_inconsistent_fold_protocol(self) -> None:
        orchestrator = Orchestrator.__new__(Orchestrator)
        boundary = {
            "research_channel": "independent",
            "source_brief_id": None,
            "source_supported": [],
        }
        alpha = {
            "source_boundary": boundary,
            "proposed_generator": {"family": "flow", "mechanism": "pressure persists"},
        }
        output = {
            "source_boundary": boundary,
            "alpha_mechanism": {"family": "flow", "mechanism": "pressure persists"},
            "counter_hypothesis": "the signal is transient attention",
            "decisive_test": {
                "method": "code_change",
                "discriminating_observation": "Run 3 folds and require improvement in 3/4 folds",
                "expected_if_alpha_holds": "3/4 folds improve",
                "expected_if_counter_holds": "no stable improvement",
            },
            "failure_conditions": ["drawdown worsens in 2/4 folds"],
            "verdict": "PROCEED",
        }

        decision, reason, _ = orchestrator._gate(
            "falsification_officer", output, None, {}, {"alpha_hunter": alpha}
        )

        self.assertEqual("retry", decision)
        self.assertIn("declared 3 folds", reason)

    def test_falsification_fold_protocol_accepts_matching_denominator(self) -> None:
        issues = Orchestrator._falsification_protocol_issues(
            {
                "method": "code_change",
                "discriminating_observation": (
                    "Run 3 folds: 2020-2022/2023/2024, "
                    "2021-2023/2024/2025, 2022-2024/2025/2026; require 3/3"
                ),
                "expected_if_alpha_holds": "3/3 folds improve",
                "expected_if_counter_holds": "fewer than 3/3 improve",
            },
            ["drawdown worsens in 2/3 folds"],
        )

        self.assertEqual([], issues)

    def test_falsification_fold_protocol_ignores_rank_buckets_and_fold_ranges(self) -> None:
        issues = Orchestrator._falsification_protocol_issues(
            {
                "method": "code_change",
                "discriminating_observation": (
                    "Run 3-fold PWF and require improvement in 2/3 folds."
                ),
                "expected_if_alpha_holds": "The same 2/3 rule beats both baselines.",
                "expected_if_counter_holds": (
                    "Top10/20 metrics improve, or folds 1-2 reverse in fold 3."
                ),
            },
            ["Account improvement fails in 2/3 folds."],
        )

        self.assertEqual([], issues)


if __name__ == "__main__":
    unittest.main()
