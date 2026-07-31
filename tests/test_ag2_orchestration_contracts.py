from __future__ import annotations

import unittest
from unittest.mock import patch
from ag2_research.orchestrator import Orchestrator
from ag2_research.config import ResearchConfig
from research_automation.autonomous_runner import AutonomousRunnerV1


class _RegistryGate:
    def __init__(self):
        self.seen = None

    def classify(self, hypothesis):
        self.seen = hypothesis
        return {"action": "pass", "registry_status": "none", "matched_id": None, "overlap": 0}


class _Router:
    def __init__(self):
        self.registry_gate = _RegistryGate()

    def build_packet(self, objective=""):
        return {"current_objective": objective}


class _WorkflowConfig:
    _raw = {"control_layer": {}}

    def get_workflow(self, workflow_id):
        return {
            "pipeline_order": [
                "statistician", "experiment_executor", "statistician", "risk_controller"
            ]
        }


class _NeverFallbackProposer:
    def __init__(self):
        self.called = False

    def propose(self, count):
        self.called = True
        return []


def _kbase_bias_check():
    return {
        "source_density_bias": "medium",
        "why_not_source_abundance": "chosen for orthogonal information, not source volume",
        "underexplored_alternative_considered": "sparse breadth branch considered",
        "novelty_or_reopen_reason": "tests a new mechanism family",
    }


def _forward_validation():
    return {
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
        "embargo_days": 20,
        "selection_rule": "select on train and validation only; test is used once unseen",
        "test_summary": {
            "average_test_metrics": {"return": 0.1},
            "worst_fold_metrics": {"return": -0.01},
            "fold_pass_rate": 2 / 3,
            "dispersion": {"return_std": 0.02},
        },
    }


def _compute_acceleration():
    return {
        "workload_type": "ranker_training",
        "gpu_applicable": True,
        "gpu_available": False,
        "selected_backend": "cpu",
        "fallback_backend": "cpu",
        "reason": "nvidia-smi not found",
        "devices": [],
    }


def _learning_scope():
    return {
        "mechanisms": ["volume_contraction_rebound"],
        "usage_modes": ["factor_candidate"],
        "market_regimes": ["all"],
        "time_windows": [{"start": "2020-01-01", "end": "2026-12-31"}],
        "universes": ["a_share"],
        "liquidity_buckets": ["production_minimum"],
        "label_protocol_families": ["rolling_forward_v1"],
        "generation_families": ["b1_v342"],
    }


class AG2OrchestrationContractTests(unittest.TestCase):
    def test_research_proposal_is_blocked_by_committed_learning_before_registry(self):
        orchestrator = Orchestrator.__new__(Orchestrator)
        router = _Router()
        output = {
            "proposal": {
                "hypothesis": "volume contraction predicts rebound",
                "alpha_source": "B1 pullback family",
                "scope": _learning_scope(),
                "novelty_justification": "tests a bounded interaction",
                "success_criteria": "improve account metrics",
                "experiment_spec": {"param": "j_max", "values": [20, 30]},
                "requested_next_role": "Data_Validator",
            }
        }
        with (
            patch(
                "research_automation.control_plane.memory."
                "CommittedLearningLedgerReader.read_claims",
                return_value=[{"claim_id": "committed-negative"}],
            ) as reader,
            patch(
                "research_automation.control_plane.memory.LearningGate.classify",
                return_value={
                    "enforcement": "HARD_BLOCK",
                    "hard_block_claim_ids": ["committed-negative"],
                    "scoped_block_claims": [],
                    "warning_codes": [],
                },
            ) as learning_gate,
        ):
            decision, reason, _ = orchestrator._gate(
                "research_proposer", output, router, {}
            )

        self.assertEqual("reject", decision)
        self.assertIn("HARD_BLOCK", reason)
        self.assertIsNone(router.registry_gate.seen)
        reader.assert_called_once_with()
        proposal = learning_gate.call_args.args[0]
        self.assertEqual(_learning_scope(), proposal["scope"])
        self.assertRegex(proposal["execution_identity"], r"^[0-9a-f]{64}$")
        self.assertRegex(proposal["semantic_identity"], r"^[0-9a-f]{64}$")

    def test_custom_agent_invoker_cannot_bypass_committed_learning(self):
        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator.config = _WorkflowConfig()
        with patch(
            "research_automation.control_plane.memory."
            "CommittedLearningLedgerReader.read_projection_input",
            side_effect=RuntimeError("committed ledger unavailable"),
        ):
            with self.assertRaisesRegex(RuntimeError, "committed ledger unavailable"):
                orchestrator.run_sequential_workflow(
                    "test",
                    topic="bounded test",
                    research_context="trusted test context",
                    agent_invoker=lambda *_args: {},
                    memory_router=_Router(),
                )

    def test_custom_agent_invoker_receives_stage_context_bundle(self):
        class SingleStageConfig:
            _raw = {"control_layer": {}}

            @staticmethod
            def get_workflow(_workflow_id):
                return {"pipeline_order": ["theory_builder"]}

        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator.config = SingleStageConfig()
        seen = {}

        def invoke(stage, _packet, _last_outputs, _topic, context_bundle):
            seen[stage] = context_bundle
            return {
                "theory_hypothesis": {
                    "mechanism": "temporary supply absorption",
                    "expected_market": "range-to-recovery",
                    "failure_mode": "broad risk-off selloff",
                    "observable_signature": "volume contracts before recovery",
                    "falsification_link": None,
                }
            }

        with patch.object(
            orchestrator,
            "_prepare_v342_agent_context",
            return_value=(
                {"theory_builder": "TRUSTED_CONTEXT_SENTINEL"},
                {
                    "theory_builder": [
                        {"role": "user", "content": "UNTRUSTED_CONTEXT_SENTINEL"}
                    ]
                },
            ),
        ):
            result = orchestrator.run_sequential_workflow(
                "test",
                topic="bounded test",
                research_context="provided",
                agent_invoker=invoke,
                memory_router=_Router(),
            )

        self.assertEqual("APPROVED", result["status"])
        self.assertEqual(
            {
                "schema_version": "control_plane.custom_invoker_context.v1",
                "trusted_system_message": "TRUSTED_CONTEXT_SENTINEL",
                "untrusted_messages": [
                    {"role": "user", "content": "UNTRUSTED_CONTEXT_SENTINEL"}
                ],
            },
            seen["theory_builder"],
        )

    def test_stage_prompt_never_serializes_legacy_memory_packet(self):
        prompt = Orchestrator._build_stage_message(
            Orchestrator.__new__(Orchestrator),
            "factor_engineer",
            {"legacy_secret": "RAW_MEMORY_PACKET_SENTINEL"},
            {},
            "bounded objective",
        )

        self.assertNotIn("RAW_MEMORY_PACKET_SENTINEL", prompt)
        self.assertNotIn("memory_packet:", prompt)

    def test_ag2_candidate_failure_does_not_downgrade_to_parameter_proposer(self):
        runner = AutonomousRunnerV1.__new__(AutonomousRunnerV1)
        runner.auto_source = "ag2"
        runner.proposer = _NeverFallbackProposer()
        runner.adapter = None
        runner._grid = {}
        runner._ag2_round = lambda count: (_ for _ in ()).throw(RuntimeError("provider down"))

        with self.assertRaisesRegex(RuntimeError, "AG2 candidate generation failed"):
            runner._generate_auto(2, None)
        self.assertFalse(runner.proposer.called)

    def setUp(self):
        self.orchestrator = Orchestrator.__new__(Orchestrator)

    def test_fenced_yaml_and_nested_proposal_are_parsed_and_gated(self):
        output = self.orchestrator._parse_stage_output(
            "research_proposer",
            "```yaml\nproposal:\n  hypothesis: volume contraction predicts rebound\n"
            "  alpha_source: B1 pullback family\n"
            "  scope:\n"
            "    mechanisms: [volume_contraction_rebound]\n"
            "    usage_modes: [factor_candidate]\n"
            "    market_regimes: [all]\n"
            "    time_windows: [{start: '2020-01-01', end: '2026-12-31'}]\n"
            "    universes: [a_share]\n"
            "    liquidity_buckets: [production_minimum]\n"
            "    label_protocol_families: [rolling_forward_v1]\n"
            "    generation_families: [b1_v342]\n"
            "  novelty_justification: tests a new volume interaction\n"
            "  success_criteria: improve account metrics\n"
            "  experiment_spec: {param: j_max, values: [20, 30]}\n"
            "  requested_next_role: Data_Validator\n```",
        )
        router = _Router()
        with patch(
            "research_automation.control_plane.memory."
            "CommittedLearningLedgerReader.read_claims",
            return_value=[],
        ):
            decision, _, _ = self.orchestrator._gate(
                "research_proposer", output, router, {}
            )
        self.assertEqual("pass", decision)
        self.assertEqual("volume contraction predicts rebound", router.registry_gate.seen)

    def test_nested_verdict_contracts_are_enforced(self):
        self.assertEqual(
            "pass",
            self.orchestrator._gate(
                "data_validator", {"data_verdict": {
                "fields_required": ["close"],
                "production_available": {"close": "yes"},
                "leakage_risk": "none",
                "data_consistency": "GBK and cache schema checked",
                "forward_validation_design": _forward_validation(),
                "verdict": "PASS", "blocking_reasons": [],
                "next_role_if_pass": "Experiment_Executor",
            }}, None, {}
            )[0],
        )
        valid_risk = {
            "execution_risk": "none", "robustness_risk": "bounded",
            "forward_validation_risk": "pass",
            "regime_risk": "tested", "deployment_risk": "bounded",
            "baseline_comparison": "fair", "escalation_triggered": [],
            "verdict": "INVALID", "rationale": "failed robustness threshold",
        }
        self.assertEqual(
            "reject",
            self.orchestrator._gate(
                "risk_controller", {"risk_verdict": valid_risk}, None, {}
            )[0],
        )
        valid_risk["verdict"] = "INCONCLUSIVE"
        self.assertEqual(
            "escalate",
            self.orchestrator._gate(
                "risk_controller", {"risk_verdict": valid_risk}, None, {}
            )[0],
        )

    def test_source_boundary_must_match_upstream_outputs(self):
        brief = {
            "brief_id": "brief-1",
            "sources_consulted": [{"source_id": "source-1"}],
        }
        alpha = {
            "source_boundary": {
                "research_channel": "kbase_inspired", "source_brief_id": "brief-1",
                "source_supported": ["source-1"],
            },
            "alpha_family_gap": {
                "existing_families": ["trend"], "missing_families": ["flow"],
                "highest_potential": "flow adds orthogonal information",
            },
            "kbase_bias_check": _kbase_bias_check(),
            "proposed_generator": {
                "family": "flow", "mechanism": "buying pressure persists",
                "required_data": "daily volume, available",
                "expected_jaccard_vs_wave_qualified": "low",
                "expected_information_gain": "orthogonal demand test",
            },
        }
        self.assertEqual(
            "pass",
            self.orchestrator._gate(
                "alpha_hunter", alpha, None, {}, {"source_librarian": brief}
            )[0],
        )
        changed = {"source_boundary": {**alpha["source_boundary"], "source_supported": ["invented"]}}
        self.assertEqual(
            "reject",
            self.orchestrator._gate(
                "factor_engineer", changed, None, {}, {"alpha_hunter": alpha}
            )[0],
        )

    def test_discovery_roles_require_substantive_payloads(self):
        brief = {"brief_id": "brief-1", "sources_consulted": [{"source_id": "source-1"}]}
        boundary = {
            "research_channel": "kbase_inspired",
            "source_brief_id": "brief-1",
            "source_supported": ["source-1"],
        }
        self.assertEqual(
            "reject",
            self.orchestrator._gate(
                "alpha_hunter", {"source_boundary": boundary}, None, {},
                {"source_librarian": brief},
            )[0],
        )
        alpha = {
            "source_boundary": boundary,
            "alpha_family_gap": {
                "existing_families": ["trend"],
                "missing_families": ["flow"],
                "highest_potential": "flow because it adds orthogonal information",
            },
            "kbase_bias_check": _kbase_bias_check(),
            "proposed_generator": {
                "family": "flow",
                "mechanism": "persistent buying pressure precedes continuation",
                "required_data": "daily volume and price, available",
                "expected_jaccard_vs_wave_qualified": "low",
                "expected_information_gain": "tests an orthogonal demand signal",
            },
        }
        self.assertEqual(
            "pass",
            self.orchestrator._gate(
                "alpha_hunter", alpha, None, {}, {"source_librarian": brief}
            )[0],
        )
        without_bias = dict(alpha)
        without_bias.pop("kbase_bias_check")
        self.assertEqual(
            "reject",
            self.orchestrator._gate(
                "alpha_hunter", without_bias, None, {}, {"source_librarian": brief}
            )[0],
        )
        self.assertEqual(
            "reject",
            self.orchestrator._gate(
                "factor_engineer", {"source_boundary": boundary, "factor_batch": []},
                None, {}, {"alpha_hunter": alpha},
            )[0],
        )

    def test_operational_roles_reject_empty_or_incomplete_outputs(self):
        for stage in (
            "research_proposer", "data_validator", "experiment_executor",
            "risk_controller", "strategy_synthesizer", "statistician",
            "research_historian",
        ):
            with self.subTest(stage=stage):
                self.assertEqual(
                    "reject", self.orchestrator._gate(stage, {"_raw": ""}, None, {})[0]
                )

        execution = {"execution_record": {
            "command": "python backtest.py --research-indicators-cache",
            "config": {"j": 29, "indicators_cache_name": "research_indicators_cache"},
            "date_range": "2021-01-01..2025-12-31",
            "forward_validation": _forward_validation(),
            "compute_acceleration": _compute_acceleration(),
            "metrics": {"return": 0.1, "trades": 20}, "output_files": ["result.csv"],
            "sanity_check": "two values produced distinct results", "anomaly_flag": "none",
        }}
        self.assertEqual("pass", self.orchestrator._gate(
            "experiment_executor", execution, None, {}
        )[0])
        missing_forward = {"execution_record": {
            "command": "python backtest.py", "config": {"j": 29},
            "date_range": "2021-01-01..2025-12-31",
            "compute_acceleration": _compute_acceleration(),
            "metrics": {"return": 0.1, "trades": 20}, "output_files": ["result.csv"],
            "sanity_check": "two values produced distinct results", "anomaly_flag": "none",
        }}
        self.assertEqual("reject", self.orchestrator._gate(
            "experiment_executor", missing_forward, None, {}
        )[0])
        missing_compute = {"execution_record": {
            "command": "python backtest.py", "config": {"j": 29},
            "date_range": "2021-01-01..2025-12-31",
            "forward_validation": _forward_validation(),
            "metrics": {"return": 0.1, "trades": 20}, "output_files": ["result.csv"],
            "sanity_check": "two values produced distinct results", "anomaly_flag": "none",
        }}
        self.assertEqual("reject", self.orchestrator._gate(
            "experiment_executor", missing_compute, None, {}
        )[0])

        missing_cache_isolation = {"execution_record": {
            "command": "python backtest.py", "config": {"j": 29},
            "date_range": "2021-01-01..2025-12-31",
            "forward_validation": _forward_validation(),
            "compute_acceleration": _compute_acceleration(),
            "metrics": {"return": 0.1, "trades": 20}, "output_files": ["result.csv"],
            "sanity_check": "two values produced distinct results", "anomaly_flag": "none",
        }}
        self.assertEqual("reject", self.orchestrator._gate(
            "experiment_executor", missing_cache_isolation, None, {}
        )[0])

        prediction = {"prediction": {
            "metrics": {"return": 0.1}, "basis": "current baseline", "locked_at": "now"
        }}
        self.assertEqual("pass", self.orchestrator._gate(
            "statistician", prediction, None, {}
        )[0])

    def test_automation_candidate_workflow_cannot_execute_backtests(self):
        workflow_id = AutonomousRunnerV1.AG2_CANDIDATE_WORKFLOW
        workflow = ResearchConfig().get_workflow(workflow_id)
        self.assertEqual("proposal_gate", workflow_id)
        self.assertNotIn("experiment_executor", workflow.get("pipeline_order", []))

    def test_repeated_stage_preserves_first_and_latest_outputs(self):
        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator.config = _WorkflowConfig()
        seen_by_risk = {}
        statistician_runs = 0

        def invoke(stage, _packet, last_outputs, _topic, _context_bundle):
            nonlocal statistician_runs
            if stage == "statistician":
                statistician_runs += 1
                if statistician_runs == 1:
                    return {"prediction": {
                        "metrics": {"return": 0.1}, "basis": "baseline", "locked_at": "now"
                    }}
                return {
                    "actual_metrics": {"return": 0.2},
                    "surprise": {"per_metric": {"return": 1.0}, "max_surprise_score": 1.0,
                                 "surprise_metric": "return"},
                    "robustness_verdict": {"passes_acceptance_bar": True},
                }
            if stage == "experiment_executor":
                return {"execution_record": {
                    "command": "python backtest.py --research-indicators-cache",
                    "config": {"j": 29, "indicators_cache_name": "research_indicators_cache"},
                    "date_range": "2020..2026",
                    "forward_validation": _forward_validation(),
                    "compute_acceleration": _compute_acceleration(),
                    "metrics": {"return": 0.2},
                    "output_files": ["result.csv"], "sanity_check": "distinct results",
                    "anomaly_flag": "none",
                }}
            seen_by_risk.update(last_outputs)
            return {"risk_verdict": {
                "execution_risk": "none", "robustness_risk": "bounded",
                "forward_validation_risk": "pass",
                "regime_risk": "tested", "deployment_risk": "bounded",
                "baseline_comparison": "fair", "escalation_triggered": [],
                "verdict": "VALID", "rationale": "all checks passed",
            }}

        result = orchestrator.run_sequential_workflow(
            "test",
            topic="test",
            research_context="provided",
            agent_invoker=invoke,
            memory_router=_Router(),
        )

        self.assertEqual("APPROVED", result["status"])
        self.assertIn("prediction", seen_by_risk["statistician__1"])
        self.assertIn("actual_metrics", seen_by_risk["statistician"])

    def test_theory_builder_gate_requires_structured_causal_contract(self):
        orchestrator = Orchestrator.__new__(Orchestrator)
        valid = {"theory_hypothesis": {
            "mechanism": "temporary supply absorption",
            "expected_market": "range-to-recovery",
            "failure_mode": "broad risk-off selloff",
            "observable_signature": "volume contracts before recovery",
            "falsification_link": None,
        }}

        self.assertEqual("pass", orchestrator._gate("theory_builder", valid, None, {})[0])
        invalid = {"theory_hypothesis": {"mechanism": "too short"}}
        self.assertEqual("reject", orchestrator._gate("theory_builder", invalid, None, {})[0])

if __name__ == "__main__":
    unittest.main()
