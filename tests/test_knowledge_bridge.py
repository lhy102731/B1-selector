from __future__ import annotations

import tempfile
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path

from ag2_research.knowledge_bridge import (
    build_validated_claim_context,
    load_validated_claims,
    write_experiment_output,
)
from research_automation.control_plane.contracts import SideEffect
from research_automation.control_plane.sink_guard import ExecutionAuthorizationError


VALID_CLAIM = """---
type: claim
status: reviewed
claim_id: b1-example-001
project_subjects: [b1_v3]
sources: [wiki/sources/example.md]
validation_status: validated
evidence_level: project_validated
information_available_at: signal bar close
lookahead_review: passed
execution_review: passed
project_kb_version: 1.0.0
confidence: high
---

# Example Validated Claim

## Claim

The signal is available only after the bar closes.

## Limits / Failure Cases

Do not use an intrabar value in a close-to-close backtest.
"""


DRAFT_CLAIM = VALID_CLAIM.replace("status: reviewed", "status: draft").replace(
    "claim_id: b1-example-001", "claim_id: b1-draft-001"
)


class KnowledgeBridgeTests(unittest.TestCase):
    def _vault(self, root: Path) -> Path:
        (root / "wiki" / "claims").mkdir(parents=True)
        (root / "wiki" / "outputs").mkdir(parents=True)
        return root

    def test_only_explicitly_validated_claims_are_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = self._vault(Path(tmp))
            (vault / "wiki" / "claims" / "valid.md").write_text(VALID_CLAIM, encoding="utf-8")
            (vault / "wiki" / "claims" / "draft.md").write_text(DRAFT_CLAIM, encoding="utf-8")

            claims = load_validated_claims("b1", vault_path=vault)

            self.assertEqual([claim.claim_id for claim in claims], ["b1-example-001"])
            self.assertEqual(claims[0].subject, "b1_v3")

    def test_context_excludes_unvalidated_notes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = self._vault(Path(tmp))
            (vault / "wiki" / "claims" / "valid.md").write_text(VALID_CLAIM, encoding="utf-8")
            (vault / "wiki" / "claims" / "draft.md").write_text(DRAFT_CLAIM, encoding="utf-8")

            context = build_validated_claim_context("b1_v3", vault_path=vault)

            self.assertIn("b1-example-001", context)
            self.assertNotIn("b1-draft-001", context)
            self.assertIn("Project hard constraints always take precedence", context)

    def test_experiment_writeback_requires_authority_before_directory_creation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = self._vault(Path(tmp))
            with self.assertRaises(ExecutionAuthorizationError):
                write_experiment_output(
                    subject="b1",
                    cycle_id="cycle-unauthorized",
                    round_n=1,
                    entry={"experiment_id": "exp-unauthorized"},
                    vault_path=vault,
                )

            self.assertFalse(
                (vault / "wiki" / "outputs" / "projects" / "b1_v3").exists()
            )


    def test_experiment_writeback_is_output_only_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = self._vault(Path(tmp))
            entry = {
                "experiment_id": "exp-001",
                "_experiment_status": "COMPLETED",
                "promotion_status": "tested",
                "hypothesis": "Test one bounded parameter change.",
                "params": {"threshold": 1.0},
                "metrics": {"sharpe": 1.2, "trades": 42},
                "delta_vs_baseline": {"sharpe": 0.1, "trades": 2},
                "evidence_claim_ids": ["b1-example-001"],
            }
            kwargs = {
                "subject": "b1",
                "cycle_id": "cycle-001",
                "round_n": 1,
                "entry": entry,
                "baseline": {"sharpe": 1.1, "trades": 40},
                "artifact_paths": ["research_state/b1_v3/cycle_log.yaml"],
                "vault_path": vault,
                "project_root": Path(tmp) / "project",
            }

            sink = MagicMock()
            sink.authorize.return_value = object()
            lease = object()
            invocation = object()
            with patch(
                "ag2_research.knowledge_bridge.AuthorizedPathMutation",
                return_value=sink,
            ):
                first = write_experiment_output(
                    **kwargs,
                    lease=lease,
                    invocation=invocation,
                    authority_reader=MagicMock(),
                    repository_root=Path(tmp),
                )
                second = write_experiment_output(
                    **kwargs,
                    lease=lease,
                    invocation=invocation,
                    authority_reader=MagicMock(),
                    repository_root=Path(tmp),
                )
            text = first.read_text(encoding="utf-8")

            self.assertEqual(first, second)
            self.assertEqual(text.count("<!-- experiment:exp-001 -->"), 1)
            self.assertIn("promotion_status: output_only", text)
            self.assertIn("validation_status: unreviewed", text)
            self.assertIn("b1-example-001", text)
            self.assertTrue((vault / "wiki" / "outputs" / "projects" / "index.md").exists())
            sink.authorize.assert_called()
            self.assertIs(lease, sink.authorize.call_args_list[0].args[0])
            self.assertIs(invocation, sink.authorize.call_args_list[0].args[1])
            authorization = sink.authorize.call_args.kwargs
            self.assertEqual("KBASE_WRITE", authorization["operation"])
            self.assertIs(SideEffect.WRITE_KBASE, authorization["effect"])
            self.assertEqual(
                authorization["paths"],
                (
                    vault / "wiki" / "outputs" / "projects",
                    vault / "wiki" / "outputs" / "projects" / "b1_v3",
                    vault / "wiki" / "outputs" / "projects" / "b1_v3" / "Cycle cycle-001.md",
                    vault / "wiki" / "outputs" / "projects" / "index.md",
                ),
            )


if __name__ == "__main__":
    unittest.main()
