"""P6A -- CandidatePool legacy-provenance vertical slice.

Every candidate-pool entry is a legacy research output: CandidatePool.add must
stamp it with the control-plane legacy provenance (controller_created=False,
trust_state=legacy_unaudited, promotion_eligible=False) BEFORE candidate_pool.yaml
is written, and return the same stamped entry. The stamp is non-overridable:
self-promoting values supplied through the public experiment surface
(proposal.scope) must be overwritten. Automation still can never write
'promoted'.

Public-interface tests only: a real minimal Experiment flows through
CandidatePool.add onto a temp path under the real safe output root, then the
YAML is read back.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from research_automation.experiment import Experiment, Proposal, StandardMetrics
from research_automation.promotion import CandidatePool
from research_automation.safety import output_root


class CandidatePoolLegacyProvenanceTests(unittest.TestCase):
    """CandidatePool.add -> persisted/returned entries carry exact stamps."""

    def _experiment(self, **scope_extra: object) -> Experiment:
        scope = {
            "params": {"j_threshold": 29},
            "evidence_claim_ids": ["p6a-claim"],
            "code_change": {"file": "research_automation/promotion.py"},
        }
        scope.update(scope_extra)
        return Experiment(
            experiment_id="p6a-legacy-provenance",
            strategy="brick",
            proposal=Proposal(
                hypothesis="pool entries carry the legacy provenance stamp",
                scope=scope,
            ),
            metrics=StandardMetrics(
                sharpe=1.25,
                max_drawdown=-0.12,
                win_rate=0.61,
                trades=12,
                extra={"total_return": 0.32},
                source="backtest",
            ),
            report_path="research_automation/_output/reports/brick_r1_20260805_000000Z.md",
        )

    def _candidate_pool(self) -> tuple[CandidatePool, Path]:
        """CandidatePool writing to a temp dir under the real safe output root
        (research_automation/_output is gitignored), so the genuine
        assert_safe_path guard is exercised with no repo dirt."""
        output_root().mkdir(parents=True, exist_ok=True)
        directory = tempfile.TemporaryDirectory(dir=output_root())
        pool_path = Path(directory.name) / "candidate_pool.yaml"
        self.addCleanup(directory.cleanup)
        return CandidatePool(pool_path), pool_path

    def test_add_stamps_legacy_provenance_in_return_and_on_disk(self) -> None:
        baseline = StandardMetrics(sharpe=0.90)
        pool, pool_path = self._candidate_pool()

        entry = pool.add(self._experiment(), "tested", baseline)

        # returned entry carries the exact stamps
        self.assertFalse(entry["controller_created"])
        self.assertEqual("legacy_unaudited", entry["trust_state"])
        self.assertFalse(entry["promotion_eligible"])

        # persisted entry carries the exact same stamps
        on_disk = yaml.safe_load(pool_path.read_text(encoding="utf-8")) or {}
        persisted = on_disk["candidates"]
        self.assertEqual(1, len(persisted))
        self.assertEqual(entry, persisted[0])  # same stamped record that was returned
        self.assertFalse(persisted[0]["controller_created"])
        self.assertEqual("legacy_unaudited", persisted[0]["trust_state"])
        self.assertFalse(persisted[0]["promotion_eligible"])

    def test_scope_cannot_self_promote_through_public_surface(self) -> None:
        baseline = StandardMetrics(sharpe=0.90)
        pool, pool_path = self._candidate_pool()

        # caller supplies self-promoting values via the public experiment surface
        entry = pool.add(
            self._experiment(
                controller_created=True,
                trust_state="controlled_research",
                promotion_eligible=True,
            ),
            "verified",
            baseline,
        )

        self.assertFalse(entry["controller_created"])
        self.assertEqual("legacy_unaudited", entry["trust_state"])
        self.assertFalse(entry["promotion_eligible"])

        persisted = (yaml.safe_load(pool_path.read_text(encoding="utf-8")) or {})[
            "candidates"
        ][0]
        self.assertFalse(persisted["controller_created"])
        self.assertEqual("legacy_unaudited", persisted["trust_state"])
        self.assertFalse(persisted["promotion_eligible"])

    def test_add_refuses_promoted_status_without_any_write(self) -> None:
        pool, pool_path = self._candidate_pool()

        with self.assertRaises(AssertionError):
            pool.add(self._experiment(), "promoted", StandardMetrics(sharpe=0.90))

        self.assertFalse(pool_path.exists())


if __name__ == "__main__":
    unittest.main()
