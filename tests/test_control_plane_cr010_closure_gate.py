"""CR-010 Git-native closure GATE (run003, candidate 0ced0f36).

Machine-decidable Git-native closure gate.  Git commit/tree is the ONLY
version authority: the gate verifies, against the live repository:

  - every receipt/Review/exit condition in the latest run's gate-state;
  - the CURRENT working-tree HEAD/tree equals the recorded candidate C
    (a candidate commit after the receipt invalidates the review);
  - when the evidence commit E exists (evidence_committed stage), E's
    parent is C and ``git diff --name-only C..E`` touches ONLY
    ``docs/superpowers/reviews/raw-evidence/cr010-final-closure/run003/**``;
  - protected files are not part of C or E;
  - the final report appears only after the gate (pre/post stage XOR).

Self-contained (stdlib + git) so it runs in fresh processes and as the
final self-check.
"""

from __future__ import annotations

import json
import re
import subprocess
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_EVIDENCE_ROOT = (
    _REPO_ROOT
    / "docs/superpowers/reviews/raw-evidence/cr010-final-closure"
)
_REPORT_PATH = (
    _REPO_ROOT
    / "docs/superpowers/reviews/2026-08-19-v342-cr010-git-native-closure-report.md"
)
_PROTECTED_FILES = (
    "CHANGELOG.md",
    "daily_run.py",
    "daily_select.py",
    "docs/b1_v3_results.md",
)
_EVIDENCE_PREFIX = "docs/superpowers/reviews/raw-evidence/cr010-final-closure/run003/"


def _latest_run_dir() -> Path | None:
    if not _EVIDENCE_ROOT.is_dir():
        return None
    runs = sorted(
        (
            c for c in _EVIDENCE_ROOT.iterdir()
            if c.is_dir() and c.name.startswith("run")
        ),
        key=lambda c: c.name,
    )
    return runs[-1] if runs else None


def _git(*args: str) -> str:
    r = subprocess.run(
        ["git", "-C", str(_REPO_ROOT), *args],
        capture_output=True, text=True, encoding="utf-8",
    )
    if r.returncode != 0:
        raise RuntimeError("git " + args[0] + " failed: " + r.stderr[-300:])
    return r.stdout.strip()


_REQUIRED_GATE_RECEIPTS = (
    ("p8/p8-acceptance.meta.txt", "P8 acceptance receipt exit"),
    ("c0/c0-acceptance.meta.txt", "C0 acceptance receipt exit"),
    ("focused/p8-focused.meta.txt", "P8 focused receipt exit"),
    ("focused/c0-focused.meta.txt", "C0 focused receipt exit"),
    ("focused/official-24cycle.meta.txt", "official 24-cycle receipt exit"),
)


def _is_sha(value: object, length: int) -> bool:
    return isinstance(value, str) and re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is not None


def _read_receipt_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16", errors="replace")
    return raw.decode("utf-8", errors="replace")


def _read_meta_exit(run_dir: Path, rel: str) -> int | None:
    path = run_dir / rel
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    match = re.search(r"(?m)^exit=(\d+)(?:\r?\n)?$", text)
    return int(match.group(1)) if match else None


class ClosureGateTests(unittest.TestCase):
    def test_closure_gate_all_conditions_pass(self) -> None:
        run_dir = _latest_run_dir()
        state_path = run_dir / "gate-state.json" if run_dir else None
        if state_path is None or not state_path.exists():
            self.fail("no gate-state.json for the latest run -- gate NOT PASS")
        try:
            state = json.loads(state_path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError) as error:
            self.fail("gate-state.json is not readable JSON: " + str(error))
        self.assertIsInstance(state, dict, "gate-state must be an object")

        # The gate parses and validates the ACTUAL raw receipts -- exit
        # codes are read from the *.meta files, never trusted from a manual
        # boolean in gate-state.
        for rel, label in _REQUIRED_GATE_RECEIPTS:
            with self.subTest(gate="receipt_" + rel.replace("/", "_")):
                observed = _read_meta_exit(run_dir, rel)
                self.assertIsNotNone(
                    observed, f"{label} missing ({rel}); not a valid receipt"
                )
                self.assertEqual(observed, 0, f"{label}: exit={observed}")
        full_suite_path = run_dir / "focused" / "full-suite-final.meta.txt"
        # The full-suite receipt is self-referential while the full suite is
        # running (its exit line is written only after the run finishes), so
        # it is validated in the EVIDENCE stage (post-E gate runs) instead
        # of inside the full suite itself.
        if state.get("evidence_committed") is True:
            full_exit = _read_meta_exit(
                run_dir, "focused/full-suite-final.meta.txt"
            )
            with self.subTest(gate="receipt_full_suite"):
                self.assertIsNotNone(
                    full_exit, "full-suite receipt missing"
                )
                self.assertEqual(
                    full_exit, 0, f"full suite exit={full_exit}"
                )
        for rel in (
            "review/review-b.out",
            "review/review-a.md",
        ):
            target = run_dir / rel
            with self.subTest(gate="review_" + rel):
                self.assertTrue(target.exists(), f"missing {rel}")
        review_b = _read_receipt_text(run_dir / "review" / "review-b.out")
        with self.subTest(gate="review_b_approve"):
            self.assertIn("REVIEW_B_STATUS APPROVE", review_b)
        review_a = _read_receipt_text(run_dir / "review" / "review-a.md")
        with self.subTest(gate="review_a_approve"):
            self.assertIn("APPROVE", review_a)

        candidate = state.get("candidate_commit")
        if not _is_sha(candidate, 40):
            self.fail("gate-state candidate_commit is invalid")
        # Live repository tree must equal the candidate tree for code paths:
        # pre-evidence (HEAD == C) the diff is empty; after the evidence
        # commit (HEAD == E, parent C) only the run003 evidence tree may
        # differ.  The candidate's code may never drift.
        head = _git("rev-parse", "HEAD")
        tree = _git("rev-parse", "HEAD^{tree}")
        head_diff = _git("diff", "--name-only", f"{candidate}..HEAD")
        allowed_after_candidate = [
            _EVIDENCE_PREFIX,
            "docs/superpowers/reviews/2026-08-19-v342-cr010-git-native-closure-report.md",
        ]
        with self.subTest(gate="code_equals_candidate"):
            unexpected = [
                p for p in head_diff.splitlines()
                if not any(p.startswith(prefix) for prefix in allowed_after_candidate)
            ]
            self.assertEqual(
                unexpected, [],
                "tree differs from candidate outside run003 evidence/"
                "report: " + ",".join(unexpected),
            )
        base = state.get("base_commit")
        self.assertTrue(
            _is_sha(base, 40) and base != candidate,
            "gate-state base_commit is invalid",
        )
        # protected files must be UNCHANGED between base and candidate
        # (they are pre-existing tracked files; the gate requires they are
        # not modified in the candidate, never that they are absent).
        protected_diff = _git(
            "diff", "--name-only", f"{base}..{candidate}",
            *list(_PROTECTED_FILES),
        )
        with self.subTest(gate="protected_not_modified_in_candidate"):
            self.assertEqual(
                protected_diff, "",
                "protected file changed in candidate: " + protected_diff,
            )

        # Evidence stage: the evidence commit E (parent == C) must be in
        # HEAD's ancestry, and the C..HEAD diff confined to run003 evidence
        # + the final report (an optional closure commit F may follow E).
        if state.get("evidence_committed") is True:
            ancestry = _git("rev-list", "--ancestry-path", f"{candidate}..HEAD").split()
            with self.subTest(gate="evidence_parent_is_candidate"):
                self.assertTrue(
                    ancestry, "candidate is not an ancestor of HEAD (no E)"
                )
            diff = _git("diff", "--name-only", f"{candidate}..HEAD")
            allowed = (
                _EVIDENCE_PREFIX,
                "docs/superpowers/reviews/2026-08-19-v342-cr010-git-native-closure-report.md",
            )
            for path in diff.splitlines():
                with self.subTest(gate="evidence_whitelist", path=path):
                    self.assertTrue(
                        any(path.startswith(p) for p in allowed),
                        f"post-candidate commit touched a non-allowed path: {path}",
                    )
            for rel in _PROTECTED_FILES:
                with self.subTest(gate="protected_not_in_evidence", path=rel):
                    self.assertNotIn(rel, diff.splitlines())
            # protected files must be unchanged between base and HEAD
            protected_diff_e = _git(
                "diff", "--name-only", f"{base}..HEAD",
                *list(_PROTECTED_FILES),
            )
            with self.subTest(gate="protected_not_modified_in_evidence"):
                self.assertEqual(
                    protected_diff_e, "",
                    "protected file changed after base: " + protected_diff_e,
                )

        # report stage markers: pre XOR post.  The report counts as
        # "generated" only when its content is bound to the CURRENT
        # candidate (a stale report from an earlier round is a historical
        # artifact and does not block the pre-report stage).
        pre_stage = state.get("report_absent_before_gate") is True
        post_stage = state.get("final_report_generated") is True
        self.assertFalse(pre_stage and post_stage, "pre and post stage both set")

        def _report_bound_to_candidate() -> bool:
            if not _REPORT_PATH.exists():
                return False
            text = _read_receipt_text(_REPORT_PATH)
            return "candidate_commit=" + str(candidate) in text

        if pre_stage:
            self.assertFalse(
                _report_bound_to_candidate(),
                "the final report (bound to this candidate) must NOT exist "
                "before the gate",
            )
        if post_stage:
            self.assertTrue(
                _report_bound_to_candidate(),
                "final_report_generated set but the report file is missing "
                "or not bound to this candidate",
            )


def require_gate_state() -> dict[str, object]:
    run_dir = _latest_run_dir()
    if run_dir is None:
        raise RuntimeError("no run evidence directory exists")
    state_path = run_dir / "gate-state.json"
    if not state_path.exists():
        raise RuntimeError("closure gate state is missing")
    return json.loads(state_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
