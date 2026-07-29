"""workspace_manager.py -- Experiment Workspace Sandbox.

Creates an isolated workspace per experiment so that future automated code
changes (e.g. by Claude Code / AG2) NEVER touch production code. The backtest
is launched from the workspace copy of the entrypoint script, which makes
``from strategy.*`` resolve to the workspace strategy copy (the script's
``sys.path.insert(0, Path(__file__).parent)`` points at the workspace, not the
repo root). Meanwhile ``cwd`` stays at the project root so ``Path('data')`` and
``from utils.*`` still hit production data/utils -- data is never copied.

Safety: all writes go under ``research_automation/_output/runs/<cycle>/experiments/
<experiment_id>/workspace/`` (already inside safety.py's SAFE_WRITE_ROOTS, so
safety.py is NOT modified). Production ``strategy/``, ``config/`` and the
entrypoint script are read-only sources -- only ``shutil.copytree``/``copy2``
read them; they are never opened for write or deleted.

No symlinks (per spec). No strategy logic. No Claude Code wiring.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .safety import assert_safe_path, repo_root


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _git_commit(project_root: Path) -> str | None:
    """Current HEAD sha (best-effort; None if git unavailable / not a repo)."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(project_root), capture_output=True, text=True, timeout=10,
        )
        if out.returncode == 0:
            return out.stdout.strip() or None
    except Exception:
        pass
    return None


class WorkspaceManager:
    """Per-experiment isolated workspace under the safe output root.

    workspace_root is the parent directory that will hold
    ``<experiment_id>/workspace/`` (e.g. ``.../experiments/``). project_root is
    the production repo root (read-only source of strategy/ + config/ + script).
    """

    def __init__(self, workspace_root: str | Path, project_root: str | Path):
        self.workspace_root = Path(workspace_root)
        # project_root defaults to the repo root (parent of research_automation/)
        self.project_root = Path(project_root) if project_root else repo_root()

    # ---- public API -------------------------------------------------------
    def create_workspace(self, experiment: Any, spec: Any) -> Path:
        """Create ``<workspace_root>/<experiment_id>/workspace/`` with copies of
        strategy/, config/ and the entrypoint script; write metadata.json.

        ``spec`` is an EntrypointSpec (or any object with a ``script`` attribute)
        -- its ``script`` names the entrypoint file to copy into the workspace.
        Returns the workspace directory path.
        """
        exp_id = self._experiment_id(experiment)
        exp_dir = self.workspace_root / exp_id
        ws = exp_dir / "workspace"

        # Guard: every path we are about to create must be inside the safe roots.
        assert_safe_path(ws)
        assert_safe_path(exp_dir / "outputs")

        script_name = getattr(spec, "script", None) or "backtest_brick_v2.py"
        src_script = self.project_root / script_name
        src_strategy = self.project_root / "strategy"
        src_config = self.project_root / "config"

        if not src_script.exists():
            raise FileNotFoundError(
                f"entrypoint script not found in production root: {src_script}")
        if not src_strategy.exists():
            raise FileNotFoundError(
                f"production strategy/ not found: {src_strategy}")

        ws.mkdir(parents=True, exist_ok=True)
        (exp_dir / "outputs").mkdir(parents=True, exist_ok=True)

        # 1) strategy/ copy (the object AG2 may edit) -- copytree, no symlink.
        #    NOTE: strategy/__init__.py does sys.path.insert(0, __file__.parent.parent);
        #    in the workspace copy that resolves to the workspace, so utils/ must also
        #    be copied below or `from utils.*` (via unified_b1_strategy) would break.
        dst_strategy = ws / "strategy"
        if dst_strategy.exists():
            shutil.rmtree(dst_strategy)
        shutil.copytree(src_strategy, dst_strategy, symlinks=False)

        # 2) utils/ copy -- read-only dependency of strategy/ (unified_b1_strategy
        #    imports utils.technical). AG2 never edits utils; copying it only closes
        #    the import loop inside the workspace. Production utils stays untouched.
        src_utils = self.project_root / "utils"
        if src_utils.exists():
            dst_utils = ws / "utils"
            if dst_utils.exists():
                shutil.rmtree(dst_utils)
            shutil.copytree(src_utils, dst_utils, symlinks=False)

        # 3) config/ copy (read by some strategies; never written back to prod)
        dst_config = ws / "config"
        if src_config.exists():
            if dst_config.exists():
                shutil.rmtree(dst_config)
            shutil.copytree(src_config, dst_config, symlinks=False)

        # 4) entrypoint script copy -- launching THIS copy makes sys.path point
        #    at the workspace, so ``from strategy.*`` hits the workspace copy.
        shutil.copy2(src_script, ws / script_name)

        # 5) metadata.json
        self._write_metadata(ws, experiment, script_name)
        return ws

    def cleanup_workspace(self, experiment: Any) -> None:
        """Remove the workspace/ subtree but keep outputs/ + sibling deltas/reports."""
        exp_id = self._experiment_id(experiment)
        ws = self.workspace_root / exp_id / "workspace"
        assert_safe_path(ws)
        if ws.exists():
            shutil.rmtree(ws)

    def archive_workspace(self, experiment: Any) -> Path:
        """Zip the experiment dir (workspace + outputs + reports/metrics/equity)
        into ``workspace.zip`` beside it. Returns the zip path.
        """
        exp_id = self._experiment_id(experiment)
        exp_dir = self.workspace_root / exp_id
        assert_safe_path(exp_dir)
        zip_path = exp_dir / "workspace.zip"
        if zip_path.exists():
            zip_path.unlink()
        # archive the whole experiment dir (workspace + outputs + reports/metrics/equity_curve)
        shutil.make_archive(str(exp_dir / "workspace"), "zip", root_dir=str(exp_dir))
        assert_safe_path(zip_path)
        return zip_path

    # ---- helpers ----------------------------------------------------------
    @staticmethod
    def _experiment_id(experiment: Any) -> str:
        eid = getattr(experiment, "experiment_id", None)
        if eid:
            return str(eid)
        if isinstance(experiment, dict):
            return str(experiment.get("experiment_id") or "adhoc")
        return "adhoc"

    def _write_metadata(self, ws: Path, experiment: Any, script_name: str) -> None:
        reg_ref = getattr(experiment, "registry_reference", None)
        if reg_ref is not None and is_dataclass(reg_ref):
            reg_ref_dict = asdict(reg_ref)
        elif isinstance(reg_ref, dict):
            reg_ref_dict = reg_ref
        else:
            reg_ref_dict = None

        meta = {
            "experiment_id": self._experiment_id(experiment),
            "parent_strategy": getattr(experiment, "strategy", None),
            "created_time": _now_iso(),
            "git_commit": _git_commit(self.project_root),
            "entrypoint_script": script_name,
            "registry_reference": reg_ref_dict,
        }
        meta_path = ws / "metadata.json"
        assert_safe_path(meta_path)
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                             encoding="utf-8")
