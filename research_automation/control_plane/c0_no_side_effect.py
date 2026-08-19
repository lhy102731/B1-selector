"""CR010-R07: full-surface no-side-effect evidence for the C0 official run.

The official 24-cycle run must prove it touched NOTHING outside its
deterministic fixture root.  The surface snapshot covers:

  - Authority + Operational SQLite store files (path + sha256), including
    the DETERMINISTIC FIXTURE ROOT stores the C0 workers actually read
    and write (CR-010 F-12: the worker store paths are part of the
    before/after constraint, never silently global);
  - the PROCESS ENVIRONMENT guard surface (proxy + provider credentials)
    before and after -- a run that scrubbed the environment must restore
    it (CR-010 F-12);
  - data/, knowledge/, config/, strategy/ trees (bounded file inventory);
  - provider registry + provider call-counter state;
  - network probe evidence (NetworkGuard interception attempts);
  - protected user files (CHANGELOG.md / daily_run.py / daily_select.py /
    docs/b1_v3_results.md);
  - git status lines (working tree delta).

``snapshot_surface`` captures the state; ``verify_surface_unchanged``
fails closed on ANY delta, so a run that touched an unintended surface can
never produce a PASS no-side-effect receipt.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .contracts import canonical_json

SURFACE_DIRS = (
    "data",
    "knowledge",
    "config",
    "strategy",
    "research_automation",
    "tools",
)
PROTECTED_FILES = (
    "CHANGELOG.md",
    "daily_run.py",
    "daily_select.py",
    "docs/b1_v3_results.md",
)
STORE_FILES = (
    "research_state/control_plane/authority/authority.sqlite3",
    "research_state/control_plane/operational/operational.sqlite3",
)
# The Authority tables the C0 rollout task may legitimately write (ticket,
# lease fields, outbox, trusted receipt); every OTHER table must stay
# byte-identical (CR-010 C0: exact Authority rows, no blanket DB exemption).
AUTHORITY_ROLLOUT_TABLES = frozenset(
    {
        "task_tickets_v2",
        "authority_outbox",
        "trusted_task_receipts_v2",
    }
)
# CR-010 F-12: the environment guard surface -- the exact variables
# NetworkGuard.install() scrubs.  The snapshot records their values so a
# run that changed the process environment fails closed.
SURFACE_ENV_VARS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "OPENAI_API_KEY",
    "AG2_OPENAI_API_KEY",
    "AG2_DEEPSEEK2_API_KEY",
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_inventory(root: Path, relative_dir: str) -> dict[str, str]:
    """Bounded file inventory (path -> sha256) under a repo-relative dir."""
    base = root / relative_dir
    if not base.exists():
        return {}
    inventory: dict[str, str] = {}
    for path in sorted(base.rglob("*")):
        if path.is_file():
            rel = str(path.relative_to(root)).replace("\\", "/")
            if any(part in (".git", "__pycache__", ".pytest_cache") for part in path.parts):
                continue
            try:
                inventory[rel] = _sha256_file(path)
            except OSError:
                inventory[rel] = "UNREADABLE"
    return inventory


def _store_state(root: Path, extra_store_files: tuple[str, ...] = ()) -> dict[str, str]:
    state: dict[str, str] = {}
    for relative in STORE_FILES:
        path = root / relative
        state[relative] = (
            _sha256_file(path) if path.exists() else "ABSENT"
        )
    for extra in extra_store_files:
        path = Path(extra)
        state[str(path)] = (
            _sha256_file(path) if path.exists() else "ABSENT"
        )
    return state


def _environment_state(environment: Mapping[str, str] | None = None) -> dict[str, str]:
    """Snapshot the guarded environment surface (name -> value)."""
    source = dict(environment) if environment is not None else dict(os.environ)
    return {
        name: source.get(name, "ABSENT")
        for name in SURFACE_ENV_VARS
    }


@dataclass(frozen=True, slots=True)
class SurfaceSnapshot:
    stores: dict[str, str]
    trees: dict[str, dict[str, str]]
    protected: dict[str, str]
    git_status: tuple[str, ...]
    network_attempts: int
    environment: dict[str, str]
    provider_registry: dict[str, str]
    provider_call_counters: dict[str, str]
    git_head: str
    git_tree: str

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": "control_plane.c0_surface_snapshot.v1",
            "stores": self.stores,
            "trees": self.trees,
            "protected": self.protected,
            "git_status": sorted(self.git_status),
            "network_attempts": self.network_attempts,
            "environment": self.environment,
            "provider_registry": self.provider_registry,
            "provider_call_counters": self.provider_call_counters,
            "git_head": self.git_head,
            "git_tree": self.git_tree,
        }


def _git_capture(root: Path, git_executable: str) -> tuple[str, str]:
    """Read-only HEAD/tree capture -- git status alone cannot prove a
    tracked mutation because an automatic commit hides it (CR-010 C0)."""
    import subprocess

    def rev(ref: str) -> str:
        result = subprocess.run(
            [git_executable, "-C", str(root), "rev-parse", ref],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        return result.stdout.strip()

    head = rev("HEAD")
    tree = rev("HEAD^{tree}")
    return head, tree


def snapshot_surface(
    repository_root: str | os.PathLike[str],
    *,
    network_attempts: int = 0,
    git_executable: str = "git",
    extra_store_files: tuple[str, ...] = (),
    environment: Mapping[str, str] | None = None,
    provider_registry: Mapping[str, str] | None = None,
    provider_call_counters: tuple[str, ...] = (),
) -> SurfaceSnapshot:
    """Capture the full no-side-effect surface of the repository.

    ``extra_store_files`` (CR-010 F-12) carries the deterministic fixture
    root's Authority/Operational stores -- the paths the C0 workers
    actually read/write.  They are recorded with the snapshot so the
    before/after constraint covers the worker store paths, never the
    process-global defaults.

    ``provider_registry`` is the provider-registry fingerprint map and
    ``provider_call_counters`` the provider call-counter file paths
    (CR-010 C0) -- both part of the surface contract.
    """
    import subprocess

    root = Path(repository_root).resolve(strict=True)
    git_status = subprocess.run(
        [git_executable, "-C", str(root), "status", "--porcelain"],
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.splitlines()
    git_head, git_tree = _git_capture(root, git_executable)
    trees = {
        relative_dir: _file_inventory(root, relative_dir)
        for relative_dir in SURFACE_DIRS
    }
    protected = {
        relative: (
            _sha256_file(root / relative)
            if (root / relative).exists()
            else "ABSENT"
        )
        for relative in PROTECTED_FILES
    }
    counters: dict[str, str] = {}
    for relative in provider_call_counters:
        path = Path(relative)
        counters[str(path)] = (
            _sha256_file(path) if path.exists() else "ABSENT"
        )
    return SurfaceSnapshot(
        stores=_store_state(root, extra_store_files),
        trees=trees,
        protected=protected,
        git_status=tuple(git_status),
        network_attempts=network_attempts,
        environment=_environment_state(environment),
        provider_registry=dict(provider_registry or {}),
        provider_call_counters=counters,
        git_head=git_head,
        git_tree=git_tree,
    )


class NoSideEffectError(RuntimeError):
    """Raised when the C0 run changed an unintended repository surface."""


def verify_surface_unchanged(
    before: SurfaceSnapshot,
    after: SurfaceSnapshot,
    *,
    allowed_git_deltas: tuple[str, ...] = (),
    store_creation_allowed: tuple[str, ...] = (),
    stores_row_verified: tuple[str, ...] = (),
    allowed_head_after: str | None = None,
    allowed_tree_after: str | None = None,
    git_executable: str = "git",
    repository_root: str | os.PathLike[str] | None = None,
) -> None:
    """Fail closed unless EVERY tracked surface is byte-identical.

    ``allowed_git_deltas`` may carry the intended evidence-file additions
    (the official run's own receipts; each entry is a status line or a
    repo-relative path); anything else fails.

    ``store_creation_allowed`` (CR-010 F-12) may carry the deterministic
    fixture root store paths that the C0 run legitimately creates under
    the OS temp dir; a store going ABSENT -> sha256 is then allowed ONLY
    for those paths -- any other store delta still fails, and a later
    tamper of a created fixture store still fails.

    ``stores_row_verified`` (CR-010 C0) exempts store FILES whose change
    is verified at Authority ROW level by the caller
    (``verify_authority_row_deltas``) -- never a blanket database
    exception.

    CR-010 F-05 (functional closure): the BEFORE/after HEAD and tree OIDs
    MUST be identical -- a hidden tracked commit (status-identical but
    HEAD/tree moved) fails closed.  When the caller declares the exact
    post-commit OIDs (``allowed_head_after``/``allowed_tree_after``) the
    commit(s) between before and after must touch ONLY the allowed
    evidence paths (verified via ``git diff-tree``), so the evidence
    commit is confined to the intended evidence files.  The provider
    registry fingerprint and the provider call-counter files are part of
    the surface and compared too -- a changed provider registration or a
    counter increment fails closed.
    """
    failures: list[str] = []
    allowed_creations = set(store_creation_allowed)
    row_verified = set(stores_row_verified)
    allowed_paths = {
        _path_of_status_line(line)
        for line in allowed_git_deltas
    }
    if before.stores != after.stores:
        for key in sorted(set(before.stores) | set(after.stores)):
            before_value = before.stores.get(key)
            after_value = after.stores.get(key)
            if before_value == after_value:
                continue
            if key in row_verified:
                continue
            if (
                before_value == "ABSENT"
                and key in allowed_creations
                and after_value not in ("ABSENT", "UNREADABLE")
            ):
                continue
            failures.append(
                f"store {key}: {before_value} -> {after_value}"
            )
    for tree in SURFACE_DIRS:
        before_tree = before.trees.get(tree, {})
        after_tree = after.trees.get(tree, {})
        for key in sorted(set(before_tree) | set(after_tree)):
            if before_tree.get(key) != after_tree.get(key):
                failures.append(
                    f"tree {tree} {key}: {before_tree.get(key)} -> "
                    f"{after_tree.get(key)}"
                )
    if before.protected != after.protected:
        for key in sorted(set(before.protected) | set(after.protected)):
            if before.protected.get(key) != after.protected.get(key):
                failures.append(
                    f"protected {key}: {before.protected.get(key)} -> "
                    f"{after.protected.get(key)}"
                )
    if before.network_attempts != after.network_attempts:
        failures.append(
            "network attempt counter changed: "
            f"{before.network_attempts} -> {after.network_attempts}"
        )
    if before.environment != after.environment:
        for key in sorted(set(before.environment) | set(after.environment)):
            if before.environment.get(key) != after.environment.get(key):
                failures.append(
                    f"environment {key}: {before.environment.get(key)} -> "
                    f"{after.environment.get(key)}"
                )
    before_status = set(before.git_status)
    after_status = set(after.git_status)
    # normalize both sides to repo-relative paths so status lines and bare
    # evidence paths are directly comparable
    unexpected_paths = (
        {_path_of_status_line(line) for line in (after_status - before_status)}
        - allowed_paths
    )
    reverted_paths = (
        {_path_of_status_line(line) for line in (before_status - after_status)}
        - allowed_paths
    )
    if unexpected_paths:
        failures.append(
            "unexpected git deltas: " + "; ".join(sorted(unexpected_paths))
        )
    if reverted_paths:
        failures.append(
            "git entries disappeared: " + "; ".join(sorted(reverted_paths))
        )
    # CR-010 F-05: HEAD/tree identity.  A hidden tracked commit moves the
    # OIDs while git status stays identical -- fail closed unless the
    # caller declared the exact post-commit OIDs AND the commit diff is
    # confined to the allowed evidence paths.
    if before.git_head != after.git_head:
        if after.git_head != allowed_head_after:
            failures.append(
                f"git HEAD changed without a declared evidence commit: "
                f"{before.git_head} -> {after.git_head}"
            )
        else:
            unexpected_commit_paths = _evidence_commit_paths(
                before.git_head,
                after.git_head,
                allowed_paths,
                git_executable,
                repository_root,
            )
            if unexpected_commit_paths:
                failures.append(
                    "evidence commit touched unintended paths: "
                    + "; ".join(sorted(unexpected_commit_paths))
                )
    elif allowed_head_after is not None and after.git_head != allowed_head_after:
        failures.append(
            "git HEAD does not match the declared evidence commit: "
            f"{after.git_head} != {allowed_head_after}"
        )
    if before.git_tree != after.git_tree and after.git_tree != allowed_tree_after:
        failures.append(
            f"git tree changed: {before.git_tree} -> {after.git_tree}"
        )
    # CR-010 F-05: the provider registry fingerprint and the provider
    # call-counter files are part of the no-side-effect surface.
    if before.provider_registry != after.provider_registry:
        for key in sorted(
            set(before.provider_registry) | set(after.provider_registry)
        ):
            if before.provider_registry.get(key) != after.provider_registry.get(
                key
            ):
                failures.append(
                    f"provider registry {key}: "
                    f"{before.provider_registry.get(key)} -> "
                    f"{after.provider_registry.get(key)}"
                )
    if before.provider_call_counters != after.provider_call_counters:
        for key in sorted(
            set(before.provider_call_counters)
            | set(after.provider_call_counters)
        ):
            before_value = before.provider_call_counters.get(key)
            after_value = after.provider_call_counters.get(key)
            if before_value == after_value:
                continue
            # a predeclared counter file going ABSENT -> sha256 is the
            # run's OWN evidence (the C0 campaign creates it); any other
            # delta (content tamper, deletion, unknown path) fails closed
            if (
                before_value == "ABSENT"
                and after_value not in ("ABSENT", "UNREADABLE")
            ):
                continue
            failures.append(
                f"provider counter {key}: {before_value} -> {after_value}"
            )
    if failures:
        raise NoSideEffectError(
            "C0 run changed an unintended surface: "
            + "; ".join(failures)
        )


def _path_of_status_line(line: str) -> str:
    """The repo-relative path of a ``git status --porcelain`` line (or a
    bare path kept as-is)."""
    text = line.strip()
    if len(text) > 3 and text[:2] in ("??", "AM", "MM", "M ", "A ", "D "):
        return text[3:].strip()
    return text


def _evidence_commit_paths(
    before_head: str,
    after_head: str,
    allowed_paths: set[str],
    git_executable: str,
    repository_root: str | os.PathLike[str] | None,
) -> tuple[str, ...]:
    """The paths touched by the commit(s) between two HEAD OIDs that are
    NOT in the allowed evidence set (CR-010 F-05)."""
    import subprocess

    argv = [git_executable]
    if repository_root is not None:
        argv.extend(["-C", str(repository_root)])
    argv.extend(
        [
            "diff-tree",
            "--no-commit-id",
            "--name-only",
            "-r",
            f"{before_head}..{after_head}",
        ]
    )
    result = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        return ("<diff-tree unavailable: " + result.stderr[-200:] + ">",)
    return tuple(
        sorted(
            {
                line.strip().replace("\\", "/")
                for line in result.stdout.splitlines()
                if line.strip()
            }
            - allowed_paths
        )
    )


def build_no_side_effect_receipt(
    repository_root: str | os.PathLike[str],
    before: SurfaceSnapshot,
    after: SurfaceSnapshot,
    *,
    allowed_git_deltas: tuple[str, ...] = (),
    store_creation_allowed: tuple[str, ...] = (),
) -> dict[str, object]:
    """Build the official no-side-effect receipt (fail closed)."""
    verify_surface_unchanged(
        before,
        after,
        allowed_git_deltas=allowed_git_deltas,
        store_creation_allowed=store_creation_allowed,
    )
    return {
        "schema_version": "control_plane.c0_no_side_effect_receipt.v2",
        "surface": {
            "stores": "UNCHANGED",
            "trees": "UNCHANGED",
            "protected": "UNCHANGED",
            "network_probe": "UNCHANGED",
            "environment": "UNCHANGED",
            "git": "UNCHANGED (allowed evidence deltas only)",
        },
        "before": before.to_payload(),
        "after": after.to_payload(),
        "pass": True,
    }


def network_telemetry_snapshot() -> dict[str, int]:
    """The run-scoped guard telemetry the surface receipt needs (CR-010
    A5 9.3): deny-probe / spawn / real-network counts survive uninstall
    because they live in the run-scoped collector."""
    from .rollout_chaos_worker import NetworkGuard

    return NetworkGuard.run_telemetry_snapshot()


def live_provider_registry_fingerprint(
    *,
    override_name: str | None = None,
) -> str:
    """Recompute the provider-registry fingerprint from the LIVE offline
    provider registry at snapshot time (CR-010 A5 9.3) -- never a cached
    dict.  ``override_name`` simulates a registry mutation for the
    negative probe (it changes the live provider identity)."""
    from .rollout_chaos_fixtures import C0ChaosProvider

    provider = C0ChaosProvider({})
    material = {
        "provider_name": (
            override_name
            if override_name is not None
            else str(provider.provider_name)
        ),
        "profile": str(provider.profile),
        "model": str(provider.model),
        "config_sha256": str(provider.config_sha256),
        "capability_sha256": str(provider.capability_sha256),
    }
    return hashlib.sha256(
        canonical_json(material).encode("utf-8")
    ).hexdigest()


def durable_model_usage_count(
    operational_db: str | os.PathLike[str],
    *,
    campaign_id: str,
    cycle_id: str,
    attempt_id: str,
    grant_id: str,
    campaign_attempt_id: str,
) -> int:
    """The DEDUPLICATED canonical count of durable MODEL_USAGE_RECORDED
    events for one root/campaign/cycle and TARGET CAMPAIGN ATTEMPT
    (CR-010 A5 9.2, F-02 git-native run003).

    The count comes ONLY from the Operational journal's campaign_events --
    never from the counter file, a surface receipt or the helper that
    writes the counter.  Events are bound to the root's authoritative
    ``grant_id`` AND the TARGET ``campaign_attempt_id`` at write time, so
    only THIS root's run AND THIS attempt count; events of another grant
    or of the SAME grant but ANOTHER campaign attempt are excluded.  Events
    are deduplicated by their invocation attempt id.
    """
    import json as _json
    import sqlite3 as _sqlite3

    connection = _sqlite3.connect(str(operational_db))
    try:
        rows = connection.execute(
            "SELECT payload_json FROM campaign_events "
            "WHERE campaign_id = ? AND cycle_id = ? "
            "AND event_type = 'MODEL_USAGE_RECORDED'",
            (campaign_id, cycle_id),
        ).fetchall()
    finally:
        connection.close()
    seen: set[str] = set()
    canonical_count = 0
    for (payload_json,) in rows:
        try:
            document = _json.loads(str(payload_json))
        except (TypeError, ValueError, _json.JSONDecodeError):
            continue
        if not isinstance(document, dict):
            continue
        if str(document.get("_authority_grant_id") or "") != grant_id:
            continue
        if str(document.get("_campaign_attempt_id") or "") != campaign_attempt_id:
            continue
        event_attempt = str(
            document.get("attempt_id")
            or (document.get("usage") or {}).get("attempt_id")
            or ""
        )
        if event_attempt in seen:
            continue
        seen.add(event_attempt)
        canonical_count += 1
    return canonical_count


def durable_usage_cycles(
    operational_db: str | os.PathLike[str],
    *,
    campaign_id: str,
    grant_id: str,
    campaign_attempt_id: str,
) -> set[str]:
    """The COMPLETE set of cycle ids that have MODEL_USAGE_RECORDED
    events for the given ROOT RUN and TARGET CAMPAIGN ATTEMPT (F-02,
    git-native run003: period-completeness + attempt isolation).

    Derived ONLY from the Operational journal, never from the counter
    files -- so deleting a counter file can never shrink the required
    period set.  Events bound to ANOTHER grant (another run / forged
    injection) are excluded; within the same grant, events of ANOTHER
    campaign attempt (same grant, different attempt) are excluded too.
    """
    import json as _json
    import sqlite3 as _sqlite3

    connection = _sqlite3.connect(str(operational_db))
    try:
        rows = connection.execute(
            "SELECT cycle_id, payload_json FROM campaign_events "
            "WHERE campaign_id = ? AND event_type = 'MODEL_USAGE_RECORDED'",
            (campaign_id,),
        ).fetchall()
    finally:
        connection.close()
    cycles: set[str] = set()
    for cycle_id, payload_json in rows:
        try:
            document = _json.loads(str(payload_json))
        except (TypeError, ValueError, _json.JSONDecodeError):
            continue
        if not isinstance(document, dict):
            continue
        if str(document.get("_authority_grant_id") or "") != grant_id:
            continue
        if str(document.get("_campaign_attempt_id") or "") != campaign_attempt_id:
            continue
        cycles.add(str(cycle_id))
    return cycles


def unique_root_grant_id(
    operational_db: str | os.PathLike[str],
    *,
    campaign_id: str,
) -> str:
    """The authoritative grant id of ONE root's usage journal (F-02,
    run002).  A journal mixing events from MORE than one grant is invalid:
    a counter verification can never mix runs, so we fail closed."""
    import json as _json
    import sqlite3 as _sqlite3

    connection = _sqlite3.connect(str(operational_db))
    try:
        rows = connection.execute(
            "SELECT payload_json FROM campaign_events "
            "WHERE campaign_id = ? AND event_type = 'MODEL_USAGE_RECORDED'",
            (campaign_id,),
        ).fetchall()
    finally:
        connection.close()
    grants: set[str] = set()
    for (payload_json,) in rows:
        try:
            document = _json.loads(str(payload_json))
        except (TypeError, ValueError, _json.JSONDecodeError):
            continue
        if not isinstance(document, dict):
            continue
        grant = str(document.get("_authority_grant_id") or "")
        if grant:
            grants.add(grant)
    if len(grants) != 1:
        raise RuntimeError(
            "operational journal for the root does not have exactly one "
            f"authoritative grant (found {len(grants)})"
        )
    return next(iter(grants))


def verify_counter_matches_durable_usage(
    *,
    counter_path: str | os.PathLike[str],
    operational_db: str | os.PathLike[str],
    campaign_id: str,
    cycle_id: str,
    attempt_id: str,
    grant_id: str,
    campaign_attempt_id: str,
    repository_root: str | os.PathLike[str],
    root_secret: str,
) -> None:
    """Fail closed unless the provider counter (an IDENTITY-BOUND record
    bound to root/grant/attempt/cycle) equals the DURABLE usage count
    (CR-010 A5 9.2, F-02 run004).

    ``ABSENT -> arbitrary`` never passes: the counter must be a
    structured ``control_plane.c0_provider_counter.v1`` record whose
    root/grant/attempt/cycle match this run and whose root-secret HMAC
    signature is valid -- a bare integer, a cross-root-exchanged file or a
    tampered count is rejected.  The expected count is derived ONLY from
    the durable journal events of this root's grant AND target attempt.
    """
    root_identity = _counter_root_identity(repository_root)
    observed = read_sealed_counter(
        counter_path,
        root_identity=root_identity,
        expect_grant=grant_id,
        expect_attempt=campaign_attempt_id,
        expect_cycle=cycle_id,
        root_secret=root_secret,
    )
    expected = durable_model_usage_count(
        operational_db,
        campaign_id=campaign_id,
        cycle_id=cycle_id,
        attempt_id=attempt_id,
        grant_id=grant_id,
        campaign_attempt_id=campaign_attempt_id,
    )
    if observed != expected:
        raise ValueError(
            f"provider counter {observed} != durable MODEL_USAGE_RECORDED "
            f"count {expected} for {campaign_id}/{cycle_id}"
        )


def _counter_root_identity(repository_root: str | os.PathLike[str]) -> str:
    import hashlib as _hashlib

    return _hashlib.sha256(
        str(Path(repository_root).resolve()).encode("utf-8")
    ).hexdigest()


def _counter_signature(
    root_secret: str,
    *,
    root_identity: str,
    grant: str,
    attempt_id: str,
    cycle_id: str,
    count: int,
) -> str:
    import hashlib as _hashlib
    import hmac as _hmac

    payload = "\0".join(
        (root_identity, grant, attempt_id, cycle_id, str(count))
    ).encode("utf-8")
    return _hmac.new(
        root_secret.encode("utf-8"),
        b"control_plane.c0_provider_counter.v1\0" + payload,
        _hashlib.sha256,
    ).hexdigest()


def seal_provider_counter(
    counter_path: str | os.PathLike[str],
    operational_db: str | os.PathLike[str],
    *,
    repository_root: str | os.PathLike[str],
    campaign_id: str,
    cycle_id: str,
    attempt_id: str,
    root_secret: str,
    grant_id: str | None = None,
) -> None:
    """F-02 (run004): rewrite one provider counter file into an
    IDENTITY-BOUND structured record: root/grant/attempt/cycle/count plus a
    root-secret HMAC signature.  A bare integer counter is never a valid
    durable record -- cross-root exchange of equal counts is rejected by
    the verifier."""
    import json as _json

    path = Path(counter_path)
    raw = path.read_text(encoding="utf-8").strip()
    if not raw.isdigit():
        raise ValueError(
            "provider counter must currently hold an integer count: "
            + raw[:80]
        )
    count = int(raw)
    try:
        grant = grant_id or unique_root_grant_id(
            operational_db, campaign_id=campaign_id
        )
    except Exception as error:
        raise RuntimeError(
            "cannot seal counter without the root's authoritative grant"
        ) from error
    record = {
        "schema_version": "control_plane.c0_provider_counter.v1",
        "root": _counter_root_identity(repository_root),
        "grant": str(grant),
        "attempt": str(attempt_id),
        "cycle": str(cycle_id),
        "count": count,
        "signature": _counter_signature(
            root_secret,
            root_identity=_counter_root_identity(repository_root),
            grant=str(grant),
            attempt_id=str(attempt_id),
            cycle_id=str(cycle_id),
            count=count,
        ),
    }
    path.write_text(
        _json.dumps(record, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def read_sealed_counter(
    counter_path: str | os.PathLike[str],
    *,
    root_identity: str,
    expect_grant: str,
    expect_attempt: str,
    expect_cycle: str,
    root_secret: str,
) -> int:
    """F-02 (run004): read + verify an identity-bound counter record.
    Returns the count ONLY if the record's root/grant/attempt/cycle match
    the expectation and the root-secret signature is valid."""
    import json as _json

    path = Path(counter_path)
    if not path.exists():
        raise RuntimeError(
            "provider counter is missing (ABSENT counters never pass)"
        )
    raw = path.read_text(encoding="utf-8").strip()
    try:
        record = _json.loads(raw)
    except ValueError as error:
        raise RuntimeError(
            "provider counter is not a structured identity record (a bare "
            "integer counter cannot pass): " + str(error)
        ) from error
    if not isinstance(record, dict):
        raise RuntimeError("provider counter record is not an object")
    if str(record.get("schema_version", "")) != "control_plane.c0_provider_counter.v1":
        raise RuntimeError("provider counter record schema is invalid")
    if str(record.get("root", "")) != str(root_identity):
        raise RuntimeError("provider counter belongs to a DIFFERENT root")
    if str(record.get("grant", "")) != str(expect_grant):
        raise RuntimeError("provider counter grant does not match this run")
    if str(record.get("attempt", "")) != str(expect_attempt):
        raise RuntimeError("provider counter attempt does not match this run")
    if str(record.get("cycle", "")) != str(expect_cycle):
        raise RuntimeError("provider counter cycle does not match this cycle")
    count = record.get("count")
    if type(count) is not int or count < 0:
        raise RuntimeError("provider counter count is not a non-negative int")
    expected_sig = _counter_signature(
        root_secret,
        root_identity=str(root_identity),
        grant=str(expect_grant),
        attempt_id=str(expect_attempt),
        cycle_id=str(expect_cycle),
        count=count,
    )
    if not hmac.compare_digest(str(record.get("signature", "")), expected_sig):
        raise RuntimeError("provider counter identity signature is invalid")
    return count


def _table_rows(db: Path, table: str) -> tuple[tuple[object, ...], ...]:
    connection = sqlite3.connect(str(db))
    try:
        rows = connection.execute(
            f'SELECT * FROM "{table}" ORDER BY 1'
        ).fetchall()
    finally:
        connection.close()
    return tuple(tuple(row) for row in rows)


def _table_names(db: Path) -> tuple[str, ...]:
    connection = sqlite3.connect(str(db))
    try:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    finally:
        connection.close()
    return tuple(str(row[0]) for row in rows)


def verify_authority_row_deltas(
    before_db: str | os.PathLike[str],
    after_db: str | os.PathLike[str],
    *,
    ticket_id: str,
    allowed_tables: frozenset[str] = AUTHORITY_ROLLOUT_TABLES,
) -> None:
    """Fail closed unless the Authority DB changed ONLY in the enumerated
    rollout-task rows bound to ``ticket_id`` (CR-010 C0).

    Every table outside ``allowed_tables`` must be byte-identical (all
    rows); inside the allowed tables only rows bound to the ticket may
    appear/change.  There is no blanket Authority database exception.
    """
    before_path = Path(before_db)
    after_path = Path(after_db)
    if not before_path.exists() or not after_path.exists():
        raise RuntimeError(
            "authority row verification needs both DB files present"
        )
    before_tables = _table_names(before_path)
    after_tables = _table_names(after_path)
    if before_tables != after_tables:
        raise RuntimeError(
            "Authority schema changed outside the rollout task"
        )
    for table in before_tables:
        before_rows = _table_rows(before_path, table)
        after_rows = _table_rows(after_path, table)
        if table not in allowed_tables:
            if before_rows != after_rows:
                raise RuntimeError(
                    "Authority table " + table + " changed outside the "
                    "rollout task rows"
                )
            continue
        before_unbound = {
            row for row in before_rows if ticket_id not in row
        }
        after_unbound = {
            row for row in after_rows if ticket_id not in row
        }
        if before_unbound != after_unbound:
            raise RuntimeError(
                "Authority table " + table + " changed outside the "
                "rollout task ticket"
            )


def _evidence_lines(
    git_status: tuple[str, ...],
    evidence_prefix: str,
) -> tuple[str, ...]:
    return tuple(
        line
        for line in git_status
        if line.lstrip("?AMDRCU ").startswith(evidence_prefix)
    )


def verify_stage3c_post_return(
    repository_root: str | os.PathLike[str],
    *,
    baseline_head: str,
    baseline_tree: str,
    evidence_prefix: str,
    evidence_blobs: dict[str, str] | None = None,
) -> dict[str, object]:
    """F-04: an EXTERNAL READ-ONLY harness re-verifies the Git surface
    AFTER stage3c has returned.

    It compares the CURRENT HEAD/tree against the PRE-run baseline
    (never the post-return current value as its own expectation), lists
    every changed path since the baseline with ``git diff-tree
    --name-status``, and requires that:

      - every committed path since the baseline falls under the evidence
        allowlist prefix;
      - every declared evidence blob (from the baseline manifest) still
        exists in HEAD with the SAME SHA (create-only, never rewritten);
      - HEAD == working tree (the harness is read-only and the repo is
        clean of delta after the evidence commit);
      - a hidden tracked commit (README / source / config / protected) is
        REJECTED.
    """
    import subprocess as _subprocess

    root = Path(repository_root).resolve(strict=True)

    def _git(*args: str) -> str:
        result = _subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise NoSideEffectError(
                "post-return git command failed: " + " ".join(args[:6])
            )
        return result.stdout.strip()

    head = _git("rev-parse", "HEAD")
    tree = _git("rev-parse", "HEAD^{tree}")
    if not _is_sha256(baseline_head) or not _is_sha256(baseline_tree):
        raise NoSideEffectError("baseline HEAD/tree are invalid digests")
    failures: list[str] = []
    if tree == baseline_tree and head == baseline_head:
        # no commit since the baseline: nothing may have hidden-change if
        # worktree is clean; still require the evidence to be committed
        failures.append("no evidence commit observed after stage3c")
    else:
        changed_names = _git(
            "diff-tree", "--no-commit-id", "--name-status", "-r",
            f"{baseline_head}..{head}",
        )
        for line in changed_names.splitlines():
            parts = line.split("\t")
            path = parts[-1].replace("\\", "/")
            if not path.startswith(evidence_prefix):
                failures.append(
                    "post-return commit touched a non-evidence path: " + path
                )
            if len(parts) >= 2 and parts[0].startswith("D"):
                failures.append(
                    "post-return commit DELETED a path: " + path
                )
    # every declaration in the baseline manifest must still exist in HEAD
    for ref, sha in (evidence_blobs or {}).items():
        if not _is_sha256(sha):
            failures.append("baseline evidence manifest sha invalid: " + ref)
            continue
        try:
            current = _git("rev-parse", f"{head}:{ref}")
        except NoSideEffectError:
            failures.append("baseline evidence blob missing in HEAD: " + ref)
            continue
        if current != sha:
            failures.append(
                "baseline evidence blob changed in HEAD: " + ref
            )
    # HEAD must equal the working tree (read-only harness, no drift)
    status = _git("status", "--porcelain")
    if status.strip():
        failures.append("post-return working tree is not clean")
    if failures:
        raise NoSideEffectError(
            "post-return Git surface verification failed: "
            + "; ".join(failures)
        )
    return {
        "verified": True,
        "baseline_head": baseline_head,
        "baseline_tree": baseline_tree,
        "head_after": head,
        "tree_after": tree,
        "evidence_prefix": evidence_prefix,
    }


def _is_sha256(value: object) -> bool:
    import re as _re

    return (
        isinstance(value, str)
        and _re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", value) is not None
    )


def build_stage3c_surface_receipt(
    repository_root: str | os.PathLike[str],
    *,
    surface_before_ticket: SurfaceSnapshot,
    surface_after_ticket_begin: SurfaceSnapshot,
    surface_before_simulation: SurfaceSnapshot,
    surface_after_simulation: SurfaceSnapshot,
    authority_db: str | os.PathLike[str],
    authority_db_before_ticket: str | os.PathLike[str],
    authority_db_before_simulation: str | os.PathLike[str],
    ticket_id: str,
    evidence_prefix: str,
    store_creation_allowed: tuple[str, ...],
    final_check_ref: str,
    evidence_commit_paths: tuple[str, ...] = (),
) -> dict[str, object]:
    """Build the immutable stage3c surface receipt across the four time
    windows (CR-010 C0):

    1. ``surface_before_ticket`` -> ``surface_after_ticket_begin``:
       the run-spec commit + the enumerated ticket/lease/outbox rows only.
       The declared evidence-commit OIDs (recorded at the window end) must
       match AND the commit diff must touch ONLY ``evidence_commit_paths``
       (CR-010 F-05: a hidden tracked commit fails closed);
    2. ``surface_after_ticket_begin`` -> ``surface_before_simulation``:
       byte-identical (no writes);
    3. ``surface_before_simulation`` -> ``surface_after_simulation``:
       only the first/second disposable fixture roots + their counters and
       the intentional evidence files under the attempt evidence dir;
    4. the final check (``c0_final_surface_check.json``) is PREDECLARED
       here and verified independently after the last artifact write.

    ``authority_db`` is the CURRENT Authority file; the
    ``authority_db_before_*`` arguments are COPIES taken at the window
    starts.  The Authority store file is exempted from byte comparison
    ONLY at the row level: ``verify_authority_row_deltas`` proves the
    changes are the enumerated ticket-bound rows.  ``git status`` alone is
    insufficient because an automatic commit can hide a tracked mutation
    -- HEAD/tree OIDs are recorded at every commit boundary.
    """
    authority_store_key = STORE_FILES[0]
    verify_surface_unchanged(
        surface_before_ticket,
        surface_after_ticket_begin,
        allowed_git_deltas=evidence_commit_paths,
        store_creation_allowed=store_creation_allowed,
        stores_row_verified=(authority_store_key,),
        allowed_head_after=surface_after_ticket_begin.git_head,
        allowed_tree_after=surface_after_ticket_begin.git_tree,
        repository_root=repository_root,
    )
    verify_authority_row_deltas(
        authority_db_before_ticket,
        authority_db,
        ticket_id=ticket_id,
    )
    verify_surface_unchanged(
        surface_after_ticket_begin,
        surface_before_simulation,
        allowed_git_deltas=(),
        store_creation_allowed=store_creation_allowed,
    )
    evidence_lines = _evidence_lines(
        surface_after_simulation.git_status,
        evidence_prefix,
    )
    verify_surface_unchanged(
        surface_before_simulation,
        surface_after_simulation,
        allowed_git_deltas=evidence_lines,
        store_creation_allowed=store_creation_allowed,
    )
    verify_authority_row_deltas(
        authority_db_before_simulation,
        authority_db,
        ticket_id=ticket_id,
    )
    return {
        "schema_version": "control_plane.c0_stage3c_surface_receipt.v1",
        "windows": {
            "surface_before_ticket": (
                surface_before_ticket.to_payload()
            ),
            "surface_after_ticket_begin": (
                surface_after_ticket_begin.to_payload()
            ),
            "surface_before_simulation": (
                surface_before_simulation.to_payload()
            ),
            "surface_after_simulation": (
                surface_after_simulation.to_payload()
            ),
        },
        "git": {
            "head_before": surface_before_ticket.git_head,
            "tree_before": surface_before_ticket.git_tree,
            "head_after_ticket_begin": (
                surface_after_ticket_begin.git_head
            ),
            "tree_after_ticket_begin": (
                surface_after_ticket_begin.git_tree
            ),
        },
        "intended_deltas": {
            "evidence_git": list(evidence_lines),
            "store_creations": sorted(store_creation_allowed),
            "authority": {
                "ticket_id": ticket_id,
                "allowed_tables": sorted(AUTHORITY_ROLLOUT_TABLES),
            },
        },
        "final_check_ref": final_check_ref,
        "pass": True,
    }


def build_c0_final_surface_check(
    repository_root: str | os.PathLike[str],
    *,
    surface_final: SurfaceSnapshot,
    surface_reference: SurfaceSnapshot,
    authority_db: str | os.PathLike[str],
    authority_db_before_final: str | os.PathLike[str],
    ticket_id: str,
    evidence_prefix: str,
    pre_publication_status: tuple[str, ...],
    final_check_ref: str,
    git_head_after: str,
    git_tree_after: str,
    store_creation_allowed: tuple[str, ...],
) -> dict[str, object]:
    """Build the independent final surface check (CR-010 C0).

    The snapshot is taken AFTER every other artifact write (including the
    internal evidence commit); the check file itself is the ONE
    self-excluded final-check path, written exactly once and never
    back-filled into the immutable receipt.  The allowed-delta projection
    is recomputed independently from ``pre_publication_status`` -- the
    receipt's list is never trusted blindly.
    """
    failures: list[str] = []
    expected_status = {
        line
        for line in pre_publication_status
        if not line.lstrip("?AMDRCU ").startswith(evidence_prefix)
    }
    actual_status = set(surface_final.git_status)
    if actual_status != expected_status:
        failures.append(
            "final git status differs from the predeclared allowed-delta "
            "projection: unexpected="
            + ";".join(sorted(actual_status - expected_status))
            + " missing="
            + ";".join(sorted(expected_status - actual_status))
        )
    for tree in SURFACE_DIRS:
        before_tree = surface_reference.trees.get(tree, {})
        after_tree = surface_final.trees.get(tree, {})
        if before_tree != after_tree:
            failures.append(f"tree {tree} changed after the final snapshot")
    if surface_reference.protected != surface_final.protected:
        failures.append("protected files changed after the final snapshot")
    if surface_reference.environment != surface_final.environment:
        failures.append("environment changed after the final snapshot")
    if (
        surface_reference.network_attempts
        != surface_final.network_attempts
    ):
        failures.append("network attempt counter changed after the run")
    if (
        surface_reference.provider_registry
        != surface_final.provider_registry
    ):
        failures.append("provider registry changed after the run")
    if (
        surface_reference.provider_call_counters
        != surface_final.provider_call_counters
    ):
        failures.append("provider call counters changed after the run")
    authority_store_key = STORE_FILES[0]
    for key in sorted(
        set(surface_reference.stores) | set(surface_final.stores)
    ):
        before_value = surface_reference.stores.get(key)
        after_value = surface_final.stores.get(key)
        if before_value == after_value:
            continue
        if key == authority_store_key:
            continue
        if (
            before_value == "ABSENT"
            and key in set(store_creation_allowed)
            and after_value not in ("ABSENT", "UNREADABLE")
        ):
            continue
        failures.append(
            f"store {key}: {before_value} -> {after_value}"
        )
    try:
        verify_authority_row_deltas(
            authority_db_before_final,
            authority_db,
            ticket_id=ticket_id,
        )
    except RuntimeError as error:
        failures.append(str(error))
    if git_head_after and surface_final.git_head != git_head_after:
        failures.append(
            "final HEAD differs from the recorded evidence commit: "
            f"{surface_final.git_head} != {git_head_after}"
        )
    if git_tree_after and surface_final.git_tree != git_tree_after:
        failures.append(
            "final tree differs from the recorded evidence commit: "
            f"{surface_final.git_tree} != {git_tree_after}"
        )
    return {
        "schema_version": "control_plane.c0_final_surface_check.v1",
        "predeclared_ref": final_check_ref,
        "git_head": surface_final.git_head,
        "git_tree": surface_final.git_tree,
        "surface": surface_final.to_payload(),
        "intended_deltas": {
            "evidence_prefix": evidence_prefix,
            "authority": {"ticket_id": ticket_id},
        },
        "failures": failures,
        "verified": not failures,
    }


__all__ = [
    "AUTHORITY_ROLLOUT_TABLES",
    "NoSideEffectError",
    "PROTECTED_FILES",
    "STORE_FILES",
    "SURFACE_DIRS",
    "SurfaceSnapshot",
    "build_c0_final_surface_check",
    "build_no_side_effect_receipt",
    "build_stage3c_surface_receipt",
    "snapshot_surface",
    "verify_authority_row_deltas",
    "verify_surface_unchanged",
]
