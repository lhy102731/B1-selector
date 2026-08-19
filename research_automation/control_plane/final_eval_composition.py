"""The ONLY production FinalEval composition root (CR-010 C0, Phase B).

``compose_final_eval_runtime`` is the single authorized factory: it
verifies the P8 grant and durable request identity, creates the sealed
root capability, the Authority-backed HoldoutStore, the synthetic staging
backend and the create-only evidence sink, and rejects a context whose
request/grant/material hashes are not an exact match.  It accepts no raw
secret, raw nonce or arbitrary holdout path from argv/env -- everything
arrives as verified in-memory objects inside ``AuthorizedFinalEvalContext``.

Ordinary runner/AG2/prompt/memory code can never construct the composition
root; an attempt id alone (even one that exists in the database) is never
an authorization.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from .contracts import Actor
from .final_eval_authority import FinalEvalRequestRejected, FinalEvalRequestV2
from .final_eval_holdout_store import SqliteHoldoutStore
from .final_eval_request_projection import (
    FinalEvalMaterialBundle,
    build_evaluator_request_v2,
)
from .final_eval_runtime import (
    FinalEvalRootCapability,
    FinalEvalRuntime,
    FinalEvalRuntimeInputs,
)
from .final_evaluator import (
    HoldoutDataBackend,
    TrustedEvaluatorDataRoot,
)
from .stores import AuthorityIdentity, Phase, _AuthorityStore

if TYPE_CHECKING:  # pragma: no cover -- runtime imports stay lazy
    from pathlib import Path as _Path


class FinalEvalCompositionError(RuntimeError):
    """Base error for the authorized composition root."""


class FinalEvalCompositionRejected(FinalEvalCompositionError):
    """The composition context is forged, drifted or unauthorized."""


@dataclass(frozen=True, slots=True)
class AuthorizedFinalEvalContext:
    """The verified in-memory authorization context (Phase B).

    Carries the P8 grant, the durable V2 request, the raw nonce (in
    memory only -- repr-redacted, it never leaves this object), the
    actor/identity lineage, the SEALED material source, the sealed data
    root, the approved worker launcher and the create-only evidence sink.

    CR-010 F-01 (functional closure): the context accepts NO caller
    selected evaluator, NO evaluator request and NO arbitrary material
    resolver -- the production composition root and the host entry are
    the ONLY assembly path.  ``material_resolver`` must be a sealed
    ``SealedMaterialResolver``; arbitrary callables are rejected so the
    caller can never inject an unsealed material bundle.
    """

    request: FinalEvalRequestV2
    grant: object = field(repr=False)
    nonce: str = field(repr=False)
    actor: Actor
    identity: AuthorityIdentity
    idempotency_key: str
    task_spec_ref: str
    task_spec_sha256: str
    authority_capability: str = field(repr=False)
    repository_root: str
    data_root: TrustedEvaluatorDataRoot
    worker_launcher: Callable[[], int] = field(repr=False)
    evidence_sink: Callable[[Mapping[str, object]], Mapping[str, object]] = (
        field(repr=False)
    )
    attempt_id: str
    material_resolver: "SealedMaterialResolver | None" = field(
        default=None, repr=False
    )


@dataclass(frozen=True, slots=True)
class MaterialRecord:
    """One validated, immutable, ref-backed research material (A1).

    ``ref`` is the repo-relative path of a COMMITTED regular blob; the
    content SHA-256 is recomputed from the committed blob bytes (never a
    caller-supplied string) and agrees with the V2 request, the manifest
    and the sealed bundle.
    """

    ref: str
    content_sha256: str
    blob_sha256: str
    frozen_commit: str


@dataclass(frozen=True, slots=True)
class MaterialRecord:
    """One validated, immutable, ref-backed research material (A1/F-01).

    ``ref`` is the repo-relative path of a COMMITTED regular blob; the
    content SHA-256 is recomputed from the committed blob bytes (never a
    caller-supplied string) and agrees with the V2 request, the manifest
    and the sealed bundle.
    """

    ref: str
    content_sha256: str
    blob_sha256: str
    frozen_commit: str


_SEALED_RESOLVER_DOMAIN = (
    b"control_plane.sealed_material_resolver.v1\x00"
)


def _sealed_resolver_factory_token(
    root_secret: str,
    *,
    manifest_digest: str,
    frozen_commit: str,
    frozen_tree: str,
    request_sha256: str,
) -> str:
    """HMAC-SHA256 factory provenance token.

    The token is derived from the in-memory root secret plus the full
    verified identity (manifest digest, frozen commit/tree, request
    digest).  Only ``build_sealed_material_resolver`` holds the secret, so
    a caller can never forge a resolver -- direct dataclass construction,
    a forged subclass or ``object.__new__`` + field stuffing all lack the
    valid token and are rejected by the composition root.
    """
    payload = b"".join(
        [
            _SEALED_RESOLVER_DOMAIN,
            manifest_digest.encode("utf-8"),
            b"\x00",
            frozen_commit.encode("utf-8"),
            b"\x00",
            frozen_tree.encode("utf-8"),
            b"\x00",
            request_sha256.encode("utf-8"),
        ]
    )
    return hmac.new(
        root_secret.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()


class SealedMaterialResolver:
    """The ONLY material source the production composition accepts (F-01).

    Sealed and unforgeable: this is NOT an open dataclass and exposes NO
    casting API.  Instances can only be created inside the manifest-backed
    factory ``build_sealed_material_resolver`` (inline construction; there
    is no ``_create``/``_mint`` entry point a caller could invoke with a
    self-chosen secret).  Instances are IMMUTABLE: any ``__setattr__``
    after sealing raises.  A forged subclass, an ``object.__new__``-stuffed
    instance, a mutated slot or a resolver whose frozen commit/tree no
    longer equals the current Git HEAD/tree is rejected by composition
    before any store/evaluator.
    """

    __slots__ = (
        "_records",
        "_repository_root",
        "_frozen_commit",
        "_frozen_tree",
        "_manifest_digest",
        "_request_sha256",
        "_bundle",
        "_factory_token",
        "_sealed",
    )

    def __init__(self, *args: object, **kwargs: object) -> None:
        # deliberate: direct construction is always a forgery attempt
        raise FinalEvalCompositionRejected(
            "SealedMaterialResolver must be created by "
            "build_sealed_material_resolver; direct construction is a "
            "forgery attempt and is rejected before any store/evaluator"
        )

    def __setattr__(self, name: str, value: object) -> None:
        # F-01 (git-native run003): a sealed resolver is IMMUTABLE -- any
        # attempt to mutate a slot after creation (records/bundle/frozen
        # commit-tree/manifest-digest) is a forgery attempt.
        if getattr(self, "_sealed", False):
            raise FinalEvalCompositionRejected(
                "SealedMaterialResolver is immutable after creation; "
                f"slot mutation ({name}) is a forgery attempt"
            )
        object.__setattr__(self, name, value)

    # ---- read-only surface (no setattr after creation) ----
    @property
    def records(self) -> tuple[MaterialRecord, ...]:
        return self._records

    @property
    def repository_root(self) -> str:
        return self._repository_root

    @property
    def frozen_commit(self) -> str:
        return self._frozen_commit

    @property
    def frozen_tree(self) -> str:
        return self._frozen_tree

    @property
    def manifest_digest(self) -> str:
        return self._manifest_digest

    @property
    def request_sha256(self) -> str:
        return self._request_sha256

    def verify_factory_token(self, root_secret: str) -> None:
        """Recompute the factory token with the caller's in-memory secret
        and fail closed unless it matches (F-01)."""
        if not isinstance(root_secret, str) or len(root_secret) < 32:
            raise FinalEvalCompositionRejected(
                "factory-token verification requires a strong in-memory "
                "secret"
            )
        expected = _sealed_resolver_factory_token(
            root_secret,
            manifest_digest=self._manifest_digest,
            frozen_commit=self._frozen_commit,
            frozen_tree=self._frozen_tree,
            request_sha256=self._request_sha256,
        )
        if not hmac.compare_digest(expected, str(self._factory_token or "")):
            raise FinalEvalCompositionRejected(
                "sealed material resolver factory provenance is invalid "
                "or forged"
            )

    def resolve(self, request: FinalEvalRequestV2) -> FinalEvalMaterialBundle:
        if not isinstance(request, FinalEvalRequestV2):
            raise FinalEvalCompositionRejected(
                "sealed resolver requires a FinalEvalRequestV2"
            )
        if request.request_sha256 != self._request_sha256:
            raise FinalEvalCompositionRejected(
                "sealed material resolver rejects the request: the "
                "request identity is not the sealed identity"
            )
        return self._bundle


# A1: the six REAL ref-backed research materials.  model/generation/
# campaign have NO ref field in V2 -- their identity keeps the existing
# canonical V2 request/grant/binding contract and is never described as
# recomputed from artifact bytes.
_REF_SHA_FIELDS = (
    ("candidate_freeze_ref", "candidate_freeze_sha256"),
    ("code_ref", "code_sha256"),
    ("execution_spec_ref", "execution_spec_sha256"),
    ("features_ref", "features_sha256"),
    ("threshold_ref", "threshold_sha256"),
    ("roster_ref", "roster_sha256"),
)
_MATERIAL_MANIFEST_SCHEMA = "control_plane.final_eval_material_manifest.v1"


def _material_manifest_digest(
    records: tuple[MaterialRecord, ...],
    *,
    request_sha256: str,
    frozen_commit: str,
) -> str:
    payload = {
        "schema_version": _MATERIAL_MANIFEST_SCHEMA,
        "request_sha256": request_sha256,
        "frozen_commit": frozen_commit,
        "materials": [
            {"ref": record.ref, "content_sha256": record.content_sha256}
            for record in records
        ],
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def build_sealed_material_resolver(
    *,
    request: FinalEvalRequestV2,
    bundle: FinalEvalMaterialBundle,
    repository_root: str | os.PathLike[str],
    root_secret: str,
) -> SealedMaterialResolver:
    """The manifest-backed sealed resolver factory (A1).

    Every ref-backed research material must exist, be non-empty, resolve
    inside the disposable repository root (no traversal/symlink/reparse
    escape), be a COMMITTED regular blob in the frozen commit (never a
    worktree-only file) and its bytes SHA-256 must agree with the V2
    request, the material record and the sealed bundle -- any mismatch
    rejects BEFORE any store/evaluator construction.  The factory stores
    no callable; the bundle is sealed for the exact request digest.
    """
    if not isinstance(request, FinalEvalRequestV2):
        raise FinalEvalCompositionRejected("request must be FinalEvalRequestV2")
    if not isinstance(bundle, FinalEvalMaterialBundle):
        raise FinalEvalCompositionRejected(
            "materials must be a FinalEvalMaterialBundle"
        )
    if not isinstance(root_secret, str) or len(root_secret) < 32:
        raise FinalEvalCompositionRejected(
            "root secret must be a strong in-memory secret"
        )
    root = Path(repository_root).resolve(strict=True)
    import subprocess as _subprocess

    def _git(*args: str) -> str:
        result = _subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise FinalEvalCompositionRejected(
                "sealed repository is not readable: " + " ".join(args[:4])
            )
        return result.stdout.strip()

    def _git_raw_bytes(*args: str) -> bytes:
        result = _subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
        )
        if result.returncode != 0:
            raise FinalEvalCompositionRejected(
                "sealed repository is not readable: " + " ".join(args[:4])
            )
        return result.stdout

    frozen_commit = _git("rev-parse", "HEAD")
    frozen_tree = _git("rev-parse", "HEAD^{tree}")
    records: list[MaterialRecord] = []
    for ref_field, sha_field in _REF_SHA_FIELDS:
        ref = str(getattr(request, ref_field) or "")
        if not ref:
            raise FinalEvalCompositionRejected(
                f"{ref_field} must be a non-empty repository ref"
            )
        # lexical containment: no traversal, no absolute path
        candidate = (root / ref).resolve(strict=False)
        try:
            candidate.relative_to(root)
        except ValueError as error:
            raise FinalEvalCompositionRejected(
                f"{ref_field} escapes the repository root"
            ) from error
        # committed regular blob only (mode 100644/100755; symlink
        # 120000, tree 040000, submodule 160000 all rejected)
        entry = _git("ls-tree", "HEAD", "--", ref)
        if not entry:
            raise FinalEvalCompositionRejected(
                f"{ref_field} is not a committed path in the frozen HEAD"
            )
        mode, kind, blob_sha = entry.split()[:3]  # e.g. 100644 blob <sha>\t<path>
        if kind != "blob" or mode not in ("100644", "100755"):
            raise FinalEvalCompositionRejected(
                f"{ref_field} must be a committed regular blob "
                f"(mode {mode})"
            )
        raw = _git_raw_bytes("cat-file", "blob", blob_sha)
        if not raw:
            raise FinalEvalCompositionRejected(
                f"{ref_field} must be a non-empty committed blob"
            )
        # candidate_freeze_sha256 is the candidate-SET digest (recomputed
        # from the parsed JSON below), NOT the file bytes hash -- skip the
        # bytes comparison for the freeze artifact only
        if sha_field == "candidate_freeze_sha256":
            records.append(
                MaterialRecord(
                    ref=ref,
                    content_sha256=hashlib.sha256(raw).hexdigest(),
                    blob_sha256=blob_sha,
                    frozen_commit=frozen_commit,
                )
            )
            continue
        observed = hashlib.sha256(raw).hexdigest()
        declared_request = str(getattr(request, sha_field) or "")
        declared_bundle = str(getattr(bundle, sha_field) or "")
        if observed != declared_request or observed != declared_bundle:
            raise FinalEvalCompositionRejected(
                f"{sha_field}: sealed blob bytes {observed[:16]} do not "
                "match the request/bundle identity"
            )
        records.append(
            MaterialRecord(
                ref=ref,
                content_sha256=observed,
                blob_sha256=blob_sha,
                frozen_commit=frozen_commit,
            )
        )
    # candidate freeze is a JSON candidate set: recompute the candidate-set
    # digest from the committed freeze.json bytes (never a caller string)
    from .final_eval_request_projection import _candidate_set_sha256
    from .final_evaluator import CandidateBinding

    freeze_raw = _git_raw_bytes(
        "cat-file", "blob", records[0].blob_sha256
    )
    try:
        document = json.loads(freeze_raw.decode("utf-8"))
        entries = document.get("candidate_set", ())
        if not isinstance(entries, list) or not entries:
            raise ValueError("candidate_set must be a non-empty list")  # noqa: TRY301
        parsed = tuple(
            CandidateBinding(
                str(item.get("candidate_id", "")),
                str(item.get("candidate_sha256", "")),
            )
            for item in entries
        )
        candidate_digest = _candidate_set_sha256(parsed)
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise FinalEvalCompositionRejected(
            "candidate freeze artifact is not a valid candidate set"
        ) from error
    if candidate_digest != request.candidate_freeze_sha256:
        raise FinalEvalCompositionRejected(
            "candidate_freeze_sha256: sealed candidate set does not match "
            "the request identity"
        )
    bundle_candidate_digest = _candidate_set_sha256(
        getattr(bundle, "candidate_set", ())
    )
    if candidate_digest != bundle_candidate_digest:
        raise FinalEvalCompositionRejected(
            "candidate_freeze_sha256: sealed candidate set does not match "
            "the material bundle"
        )
    ordered = tuple(records)
    manifest_digest = _material_manifest_digest(
        ordered,
        request_sha256=request.request_sha256,
        frozen_commit=frozen_commit,
    )
    # identity three-way check at sealing time: the non-ref identities
    # (campaign/model/generation/nonce fingerprints and every declared
    # hash) must agree between request, grant lineage and the sealed
    # bundle -- a drift in ANY identity field rejects at sealing, never
    # at evaluate time.
    try:
        build_evaluator_request_v2(
            request,
            bundle,
            root_secret=root_secret,
        )
    except FinalEvalRequestRejected as error:
        raise FinalEvalCompositionRejected(
            "sealed materials do not match the request identity: "
            + str(error)
        ) from error
    # F-01 (run005): the resolver is minted INLINE inside the factory -- the
    # class exposes NO casting API (no _create/_mint), so a caller cannot
    # construct a resolver with a self-chosen secret.  Everything is
    # verified above (git ls-tree/cat-file bytes, manifest digest,
    # three-way identity); the object is sealed immutable here.
    resolver = object.__new__(SealedMaterialResolver)
    object.__setattr__(resolver, "_records", ordered)
    object.__setattr__(resolver, "_repository_root", str(root))
    object.__setattr__(resolver, "_frozen_commit", frozen_commit)
    object.__setattr__(resolver, "_frozen_tree", frozen_tree)
    object.__setattr__(resolver, "_manifest_digest", manifest_digest)
    object.__setattr__(resolver, "_request_sha256", request.request_sha256)
    object.__setattr__(resolver, "_bundle", bundle)
    object.__setattr__(
        resolver,
        "_factory_token",
        _sealed_resolver_factory_token(
            root_secret,
            manifest_digest=manifest_digest,
            frozen_commit=frozen_commit,
            frozen_tree=frozen_tree,
            request_sha256=request.request_sha256,
        ),
    )
    object.__setattr__(resolver, "_sealed", True)
    return resolver


class SyntheticHoldoutBackend(HoldoutDataBackend):
    """Bounded synthetic staging backend (Phase B).

    Reads the synthetic holdout summary JSON from the sealed data root
    (the adapter resolves only blessed refs), verifies its content hash
    against the durable ``holdout_sha256`` and returns ONLY the bounded
    summary fields.  It can never open the real Final Holdout.
    """

    def read_holdout_summary(
        self,
        *,
        path: Path,
        holdout_id: str,
        holdout_sha256: str,
    ) -> dict[str, object]:
        try:
            raw = path.read_bytes()
        except OSError as error:
            raise FinalEvalCompositionRejected(
                "synthetic holdout artifact is unreadable"
            ) from error
        observed = hashlib.sha256(raw).hexdigest()
        if observed != holdout_sha256:
            raise FinalEvalCompositionRejected(
                "synthetic holdout content hash does not match the "
                "durable holdout identity"
            )
        try:
            document = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise FinalEvalCompositionRejected(
                "synthetic holdout artifact is not strict JSON"
            ) from error
        if not isinstance(document, dict):
            raise FinalEvalCompositionRejected(
                "synthetic holdout artifact must be a JSON object"
            )
        if str(document.get("holdout_id", "")) != holdout_id:
            raise FinalEvalCompositionRejected(
                "synthetic holdout artifact does not match the durable "
                "holdout id"
            )
        summary: dict[str, object] = {
            "metrics": document.get("metrics", ()),
            "counts": document.get("counts", ()),
            "sha256s": document.get("sha256s", ()),
            "evidence_refs": document.get("evidence_refs", ()),
        }
        return summary


def _verify_resolver_manifest_digest(
    resolver: SealedMaterialResolver,
) -> None:
    """F-01 (git-native run003): recompute the manifest digest from the
    resolver's OWN records + frozen commit + request digest and fail closed
    unless it equals the stored manifest digest."""
    recomputed = _material_manifest_digest(
        tuple(resolver.records),
        request_sha256=str(getattr(resolver, "_request_sha256", "")),
        frozen_commit=str(getattr(resolver, "_frozen_commit", "")),
    )
    if recomputed != str(getattr(resolver, "_manifest_digest", "")):
        raise FinalEvalCompositionRejected(
            "sealed resolver manifest digest does not match its own "
            "records/commit -- forged"
        )


def _verify_resolver_head_snapshot(
    resolver: SealedMaterialResolver,
    repository_root: str | os.PathLike[str],
) -> None:
    """F-01 (git-native run003): the resolver is a frozen snapshot.  The
    CURRENT Git HEAD/tree must EQUAL the resolver's frozen commit/tree;
    otherwise (forged fake commit, or the repository HEAD/tree moved after
    the resolver was frozen) composition rejects before any store/
    evaluator is created."""
    import subprocess as _subprocess

    root = Path(repository_root).resolve(strict=True)
    head = _subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        capture_output=True, text=True, encoding="utf-8",
    )
    tree = _subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD^{tree}"],
        capture_output=True, text=True, encoding="utf-8",
    )
    if head.returncode != 0 or tree.returncode != 0:
        raise FinalEvalCompositionRejected(
            "sealed repository HEAD/tree is not readable for the frozen "
            "snapshot check"
        )
    head_text = head.stdout.strip()
    tree_text = tree.stdout.strip()
    frozen_commit = str(getattr(resolver, "_frozen_commit", ""))
    frozen_tree = str(getattr(resolver, "_frozen_tree", ""))
    if head_text != frozen_commit or tree_text != frozen_tree:
        raise FinalEvalCompositionRejected(
            "sealed material resolver frozen snapshot does not match the "
            "current Git HEAD/tree (HEAD drift or forged commit): "
            f"HEAD={head_text[:12]}/frozen={frozen_commit[:12]} "
            f"TREE={tree_text[:12]}/frozen={frozen_tree[:12]}"
        )


def _verify_resolver_factory_token_inline(
    resolver: SealedMaterialResolver,
    root_secret: str,
) -> None:
    """F-01 (run002): verify factory provenance INLINE from the resolver's
    slot value, recomputed with the caller's in-memory secret.  Never calls
    an overridable instance method (a forged subclass could stub it); the
    exact-type check in the caller already excluded subclasses."""
    if not isinstance(root_secret, str) or len(root_secret) < 32:
        raise FinalEvalCompositionRejected(
            "factory-token verification requires a strong in-memory secret"
        )
    stored = getattr(resolver, "_factory_token", None)
    if not isinstance(stored, str) or len(stored) != 64:
        raise FinalEvalCompositionRejected(
            "sealed material resolver factory provenance is invalid or "
            "forged"
        )
    expected = _sealed_resolver_factory_token(
        root_secret,
        manifest_digest=str(getattr(resolver, "_manifest_digest", "")),
        frozen_commit=str(getattr(resolver, "_frozen_commit", "")),
        frozen_tree=str(getattr(resolver, "_frozen_tree", "")),
        request_sha256=str(getattr(resolver, "_request_sha256", "")),
    )
    if not hmac.compare_digest(stored, expected):
        raise FinalEvalCompositionRejected(
            "sealed material resolver factory provenance is invalid or "
            "forged"
        )


def _verify_resolver_records_cover_request(
    resolver: SealedMaterialResolver,
    request: FinalEvalRequestV2,
) -> None:
    """F-01: the sealed resolver's material records must mechanically
    cover the request's six ref-backed materials (ref AND committed blob
    identity) -- a forged/partial record set is rejected before use."""
    records = resolver.records
    if not isinstance(records, tuple) or len(records) != len(_REF_SHA_FIELDS):
        raise FinalEvalCompositionRejected(
            "sealed resolver records do not cover the six materials"
        )
    covered = {
        record.ref: record
        for record in records
        if isinstance(record, MaterialRecord)
    }
    for ref_field, _sha_field in _REF_SHA_FIELDS:
        ref = str(getattr(request, ref_field) or "")
        record = covered.get(ref)
        if record is None:
            raise FinalEvalCompositionRejected(
                "sealed resolver records do not cover " + ref_field
            )
        if record.frozen_commit != resolver.frozen_commit:
            raise FinalEvalCompositionRejected(
                "sealed resolver record was frozen in a different commit"
            )


def compose_holdout_store(
    context: AuthorizedFinalEvalContext,
) -> SqliteHoldoutStore:
    """The Authority-backed HoldoutStore (Phase B): reads + verifies the
    committed begin-time consumption receipt from the SAME Authority
    database -- never a third ledger, never a second consume record."""
    if not isinstance(context.authority_capability, str) or len(
        context.authority_capability
    ) < 32:
        raise FinalEvalCompositionRejected(
            "context authority capability must be a strong in-memory secret"
        )
    return SqliteHoldoutStore(
        authority=_AuthorityStore(root_secret=context.authority_capability)
    )


def compose_staging_backend() -> SyntheticHoldoutBackend:
    """The sealed synthetic staging backend (Phase B): opens exactly one
    synthetic staging artifact under the sealed data root."""
    return SyntheticHoldoutBackend()


def verify_sealed_material_content(
    request: FinalEvalRequestV2,
    materials: FinalEvalMaterialBundle,
    repository_root: str | os.PathLike[str],
) -> None:
    """Recompute every ref-backed artifact digest from the SEALED
    COMMITTED blob bytes and fail closed unless it equals the request and
    bundle identity (CR-010 F-01).

    F-01: artifacts are read ONLY from the frozen commit (git ls-tree +
    cat-file), never from the working tree, and a MISSING / uncommitted /
    directory / symlink / reparse material is rejected immediately --
    there is NO skip.  The frozen commit HEAD is captured once so the
    verification is a single consistent snapshot.
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
            raise FinalEvalCompositionRejected(
                "sealed repository is not readable: " + " ".join(args[:4])
            )
        return result.stdout.strip()

    frozen_commit = _git("rev-parse", "HEAD")

    def _committed_blob_sha(ref: str) -> str:
        candidate = (root / ref).resolve(strict=False)
        try:
            candidate.relative_to(root)
        except ValueError as error:
            raise FinalEvalCompositionRejected(
                "material ref escapes the repository root: " + ref
            ) from error
        entry = _git("ls-tree", frozen_commit, "--", ref)
        if not entry:
            raise FinalEvalCompositionRejected(
                "sealed material is MISSING from the frozen commit: " + ref
            )
        mode, kind, blob_sha = entry.split()[:3]
        if kind != "blob" or mode not in ("100644", "100755"):
            raise FinalEvalCompositionRejected(
                "sealed material must be a committed regular blob "
                f"({mode}/{kind}): " + ref
            )
        return blob_sha

    def _committed_blob_bytes(blob_sha: str, ref: str) -> bytes:
        raw = _subprocess.run(
            ["git", "-C", str(root), "cat-file", "blob", blob_sha],
            capture_output=True,
        )
        if raw.returncode != 0:
            raise FinalEvalCompositionRejected(
                "sealed material blob is unreadable: " + ref
            )
        if not raw.stdout:
            raise FinalEvalCompositionRejected(
                "sealed material must be a non-empty committed blob: " + ref
            )
        return raw.stdout

    for ref_field, sha_field in (
        ("code_ref", "code_sha256"),
        ("features_ref", "features_sha256"),
        ("threshold_ref", "threshold_sha256"),
        ("execution_spec_ref", "execution_spec_sha256"),
        ("roster_ref", "roster_sha256"),
    ):
        ref = str(getattr(request, ref_field) or "")
        if not ref:
            raise FinalEvalCompositionRejected(
                f"{ref_field} must be a non-empty repository ref"
            )
        blob_sha = _committed_blob_sha(ref)
        raw = _committed_blob_bytes(blob_sha, ref)
        observed = hashlib.sha256(raw).hexdigest()
        declared_request = str(getattr(request, sha_field) or "")
        declared_bundle = str(getattr(materials, sha_field) or "")
        if observed != declared_request or observed != declared_bundle:
            raise FinalEvalCompositionRejected(
                f"{sha_field}: sealed committed blob {observed[:16]} does "
                "not match the request/bundle identity"
            )
    # candidate freeze: parse the COMMITTED freeze.json and recompute the
    # candidate-set digest (F-01: the file bytes hash itself is a
    # different identity than the candidate-set digest)
    freeze_ref = str(request.candidate_freeze_ref or "")
    if not freeze_ref:
        raise FinalEvalCompositionRejected(
            "candidate_freeze_ref must be a non-empty repository ref"
        )
    freeze_blob = _committed_blob_sha(freeze_ref)
    freeze_raw = _committed_blob_bytes(freeze_blob, freeze_ref)
    from .final_eval_request_projection import _candidate_set_sha256
    from .final_evaluator import CandidateBinding

    try:
        document = json.loads(freeze_raw.decode("utf-8"))
        entries = document.get("candidate_set", ())
        if not isinstance(entries, list) or not entries:
            raise ValueError("candidate_set must be a non-empty list")  # noqa: TRY301
        parsed = tuple(
            CandidateBinding(
                str(item.get("candidate_id", "")),
                str(item.get("candidate_sha256", "")),
            )
            for item in entries
        )
        observed = _candidate_set_sha256(parsed)
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise FinalEvalCompositionRejected(
            "candidate freeze artifact is not a valid candidate set"
        ) from error
    if observed != request.candidate_freeze_sha256:
        raise FinalEvalCompositionRejected(
            "candidate_freeze_sha256: sealed committed candidate set does "
            "not match the request identity"
        )
    bundle_candidate_digest = _candidate_set_sha256(
        getattr(materials, "candidate_set", ())
    )
    if observed != bundle_candidate_digest:
        raise FinalEvalCompositionRejected(
            "candidate_freeze_sha256: sealed committed candidate set does "
            "not match the material bundle"
        )


def compose_final_eval_runtime(
    context: AuthorizedFinalEvalContext,
) -> FinalEvalRuntime:
    """The ONLY production factory (CR-010 C0, Phase B).

    Verifies the P8 grant lineage (phase, actor, identity, attempt), the
    durable request identity against the sealed materials (every digest
    recomputed -- any drift rejects BEFORE any store or evaluator is
    opened), then constructs the runtime with the Authority-backed
    HoldoutStore, the synthetic staging backend and the create-only
    evidence sink.
    """
    if not isinstance(context, AuthorizedFinalEvalContext):
        raise FinalEvalCompositionRejected(
            "composition requires an AuthorizedFinalEvalContext"
        )
    if not isinstance(context.request, FinalEvalRequestV2):
        raise FinalEvalCompositionRejected("context request must be FinalEvalRequestV2")
    if not isinstance(context.actor, Actor):
        raise FinalEvalCompositionRejected("context actor is invalid")
    if not isinstance(context.identity, AuthorityIdentity):
        raise FinalEvalCompositionRejected("context identity is invalid")
    if not isinstance(context.authority_capability, str) or len(
        context.authority_capability
    ) < 32:
        raise FinalEvalCompositionRejected(
            "context authority capability must be a strong in-memory secret"
        )
    grant = context.grant
    grant_phase = getattr(grant, "phase", None)
    if grant_phase is not Phase.P8:
        raise FinalEvalCompositionRejected(
            "FinalEval composition requires a P8 grant, got "
            + str(getattr(grant_phase, "value", grant_phase))
        )
    grant_actor = getattr(grant, "actor", None)
    grant_identity = getattr(grant, "identity", None)
    if (
        getattr(grant_actor, "actor_id", None) != context.request.actor_id
        or getattr(grant_actor, "actor_type", None)
        != context.request.actor_type
        or getattr(grant_actor, "invocation_id", None)
        != context.request.invocation_id
    ):
        raise FinalEvalCompositionRejected(
            "composition grant actor does not match the request actor"
        )
    if (
        getattr(grant_identity, "plan_hash", None)
        != context.request.authority_plan_hash
    ):
        raise FinalEvalCompositionRejected(
            "composition grant plan does not match the request identity"
        )
    if (
        getattr(grant_identity, "scope_hash", None)
        != context.identity.scope_hash
        or getattr(grant_identity, "instruction_policy_hash", None)
        != context.identity.instruction_policy_hash
    ):
        raise FinalEvalCompositionRejected(
            "composition grant identity does not match the context identity"
        )
    if getattr(grant, "attempt_id", None) != context.attempt_id:
        raise FinalEvalCompositionRejected(
            "composition grant attempt does not match the context attempt"
        )
    if context.attempt_id != context.request.attempt_id:
        raise FinalEvalCompositionRejected(
            "context attempt does not match the request attempt"
        )
    if not isinstance(context.data_root, TrustedEvaluatorDataRoot):
        raise FinalEvalCompositionRejected(
            "context data_root must be a sealed TrustedEvaluatorDataRoot"
        )
    if not isinstance(context.repository_root, str) or not Path(
        context.repository_root
    ).is_dir():
        raise FinalEvalCompositionRejected(
            "context repository root is unavailable"
        )
    # CR-010 F-01: the production composition accepts ONLY a SEALED
    # material resolver -- an arbitrary callable (or None) is rejected, so
    # the caller can never inject an unsealed material bundle.
    resolver = context.material_resolver
    # F-01 (run002): a STRICT exact-type check -- isinstance alone allows a
    # forged SUBCLASS whose overridden methods disable token verification.
    # Only the exact class is accepted; any subclass is rejected here.
    if type(resolver) is not SealedMaterialResolver:
        raise FinalEvalCompositionRejected(
            "composition requires the exact SealedMaterialResolver type "
            "-- a forged subclass, callable or other instance is never "
            "accepted by the production root"
        )
    # F-01: factory provenance is verified INLINE from the slot value (not
    # via an overridable instance method), recomputed with the in-memory
    # root capability.  A directly-constructed / object.__new__-stuffed
    # resolver with a wrong token is rejected before any store/evaluator.
    _verify_resolver_factory_token_inline(
        resolver, context.authority_capability
    )
    # F-01: the sealed records must mechanically cover the request's six
    # ref-backed materials (a forged record set is rejected here too).
    _verify_resolver_records_cover_request(resolver, context.request)
    # F-01 (git-native run003): the manifest digest must be recomputable
    # from the resolver's own records/commit/request (records, manifest,
    # commit/tree all enter the verification -- never only the token).
    _verify_resolver_manifest_digest(resolver)
    # F-01 (git-native run003): the resolver is a FROZEN SNAPSHOT of the
    # repository.  The CURRENT Git HEAD/tree at compose time must EQUAL the
    # resolver's frozen commit/tree -- a resolver built from a fake commit
    # (direct _create forgery) or a HEAD that moved after the resolver was
    # frozen (HEAD drift) is rejected here, before any store/evaluator.
    _verify_resolver_head_snapshot(resolver, context.repository_root)
    # the durable request identity must EXACTLY match the sealed materials
    # (every digest recomputed; raw nonce stays in memory) and the sealed
    # repository artifact content must agree with the declared digests.
    materials = resolver.resolve(context.request)
    if not isinstance(materials, FinalEvalMaterialBundle):
        raise FinalEvalCompositionRejected(
            "material resolver must return a FinalEvalMaterialBundle"
        )
    verify_sealed_material_content(
        context.request,
        materials,
        context.repository_root,
    )
    try:
        build_evaluator_request_v2(
            context.request,
            materials,
            root_secret=context.authority_capability,
        )
    except FinalEvalRequestRejected as error:
        raise FinalEvalCompositionRejected(
            "composition materials do not match the request identity: "
            + str(error)
        ) from error
    if materials.attempt_id != context.attempt_id:
        raise FinalEvalCompositionRejected(
            "composition materials attempt does not match the context"
        )
    # construct the sealed root capability from the verified context
    root_capability = FinalEvalRootCapability.create(
        root_secret=context.authority_capability,
        repository_root=context.repository_root,
    )
    return FinalEvalRuntime(
        inputs=FinalEvalRuntimeInputs(
            authority_capability=context.authority_capability,
            root_capability=root_capability,
            worker_launcher=context.worker_launcher,
            evidence_sink=context.evidence_sink,
            attempt_id=context.attempt_id,
            material_resolver=resolver.resolve,
        ),
    )


__all__ = [
    "AuthorizedFinalEvalContext",
    "FinalEvalCompositionError",
    "FinalEvalCompositionRejected",
    "MaterialRecord",
    "SealedMaterialResolver",
    "SyntheticHoldoutBackend",
    "build_sealed_material_resolver",
    "compose_final_eval_runtime",
    "compose_holdout_store",
    "compose_staging_backend",
    "verify_sealed_material_content",
]
