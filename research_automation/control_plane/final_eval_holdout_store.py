"""Authority-backed durable Holdout consume store (CR-010 C0, Phase B).

The production Holdout consume is ONE immutable begin-time receipt in the
SAME Authority database/transaction as the ticket/binding/outbox rows --
there is no third ledger and no second consume record.  This module reads
and VERIFIES that committed receipt (the receipt hash is recomputed from
the durable identifiers); runtime replay reads the receipt and never
reopens the Holdout or calls a backend again.

``InMemoryHoldoutStore`` and the V1 ``AuthorityBroker.consume`` path never
serve production composition.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from .final_evaluator import HoldoutStore
from .stores import FinalEvalHoldoutConsumption, _AuthorityStore

CONSUMPTION_SCHEMA = "control_plane.final_eval_holdout_consumption.v1"


class FinalEvalHoldoutStoreError(RuntimeError):
    """Base error for the Authority-backed Holdout consume store."""


class FinalEvalConsumptionRejected(FinalEvalHoldoutStoreError):
    """The committed consumption receipt is invalid or unreadable."""


@dataclass(frozen=True, slots=True)
class HoldoutConsumedV2:
    """The durable begin-time consume receipt (durable identifiers only).

    Carries ticket, V2 request digest, nonce fingerprint, attempt and
    holdout id/hash -- NEVER the raw nonce and NEVER the terminal outcome
    (the verified worker outcome belongs to the later fixed result claim).
    """

    ticket_id: str
    request_sha256: str
    nonce_fingerprint: str
    holdout_id: str
    holdout_sha256: str
    attempt_id: str
    actor_id: str
    actor_type: str
    invocation_id: str
    consumed_at_utc: str
    receipt_sha256: str

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": CONSUMPTION_SCHEMA,
            "ticket_id": self.ticket_id,
            "request_sha256": self.request_sha256,
            "nonce_fingerprint": self.nonce_fingerprint,
            "holdout_id": self.holdout_id,
            "holdout_sha256": self.holdout_sha256,
            "attempt_id": self.attempt_id,
            "actor_id": self.actor_id,
            "actor_type": self.actor_type,
            "invocation_id": self.invocation_id,
            "consumed_at_utc": self.consumed_at_utc,
            "receipt_sha256": self.receipt_sha256,
        }


def consumption_receipt_sha256(
    *,
    ticket_id: str,
    request_sha256: str,
    nonce_fingerprint: str,
    holdout_id: str,
    holdout_sha256: str,
    attempt_id: str,
    actor_id: str,
    actor_type: str,
    invocation_id: str,
    consumed_at_utc: str,
) -> str:
    """The receipt digest over the durable identifiers (canonical JSON).

    Identical construction to the Authority transaction's insert, so the
    committed receipt hash can be recomputed and verified independently.
    """
    payload = {
        "ticket_id": ticket_id,
        "request_sha256": request_sha256,
        "nonce_fingerprint": nonce_fingerprint,
        "holdout_id": holdout_id,
        "holdout_sha256": holdout_sha256,
        "attempt_id": attempt_id,
        "actor_id": actor_id,
        "actor_type": actor_type,
        "invocation_id": invocation_id,
        "consumed_at_utc": consumed_at_utc,
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def verify_consumption_receipt(
    consumption: FinalEvalHoldoutConsumption,
) -> None:
    """Recompute the receipt hash over the durable row and compare --
    a tampered immutable consume receipt fails closed."""
    if not isinstance(consumption, FinalEvalHoldoutConsumption):
        raise FinalEvalConsumptionRejected(
            "holdout consumption must be a FinalEvalHoldoutConsumption"
        )
    expected = consumption_receipt_sha256(
        ticket_id=consumption.ticket_id,
        request_sha256=consumption.request_sha256,
        nonce_fingerprint=consumption.nonce_fingerprint,
        holdout_id=consumption.holdout_id,
        holdout_sha256=consumption.holdout_sha256,
        attempt_id=consumption.attempt_id,
        actor_id=consumption.actor_id,
        actor_type=consumption.actor_type,
        invocation_id=consumption.invocation_id,
        consumed_at_utc=consumption.consumed_at_utc,
    )
    if expected != consumption.receipt_sha256:
        raise FinalEvalConsumptionRejected(
            "holdout consumption receipt hash does not match its "
            "durable identifiers"
        )


class SqliteHoldoutStore(HoldoutStore):
    """Authority-backed durable consume store (CR-010 C0, Phase B).

    Reads and VERIFIES the one committed immutable consumption receipt
    from the SAME Authority database/transaction -- never a second
    database, never a second consume record after the worker runs, never
    a backend call.  The ``HoldoutStore.consume`` contract is satisfied
    by REJECTION: the begin-time transaction already committed the one
    receipt, so any second consume attempt fails closed and leaves the
    count unchanged; replay belongs in the durable-binding read path.
    """

    def __init__(self, *, authority: _AuthorityStore) -> None:
        if not isinstance(authority, _AuthorityStore):
            raise TypeError("authority must be an _AuthorityStore")
        self._authority = authority

    def consume(
        self,
        *,
        nonce: str,
        request_sha256: str,
        outcome: str,
        durable_ticket_id: str | None = None,
        durable_request_sha256: str | None = None,
        durable_nonce_fingerprint: str | None = None,
    ) -> object:
        raise FinalEvalConsumptionRejected(
            "the Authority-backed HoldoutStore never consumes a second "
            "nonce: the begin-time Authority transaction already committed "
            "the one immutable consumption receipt"
        )

    def read_consumption(self, ticket_id: str) -> HoldoutConsumedV2:
        """Read + verify the committed receipt for one binding."""
        consumption = self._authority.final_eval_holdout_consumption(
            ticket_id
        )
        verify_consumption_receipt(consumption)
        return HoldoutConsumedV2(
            ticket_id=consumption.ticket_id,
            request_sha256=consumption.request_sha256,
            nonce_fingerprint=consumption.nonce_fingerprint,
            holdout_id=consumption.holdout_id,
            holdout_sha256=consumption.holdout_sha256,
            attempt_id=consumption.attempt_id,
            actor_id=consumption.actor_id,
            actor_type=consumption.actor_type,
            invocation_id=consumption.invocation_id,
            consumed_at_utc=consumption.consumed_at_utc,
            receipt_sha256=consumption.receipt_sha256,
        )

    def consumption_count(self, request_sha256: str) -> int:
        """The durable consume count for one request digest."""
        return self._authority.final_eval_holdout_consumption_count(
            request_sha256
        )


__all__ = [
    "CONSUMPTION_SCHEMA",
    "FinalEvalConsumptionRejected",
    "FinalEvalHoldoutStoreError",
    "HoldoutConsumedV2",
    "SqliteHoldoutStore",
    "consumption_receipt_sha256",
    "verify_consumption_receipt",
]
