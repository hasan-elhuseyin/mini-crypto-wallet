"""Event envelope + the platform's event catalogue.

Envelope design notes
---------------------
* ``event_id`` is a *deterministic* UUIDv5 derived from ``(event_type,
  dedupe_key)`` whenever the producer can name a natural key. If the same
  business fact is emitted twice (retry, replay, restart) it carries the same
  id, so consumer-side deduplication works even across producer restarts.
* ``correlation_id`` follows the business operation; ``causation_id`` names the
  event that caused this one, which makes the flow reconstructable.
* ``schema_version`` is explicit: consumers must tolerate additive changes.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .correlation import get_correlation_id, new_correlation_id

__all__ = ["EventEnvelope", "new_event", "EventType", "STREAM_PREFIX", "stream_for"]

#: UUIDv5 namespace for deterministic event ids (fixed for the lifetime of the platform).
EVENT_NAMESPACE = uuid.UUID("6d1f0d1e-3f2b-4c33-9a7e-0b1a1f9d4c11")

STREAM_PREFIX = "mcw:events"


class EventType:
    """Event names are part of the public contract between services."""

    # blockchain-service -> wallet-service
    DEPOSIT_DETECTED = "deposit.detected"
    DEPOSIT_CONFIRMED = "deposit.confirmed"
    DEPOSIT_REORGED = "deposit.reorged"
    TX_BROADCASTED = "blockchain.transaction.broadcasted"
    TX_CONFIRMED = "blockchain.transaction.confirmed"
    TX_FAILED = "blockchain.transaction.failed"

    # wallet-service -> blockchain-service
    TRANSFER_REQUESTED = "transfer.requested"

    ALL = (
        DEPOSIT_DETECTED,
        DEPOSIT_CONFIRMED,
        DEPOSIT_REORGED,
        TX_BROADCASTED,
        TX_CONFIRMED,
        TX_FAILED,
        TRANSFER_REQUESTED,
    )


def stream_for(event_type: str) -> str:
    """One Redis stream per event type: independent lag, retries and DLQ per topic."""
    return f"{STREAM_PREFIX}:{event_type}"


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    event_id: str
    event_type: str
    schema_version: int = 1
    occurred_at: datetime
    producer: str
    correlation_id: str
    causation_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    def to_wire(self) -> dict[str, str]:
        """Redis stream fields must be flat strings."""
        return {"body": self.model_dump_json()}

    @classmethod
    def from_wire(cls, fields: dict[str, Any]) -> EventEnvelope:
        body = fields.get("body") or fields.get(b"body")
        if isinstance(body, bytes):
            body = body.decode()
        return cls.model_validate_json(body)


def new_event(
    event_type: str,
    payload: dict[str, Any],
    *,
    producer: str,
    dedupe_key: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> EventEnvelope:
    event_id = (
        str(uuid.uuid5(EVENT_NAMESPACE, f"{event_type}|{dedupe_key}"))
        if dedupe_key
        else str(uuid.uuid4())
    )
    return EventEnvelope(
        event_id=event_id,
        event_type=event_type,
        occurred_at=datetime.now(UTC),
        producer=producer,
        correlation_id=correlation_id or get_correlation_id() or new_correlation_id(),
        causation_id=causation_id,
        payload=payload,
    )
