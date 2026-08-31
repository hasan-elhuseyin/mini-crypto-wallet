"""Idempotent event dispatch for blockchain-service.

The dedupe row and the handler's side effects share **one** database
transaction. Either both land or neither does, so "handled but not recorded"
(and its mirror, "recorded but not handled") cannot happen -- which is what
makes at-least-once delivery safe here.
"""

from __future__ import annotations

import uuid
from typing import Any

from mcw_common.bus import DeadLetter
from mcw_common.correlation import correlation_scope
from mcw_common.events import EventEnvelope, EventType
from mcw_common.logging import get_logger
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..models import DeadLetter as DeadLetterRow
from ..models import ProcessedEvent
from ..services.transactions import accept_transfer_request

log = get_logger("consumer")

CONSUMER_NAME = "blockchain-service"
SUBSCRIBED_EVENTS = (EventType.TRANSFER_REQUESTED,)


class BlockchainDispatcher:
    def __init__(self, ctx: Any) -> None:
        self._ctx = ctx

    async def dispatch(self, envelope: EventEnvelope) -> str:
        with correlation_scope(envelope.correlation_id):
            async with self._ctx.db.sessionmaker() as session, session.begin():
                claimed = (
                    await session.execute(
                        pg_insert(ProcessedEvent)
                        .values(
                            consumer=CONSUMER_NAME,
                            event_id=uuid.UUID(envelope.event_id),
                            event_type=envelope.event_type,
                        )
                        .on_conflict_do_nothing(index_elements=["consumer", "event_id"])
                        .returning(ProcessedEvent.event_id)
                    )
                ).scalar_one_or_none()
                if claimed is None:
                    log.info(
                        "consumer.duplicate_ignored",
                        event_type=envelope.event_type, event_id=envelope.event_id,
                    )
                    return "duplicate"

                if envelope.event_type == EventType.TRANSFER_REQUESTED:
                    return await accept_transfer_request(
                        session, envelope.payload, envelope.correlation_id
                    )
                log.warning("consumer.unhandled_event_type", event_type=envelope.event_type)
                return "ignored"

    async def record_dead_letter(self, item: DeadLetter) -> None:
        async with self._ctx.db.sessionmaker() as session, session.begin():
            session.add(
                DeadLetterRow(
                    consumer=CONSUMER_NAME,
                    event_id=uuid.UUID(item.envelope.event_id),
                    event_type=item.envelope.event_type,
                    envelope=item.envelope.model_dump(mode="json"),
                    delivery_count=item.delivery_count,
                    error=item.error,
                )
            )
