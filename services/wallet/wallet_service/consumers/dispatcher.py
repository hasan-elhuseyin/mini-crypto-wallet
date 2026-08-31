"""Idempotent event dispatch for wallet-service.

The dedupe row (``processed_events``) and the ledger postings share one
database transaction. Combined with the unique constraint on
``ledger_entries``, that gives two independent layers of protection against
double counting:

* replay of the *same event* -> stopped by ``processed_events``;
* two *different events* describing the same financial fact (a re-emitted
  deposit, a re-delivered confirmation from another producer) -> stopped by
  ``uq_ledger_idempotency``.

Only the second one survives a database restore or a change of event id
strategy, which is why both exist.
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
from ..services.deposits import credit_deposit, reverse_deposit
from ..services.transfers import fail_transfer, mark_broadcasted, settle_transfer

log = get_logger("consumer")

CONSUMER_NAME = "wallet-service"
SUBSCRIBED_EVENTS = (
    EventType.DEPOSIT_DETECTED,
    EventType.DEPOSIT_CONFIRMED,
    EventType.DEPOSIT_REORGED,
    EventType.TX_BROADCASTED,
    EventType.TX_CONFIRMED,
    EventType.TX_FAILED,
)


class WalletDispatcher:
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
                return await self._handle(session, envelope)

    async def _handle(self, session, envelope: EventEnvelope) -> str:
        payload = envelope.payload
        match envelope.event_type:
            case EventType.DEPOSIT_DETECTED:
                # Observability only. Crediting here would credit money that a
                # reorg can still take away.
                log.info(
                    "deposit.detected_noted",
                    tx_hash=payload.get("tx_hash"), to_address=payload.get("to_address"),
                    amount=payload.get("amount"), confirmations=payload.get("confirmations"),
                )
                return "processed"
            case EventType.DEPOSIT_CONFIRMED:
                return await credit_deposit(session, payload)
            case EventType.DEPOSIT_REORGED:
                return await reverse_deposit(session, payload)
            case EventType.TX_BROADCASTED:
                return await mark_broadcasted(session, payload)
            case EventType.TX_CONFIRMED:
                return await settle_transfer(session, payload)
            case EventType.TX_FAILED:
                return await fail_transfer(session, payload)
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
        log.error(
            "consumer.dead_letter_recorded",
            event_type=item.envelope.event_type, event_id=item.envelope.event_id,
        )
