"""Transactional outbox.

The problem it solves: *"the database committed but the event was never
published"*. Handlers write their state change **and** the outbox row in one
local transaction. A separate relay publishes rows to the bus and marks them
published. If the relay dies between publish and mark, the row is republished
-- at-least-once -- and consumers deduplicate on ``event_id``.

The relay claims rows with ``FOR UPDATE SKIP LOCKED`` so several relay replicas
can run concurrently without publishing the same row twice.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .bus import EventBus
from .events import EventEnvelope
from .logging import get_logger
from .metrics import OUTBOX_BACKLOG

__all__ = ["OutboxRelay", "enqueue_event"]

log = get_logger("outbox")

_INSERT_SQL = text(
    """
    INSERT INTO outbox (event_id, event_type, envelope, correlation_id)
    VALUES (:event_id, :event_type, CAST(:envelope AS jsonb), :correlation_id)
    ON CONFLICT (event_id) DO NOTHING
    """
)

_CLAIM_SQL = text(
    """
    SELECT id, envelope, attempts
      FROM outbox
     WHERE published_at IS NULL
       AND (next_attempt_at IS NULL OR next_attempt_at <= now())
     ORDER BY id
     FOR UPDATE SKIP LOCKED
     LIMIT :limit
    """
)

_MARK_PUBLISHED_SQL = text(
    "UPDATE outbox SET published_at = now(), last_error = NULL WHERE id = ANY(:ids)"
)

_MARK_FAILED_SQL = text(
    """
    UPDATE outbox
       SET attempts = attempts + 1,
           last_error = :error,
           next_attempt_at = now() + (interval '1 second' * :backoff)
     WHERE id = :id
    """
)

_BACKLOG_SQL = text("SELECT count(*) FROM outbox WHERE published_at IS NULL")


async def enqueue_event(session: AsyncSession, envelope: EventEnvelope) -> None:
    """Append an event to the outbox **inside the caller's transaction**.

    ``ON CONFLICT DO NOTHING`` on ``event_id`` means re-running a handler that
    builds a deterministic event id never enqueues the same fact twice.
    """
    await session.execute(
        _INSERT_SQL,
        {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "envelope": envelope.model_dump_json(),
            "correlation_id": envelope.correlation_id,
        },
    )


class OutboxRelay:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[Any],
        bus: EventBus,
        *,
        batch_size: int = 100,
        max_backoff: int = 60,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._bus = bus
        self._batch_size = batch_size
        self._max_backoff = max_backoff

    async def run_once(self) -> int:
        """Publish one batch; returns how many events were published."""
        published = 0
        async with self._sessionmaker() as session, session.begin():
            rows = (
                await session.execute(_CLAIM_SQL, {"limit": self._batch_size})
            ).mappings().all()
            if not rows:
                return 0
            succeeded: list[int] = []
            for row in rows:
                envelope = EventEnvelope.model_validate(row["envelope"])
                try:
                    await self._bus.publish(envelope)
                    succeeded.append(row["id"])
                    published += 1
                except Exception as exc:
                    # Publishing failed: the row stays unpublished and is
                    # retried with backoff. The business state is already
                    # durable, so nothing is lost -- only delayed.
                    backoff = min(self._max_backoff, 2 ** min(int(row["attempts"]), 6))
                    await session.execute(
                        _MARK_FAILED_SQL,
                        {"id": row["id"], "error": str(exc)[:1000], "backoff": backoff},
                    )
                    log.error(
                        "outbox.publish_failed",
                        outbox_id=row["id"],
                        event_type=envelope.event_type,
                        correlation_id=envelope.correlation_id,
                        retry_in_seconds=backoff,
                        error=str(exc),
                    )
            if succeeded:
                await session.execute(_MARK_PUBLISHED_SQL, {"ids": succeeded})
        return published

    async def backlog(self) -> int:
        async with self._sessionmaker() as session:
            count = int((await session.execute(_BACKLOG_SQL)).scalar_one())
        OUTBOX_BACKLOG.set(count)
        return count
