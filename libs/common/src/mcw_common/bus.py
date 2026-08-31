"""Message bus abstraction over Redis Streams.

Why Redis Streams
-----------------
It gives us the three properties this system actually needs -- consumer groups
(competing consumers), per-message acknowledgement with a Pending Entries List
(so a crashed consumer's in-flight message is redelivered), and a delivery
counter we can use to dead-letter poison messages -- while adding exactly one
infrastructure component that we already want for other reasons. Kafka would
give ordering-per-partition and long retention we do not need here; RabbitMQ
would need a separate DLX/retry topology to get the same behaviour.

Delivery semantics are **at-least-once**. Every consumer is therefore
idempotent (see `processed_events` in each service).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from .events import EventEnvelope, stream_for
from .logging import get_logger
from .metrics import DEAD_LETTERS, EVENT_HANDLER_LATENCY, EVENTS_CONSUMED, EVENTS_PUBLISHED

__all__ = [
    "DeliveredEvent",
    "EventBus",
    "RedisStreamsBus",
    "InMemoryBus",
    "ConsumerRuntime",
    "DeadLetter",
]

log = get_logger("bus")


@dataclass(slots=True)
class DeliveredEvent:
    stream: str
    message_id: str
    envelope: EventEnvelope
    delivery_count: int


@dataclass(slots=True)
class DeadLetter:
    envelope: EventEnvelope
    delivery_count: int
    error: str


class EventBus(Protocol):
    async def publish(self, envelope: EventEnvelope) -> None: ...
    async def ensure_group(self, event_types: Iterable[str], group: str) -> None: ...
    async def read_new(
        self, event_types: Iterable[str], group: str, consumer: str, count: int, block_ms: int
    ) -> list[DeliveredEvent]: ...
    async def claim_stale(
        self, event_types: Iterable[str], group: str, consumer: str, min_idle_ms: int, count: int
    ) -> tuple[list[DeliveredEvent], list[DeliveredEvent]]: ...
    async def ack(self, stream: str, group: str, message_id: str) -> None: ...
    async def dead_letter(self, group: str, item: DeadLetter) -> None: ...


class RedisStreamsBus:
    def __init__(self, redis: Any, *, dlq_stream: str = "mcw:dlq", max_len: int = 100_000) -> None:
        self._redis = redis
        self._dlq_stream = dlq_stream
        self._max_len = max_len

    async def publish(self, envelope: EventEnvelope) -> None:
        await self._redis.xadd(
            stream_for(envelope.event_type),
            envelope.to_wire(),
            maxlen=self._max_len,
            approximate=True,
        )
        EVENTS_PUBLISHED.labels(event_type=envelope.event_type).inc()

    async def ensure_group(self, event_types: Iterable[str], group: str) -> None:
        for event_type in event_types:
            try:
                await self._redis.xgroup_create(
                    stream_for(event_type), group, id="0", mkstream=True
                )
            except Exception as exc:  # BUSYGROUP -> already created
                if "BUSYGROUP" not in str(exc):
                    raise

    async def read_new(
        self, event_types: Iterable[str], group: str, consumer: str, count: int, block_ms: int
    ) -> list[DeliveredEvent]:
        streams = {stream_for(t): ">" for t in event_types}
        if not streams:
            return []
        # Careful: `BLOCK 0` means *block forever* in Redis. A non-positive
        # value here means "poll and return immediately" instead.
        response = await self._redis.xreadgroup(
            group, consumer, streams, count=count,
            block=block_ms if block_ms and block_ms > 0 else None,
        )
        return [
            DeliveredEvent(
                stream=stream, message_id=msg_id, envelope=EventEnvelope.from_wire(fields),
                delivery_count=1,
            )
            for stream, messages in (response or [])
            for msg_id, fields in messages
        ]

    async def claim_stale(
        self, event_types: Iterable[str], group: str, consumer: str, min_idle_ms: int, count: int
    ) -> tuple[list[DeliveredEvent], list[DeliveredEvent]]:
        """Reclaim messages abandoned by a crashed/slow consumer.

        Returns ``(retryable, exhausted)``. ``exhausted`` are messages whose
        delivery count has passed the limit; the caller dead-letters them.
        """
        retryable: list[DeliveredEvent] = []
        exhausted: list[DeliveredEvent] = []
        for event_type in event_types:
            stream = stream_for(event_type)
            # XPENDING ... IDLE <ms> - + <count>: only entries that have been
            # unacknowledged for longer than the idle window, i.e. entries whose
            # consumer most likely died.
            pending = await self._redis.xpending_range(
                stream, group, min="-", max="+", count=count, idle=min_idle_ms
            )
            if not pending:
                continue
            counts = {str(p["message_id"]): int(p["times_delivered"]) for p in pending}
            claimed = await self._redis.xclaim(
                stream, group, consumer, min_idle_time=min_idle_ms,
                message_ids=list(counts.keys()),
            )
            for msg_id, fields in claimed:
                if not fields:  # message was trimmed away; drop the PEL entry
                    await self._redis.xack(stream, group, msg_id)
                    continue
                retryable.append(
                    DeliveredEvent(
                        stream=stream,
                        message_id=str(msg_id),
                        envelope=EventEnvelope.from_wire(fields),
                        delivery_count=counts.get(str(msg_id), 1),
                    )
                )
        return retryable, exhausted

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        await self._redis.xack(stream, group, message_id)

    async def dead_letter(self, group: str, item: DeadLetter) -> None:
        await self._redis.xadd(
            f"{self._dlq_stream}:{group}",
            {
                "body": item.envelope.model_dump_json(),
                "error": item.error[:2000],
                "delivery_count": str(item.delivery_count),
                "dead_lettered_at": str(time.time()),
            },
            maxlen=self._max_len,
            approximate=True,
        )
        DEAD_LETTERS.labels(event_type=item.envelope.event_type).inc()


class InMemoryBus:
    """Deterministic in-process bus used by unit tests.

    Mirrors the Redis Streams semantics we rely on (consumer groups, pending
    entries, delivery counts) closely enough to test retry/DLQ behaviour.
    """

    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, EventEnvelope]]] = {}
        self._cursors: dict[tuple[str, str], int] = {}
        self._pending: dict[tuple[str, str], dict[str, tuple[EventEnvelope, int, float]]] = {}
        self.dead_letters: list[DeadLetter] = []
        self._seq = 0

    async def publish(self, envelope: EventEnvelope) -> None:
        self._seq += 1
        stream = stream_for(envelope.event_type)
        self.streams.setdefault(stream, []).append((f"{self._seq}-0", envelope))
        EVENTS_PUBLISHED.labels(event_type=envelope.event_type).inc()

    async def ensure_group(self, event_types: Iterable[str], group: str) -> None:
        for event_type in event_types:
            key = (stream_for(event_type), group)
            self._cursors.setdefault(key, 0)
            self._pending.setdefault(key, {})

    async def read_new(
        self, event_types: Iterable[str], group: str, consumer: str, count: int, block_ms: int
    ) -> list[DeliveredEvent]:
        out: list[DeliveredEvent] = []
        for event_type in event_types:
            stream = stream_for(event_type)
            key = (stream, group)
            messages = self.streams.get(stream, [])
            cursor = self._cursors.get(key, 0)
            for msg_id, envelope in messages[cursor : cursor + count]:
                self._pending.setdefault(key, {})[msg_id] = (envelope, 1, time.monotonic())
                out.append(DeliveredEvent(stream, msg_id, envelope, 1))
            self._cursors[key] = min(cursor + count, len(messages))
        return out

    async def claim_stale(
        self, event_types: Iterable[str], group: str, consumer: str, min_idle_ms: int, count: int
    ) -> tuple[list[DeliveredEvent], list[DeliveredEvent]]:
        now = time.monotonic()
        retryable: list[DeliveredEvent] = []
        for event_type in event_types:
            key = (stream_for(event_type), group)
            for msg_id, (envelope, delivered, ts) in list(self._pending.get(key, {}).items()):
                if (now - ts) * 1000 >= min_idle_ms:
                    self._pending[key][msg_id] = (envelope, delivered + 1, now)
                    retryable.append(
                        DeliveredEvent(key[0], msg_id, envelope, delivered + 1)
                    )
                    if len(retryable) >= count:
                        break
        return retryable, []

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        self._pending.get((stream, group), {}).pop(message_id, None)

    async def dead_letter(self, group: str, item: DeadLetter) -> None:
        self.dead_letters.append(item)
        DEAD_LETTERS.labels(event_type=item.envelope.event_type).inc()


class ConsumerRuntime:
    """Transport-level consumer loop.

    Business concerns (deduplication, DB transactions) live behind ``dispatch``;
    this class only owns *delivery*: read, retry stale, ack, dead-letter.

    ``dispatch`` must be idempotent and is expected to persist the fact that it
    handled ``event_id`` in the *same* database transaction as its side effects.
    """

    def __init__(
        self,
        *,
        bus: EventBus,
        group: str,
        consumer_name: str,
        event_types: Iterable[str],
        dispatch: Callable[[EventEnvelope], Awaitable[str]],
        on_dead_letter: Callable[[DeadLetter], Awaitable[None]] | None = None,
        max_delivery_count: int = 5,
        claim_idle_ms: int = 30_000,
        batch_size: int = 32,
        block_ms: int = 2_000,
    ) -> None:
        self._bus = bus
        self._group = group
        self._consumer = consumer_name
        self._event_types = list(event_types)
        self._dispatch = dispatch
        self._on_dead_letter = on_dead_letter
        self._max_delivery = max_delivery_count
        self._claim_idle_ms = claim_idle_ms
        self._batch = batch_size
        self._block_ms = block_ms
        self._started = False

    async def start(self) -> None:
        if not self._started:
            await self._bus.ensure_group(self._event_types, self._group)
            self._started = True

    async def run_once(self, *, block_ms: int | None = None) -> int:
        """Process one batch. Returns the number of messages handled.

        Tests call this to drain the bus deterministically instead of sleeping.
        """
        await self.start()
        stale, _ = await self._bus.claim_stale(
            self._event_types, self._group, self._consumer, self._claim_idle_ms, self._batch
        )
        fresh = await self._bus.read_new(
            self._event_types,
            self._group,
            self._consumer,
            self._batch,
            self._block_ms if block_ms is None else block_ms,
        )
        handled = 0
        for delivered in [*stale, *fresh]:
            await self._handle(delivered)
            handled += 1
        return handled

    async def run_forever(self, stop: asyncio.Event) -> None:
        await self.start()
        log.info("consumer.started", group=self._group, consumer=self._consumer,
                 event_types=self._event_types)
        while not stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("consumer.loop_error", group=self._group)
                await asyncio.sleep(1.0)

    async def _handle(self, delivered: DeliveredEvent) -> None:
        envelope = delivered.envelope
        if delivered.delivery_count > self._max_delivery:
            await self._reject(delivered, "max delivery count exceeded")
            return
        try:
            with EVENT_HANDLER_LATENCY.labels(event_type=envelope.event_type).time():
                outcome = await self._dispatch(envelope)
            EVENTS_CONSUMED.labels(event_type=envelope.event_type, outcome=outcome).inc()
            await self._bus.ack(delivered.stream, self._group, delivered.message_id)
        except Exception as exc:
            EVENTS_CONSUMED.labels(event_type=envelope.event_type, outcome="failed").inc()
            log.exception(
                "consumer.handler_failed",
                event_type=envelope.event_type,
                event_id=envelope.event_id,
                correlation_id=envelope.correlation_id,
                delivery_count=delivered.delivery_count,
            )
            if delivered.delivery_count >= self._max_delivery:
                await self._reject(delivered, f"{type(exc).__name__}: {exc}")
            # else: leave unacked -> redelivered after claim_idle_ms (backoff by idle time)

    async def _reject(self, delivered: DeliveredEvent, error: str) -> None:
        item = DeadLetter(delivered.envelope, delivered.delivery_count, error)
        await self._bus.dead_letter(self._group, item)
        if self._on_dead_letter is not None:
            await self._on_dead_letter(item)
        await self._bus.ack(delivered.stream, self._group, delivered.message_id)
        EVENTS_CONSUMED.labels(
            event_type=delivered.envelope.event_type, outcome="dead_lettered"
        ).inc()
        log.error(
            "consumer.dead_lettered",
            event_type=delivered.envelope.event_type,
            event_id=delivered.envelope.event_id,
            correlation_id=delivered.envelope.correlation_id,
            error=error,
        )
