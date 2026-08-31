"""Consumer delivery semantics: ack, retry, dead-letter.

Uses the in-memory bus so the retry/DLQ policy can be asserted without Redis.
"""

import pytest
from mcw_common.bus import ConsumerRuntime, InMemoryBus
from mcw_common.events import EventType, new_event

pytestmark = pytest.mark.unit


def _event(key: str = "k"):
    return new_event(EventType.DEPOSIT_CONFIRMED, {"k": key}, producer="test", dedupe_key=key)


async def test_successful_event_is_acked_once():
    bus = InMemoryBus()
    seen = []

    async def dispatch(envelope):
        seen.append(envelope.event_id)
        return "processed"

    runtime = ConsumerRuntime(
        bus=bus, group="g", consumer_name="c", event_types=[EventType.DEPOSIT_CONFIRMED],
        dispatch=dispatch, claim_idle_ms=0, block_ms=0,
    )
    await bus.publish(_event())
    assert await runtime.run_once() == 1
    # Acked: a second poll finds nothing, not even as a stale claim.
    assert await runtime.run_once() == 0
    assert len(seen) == 1


async def test_failing_handler_is_retried_then_dead_lettered():
    bus = InMemoryBus()
    attempts = []

    async def dispatch(envelope):
        attempts.append(envelope.event_id)
        raise RuntimeError("handler exploded")

    runtime = ConsumerRuntime(
        bus=bus, group="g", consumer_name="c", event_types=[EventType.DEPOSIT_CONFIRMED],
        dispatch=dispatch, max_delivery_count=3, claim_idle_ms=0, block_ms=0,
    )
    await bus.publish(_event())

    for _ in range(6):
        await runtime.run_once()
        if bus.dead_letters:
            break

    assert bus.dead_letters, "a poison message must end up in the dead letter store"
    assert bus.dead_letters[0].delivery_count >= 3
    assert "handler exploded" in bus.dead_letters[0].error
    # And it stops being redelivered once dead-lettered.
    before = len(attempts)
    await runtime.run_once()
    assert len(attempts) == before


async def test_unacked_message_is_reclaimed_after_a_crash():
    """A consumer that dies mid-processing must not lose the message."""
    bus = InMemoryBus()
    calls = {"n": 0}

    async def flaky(envelope):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("process died")
        return "processed"

    runtime = ConsumerRuntime(
        bus=bus, group="g", consumer_name="c", event_types=[EventType.DEPOSIT_CONFIRMED],
        dispatch=flaky, max_delivery_count=5, claim_idle_ms=0, block_ms=0,
    )
    await bus.publish(_event())
    await runtime.run_once()   # fails, stays pending
    await runtime.run_once()   # reclaimed and handled
    assert calls["n"] == 2
    assert not bus.dead_letters
