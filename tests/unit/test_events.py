"""Event envelope and deterministic event identity."""

import pytest
from mcw_common.events import EventEnvelope, EventType, new_event, stream_for

pytestmark = pytest.mark.unit


def test_dedupe_key_produces_a_stable_event_id():
    """Re-emitting the same fact must produce the same event id.

    This is what lets a producer restart mid-flight without a consumer
    double-counting the event.
    """
    first = new_event(
        EventType.DEPOSIT_CONFIRMED, {"amount_units": "1000000000"},
        producer="blockchain-service", dedupe_key="BSC:0xabc:2",
    )
    second = new_event(
        EventType.DEPOSIT_CONFIRMED, {"amount_units": "1000000000", "extra": "ignored"},
        producer="blockchain-service", dedupe_key="BSC:0xabc:2",
    )
    assert first.event_id == second.event_id


def test_different_dedupe_keys_produce_different_ids():
    a = new_event(EventType.DEPOSIT_CONFIRMED, {}, producer="x", dedupe_key="BSC:0xabc:1")
    b = new_event(EventType.DEPOSIT_CONFIRMED, {}, producer="x", dedupe_key="BSC:0xabc:2")
    assert a.event_id != b.event_id


def test_event_type_is_part_of_the_identity():
    a = new_event(EventType.DEPOSIT_DETECTED, {}, producer="x", dedupe_key="same")
    b = new_event(EventType.DEPOSIT_CONFIRMED, {}, producer="x", dedupe_key="same")
    assert a.event_id != b.event_id


def test_without_a_dedupe_key_ids_are_unique():
    a = new_event(EventType.TRANSFER_REQUESTED, {}, producer="x")
    b = new_event(EventType.TRANSFER_REQUESTED, {}, producer="x")
    assert a.event_id != b.event_id


def test_wire_round_trip_preserves_everything():
    original = new_event(
        EventType.TX_CONFIRMED,
        {"transfer_id": "t-1", "amount_units": "250000000"},
        producer="blockchain-service",
        correlation_id="cid-123",
        causation_id="evt-0",
        dedupe_key="t-1:0xdead:confirmed",
    )
    restored = EventEnvelope.from_wire(original.to_wire())
    assert restored.event_id == original.event_id
    assert restored.correlation_id == "cid-123"
    assert restored.causation_id == "evt-0"
    assert restored.payload == original.payload


def test_amounts_travel_as_strings():
    """A JSON number would be parsed as a float by most clients."""
    event = new_event(
        EventType.DEPOSIT_CONFIRMED, {"amount_units": str(1_000_000_000)},
        producer="x", dedupe_key="k",
    )
    assert isinstance(event.payload["amount_units"], str)


def test_stream_per_event_type():
    assert stream_for("deposit.confirmed") == "mcw:events:deposit.confirmed"
    assert stream_for("transfer.requested") != stream_for("deposit.confirmed")
