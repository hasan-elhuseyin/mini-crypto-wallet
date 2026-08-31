"""Task 2 + 7: deposit detection, confirmations, and duplicate protection."""

import pytest
from mcw_common.events import EventType

pytestmark = pytest.mark.integration


async def _deposit(blockchain_client, address, amount="1000.000000", reference=None):
    response = await blockchain_client.post(
        "/simulate/deposits",
        json={"to_address": address, "amount": amount, "asset": "USDT",
              "reference": reference},
    )
    assert response.status_code == 202, response.text
    return response.json()


async def test_deposit_is_credited_only_after_enough_confirmations(
    wallet_client, blockchain_client, pipeline, two_users
):
    user_a = two_users[0]
    await _deposit(blockchain_client, user_a["wallet"]["address"])

    # One block: the deposit is on chain but has a single confirmation.
    await pipeline.pump(1)
    detected = await blockchain_client.get("/deposits")
    assert detected.json()[0]["status"] == "DETECTED"
    balance = await wallet_client.get(f"/users/{user_a['id']}/balance")
    assert balance.json()["posted"] == "0.000000", "money must not move on 1 confirmation"

    # Enough blocks to reach CONFIRMATIONS_REQUIRED=3.
    await pipeline.pump(5)

    deposits = (await blockchain_client.get("/deposits")).json()
    assert deposits[0]["status"] == "CONFIRMED"
    assert deposits[0]["confirmations"] >= 3
    assert deposits[0]["amount"] == "1000.000000"

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["posted"] == "1000.000000"
    assert balance["available"] == "1000.000000"
    assert balance["reserved"] == "0.000000"


async def test_deposit_writes_exactly_one_ledger_entry(
    wallet_client, blockchain_client, pipeline, two_users
):
    user_a = two_users[0]
    await _deposit(blockchain_client, user_a["wallet"]["address"])
    await pipeline.pump(6)

    history = (await wallet_client.get(f"/users/{user_a['id']}/transactions")).json()
    assert len(history["items"]) == 1
    entry = history["items"][0]
    assert entry["entry_type"] == "DEPOSIT"
    assert entry["amount"] == "1000.000000"
    assert entry["reference_type"] == "DEPOSIT"
    assert entry["correlation_id"]


async def test_replaying_the_same_event_does_not_credit_twice(
    wallet_client, blockchain_client, pipeline, two_users
):
    """Guard #1: `processed_events` stops a redelivery of the same event."""
    user_a = two_users[0]
    await _deposit(blockchain_client, user_a["wallet"]["address"])
    await pipeline.pump(6)

    envelope = await pipeline.last_event(pipeline.blockchain_ctx, EventType.DEPOSIT_CONFIRMED)
    assert envelope is not None

    assert await pipeline.wallet_dispatcher.dispatch(envelope) == "duplicate"
    assert await pipeline.wallet_dispatcher.dispatch(envelope) == "duplicate"

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["posted"] == "1000.000000"


async def test_a_new_event_for_the_same_deposit_does_not_credit_twice(
    wallet_client, blockchain_client, pipeline, two_users
):
    """Guard #2: even with a *fresh* event id, the ledger's unique key holds.

    This is the guard that survives a change of event-id strategy, a database
    restore, or a second producer emitting the same financial fact.
    """
    from mcw_common.events import new_event

    user_a = two_users[0]
    await _deposit(blockchain_client, user_a["wallet"]["address"])
    await pipeline.pump(6)

    original = await pipeline.last_event(pipeline.blockchain_ctx, EventType.DEPOSIT_CONFIRMED)
    impostor = new_event(
        EventType.DEPOSIT_CONFIRMED, dict(original.payload), producer="test"
    )  # no dedupe key -> genuinely new event id
    assert impostor.event_id != original.event_id
    assert await pipeline.wallet_dispatcher.dispatch(impostor) == "duplicate"

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["posted"] == "1000.000000"


async def test_rescanning_the_same_blocks_creates_one_deposit(
    blockchain_client, pipeline, two_users
):
    """The scanner is idempotent: rewinding its cursor cannot duplicate a deposit."""
    from sqlalchemy import text

    user_a = two_users[0]
    await _deposit(blockchain_client, user_a["wallet"]["address"])
    await pipeline.pump(6)

    async with pipeline.blockchain_ctx.db.sessionmaker() as session, session.begin():
        await session.execute(text("UPDATE scan_state SET last_scanned_block = 0"))
    await pipeline.pump(2)

    deposits = (await blockchain_client.get("/deposits")).json()
    assert len(deposits) == 1


async def test_two_distinct_deposits_are_both_credited(
    wallet_client, blockchain_client, pipeline, two_users
):
    user_a = two_users[0]
    await _deposit(blockchain_client, user_a["wallet"]["address"], "1000.000000", "dep-1")
    await _deposit(blockchain_client, user_a["wallet"]["address"], "500.000000", "dep-2")
    await pipeline.pump(6)

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["posted"] == "1500.000000"


async def test_resubmitting_the_same_chain_reference_is_one_deposit(
    wallet_client, blockchain_client, pipeline, two_users
):
    """Same client reference -> same on-chain transaction -> one deposit."""
    user_a = two_users[0]
    first = await _deposit(blockchain_client, user_a["wallet"]["address"], "1000.000000", "dep-x")
    second = await _deposit(blockchain_client, user_a["wallet"]["address"], "1000.000000", "dep-x")
    assert first["tx_hash"] == second["tx_hash"]

    await pipeline.pump(6)
    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["posted"] == "1000.000000"


async def test_an_internal_transfer_is_not_also_a_deposit(
    wallet_client, blockchain_client, pipeline, two_users
):
    """The recipient's leg of a user-to-user transfer must not be credited twice.

    Settling A -> B on chain produces a Transfer log addressed to B, which looks
    exactly like an inbound deposit. Treating it as one would pay B twice: once
    through the transfer ledger postings and once as a deposit.
    """
    user_a, user_b = two_users
    await _deposit(blockchain_client, user_a["wallet"]["address"], "1000.000000")
    await pipeline.pump(6)

    await wallet_client.post(
        "/transfers",
        json={"from_user_id": user_a["id"], "to_user_id": user_b["id"], "asset": "USDT",
              "amount": "250.000000", "idempotency_key": "internal-1"},
    )
    await pipeline.settle(rounds=14)

    deposits = (await blockchain_client.get("/deposits")).json()
    assert len(deposits) == 1, "only the external deposit is a deposit"
    assert deposits[0]["to_address"] == user_a["wallet"]["address"]

    balance_b = (await wallet_client.get(f"/users/{user_b['id']}/balance")).json()
    assert balance_b["posted"] == "250.000000"
    history_b = (await wallet_client.get(f"/users/{user_b['id']}/transactions")).json()
    assert [i["entry_type"] for i in history_b["items"]] == ["TRANSFER_CREDIT"]
