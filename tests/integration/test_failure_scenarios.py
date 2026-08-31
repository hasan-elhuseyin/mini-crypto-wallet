"""Task 8: the failure paths.

Covers all six scenarios from the brief:

1. the RPC node is unreachable;
2. the on-chain transaction reverts;
3. a transaction stays pending forever;
4. a confirmed deposit disappears in a reorg;
5. a consumer receives the same event twice;
6. a consumer crashes half way through an event.

Plus the one the brief calls out as bonus: the database commits but the event
cannot be published.
"""

import asyncio

import pytest
from mcw_common.events import EventType, new_event
from sqlalchemy import text

pytestmark = pytest.mark.integration


def _body(sender, recipient, amount="250.000000", key="fail-001"):
    return {
        "from_user_id": sender["id"], "to_user_id": recipient["id"], "asset": "USDT",
        "amount": amount, "idempotency_key": key,
    }


async def _nudge_retries(ctx) -> None:
    """Skip the broadcaster's backoff so the test does not have to sleep."""
    async with ctx.db.sessionmaker() as session, session.begin():
        await session.execute(
            text("UPDATE outgoing_transactions SET next_attempt_at = now() - interval '1 s'")
        )


# --- 1. RPC unreachable ----------------------------------------------------


async def test_transfer_survives_an_unreachable_rpc_and_recovers(
    wallet_client, blockchain_client, pipeline, funded_users
):
    user_a, user_b = funded_users
    await blockchain_client.post("/simulate/faults", json={"rpc_available": False})

    created = await wallet_client.post("/transfers", json=_body(user_a, user_b))
    assert created.status_code == 202, "the API must still accept the request"
    transfer_id = created.json()["id"]

    for _ in range(3):
        await pipeline.pump(1)
        await _nudge_retries(pipeline.blockchain_ctx)

    transfer = (await wallet_client.get(f"/transfers/{transfer_id}")).json()
    assert transfer["status"] in ("CREATED", "PROCESSING"), transfer
    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["reserved"] == "250.000000", "funds stay held while we retry"

    async with pipeline.blockchain_ctx.db.sessionmaker() as session:
        row = (
            await session.execute(
                text("SELECT status, attempts, failure_reason FROM outgoing_transactions")
            )
        ).mappings().one()
    assert row["status"] == "PENDING"
    assert row["attempts"] >= 1
    assert "unreachable" in (row["failure_reason"] or "")

    # The node comes back: no operator action required.
    await blockchain_client.post("/simulate/faults", json={"rpc_available": True})
    await _nudge_retries(pipeline.blockchain_ctx)
    await pipeline.settle(rounds=14)

    assert (await wallet_client.get(f"/transfers/{transfer_id}")).json()["status"] == "CONFIRMED"
    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["posted"] == "750.000000"


async def test_the_retry_budget_is_finite_and_releases_the_hold(
    wallet_client, blockchain_client, pipeline, funded_users
):
    """After BROADCAST_MAX_ATTEMPTS the transfer fails cleanly -- no stuck money."""
    user_a, user_b = funded_users
    # Assert on the budget this test controls, not on ambient configuration.
    pipeline.blockchain_ctx.settings.broadcast_max_attempts = 3

    await blockchain_client.post("/simulate/faults", json={"rpc_available": False})
    transfer_id = (
        await wallet_client.post("/transfers", json=_body(user_a, user_b))
    ).json()["id"]

    for _ in range(8):
        await pipeline.pump(1)
        await _nudge_retries(pipeline.blockchain_ctx)

    transfer = (await wallet_client.get(f"/transfers/{transfer_id}")).json()
    assert transfer["status"] == "FAILED"
    assert transfer["failure_code"] == "RPC_UNAVAILABLE"

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["reserved"] == "0.000000", "the hold must be released"
    assert balance["available"] == "1000.000000", "and the money must still be there"

    history = (await wallet_client.get(f"/users/{user_a['id']}/transactions")).json()
    assert [i["entry_type"] for i in history["items"]] == ["DEPOSIT"], (
        "a failed transfer must leave no postings behind"
    )


# --- 2. transaction reverted on chain --------------------------------------


async def test_a_reverted_transaction_releases_the_hold(
    wallet_client, blockchain_client, pipeline, funded_users
):
    user_a, user_b = funded_users
    await blockchain_client.post("/simulate/faults", json={"fail_next_transfers": 1})

    transfer_id = (
        await wallet_client.post("/transfers", json=_body(user_a, user_b))
    ).json()["id"]
    await pipeline.settle(rounds=12)

    transfer = (await wallet_client.get(f"/transfers/{transfer_id}")).json()
    assert transfer["status"] == "FAILED"
    assert transfer["failure_code"] == "REVERTED_ON_CHAIN"
    assert transfer["tx_hash"], "we still know which transaction reverted"

    balance_a = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    balance_b = (await wallet_client.get(f"/users/{user_b['id']}/balance")).json()
    assert balance_a["available"] == "1000.000000"
    assert balance_a["reserved"] == "0.000000"
    assert balance_b["posted"] == "0.000000"

    report = (await wallet_client.get("/admin/reconciliation")).json()
    assert report["inconsistent"] == 0


# --- 3. stuck pending ------------------------------------------------------


async def test_a_transaction_stuck_pending_is_retried_then_failed(
    wallet_client, blockchain_client, pipeline, funded_users
):
    """Blocks keep coming but nothing gets mined: the classic stuck transaction."""
    user_a, user_b = funded_users
    settings = pipeline.blockchain_ctx.settings
    settings.pending_timeout_seconds = 1
    settings.broadcast_max_attempts = 3

    await blockchain_client.post("/simulate/faults", json={"halt_mining": True})

    transfer_id = (
        await wallet_client.post("/transfers", json=_body(user_a, user_b))
    ).json()["id"]
    await pipeline.pump(3)

    async with pipeline.blockchain_ctx.db.sessionmaker() as session:
        status = (
            await session.execute(text("SELECT status FROM outgoing_transactions"))
        ).scalar_one()
    assert status == "BROADCASTED", "it was accepted by the node, just never mined"

    # pending_timeout_seconds is pinned to 1 above.
    for _ in range(6):
        await asyncio.sleep(1.1)
        await _nudge_retries(pipeline.blockchain_ctx)
        await pipeline.pump(2)
        transfer = (await wallet_client.get(f"/transfers/{transfer_id}")).json()
        if transfer["status"] == "FAILED":
            break

    assert transfer["status"] == "FAILED", transfer
    assert transfer["failure_code"] == "STUCK_PENDING"
    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["available"] == "1000.000000"
    assert balance["reserved"] == "0.000000"


# --- 4. reorg --------------------------------------------------------------


async def test_a_confirmed_deposit_lost_to_a_reorg_is_reversed(
    wallet_client, blockchain_client, pipeline, two_users, deposit
):
    user_a = two_users[0]
    submitted = await deposit(user_a["wallet"]["address"], "1000.000000")

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["posted"] == "1000.000000", "credited before the reorg"

    # The chain reorganises and this transaction does not make it into the new fork.
    reorg = await blockchain_client.post(
        "/simulate/reorg", json={"depth": 6, "drop_tx_hashes": [submitted["tx_hash"]]}
    )
    assert reorg.status_code == 200, reorg.text
    await pipeline.pump(4)

    deposits = (await blockchain_client.get("/deposits")).json()
    assert deposits[0]["status"] == "REORGED"

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["posted"] == "0.000000", "the credit must be taken back"

    history = (await wallet_client.get(f"/users/{user_a['id']}/transactions")).json()
    types = [item["entry_type"] for item in history["items"]]
    assert types == ["REVERSAL", "DEPOSIT"], "corrected by a new entry, not by a delete"
    assert history["items"][0]["amount"] == "-1000.000000"

    report = (await wallet_client.get("/admin/reconciliation")).json()
    assert report["inconsistent"] == 0


async def test_a_reorg_that_only_moves_a_transaction_does_not_reverse_it(
    wallet_client, blockchain_client, pipeline, two_users, deposit
):
    """A transaction re-mined into a new block is still the same deposit."""
    user_a = two_users[0]
    await deposit(user_a["wallet"]["address"], "1000.000000")

    await blockchain_client.post("/simulate/reorg", json={"depth": 6})
    await pipeline.pump(6)

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["posted"] == "1000.000000"
    history = (await wallet_client.get(f"/users/{user_a['id']}/transactions")).json()
    assert [i["entry_type"] for i in history["items"]] == ["DEPOSIT"]


# --- 5. duplicate delivery, 6. consumer crash ------------------------------


async def test_a_consumer_crash_does_not_lose_or_duplicate_the_credit(
    wallet_client, blockchain_client, pipeline, two_users
):
    original_dispatch = pipeline.wallet_consumer._dispatch
    state = {"crashed": False}

    async def crash_once(envelope):
        if envelope.event_type == EventType.DEPOSIT_CONFIRMED and not state["crashed"]:
            state["crashed"] = True
            raise RuntimeError("consumer process died mid-transaction")
        return await original_dispatch(envelope)

    pipeline.wallet_consumer._dispatch = crash_once

    user_a = two_users[0]
    await blockchain_client.post(
        "/simulate/deposits",
        json={"to_address": user_a["wallet"]["address"], "amount": "1000.000000",
              "asset": "USDT"},
    )
    await pipeline.pump(5)
    assert state["crashed"], "the failure path was not exercised"

    await asyncio.sleep(0.1)  # let the pending entry pass claim_idle_ms
    await pipeline.pump(4)

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["posted"] == "1000.000000", "credited exactly once after recovery"

    async with pipeline.wallet_ctx.db.sessionmaker() as session:
        entries = (
            await session.execute(text("SELECT count(*) FROM ledger_entries"))
        ).scalar_one()
        dead = (await session.execute(text("SELECT count(*) FROM dead_letters"))).scalar_one()
    assert entries == 1
    assert dead == 0


async def test_a_poison_event_ends_up_in_the_dead_letter_store(pipeline):
    """An event that can never be handled must not block the stream forever."""
    poison = new_event(
        EventType.DEPOSIT_CONFIRMED,
        {"network": "BSC"},  # missing every field the handler needs
        producer="test",
        dedupe_key="poison-1",
    )
    await pipeline.wallet_ctx.bus.publish(poison)

    for _ in range(6):
        await pipeline.wallet_consumer.run_once(block_ms=0)
        await asyncio.sleep(0.06)  # exceed claim_idle_ms so it is reclaimed

    async with pipeline.wallet_ctx.db.sessionmaker() as session:
        row = (
            await session.execute(
                text("SELECT event_type, delivery_count, error FROM dead_letters")
            )
        ).mappings().one()
    assert row["event_type"] == EventType.DEPOSIT_CONFIRMED
    assert row["delivery_count"] >= 3


# --- bonus: committed but not published ------------------------------------


async def test_a_publish_failure_does_not_lose_the_transfer(
    wallet_client, pipeline, funded_users
):
    """The outbox is what makes "committed but not published" recoverable."""
    user_a, user_b = funded_users
    bus = pipeline.wallet_ctx.bus
    original_publish = bus.publish

    async def broken_publish(envelope):
        raise ConnectionError("redis is down")

    bus.publish = broken_publish
    created = await wallet_client.post("/transfers", json=_body(user_a, user_b))
    assert created.status_code == 202, "the business transaction still commits"
    transfer_id = created.json()["id"]

    assert await pipeline.wallet_ctx.relay.run_once() == 0

    async with pipeline.wallet_ctx.db.sessionmaker() as session:
        row = (
            await session.execute(
                text(
                    "SELECT published_at, attempts, last_error FROM outbox "
                    "WHERE event_type = 'transfer.requested'"
                )
            )
        ).mappings().one()
    assert row["published_at"] is None
    assert row["attempts"] == 1
    assert "redis is down" in row["last_error"]

    # The broker recovers; the relay picks the event back up on its own.
    bus.publish = original_publish
    async with pipeline.wallet_ctx.db.sessionmaker() as session, session.begin():
        await session.execute(text("UPDATE outbox SET next_attempt_at = now()"))

    await pipeline.settle(rounds=14)
    assert (await wallet_client.get(f"/transfers/{transfer_id}")).json()["status"] == "CONFIRMED"
