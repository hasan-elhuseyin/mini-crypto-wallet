"""Task 6: concurrent transfers must never overdraw an account.

The scenario from the brief: User A has 300 USDT available and two 200 USDT
transfers arrive at the same time. Exactly one may succeed.
"""

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

pytestmark = pytest.mark.integration


def _body(sender, recipient, amount, key):
    return {
        "from_user_id": sender["id"], "to_user_id": recipient["id"], "asset": "USDT",
        "amount": amount, "idempotency_key": key,
    }


async def test_two_concurrent_transfers_cannot_overdraw(
    wallet_client, two_users, deposit
):
    user_a, user_b = two_users
    await deposit(user_a["wallet"]["address"], "300.000000")

    first, second = await asyncio.gather(
        wallet_client.post("/transfers", json=_body(user_a, user_b, "200.000000", "conc-1")),
        wallet_client.post("/transfers", json=_body(user_a, user_b, "200.000000", "conc-2")),
    )

    statuses = sorted([first.status_code, second.status_code])
    assert statuses == [202, 409], f"{first.text} / {second.text}"
    rejected = first if first.status_code == 409 else second
    assert rejected.json()["code"] == "INSUFFICIENT_FUNDS"

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["reserved"] == "200.000000"
    assert balance["available"] == "100.000000"
    assert balance["posted"] == "300.000000"


async def test_many_concurrent_transfers_reserve_exactly_the_balance(
    wallet_client, two_users, deposit
):
    """Ten simultaneous 100 USDT transfers against a 300 USDT balance."""
    user_a, user_b = two_users
    await deposit(user_a["wallet"]["address"], "300.000000")

    responses = await asyncio.gather(
        *(
            wallet_client.post(
                "/transfers", json=_body(user_a, user_b, "100.000000", f"burst-{n}")
            )
            for n in range(10)
        )
    )
    accepted = [r for r in responses if r.status_code == 202]
    rejected = [r for r in responses if r.status_code == 409]
    assert len(accepted) == 3, [r.status_code for r in responses]
    assert len(rejected) == 7
    assert all(r.json()["code"] == "INSUFFICIENT_FUNDS" for r in rejected)

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["available"] == "0.000000"
    assert balance["reserved"] == "300.000000"


async def test_concurrent_transfers_settle_correctly(
    wallet_client, pipeline, two_users, deposit
):
    """Both accepted transfers must settle to the right final balances."""
    user_a, user_b = two_users
    await deposit(user_a["wallet"]["address"], "300.000000")

    responses = await asyncio.gather(
        wallet_client.post("/transfers", json=_body(user_a, user_b, "100.000000", "settle-1")),
        wallet_client.post("/transfers", json=_body(user_a, user_b, "150.000000", "settle-2")),
    )
    assert all(r.status_code == 202 for r in responses)

    await pipeline.settle(rounds=14)

    balance_a = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    balance_b = (await wallet_client.get(f"/users/{user_b['id']}/balance")).json()
    assert balance_a["posted"] == "50.000000"
    assert balance_a["reserved"] == "0.000000"
    assert balance_b["posted"] == "250.000000"

    report = (await wallet_client.get("/admin/reconciliation")).json()
    assert report["inconsistent"] == 0


async def test_the_database_itself_refuses_a_negative_balance(
    pipeline, two_users, deposit
):
    """Proof that the guarantee is not only application-level.

    This bypasses every service and writes straight to the table; the CHECK
    constraint still refuses to let the account go negative.
    """
    user_a = two_users[0]
    await deposit(user_a["wallet"]["address"], "100.000000")

    async with pipeline.wallet_ctx.db.sessionmaker() as session:
        with pytest.raises(IntegrityError, match="ck_balance_available_non_negative"):
            async with session.begin():
                await session.execute(
                    text(
                        "UPDATE balances SET reserved = reserved + 100000001 "
                        "WHERE user_id = :uid AND asset = 'USDT'"
                    ),
                    {"uid": user_a["id"]},
                )


async def test_the_ledger_is_append_only(pipeline, two_users, deposit):
    """Financial history is corrected with new entries, never edited."""
    user_a = two_users[0]
    await deposit(user_a["wallet"]["address"], "100.000000")

    async with pipeline.wallet_ctx.db.sessionmaker() as session:
        with pytest.raises(Exception, match="append-only"):
            async with session.begin():
                await session.execute(text("UPDATE ledger_entries SET amount = 1"))

    async with pipeline.wallet_ctx.db.sessionmaker() as session:
        with pytest.raises(Exception, match="append-only"):
            async with session.begin():
                await session.execute(text("DELETE FROM ledger_entries"))
