"""Task 4 + 5: the end-to-end scenario.

User A deposits 1000 USDT, sends 250 to User B, and the platform ends up with
A = 750 and B = 250 -- with the transfer settled on chain and both sides of the
movement recorded in the ledger.
"""

import pytest

pytestmark = pytest.mark.integration


async def _transfer(client, sender, recipient, amount="250.000000", key="transfer-001"):
    return await client.post(
        "/transfers",
        json={
            "from_user_id": sender["id"],
            "to_user_id": recipient["id"],
            "asset": "USDT",
            "amount": amount,
            "idempotency_key": key,
        },
    )


async def test_the_case_scenario_end_to_end(wallet_client, pipeline, funded_users):
    user_a, user_b = funded_users

    created = await _transfer(wallet_client, user_a, user_b)
    assert created.status_code == 202, created.text
    transfer = created.json()
    assert transfer["status"] == "CREATED"
    assert transfer["amount"] == "250.000000"
    transfer_id = transfer["id"]

    # Funds are held immediately, but nothing is posted yet.
    balance_a = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance_a["posted"] == "1000.000000"
    assert balance_a["reserved"] == "250.000000"
    assert balance_a["available"] == "750.000000"

    await pipeline.settle(rounds=12)

    settled = (await wallet_client.get(f"/transfers/{transfer_id}")).json()
    assert settled["status"] == "CONFIRMED", settled
    assert settled["tx_hash"].startswith("0x")
    assert settled["settled_at"]

    balance_a = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    balance_b = (await wallet_client.get(f"/users/{user_b['id']}/balance")).json()
    assert balance_a["posted"] == "750.000000"
    assert balance_a["reserved"] == "0.000000"
    assert balance_a["available"] == "750.000000"
    assert balance_b["posted"] == "250.000000"
    assert balance_b["available"] == "250.000000"


async def test_transfer_moves_through_its_states(wallet_client, pipeline, funded_users):
    user_a, user_b = funded_users
    transfer_id = (await _transfer(wallet_client, user_a, user_b)).json()["id"]

    seen = {"CREATED"}
    for _ in range(14):
        await pipeline.pump(1)
        seen.add((await wallet_client.get(f"/transfers/{transfer_id}")).json()["status"])
    assert "BROADCASTED" in seen
    assert "CONFIRMED" in seen


async def test_both_sides_of_the_movement_are_in_the_ledger(
    wallet_client, pipeline, funded_users
):
    user_a, user_b = funded_users
    transfer_id = (await _transfer(wallet_client, user_a, user_b)).json()["id"]
    await pipeline.settle(rounds=12)

    history_a = (await wallet_client.get(f"/users/{user_a['id']}/transactions")).json()
    types_a = [item["entry_type"] for item in history_a["items"]]
    assert types_a == ["TRANSFER_DEBIT", "DEPOSIT"]  # newest first
    debit = history_a["items"][0]
    assert debit["amount"] == "-250.000000"
    assert debit["reference_id"] == transfer_id
    assert not history_a["pending_transfers"]

    history_b = (await wallet_client.get(f"/users/{user_b['id']}/transactions")).json()
    assert [item["entry_type"] for item in history_b["items"]] == ["TRANSFER_CREDIT"]
    assert history_b["items"][0]["amount"] == "250.000000"


async def test_in_flight_transfers_are_visible_in_history(
    wallet_client, funded_users
):
    """A held transfer has no ledger entries yet -- it must still be visible."""
    user_a, user_b = funded_users
    await _transfer(wallet_client, user_a, user_b)
    history = (await wallet_client.get(f"/users/{user_a['id']}/transactions")).json()
    assert len(history["pending_transfers"]) == 1
    assert history["pending_transfers"][0]["status"] in ("CREATED", "PROCESSING")


async def test_the_chain_saw_the_same_transfer(
    wallet_client, blockchain_client, pipeline, funded_users
):
    user_a, user_b = funded_users
    transfer_id = (await _transfer(wallet_client, user_a, user_b)).json()["id"]
    await pipeline.settle(rounds=12)

    on_chain = (await blockchain_client.get(f"/transactions/{transfer_id}")).json()
    assert on_chain["status"] == "CONFIRMED"
    assert on_chain["amount"] == "250.000000"
    assert on_chain["from_address"] == user_a["wallet"]["address"]
    assert on_chain["to_address"] == user_b["wallet"]["address"]
    assert on_chain["confirmations"] >= 3

    # And the token really moved on the simulated chain.
    balance = (
        await blockchain_client.get(
            f"/addresses/{user_b['wallet']['address']}/onchain-balance"
        )
    ).json()
    assert balance["amount"] == "250.000000"


async def test_snapshots_agree_with_the_ledger(wallet_client, pipeline, funded_users):
    user_a, user_b = funded_users
    await _transfer(wallet_client, user_a, user_b)
    await pipeline.settle(rounds=12)

    report = (await wallet_client.get("/admin/reconciliation")).json()
    assert report["inconsistent"] == 0
    assert report["checked"] >= 2


async def test_a_correlation_id_survives_the_whole_flow(
    wallet_client, pipeline, funded_users
):
    user_a, user_b = funded_users
    response = await wallet_client.post(
        "/transfers",
        json={
            "from_user_id": user_a["id"], "to_user_id": user_b["id"], "asset": "USDT",
            "amount": "250.000000", "idempotency_key": "corr-001",
        },
        headers={"X-Correlation-ID": "tx-238791"},
    )
    assert response.headers["X-Correlation-ID"] == "tx-238791"
    assert response.json()["correlation_id"] == "tx-238791"

    await pipeline.settle(rounds=12)

    from sqlalchemy import text

    async with pipeline.blockchain_ctx.db.sessionmaker() as session:
        correlation_ids = list(
            (
                await session.execute(
                    text("SELECT DISTINCT correlation_id FROM outgoing_transactions")
                )
            ).scalars()
        )
    assert correlation_ids == ["tx-238791"], "the id must cross the service boundary"

    async with pipeline.wallet_ctx.db.sessionmaker() as session:
        ledger_ids = list(
            (
                await session.execute(
                    text(
                        "SELECT DISTINCT correlation_id FROM ledger_entries "
                        "WHERE entry_type LIKE 'TRANSFER%'"
                    )
                )
            ).scalars()
        )
    assert ledger_ids == ["tx-238791"]
