"""Tasks 4 + 7 + 9: validation, insufficient funds, and API idempotency."""

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.integration


def _body(sender, recipient, amount="250.000000", key="transfer-001", **extra):
    return {
        "from_user_id": sender["id"], "to_user_id": recipient["id"], "asset": "USDT",
        "amount": amount, "idempotency_key": key, **extra,
    }


async def test_the_same_key_three_times_creates_one_transfer(
    wallet_client, pipeline, funded_users
):
    user_a, user_b = funded_users
    body = _body(user_a, user_b)

    first = await wallet_client.post("/transfers", json=body)
    second = await wallet_client.post("/transfers", json=body)
    third = await wallet_client.post("/transfers", json=body)

    assert first.status_code == 202
    assert second.status_code == 202
    assert third.status_code == 202
    assert first.json()["id"] == second.json()["id"] == third.json()["id"]
    assert "Idempotent-Replay" not in first.headers
    assert second.headers["Idempotent-Replay"] == "true"

    async with pipeline.wallet_ctx.db.sessionmaker() as session:
        count = (await session.execute(text("SELECT count(*) FROM transfers"))).scalar_one()
    assert count == 1

    # And only one lot of funds was held.
    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["reserved"] == "250.000000"


async def test_reusing_a_key_with_a_different_body_is_rejected(wallet_client, funded_users):
    user_a, user_b = funded_users
    assert (
        await wallet_client.post("/transfers", json=_body(user_a, user_b, "250.000000"))
    ).status_code == 202
    clash = await wallet_client.post(
        "/transfers", json=_body(user_a, user_b, "100.000000")
    )
    assert clash.status_code == 409
    assert clash.json()["code"] == "IDEMPOTENCY_KEY_REUSED"


async def test_insufficient_funds_is_a_409_and_holds_nothing(wallet_client, funded_users):
    user_a, user_b = funded_users
    response = await wallet_client.post(
        "/transfers", json=_body(user_a, user_b, "5000.000000", key="too-big")
    )
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "INSUFFICIENT_FUNDS"
    assert body["errors"][0]["available"] == "1000.000000"
    assert body["correlation_id"]

    balance = (await wallet_client.get(f"/users/{user_a['id']}/balance")).json()
    assert balance["reserved"] == "0.000000"


async def test_a_rejected_request_releases_its_idempotency_key(
    wallet_client, pipeline, funded_users
):
    """A 409 must not burn the key: the client can retry once funded."""
    user_a, user_b = funded_users
    key = "retry-after-funding"
    assert (
        await wallet_client.post("/transfers", json=_body(user_a, user_b, "5000.000000", key))
    ).status_code == 409

    retry = await wallet_client.post("/transfers", json=_body(user_a, user_b, "250.000000", key))
    assert retry.status_code == 202


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ({"amount": "0"}, "zero"),
        ({"amount": "-5"}, "negative"),
        ({"amount": "0.0000001"}, "too many decimals for USDT"),
        ({"amount": "1e3"}, "scientific notation"),
        ({"amount": "abc"}, "not a number"),
        ({"asset": "DOGE"}, "unsupported asset"),
        ({"idempotency_key": "x"}, "key too short"),
    ],
)
async def test_bad_requests_are_422(wallet_client, funded_users, payload, reason):
    user_a, user_b = funded_users
    response = await wallet_client.post(
        "/transfers", json={**_body(user_a, user_b), **payload}
    )
    assert response.status_code == 422, f"{reason}: {response.text}"
    assert response.json()["code"] == "VALIDATION_ERROR"


async def test_sending_to_yourself_is_rejected(wallet_client, funded_users):
    user_a = funded_users[0]
    response = await wallet_client.post("/transfers", json=_body(user_a, user_a))
    assert response.status_code == 422


async def test_unknown_user_is_404(wallet_client, funded_users):
    user_a = funded_users[0]
    response = await wallet_client.post(
        "/transfers",
        json={"from_user_id": user_a["id"], "to_user_id": 999999, "asset": "USDT",
              "amount": "1.000000", "idempotency_key": "ghost"},
    )
    assert response.status_code == 404


async def test_transfer_lookup_of_an_unknown_id_is_404(wallet_client):
    import uuid

    assert (await wallet_client.get(f"/transfers/{uuid.uuid4()}")).status_code == 404
    assert (await wallet_client.get("/transfers/not-a-uuid")).status_code == 422
