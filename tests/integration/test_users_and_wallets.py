"""Task 1: users, wallets, and one address per user."""

import pytest

pytestmark = pytest.mark.integration


async def test_create_user(wallet_client):
    response = await wallet_client.post(
        "/users", json={"name": "User A", "email": "user-a@example.com"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["id"] >= 1
    assert body["email"] == "user-a@example.com"
    assert body["status"] == "ACTIVE"


async def test_duplicate_email_is_rejected(wallet_client):
    payload = {"name": "User A", "email": "dupe@example.com"}
    assert (await wallet_client.post("/users", json=payload)).status_code == 201
    conflict = await wallet_client.post("/users", json=payload)
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "CONFLICT"


async def test_invalid_email_is_a_422(wallet_client):
    response = await wallet_client.post("/users", json={"name": "x", "email": "not-an-email"})
    assert response.status_code == 422
    assert response.json()["code"] == "VALIDATION_ERROR"


async def test_requests_without_an_api_key_are_rejected(wallet_app):
    import httpx

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wallet_app), base_url="http://wallet.test"
    ) as anonymous:
        response = await anonymous.post("/users", json={"name": "x", "email": "x@y.com"})
    assert response.status_code == 401
    assert response.json()["code"] == "UNAUTHORIZED"


async def test_each_user_gets_a_distinct_address(two_users):
    a, b = two_users
    assert a["wallet"]["address"] != b["wallet"]["address"]
    for user in two_users:
        wallet = user["wallet"]
        assert wallet["address"].startswith("0x")
        assert len(wallet["address"]) == 42
        assert wallet["network"] == "BSC"
        assert wallet["asset"] == "USDT"
        assert wallet["status"] == "ACTIVE"
        assert wallet["created_at"]


async def test_wallet_creation_is_idempotent(wallet_client, two_users):
    user = two_users[0]
    again = await wallet_client.post(f"/users/{user['id']}/wallet", json={})
    assert again.status_code == 200  # 200, not 201: nothing new was created
    assert again.json()["address"] == user["wallet"]["address"]


async def test_get_wallet(wallet_client, two_users):
    user = two_users[0]
    response = await wallet_client.get(f"/users/{user['id']}/wallet")
    assert response.status_code == 200
    assert response.json()["address"] == user["wallet"]["address"]


async def test_wallet_for_unknown_user_is_404(wallet_client):
    assert (await wallet_client.get("/users/999999/wallet")).status_code == 404


async def test_private_keys_are_never_exposed_over_the_api(
    wallet_client, blockchain_client, two_users
):
    """The custody key must not appear in any API response."""
    user = two_users[0]
    address = user["wallet"]["address"]

    wallet_body = (await wallet_client.get(f"/users/{user['id']}/wallet")).text
    chain_body = (
        await blockchain_client.get(f"/addresses/user:{user['id']}")
    ).text
    for body in (wallet_body, chain_body):
        lowered = body.lower()
        assert "private" not in lowered
        assert "secret" not in lowered
        assert "key_material" not in lowered
    assert address in chain_body


async def test_starting_balance_is_zero(wallet_client, two_users):
    response = await wallet_client.get(f"/users/{two_users[0]['id']}/balance")
    assert response.status_code == 200
    body = response.json()
    assert body["posted"] == "0.000000"
    assert body["reserved"] == "0.000000"
    assert body["available"] == "0.000000"
