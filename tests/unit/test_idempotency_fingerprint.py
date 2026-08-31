import pytest
from wallet_service.services.idempotency import fingerprint

pytestmark = pytest.mark.unit


def test_key_order_does_not_change_the_fingerprint():
    a = fingerprint({"from_user_id": 1, "to_user_id": 2, "amount_units": "250000000"})
    b = fingerprint({"amount_units": "250000000", "to_user_id": 2, "from_user_id": 1})
    assert a == b


def test_a_different_amount_changes_the_fingerprint():
    a = fingerprint({"from_user_id": 1, "amount_units": "250000000"})
    b = fingerprint({"from_user_id": 1, "amount_units": "250000001"})
    assert a != b


def test_a_different_recipient_changes_the_fingerprint():
    assert fingerprint({"to_user_id": 2}) != fingerprint({"to_user_id": 3})
