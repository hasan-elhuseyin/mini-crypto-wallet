"""Private keys and secrets must be impossible to log."""

import pytest
from blockchain_service.chain.keys import Keystore
from mcw_common.logging import redact

pytestmark = pytest.mark.unit

DEV_KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="


def test_named_secret_fields_are_redacted():
    payload = {
        "address": "0xabc",
        "private_key": "0x" + "de" * 32,
        "nested": {"api_key": "sk-live-123", "password": "hunter2"},
    }
    scrubbed = redact(payload)
    assert scrubbed["address"] == "0xabc"
    assert scrubbed["private_key"] == "***REDACTED***"
    assert scrubbed["nested"]["api_key"] == "***REDACTED***"
    assert scrubbed["nested"]["password"] == "***REDACTED***"


def test_raw_key_material_in_free_text_is_redacted():
    message = "failed to sign with 0x" + "ab" * 32
    assert "ab" * 32 not in redact({"error": message})["error"]


def test_transaction_hashes_stay_readable():
    """Redaction must not destroy the identifiers we debug with."""
    tx_hash = "0x" + "1f" * 32
    assert redact({"tx_hash": tx_hash})["tx_hash"] == tx_hash


def test_generated_key_never_renders_its_secret():
    generated = Keystore(DEV_KEY).create_account()
    assert generated.private_key not in repr(generated)
    assert generated.private_key not in str(generated)
    assert "REDACTED" in repr(generated)


def test_keystore_round_trip_and_address_derivation():
    keystore = Keystore(DEV_KEY)
    generated = keystore.create_account()
    blob = keystore.encrypt(generated.private_key)
    assert generated.private_key.encode() not in blob  # actually encrypted
    assert keystore.decrypt(blob) == generated.private_key
    assert Keystore.address_from_private_key(generated.private_key) == generated.address


def test_two_accounts_get_different_addresses():
    keystore = Keystore(DEV_KEY)
    assert keystore.create_account().address != keystore.create_account().address


def test_a_missing_encryption_key_fails_closed():
    with pytest.raises(ValueError, match="KEYSTORE_ENCRYPTION_KEY"):
        Keystore("")


def test_logging_at_every_level_works(capsys):
    """Regression: a broken processor chain must not swallow error logs."""
    import os

    from mcw_common.logging import configure_logging, get_logger

    configure_logging("test-service", "DEBUG")
    log = get_logger("regression")
    log.info("hello", user_id=1)
    log.warning("careful", private_key="0x" + "ab" * 32)
    log.error("boom", tx_hash="0x" + "cd" * 32)

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 3
    assert all('"service": "test-service"' in line for line in lines)
    assert all('"logger": "regression"' in line for line in lines)
    assert "ab" * 32 not in "\n".join(lines)
    assert "cd" * 32 in "\n".join(lines)

    # Logging config is global: restore it so later tests are not made noisy.
    configure_logging("test-service", os.environ.get("LOG_LEVEL", "WARNING"))
