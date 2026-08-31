"""Key generation and encryption-at-rest.

What this is: real secp256k1 EVM keypairs (``eth-account``), encrypted with an
authenticated symmetric cipher (Fernet = AES-128-CBC + HMAC-SHA256) before they
touch the database.

What this is **not**: a production custody solution. See README ->
"Security Considerations" for the KMS/HSM design this stands in for.
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass

from cryptography.fernet import Fernet, InvalidToken
from eth_account import Account
from mcw_common.logging import get_logger

__all__ = ["GeneratedKey", "Keystore"]

log = get_logger("keystore")


@dataclass(frozen=True, slots=True)
class GeneratedKey:
    address: str
    #: Only ever held in memory, only long enough to encrypt it.
    private_key: str

    def __repr__(self) -> str:  # pragma: no cover - defensive
        return f"GeneratedKey(address={self.address!r}, private_key='***REDACTED***')"

    __str__ = __repr__


class Keystore:
    """Encrypts/decrypts private keys with a versioned data key."""

    def __init__(self, encryption_key: str, *, key_version: int = 1) -> None:
        if not encryption_key:
            raise ValueError(
                "KEYSTORE_ENCRYPTION_KEY is required. Generate one with: "
                "python -c \"from cryptography.fernet import Fernet;"
                "print(Fernet.generate_key().decode())\""
            )
        try:
            self._fernet = Fernet(encryption_key.encode())
        except (ValueError, TypeError) as exc:
            raise ValueError("KEYSTORE_ENCRYPTION_KEY is not a valid Fernet key") from exc
        self.key_version = key_version

    @staticmethod
    def generate_key() -> str:
        return Fernet.generate_key().decode()

    def create_account(self) -> GeneratedKey:
        """Generate a fresh keypair from the OS CSPRNG."""
        account = Account.create(os.urandom(32))
        return GeneratedKey(address=account.address, private_key=account.key.hex())

    def encrypt(self, private_key: str) -> bytes:
        return self._fernet.encrypt(private_key.encode())

    def decrypt(self, blob: bytes) -> str:
        try:
            return self._fernet.decrypt(blob).decode()
        except InvalidToken as exc:
            # Never include the ciphertext or key in the message.
            raise ValueError("stored key material could not be decrypted") from exc

    @staticmethod
    def address_from_private_key(private_key: str) -> str:
        return Account.from_key(private_key).address


def looks_like_fernet_key(value: str) -> bool:
    try:
        return len(base64.urlsafe_b64decode(value.encode())) == 32
    except Exception:
        return False
