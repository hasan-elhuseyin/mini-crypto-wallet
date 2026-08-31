"""Address issuance.

Idempotent by ``owner_ref``: wallet-service can retry ``POST /addresses`` any
number of times and always gets the same address back. That matters because a
lost response must never strand a second unusable custody key.
"""

from __future__ import annotations

from mcw_common.logging import get_logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..chain import Keystore
from ..models import Address, KeyMaterial

log = get_logger("addresses")


async def get_or_create_address(
    session: AsyncSession, *, owner_ref: str, network: str, keystore: Keystore
) -> tuple[Address, bool]:
    """Returns ``(address, created)``."""
    existing = (
        await session.execute(select(Address).where(Address.owner_ref == owner_ref))
    ).scalar_one_or_none()
    if existing is not None:
        return existing, False

    generated = keystore.create_account()
    address = Address(
        owner_ref=owner_ref,
        network=network,
        address=generated.address,
        derivation="random-keypair",
        status="ACTIVE",
    )
    session.add(address)
    await session.flush()
    session.add(
        KeyMaterial(
            address=generated.address,
            encrypted_private_key=keystore.encrypt(generated.private_key),
            key_version=keystore.key_version,
        )
    )
    # NOTE: `generated.private_key` is never logged, returned or serialised.
    log.info(
        "address.created", owner_ref=owner_ref, address=generated.address, network=network
    )
    return address, True


async def find_by_owner(session: AsyncSession, owner_ref: str) -> Address | None:
    return (
        await session.execute(select(Address).where(Address.owner_ref == owner_ref))
    ).scalar_one_or_none()


async def active_addresses(session: AsyncSession) -> list[str]:
    return list(
        (
            await session.execute(select(Address.address).where(Address.status == "ACTIVE"))
        ).scalars()
    )
