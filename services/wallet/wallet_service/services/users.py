"""User and wallet management."""

from __future__ import annotations

from mcw_common.errors import ConflictError, NotFoundError
from mcw_common.logging import get_logger
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import User, Wallet
from .ledger import ensure_balance

log = get_logger("users")


async def create_user(session: AsyncSession, *, name: str, email: str) -> User:
    user = User(name=name, email=email.lower(), status="ACTIVE")
    session.add(user)
    try:
        await session.flush()
    except IntegrityError as exc:
        raise ConflictError(f"A user with email '{email}' already exists.") from exc
    log.info("user.created", user_id=user.id, email=user.email)
    return user


async def get_user(session: AsyncSession, user_id: int) -> User:
    user = await session.get(User, user_id)
    if user is None:
        raise NotFoundError(f"User {user_id} does not exist.")
    return user


async def get_wallet(
    session: AsyncSession, *, user_id: int, network: str, asset: str
) -> Wallet | None:
    return (
        await session.execute(
            select(Wallet).where(
                Wallet.user_id == user_id, Wallet.network == network, Wallet.asset == asset
            )
        )
    ).scalar_one_or_none()


async def attach_wallet(
    session: AsyncSession, *, user_id: int, network: str, asset: str, address: str
) -> tuple[Wallet, bool]:
    """Record the address issued by blockchain-service. Idempotent.

    ``(wallet, created)``. Concurrent calls collapse onto the same row because
    of the ``(user_id, network, asset)`` unique constraint.
    """
    inserted = (
        await session.execute(
            pg_insert(Wallet)
            .values(
                user_id=user_id, network=network, asset=asset, address=address, status="ACTIVE"
            )
            .on_conflict_do_nothing(constraint="uq_wallet_user_network_asset")
            .returning(Wallet.id)
        )
    ).scalar_one_or_none()
    await ensure_balance(session, user_id, asset)
    wallet = await get_wallet(session, user_id=user_id, network=network, asset=asset)
    assert wallet is not None
    if inserted is not None:
        log.info(
            "wallet.created", user_id=user_id, network=network, asset=asset, address=address
        )
    return wallet, inserted is not None


async def wallet_by_address(session: AsyncSession, address: str) -> Wallet | None:
    return (
        await session.execute(select(Wallet).where(Wallet.address == address))
    ).scalar_one_or_none()
