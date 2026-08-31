"""Composition root for blockchain-service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis
from mcw_common.bus import RedisStreamsBus
from mcw_common.db import Database
from mcw_common.logging import get_logger
from mcw_common.outbox import OutboxRelay
from sqlalchemy import select, text

from .chain import Keystore, MockChain, build_chain_adapter
from .config import Settings
from .models import KeyMaterial, OutgoingTransaction

log = get_logger("context")


@dataclass
class Context:
    settings: Settings
    db: Database
    redis: Any
    bus: RedisStreamsBus
    keystore: Keystore
    adapter: Any
    mock_chain: MockChain | None
    relay: OutboxRelay

    @classmethod
    async def create(cls, settings: Settings) -> Context:
        db = Database(settings.database_url)
        redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        bus = RedisStreamsBus(redis)
        keystore = Keystore(settings.keystore_encryption_key)

        async def key_resolver(address: str) -> str:
            """Only ever called by the signer, and only for the sending address."""
            async with db.sessionmaker() as session:
                blob = (
                    await session.execute(
                        select(KeyMaterial.encrypted_private_key).where(
                            KeyMaterial.address == address
                        )
                    )
                ).scalar_one_or_none()
            if blob is None:
                raise LookupError(f"no key material for {address}")
            return keystore.decrypt(blob)

        async def nonce_allocator(address: str, client_ref: str) -> int:
            """Stable nonce per outgoing transfer (see chain/evm.py docstring)."""
            async with db.sessionmaker() as session, session.begin():
                existing = (
                    await session.execute(
                        select(OutgoingTransaction.nonce).where(
                            OutgoingTransaction.transfer_id == client_ref,
                            OutgoingTransaction.nonce.is_not(None),
                        )
                    )
                ).scalar_one_or_none()
                if existing is not None:
                    return int(existing)
                highest = (
                    await session.execute(
                        select(OutgoingTransaction.nonce)
                        .where(OutgoingTransaction.from_address == address)
                        .order_by(OutgoingTransaction.nonce.desc().nulls_last())
                        .limit(1)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
            return int(highest) + 1 if highest is not None else 0

        adapter, mock_chain = build_chain_adapter(
            settings,
            sessionmaker=db.sessionmaker,
            key_resolver=key_resolver,
            nonce_allocator=nonce_allocator,
        )
        if mock_chain is not None:
            await mock_chain.ensure_started()
        relay = OutboxRelay(db.sessionmaker, bus)
        log.info(
            "context.ready",
            chain_backend=settings.chain_backend,
            network=settings.network,
            confirmations_required=settings.confirmations_required,
        )
        return cls(
            settings=settings, db=db, redis=redis, bus=bus, keystore=keystore,
            adapter=adapter, mock_chain=mock_chain, relay=relay,
        )

    async def close(self) -> None:
        await self.db.dispose()
        await self.redis.aclose()

    # -- readiness probes --------------------------------------------------

    async def check_database(self) -> bool:
        async with self.db.sessionmaker() as session:
            await session.execute(text("SELECT 1"))
        return True

    async def check_redis(self) -> bool:
        return bool(await self.redis.ping())

    async def check_chain(self) -> bool:
        await self.adapter.get_block_number()
        return True
