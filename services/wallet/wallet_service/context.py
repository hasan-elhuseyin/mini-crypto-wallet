"""Composition root for wallet-service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis
from mcw_common.bus import RedisStreamsBus
from mcw_common.db import Database
from mcw_common.logging import get_logger
from mcw_common.outbox import OutboxRelay
from sqlalchemy import text

from .chain_client import BlockchainServiceClient
from .config import Settings

log = get_logger("context")


@dataclass
class Context:
    settings: Settings
    db: Database
    redis: Any
    bus: RedisStreamsBus
    relay: OutboxRelay
    blockchain: BlockchainServiceClient

    @classmethod
    async def create(cls, settings: Settings) -> Context:
        db = Database(settings.database_url)
        redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        bus = RedisStreamsBus(redis)
        blockchain = BlockchainServiceClient(
            settings.blockchain_service_url,
            internal_key=settings.internal_api_key,
            timeout=settings.blockchain_timeout_seconds,
        )
        log.info("context.ready", network=settings.default_network)
        return cls(
            settings=settings, db=db, redis=redis, bus=bus,
            relay=OutboxRelay(db.sessionmaker, bus), blockchain=blockchain,
        )

    async def close(self) -> None:
        await self.blockchain.aclose()
        await self.db.dispose()
        await self.redis.aclose()

    async def check_database(self) -> bool:
        async with self.db.sessionmaker() as session:
            await session.execute(text("SELECT 1"))
        return True

    async def check_redis(self) -> bool:
        return bool(await self.redis.ping())

    async def check_blockchain_service(self) -> bool:
        return await self.blockchain.ping()
