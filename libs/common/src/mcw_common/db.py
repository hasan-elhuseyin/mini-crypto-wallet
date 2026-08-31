"""Database plumbing shared by both services.

Each service owns its **own database** (`wallet` / `blockchain`). There is no
cross-service SQL and no distributed transaction: a service commits locally and
publishes an event from its outbox in the same transaction.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

from sqlalchemy import Numeric, types
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

__all__ = ["SmallestUnit", "Database", "utcnow_sql"]

utcnow_sql = "timezone('utc', now())"


class SmallestUnit(types.TypeDecorator):
    """NUMERIC(78,0) <-> Python ``int``.

    Deliberately *not* Float and deliberately not Decimal at the application
    level: balances are integers in the asset's smallest unit, so arithmetic in
    Python is exact by construction and no rounding mode ever applies.
    """

    impl = Numeric(78, 0)
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(
                f"amount columns accept int (smallest units) only, got {type(value).__name__}"
            )
        return Decimal(value)

    def process_result_value(self, value: Any, dialect: Any) -> int | None:
        if value is None:
            return None
        return int(value)


class Database:
    """Owns the engine/sessionmaker pair for one service."""

    def __init__(self, url: str, *, echo: bool = False, pool_size: int = 10) -> None:
        self.engine: AsyncEngine = create_async_engine(
            url,
            echo=echo,
            pool_size=pool_size,
            max_overflow=pool_size,
            pool_pre_ping=True,
            pool_recycle=1800,
        )
        self.sessionmaker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self.engine, expire_on_commit=False, autoflush=False
        )

    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.sessionmaker() as session:
            yield session

    async def dispose(self) -> None:
        await self.engine.dispose()
