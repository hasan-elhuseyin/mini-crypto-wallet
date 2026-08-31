"""Moves transfers from CREATED to PROCESSING once their event is out.

Small on purpose: it exists so ``GET /transfers/{id}`` can tell a client the
difference between "we accepted it and it is still in our outbox" and "it is
on its way to the chain". That distinction is exactly what the outbox pattern
buys, so it is worth surfacing.
"""

from __future__ import annotations

from typing import Any

from mcw_common.logging import get_logger
from sqlalchemy import text

log = get_logger("progress")

_ADVANCE_SQL = text(
    """
    UPDATE transfers t
       SET status = 'PROCESSING', updated_at = now()
      FROM outbox o
     WHERE o.event_type = 'transfer.requested'
       AND o.published_at IS NOT NULL
       AND (o.envelope -> 'payload' ->> 'transfer_id') = t.id::text
       AND t.status = 'CREATED'
    """
)


class TransferProgressWorker:
    def __init__(self, ctx: Any) -> None:
        self._ctx = ctx

    async def run_once(self) -> int:
        async with self._ctx.db.sessionmaker() as session, session.begin():
            result = await session.execute(_ADVANCE_SQL)
        return result.rowcount or 0
