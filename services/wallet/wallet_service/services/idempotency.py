"""API-level idempotency.

Design: the key is claimed and completed inside the **same** transaction as the
work it protects.

* Two concurrent requests with the same key: the second blocks on the unique
  index until the first commits, then observes the conflict and replays the
  stored response. No "in progress" state to reap.
* The request crashes mid-flight: the transaction rolls back, the key is
  released, and a retry is processed normally.
* Same key, different body: 409. Silently replaying an unrelated response would
  be worse than an error.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from mcw_common.errors import IdempotencyConflictError, IdempotencyInProgressError
from mcw_common.logging import get_logger
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import IdempotencyKey

log = get_logger("idempotency")

_PLACEHOLDER_STATUS = 0


def fingerprint(payload: dict[str, Any]) -> str:
    """Stable hash of a request body (key order and spacing independent)."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def claim(
    session: AsyncSession, *, scope: str, key: str, request_hash: str
) -> IdempotencyKey | None:
    """Claim the key. ``None`` means we own it and must do the work.

    A returned row means the request was already processed; replay its response.
    """
    owned = (
        await session.execute(
            pg_insert(IdempotencyKey)
            .values(
                scope=scope,
                key=key,
                request_hash=request_hash,
                response_status=_PLACEHOLDER_STATUS,
                response_body={},
            )
            .on_conflict_do_nothing(index_elements=["scope", "key"])
            .returning(IdempotencyKey.key)
        )
    ).scalar_one_or_none()
    if owned is not None:
        return None

    existing = (
        await session.execute(
            select(IdempotencyKey).where(
                IdempotencyKey.scope == scope, IdempotencyKey.key == key
            )
        )
    ).scalar_one_or_none()
    if existing is None:  # the other transaction rolled back between our statements
        raise IdempotencyInProgressError(
            "This idempotency key is being processed. Retry in a moment."
        )
    if existing.request_hash != request_hash:
        log.warning("idempotency.payload_mismatch", scope=scope, key=key)
        raise IdempotencyConflictError(
            "This idempotency key was already used with a different request body."
        )
    if existing.response_status == _PLACEHOLDER_STATUS:
        raise IdempotencyInProgressError(
            "This idempotency key is being processed. Retry in a moment."
        )
    log.info("idempotency.replayed", scope=scope, key=key, resource_id=existing.resource_id)
    return existing


async def complete(
    session: AsyncSession,
    *,
    scope: str,
    key: str,
    status: int,
    body: dict[str, Any],
    resource_id: str | None = None,
) -> None:
    await session.execute(
        update(IdempotencyKey)
        .where(IdempotencyKey.scope == scope, IdempotencyKey.key == key)
        .values(response_status=status, response_body=body, resource_id=resource_id)
    )
