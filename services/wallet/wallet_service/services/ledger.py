"""The posting engine.

Every balance change in the system goes through :func:`post_entries`. It is the
only place that writes ``ledger_entries`` and the only place that moves
``balances.posted``, which means the two can never drift for a reason other
than a bug in this one function.

Three properties are enforced here:

* **Atomicity** -- entries and snapshots move in the caller's transaction.
* **Idempotency** -- the unique key ``(user_id, entry_type, reference_type,
  reference_id)`` makes a replayed event a no-op at the *database* level.
* **Deadlock freedom** -- balance rows are locked in a deterministic order
  (ascending user id), so two opposite transfers can never deadlock.
"""

from __future__ import annotations

from dataclasses import dataclass

from mcw_common.correlation import get_correlation_id
from mcw_common.logging import get_logger
from mcw_common.metrics import LEDGER_ENTRIES
from sqlalchemy import and_, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Balance, LedgerEntry

log = get_logger("ledger")


@dataclass(frozen=True, slots=True)
class Posting:
    user_id: int
    #: Signed, in the asset's smallest unit. Credits positive, debits negative.
    amount: int
    entry_type: str
    reference_type: str
    reference_id: str


async def ensure_balance(session: AsyncSession, user_id: int, asset: str) -> None:
    await session.execute(
        pg_insert(Balance)
        .values(user_id=user_id, asset=asset, posted=0, reserved=0)
        .on_conflict_do_nothing(index_elements=["user_id", "asset"])
    )


async def lock_balance(session: AsyncSession, user_id: int, asset: str) -> Balance:
    """Take a row lock on the (user, asset) balance -- the serialisation point."""
    await ensure_balance(session, user_id, asset)
    balance = (
        await session.execute(
            select(Balance)
            .where(Balance.user_id == user_id, Balance.asset == asset)
            .with_for_update()
        )
    ).scalar_one()
    return balance


async def read_balance(session: AsyncSession, user_id: int, asset: str) -> Balance | None:
    return (
        await session.execute(
            select(Balance).where(Balance.user_id == user_id, Balance.asset == asset)
        )
    ).scalar_one_or_none()


async def post_entries(
    session: AsyncSession, *, asset: str, postings: list[Posting]
) -> int:
    """Write ledger entries and move the snapshots. Returns entries written.

    A return value of ``0`` means every posting was already applied -- the
    caller is replaying an event and must treat that as success.
    """
    correlation_id = get_correlation_id()
    written = 0
    for posting in sorted(postings, key=lambda p: p.user_id):
        await lock_balance(session, posting.user_id, asset)
        inserted = (
            await session.execute(
                pg_insert(LedgerEntry)
                .values(
                    user_id=posting.user_id,
                    asset=asset,
                    amount=posting.amount,
                    entry_type=posting.entry_type,
                    reference_type=posting.reference_type,
                    reference_id=posting.reference_id,
                    correlation_id=correlation_id,
                )
                .on_conflict_do_nothing(constraint="uq_ledger_idempotency")
                .returning(LedgerEntry.id)
            )
        ).scalar_one_or_none()
        if inserted is None:
            log.info(
                "ledger.duplicate_posting_ignored",
                user_id=posting.user_id, entry_type=posting.entry_type,
                reference_id=posting.reference_id,
            )
            continue
        await session.execute(
            update(Balance)
            .where(Balance.user_id == posting.user_id, Balance.asset == asset)
            .values(posted=Balance.posted + posting.amount, version=Balance.version + 1)
        )
        LEDGER_ENTRIES.labels(entry_type=posting.entry_type).inc()
        written += 1
        log.info(
            "ledger.posted",
            user_id=posting.user_id, asset=asset, amount_units=str(posting.amount),
            entry_type=posting.entry_type, reference_id=posting.reference_id,
        )
    return written


async def place_hold(session: AsyncSession, *, user_id: int, asset: str, amount: int) -> bool:
    """Reserve ``amount`` of available balance. False when there is not enough.

    Two independent guards, both in the database:

    1. the caller holds a ``FOR UPDATE`` lock on the row (see :func:`lock_balance`),
       which serialises concurrent transfers for the same account;
    2. the ``WHERE posted - reserved >= amount`` predicate makes the update
       itself conditional, so even without the lock the second of two
       concurrent updates would fail rather than overspend.

    On top of that the ``ck_balance_available_non_negative`` CHECK constraint
    would reject the row outright.
    """
    result = await session.execute(
        update(Balance)
        .where(
            Balance.user_id == user_id,
            Balance.asset == asset,
            Balance.posted - Balance.reserved >= amount,
        )
        .values(reserved=Balance.reserved + amount, version=Balance.version + 1)
    )
    return result.rowcount == 1


async def release_hold(session: AsyncSession, *, user_id: int, asset: str, amount: int) -> None:
    await session.execute(
        update(Balance)
        .where(Balance.user_id == user_id, Balance.asset == asset)
        .values(reserved=Balance.reserved - amount, version=Balance.version + 1)
    )


async def reconcile(session: AsyncSession) -> list[dict]:
    """Compare every snapshot against the sum of its ledger entries.

    This is the check that would run on a schedule in production and page
    somebody the moment it returned an inconsistent row.
    """
    sums = (
        select(
            LedgerEntry.user_id.label("user_id"),
            LedgerEntry.asset.label("asset"),
            func.coalesce(func.sum(LedgerEntry.amount), 0).label("total"),
        )
        .group_by(LedgerEntry.user_id, LedgerEntry.asset)
        .subquery()
    )
    rows = (
        await session.execute(
            select(
                Balance.user_id, Balance.asset, Balance.posted, Balance.reserved,
                func.coalesce(sums.c.total, 0).label("ledger_sum"),
            ).outerjoin(
                sums,
                and_(sums.c.user_id == Balance.user_id, sums.c.asset == Balance.asset),
            ).order_by(Balance.user_id, Balance.asset)
        )
    ).all()
    report = []
    for row in rows:
        posted, ledger_sum = int(row.posted), int(row.ledger_sum)
        if posted != ledger_sum:
            log.error(
                "ledger.reconciliation_mismatch",
                user_id=row.user_id, asset=row.asset,
                snapshot_posted=str(posted), ledger_sum=str(ledger_sum),
            )
        report.append(
            {
                "user_id": row.user_id,
                "asset": row.asset,
                "snapshot_posted": str(posted),
                "ledger_sum": str(ledger_sum),
                "reserved": str(row.reserved),
                "consistent": posted == ledger_sum,
            }
        )
    return report
