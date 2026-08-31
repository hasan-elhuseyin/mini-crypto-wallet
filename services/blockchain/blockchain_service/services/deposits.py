"""Deposit detection and confirmation tracking.

Two cooperating loops:

``DepositScanner``
    Walks the chain with a persisted cursor, turns token ``Transfer`` logs
    addressed to one of our addresses into ``deposits`` rows, and emits
    ``deposit.detected``. The insert is an upsert on
    ``(network, tx_hash, log_index)`` -- rescanning a range (after a crash, a
    restart or a reorg rewind) can therefore never create a second deposit.

``DepositConfirmationWatcher``
    Re-checks every non-final deposit against the chain. It promotes deposits
    to ``CONFIRMED`` once they are deep enough, and -- crucially -- keeps
    re-verifying them until they are past the finality depth, so a deposit that
    disappears in a reorg is caught *after* it was already confirmed and a
    ``deposit.reorged`` event is emitted.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from mcw_common.assets import get_asset
from mcw_common.correlation import correlation_scope
from mcw_common.events import EventType, new_event
from mcw_common.logging import get_logger
from mcw_common.metrics import DEPOSITS
from mcw_common.money import format_amount
from mcw_common.outbox import enqueue_event
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..chain.base import RpcUnavailableError, TransferLog
from ..models import Deposit, DepositStatus, ScanState
from .addresses import active_addresses

log = get_logger("deposits")

PRODUCER = "blockchain-service"


def deposit_payload(deposit: Deposit, *, confirmations: int | None = None) -> dict[str, Any]:
    asset = get_asset(deposit.asset)
    return {
        "deposit_id": str(deposit.id),
        "network": deposit.network,
        "asset": deposit.asset,
        "tx_hash": deposit.tx_hash,
        "log_index": deposit.log_index,
        "from_address": deposit.from_address,
        "to_address": deposit.to_address,
        # `amount_units` is authoritative (integer smallest units, as a string
        # so no JSON parser can turn it into a float). `amount` is for humans.
        "amount": format_amount(deposit.amount, asset.decimals),
        "amount_units": str(deposit.amount),
        "decimals": asset.decimals,
        "block_number": deposit.block_number,
        "block_hash": deposit.block_hash,
        "confirmations": deposit.confirmations if confirmations is None else confirmations,
    }


class DepositScanner:
    def __init__(self, ctx: Any) -> None:
        self._ctx = ctx
        self._settings = ctx.settings

    async def run_once(self) -> dict[str, Any]:
        adapter = self._ctx.adapter
        network = self._settings.network
        head = await adapter.get_block_number()
        if head <= 0:
            return {"scanned": 0, "detected": 0, "head": head}

        async with self._ctx.db.sessionmaker() as session:
            state = await session.get(ScanState, network)
            cursor = int(state.last_scanned_block) if state else 0
            last_hash = state.last_scanned_hash if state else None

        # Reorg guard: if the block we stopped at no longer hashes the same, the
        # chain moved under us -- rewind by the safety margin and rescan.
        if last_hash:
            current_hash = await adapter.get_block_hash(cursor)
            if current_hash != last_hash:
                rewound = max(0, cursor - self._settings.reorg_safety_blocks)
                log.warning(
                    "scanner.reorg_rewind",
                    from_block=cursor, to_block=rewound,
                    expected_hash=last_hash, actual_hash=current_hash,
                )
                cursor, last_hash = rewound, None

        from_block = cursor + 1
        to_block = min(head, from_block + self._settings.scan_batch_blocks - 1)
        if from_block > to_block:
            return {"scanned": 0, "detected": 0, "head": head}

        async with self._ctx.db.sessionmaker() as session:
            watched = await active_addresses(session)
        logs: list[TransferLog] = (
            await adapter.get_transfer_logs(from_block, to_block, watched) if watched else []
        )
        tip_hash = await adapter.get_block_hash(to_block)

        # A user-to-user transfer is settled on chain between two addresses we
        # custody, so the recipient's leg also shows up here as an incoming
        # Transfer log. Crediting it would pay the recipient twice -- once for
        # the transfer and once as a "deposit". Anything sent *from* an address
        # we control is an internal movement that the transfer flow already
        # accounts for.
        ours = {address.lower() for address in watched}
        external, internal = [], 0
        for entry in logs:
            if entry.from_address.lower() in ours:
                internal += 1
            else:
                external.append(entry)
        if internal:
            log.debug(
                "scanner.internal_transfers_skipped",
                count=internal, from_block=from_block, to_block=to_block,
            )
        logs = external

        detected = 0
        # One transaction: deposits, their events and the cursor advance commit
        # together. A crash mid-way simply rescans the same range.
        async with self._ctx.db.sessionmaker() as session, session.begin():
            for entry in logs:
                if await self._upsert_deposit(session, entry):
                    detected += 1
            await session.execute(
                pg_insert(ScanState)
                .values(network=network, last_scanned_block=to_block, last_scanned_hash=tip_hash)
                .on_conflict_do_update(
                    index_elements=["network"],
                    set_={"last_scanned_block": to_block, "last_scanned_hash": tip_hash},
                )
            )
        if detected:
            log.info(
                "scanner.batch", from_block=from_block, to_block=to_block,
                head=head, detected=detected,
            )
        return {
            "scanned": to_block - from_block + 1, "detected": detected,
            "internal_skipped": internal,
            "head": head, "from_block": from_block, "to_block": to_block,
        }

    async def _upsert_deposit(self, session, entry: TransferLog) -> bool:
        """Insert a new deposit, or revive one that a reorg had knocked out.

        Returns True when a ``deposit.detected`` event was enqueued.
        """
        stmt = (
            pg_insert(Deposit)
            .values(
                network=entry.network,
                asset=entry.asset,
                tx_hash=entry.tx_hash,
                log_index=entry.log_index,
                to_address=entry.to_address,
                from_address=entry.from_address,
                amount=entry.amount,
                block_number=entry.block_number,
                block_hash=entry.block_hash,
                confirmations=0,
                status=DepositStatus.DETECTED,
            )
            .on_conflict_do_update(
                constraint="uq_deposit_chain_identity",
                set_={
                    "block_number": entry.block_number,
                    "block_hash": entry.block_hash,
                    "status": DepositStatus.DETECTED,
                    "confirmations": 0,
                    "confirmed_at": None,
                    "reorged_at": None,
                },
                # Only revive a deposit that a reorg removed; never touch a live one.
                where=Deposit.status == DepositStatus.REORGED,
            )
            .returning(Deposit.id)
        )
        deposit_id = (await session.execute(stmt)).scalar_one_or_none()
        if deposit_id is None:
            return False  # already known and unchanged -> idempotent no-op
        deposit = await session.get(Deposit, deposit_id)
        assert deposit is not None
        with correlation_scope(f"dep-{entry.tx_hash[2:14]}") as correlation_id:
            await enqueue_event(
                session,
                new_event(
                    EventType.DEPOSIT_DETECTED,
                    deposit_payload(deposit, confirmations=0),
                    producer=PRODUCER,
                    dedupe_key=f"{entry.identity}:{entry.block_hash}",
                    correlation_id=correlation_id,
                ),
            )
            DEPOSITS.labels(status=DepositStatus.DETECTED).inc()
            log.info(
                "deposit.detected",
                tx_hash=entry.tx_hash, log_index=entry.log_index,
                to_address=entry.to_address, amount_units=str(entry.amount),
                block_number=entry.block_number,
            )
        return True


class DepositConfirmationWatcher:
    def __init__(self, ctx: Any) -> None:
        self._ctx = ctx
        self._settings = ctx.settings

    async def run_once(self) -> dict[str, Any]:
        adapter = self._ctx.adapter
        try:
            head = await adapter.get_block_number()
        except RpcUnavailableError:
            log.warning("watcher.rpc_unavailable", component="deposit-confirmations")
            return {"confirmed": 0, "reorged": 0, "checked": 0, "rpc_available": False}
        if head <= 0:
            return {"confirmed": 0, "reorged": 0, "checked": 0}

        required = self._settings.confirmations_required
        finality = self._settings.finality_depth
        confirmed = reorged = checked = 0

        async with self._ctx.db.sessionmaker() as session:
            watchlist = (
                await session.execute(
                    select(Deposit).where(
                        Deposit.status.in_([DepositStatus.DETECTED, DepositStatus.CONFIRMED]),
                        # Past the finality depth we stop re-verifying: history
                        # is treated as settled and the row becomes read-only.
                        Deposit.block_number > head - finality,
                    ).order_by(Deposit.block_number)
                )
            ).scalars().all()
            deposit_ids = [d.id for d in watchlist]

        for deposit_id in deposit_ids:
            outcome = await self._reverify(deposit_id, head, required)
            checked += 1
            confirmed += outcome == "confirmed"
            reorged += outcome == "reorged"
        return {"confirmed": confirmed, "reorged": reorged, "checked": checked, "head": head}

    async def _reverify(self, deposit_id, head: int, required: int) -> str:
        adapter = self._ctx.adapter
        async with self._ctx.db.sessionmaker() as session:
            deposit = await session.get(Deposit, deposit_id)
            if deposit is None:
                return "gone"
            tx_hash, stored_block_hash = deposit.tx_hash, deposit.block_hash
            to_address, log_index = deposit.to_address, deposit.log_index

        receipt = await adapter.get_receipt(tx_hash)
        still_present = False
        new_block_number = new_block_hash = None
        if receipt is not None:
            if receipt.block_hash == stored_block_hash:
                still_present = True
                new_block_number, new_block_hash = receipt.block_number, receipt.block_hash
            else:
                # The transaction was re-mined into a different block. It is only
                # the *same deposit* if the very same log still exists there.
                entries = await adapter.get_transfer_logs(
                    receipt.block_number, receipt.block_number, [to_address]
                )
                match = next(
                    (e for e in entries if e.tx_hash == tx_hash and e.log_index == log_index),
                    None,
                )
                if match is not None:
                    still_present = True
                    new_block_number, new_block_hash = match.block_number, match.block_hash

        async with self._ctx.db.sessionmaker() as session, session.begin():
            deposit = await session.get(Deposit, deposit_id, with_for_update=True)
            if deposit is None:
                return "gone"
            with correlation_scope(f"dep-{deposit.tx_hash[2:14]}") as correlation_id:
                if not still_present:
                    return await self._mark_reorged(session, deposit, correlation_id)
                deposit.block_number = new_block_number
                deposit.block_hash = new_block_hash
                deposit.confirmations = max(0, head - int(new_block_number) + 1)
                if (
                    deposit.status == DepositStatus.DETECTED
                    and deposit.confirmations >= required
                ):
                    deposit.status = DepositStatus.CONFIRMED
                    deposit.confirmed_at = datetime.now(UTC)
                    await enqueue_event(
                        session,
                        new_event(
                            EventType.DEPOSIT_CONFIRMED,
                            deposit_payload(deposit),
                            producer=PRODUCER,
                            # block hash in the key: a deposit that is reorged
                            # and later re-mined must confirm again, as a
                            # genuinely new fact.
                            dedupe_key=f"{deposit.network}:{deposit.tx_hash}:"
                                       f"{deposit.log_index}:{deposit.block_hash}",
                            correlation_id=correlation_id,
                        ),
                    )
                    DEPOSITS.labels(status=DepositStatus.CONFIRMED).inc()
                    log.info(
                        "deposit.confirmed",
                        tx_hash=deposit.tx_hash, log_index=deposit.log_index,
                        confirmations=deposit.confirmations,
                        amount_units=str(deposit.amount), to_address=deposit.to_address,
                    )
                    return "confirmed"
        return "checked"

    async def _mark_reorged(self, session, deposit: Deposit, correlation_id: str) -> str:
        was_confirmed = deposit.status == DepositStatus.CONFIRMED
        orphaned_block_hash = deposit.block_hash
        deposit.status = DepositStatus.REORGED
        deposit.reorged_at = datetime.now(UTC)
        payload = deposit_payload(deposit)
        payload["was_confirmed"] = was_confirmed
        payload["orphaned_block_hash"] = orphaned_block_hash
        await enqueue_event(
            session,
            new_event(
                EventType.DEPOSIT_REORGED,
                payload,
                producer=PRODUCER,
                dedupe_key=f"{deposit.network}:{deposit.tx_hash}:{deposit.log_index}:"
                           f"{orphaned_block_hash}:reorg",
                correlation_id=correlation_id,
            ),
        )
        DEPOSITS.labels(status=DepositStatus.REORGED).inc()
        log.warning(
            "deposit.reorged",
            tx_hash=deposit.tx_hash, log_index=deposit.log_index,
            was_confirmed=was_confirmed, orphaned_block_hash=orphaned_block_hash,
        )
        return "reorged"
