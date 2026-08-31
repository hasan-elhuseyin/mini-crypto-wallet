"""Outgoing transaction lifecycle: accept -> broadcast -> confirm.

Deliberately **not** done inside the message consumer. The consumer only
records the intent (one row, one transaction, then ack). Broadcasting -- the
part that talks to a flaky network and must be retried on a schedule -- is a
separate worker with a lease.

The lease is what makes a crash safe: a claimed row is retried after its lease
expires, and because ``client_ref`` (= transfer id) determines the on-chain
identity, a retry of a send that actually succeeded is a no-op rather than a
second spend.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from mcw_common.assets import get_asset
from mcw_common.correlation import correlation_scope
from mcw_common.events import EventType, new_event
from mcw_common.logging import get_logger
from mcw_common.money import format_amount
from mcw_common.outbox import enqueue_event
from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..chain.base import RpcUnavailableError, TransactionRejectedError
from ..models import Address, OutgoingTransaction, TxStatus

log = get_logger("outgoing")

PRODUCER = "blockchain-service"

_CLAIM_SQL = text(
    """
    UPDATE outgoing_transactions AS o
       SET attempts = o.attempts + 1,
           status = 'PENDING',
           next_attempt_at = now() + (interval '1 second' * :lease)
     WHERE o.id IN (
           SELECT id FROM outgoing_transactions
            WHERE status IN ('CREATED', 'PENDING')
              AND (next_attempt_at IS NULL OR next_attempt_at <= now())
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT :limit
     )
     RETURNING o.id, o.transfer_id, o.from_address, o.to_address, o.amount,
               o.attempts, o.correlation_id, o.asset, o.network
    """
)


def failure_payload(record: OutgoingTransaction, *, reason: str, code: str) -> dict[str, Any]:
    asset = get_asset(record.asset)
    return {
        "transfer_id": str(record.transfer_id),
        "tx_hash": record.tx_hash,
        "network": record.network,
        "asset": record.asset,
        "amount": format_amount(record.amount, asset.decimals),
        "amount_units": str(record.amount),
        "reason": reason,
        "code": code,
        "attempts": record.attempts,
        # Tells wallet-service whether it must *reverse* postings or merely
        # release a hold.
        "was_confirmed": record.status == TxStatus.CONFIRMED,
    }


async def accept_transfer_request(session, payload: dict[str, Any], correlation_id: str) -> str:
    """Idempotently record a transfer request. Returns an outcome label."""
    transfer_id = uuid.UUID(str(payload["transfer_id"]))
    from_address = payload["from_address"]
    to_address = payload["to_address"]
    amount = int(payload["amount_units"])
    asset = payload.get("asset", "USDT")
    network = payload.get("network", "BSC")

    known = (
        await session.execute(select(Address.id).where(Address.address == from_address))
    ).scalar_one_or_none()

    inserted = (
        await session.execute(
            pg_insert(OutgoingTransaction)
            .values(
                transfer_id=transfer_id,
                network=network,
                asset=asset,
                from_address=from_address,
                to_address=to_address,
                amount=amount,
                status=TxStatus.CREATED,
                correlation_id=correlation_id,
            )
            .on_conflict_do_nothing(index_elements=["transfer_id"])
            .returning(OutgoingTransaction.id)
        )
    ).scalar_one_or_none()

    if inserted is None:
        return "duplicate"

    if known is None:
        # We cannot sign for an address we do not custody: fail fast and loudly
        # rather than leaving the transfer stuck forever.
        record = await session.get(OutgoingTransaction, inserted, with_for_update=True)
        record.status = TxStatus.FAILED
        record.failure_reason = "unknown sending address"
        await enqueue_event(
            session,
            new_event(
                EventType.TX_FAILED,
                failure_payload(record, reason="unknown sending address",
                                code="UNKNOWN_SENDING_ADDRESS"),
                producer=PRODUCER,
                dedupe_key=f"{transfer_id}:UNKNOWN_SENDING_ADDRESS",
                correlation_id=correlation_id,
            ),
        )
        log.error("outgoing.unknown_address", transfer_id=str(transfer_id),
                  from_address=from_address)
        return "processed"

    log.info(
        "outgoing.accepted", transfer_id=str(transfer_id), from_address=from_address,
        to_address=to_address, amount_units=str(amount),
    )
    return "processed"


class BroadcastWorker:
    def __init__(self, ctx: Any) -> None:
        self._ctx = ctx
        self._settings = ctx.settings

    def _lease_seconds(self, attempts: int) -> int:
        return min(60, 2 ** min(attempts, 6))

    async def run_once(self, limit: int = 20) -> dict[str, Any]:
        # Phase 1: claim work under a short lease. Nothing here talks to the network.
        async with self._ctx.db.sessionmaker() as session, session.begin():
            claimed = (
                await session.execute(
                    _CLAIM_SQL, {"limit": limit, "lease": self._lease_seconds(1)}
                )
            ).mappings().all()
        if not claimed:
            return {"broadcast": 0, "failed": 0, "claimed": 0}

        broadcast = failed = 0
        for row in claimed:
            # Phase 2: the network call, outside any transaction.
            outcome = await self._broadcast_one(row)
            broadcast += outcome == "broadcast"
            failed += outcome == "failed"
        return {"broadcast": broadcast, "failed": failed, "claimed": len(claimed)}

    async def _broadcast_one(self, row) -> str:
        transfer_id = row["transfer_id"]
        correlation_id = row["correlation_id"]
        with correlation_scope(correlation_id):
            try:
                tx_hash = await self._ctx.adapter.send_transfer(
                    from_address=row["from_address"],
                    to_address=row["to_address"],
                    amount=int(row["amount"]),
                    client_ref=str(transfer_id),
                )
            except TransactionRejectedError as exc:
                # The node will never accept this transaction; retrying is pointless.
                return await self._record_failure(
                    row, reason=str(exc), code="REJECTED_BY_NODE"
                )
            except Exception as exc:
                # RPC down / timeout / unknown: retriable until the budget runs out.
                exhausted = int(row["attempts"]) >= self._settings.broadcast_max_attempts
                if not exhausted:
                    backoff = self._lease_seconds(int(row["attempts"]))
                    async with self._ctx.db.sessionmaker() as session, session.begin():
                        await session.execute(
                            update(OutgoingTransaction)
                            .where(OutgoingTransaction.id == row["id"])
                            .values(
                                status=TxStatus.PENDING,
                                failure_reason=str(exc)[:500],
                                next_attempt_at=datetime.now(UTC)
                                + timedelta(seconds=backoff),
                            )
                        )
                    log.warning(
                        "outgoing.broadcast_retry",
                        transfer_id=str(transfer_id), attempt=int(row["attempts"]),
                        retry_in_seconds=backoff, error=str(exc),
                    )
                    return "retry"
                return await self._record_failure(
                    row,
                    reason=str(exc),
                    code="RPC_UNAVAILABLE"
                    if isinstance(exc, RpcUnavailableError)
                    else "BROADCAST_FAILED",
                )

            async with self._ctx.db.sessionmaker() as session, session.begin():
                record = await session.get(
                    OutgoingTransaction, row["id"], with_for_update=True
                )
                if record is None or record.status in (TxStatus.CONFIRMED, TxStatus.FAILED):
                    return "skipped"
                record.tx_hash = tx_hash
                record.status = TxStatus.BROADCASTED
                record.broadcast_at = datetime.now(UTC)
                record.failure_reason = None
                record.next_attempt_at = None
                asset = get_asset(record.asset)
                await enqueue_event(
                    session,
                    new_event(
                        EventType.TX_BROADCASTED,
                        {
                            "transfer_id": str(record.transfer_id),
                            "tx_hash": tx_hash,
                            "network": record.network,
                            "asset": record.asset,
                            "amount": format_amount(record.amount, asset.decimals),
                            "amount_units": str(record.amount),
                            "from_address": record.from_address,
                            "to_address": record.to_address,
                            "attempts": record.attempts,
                        },
                        producer=PRODUCER,
                        dedupe_key=f"{record.transfer_id}:{tx_hash}:broadcast",
                        correlation_id=correlation_id,
                    ),
                )
            log.info(
                "outgoing.broadcasted", transfer_id=str(transfer_id), tx_hash=tx_hash,
                attempts=int(row["attempts"]),
            )
            return "broadcast"

    async def _record_failure(self, row, *, reason: str, code: str) -> str:
        async with self._ctx.db.sessionmaker() as session, session.begin():
            record = await session.get(OutgoingTransaction, row["id"], with_for_update=True)
            if record is None or record.status == TxStatus.CONFIRMED:
                return "skipped"
            record.status = TxStatus.FAILED
            record.failure_reason = reason[:500]
            record.next_attempt_at = None
            await enqueue_event(
                session,
                new_event(
                    EventType.TX_FAILED,
                    failure_payload(record, reason=reason[:500], code=code),
                    producer=PRODUCER,
                    dedupe_key=f"{record.transfer_id}:{code}",
                    correlation_id=record.correlation_id,
                ),
            )
        log.error(
            "outgoing.failed", transfer_id=str(row["transfer_id"]), code=code, reason=reason,
        )
        return "failed"


class OutgoingTransactionWatcher:
    """Confirmations, on-chain reverts, stuck transactions and reorgs."""

    def __init__(self, ctx: Any) -> None:
        self._ctx = ctx
        self._settings = ctx.settings

    async def run_once(self) -> dict[str, Any]:
        adapter = self._ctx.adapter
        try:
            head = await adapter.get_block_number()
        except RpcUnavailableError:
            log.warning("watcher.rpc_unavailable", component="outgoing-transactions")
            return {"confirmed": 0, "failed": 0, "checked": 0, "rpc_available": False}

        async with self._ctx.db.sessionmaker() as session:
            candidates = (
                await session.execute(
                    select(OutgoingTransaction.id).where(
                        OutgoingTransaction.status.in_(
                            [TxStatus.BROADCASTED, TxStatus.CONFIRMED]
                        ),
                        (OutgoingTransaction.block_number.is_(None))
                        | (OutgoingTransaction.block_number > head - self._settings.finality_depth),
                    )
                )
            ).scalars().all()

        confirmed = failed = 0
        for record_id in candidates:
            outcome = await self._check_one(record_id, head)
            confirmed += outcome == "confirmed"
            failed += outcome in ("failed", "reorged")
        return {"confirmed": confirmed, "failed": failed, "checked": len(candidates), "head": head}

    async def _check_one(self, record_id, head: int) -> str:
        async with self._ctx.db.sessionmaker() as session:
            record = await session.get(OutgoingTransaction, record_id)
            if record is None or not record.tx_hash:
                return "skipped"
            tx_hash, status = record.tx_hash, record.status
            broadcast_at, attempts = record.broadcast_at, record.attempts

        receipt = await self._ctx.adapter.get_receipt(tx_hash)

        async with self._ctx.db.sessionmaker() as session, session.begin():
            record = await session.get(OutgoingTransaction, record_id, with_for_update=True)
            if record is None:
                return "skipped"
            with correlation_scope(record.correlation_id):
                if receipt is None:
                    if status == TxStatus.CONFIRMED:
                        return await self._mark_reorged(session, record)
                    return await self._handle_stuck(session, record, broadcast_at, attempts)

                record.block_number = receipt.block_number
                record.block_hash = receipt.block_hash
                record.confirmations = max(0, head - receipt.block_number + 1)

                if receipt.status == 0:
                    if record.status == TxStatus.FAILED:
                        return "skipped"
                    record.status = TxStatus.FAILED
                    record.failure_reason = "transaction reverted on chain"
                    await enqueue_event(
                        session,
                        new_event(
                            EventType.TX_FAILED,
                            failure_payload(
                                record, reason="transaction reverted on chain",
                                code="REVERTED_ON_CHAIN",
                            ),
                            producer=PRODUCER,
                            dedupe_key=f"{record.transfer_id}:REVERTED_ON_CHAIN",
                            correlation_id=record.correlation_id,
                        ),
                    )
                    log.error(
                        "outgoing.reverted", transfer_id=str(record.transfer_id),
                        tx_hash=tx_hash, block_number=receipt.block_number,
                    )
                    return "failed"

                if (
                    record.status == TxStatus.BROADCASTED
                    and record.confirmations >= self._settings.confirmations_required
                ):
                    record.status = TxStatus.CONFIRMED
                    asset = get_asset(record.asset)
                    await enqueue_event(
                        session,
                        new_event(
                            EventType.TX_CONFIRMED,
                            {
                                "transfer_id": str(record.transfer_id),
                                "tx_hash": tx_hash,
                                "network": record.network,
                                "asset": record.asset,
                                "amount": format_amount(record.amount, asset.decimals),
                                "amount_units": str(record.amount),
                                "from_address": record.from_address,
                                "to_address": record.to_address,
                                "block_number": receipt.block_number,
                                "block_hash": receipt.block_hash,
                                "confirmations": record.confirmations,
                            },
                            producer=PRODUCER,
                            dedupe_key=f"{record.transfer_id}:{tx_hash}:confirmed",
                            correlation_id=record.correlation_id,
                        ),
                    )
                    log.info(
                        "outgoing.confirmed", transfer_id=str(record.transfer_id),
                        tx_hash=tx_hash, confirmations=record.confirmations,
                    )
                    return "confirmed"
        return "checked"

    async def _handle_stuck(self, session, record, broadcast_at, attempts) -> str:
        """No receipt yet. Either still normal, or stuck long enough to act."""
        if broadcast_at is None:
            return "checked"
        age = (datetime.now(UTC) - broadcast_at).total_seconds()
        if age < self._settings.pending_timeout_seconds:
            return "checked"
        if attempts < self._settings.broadcast_max_attempts:
            # Re-queue for rebroadcast. Same client_ref => same transaction
            # identity => this is a rebroadcast, never a second spend.
            record.status = TxStatus.PENDING
            record.next_attempt_at = datetime.now(UTC)
            log.warning(
                "outgoing.stuck_pending_requeued",
                transfer_id=str(record.transfer_id), tx_hash=record.tx_hash,
                pending_for_seconds=round(age), attempts=attempts,
            )
            return "requeued"
        record.status = TxStatus.FAILED
        record.failure_reason = f"still pending after {round(age)}s and {attempts} attempts"
        await enqueue_event(
            session,
            new_event(
                EventType.TX_FAILED,
                failure_payload(
                    record, reason=record.failure_reason, code="STUCK_PENDING"
                ),
                producer=PRODUCER,
                dedupe_key=f"{record.transfer_id}:STUCK_PENDING",
                correlation_id=record.correlation_id,
            ),
        )
        log.error(
            "outgoing.stuck_pending_failed", transfer_id=str(record.transfer_id),
            tx_hash=record.tx_hash, pending_for_seconds=round(age),
        )
        return "failed"

    async def _mark_reorged(self, session, record) -> str:
        payload = failure_payload(
            record, reason="transaction disappeared from the chain (reorg)",
            code="CHAIN_REORG",
        )
        payload["was_confirmed"] = True
        record.status = TxStatus.REORGED
        record.failure_reason = "transaction disappeared from the chain (reorg)"
        await enqueue_event(
            session,
            new_event(
                EventType.TX_FAILED,
                payload,
                producer=PRODUCER,
                dedupe_key=f"{record.transfer_id}:CHAIN_REORG:{record.block_hash}",
                correlation_id=record.correlation_id,
            ),
        )
        log.error(
            "outgoing.reorged", transfer_id=str(record.transfer_id), tx_hash=record.tx_hash,
        )
        return "reorged"
