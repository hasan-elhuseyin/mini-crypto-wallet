"""Transfer lifecycle.

    CREATED -> PROCESSING -> BROADCASTED -> CONFIRMED
                                         \\-> FAILED

Money is **held** at creation and **posted** at settlement. Nothing is debited
while the chain has not confirmed, which is what makes the failure path cheap:
if the transaction never lands we release a hold, we do not unwind postings.

A transfer that was already settled and then lost to a reorg is the one case
that needs unwinding, and it is done with explicit ``REVERSAL`` entries -- the
ledger is append-only, so a mistake is corrected by a new fact, never by
editing an old one.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from mcw_common.assets import get_asset
from mcw_common.correlation import get_correlation_id
from mcw_common.errors import (
    ConflictError,
    InsufficientFundsError,
    NotFoundError,
    ValidationError,
    WalletInactiveError,
)
from mcw_common.events import EventType, new_event
from mcw_common.logging import get_logger
from mcw_common.metrics import TRANSFERS
from mcw_common.money import format_amount
from mcw_common.outbox import enqueue_event
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import EntryType, Transfer, TransferStatus
from .ledger import Posting, lock_balance, place_hold, post_entries, release_hold
from .users import get_user, get_wallet

log = get_logger("transfers")

PRODUCER = "wallet-service"
REFERENCE_TYPE = "TRANSFER"


async def create_transfer(
    session: AsyncSession,
    *,
    from_user_id: int,
    to_user_id: int,
    asset: str,
    amount_units: int,
    idempotency_key: str,
    network: str,
) -> Transfer:
    """Validate, hold the funds and queue the on-chain request. One transaction."""
    if from_user_id == to_user_id:
        raise ValidationError("A transfer must have two distinct parties.")

    asset_def = get_asset(asset)
    sender = await get_user(session, from_user_id)
    recipient = await get_user(session, to_user_id)
    for user in (sender, recipient):
        if user.status != "ACTIVE":
            raise ConflictError(f"User {user.id} is not active.")

    from_wallet = await get_wallet(
        session, user_id=from_user_id, network=network, asset=asset_def.symbol
    )
    to_wallet = await get_wallet(
        session, user_id=to_user_id, network=network, asset=asset_def.symbol
    )
    if from_wallet is None or to_wallet is None:
        missing = from_user_id if from_wallet is None else to_user_id
        raise NotFoundError(
            f"User {missing} has no {asset_def.symbol} wallet on {network}."
        )
    for wallet in (from_wallet, to_wallet):
        if wallet.status != "ACTIVE":
            raise WalletInactiveError(f"Wallet {wallet.id} is not active.")

    # Serialisation point: lock the sender's balance row, then reserve the funds
    # with a conditional update. See services/ledger.py::place_hold.
    balance = await lock_balance(session, from_user_id, asset_def.symbol)
    if not await place_hold(
        session, user_id=from_user_id, asset=asset_def.symbol, amount=amount_units
    ):
        available = balance.posted - balance.reserved
        log.info(
            "transfer.rejected_insufficient_funds",
            user_id=from_user_id, requested_units=str(amount_units),
            available_units=str(available),
        )
        raise InsufficientFundsError(
            f"Available balance is {format_amount(available, asset_def.decimals)} "
            f"{asset_def.symbol}, requested "
            f"{format_amount(amount_units, asset_def.decimals)}.",
            errors=[
                {
                    "available": format_amount(available, asset_def.decimals),
                    "requested": format_amount(amount_units, asset_def.decimals),
                    "asset": asset_def.symbol,
                }
            ],
        )

    transfer = Transfer(
        id=uuid.uuid4(),
        idempotency_key=idempotency_key,
        from_user_id=from_user_id,
        to_user_id=to_user_id,
        asset=asset_def.symbol,
        network=network,
        amount=amount_units,
        from_address=from_wallet.address,
        to_address=to_wallet.address,
        status=TransferStatus.CREATED,
        correlation_id=get_correlation_id(),
    )
    session.add(transfer)
    try:
        await session.flush()
    except IntegrityError as exc:
        raise ConflictError("This transfer has already been created.") from exc

    await enqueue_event(
        session,
        new_event(
            EventType.TRANSFER_REQUESTED,
            {
                "transfer_id": str(transfer.id),
                "from_user_id": from_user_id,
                "to_user_id": to_user_id,
                "asset": asset_def.symbol,
                "network": network,
                "from_address": from_wallet.address,
                "to_address": to_wallet.address,
                "amount": format_amount(amount_units, asset_def.decimals),
                "amount_units": str(amount_units),
            },
            producer=PRODUCER,
            dedupe_key=str(transfer.id),
        ),
    )
    TRANSFERS.labels(status=TransferStatus.CREATED).inc()
    log.info(
        "transfer.created",
        transfer_id=str(transfer.id), from_user_id=from_user_id, to_user_id=to_user_id,
        amount_units=str(amount_units), asset=asset_def.symbol,
        idempotency_key=idempotency_key,
    )
    return transfer


async def _load_for_update(session: AsyncSession, transfer_id: str) -> Transfer | None:
    return (
        await session.execute(
            select(Transfer).where(Transfer.id == uuid.UUID(str(transfer_id))).with_for_update()
        )
    ).scalar_one_or_none()


async def mark_broadcasted(session: AsyncSession, payload: dict[str, Any]) -> str:
    transfer = await _load_for_update(session, payload["transfer_id"])
    if transfer is None:
        log.error("transfer.unknown", transfer_id=payload.get("transfer_id"))
        return "ignored"
    if transfer.status in TransferStatus.TERMINAL:
        return "duplicate"  # a late broadcast event for a settled transfer
    transfer.status = TransferStatus.BROADCASTED
    transfer.tx_hash = payload.get("tx_hash")
    TRANSFERS.labels(status=TransferStatus.BROADCASTED).inc()
    log.info(
        "transfer.broadcasted", transfer_id=str(transfer.id), tx_hash=transfer.tx_hash
    )
    return "processed"


async def settle_transfer(session: AsyncSession, payload: dict[str, Any]) -> str:
    """Release the hold and write the two ledger postings. Atomic and idempotent."""
    transfer = await _load_for_update(session, payload["transfer_id"])
    if transfer is None:
        log.error("transfer.unknown", transfer_id=payload.get("transfer_id"))
        return "ignored"
    if transfer.status == TransferStatus.CONFIRMED:
        return "duplicate"
    if transfer.status == TransferStatus.FAILED:
        # The chain says confirmed but we already failed it: never silently
        # correct money -- record it loudly for an operator.
        log.error(
            "transfer.confirmed_after_failure",
            transfer_id=str(transfer.id), tx_hash=payload.get("tx_hash"),
        )
        return "ignored"

    await release_hold(
        session, user_id=transfer.from_user_id, asset=transfer.asset, amount=transfer.amount
    )
    await post_entries(
        session,
        asset=transfer.asset,
        postings=[
            Posting(
                user_id=transfer.from_user_id,
                amount=-transfer.amount,
                entry_type=EntryType.TRANSFER_DEBIT,
                reference_type=REFERENCE_TYPE,
                reference_id=str(transfer.id),
            ),
            Posting(
                user_id=transfer.to_user_id,
                amount=transfer.amount,
                entry_type=EntryType.TRANSFER_CREDIT,
                reference_type=REFERENCE_TYPE,
                reference_id=str(transfer.id),
            ),
        ],
    )
    transfer.status = TransferStatus.CONFIRMED
    transfer.tx_hash = payload.get("tx_hash") or transfer.tx_hash
    transfer.settled_at = datetime.now(UTC)
    TRANSFERS.labels(status=TransferStatus.CONFIRMED).inc()
    log.info(
        "transfer.settled",
        transfer_id=str(transfer.id), tx_hash=transfer.tx_hash,
        amount_units=str(transfer.amount), from_user_id=transfer.from_user_id,
        to_user_id=transfer.to_user_id,
    )
    return "processed"


async def fail_transfer(session: AsyncSession, payload: dict[str, Any]) -> str:
    """Release the hold, or reverse the postings if the transfer had settled."""
    transfer = await _load_for_update(session, payload["transfer_id"])
    if transfer is None:
        log.error("transfer.unknown", transfer_id=payload.get("transfer_id"))
        return "ignored"
    if transfer.status == TransferStatus.FAILED:
        return "duplicate"

    code = payload.get("code", "UNKNOWN")
    reason = payload.get("reason", "transaction failed")

    if transfer.status == TransferStatus.CONFIRMED:
        # Settled money that the chain took back (reorg). Correct it with new
        # entries; the originals stay in the ledger for audit.
        await post_entries(
            session,
            asset=transfer.asset,
            postings=[
                Posting(
                    user_id=transfer.from_user_id,
                    amount=transfer.amount,
                    entry_type=EntryType.REVERSAL,
                    reference_type=REFERENCE_TYPE,
                    reference_id=str(transfer.id),
                ),
                Posting(
                    user_id=transfer.to_user_id,
                    amount=-transfer.amount,
                    entry_type=EntryType.REVERSAL,
                    reference_type=REFERENCE_TYPE,
                    reference_id=str(transfer.id),
                ),
            ],
        )
        log.error(
            "transfer.reversed", transfer_id=str(transfer.id), code=code, reason=reason
        )
    else:
        await release_hold(
            session, user_id=transfer.from_user_id, asset=transfer.asset,
            amount=transfer.amount,
        )
        log.warning(
            "transfer.failed", transfer_id=str(transfer.id), code=code, reason=reason
        )

    transfer.status = TransferStatus.FAILED
    transfer.failure_code = code
    transfer.failure_reason = str(reason)[:1000]
    transfer.tx_hash = payload.get("tx_hash") or transfer.tx_hash
    TRANSFERS.labels(status=TransferStatus.FAILED).inc()
    return "processed"
