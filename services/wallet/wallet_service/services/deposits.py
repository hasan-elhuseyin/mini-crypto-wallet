"""Deposit handling on the wallet side.

The only thing that moves a balance is a *confirmed* deposit. ``detected`` is
recorded for observability only -- crediting on zero confirmations is how you
lose money to a reorg.
"""

from __future__ import annotations

from typing import Any

from mcw_common.logging import get_logger
from mcw_common.metrics import DEPOSITS
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import EntryType
from .ledger import Posting, post_entries
from .users import wallet_by_address

log = get_logger("deposits")

REFERENCE_TYPE = "DEPOSIT"


def reference_id(payload: dict[str, Any]) -> str:
    """The deposit's chain identity, including the block hash.

    ``network:tx_hash:log_index`` uniquely identifies the *log*. The block hash
    is appended because a deposit that is reorged out and later re-mined is a
    genuinely new fact that must be creditable again after its reversal.
    """
    return (
        f"{payload['network']}:{payload['tx_hash']}:{payload['log_index']}"
        f":{payload.get('block_hash', '')}"
    )


async def credit_deposit(session: AsyncSession, payload: dict[str, Any]) -> str:
    wallet = await wallet_by_address(session, payload["to_address"])
    if wallet is None:
        # Not ours: nothing to credit. Loud, because it should be impossible.
        log.error(
            "deposit.unknown_address",
            to_address=payload.get("to_address"), tx_hash=payload.get("tx_hash"),
        )
        return "ignored"

    amount = int(payload["amount_units"])
    written = await post_entries(
        session,
        asset=payload["asset"],
        postings=[
            Posting(
                user_id=wallet.user_id,
                amount=amount,
                entry_type=EntryType.DEPOSIT,
                reference_type=REFERENCE_TYPE,
                reference_id=reference_id(payload),
            )
        ],
    )
    if written == 0:
        # The unique constraint absorbed a replayed event.
        return "duplicate"
    DEPOSITS.labels(status="CREDITED").inc()
    log.info(
        "deposit.credited",
        user_id=wallet.user_id, asset=payload["asset"], amount_units=str(amount),
        tx_hash=payload["tx_hash"], confirmations=payload.get("confirmations"),
    )
    return "processed"


async def reverse_deposit(session: AsyncSession, payload: dict[str, Any]) -> str:
    """A deposit disappeared from the chain. Undo it with a REVERSAL entry."""
    if not payload.get("was_confirmed"):
        # Never credited, so there is nothing to take back.
        log.info(
            "deposit.reorged_before_credit",
            tx_hash=payload.get("tx_hash"), log_index=payload.get("log_index"),
        )
        return "processed"

    wallet = await wallet_by_address(session, payload["to_address"])
    if wallet is None:
        log.error("deposit.unknown_address", to_address=payload.get("to_address"))
        return "ignored"

    amount = int(payload["amount_units"])
    ref = (
        f"{payload['network']}:{payload['tx_hash']}:{payload['log_index']}"
        f":{payload.get('orphaned_block_hash', '')}"
    )
    written = await post_entries(
        session,
        asset=payload["asset"],
        postings=[
            Posting(
                user_id=wallet.user_id,
                amount=-amount,
                entry_type=EntryType.REVERSAL,
                reference_type=REFERENCE_TYPE,
                reference_id=ref,
            )
        ],
    )
    if written == 0:
        return "duplicate"
    DEPOSITS.labels(status="REVERSED").inc()
    log.error(
        "deposit.reversed",
        user_id=wallet.user_id, amount_units=str(amount), tx_hash=payload["tx_hash"],
        orphaned_block_hash=payload.get("orphaned_block_hash"),
    )
    return "processed"
