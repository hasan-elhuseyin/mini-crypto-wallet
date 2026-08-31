"""A simulated blockchain that behaves like one.

This is the default backend. It is *not* a stub that returns canned values --
it maintains real state and reproduces the properties that make blockchain
integration hard:

* blocks are produced over time, so **confirmations accumulate gradually**;
* a transaction sits in a mempool before it is mined (``PENDING``);
* a mined transaction can have receipt status 0 (**reverted on chain**);
* a transfer whose sender lacks tokens fails at mining time, not at submit;
* **reorgs** happen: canonical blocks can be replaced by a longer fork and a
  previously mined transaction can disappear from the chain entirely;
* the node can be **unreachable** (RPC faults) or **congested** (nothing mined).

Those last three are exactly the scenarios a real integration has to survive,
and a public testnet will not produce them on demand.

State lives in the ``mockchain`` Postgres schema, in its own transactions --
the service never mixes node state and service state in one transaction, which
keeps the boundary the same as it would be against a real node.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Sequence

from mcw_common.logging import get_logger
from mcw_common.metrics import CHAIN_RPC_CALLS
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from ..models import MockBalance, MockBlock, MockFaults, MockTx
from .base import RpcUnavailableError, TransactionRejectedError, TransferLog, TxReceipt

__all__ = ["MockChain", "MockChainAdapter", "EXTERNAL_SOURCE_ADDRESS", "GENESIS_NUMBER"]

log = get_logger("mockchain")

#: Stands in for "some exchange out there" as the source of an incoming deposit.
EXTERNAL_SOURCE_ADDRESS = "0x1111111111111111111111111111111111111111"
GENESIS_NUMBER = 1
_MINING_LOCK_KEY = 0x6D636D696E65  # arbitrary, stable advisory-lock id
_MAX_TXS_PER_BLOCK = 50


def _hash(*parts: str) -> str:
    return "0x" + hashlib.sha256("|".join(parts).encode()).hexdigest()


class MockChain:
    """The 'node'. Owns its own sessions; callers never share a transaction."""

    def __init__(
        self, sessionmaker: async_sessionmaker, *, network: str, asset: str = "USDT"
    ) -> None:
        self._sf = sessionmaker
        self.network = network
        self.asset = asset

    # -- lifecycle ---------------------------------------------------------

    async def ensure_started(self) -> None:
        """Create the genesis block and the fault-switch row if missing."""
        async with self._sf() as session, session.begin():
            await session.execute(
                pg_insert(MockFaults).values(id=1).on_conflict_do_nothing(index_elements=["id"])
            )
            exists = (await session.execute(select(MockBlock.number).limit(1))).first()
            if exists is None:
                genesis_hash = _hash("genesis", self.network)
                session.add(
                    MockBlock(
                        number=GENESIS_NUMBER,
                        hash=genesis_hash,
                        parent_hash="0x" + "0" * 64,
                        is_canonical=True,
                    )
                )
                log.info("mockchain.genesis", number=GENESIS_NUMBER)

    async def _faults(self, session) -> MockFaults:
        faults = await session.get(MockFaults, 1, with_for_update=True)
        if faults is None:
            faults = MockFaults(id=1)
            session.add(faults)
            await session.flush()
        return faults

    async def _assert_rpc_up(self, session) -> None:
        faults = await session.get(MockFaults, 1)
        if faults is not None and not faults.rpc_available:
            raise RpcUnavailableError("simulated RPC node is unreachable")

    # -- read side (mirrors JSON-RPC) --------------------------------------

    async def get_block_number(self) -> int:
        async with self._sf() as session:
            await self._assert_rpc_up(session)
            head = (
                await session.execute(
                    select(MockBlock.number).where(MockBlock.is_canonical.is_(True))
                    .order_by(MockBlock.number.desc()).limit(1)
                )
            ).scalar_one_or_none()
            return int(head or 0)

    async def get_block_hash(self, number: int) -> str | None:
        async with self._sf() as session:
            await self._assert_rpc_up(session)
            return (
                await session.execute(
                    select(MockBlock.hash).where(
                        MockBlock.number == number, MockBlock.is_canonical.is_(True)
                    )
                )
            ).scalar_one_or_none()

    async def get_transfer_logs(
        self, from_block: int, to_block: int, addresses: Sequence[str] | None = None
    ) -> list[TransferLog]:
        if from_block > to_block:
            return []
        async with self._sf() as session:
            await self._assert_rpc_up(session)
            stmt = (
                # Join on the block *hash*: after a reorg the same height can
                # exist twice, and only the canonical block counts.
                select(MockTx, MockBlock.hash)
                .join(MockBlock, MockBlock.hash == MockTx.block_hash)
                .where(
                    MockTx.status == "MINED",
                    MockTx.block_number >= from_block,
                    MockTx.block_number <= to_block,
                    MockBlock.is_canonical.is_(True),
                )
                .order_by(MockTx.block_number, MockTx.log_index)
            )
            if addresses:
                # The equivalent of an eth_getLogs topic filter on `to`.
                lowered = [a.lower() for a in addresses]
                stmt = stmt.where(func.lower(MockTx.to_address).in_(lowered))
            rows = (await session.execute(stmt)).all()
            return [
                TransferLog(
                    network=self.network,
                    asset=tx.asset,
                    tx_hash=tx.hash,
                    log_index=int(tx.log_index or 0),
                    block_number=int(tx.block_number or 0),
                    block_hash=block_hash,
                    from_address=tx.from_address,
                    to_address=tx.to_address,
                    amount=tx.amount,
                )
                for tx, block_hash in rows
            ]

    async def get_receipt(self, tx_hash: str) -> TxReceipt | None:
        async with self._sf() as session:
            await self._assert_rpc_up(session)
            row = (
                await session.execute(
                    select(MockTx, MockBlock.is_canonical, MockBlock.hash)
                    .outerjoin(MockBlock, MockBlock.hash == MockTx.block_hash)
                    .where(MockTx.hash == tx_hash)
                )
            ).first()
            if row is None:
                return None
            tx, is_canonical, block_hash = row
            if tx.status in ("PENDING", "DROPPED") or not is_canonical:
                # Pending, or evicted by a reorg: a real node returns null here.
                return None
            return TxReceipt(
                tx_hash=tx.hash,
                block_number=int(tx.block_number or 0),
                block_hash=block_hash or "",
                status=1 if tx.status == "MINED" else 0,
            )

    async def get_token_balance(self, address: str) -> int:
        async with self._sf() as session:
            await self._assert_rpc_up(session)
            amount = (
                await session.execute(
                    select(MockBalance.amount).where(MockBalance.address == address)
                )
            ).scalar_one_or_none()
            return int(amount or 0)

    # -- write side --------------------------------------------------------

    async def send_transfer(
        self, *, from_address: str, to_address: str, amount: int, client_ref: str,
        is_mint: bool = False,
    ) -> str:
        """Submit a transfer to the mempool.

        The transaction hash is derived from ``client_ref`` -- the simulation of
        "same nonce + same payload => same signed transaction => same hash".
        Re-submitting is therefore a no-op rather than a second spend.
        """
        if amount <= 0:
            raise TransactionRejectedError("amount must be positive")
        tx_hash = _hash("tx", self.network, client_ref)
        async with self._sf() as session, session.begin():
            await self._assert_rpc_up(session)
            result = await session.execute(
                pg_insert(MockTx)
                .values(
                    hash=tx_hash,
                    from_address=from_address,
                    to_address=to_address,
                    amount=amount,
                    asset=self.asset,
                    status="PENDING",
                    is_mint=is_mint,
                )
                .on_conflict_do_nothing(index_elements=["hash"])
                .returning(MockTx.hash)
            )
            duplicated = result.scalar_one_or_none() is None
        log.info(
            "mockchain.tx_submitted",
            tx_hash=tx_hash, to_address=to_address, amount=str(amount),
            duplicate_submission=duplicated,
        )
        return tx_hash

    async def mine_block(self) -> int:
        """Produce one block, including pending transactions."""
        async with self._sf() as session, session.begin():
            await session.execute(
                text("SELECT pg_advisory_xact_lock(:key)"), {"key": _MINING_LOCK_KEY}
            )
            faults = await self._faults(session)
            head = (
                await session.execute(
                    select(MockBlock).where(MockBlock.is_canonical.is_(True))
                    .order_by(MockBlock.number.desc()).limit(1)
                )
            ).scalar_one_or_none()
            number = (head.number + 1) if head else GENESIS_NUMBER
            parent_hash = head.hash if head else "0x" + "0" * 64
            # Randomised: a re-mined height after a reorg gets a *different* hash,
            # which is what makes hash-based reorg detection meaningful.
            block_hash = _hash("block", str(number), parent_hash, uuid.uuid4().hex)
            session.add(
                MockBlock(
                    number=number, hash=block_hash, parent_hash=parent_hash,
                    is_canonical=True,
                )
            )

            included = 0
            if not faults.halt_mining:
                pending = (
                    await session.execute(
                        select(MockTx).where(MockTx.status == "PENDING")
                        .order_by(MockTx.created_at, MockTx.hash)
                        .limit(_MAX_TXS_PER_BLOCK).with_for_update()
                    )
                ).scalars().all()
                log_index = 0
                for tx in pending:
                    forced_failure = tx.fail_on_mine or (
                        faults.fail_next_transfers > 0 and not tx.is_mint
                    )
                    if forced_failure and not tx.fail_on_mine:
                        faults.fail_next_transfers -= 1
                    reverted = forced_failure
                    if not reverted and not tx.is_mint:
                        sender = await session.get(
                            MockBalance, tx.from_address, with_for_update=True
                        )
                        if sender is None or sender.amount < tx.amount:
                            reverted = True  # insufficient token balance -> revert
                    tx.block_number = number
                    tx.block_hash = block_hash
                    if reverted:
                        tx.status = "FAILED"
                        tx.log_index = None
                    else:
                        await self._apply_balances(session, tx, sign=1)
                        tx.status = "MINED"
                        tx.log_index = log_index
                        log_index += 1
                    included += 1
        log.debug("mockchain.block_mined", number=number, transactions=included)
        return number

    async def _apply_balances(self, session, tx: MockTx, *, sign: int) -> None:
        if not tx.is_mint:
            await self._adjust(session, tx.from_address, -sign * tx.amount)
        await self._adjust(session, tx.to_address, sign * tx.amount)

    async def _adjust(self, session, address: str, delta: int) -> None:
        await session.execute(
            pg_insert(MockBalance)
            .values(address=address, amount=0)
            .on_conflict_do_nothing(index_elements=["address"])
        )
        await session.execute(
            update(MockBalance)
            .where(MockBalance.address == address)
            .values(amount=MockBalance.amount + delta)
        )

    # -- simulation controls ----------------------------------------------

    async def simulate_deposit(self, *, to_address: str, amount: int, client_ref: str) -> str:
        """An external party sends tokens to one of our addresses."""
        return await self.send_transfer(
            from_address=EXTERNAL_SOURCE_ADDRESS,
            to_address=to_address,
            amount=amount,
            client_ref=client_ref,
            is_mint=True,
        )

    async def reorg(self, *, depth: int, drop_tx_hashes: Sequence[str] = ()) -> dict:
        """Replace the top ``depth`` blocks with a longer competing fork.

        Transactions from the orphaned blocks return to the mempool, except
        those named in ``drop_tx_hashes`` which are dropped for good -- that is
        the case that makes a *confirmed* deposit vanish.
        """
        drop = {h.lower() for h in drop_tx_hashes}
        async with self._sf() as session, session.begin():
            await session.execute(
                text("SELECT pg_advisory_xact_lock(:key)"), {"key": _MINING_LOCK_KEY}
            )
            head = (
                await session.execute(
                    select(MockBlock.number).where(MockBlock.is_canonical.is_(True))
                    .order_by(MockBlock.number.desc()).limit(1)
                )
            ).scalar_one_or_none()
            if head is None:
                return {"orphaned_blocks": [], "reverted_transactions": []}
            lowest = max(GENESIS_NUMBER + 1, head - depth + 1)
            victims = (
                await session.execute(
                    select(MockBlock).where(
                        MockBlock.is_canonical.is_(True), MockBlock.number >= lowest
                    ).order_by(MockBlock.number.desc()).with_for_update()
                )
            ).scalars().all()
            reverted: list[str] = []
            for block in victims:
                txs = (
                    await session.execute(
                        select(MockTx).where(MockTx.block_hash == block.hash)
                        .with_for_update()
                    )
                ).scalars().all()
                for tx in txs:
                    if tx.status == "MINED":
                        await self._apply_balances(session, tx, sign=-1)
                    tx.block_number = None
                    tx.block_hash = None
                    tx.log_index = None
                    tx.status = "DROPPED" if tx.hash.lower() in drop else "PENDING"
                    reverted.append(tx.hash)
                block.is_canonical = False
            orphaned = [b.number for b in victims]
        # Build the competing fork so the new chain is strictly longer.
        for _ in range(len(orphaned) + 1):
            await self.mine_block()
        log.warning(
            "mockchain.reorg", depth=len(orphaned), orphaned_blocks=orphaned,
            dropped=sorted(drop),
        )
        return {"orphaned_blocks": orphaned, "reverted_transactions": reverted,
                "dropped_transactions": sorted(drop)}

    async def set_faults(
        self,
        *,
        rpc_available: bool | None = None,
        halt_mining: bool | None = None,
        fail_next_transfers: int | None = None,
    ) -> dict:
        async with self._sf() as session, session.begin():
            faults = await self._faults(session)
            if rpc_available is not None:
                faults.rpc_available = rpc_available
            if halt_mining is not None:
                faults.halt_mining = halt_mining
            if fail_next_transfers is not None:
                faults.fail_next_transfers = max(0, fail_next_transfers)
            snapshot = {
                "rpc_available": faults.rpc_available,
                "halt_mining": faults.halt_mining,
                "fail_next_transfers": faults.fail_next_transfers,
            }
        log.warning("mockchain.faults_updated", **snapshot)
        return snapshot

    async def get_faults(self) -> dict:
        async with self._sf() as session:
            faults = await session.get(MockFaults, 1)
            return {
                "rpc_available": bool(faults.rpc_available) if faults else True,
                "halt_mining": bool(faults.halt_mining) if faults else False,
                "fail_next_transfers": int(faults.fail_next_transfers) if faults else 0,
            }


class MockChainAdapter:
    """Adapts :class:`MockChain` to the :class:`ChainAdapter` port."""

    def __init__(self, chain: MockChain) -> None:
        self.chain = chain
        self.network = chain.network
        self.asset = chain.asset

    async def get_block_number(self) -> int:
        return await self._call("get_block_number", self.chain.get_block_number())

    async def get_block_hash(self, number: int) -> str | None:
        return await self._call("get_block_hash", self.chain.get_block_hash(number))

    async def get_transfer_logs(
        self, from_block: int, to_block: int, addresses: Sequence[str] | None = None
    ) -> list[TransferLog]:
        return await self._call(
            "get_transfer_logs", self.chain.get_transfer_logs(from_block, to_block, addresses)
        )

    async def get_receipt(self, tx_hash: str) -> TxReceipt | None:
        return await self._call("get_receipt", self.chain.get_receipt(tx_hash))

    async def send_transfer(
        self, *, from_address: str, to_address: str, amount: int, client_ref: str
    ) -> str:
        return await self._call(
            "send_transfer",
            self.chain.send_transfer(
                from_address=from_address, to_address=to_address,
                amount=amount, client_ref=client_ref,
            ),
        )

    async def get_token_balance(self, address: str) -> int:
        return await self._call("get_token_balance", self.chain.get_token_balance(address))

    async def _call(self, method: str, coro):
        try:
            result = await coro
        except Exception:
            CHAIN_RPC_CALLS.labels(method=method, outcome="error").inc()
            raise
        CHAIN_RPC_CALLS.labels(method=method, outcome="ok").inc()
        return result
