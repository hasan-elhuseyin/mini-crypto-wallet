"""The chain port.

Everything above this module talks to *a chain* through this interface only.
Two adapters implement it:

* :class:`~app.chain.mock.MockChainAdapter` -- a self-contained simulated chain
  with blocks, confirmations, receipts, failed transactions and **reorgs**.
* :class:`~app.chain.evm.Web3ChainAdapter` -- a real EVM node (anvil, Hardhat,
  BSC testnet) speaking JSON-RPC through web3.py.

Selected with ``CHAIN_BACKEND``. Nothing else in the service changes.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

__all__ = [
    "ChainError",
    "RpcUnavailableError",
    "TransactionRejectedError",
    "TransferLog",
    "TxReceipt",
    "ChainAdapter",
]


class ChainError(Exception):
    """Base class for chain-level failures."""


class RpcUnavailableError(ChainError):
    """The node could not be reached. Always safe to retry a *read*."""


class TransactionRejectedError(ChainError):
    """The node refused the transaction outright (bad nonce, no funds, ...)."""


@dataclass(frozen=True, slots=True)
class TransferLog:
    """A decoded ERC-20/BEP-20 ``Transfer`` event."""

    network: str
    asset: str
    tx_hash: str
    log_index: int
    block_number: int
    block_hash: str
    from_address: str
    to_address: str
    amount: int  # smallest units

    @property
    def identity(self) -> str:
        """The natural key that makes deposits idempotent."""
        return f"{self.network}:{self.tx_hash}:{self.log_index}"


@dataclass(frozen=True, slots=True)
class TxReceipt:
    tx_hash: str
    block_number: int
    block_hash: str
    status: int  # 1 = success, 0 = reverted


@runtime_checkable
class ChainAdapter(Protocol):
    network: str
    asset: str

    async def get_block_number(self) -> int:
        """Height of the canonical head."""

    async def get_block_hash(self, number: int) -> str | None:
        """Canonical hash at ``number``; ``None`` if unknown//pruned."""

    async def get_transfer_logs(
        self, from_block: int, to_block: int, addresses: Sequence[str] | None = None
    ) -> list[TransferLog]:
        """Token Transfer logs in an inclusive block range (``eth_getLogs``)."""

    async def get_receipt(self, tx_hash: str) -> TxReceipt | None:
        """``None`` while pending -- and also after a reorg evicted the tx."""

    async def send_transfer(
        self, *, from_address: str, to_address: str, amount: int, client_ref: str
    ) -> str:
        """Sign and broadcast a token transfer; returns the transaction hash.

        ``client_ref`` makes the call **idempotent**: the same reference always
        produces the same on-chain identity, so a retry after an ambiguous
        failure can never double spend.
        """

    async def get_token_balance(self, address: str) -> int:
        ...
