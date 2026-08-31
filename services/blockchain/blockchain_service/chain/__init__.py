"""Chain adapter selection."""

from __future__ import annotations

from .base import (
    ChainAdapter,
    ChainError,
    RpcUnavailableError,
    TransactionRejectedError,
    TransferLog,
    TxReceipt,
)
from .keys import GeneratedKey, Keystore
from .mock import EXTERNAL_SOURCE_ADDRESS, MockChain, MockChainAdapter

__all__ = [
    "ChainAdapter",
    "ChainError",
    "RpcUnavailableError",
    "TransactionRejectedError",
    "TransferLog",
    "TxReceipt",
    "Keystore",
    "GeneratedKey",
    "MockChain",
    "MockChainAdapter",
    "EXTERNAL_SOURCE_ADDRESS",
    "build_chain_adapter",
]


def build_chain_adapter(settings, *, sessionmaker, key_resolver=None, nonce_allocator=None):
    """Construct the adapter selected by ``CHAIN_BACKEND``.

    Returns ``(adapter, mock_chain_or_None)``; the raw mock chain is exposed so
    the simulation endpoints can drive it.
    """
    if settings.chain_backend == "mock":
        chain = MockChain(sessionmaker, network=settings.network)
        return MockChainAdapter(chain), chain

    if settings.chain_backend == "web3":
        from .evm import Web3ChainAdapter

        if key_resolver is None or nonce_allocator is None:
            raise ValueError("the web3 backend requires a key resolver and a nonce allocator")
        adapter = Web3ChainAdapter(
            rpc_url=settings.web3_rpc_url,
            chain_id=settings.web3_chain_id,
            token_address=settings.usdt_contract_address,
            network=settings.network,
            asset="USDT",
            key_resolver=key_resolver,
            nonce_allocator=nonce_allocator,
        )
        return adapter, None

    raise ValueError(f"unknown CHAIN_BACKEND: {settings.chain_backend!r}")
