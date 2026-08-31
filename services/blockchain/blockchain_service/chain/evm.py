"""Real EVM backend (anvil / Hardhat / BSC testnet) over web3.py.

Enabled with ``CHAIN_BACKEND=web3``. The rest of the service is unchanged --
the scanner, the confirmation watcher and the consumers only know the
:class:`ChainAdapter` port.

Two details worth calling out, because they are where money is lost in real
integrations:

1. **Nonce determinism.** The nonce is allocated once per outgoing transfer and
   persisted (``outgoing_transactions.nonce``). A retry re-signs the *identical*
   transaction, so it has the identical hash: the node either already has it
   (``already known``) or accepts it once. A blind "get pending nonce and
   resend" is how you double spend.
2. **Ambiguous broadcast.** If ``eth_sendRawTransaction`` times out we do not
   know whether the node accepted it. We therefore never mark the transfer
   failed on a send timeout -- we record the deterministic hash and let the
   confirmation watcher decide from the receipt.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence

from mcw_common.logging import get_logger
from mcw_common.metrics import CHAIN_RPC_CALLS

from .base import RpcUnavailableError, TransactionRejectedError, TransferLog, TxReceipt

__all__ = ["Web3ChainAdapter", "ERC20_ABI", "TRANSFER_TOPIC"]

log = get_logger("evm")

#: keccak256("Transfer(address,address,uint256)")
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

ERC20_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    },
    {
        "inputs": [{"name": "to", "type": "address"}, {"name": "value", "type": "uint256"}],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]

#: (address) -> private key. Injected so the adapter never touches the keystore.
KeyResolver = Callable[[str], Awaitable[str]]
#: (address, client_ref) -> nonce. Must be stable for a given client_ref.
NonceAllocator = Callable[[str, str], Awaitable[int]]


class Web3ChainAdapter:
    def __init__(
        self,
        *,
        rpc_url: str,
        chain_id: int,
        token_address: str,
        network: str,
        asset: str,
        key_resolver: KeyResolver,
        nonce_allocator: NonceAllocator,
        gas_limit: int = 120_000,
    ) -> None:
        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("CHAIN_BACKEND=web3 requires the 'web3' package") from exc

        self._w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
        self._chain_id = chain_id
        self._token_address = self._w3.to_checksum_address(token_address)
        self._contract = self._w3.eth.contract(address=self._token_address, abi=ERC20_ABI)
        self._key_resolver = key_resolver
        self._nonce_allocator = nonce_allocator
        self._gas_limit = gas_limit
        self.network = network
        self.asset = asset

    # -- reads -------------------------------------------------------------

    async def get_block_number(self) -> int:
        return await self._call("get_block_number", self._w3.eth.get_block_number())

    async def get_block_hash(self, number: int) -> str | None:
        try:
            block = await self._call("get_block", self._w3.eth.get_block(number))
        except Exception:
            return None
        return block["hash"].hex() if block else None

    async def get_transfer_logs(
        self, from_block: int, to_block: int, addresses: Sequence[str] | None = None
    ) -> list[TransferLog]:
        topics: list = [TRANSFER_TOPIC, None]
        if addresses:
            topics.append([_address_topic(a) for a in addresses])
        raw = await self._call(
            "get_logs",
            self._w3.eth.get_logs(
                {
                    "fromBlock": from_block,
                    "toBlock": to_block,
                    "address": self._token_address,
                    "topics": topics,
                }
            ),
        )
        out: list[TransferLog] = []
        for entry in raw:
            out.append(
                TransferLog(
                    network=self.network,
                    asset=self.asset,
                    tx_hash=entry["transactionHash"].hex(),
                    log_index=int(entry["logIndex"]),
                    block_number=int(entry["blockNumber"]),
                    block_hash=entry["blockHash"].hex(),
                    from_address=_topic_to_address(entry["topics"][1]),
                    to_address=_topic_to_address(entry["topics"][2]),
                    amount=int(entry["data"], 16) if isinstance(entry["data"], str)
                    else int.from_bytes(entry["data"], "big"),
                )
            )
        return out

    async def get_receipt(self, tx_hash: str) -> TxReceipt | None:
        try:
            receipt = await self._call(
                "get_transaction_receipt", self._w3.eth.get_transaction_receipt(tx_hash)
            )
        except Exception as exc:
            if "not found" in str(exc).lower() or type(exc).__name__ == "TransactionNotFound":
                return None  # still pending, or evicted by a reorg
            raise RpcUnavailableError(str(exc)) from exc
        if receipt is None or receipt.get("blockNumber") is None:
            return None
        return TxReceipt(
            tx_hash=tx_hash,
            block_number=int(receipt["blockNumber"]),
            block_hash=receipt["blockHash"].hex(),
            status=int(receipt.get("status", 1)),
        )

    async def get_token_balance(self, address: str) -> int:
        return int(
            await self._call(
                "balanceOf",
                self._contract.functions.balanceOf(
                    self._w3.to_checksum_address(address)
                ).call(),
            )
        )

    # -- write -------------------------------------------------------------

    async def send_transfer(
        self, *, from_address: str, to_address: str, amount: int, client_ref: str
    ) -> str:
        from eth_account import Account

        sender = self._w3.to_checksum_address(from_address)
        recipient = self._w3.to_checksum_address(to_address)
        nonce = await self._nonce_allocator(sender, client_ref)
        private_key = await self._key_resolver(sender)

        base_fee = await self._w3.eth.gas_price
        tx = await self._contract.functions.transfer(recipient, amount).build_transaction(
            {
                "chainId": self._chain_id,
                "from": sender,
                "nonce": nonce,
                "gas": self._gas_limit,
                "maxFeePerGas": int(base_fee * 2),
                "maxPriorityFeePerGas": int(base_fee),
            }
        )
        signed = Account.sign_transaction(tx, private_key)
        # The hash is fixed by the signed payload: computing it *before* sending
        # means an ambiguous send still leaves us able to look the tx up.
        tx_hash = signed.hash.hex()
        try:
            await self._call(
                "send_raw_transaction", self._w3.eth.send_raw_transaction(signed.raw_transaction)
            )
        except Exception as exc:
            message = str(exc).lower()
            if "already known" in message or "nonce too low" in message:
                # Previously broadcast; the deterministic hash is still correct.
                log.info("evm.send_already_known", tx_hash=tx_hash, client_ref=client_ref)
                return tx_hash
            if "insufficient" in message or "revert" in message:
                raise TransactionRejectedError(str(exc)) from exc
            raise RpcUnavailableError(str(exc)) from exc
        return tx_hash

    async def _call(self, method: str, awaitable):
        try:
            result = await awaitable
        except Exception:
            CHAIN_RPC_CALLS.labels(method=method, outcome="error").inc()
            raise
        CHAIN_RPC_CALLS.labels(method=method, outcome="ok").inc()
        return result


def _address_topic(address: str) -> str:
    return "0x" + address.lower().removeprefix("0x").rjust(64, "0")


def _topic_to_address(topic) -> str:
    raw = topic.hex() if hasattr(topic, "hex") else str(topic)
    return "0x" + raw.removeprefix("0x")[-40:]
