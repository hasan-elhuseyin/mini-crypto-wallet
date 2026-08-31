from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AddressRequest(BaseModel):
    owner_ref: str = Field(
        min_length=3, max_length=128, examples=["user:1"],
        description="Opaque owner reference from the calling service. Unique -> idempotent.",
    )
    network: str = Field(default="BSC", examples=["BSC"])


class AddressResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    owner_ref: str
    address: str
    network: str
    status: str
    created_at: datetime
    # Note: there is deliberately no private key field anywhere in this schema.


class BalanceResponse(BaseModel):
    address: str
    asset: str
    amount: str
    amount_units: str


class DepositResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    network: str
    asset: str
    tx_hash: str
    log_index: int
    to_address: str
    amount: str
    amount_units: str
    block_number: int
    confirmations: int
    status: str
    first_seen_at: datetime
    confirmed_at: datetime | None = None


class OutgoingTransactionResponse(BaseModel):
    transfer_id: str
    network: str
    asset: str
    from_address: str
    to_address: str
    amount: str
    amount_units: str
    tx_hash: str | None
    status: str
    confirmations: int
    block_number: int | None
    failure_reason: str | None
    attempts: int
    created_at: datetime


class SimulateDepositRequest(BaseModel):
    to_address: str = Field(examples=["0x1234..."])
    amount: str = Field(examples=["1000.000000"], description="Decimal string; never a float.")
    asset: str = Field(default="USDT")
    reference: str | None = Field(
        default=None,
        description="Client reference. Reusing it re-submits the *same* on-chain "
                    "transaction instead of creating a second deposit.",
    )


class SimulateDepositResponse(BaseModel):
    tx_hash: str
    to_address: str
    amount: str
    amount_units: str
    asset: str


class MineRequest(BaseModel):
    blocks: int = Field(default=1, ge=1, le=100)


class ReorgRequest(BaseModel):
    depth: int = Field(default=2, ge=1, le=50)
    drop_tx_hashes: list[str] = Field(
        default_factory=list,
        description="Transactions that must NOT survive the reorg (they vanish).",
    )


class FaultRequest(BaseModel):
    rpc_available: bool | None = None
    halt_mining: bool | None = Field(
        default=None, description="Blocks keep being produced but nothing gets mined."
    )
    fail_next_transfers: int | None = Field(
        default=None, ge=0, description="Next N transfers revert on chain (receipt status 0)."
    )


class ChainStateResponse(BaseModel):
    backend: str
    network: str
    head_block: int | None
    confirmations_required: int
    faults: dict[str, Any] | None = None
