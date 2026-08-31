"""blockchain-service schema (its own database; no cross-service joins)."""

from __future__ import annotations

import uuid
from datetime import datetime

from mcw_common.db import SmallestUnit
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

MOCK_SCHEMA = "mockchain"


class Base(DeclarativeBase):
    pass


def _uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


class DepositStatus:
    DETECTED = "DETECTED"
    CONFIRMED = "CONFIRMED"
    REORGED = "REORGED"


class TxStatus:
    """Lifecycle of an outgoing on-chain transaction."""

    CREATED = "CREATED"
    PENDING = "PENDING"          # signed, waiting to be broadcast / retried
    BROADCASTED = "BROADCASTED"  # accepted by the node, not yet mined
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"
    REORGED = "REORGED"


class Address(Base):
    """A deposit/custody address owned by exactly one wallet-service subject."""

    __tablename__ = "addresses"

    id: Mapped[uuid.UUID] = _uuid_pk()
    #: Opaque reference supplied by wallet-service, e.g. "user:1". Unique, so
    #: address creation is idempotent without an idempotency key.
    owner_ref: Mapped[str] = mapped_column(String(128), unique=True)
    network: Mapped[str] = mapped_column(String(32))
    address: Mapped[str] = mapped_column(String(64), unique=True)
    derivation: Mapped[str] = mapped_column(String(32), default="random-keypair")
    status: Mapped[str] = mapped_column(String(16), default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (Index("ix_addresses_address_lower", func.lower(address)),)


class KeyMaterial(Base):
    """Encrypted private keys, isolated in their own table.

    Never selected by any read path that serves an API response; the only
    consumer is the signer. See README -> Security Considerations.
    """

    __tablename__ = "key_material"

    address: Mapped[str] = mapped_column(
        String(64), ForeignKey("addresses.address", ondelete="RESTRICT"), primary_key=True
    )
    encrypted_private_key: Mapped[bytes] = mapped_column(LargeBinary)
    key_version: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Deposit(Base):
    __tablename__ = "deposits"

    id: Mapped[uuid.UUID] = _uuid_pk()
    network: Mapped[str] = mapped_column(String(32))
    asset: Mapped[str] = mapped_column(String(16))
    tx_hash: Mapped[str] = mapped_column(String(80))
    log_index: Mapped[int] = mapped_column(Integer)
    to_address: Mapped[str] = mapped_column(String(64))
    from_address: Mapped[str | None] = mapped_column(String(64), nullable=True)
    amount: Mapped[int] = mapped_column(SmallestUnit)
    block_number: Mapped[int] = mapped_column(BigInteger)
    block_hash: Mapped[str] = mapped_column(String(80))
    confirmations: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(16), default=DepositStatus.DETECTED)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reorged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # The rule that makes deposit ingestion idempotent at the storage layer.
        UniqueConstraint("network", "tx_hash", "log_index", name="uq_deposit_chain_identity"),
        CheckConstraint("amount > 0", name="ck_deposit_amount_positive"),
        Index("ix_deposits_status", "status"),
        Index("ix_deposits_to_address", "to_address"),
    )


class OutgoingTransaction(Base):
    __tablename__ = "outgoing_transactions"

    id: Mapped[uuid.UUID] = _uuid_pk()
    #: One on-chain transaction per wallet-service transfer. Unique -> the
    #: consumer of `transfer.requested` is idempotent by construction.
    transfer_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), unique=True)
    network: Mapped[str] = mapped_column(String(32))
    asset: Mapped[str] = mapped_column(String(16))
    from_address: Mapped[str] = mapped_column(String(64))
    to_address: Mapped[str] = mapped_column(String(64))
    amount: Mapped[int] = mapped_column(SmallestUnit)
    tx_hash: Mapped[str | None] = mapped_column(String(80), unique=True, nullable=True)
    nonce: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default=TxStatus.CREATED)
    block_number: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    block_hash: Mapped[str | None] = mapped_column(String(80), nullable=True)
    confirmations: Mapped[int] = mapped_column(Integer, default=0)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    broadcast_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_outgoing_amount_positive"),
        Index("ix_outgoing_status", "status"),
    )


class ScanState(Base):
    """Cursor for the deposit scanner (one row per network)."""

    __tablename__ = "scan_state"

    network: Mapped[str] = mapped_column(String(32), primary_key=True)
    last_scanned_block: Mapped[int] = mapped_column(BigInteger, default=0)
    last_scanned_hash: Mapped[str | None] = mapped_column(String(80), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Outbox(Base):
    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), unique=True)
    event_type: Mapped[str] = mapped_column(String(64))
    envelope: Mapped[dict] = mapped_column(JSONB)
    correlation_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_outbox_unpublished", "id", postgresql_where=published_at.is_(None)),
    )


class ProcessedEvent(Base):
    """Consumer-side dedupe table: written in the same tx as the side effects."""

    __tablename__ = "processed_events"

    consumer: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64))
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class DeadLetter(Base):
    __tablename__ = "dead_letters"

    id: Mapped[uuid.UUID] = _uuid_pk()
    consumer: Mapped[str] = mapped_column(String(64))
    event_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True))
    event_type: Mapped[str] = mapped_column(String(64))
    envelope: Mapped[dict] = mapped_column(JSONB)
    delivery_count: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


# ---------------------------------------------------------------------------
# Simulated chain. Lives in its own schema because it models the *node*, not
# the service: swapping CHAIN_BACKEND=web3 makes these tables unused.
# ---------------------------------------------------------------------------


class MockBlock(Base):
    """A block, keyed by **hash**, not height.

    Two blocks can share a height -- that is exactly what a reorg produces.
    A partial unique index (see the migration) enforces the real invariant:
    at most one *canonical* block per height.
    """

    __tablename__ = "blocks"
    __table_args__ = {"schema": MOCK_SCHEMA}

    hash: Mapped[str] = mapped_column(String(80), primary_key=True)
    number: Mapped[int] = mapped_column(BigInteger, index=True)
    parent_hash: Mapped[str] = mapped_column(String(80))
    is_canonical: Mapped[bool] = mapped_column(Boolean, default=True)
    mined_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class MockTx(Base):
    __tablename__ = "transactions"
    __table_args__ = {"schema": MOCK_SCHEMA}

    hash: Mapped[str] = mapped_column(String(80), primary_key=True)
    from_address: Mapped[str] = mapped_column(String(64))
    to_address: Mapped[str] = mapped_column(String(64))
    amount: Mapped[int] = mapped_column(SmallestUnit)
    asset: Mapped[str] = mapped_column(String(16), default="USDT")
    #: PENDING -> MINED | FAILED ; DROPPED when evicted by a reorg.
    status: Mapped[str] = mapped_column(String(16), default="PENDING")
    block_number: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    block_hash: Mapped[str | None] = mapped_column(String(80), nullable=True)
    log_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_mint: Mapped[bool] = mapped_column(Boolean, default=False)
    fail_on_mine: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class MockBalance(Base):
    __tablename__ = "token_balances"
    __table_args__ = {"schema": MOCK_SCHEMA}

    address: Mapped[str] = mapped_column(String(64), primary_key=True)
    amount: Mapped[int] = mapped_column(SmallestUnit, default=0)


class MockFaults(Base):
    """Fault injection switches for exercising failure scenarios in tests/demo."""

    __tablename__ = "faults"
    __table_args__ = {"schema": MOCK_SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    rpc_available: Mapped[bool] = mapped_column(Boolean, default=True)
    halt_mining: Mapped[bool] = mapped_column(Boolean, default=False)
    #: Number of upcoming transfers that must be mined with receipt status 0.
    fail_next_transfers: Mapped[int] = mapped_column(Integer, default=0)
