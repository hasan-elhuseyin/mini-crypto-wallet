"""wallet-service schema.

Balance model: **hybrid**.

* ``ledger_entries`` is the source of truth -- append-only, signed amounts, one
  row per financial movement, with an idempotency key built into a unique
  constraint. It is what an auditor reads.
* ``balances`` is a derived snapshot (``posted`` = sum of the ledger, plus
  ``reserved`` for in-flight holds). It exists for two reasons: O(1) reads, and
  -- more importantly -- it gives concurrency control a single row to lock.

The two are reconciled by ``GET /admin/reconciliation``; a mismatch is a bug
and is reported rather than silently repaired.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from mcw_common.db import SmallestUnit
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class EntryType:
    DEPOSIT = "DEPOSIT"
    TRANSFER_DEBIT = "TRANSFER_DEBIT"
    TRANSFER_CREDIT = "TRANSFER_CREDIT"
    FEE = "FEE"
    REVERSAL = "REVERSAL"


class TransferStatus:
    CREATED = "CREATED"          # accepted, funds held, event queued
    PROCESSING = "PROCESSING"    # request handed to blockchain-service
    BROADCASTED = "BROADCASTED"  # on chain, not yet confirmed
    CONFIRMED = "CONFIRMED"      # settled: ledger postings written
    FAILED = "FAILED"            # hold released (or postings reversed)

    OPEN = (CREATED, PROCESSING, BROADCASTED)
    TERMINAL = (CONFIRMED, FAILED)


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128))
    email: Mapped[str] = mapped_column(String(255), unique=True)
    status: Mapped[str] = mapped_column(String(16), default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Wallet(Base):
    __tablename__ = "wallets"

    id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="RESTRICT"), index=True
    )
    network: Mapped[str] = mapped_column(String(32))
    asset: Mapped[str] = mapped_column(String(16))
    address: Mapped[str] = mapped_column(String(64), unique=True)
    status: Mapped[str] = mapped_column(String(16), default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("user_id", "network", "asset", name="uq_wallet_user_network_asset"),
    )


class Balance(Base):
    """Derived snapshot. ``available = posted - reserved``."""

    __tablename__ = "balances"

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="RESTRICT"), primary_key=True
    )
    asset: Mapped[str] = mapped_column(String(16), primary_key=True)
    #: Sum of all ledger entries for (user, asset).
    posted: Mapped[int] = mapped_column(SmallestUnit, default=0)
    #: Funds committed to in-flight transfers; spendable balance excludes these.
    reserved: Mapped[int] = mapped_column(SmallestUnit, default=0)
    version: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        CheckConstraint("reserved >= 0", name="ck_balance_reserved_non_negative"),
        # The invariant that makes double spending impossible even if every
        # application-level check were removed.
        CheckConstraint("posted - reserved >= 0", name="ck_balance_available_non_negative"),
    )


class LedgerEntry(Base):
    """Append-only financial movement. Never updated, never deleted."""

    __tablename__ = "ledger_entries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    entry_id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True), default=uuid.uuid4, unique=True
    )
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"), index=True)
    asset: Mapped[str] = mapped_column(String(16))
    #: Signed: credits positive, debits negative.
    amount: Mapped[int] = mapped_column(SmallestUnit)
    entry_type: Mapped[str] = mapped_column(String(24))
    reference_type: Mapped[str] = mapped_column(String(24))
    #: Natural key of the thing that caused the movement (deposit identity,
    #: transfer id, ...). Together with the columns below it is what makes a
    #: replayed event a no-op instead of a double posting.
    reference_id: Mapped[str] = mapped_column(String(200))
    correlation_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "user_id", "entry_type", "reference_type", "reference_id",
            name="uq_ledger_idempotency",
        ),
        CheckConstraint("amount <> 0", name="ck_ledger_amount_non_zero"),
        Index("ix_ledger_user_created", "user_id", "created_at"),
    )


class Transfer(Base):
    __tablename__ = "transfers"

    id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True)
    from_user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"), index=True)
    to_user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"), index=True)
    asset: Mapped[str] = mapped_column(String(16))
    network: Mapped[str] = mapped_column(String(32))
    amount: Mapped[int] = mapped_column(SmallestUnit)
    from_address: Mapped[str] = mapped_column(String(64))
    to_address: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default=TransferStatus.CREATED)
    tx_hash: Mapped[str | None] = mapped_column(String(80), nullable=True)
    failure_code: Mapped[str | None] = mapped_column(String(48), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_transfer_amount_positive"),
        CheckConstraint("from_user_id <> to_user_id", name="ck_transfer_distinct_parties"),
        Index("ix_transfers_status", "status"),
    )


class IdempotencyKey(Base):
    """API-level idempotency record.

    Claimed and completed inside the *same* transaction as the work it guards,
    so there is no "in progress" window to reap and a crash releases the key
    automatically.
    """

    __tablename__ = "idempotency_keys"

    scope: Mapped[str] = mapped_column(String(64), primary_key=True)
    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    #: Hash of the canonicalised request body; a different body under the same
    #: key is a client bug and gets a 409 rather than a wrong replay.
    request_hash: Mapped[str] = mapped_column(String(64))
    response_status: Mapped[int] = mapped_column(Integer)
    response_body: Mapped[dict] = mapped_column(JSONB)
    resource_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
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
    __tablename__ = "processed_events"

    consumer: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64))
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class DeadLetter(Base):
    __tablename__ = "dead_letters"

    id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    consumer: Mapped[str] = mapped_column(String(64))
    event_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True))
    event_type: Mapped[str] = mapped_column(String(64))
    envelope: Mapped[dict] = mapped_column(JSONB)
    delivery_count: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
