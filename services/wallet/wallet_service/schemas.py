"""Request/response models.

Every monetary value crosses the API boundary as a **decimal string**. JSON
numbers are IEEE-754 doubles in most clients; `250.000000` parsed as a float is
already a rounding bug waiting to happen.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

#: Positive decimal, no sign, no exponent. Precision per asset is checked later.
AMOUNT_PATTERN = r"^\d{1,30}(\.\d{1,18})?$"


class CreateUserRequest(BaseModel):
    name: str = Field(min_length=1, max_length=128, examples=["User A"])
    email: EmailStr = Field(examples=["user-a@example.com"])


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    email: str
    status: str
    created_at: datetime


class CreateWalletRequest(BaseModel):
    network: str | None = Field(default=None, examples=["BSC"])
    asset: str | None = Field(default=None, examples=["USDT"])


class WalletResponse(BaseModel):
    id: str
    user_id: int
    network: str
    asset: str
    address: str
    status: str
    created_at: datetime


class BalanceResponse(BaseModel):
    user_id: int
    asset: str
    #: Sum of the ledger.
    posted: str
    #: Held for in-flight transfers.
    reserved: str
    #: What the user can actually spend: posted - reserved.
    available: str
    decimals: int
    updated_at: datetime | None = None


class CreateTransferRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "from_user_id": 1,
            "to_user_id": 2,
            "asset": "USDT",
            "amount": "250.000000",
            "idempotency_key": "transfer-001",
        }
    })

    from_user_id: int = Field(ge=1)
    to_user_id: int = Field(ge=1)
    asset: str = Field(default="USDT", max_length=16)
    amount: str = Field(pattern=AMOUNT_PATTERN, examples=["250.000000"])
    idempotency_key: str = Field(
        min_length=4, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$",
        examples=["transfer-001"],
        description="Replaying this key returns the original result instead of "
                    "creating a second transfer.",
    )
    network: str | None = None

    @field_validator("amount")
    @classmethod
    def _reject_zero(cls, value: str) -> str:
        if float(value) == 0:  # pattern already guarantees a plain decimal
            raise ValueError("amount must be greater than zero")
        return value


class TransferResponse(BaseModel):
    id: str
    idempotency_key: str
    from_user_id: int
    to_user_id: int
    asset: str
    network: str
    amount: str
    status: str
    tx_hash: str | None = None
    failure_code: str | None = None
    failure_reason: str | None = None
    correlation_id: str | None = None
    created_at: datetime
    updated_at: datetime
    settled_at: datetime | None = None


class LedgerEntryResponse(BaseModel):
    id: int
    entry_id: str
    asset: str
    amount: str
    entry_type: str
    reference_type: str
    reference_id: str
    correlation_id: str | None = None
    created_at: datetime


class TransactionsResponse(BaseModel):
    user_id: int
    asset: str
    items: list[LedgerEntryResponse]
    #: Transfers that are accepted but not settled yet: they hold funds but have
    #: no ledger entries, so they would otherwise be invisible here.
    pending_transfers: list[TransferResponse]
    next_cursor: int | None = None


class ReconciliationResponse(BaseModel):
    checked: int
    inconsistent: int
    rows: list[dict]
