"""wallet-service public API."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, Query, Request, Response, status
from mcw_common.assets import UnsupportedAssetError, get_asset
from mcw_common.errors import NotFoundError, ValidationError
from mcw_common.money import AmountError, format_amount, parse_amount
from sqlalchemy import select

from ..models import LedgerEntry, Transfer, TransferStatus, Wallet
from ..schemas import (
    BalanceResponse,
    CreateTransferRequest,
    CreateUserRequest,
    CreateWalletRequest,
    LedgerEntryResponse,
    ReconciliationResponse,
    TransactionsResponse,
    TransferResponse,
    UserResponse,
    WalletResponse,
)
from ..services import idempotency, users
from ..services.ledger import read_balance, reconcile
from ..services.transfers import create_transfer

TRANSFER_SCOPE = "transfers"


def _ctx(request: Request) -> Any:
    return request.app.state.ctx


def _wallet_response(wallet: Wallet) -> WalletResponse:
    return WalletResponse(
        id=str(wallet.id), user_id=wallet.user_id, network=wallet.network,
        asset=wallet.asset, address=wallet.address, status=wallet.status,
        created_at=wallet.created_at,
    )


def _transfer_response(transfer: Transfer) -> TransferResponse:
    decimals = get_asset(transfer.asset).decimals
    return TransferResponse(
        id=str(transfer.id),
        idempotency_key=transfer.idempotency_key,
        from_user_id=transfer.from_user_id,
        to_user_id=transfer.to_user_id,
        asset=transfer.asset,
        network=transfer.network,
        amount=format_amount(transfer.amount, decimals),
        status=transfer.status,
        tx_hash=transfer.tx_hash,
        failure_code=transfer.failure_code,
        failure_reason=transfer.failure_reason,
        correlation_id=transfer.correlation_id,
        created_at=transfer.created_at,
        updated_at=transfer.updated_at,
        settled_at=transfer.settled_at,
    )


def _resolve_asset(symbol: str):
    try:
        return get_asset(symbol)
    except UnsupportedAssetError as exc:
        raise ValidationError(
            f"Asset '{symbol}' is not supported.",
            errors=[{"field": "asset", "message": "unsupported asset"}],
        ) from exc


def build_router(*, auth) -> APIRouter:
    router = APIRouter(dependencies=[Depends(auth)])

    # -- users ------------------------------------------------------------

    @router.post(
        "/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED,
        tags=["users"], summary="Create a user",
    )
    async def create_user(body: CreateUserRequest, request: Request) -> UserResponse:
        ctx = _ctx(request)
        async with ctx.db.sessionmaker() as session, session.begin():
            user = await users.create_user(session, name=body.name, email=str(body.email))
            return UserResponse.model_validate(user, from_attributes=True)

    @router.get("/users/{user_id}", response_model=UserResponse, tags=["users"],
                summary="Fetch a user")
    async def get_user(user_id: int, request: Request) -> UserResponse:
        ctx = _ctx(request)
        async with ctx.db.sessionmaker() as session:
            user = await users.get_user(session, user_id)
            return UserResponse.model_validate(user, from_attributes=True)

    # -- wallets ----------------------------------------------------------

    @router.post(
        "/users/{user_id}/wallet", response_model=WalletResponse,
        status_code=status.HTTP_201_CREATED, tags=["wallets"],
        summary="Create the user's blockchain wallet",
        description=(
            "Asks blockchain-service for a custody address and records it. "
            "Idempotent: calling it again returns the existing wallet with 200."
        ),
    )
    async def create_wallet(
        user_id: int, body: CreateWalletRequest, request: Request, response: Response
    ) -> WalletResponse:
        ctx = _ctx(request)
        network = body.network or ctx.settings.default_network
        asset = _resolve_asset(body.asset or ctx.settings.default_asset)

        async with ctx.db.sessionmaker() as session:
            await users.get_user(session, user_id)
            existing = await users.get_wallet(
                session, user_id=user_id, network=network, asset=asset.symbol
            )
        if existing is not None:
            response.status_code = status.HTTP_200_OK
            return _wallet_response(existing)

        # Idempotent upstream call: a retry returns the same address, so a lost
        # response can never strand an orphaned custody key.
        issued = await ctx.blockchain.create_address(
            owner_ref=f"user:{user_id}", network=network
        )
        async with ctx.db.sessionmaker() as session, session.begin():
            wallet, created = await users.attach_wallet(
                session, user_id=user_id, network=network, asset=asset.symbol,
                address=issued["address"],
            )
            payload = _wallet_response(wallet)
        if not created:
            response.status_code = status.HTTP_200_OK
        return payload

    @router.get(
        "/users/{user_id}/wallet", response_model=WalletResponse, tags=["wallets"],
        summary="Fetch the user's wallet",
    )
    async def get_wallet(
        user_id: int, request: Request,
        network: str | None = None, asset: str | None = None,
    ) -> WalletResponse:
        ctx = _ctx(request)
        resolved_network = network or ctx.settings.default_network
        resolved_asset = _resolve_asset(asset or ctx.settings.default_asset)
        async with ctx.db.sessionmaker() as session:
            await users.get_user(session, user_id)
            wallet = await users.get_wallet(
                session, user_id=user_id, network=resolved_network,
                asset=resolved_asset.symbol,
            )
        if wallet is None:
            raise NotFoundError(
                f"User {user_id} has no {resolved_asset.symbol} wallet on {resolved_network}."
            )
        return _wallet_response(wallet)

    # -- balances ---------------------------------------------------------

    @router.get(
        "/users/{user_id}/balance", response_model=BalanceResponse, tags=["balances"],
        summary="Current balance",
        description="`available` = `posted` - `reserved`. Only `available` is spendable.",
    )
    async def get_balance(
        user_id: int, request: Request, asset: str | None = None
    ) -> BalanceResponse:
        ctx = _ctx(request)
        resolved = _resolve_asset(asset or ctx.settings.default_asset)
        async with ctx.db.sessionmaker() as session:
            await users.get_user(session, user_id)
            balance = await read_balance(session, user_id, resolved.symbol)
        posted = balance.posted if balance else 0
        reserved = balance.reserved if balance else 0
        return BalanceResponse(
            user_id=user_id,
            asset=resolved.symbol,
            posted=format_amount(posted, resolved.decimals),
            reserved=format_amount(reserved, resolved.decimals),
            available=format_amount(posted - reserved, resolved.decimals),
            decimals=resolved.decimals,
            updated_at=balance.updated_at if balance else None,
        )

    # -- transfers --------------------------------------------------------

    @router.post(
        "/transfers", response_model=TransferResponse,
        status_code=status.HTTP_202_ACCEPTED, tags=["transfers"],
        summary="Request a transfer between two users",
        description=(
            "Accepted synchronously: the funds are held and the on-chain request "
            "is queued. Settlement is asynchronous -- poll `GET /transfers/{id}` "
            "or watch for status `CONFIRMED`.\n\n"
            "`idempotency_key` is mandatory. Replaying a key returns the original "
            "transfer (with `Idempotent-Replay: true`), never a second one."
        ),
        responses={
            409: {"description": "Insufficient funds, or idempotency key reused "
                                 "with a different body."},
            422: {"description": "Validation failed."},
        },
    )
    async def post_transfer(
        body: CreateTransferRequest, request: Request, response: Response
    ) -> TransferResponse:
        ctx = _ctx(request)
        asset = _resolve_asset(body.asset)
        network = body.network or ctx.settings.default_network
        try:
            amount_units = parse_amount(body.amount, asset.decimals)
        except AmountError as exc:
            raise ValidationError(
                str(exc), errors=[{"field": "amount", "message": str(exc)}]
            ) from exc

        request_hash = idempotency.fingerprint(
            {
                "from_user_id": body.from_user_id,
                "to_user_id": body.to_user_id,
                "asset": asset.symbol,
                "network": network,
                "amount_units": str(amount_units),
            }
        )

        # One transaction: idempotency claim, validation, hold, transfer row and
        # the outbox event either all commit or none of them do.
        async with ctx.db.sessionmaker() as session, session.begin():
            replay = await idempotency.claim(
                session, scope=TRANSFER_SCOPE, key=body.idempotency_key,
                request_hash=request_hash,
            )
            if replay is not None:
                response.status_code = replay.response_status
                response.headers["Idempotent-Replay"] = "true"
                return TransferResponse.model_validate(replay.response_body)

            transfer = await create_transfer(
                session,
                from_user_id=body.from_user_id,
                to_user_id=body.to_user_id,
                asset=asset.symbol,
                amount_units=amount_units,
                idempotency_key=body.idempotency_key,
                network=network,
            )
            payload = _transfer_response(transfer)
            await idempotency.complete(
                session,
                scope=TRANSFER_SCOPE,
                key=body.idempotency_key,
                status=status.HTTP_202_ACCEPTED,
                body=payload.model_dump(mode="json"),
                resource_id=str(transfer.id),
            )
        return payload

    @router.get(
        "/transfers/{transfer_id}", response_model=TransferResponse, tags=["transfers"],
        summary="Transfer status",
    )
    async def get_transfer(transfer_id: str, request: Request) -> TransferResponse:
        ctx = _ctx(request)
        try:
            parsed = uuid.UUID(transfer_id)
        except ValueError as exc:
            raise ValidationError("transfer_id must be a UUID.") from exc
        async with ctx.db.sessionmaker() as session:
            transfer = await session.get(Transfer, parsed)
        if transfer is None:
            raise NotFoundError(f"Transfer {transfer_id} does not exist.")
        return _transfer_response(transfer)

    # -- history ----------------------------------------------------------

    @router.get(
        "/users/{user_id}/transactions", response_model=TransactionsResponse,
        tags=["transactions"], summary="Ledger history",
        description="Append-only ledger entries, newest first, plus any transfers "
                    "that are still in flight.",
    )
    async def get_transactions(
        user_id: int,
        request: Request,
        asset: str | None = None,
        limit: int = Query(default=50, ge=1, le=200),
        cursor: int | None = Query(default=None, description="Return entries with id < cursor."),
    ) -> TransactionsResponse:
        ctx = _ctx(request)
        resolved = _resolve_asset(asset or ctx.settings.default_asset)
        async with ctx.db.sessionmaker() as session:
            await users.get_user(session, user_id)
            stmt = (
                select(LedgerEntry)
                .where(LedgerEntry.user_id == user_id, LedgerEntry.asset == resolved.symbol)
                .order_by(LedgerEntry.id.desc())
                .limit(limit)
            )
            if cursor is not None:
                stmt = stmt.where(LedgerEntry.id < cursor)
            entries = (await session.execute(stmt)).scalars().all()
            pending = (
                await session.execute(
                    select(Transfer).where(
                        Transfer.status.in_(TransferStatus.OPEN),
                        (Transfer.from_user_id == user_id) | (Transfer.to_user_id == user_id),
                    ).order_by(Transfer.created_at.desc()).limit(50)
                )
            ).scalars().all()

        return TransactionsResponse(
            user_id=user_id,
            asset=resolved.symbol,
            items=[
                LedgerEntryResponse(
                    id=entry.id,
                    entry_id=str(entry.entry_id),
                    asset=entry.asset,
                    amount=format_amount(entry.amount, resolved.decimals),
                    entry_type=entry.entry_type,
                    reference_type=entry.reference_type,
                    reference_id=entry.reference_id,
                    correlation_id=entry.correlation_id,
                    created_at=entry.created_at,
                )
                for entry in entries
            ],
            pending_transfers=[_transfer_response(t) for t in pending],
            next_cursor=entries[-1].id if len(entries) == limit else None,
        )

    # -- ops --------------------------------------------------------------

    @router.get(
        "/admin/reconciliation", response_model=ReconciliationResponse, tags=["admin"],
        summary="Balance snapshots vs. the ledger",
        description="Recomputes every balance from the ledger and reports drift. "
                    "In production this runs on a schedule and alerts on any row.",
    )
    async def reconciliation(request: Request) -> ReconciliationResponse:
        ctx = _ctx(request)
        async with ctx.db.sessionmaker() as session:
            rows = await reconcile(session)
        return ReconciliationResponse(
            checked=len(rows),
            inconsistent=sum(1 for row in rows if not row["consistent"]),
            rows=rows,
        )

    return router
