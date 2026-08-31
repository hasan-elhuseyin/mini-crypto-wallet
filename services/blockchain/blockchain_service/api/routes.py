"""blockchain-service HTTP API.

This API is **internal**: it is called by wallet-service and by operators, not
by end users. It is authenticated with a separate internal credential.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, Query, Request, Response, status
from mcw_common.assets import UnsupportedAssetError, get_asset
from mcw_common.errors import NotFoundError, ValidationError
from mcw_common.money import AmountError, format_amount, parse_amount
from sqlalchemy import select

from ..models import Address, Deposit, OutgoingTransaction
from ..schemas import (
    AddressRequest,
    AddressResponse,
    BalanceResponse,
    ChainStateResponse,
    DepositResponse,
    FaultRequest,
    MineRequest,
    OutgoingTransactionResponse,
    ReorgRequest,
    SimulateDepositRequest,
    SimulateDepositResponse,
)
from ..services.addresses import get_or_create_address


def _ctx(request: Request) -> Any:
    return request.app.state.ctx


def _deposit_response(deposit: Deposit) -> DepositResponse:
    decimals = get_asset(deposit.asset).decimals
    return DepositResponse(
        id=str(deposit.id),
        network=deposit.network,
        asset=deposit.asset,
        tx_hash=deposit.tx_hash,
        log_index=deposit.log_index,
        to_address=deposit.to_address,
        amount=format_amount(deposit.amount, decimals),
        amount_units=str(deposit.amount),
        block_number=deposit.block_number,
        confirmations=deposit.confirmations,
        status=deposit.status,
        first_seen_at=deposit.first_seen_at,
        confirmed_at=deposit.confirmed_at,
    )


def build_router(*, auth) -> APIRouter:
    router = APIRouter(dependencies=[Depends(auth)])

    # -- addresses --------------------------------------------------------

    @router.post(
        "/addresses",
        response_model=AddressResponse,
        status_code=status.HTTP_201_CREATED,
        tags=["addresses"],
        summary="Create (or return) the custody address for an owner",
        description="Idempotent on `owner_ref`: returns 200 when the address already exists.",
    )
    async def create_address(
        body: AddressRequest, request: Request, response: Response
    ) -> AddressResponse:
        ctx = _ctx(request)
        async with ctx.db.sessionmaker() as session, session.begin():
            address, created = await get_or_create_address(
                session, owner_ref=body.owner_ref, network=body.network,
                keystore=ctx.keystore,
            )
            payload = AddressResponse.model_validate(address, from_attributes=True)
        if not created:
            response.status_code = status.HTTP_200_OK
        return payload

    @router.get(
        "/addresses/{owner_ref}", response_model=AddressResponse, tags=["addresses"],
        summary="Look up an owner's address",
    )
    async def get_address(owner_ref: str, request: Request) -> AddressResponse:
        ctx = _ctx(request)
        async with ctx.db.sessionmaker() as session:
            address = (
                await session.execute(select(Address).where(Address.owner_ref == owner_ref))
            ).scalar_one_or_none()
        if address is None:
            raise NotFoundError(f"No address for owner_ref '{owner_ref}'.")
        return AddressResponse.model_validate(address, from_attributes=True)

    @router.get(
        "/addresses/{address}/onchain-balance", response_model=BalanceResponse,
        tags=["addresses"], summary="Token balance as seen on chain",
    )
    async def onchain_balance(address: str, request: Request) -> BalanceResponse:
        ctx = _ctx(request)
        units = await ctx.adapter.get_token_balance(address)
        asset = get_asset(ctx.adapter.asset)
        return BalanceResponse(
            address=address, asset=asset.symbol,
            amount=format_amount(units, asset.decimals), amount_units=str(units),
        )

    # -- chain state ------------------------------------------------------

    @router.get(
        "/deposits", response_model=list[DepositResponse], tags=["deposits"],
        summary="List detected deposits",
    )
    async def list_deposits(
        request: Request,
        to_address: str | None = None,
        deposit_status: str | None = Query(default=None, alias="status"),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[DepositResponse]:
        ctx = _ctx(request)
        stmt = select(Deposit).order_by(Deposit.first_seen_at.desc()).limit(limit)
        if to_address:
            stmt = stmt.where(Deposit.to_address == to_address)
        if deposit_status:
            stmt = stmt.where(Deposit.status == deposit_status.upper())
        async with ctx.db.sessionmaker() as session:
            rows = (await session.execute(stmt)).scalars().all()
        return [_deposit_response(d) for d in rows]

    @router.get(
        "/transactions/{transfer_id}", response_model=OutgoingTransactionResponse,
        tags=["transactions"], summary="On-chain state of a wallet transfer",
    )
    async def get_transaction(transfer_id: str, request: Request) -> OutgoingTransactionResponse:
        ctx = _ctx(request)
        try:
            parsed = uuid.UUID(transfer_id)
        except ValueError as exc:
            raise ValidationError("transfer_id must be a UUID.") from exc
        async with ctx.db.sessionmaker() as session:
            record = (
                await session.execute(
                    select(OutgoingTransaction).where(
                        OutgoingTransaction.transfer_id == parsed
                    )
                )
            ).scalar_one_or_none()
        if record is None:
            raise NotFoundError(f"No on-chain transaction for transfer '{transfer_id}'.")
        decimals = get_asset(record.asset).decimals
        return OutgoingTransactionResponse(
            transfer_id=str(record.transfer_id),
            network=record.network,
            asset=record.asset,
            from_address=record.from_address,
            to_address=record.to_address,
            amount=format_amount(record.amount, decimals),
            amount_units=str(record.amount),
            tx_hash=record.tx_hash,
            status=record.status,
            confirmations=record.confirmations,
            block_number=record.block_number,
            failure_reason=record.failure_reason,
            attempts=record.attempts,
            created_at=record.created_at,
        )

    @router.get("/chain", response_model=ChainStateResponse, tags=["chain"],
                summary="Chain backend state")
    async def chain_state(request: Request) -> ChainStateResponse:
        ctx = _ctx(request)
        try:
            head = await ctx.adapter.get_block_number()
        except Exception:
            head = None
        return ChainStateResponse(
            backend=ctx.settings.chain_backend,
            network=ctx.settings.network,
            head_block=head,
            confirmations_required=ctx.settings.confirmations_required,
            faults=await ctx.mock_chain.get_faults() if ctx.mock_chain else None,
        )

    return router


def build_simulation_router(*, auth) -> APIRouter:
    """Endpoints that drive the simulated chain.

    Guarded by ``ENABLE_SIMULATION_API`` -- these must never be reachable in a
    real deployment, which is why they live on a separate router that is simply
    not mounted when the flag is off.
    """
    router = APIRouter(prefix="/simulate", tags=["simulation"], dependencies=[Depends(auth)])

    def _require_mock(ctx) -> Any:
        if ctx.mock_chain is None:
            raise ValidationError("Simulation endpoints require CHAIN_BACKEND=mock.")
        return ctx.mock_chain

    @router.post(
        "/deposits", response_model=SimulateDepositResponse,
        status_code=status.HTTP_202_ACCEPTED,
        summary="Simulate an inbound USDT deposit from an external party",
    )
    async def simulate_deposit(
        body: SimulateDepositRequest, request: Request
    ) -> SimulateDepositResponse:
        ctx = _ctx(request)
        chain = _require_mock(ctx)
        try:
            asset = get_asset(body.asset)
            units = parse_amount(body.amount, asset.decimals)
        except UnsupportedAssetError as exc:
            raise ValidationError(f"Asset '{body.asset}' is not supported.") from exc
        except AmountError as exc:
            raise ValidationError(str(exc)) from exc
        reference = body.reference or f"deposit-{uuid.uuid4()}"
        tx_hash = await chain.simulate_deposit(
            to_address=body.to_address, amount=units, client_ref=reference
        )
        return SimulateDepositResponse(
            tx_hash=tx_hash, to_address=body.to_address,
            amount=format_amount(units, asset.decimals), amount_units=str(units),
            asset=asset.symbol,
        )

    @router.post("/mine", summary="Mine N blocks")
    async def mine(body: MineRequest, request: Request) -> dict[str, Any]:
        chain = _require_mock(_ctx(request))
        last = 0
        for _ in range(body.blocks):
            last = await chain.mine_block()
        return {"head_block": last, "mined": body.blocks}

    @router.post("/reorg", summary="Force a chain reorganisation")
    async def reorg(body: ReorgRequest, request: Request) -> dict[str, Any]:
        chain = _require_mock(_ctx(request))
        return await chain.reorg(depth=body.depth, drop_tx_hashes=body.drop_tx_hashes)

    @router.post("/faults", summary="Inject chain faults (RPC down, congestion, reverts)")
    async def faults(body: FaultRequest, request: Request) -> dict[str, Any]:
        chain = _require_mock(_ctx(request))
        return await chain.set_faults(
            rpc_available=body.rpc_available,
            halt_mining=body.halt_mining,
            fail_next_transfers=body.fail_next_transfers,
        )

    @router.post(
        "/tick",
        summary="Run one iteration of every background worker",
        description="Test/demo hook: makes the asynchronous pipeline advance "
                    "deterministically instead of waiting on timers.",
    )
    async def tick(request: Request) -> dict[str, Any]:
        ctx = _ctx(request)
        from ..services.deposits import DepositConfirmationWatcher, DepositScanner
        from ..services.transactions import BroadcastWorker, OutgoingTransactionWatcher

        results = {
            "scanner": await DepositScanner(ctx).run_once(),
            "broadcaster": await BroadcastWorker(ctx).run_once(),
            "deposit_confirmations": await DepositConfirmationWatcher(ctx).run_once(),
            "transaction_confirmations": await OutgoingTransactionWatcher(ctx).run_once(),
        }
        results["published"] = await ctx.relay.run_once()
        return results

    return router
