"""wallet-service API process."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from mcw_common.http import (
    CorrelationMiddleware,
    api_key_auth,
    build_ops_router,
    install_error_handlers,
)
from mcw_common.logging import configure_logging, get_logger

from .api.routes import build_router
from .config import get_settings
from .context import Context

settings = get_settings()
configure_logging(settings.service_name, settings.log_level, json_output=settings.log_json)
log = get_logger("main")

DESCRIPTION = """
Owns users, wallets, the ledger and balances. It is the only service that knows
what anyone is owed.

It never talks to a blockchain. Deposits arrive as `deposit.confirmed` events;
transfers are published as `transfer.requested` and settled when
`blockchain.transaction.confirmed` comes back.

**Authentication** -- every endpoint requires `X-API-Key` (or
`Authorization: Bearer <key>`). This stands in for real OAuth2/JWT; see the
README.

**Money format** -- all amounts are decimal strings (`"250.000000"`), never JSON
numbers.
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.ctx = await Context.create(settings)
    log.info("api.started", service=settings.service_name, version=settings.version)
    try:
        yield
    finally:
        await app.state.ctx.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Mini Crypto Wallet - Wallet Service",
        version=settings.version,
        description=DESCRIPTION,
        lifespan=lifespan,
        openapi_tags=[
            {"name": "users", "description": "User management."},
            {"name": "wallets", "description": "Wallet/address assignment."},
            {"name": "balances", "description": "Posted, reserved and available balances."},
            {"name": "transfers", "description": "User-to-user transfers, settled on chain."},
            {"name": "transactions", "description": "Append-only ledger history."},
            {"name": "admin", "description": "Reconciliation and operational views."},
            {"name": "ops", "description": "Health and metrics."},
        ],
    )
    app.add_middleware(CorrelationMiddleware, service=settings.service_name)
    install_error_handlers(app)

    auth = api_key_auth(
        keys=[settings.api_key],
        header="X-API-Key",
        enabled=settings.auth_enabled,
        description=(
            "Client API key. Click **Authorize**, paste the key and every request "
            "from this page will carry it.\n\n"
            "Development default: `dev-api-key-change-me` (set `API_KEY` in `.env`)."
        ),
    )

    app.include_router(
        build_ops_router(
            service=settings.service_name,
            version=settings.version,
            readiness_checks={
                "database": lambda: app.state.ctx.check_database(),
                "redis": lambda: app.state.ctx.check_redis(),
                "blockchain_service": lambda: app.state.ctx.check_blockchain_service(),
            },
        )
    )
    app.include_router(build_router(auth=auth))
    return app


app = create_app()
