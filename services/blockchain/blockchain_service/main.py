"""blockchain-service API process."""

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

from .api.routes import build_router, build_simulation_router
from .config import get_settings
from .context import Context

settings = get_settings()
configure_logging(settings.service_name, settings.log_level, json_output=settings.log_json)
log = get_logger("main")

DESCRIPTION = """
Owns everything that touches the chain: address custody, deposit detection,
confirmation tracking, transaction broadcasting and reorg handling.

It never holds user balances -- those belong to wallet-service. Communication
is over events (`deposit.*`, `blockchain.transaction.*`) with the single
exception of address creation, which is a synchronous idempotent call.
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
        title="Mini Crypto Wallet - Blockchain Service",
        version=settings.version,
        description=DESCRIPTION,
        lifespan=lifespan,
        openapi_tags=[
            {"name": "addresses", "description": "Custody address issuance and lookup."},
            {"name": "deposits", "description": "Detected inbound transfers."},
            {"name": "transactions", "description": "Outgoing on-chain transactions."},
            {"name": "chain", "description": "Chain backend state."},
            {"name": "simulation", "description": "Drive the simulated chain (non-production)."},
            {"name": "ops", "description": "Health and metrics."},
        ],
    )
    app.add_middleware(CorrelationMiddleware, service=settings.service_name)
    install_error_handlers(app)

    auth = api_key_auth(
        keys=[settings.internal_api_key],
        header="X-Internal-Key",
        enabled=settings.auth_enabled,
        description=(
            "Service-to-service key. Click **Authorize**, paste the key and every "
            "request from this page will carry it.\n\n"
            "Development default: `dev-internal-key-change-me` "
            "(set `INTERNAL_API_KEY` in `.env`)."
        ),
    )

    app.include_router(
        build_ops_router(
            service=settings.service_name,
            version=settings.version,
            readiness_checks={
                "database": lambda: app.state.ctx.check_database(),
                "redis": lambda: app.state.ctx.check_redis(),
                "chain": lambda: app.state.ctx.check_chain(),
            },
        )
    )
    app.include_router(build_router(auth=auth))
    if settings.enable_simulation_api:
        app.include_router(build_simulation_router(auth=auth))
    else:
        log.info("api.simulation_disabled")
    return app


app = create_app()
