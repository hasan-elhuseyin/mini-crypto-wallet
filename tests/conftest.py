"""Test fixtures.

Integration tests drive the asynchronous pipeline **deterministically**: rather
than starting background workers and sleeping, they call each worker's
``run_once`` in a loop (see :class:`Pipeline`). The production code path is
identical -- only the scheduler differs -- so the tests exercise the real
consumers, the real outbox relay and the real chain adapter without being
flaky.
"""

from __future__ import annotations

import os
import uuid

import pytest

# Environment must be set before any service module (and its cached Settings)
# is imported.
os.environ.setdefault(
    "WALLET_DATABASE_URL", "postgresql+psycopg://mcw:mcw@localhost:5432/wallet_test"
)
os.environ.setdefault(
    "BLOCKCHAIN_DATABASE_URL", "postgresql+psycopg://mcw:mcw@localhost:5432/blockchain_test"
)
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/1")
os.environ.setdefault("AUTH_ENABLED", "true")
os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("INTERNAL_API_KEY", "test-internal-key")
os.environ.setdefault(
    "KEYSTORE_ENCRYPTION_KEY", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
)
os.environ.setdefault("CHAIN_BACKEND", "mock")
os.environ.setdefault("CONFIRMATIONS_REQUIRED", "3")
os.environ.setdefault("FINALITY_DEPTH", "15")
os.environ.setdefault("MOCK_AUTO_MINE", "false")
os.environ.setdefault("ENABLE_SIMULATION_API", "true")
os.environ.setdefault("LOG_LEVEL", "WARNING")
os.environ.setdefault("BROADCAST_MAX_ATTEMPTS", "3")
os.environ.setdefault("PENDING_TIMEOUT_SECONDS", "1")

API_HEADERS = {"X-API-Key": os.environ["API_KEY"]}
INTERNAL_HEADERS = {"X-Internal-Key": os.environ["INTERNAL_API_KEY"]}

WALLET_TABLES = (
    "dead_letters", "processed_events", "outbox", "idempotency_keys", "transfers",
    "ledger_entries", "balances", "wallets", "users",
)
BLOCKCHAIN_TABLES = (
    "dead_letters", "processed_events", "outbox", "scan_state",
    "outgoing_transactions", "deposits", "key_material", "addresses",
    "mockchain.transactions", "mockchain.blocks", "mockchain.token_balances",
    "mockchain.faults",
)


def _sync_url(url: str) -> str:
    return url.replace("postgresql+psycopg://", "postgresql://")


def _infrastructure_available() -> tuple[bool, str]:
    try:
        import psycopg
        import redis
    except ImportError as exc:  # pragma: no cover
        return False, f"missing driver: {exc}"
    try:
        with psycopg.connect(_sync_url(os.environ["WALLET_DATABASE_URL"]), connect_timeout=2):
            pass
        with psycopg.connect(
            _sync_url(os.environ["BLOCKCHAIN_DATABASE_URL"]), connect_timeout=2
        ):
            pass
        redis.Redis.from_url(os.environ["REDIS_URL"], socket_connect_timeout=2).ping()
    except Exception as exc:
        return False, str(exc)
    return True, ""


INFRA_OK, INFRA_ERROR = _infrastructure_available()

@pytest.fixture(autouse=True)
def skip_without_infrastructure(request):
    """Integration tests are skipped -- loudly -- when postgres/redis are absent."""
    if request.node.get_closest_marker("integration") and not INFRA_OK:
        pytest.skip(
            "needs postgres + redis: run `make test`, or "
            f"`docker compose up -d postgres redis` ({INFRA_ERROR})"
        )


@pytest.fixture(scope="session", autouse=True)
def migrated_databases():
    """Rebuild both test databases by running the real Alembic migrations."""
    if not INFRA_OK:
        yield
        return

    import psycopg
    from alembic import command
    from alembic.config import Config

    for service, url_var in (("wallet", "WALLET_DATABASE_URL"),
                             ("blockchain", "BLOCKCHAIN_DATABASE_URL")):
        with psycopg.connect(_sync_url(os.environ[url_var]), autocommit=True) as conn:
            conn.execute("DROP SCHEMA IF EXISTS mockchain CASCADE")
            conn.execute("DROP SCHEMA public CASCADE")
            conn.execute("CREATE SCHEMA public")
        config = Config()
        config.set_main_option("script_location", f"services/{service}/migrations")
        config.set_main_option("sqlalchemy.url", os.environ[url_var].replace("%", "%%"))
        command.upgrade(config, "head")
    yield


@pytest.fixture(autouse=True)
async def clean_state(migrated_databases):
    """Truncate every table and flush Redis between tests."""
    if not INFRA_OK:
        yield
        return
    import psycopg
    import redis.asyncio as aioredis

    for url_var, tables in (
        ("WALLET_DATABASE_URL", WALLET_TABLES),
        ("BLOCKCHAIN_DATABASE_URL", BLOCKCHAIN_TABLES),
    ):
        with psycopg.connect(_sync_url(os.environ[url_var]), autocommit=True) as conn:
            conn.execute(
                f"TRUNCATE {', '.join(tables)} RESTART IDENTITY CASCADE"
            )
    client = aioredis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    await client.flushdb()
    await client.aclose()
    yield


@pytest.fixture
async def blockchain_ctx():
    from blockchain_service.config import get_settings
    from blockchain_service.context import Context

    ctx = await Context.create(get_settings())
    try:
        yield ctx
    finally:
        await ctx.close()


@pytest.fixture
def blockchain_app(blockchain_ctx):
    from blockchain_service.main import create_app

    app = create_app()
    app.state.ctx = blockchain_ctx
    return app


@pytest.fixture
async def wallet_ctx(blockchain_app):
    """Wallet context whose blockchain client talks to the in-process app."""
    import httpx
    from wallet_service.chain_client import BlockchainServiceClient
    from wallet_service.config import get_settings
    from wallet_service.context import Context

    settings = get_settings()
    ctx = await Context.create(settings)
    await ctx.blockchain.aclose()
    ctx.blockchain = BlockchainServiceClient(
        "http://blockchain.test",
        internal_key=settings.internal_api_key,
        transport=httpx.ASGITransport(app=blockchain_app),
    )
    try:
        yield ctx
    finally:
        await ctx.close()


@pytest.fixture
def wallet_app(wallet_ctx):
    from wallet_service.main import create_app

    app = create_app()
    app.state.ctx = wallet_ctx
    return app


@pytest.fixture
async def wallet_client(wallet_app):
    import httpx

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wallet_app),
        base_url="http://wallet.test",
        headers=API_HEADERS,
    ) as client:
        yield client


@pytest.fixture
async def blockchain_client(blockchain_app):
    import httpx

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=blockchain_app),
        base_url="http://blockchain.test",
        headers=INTERNAL_HEADERS,
    ) as client:
        yield client


class Pipeline:
    """Deterministic driver for the asynchronous pipeline.

    One ``pump()`` round is one turn of every background loop in the platform,
    in the order they would naturally fire.
    """

    def __init__(self, wallet_ctx, blockchain_ctx) -> None:
        from blockchain_service.consumers.dispatcher import (
            SUBSCRIBED_EVENTS as BC_EVENTS,
        )
        from blockchain_service.consumers.dispatcher import (
            BlockchainDispatcher,
        )
        from blockchain_service.services.deposits import (
            DepositConfirmationWatcher,
            DepositScanner,
        )
        from blockchain_service.services.transactions import (
            BroadcastWorker,
            OutgoingTransactionWatcher,
        )
        from mcw_common.bus import ConsumerRuntime
        from wallet_service.consumers.dispatcher import (
            SUBSCRIBED_EVENTS as W_EVENTS,
        )
        from wallet_service.consumers.dispatcher import (
            WalletDispatcher,
        )
        from wallet_service.services.progress import TransferProgressWorker

        self.wallet_ctx = wallet_ctx
        self.blockchain_ctx = blockchain_ctx
        self.chain = blockchain_ctx.mock_chain

        suffix = uuid.uuid4().hex[:8]
        self.wallet_dispatcher = WalletDispatcher(wallet_ctx)
        self.blockchain_dispatcher = BlockchainDispatcher(blockchain_ctx)
        self.wallet_consumer = ConsumerRuntime(
            bus=wallet_ctx.bus, group="wallet-service", consumer_name=f"w-{suffix}",
            event_types=W_EVENTS, dispatch=self.wallet_dispatcher.dispatch,
            on_dead_letter=self.wallet_dispatcher.record_dead_letter,
            max_delivery_count=3, claim_idle_ms=50, block_ms=0,
        )
        self.blockchain_consumer = ConsumerRuntime(
            bus=blockchain_ctx.bus, group="blockchain-service", consumer_name=f"b-{suffix}",
            event_types=BC_EVENTS, dispatch=self.blockchain_dispatcher.dispatch,
            on_dead_letter=self.blockchain_dispatcher.record_dead_letter,
            max_delivery_count=3, claim_idle_ms=50, block_ms=0,
        )
        self.scanner = DepositScanner(blockchain_ctx)
        self.deposit_watcher = DepositConfirmationWatcher(blockchain_ctx)
        self.broadcaster = BroadcastWorker(blockchain_ctx)
        self.tx_watcher = OutgoingTransactionWatcher(blockchain_ctx)
        self.progress = TransferProgressWorker(wallet_ctx)

    async def mine(self, blocks: int = 1) -> None:
        for _ in range(blocks):
            await self.chain.mine_block()

    @staticmethod
    async def _safe(coro_factory):
        """Mirror the worker's behaviour: one failing loop never stops the rest."""
        try:
            return await coro_factory()
        except Exception:
            return None

    async def pump(self, rounds: int = 1, *, mine: bool = True) -> None:
        for _ in range(rounds):
            if mine:
                await self._safe(self.chain.mine_block)
            await self._safe(self.scanner.run_once)
            await self._safe(self.broadcaster.run_once)
            await self._safe(self.deposit_watcher.run_once)
            await self._safe(self.tx_watcher.run_once)
            await self._safe(self.blockchain_ctx.relay.run_once)
            await self._safe(self.wallet_ctx.relay.run_once)
            await self._safe(lambda: self.wallet_consumer.run_once(block_ms=0))
            await self._safe(lambda: self.blockchain_consumer.run_once(block_ms=0))
            await self._safe(self.progress.run_once)

    async def last_event(self, ctx, event_type: str):
        """Read the newest envelope of a type out of a service's outbox."""
        from mcw_common.events import EventEnvelope
        from sqlalchemy import text

        async with ctx.db.sessionmaker() as session:
            row = (
                await session.execute(
                    text(
                        "SELECT envelope FROM outbox WHERE event_type = :t "
                        "ORDER BY id DESC LIMIT 1"
                    ),
                    {"t": event_type},
                )
            ).scalar_one_or_none()
        return EventEnvelope.model_validate(row) if row else None

    async def settle(self, rounds: int = 8) -> None:
        """Enough rounds to carry a transaction from submission to CONFIRMED."""
        await self.pump(rounds)


@pytest.fixture
async def pipeline(wallet_ctx, blockchain_ctx):
    return Pipeline(wallet_ctx, blockchain_ctx)


@pytest.fixture
async def two_users(wallet_client):
    """User A and User B, each with a USDT wallet on BSC."""
    users = []
    for name, email in (("User A", "user-a@example.com"), ("User B", "user-b@example.com")):
        created = await wallet_client.post("/users", json={"name": name, "email": email})
        assert created.status_code == 201, created.text
        user = created.json()
        wallet = await wallet_client.post(f"/users/{user['id']}/wallet", json={})
        assert wallet.status_code == 201, wallet.text
        users.append({**user, "wallet": wallet.json()})
    return users


@pytest.fixture
def deposit(blockchain_client, pipeline):
    """Simulate an inbound deposit and (by default) drive it to CONFIRMED."""

    async def _deposit(address: str, amount: str = "1000.000000", *,
                       reference: str | None = None, settle: bool = True) -> dict:
        response = await blockchain_client.post(
            "/simulate/deposits",
            json={"to_address": address, "amount": amount, "asset": "USDT",
                  "reference": reference},
        )
        assert response.status_code == 202, response.text
        if settle:
            await pipeline.pump(6)
        return response.json()

    return _deposit


@pytest.fixture
async def funded_users(two_users, deposit):
    """User A holds 1000 USDT; User B holds nothing."""
    await deposit(two_users[0]["wallet"]["address"], "1000.000000")
    return two_users
