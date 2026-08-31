"""blockchain-service background process.

Runs, in one asyncio event loop:

* the ``transfer.requested`` consumer;
* the outbox relay;
* the deposit scanner and its confirmation watcher;
* the broadcaster and the outgoing-transaction watcher;
* (mock backend only) the block producer.

Split into its own container so API latency is never coupled to chain polling,
and so the two can be scaled independently.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import Awaitable, Callable
from typing import Any

from mcw_common.bus import ConsumerRuntime
from mcw_common.logging import configure_logging, get_logger

from .config import get_settings
from .consumers.dispatcher import SUBSCRIBED_EVENTS, BlockchainDispatcher
from .context import Context
from .services.deposits import DepositConfirmationWatcher, DepositScanner
from .services.transactions import BroadcastWorker, OutgoingTransactionWatcher

log = get_logger("worker")


async def periodic(
    name: str, fn: Callable[[], Awaitable[Any]], interval: float, stop: asyncio.Event
) -> None:
    """Run ``fn`` on a fixed interval, surviving individual failures."""
    while not stop.is_set():
        try:
            await fn()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A failing loop must never take the process down: the next tick
            # retries, and the state it works from is durable.
            log.exception("worker.loop_failed", loop=name, error=str(exc))
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue


async def run() -> None:
    settings = get_settings()
    configure_logging(
        f"{settings.service_name}-worker", settings.log_level, json_output=settings.log_json
    )
    ctx = await Context.create(settings)
    dispatcher = BlockchainDispatcher(ctx)
    consumer = ConsumerRuntime(
        bus=ctx.bus,
        group=settings.consumer_group,
        consumer_name=settings.consumer_name,
        event_types=SUBSCRIBED_EVENTS,
        dispatch=dispatcher.dispatch,
        on_dead_letter=dispatcher.record_dead_letter,
        max_delivery_count=settings.consumer_max_delivery,
        claim_idle_ms=settings.consumer_claim_idle_ms,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not available on every platform; the loop still exits on cancellation.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    poll = settings.worker_poll_seconds
    tasks = [
        asyncio.create_task(consumer.run_forever(stop), name="consumer"),
        asyncio.create_task(
            periodic("outbox-relay", ctx.relay.run_once, 0.25, stop), name="outbox"
        ),
        asyncio.create_task(
            periodic("deposit-scanner", DepositScanner(ctx).run_once, poll, stop),
            name="scanner",
        ),
        asyncio.create_task(
            periodic(
                "deposit-confirmations", DepositConfirmationWatcher(ctx).run_once, poll, stop
            ),
            name="deposit-confirmations",
        ),
        asyncio.create_task(
            periodic("broadcaster", BroadcastWorker(ctx).run_once, poll, stop),
            name="broadcaster",
        ),
        asyncio.create_task(
            periodic(
                "tx-confirmations", OutgoingTransactionWatcher(ctx).run_once, poll, stop
            ),
            name="tx-confirmations",
        ),
    ]
    if ctx.mock_chain is not None and settings.mock_auto_mine:
        tasks.append(
            asyncio.create_task(
                periodic(
                    "block-producer", ctx.mock_chain.mine_block,
                    settings.mock_block_time_seconds, stop,
                ),
                name="miner",
            )
        )

    log.info("worker.started", loops=[t.get_name() for t in tasks])
    await stop.wait()
    log.info("worker.stopping")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await ctx.close()
    log.info("worker.stopped")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
