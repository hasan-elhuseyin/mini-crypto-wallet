"""wallet-service background process: event consumer + outbox relay."""

from __future__ import annotations

import asyncio
import contextlib
import signal

from mcw_common.bus import ConsumerRuntime
from mcw_common.logging import configure_logging, get_logger

from .config import get_settings
from .consumers.dispatcher import SUBSCRIBED_EVENTS, WalletDispatcher
from .context import Context
from .services.progress import TransferProgressWorker

log = get_logger("worker")


async def periodic(name, fn, interval: float, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await fn()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
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
    dispatcher = WalletDispatcher(ctx)
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

    tasks = [
        asyncio.create_task(consumer.run_forever(stop), name="consumer"),
        asyncio.create_task(
            periodic("outbox-relay", ctx.relay.run_once, 0.25, stop), name="outbox"
        ),
        asyncio.create_task(
            periodic(
                "transfer-progress", TransferProgressWorker(ctx).run_once,
                settings.worker_poll_seconds, stop,
            ),
            name="transfer-progress",
        ),
    ]
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
