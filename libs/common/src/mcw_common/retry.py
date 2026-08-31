"""Retry and circuit-breaking for calls that leave the process.

Retries are only applied to operations we know to be safe to repeat:
read-only RPC (``eth_getLogs``, receipts) and *idempotent* writes. Broadcasting
a transaction is guarded by a deterministic client-side identity instead of a
blind retry (see the chain adapter), because a naive retry there could double
spend.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TypeVar

from .logging import get_logger

__all__ = ["RetryPolicy", "retry_async", "CircuitBreaker", "CircuitOpenError"]

T = TypeVar("T")
log = get_logger("retry")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    attempts: int = 4
    base_delay: float = 0.2
    max_delay: float = 5.0
    jitter: float = 0.25

    def delay_for(self, attempt: int) -> float:
        """Exponential backoff with full-ish jitter (avoids retry storms)."""
        raw = min(self.max_delay, self.base_delay * (2 ** max(0, attempt - 1)))
        return raw * (1 - self.jitter + random.random() * self.jitter * 2)


async def retry_async(
    fn: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy | None = None,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    operation: str = "operation",
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    policy = policy or RetryPolicy()
    last: BaseException | None = None
    for attempt in range(1, policy.attempts + 1):
        try:
            return await fn()
        except retry_on as exc:
            last = exc
            if attempt == policy.attempts:
                break
            delay = policy.delay_for(attempt)
            log.warning(
                "retry.attempt_failed",
                operation=operation,
                attempt=attempt,
                max_attempts=policy.attempts,
                retry_in_seconds=round(delay, 3),
                error=str(exc),
            )
            await sleep(delay)
    assert last is not None
    raise last


class CircuitOpenError(RuntimeError):
    pass


@dataclass
class CircuitBreaker:
    """Stops hammering a dead RPC node; fails fast until the cooldown elapses."""

    failure_threshold: int = 5
    reset_timeout: float = 15.0
    name: str = "chain-rpc"
    _failures: int = field(default=0, init=False)
    _opened_at: float | None = field(default=None, init=False)

    @property
    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        if (time.monotonic() - self._opened_at) >= self.reset_timeout:
            self._opened_at = None  # half-open: let the next call through
            self._failures = self.failure_threshold - 1
            return False
        return True

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.failure_threshold and self._opened_at is None:
            self._opened_at = time.monotonic()
            log.error("circuit.opened", circuit=self.name, failures=self._failures)

    def guard(self) -> None:
        if self.is_open:
            raise CircuitOpenError(f"circuit '{self.name}' is open")
