"""Correlation / causation id propagation.

One id follows a business operation across HTTP calls, events and both
services, so a single `correlation_id` grep reconstructs the whole flow.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

__all__ = [
    "CORRELATION_HEADER",
    "correlation_id_var",
    "get_correlation_id",
    "new_correlation_id",
    "correlation_scope",
]

CORRELATION_HEADER = "X-Correlation-ID"

correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def new_correlation_id(prefix: str = "cid") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


def get_correlation_id() -> str | None:
    return correlation_id_var.get()


@contextmanager
def correlation_scope(correlation_id: str | None) -> Iterator[str]:
    """Bind a correlation id for the duration of a block (task/consumer safe)."""
    value = correlation_id or new_correlation_id()
    token = correlation_id_var.set(value)
    try:
        yield value
    finally:
        correlation_id_var.reset(token)
