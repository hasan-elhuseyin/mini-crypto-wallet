"""Prometheus metrics shared by both services."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

__all__ = [
    "REGISTRY",
    "HTTP_REQUESTS",
    "HTTP_LATENCY",
    "EVENTS_PUBLISHED",
    "EVENTS_CONSUMED",
    "EVENT_HANDLER_LATENCY",
    "OUTBOX_BACKLOG",
    "DEAD_LETTERS",
    "LEDGER_ENTRIES",
    "TRANSFERS",
    "DEPOSITS",
    "CHAIN_RPC_CALLS",
    "render_metrics",
    "CONTENT_TYPE",
]

REGISTRY = CollectorRegistry(auto_describe=True)
CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

HTTP_REQUESTS = Counter(
    "mcw_http_requests_total", "HTTP requests", ["method", "path", "status"], registry=REGISTRY
)
HTTP_LATENCY = Histogram(
    "mcw_http_request_duration_seconds", "HTTP latency", ["method", "path"], registry=REGISTRY
)
EVENTS_PUBLISHED = Counter(
    "mcw_events_published_total",
    "Events published from the outbox",
    ["event_type"],
    registry=REGISTRY,
)
EVENTS_CONSUMED = Counter(
    "mcw_events_consumed_total",
    "Events consumed",
    ["event_type", "outcome"],  # outcome: processed | duplicate | failed | dead_lettered
    registry=REGISTRY,
)
EVENT_HANDLER_LATENCY = Histogram(
    "mcw_event_handler_duration_seconds", "Event handler latency", ["event_type"], registry=REGISTRY
)
OUTBOX_BACKLOG = Gauge(
    "mcw_outbox_unpublished", "Unpublished outbox rows", registry=REGISTRY
)
DEAD_LETTERS = Counter(
    "mcw_dead_letters_total",
    "Events moved to the dead letter store",
    ["event_type"],
    registry=REGISTRY,
)
LEDGER_ENTRIES = Counter(
    "mcw_ledger_entries_total", "Ledger entries written", ["entry_type"], registry=REGISTRY
)
TRANSFERS = Counter(
    "mcw_transfers_total", "Transfer state transitions", ["status"], registry=REGISTRY
)
DEPOSITS = Counter(
    "mcw_deposits_total", "Deposit state transitions", ["status"], registry=REGISTRY
)
CHAIN_RPC_CALLS = Counter(
    "mcw_chain_rpc_calls_total", "Chain adapter calls", ["method", "outcome"], registry=REGISTRY
)


def render_metrics() -> bytes:
    return generate_latest(REGISTRY)
