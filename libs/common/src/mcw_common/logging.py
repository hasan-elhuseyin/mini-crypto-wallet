"""Structured JSON logging with secret redaction.

Two non-negotiables encoded here:

* every log line is JSON and carries ``service`` + ``correlation_id``;
* private keys / secrets can never be logged, even by accident. The redaction
  processor scrubs known-sensitive keys *and* anything that looks like a raw
  32-byte hex secret, before the renderer ever sees it.
"""

from __future__ import annotations

import logging
import re
import sys
from typing import Any

import structlog

from .correlation import correlation_id_var

__all__ = ["configure_logging", "get_logger", "redact"]

_SENSITIVE_KEY_RE = re.compile(
    r"(private[_-]?key|priv[_-]?key|secret|password|passwd|mnemonic|seed|"
    r"api[_-]?key|authorization|token|credential|keystore)",
    re.IGNORECASE,
)
# A 32-byte hex blob is either a private key or a hash. Hashes are 32 bytes too,
# so we only redact when it is *not* prefixed as a tx hash field; the key based
# rule above catches the named cases and this is the belt-and-braces net for
# free-form strings such as exception messages.
_HEX32_RE = re.compile(r"\b(0x)?[0-9a-fA-F]{64}\b")
_REDACTED = "***REDACTED***"

#: Field names whose 64-hex values are legitimate and must stay readable.
_HASH_FIELDS = {"tx_hash", "block_hash", "parent_hash", "hash", "request_hash", "payload_hash"}


def _scrub_value(value: Any) -> Any:
    if isinstance(value, str):
        return _HEX32_RE.sub(_REDACTED, value)
    return value


def redact(obj: Any, *, key: str | None = None) -> Any:
    """Recursively redact secrets from an arbitrary log payload."""
    if key is not None and _SENSITIVE_KEY_RE.search(key):
        return _REDACTED
    if isinstance(obj, dict):
        return {k: redact(v, key=str(k)) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return type(obj)(redact(v) for v in obj)
    if key in _HASH_FIELDS:
        return obj
    return _scrub_value(obj)


def _redaction_processor(_logger: Any, _name: str, event_dict: dict) -> dict:
    return {k: redact(v, key=str(k)) for k, v in event_dict.items()}


def _correlation_processor(_logger: Any, _name: str, event_dict: dict) -> dict:
    cid = correlation_id_var.get()
    if cid and "correlation_id" not in event_dict:
        event_dict["correlation_id"] = cid
    return event_dict


def configure_logging(service: str, level: str = "INFO", *, json_output: bool = True) -> None:
    renderer: Any = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            _correlation_processor,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _redaction_processor,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[level.upper()]
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level.upper())
    for noisy in ("uvicorn.access", "sqlalchemy.engine.Engine", "httpx", "web3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    structlog.contextvars.bind_contextvars(service=service)


def get_logger(name: str | None = None) -> Any:
    """Return a logger tagged with ``name``.

    The name is bound as an ordinary event key rather than through
    ``structlog.stdlib.add_logger_name``: that processor reads ``logger.name``,
    which only exists on stdlib loggers, and we render through structlog's own
    PrintLogger.
    """
    logger = structlog.get_logger()
    return logger.bind(logger=name) if name else logger
