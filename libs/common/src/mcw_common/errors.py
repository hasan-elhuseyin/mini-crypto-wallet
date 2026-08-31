"""Application error taxonomy.

Every failure that reaches a client is an :class:`AppError` with a stable
machine readable ``code``. HTTP status codes are an attribute of the error,
not something route handlers decide ad hoc.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "AppError",
    "ValidationError",
    "NotFoundError",
    "ConflictError",
    "InsufficientFundsError",
    "IdempotencyConflictError",
    "IdempotencyInProgressError",
    "UnauthorizedError",
    "UpstreamUnavailableError",
    "WalletInactiveError",
]


class AppError(Exception):
    status_code: int = 500
    code: str = "INTERNAL_ERROR"
    title: str = "Internal server error"

    def __init__(self, detail: str | None = None, **extra: Any) -> None:
        super().__init__(detail or self.title)
        self.detail = detail or self.title
        self.extra = extra


class ValidationError(AppError):
    status_code = 422
    code = "VALIDATION_ERROR"
    title = "Request validation failed"


class UnauthorizedError(AppError):
    status_code = 401
    code = "UNAUTHORIZED"
    title = "Missing or invalid credentials"


class NotFoundError(AppError):
    status_code = 404
    code = "NOT_FOUND"
    title = "Resource not found"


class ConflictError(AppError):
    status_code = 409
    code = "CONFLICT"
    title = "Conflicting state"


class WalletInactiveError(ConflictError):
    code = "WALLET_INACTIVE"
    title = "Wallet is not active"


class InsufficientFundsError(ConflictError):
    code = "INSUFFICIENT_FUNDS"
    title = "Insufficient available balance"


class IdempotencyConflictError(ConflictError):
    code = "IDEMPOTENCY_KEY_REUSED"
    title = "Idempotency key already used with a different payload"


class IdempotencyInProgressError(ConflictError):
    status_code = 409
    code = "IDEMPOTENT_REQUEST_IN_PROGRESS"
    title = "An identical request is currently being processed"


class UpstreamUnavailableError(AppError):
    status_code = 503
    code = "UPSTREAM_UNAVAILABLE"
    title = "A downstream dependency is unavailable"
