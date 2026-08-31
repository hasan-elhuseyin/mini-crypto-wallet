"""FastAPI wiring shared by both services: correlation, errors, auth, probes."""

from __future__ import annotations

import hmac
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from fastapi import APIRouter, FastAPI, Request, Response, Security
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from starlette.middleware.base import BaseHTTPMiddleware

from .correlation import CORRELATION_HEADER, correlation_id_var, new_correlation_id
from .errors import AppError, UnauthorizedError
from .logging import get_logger
from .metrics import CONTENT_TYPE, HTTP_LATENCY, HTTP_REQUESTS, render_metrics

__all__ = [
    "CorrelationMiddleware",
    "install_error_handlers",
    "api_key_auth",
    "build_ops_router",
    "problem",
]

log = get_logger("http")

PROBLEM_CONTENT_TYPE = "application/problem+json"


def problem(
    *, status: int, code: str, title: str, detail: str, errors: Sequence[Any] | None = None
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": f"https://errors.mini-crypto-wallet.local/{code.lower()}",
        "title": title,
        "status": status,
        "code": code,
        "detail": detail,
        "correlation_id": correlation_id_var.get(),
    }
    if errors:
        body["errors"] = list(errors)
    return JSONResponse(body, status_code=status, media_type=PROBLEM_CONTENT_TYPE)


class CorrelationMiddleware(BaseHTTPMiddleware):
    """Accepts an inbound correlation id or mints one; echoes it on the response."""

    def __init__(self, app: Any, *, service: str) -> None:
        super().__init__(app)
        self._service = service

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        incoming = request.headers.get(CORRELATION_HEADER) or request.headers.get("X-Request-ID")
        correlation_id = incoming or new_correlation_id()
        token = correlation_id_var.set(correlation_id)
        route = request.scope.get("route")
        path = getattr(route, "path", request.url.path)
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            HTTP_REQUESTS.labels(request.method, path, "500").inc()
            log.exception(
                "http.unhandled_error", method=request.method, path=request.url.path
            )
            raise
        finally:
            correlation_id_var.reset(token)
        elapsed = time.perf_counter() - started
        # Re-read the matched route: it is only known after routing happened.
        route = request.scope.get("route")
        path = getattr(route, "path", request.url.path)
        HTTP_REQUESTS.labels(request.method, path, str(response.status_code)).inc()
        HTTP_LATENCY.labels(request.method, path).observe(elapsed)
        response.headers[CORRELATION_HEADER] = correlation_id
        quiet = ("/health", "/health/live", "/health/ready", "/metrics")
        if request.url.path not in quiet:
            log.info(
                "http.request",
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                duration_ms=round(elapsed * 1000, 2),
                correlation_id=correlation_id,
            )
        return response


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(_: Request, exc: AppError) -> JSONResponse:
        if exc.status_code >= 500:
            log.error("app.error", code=exc.code, detail=exc.detail, **exc.extra)
        else:
            log.info("app.error", code=exc.code, detail=exc.detail, **exc.extra)
        return problem(
            status=exc.status_code,
            code=exc.code,
            title=exc.title,
            detail=exc.detail,
            errors=exc.extra.get("errors"),
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {
                "field": ".".join(str(p) for p in err.get("loc", [])[1:]) or None,
                "message": err.get("msg"),
                "type": err.get("type"),
            }
            for err in exc.errors()
        ]
        return problem(
            status=422,
            code="VALIDATION_ERROR",
            title="Request validation failed",
            detail="One or more fields are invalid.",
            errors=errors,
        )

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
        log.exception("app.unhandled_error", error=str(exc))
        return problem(
            status=500,
            code="INTERNAL_ERROR",
            title="Internal server error",
            detail="An unexpected error occurred. The correlation id identifies this request.",
        )


def api_key_auth(
    *,
    keys: Sequence[str],
    header: str = "X-API-Key",
    enabled: bool = True,
    description: str | None = None,
):
    """Build a FastAPI dependency that authenticates with an API key header.

    Returned as a closure rather than a callable object on purpose: FastAPI
    inspects the dependency's *signature*, and it is the ``Security(...)``
    marker below that registers the scheme in the OpenAPI document -- which is
    what makes the **Authorize** button appear in the Swagger UI. A plain
    ``__call__`` that reads the header off the request works at runtime but
    documents nothing, leaving the interactive docs unusable.

    Deliberately simple otherwise: a real deployment puts OAuth2/JWT (or mTLS
    between services) in front of this. What matters here is that the shape is
    right -- every endpoint is authenticated, credentials are compared in
    constant time, and they never reach the logs.
    """
    valid_keys = [key for key in keys if key]
    scheme = APIKeyHeader(
        name=header,
        auto_error=False,  # we raise our own problem+json instead of FastAPI's
        scheme_name=header,
        description=description or f"API key sent in the `{header}` header.",
    )

    async def authenticate(
        request: Request, presented: str | None = Security(scheme)
    ) -> str:
        if not enabled:
            return "auth-disabled"
        if not presented:
            # Also accept `Authorization: Bearer <key>` for clients that prefer it.
            authorization = request.headers.get("Authorization", "")
            if authorization.lower().startswith("bearer "):
                presented = authorization[7:]
        if not presented or not any(hmac.compare_digest(presented, k) for k in valid_keys):
            raise UnauthorizedError(f"A valid {header} header is required.")
        return "authenticated"

    return authenticate


def build_ops_router(
    *,
    service: str,
    version: str,
    readiness_checks: dict[str, Callable[[], Awaitable[bool]]] | None = None,
) -> APIRouter:
    router = APIRouter(tags=["ops"])

    @router.get("/health/live", summary="Liveness probe")
    async def live() -> dict[str, str]:
        return {"status": "alive", "service": service, "version": version}

    @router.get("/health", summary="Readiness probe")
    @router.get("/health/ready", include_in_schema=False)
    async def ready() -> Response:
        results: dict[str, str] = {}
        healthy = True
        for name, check in (readiness_checks or {}).items():
            try:
                ok = await check()
            except Exception as exc:
                ok = False
                log.warning("health.check_failed", dependency=name, error=str(exc))
            results[name] = "up" if ok else "down"
            healthy = healthy and ok
        return JSONResponse(
            {"status": "ready" if healthy else "degraded", "service": service,
             "version": version, "dependencies": results},
            status_code=200 if healthy else 503,
        )

    @router.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(render_metrics(), media_type=CONTENT_TYPE)

    return router
