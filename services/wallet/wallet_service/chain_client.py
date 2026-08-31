"""Synchronous client for blockchain-service.

Only one call is synchronous in this platform: issuing a custody address. It is
a request/response question with an immediate answer and no money attached, so
making it asynchronous would buy nothing and cost the client a polling loop.
Everything that moves value is asynchronous and event driven.

The call is safe to retry because it is idempotent on ``owner_ref``.
"""

from __future__ import annotations

from typing import Any

import httpx
from mcw_common.correlation import CORRELATION_HEADER, get_correlation_id
from mcw_common.errors import UpstreamUnavailableError
from mcw_common.logging import get_logger
from mcw_common.retry import RetryPolicy, retry_async

log = get_logger("chain_client")


class _Retryable(Exception):
    """Transient upstream failure (5xx / transport)."""


class _Permanent(Exception):
    """Upstream rejected the request; retrying cannot help."""


class BlockchainServiceClient:
    def __init__(
        self,
        base_url: str,
        *,
        internal_key: str,
        timeout: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        # `transport` is an injection point: the integration tests wire it
        # straight to the blockchain-service ASGI app, so the real client code
        # (headers, retries, error mapping) is what gets exercised.
        self._base_url = base_url.rstrip("/")
        self._internal_key = internal_key
        self._timeout = timeout
        self._client = httpx.AsyncClient(
            base_url=self._base_url, timeout=timeout, transport=transport
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    def _headers(self) -> dict[str, str]:
        headers = {"X-Internal-Key": self._internal_key}
        correlation_id = get_correlation_id()
        if correlation_id:
            headers[CORRELATION_HEADER] = correlation_id
        return headers

    async def create_address(self, *, owner_ref: str, network: str) -> dict[str, Any]:
        async def _call() -> dict[str, Any]:
            response = await self._client.post(
                "/addresses",
                json={"owner_ref": owner_ref, "network": network},
                headers=self._headers(),
            )
            if response.status_code >= 500:
                raise _Retryable(f"blockchain-service returned {response.status_code}")
            if response.status_code >= 400:
                # A 4xx will not get better on retry: fail immediately.
                raise _Permanent(
                    f"blockchain-service rejected the request: {response.status_code}"
                )
            return response.json()

        try:
            return await retry_async(
                _call,
                policy=RetryPolicy(attempts=3, base_delay=0.2, max_delay=1.5),
                retry_on=(httpx.TransportError, _Retryable),
                operation="blockchain.create_address",
            )
        except (httpx.HTTPError, _Retryable, _Permanent) as exc:
            log.error("chain_client.create_address_failed", owner_ref=owner_ref, error=str(exc))
            raise UpstreamUnavailableError(
                "blockchain-service could not issue an address. No wallet was created; "
                "retry the request."
            ) from exc

    async def ping(self) -> bool:
        try:
            response = await self._client.get("/health/live", timeout=2.0)
            return response.status_code == 200
        except httpx.HTTPError:
            return False
