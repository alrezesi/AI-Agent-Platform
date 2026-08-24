"""Rate limiting middleware.

Implements a simple per-tenant token-bucket rate limiter.  The default
configuration allows 100 requests per second with a burst capacity of
200.  This protects the API against abusive callers without introducing
an external dependency (the algorithm is deterministic and thread-safe
within a single process).

The limiter is keyed by tenant_id (resolved by TenantMiddleware).  When
no tenant_id is available, the client IP is used as a fallback.
"""

import asyncio
import logging
import time
from typing import Optional

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

# Default limits
DEFAULT_REQUESTS_PER_SECOND: float = 100.0
DEFAULT_BURST: int = 200


class _TokenBucket:
    """A simple in-memory token bucket."""

    __slots__ = ("_tokens", "_capacity", "_refill_rate", "_last_refill", "_lock")

    def __init__(self, capacity: int, refill_rate: float) -> None:
        self._tokens: float = float(capacity)
        self._capacity: float = float(capacity)
        self._refill_rate: float = refill_rate
        self._last_refill: float = time.monotonic()
        self._lock = asyncio.Lock()

    async def consume(self, tokens: int = 1) -> bool:
        async with self._lock:
            now = time.monotonic()
            # Refill based on elapsed time since last call.
            elapsed = now - self._last_refill
            if elapsed > 0:
                self._tokens = min(
                    self._capacity,
                    self._tokens + elapsed * self._refill_rate,
                )
            self._last_refill = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False


class RateLimiter:
    """
    Per-key token-bucket rate limiter.

    In a single-process deployment each API process has its own limiter
    instance.  For the two-worker Docker stack this is sufficient because
    each worker is an independent consumer and the API process is
    single-worker.
    """

    def __init__(self, requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND, burst: int = DEFAULT_BURST) -> None:
        self._rps = requests_per_second
        self._burst = burst
        self._buckets: dict[str, _TokenBucket] = {}
        self._lock = asyncio.Lock()

    async def is_allowed(self, key: str) -> bool:
        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _TokenBucket(self._burst, self._rps)
                self._buckets[key] = bucket
        return await bucket.consume()

    def _get_key(self, request) -> str:
        """Resolve a rate-limit key from the request."""
        tenant_id = getattr(request.state, "tenant_id", None) if hasattr(request, "state") else None
        if tenant_id:
            return f"tenant:{tenant_id}"
        # Fallback to client IP
        client_host = getattr(getattr(request, "client", None), "host", None)
        return f"ip:{client_host or 'unknown'}"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Middleware that enforces per-tenant rate limits.

    Exceeds limits return HTTP 429 with a ``Retry-After`` header.
    """

    def __init__(self, app, requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND, burst: int = DEFAULT_BURST):
        super().__init__(app)
        self._limiter = RateLimiter(requests_per_second, burst)

    async def dispatch(self, request, call_next):
        # Skip rate limiting for health checks and monitoring endpoints.
        path = request.url.path
        if path in ("/health", "/") or path.startswith("/docs") or path.startswith("/redoc") or path.startswith("/openapi"):
            return await call_next(request)

        key = self._limiter._get_key(request)
        if not await self._limiter.is_allowed(key):
            logger.warning("Rate limit exceeded for key: %s", key)
            return JSONResponse(
                status_code=429,
                headers={"Retry-After": "1"},
                content={"detail": "Rate limit exceeded"},
            )
        return await call_next(request)
