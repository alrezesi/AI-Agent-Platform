"""Request ID and tenant correlation middleware.

Generates or propagates a unique request ID for every incoming HTTP request
and ensures it is available in both the structured log context and the
OpenTelemetry-style trace spans.

The request ID flows through the full task lifecycle:
    Request ID → Task ID → Tenant ID → Queue Message → Worker ID →
    Execution ID → Retry → Final Result
"""

import logging
import uuid
from contextvars import ContextVar

from starlette.middleware.base import BaseHTTPMiddleware

# Context variable accessible from any coroutine in the same request scope.
_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)


def get_request_id() -> str | None:
    """Return the current request ID, or None if not in a request context."""
    return _request_id.get()


def set_request_id(value: str | None) -> None:
    """Set (or clear) the current request ID."""
    _request_id.set(value)


def generate_request_id() -> str:
    """Generate a new random request ID."""
    return uuid.uuid4().hex[:16]


class RequestIdMiddleware(BaseHTTPMiddleware):
    """
    Inject a request ID into the request state and logging context.

    The ID is taken from the ``X-Request-ID`` header if present (for
    cross-service tracing) or generated fresh otherwise.
    """

    HEADER_NAME = "X-Request-ID"

    async def dispatch(self, request, call_next):
        request_id = request.headers.get(self.HEADER_NAME)
        if not request_id:
            request_id = generate_request_id()

        request.state.request_id = request_id

        token = _request_id.set(request_id)
        try:
            response = await call_next(request)
            response.headers[self.HEADER_NAME] = request_id
        finally:
            _request_id.reset(token)

        return response


class CorrelationLoggingFilter(logging.Filter):
    """Inject request_id into every log record produced within a request."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: ACA002
        rid = _request_id.get()
        if rid:
            record.request_id = rid
        else:
            record.request_id = "-"
        return True
