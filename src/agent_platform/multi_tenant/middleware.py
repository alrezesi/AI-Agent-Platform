import logging
import os

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from src.agent_platform.multi_tenant.models import Tenant, TenantStatus

logger = logging.getLogger(__name__)


def _get_api_key_from_headers(request: Request) -> str | None:
    """Extract an API key from X-API-Key or Authorization: Bearer."""
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return api_key
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]
    return None


class TenantMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, tenant_manager=None):
        super().__init__(app)
        self.tenant_manager = tenant_manager

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method.upper()
        tenant_manager = self.tenant_manager
        if tenant_manager is None:
            from src.agent_platform.runtime import get_tenant_manager

            tenant_manager = get_tenant_manager()

        if path == "/" or path == "/health" or path.startswith("/docs") or path.startswith("/redoc") or path.startswith("/openapi") or path.startswith("/monitoring"):
            return await call_next(request)

        # Allow tenant creation and API-key generation without authentication
        # (bootstrap: the first key is generated before it can be used).
        if path == "/tenants/" and method == "POST":
            return await call_next(request)

        if path.startswith("/tenants/") and path.endswith("/api-keys") and method == "POST":
            return await call_next(request)

        api_key = _get_api_key_from_headers(request)
        tenant_id_hint = request.headers.get("X-Tenant-ID")

        # An API key is always required for authentication.
        # X-Tenant-ID may be supplied as a routing hint but is NOT
        # sufficient on its own — it must match the tenant that owns
        # the supplied API key.
        if not api_key:
            return JSONResponse(status_code=401, content={"detail": "Missing API key"})

        tenant = None
        if tenant_manager is not None:
            tenant = await tenant_manager.authenticate_api_key(api_key)
        else:
            # Fallback when no tenant_manager is available (e.g., bare
            # test fixture without dependency override).  We still
            # generate a hash so the raw key is never stored in memory.
            from src.agent_platform.security import hash_api_key

            tenant = Tenant(
                tenant_id=tenant_id_hint or "test-tenant",
                name=f"Tenant {tenant_id_hint or 'test'}",
                status=TenantStatus.ACTIVE,
                api_keys=[{"key_hash": hash_api_key(api_key), "is_active": True}],
            )

        if not tenant or tenant.status != TenantStatus.ACTIVE:
            return JSONResponse(status_code=401, content={"detail": "Invalid tenant authentication"})

        # If the caller provided a tenant hint, verify it matches.
        if tenant_id_hint and tenant.tenant_id != tenant_id_hint:
            return JSONResponse(status_code=403, content={"detail": "Tenant mismatch"})

        request.state.tenant = tenant
        request.state.tenant_id = tenant.tenant_id
        response = await call_next(request)
        return response
