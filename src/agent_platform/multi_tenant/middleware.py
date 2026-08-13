# src/agent_platform/multi_tenant/middleware.py
# Middleware for tenant identification and isolation

from typing import Optional, List
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from .manager import TenantManager


class TenantMiddleware(BaseHTTPMiddleware):
    """
    Resolve the tenant from an API key or tenant ID header.
    - X-API-Key: Primary authentication method (secure).
    - X-Tenant-ID: Fallback for development/testing.

    Public endpoints (no auth required):
    - POST /tenants/      (create tenant)
    - GET /health         (health check)
    - GET /               (root)
    - GET /docs           (Swagger UI)
    - GET /openapi.json   (OpenAPI spec)
    - GET /monitoring/*   (monitoring endpoints – can be made public)
    """

    def __init__(
        self,
        app: ASGIApp,
        tenant_manager: Optional[TenantManager] = None,
        header_name: str = "X-Tenant-ID",
    ):
        super().__init__(app)
        self._tenant_manager = tenant_manager
        self.header_name = header_name

        # Paths that do not require authentication
        self._public_paths: List[str] = [
            "/health",
            "/",
            "/docs",
            "/openapi.json",
        ]

        # Prefixes that are public
        self._public_prefixes: List[str] = [
            "/monitoring",
        ]

    def _get_tenant_manager(self) -> TenantManager:
        """Get the tenant manager instance (cached or from runtime)."""
        if self._tenant_manager is not None:
            return self._tenant_manager
        from src.agent_platform.runtime import get_tenant_manager
        return get_tenant_manager()

    def _is_public_path(self, path: str, method: str) -> bool:
        """Check if the request path is public and does not require auth."""
        # Exact public paths
        if path in self._public_paths:
            return True

        # POST /tenants/ is public (tenant creation)
        if method == "POST" and path == "/tenants/":
            return True

        # Public prefixes (e.g., /monitoring/status)
        for prefix in self._public_prefixes:
            if path.startswith(prefix):
                return True

        return False

    async def dispatch(self, request: Request, call_next):
        # Allow public endpoints without authentication
        if self._is_public_path(request.url.path, request.method):
            return await call_next(request)

        # Authenticate via API key (primary)
        api_key = request.headers.get("X-API-Key")
        if api_key:
            tenant = await self._get_tenant_manager().get_tenant_by_api_key(api_key)
            if not tenant:
                return JSONResponse(status_code=401, content={"detail": "Invalid API key"})

            requested_tenant = request.headers.get(self.header_name)
            if requested_tenant and requested_tenant != tenant.tenant_id:
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Tenant header does not match API key"},
                )

            request.state.tenant = tenant
            request.state.tenant_id = tenant.tenant_id
            return await call_next(request)

        # Fallback: try X-Tenant-ID header (for testing/development)
        tenant_id_header = request.headers.get(self.header_name)
        if tenant_id_header:
            tenant = await self._get_tenant_manager().get_tenant(tenant_id_header)
            if not tenant:
                return JSONResponse(status_code=401, content={"detail": "Invalid tenant ID"})
            if not tenant.is_active():
                return JSONResponse(status_code=403, content={"detail": "Tenant is inactive"})
            request.state.tenant = tenant
            request.state.tenant_id = tenant.tenant_id
            return await call_next(request)

        # No authentication provided
        return JSONResponse(
            status_code=401,
            content={"detail": "Missing tenant authentication (X-API-Key or X-Tenant-ID)"},
        )