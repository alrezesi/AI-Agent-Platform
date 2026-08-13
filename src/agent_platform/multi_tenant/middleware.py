# Middleware for tenant identification and isolation

from typing import Optional

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from .manager import TenantManager


class TenantMiddleware(BaseHTTPMiddleware):
    """
    Resolve the tenant from an API key and store it on request.state.

    X-Tenant-ID is never treated as identity. If present, it must match the
    tenant authenticated by X-API-Key.
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

    def _get_tenant_manager(self) -> TenantManager:
        if self._tenant_manager is not None:
            return self._tenant_manager
        from src.agent_platform.runtime import get_tenant_manager

        return get_tenant_manager()

    async def dispatch(self, request: Request, call_next):
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

        response = await call_next(request)
        return response
