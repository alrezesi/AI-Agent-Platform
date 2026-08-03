
# Middleware for tenant identification and isolation

from typing import Optional, Dict, Any
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from .manager import TenantManager
from .exceptions import TenantNotFoundError, TenantInactiveError


class TenantMiddleware(BaseHTTPMiddleware):
    """
    Middleware that extracts tenant information from requests.
    Expects tenant_id in header: X-Tenant-ID or API key.
    """

    def __init__(
        self,
        app: ASGIApp,
        tenant_manager: TenantManager,
        header_name: str = "X-Tenant-ID",
    ):
        super().__init__(app)
        self.tenant_manager = tenant_manager
        self.header_name = header_name

    async def dispatch(self, request: Request, call_next):
        # Extract tenant ID from header
        tenant_id = request.headers.get(self.header_name)
        if tenant_id:
            # Validate tenant exists and is active
            try:
                tenant = await self.tenant_manager.get_tenant_or_raise(tenant_id)
                if not tenant.is_active():
                    raise TenantInactiveError(f"Tenant {tenant_id} is not active")
                request.state.tenant = tenant
                request.state.tenant_id = tenant_id
            except (TenantNotFoundError, TenantInactiveError) as e:
                raise HTTPException(status_code=403, detail=str(e))

        # Also try to extract from API key (if present)
        api_key = request.headers.get("X-API-Key")
        if api_key and not tenant_id:
            # Find tenant by API key
            tenants = await self.tenant_manager.list_tenants(limit=1000)
            for tenant in tenants:
                if tenant.has_api_key(api_key) and tenant.is_active():
                    request.state.tenant = tenant
                    request.state.tenant_id = tenant.tenant_id
                    break
            if not hasattr(request.state, 'tenant_id'):
                raise HTTPException(status_code=401, detail="Invalid API key")

        response = await call_next(request)
        return response