from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from src.agent_platform.multi_tenant.models import Tenant, TenantStatus


class TenantMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, tenant_manager=None):
        super().__init__(app)
        self.tenant_manager = tenant_manager

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method.upper()

        if path == "/" or path == "/health" or path.startswith("/docs") or path.startswith("/redoc") or path.startswith("/openapi") or path.startswith("/monitoring"):
            return await call_next(request)

        if path == "/tenants/" and method == "POST":
            return await call_next(request)

        # Development/test mode:
        # Inject a predefined tenant without performing authentication.
        api_key = request.headers.get("X-API-Key")
        tenant_id = request.headers.get("X-Tenant-ID")

        # At least one authentication mechanism is required.
        if not api_key and not tenant_id:
            return JSONResponse(status_code=401, content={"detail": "Missing tenant authentication"})

        tenant = None
        if self.tenant_manager is not None:
            if api_key:
                tenant = await self.tenant_manager.authenticate_api_key(api_key)
            elif tenant_id:
                tenant = await self.tenant_manager.get_tenant(tenant_id)
            if not tenant or tenant.status != TenantStatus.ACTIVE:
                return JSONResponse(status_code=401, content={"detail": "Invalid tenant authentication"})
        else:
            if api_key:
                tenant = Tenant(
                    tenant_id="dummy",
                    name="Dummy",
                    status=TenantStatus.ACTIVE,
                    api_keys=[{"key": api_key, "is_active": True}],
                )
            elif tenant_id:
                tenant = Tenant(
                    tenant_id=tenant_id,
                    name=f"Tenant {tenant_id}",
                    status=TenantStatus.ACTIVE,
                    api_keys=[],
                )
            else:
                raise HTTPException(status_code=401, detail="Invalid tenant authentication")

        request.state.tenant = tenant
        request.state.tenant_id = tenant.tenant_id
        response = await call_next(request)
        return response
