import os

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware

from src.agent_platform.multi_tenant.models import Tenant, TenantStatus


DISABLE_AUTH = os.getenv("DISABLE_AUTH", "false").lower() == "true"


class TenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Development/test mode:
        # Inject a predefined tenant without performing authentication.
        if DISABLE_AUTH:
            tenant = Tenant(
                tenant_id="test-tenant",
                name="Test Tenant",
                status=TenantStatus.ACTIVE,
                api_keys=[
                    {
                        "key": "test-api-key-12345",
                        "is_active": True,
                    }
                ],
            )

            request.state.tenant = tenant
            request.state.tenant_id = tenant.tenant_id

            response = await call_next(request)
            return response

        # Normal authentication flow.
        api_key = request.headers.get("X-API-Key")
        tenant_id = request.headers.get("X-Tenant-ID")

        # At least one authentication mechanism is required.
        if not api_key and not tenant_id:
            raise HTTPException(
                status_code=401,
                detail="Missing tenant authentication",
            )

        # If an API key is provided, resolve the tenant.
        # NOTE:
        # This is currently a simplified implementation.
        # In production, the API key should be validated against
        # the tenant store/database using the project's authentication logic.
        if api_key:
            tenant = Tenant(
                tenant_id="dummy",
                name="Dummy",
                status=TenantStatus.ACTIVE,
                api_keys=[
                    {
                        "key": api_key,
                        "is_active": True,
                    }
                ],
            )

            request.state.tenant = tenant
            request.state.tenant_id = tenant.tenant_id

        # If only Tenant ID is provided, create/use the corresponding tenant.
        elif tenant_id:
            tenant = Tenant(
                tenant_id=tenant_id,
                name=f"Tenant {tenant_id}",
                status=TenantStatus.ACTIVE,
                api_keys=[],
            )

            request.state.tenant = tenant
            request.state.tenant_id = tenant.tenant_id

        else:
            raise HTTPException(
                status_code=401,
                detail="Invalid tenant authentication",
            )

        response = await call_next(request)
        return response