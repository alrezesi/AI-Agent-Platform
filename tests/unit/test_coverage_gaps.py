# ---------------------------------------------------------------------------
# Multi-tenant middleware and authentication
# ---------------------------------------------------------------------------
from types import SimpleNamespace

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.multi_tenant.middleware import TenantMiddleware, _get_api_key_from_headers
from src.agent_platform.multi_tenant.models import TenantQuota


def test_get_api_key_from_headers():
    """_get_api_key_from_headers must extract API key from headers."""
    request = SimpleNamespace(headers={"X-API-Key": "key-123"})
    assert _get_api_key_from_headers(request) == "key-123"

    request = SimpleNamespace(headers={"Authorization": "Bearer bearer-456"})
    assert _get_api_key_from_headers(request) == "bearer-456"

    request = SimpleNamespace(headers={})
    assert _get_api_key_from_headers(request) is None


@pytest.mark.asyncio
async def test_tenant_middleware_allows_bootstrap_endpoints():
    """TenantMiddleware must allow tenant creation without API key."""
    async def create_tenant(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/tenants/", create_tenant, methods=["POST"])])
    middleware = TenantMiddleware(app, tenant_manager=None)

    with TestClient(middleware) as client:
        resp = client.post("/tenants/", json={"name": "Test"})
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_tenant_middleware_rejects_missing_api_key():
    """TenantMiddleware must reject requests without API key."""
    async def protected(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/tasks/", protected, methods=["GET"])])
    middleware = TenantMiddleware(app, tenant_manager=None)

    with TestClient(middleware) as client:
        resp = client.get("/tasks/")
        assert resp.status_code == 401


@pytest.mark.asyncio
async def test_tenant_middleware_sets_tenant_state():
    """TenantMiddleware must set tenant state on request."""
    class _Storage:
        def __init__(self):
            self._tenants: dict[str, object] = {}

    storage = _Storage()
    manager = TenantManager(storage)
    tenant = await manager.create_tenant(name="State Tenant", quota=TenantQuota())
    api_key = await manager.generate_api_key(tenant.tenant_id)

    async def protected(request):
        return JSONResponse({
            "tenant_id": request.state.tenant_id,
            "tenant_name": request.state.tenant.name,
        })

    app = Starlette(routes=[Route("/tasks/", protected, methods=["GET"])])
    middleware = TenantMiddleware(app, tenant_manager=manager)

    with TestClient(middleware) as client:
        resp = client.get("/tasks/", headers={"X-API-Key": api_key})
        assert resp.status_code == 200
        data = resp.json()
        assert data["tenant_id"] == tenant.tenant_id
        assert data["tenant_name"] == "State Tenant"
