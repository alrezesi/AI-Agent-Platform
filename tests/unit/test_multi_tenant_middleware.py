# tests/unit/test_multi_tenant_middleware.py
# Unit tests for tenant middleware using httpx.AsyncClient

import pytest
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient

from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.multi_tenant.middleware import TenantMiddleware


@pytest.fixture
def tenant_manager():
    class Storage:
        _tenants = {}
    manager = TenantManager(Storage())
    return manager


@pytest.fixture
def app(tenant_manager):
    app = FastAPI()
    app.add_middleware(TenantMiddleware, tenant_manager=tenant_manager)

    @app.get("/test")
    async def test_endpoint(request: Request):
        return {"tenant_id": getattr(request.state, "tenant_id", None)}

    return app


@pytest.mark.asyncio
async def test_middleware_with_api_key_only(app, tenant_manager):
    """API key alone must authenticate the tenant."""
    tenant = await tenant_manager.create_tenant("Test")
    api_key = await tenant_manager.generate_api_key(tenant.tenant_id)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-API-Key": api_key})
    assert response.status_code == 200
    assert response.json()["tenant_id"] == tenant.tenant_id


@pytest.mark.asyncio
async def test_middleware_rejects_tenant_id_without_api_key(app, tenant_manager):
    """X-Tenant-ID alone must NOT authenticate — prevents impersonation."""
    tenant = await tenant_manager.create_tenant("Test")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-Tenant-ID": tenant.tenant_id})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_middleware_rejects_tenant_hint_mismatch(app, tenant_manager):
    """X-Tenant-ID that doesn't match the API key tenant must be rejected."""
    tenant_a = await tenant_manager.create_tenant("A")
    tenant_b = await tenant_manager.create_tenant("B")
    api_key_b = await tenant_manager.generate_api_key(tenant_b.tenant_id)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-API-Key": api_key_b, "X-Tenant-ID": tenant_a.tenant_id})
    # API key belongs to tenant B, but hint says tenant A — should be rejected
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_middleware_with_api_key(app, tenant_manager):
    tenant = await tenant_manager.create_tenant("Test")
    api_key = await tenant_manager.generate_api_key(tenant.tenant_id)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-API-Key": api_key})
    assert response.status_code == 200
    assert response.json()["tenant_id"] == tenant.tenant_id

