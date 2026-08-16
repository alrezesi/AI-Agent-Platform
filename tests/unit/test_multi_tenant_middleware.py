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
async def test_middleware_with_valid_tenant_header(app, tenant_manager):
    tenant = await tenant_manager.create_tenant("Test")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-Tenant-ID": tenant.tenant_id})
    assert response.status_code == 200
    assert response.json()["tenant_id"] == tenant.tenant_id


@pytest.mark.asyncio
async def test_middleware_with_api_key(app, tenant_manager):
    tenant = await tenant_manager.create_tenant("Test")
    api_key = await tenant_manager.generate_api_key(tenant.tenant_id)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-API-Key": api_key})
    assert response.status_code == 200
    assert response.json()["tenant_id"] == tenant.tenant_id

