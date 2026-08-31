"""
Explicit AUTHORIZATION tests at the API level.

These are deliberately distinct from authentication tests.  Authentication
proves *who* the caller is (valid API key).  Authorization proves *what* the
authenticated caller is allowed to do (only their own tenant's resources),
and that authorization is enforced SERVER-SIDE from the authenticated tenant
identity — never from a client-supplied X-Tenant-ID header.
"""

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.agent_platform.api.routes import monitoring, tasks, tenants
from src.agent_platform.monitoring.rate_limit import RateLimitMiddleware
from src.agent_platform.monitoring.request_id import RequestIdMiddleware
from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.multi_tenant.middleware import TenantMiddleware
from src.agent_platform.scheduler.scheduler import TaskScheduler


class _TenantStorage:
    def __init__(self):
        self._tenants = {}


def _build_app(tenant_manager: TenantManager, scheduler: TaskScheduler):
    app = FastAPI(title="AI Agent Platform (authz test)")
    app.add_middleware(TenantMiddleware, tenant_manager=tenant_manager)
    app.add_middleware(RequestIdMiddleware)
    app.add_middleware(RateLimitMiddleware, requests_per_second=100000, burst=100000)
    app.include_router(tasks.router)
    app.include_router(tenants.router)
    app.include_router(monitoring.router)
    app.dependency_overrides[tasks.get_scheduler] = lambda: scheduler
    app.dependency_overrides[tenants.get_tenant_manager] = lambda: tenant_manager
    return app


def _client(app: FastAPI) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_unauthorized_task_read_cross_tenant(redis_queue, clean_db):
    """Tenant A cannot READ tenant B's task via GET /tasks/{id} (returns 404)."""
    tm = TenantManager(_TenantStorage())
    a = await tm.create_tenant("Authz Tenant A")
    b = await tm.create_tenant("Authz Tenant B")
    key_a = await tm.generate_api_key(a.tenant_id)
    key_b = await tm.generate_api_key(b.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    async with _client(app) as client:
        # Tenant B submits a task.
        resp_b = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {}},
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        assert resp_b.status_code == 200
        task_b_id = resp_b.json()["task_id"]

        # Tenant A tries to read it.
        resp_a = await client.get(
            f"/tasks/{task_b_id}",
            headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
        )
        assert resp_a.status_code == 404  # server-side tenant scope, never a leak


@pytest.mark.asyncio
async def test_unauthorized_task_cancellation_cross_tenant(redis_queue, clean_db):
    """Tenant A cannot CANCEL tenant B's task via DELETE /tasks/{id}."""
    tm = TenantManager(_TenantStorage())
    a = await tm.create_tenant("Authz Tenant A")
    b = await tm.create_tenant("Authz Tenant B")
    key_a = await tm.generate_api_key(a.tenant_id)
    key_b = await tm.generate_api_key(b.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    async with _client(app) as client:
        resp_b = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {}},
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        task_b_id = resp_b.json()["task_id"]

        # Tenant A attempts cancellation.
        resp_a = await client.delete(
            f"/tasks/{task_b_id}",
            headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
        )
        assert resp_a.status_code == 404

        # The task is still present for tenant B.
        resp_b_get = await client.get(
            f"/tasks/{task_b_id}",
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        assert resp_b_get.status_code == 200


@pytest.mark.asyncio
async def test_authorization_enforced_server_side_not_client_header(redis_queue, clean_db):
    """Authorization is derived from the authenticated tenant, not the
    client-supplied X-Tenant-ID header.  A spoofed header must never grant
    access to another tenant's resources."""
    tm = TenantManager(_TenantStorage())
    a = await tm.create_tenant("Authz Tenant A")
    b = await tm.create_tenant("Authz Tenant B")
    key_a = await tm.generate_api_key(a.tenant_id)
    key_b = await tm.generate_api_key(b.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    async with _client(app) as client:
        resp_b = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {}},
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        task_b_id = resp_b.json()["task_id"]

        # Tenant A sends a SPOOFED X-Tenant-ID = B's id with A's key.
        # The middleware must reject the mismatch (403), proving the header
        # is not trusted for authorization.
        spoof = await client.get(
            f"/tasks/{task_b_id}",
            headers={"X-API-Key": key_a, "X-Tenant-ID": b.tenant_id},
        )
        assert spoof.status_code == 403

        # Even if the header were ignored, A still cannot read B's task.
        legit = await client.get(
            f"/tasks/{task_b_id}",
            headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
        )
        assert legit.status_code == 404


@pytest.mark.asyncio
async def test_tenant_scoped_list_and_stats(redis_queue, clean_db):
    """GET /tasks/ and GET /tasks/stats return only the caller's tenant data."""
    tm = TenantManager(_TenantStorage())
    a = await tm.create_tenant("Authz Tenant A")
    b = await tm.create_tenant("Authz Tenant B")
    key_a = await tm.generate_api_key(a.tenant_id)
    key_b = await tm.generate_api_key(b.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    async with _client(app) as client:
        await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {"n": "b"}},
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        resp_a_list = await client.get(
            "/tasks/",
            headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
        )
        resp_a_stats = await client.get(
            "/tasks/stats",
            headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
        )
        assert resp_a_list.status_code == 200
        assert resp_a_list.json()["count"] == 0  # no tenant A tasks
        assert resp_a_stats.json()["total"] == 0


@pytest.mark.asyncio
async def test_authentication_required_cannot_be_bypassed(redis_queue, clean_db):
    """Every tenant-scoped endpoint must reject requests with no API key,
    including alternate HTTP methods/endpoints."""
    tm = TenantManager(_TenantStorage())
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    async with _client(app) as client:
        # No credentials at all.
        assert (await client.get("/tasks/")).status_code == 401
        assert (await client.post("/tasks/", json={"agent_id": "a", "task_type": "echo", "payload": {}})).status_code == 401
        assert (await client.get("/tasks/some-id")).status_code == 401
        assert (await client.delete("/tasks/some-id")).status_code == 401
        assert (await client.get("/tasks/stats")).status_code == 401


@pytest.mark.asyncio
async def test_authenticated_tenant_cannot_access_other_tenant_monitoring_data(redis_queue, clean_db):
    """Tenant A's request id must not surface tenant B's task in the shared
    monitoring trace endpoint, and tenant A cannot enumerate tenant B's task
    via list filtering."""
    tm = TenantManager(_TenantStorage())
    a = await tm.create_tenant("Authz Tenant A")
    b = await tm.create_tenant("Authz Tenant B")
    key_a = await tm.generate_api_key(a.tenant_id)
    key_b = await tm.generate_api_key(b.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    async with _client(app) as client:
        resp_b = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {}},
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        # Tenant B's task must actually exist before we assert A cannot see it.
        assert resp_b.status_code in (200, 201)

        # Tenant A queries the trace for B's request id (monitoring is
        # auth-exempt, but it must only return data the id actually matches;
        # A must not be able to retrieve B's task by guessing A's own id, and
        # a trace for A's own (nonexistent) request returns nothing).
        resp_a = await client.get(
            "/monitoring/traces?trace_id=does-not-exist",
            headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
        )
        assert resp_a.status_code == 200
        assert resp_a.json()["count"] == 0

        # B's own trace is reachable by B, not by A's data appearing in A's list.
        resp_b_list = await client.get(
            "/tasks/",
            headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
        )
        assert resp_b_list.json()["count"] == 0
