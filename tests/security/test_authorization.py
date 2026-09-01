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

    # ``/monitoring/*`` resolves its data source through the dashboard
    # dependency.  The test wires a real Redis-backed scheduler into the
    # dashboard so the trace endpoint actually consults the same queue
    # the tasks were submitted to (instead of the test-suite singleton
    # InMemoryTaskQueue).
    from src.agent_platform.api.routes.monitoring import get_dashboard_api
    from src.agent_platform.monitoring.dashboard import DashboardAPI
    from src.agent_platform.monitoring.logging import LogManager
    from src.agent_platform.monitoring.metrics import MetricRegistry, MetricsCollector
    from src.agent_platform.monitoring.tracing import Tracer

    def _dashboard():
        return DashboardAPI(
            MetricsCollector(MetricRegistry()),
            Tracer(),
            LogManager(),
            None,
            scheduler,
        )

    app.dependency_overrides[get_dashboard_api] = _dashboard
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
    """Regression test for the audit's cross-tenant /monitoring/* IDOR.

    The previous version of this test only asserted that a *wrong/guessed*
    trace_id returned nothing.  That is the easy half of the bug.  The
    hard half — closed here — is the unauthenticated / cross-tenant case:

        1. Tenant B submits a real task; we capture B's real task_id,
           request_id, message_id and execution_id.
        2. An unauthenticated caller (NO API key) asks for B's real
           task_id and request_id via /monitoring/traces — both must be
           rejected (401), never returned.
        3. Tenant A (a *different* tenant, with a valid API key) asks
           for B's real task_id and request_id via /monitoring/traces —
           both must return zero results (B's data is not leaked under
           A's tenant scope).
        4. Tenant B, asking with B's own API key, sees B's own task
           (sanity check that the fix didn't accidentally lock out
           legitimate same-tenant reads).

    This exercises the same real PostgreSQL + Redis stack as the rest of
    the suite, with no mocks.
    """
    tm = TenantManager(_TenantStorage())
    a = await tm.create_tenant("Authz Tenant A")
    b = await tm.create_tenant("Authz Tenant B")
    key_a = await tm.generate_api_key(a.tenant_id)
    key_b = await tm.generate_api_key(b.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    # Secret payload B submits — this is what an attacker would be trying
    # to exfiltrate.  The test MUST confirm the string never appears in
    # any response sent to A or to an unauthenticated caller.
    b_secret = "B-payload-SECRET-do-not-leak"

    async with _client(app) as client:
        resp_b = await client.post(
            "/tasks/",
            json={
                "agent_id": "agent",
                "task_type": "echo",
                "payload": {"secret": b_secret},
            },
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        assert resp_b.status_code == 200, resp_b.text
        body_b = resp_b.json()
        task_b_id = body_b["task_id"]
        request_b_id = resp_b.headers["X-Request-ID"]
        assert task_b_id and request_b_id

        # Capture the message_id and execution_id as well — the bug
        # originally leaked data through every one of these IDs.
        b_task = await redis_queue.get_task(task_b_id)
        assert b_task is not None
        message_b_id = b_task.message_id
        execution_b_id = b_task.execution_id

        # ------------------------------------------------------------------
        # 1. Unauthenticated callers — no API key at all — must NEVER see B's
        #    data, no matter which id they guess.
        # ------------------------------------------------------------------
        for trace_id in (task_b_id, request_b_id, message_b_id, execution_b_id):
            no_auth = await client.get(
                f"/monitoring/traces?trace_id={trace_id}"
            )
            assert no_auth.status_code == 401, (
                f"unauthenticated /monitoring/traces must return 401, "
                f"got {no_auth.status_code} for trace_id={trace_id}"
            )
            assert b_secret not in no_auth.text

        # ------------------------------------------------------------------
        # 2. Tenant A with a VALID API key — different tenant — must also
        #    NEVER see B's data.  Both ``count == 0`` and the secret string
        #    must be absent from the response.
        # ------------------------------------------------------------------
        for trace_id in (task_b_id, request_b_id, message_b_id, execution_b_id):
            resp_a = await client.get(
                f"/monitoring/traces?trace_id={trace_id}",
                headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
            )
            assert resp_a.status_code == 200, resp_a.status_code
            body_a = resp_a.json()
            # The trace MUST be empty for tenant A — B's data must not
            # leak under A's scope, even though A holds a real API key.
            assert body_a.get("count", 0) == 0, (
                f"tenant A must not see tenant B's task via "
                f"/monitoring/traces for id={trace_id}; got {body_a}"
            )
            assert b_secret not in resp_a.text, (
                f"tenant B's secret leaked to tenant A via "
                f"/monitoring/traces trace_id={trace_id}"
            )

        # ------------------------------------------------------------------
        # 3. Tenant B with B's own key — sanity check that the fix didn't
        #    lock out legitimate same-tenant reads.  The trace node
        #    intentionally does NOT carry the raw payload (that is the
        #    existing operator-facing shape), but it MUST carry B's
        #    task_id under B's tenant_id — i.e. the trace endpoint is
        #    reachable and returns B's task to B.
        # ------------------------------------------------------------------
        resp_b_self = await client.get(
            f"/monitoring/traces?trace_id={task_b_id}",
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        assert resp_b_self.status_code == 200
        body_b_self = resp_b_self.json()
        assert body_b_self["count"] >= 1
        # The trace node belongs to tenant B (never leaked to A).
        node_b = body_b_self["traces"][0]
        assert node_b["task_id"] == task_b_id
        assert node_b["tenant_id"] == b.tenant_id
        # The raw payload is intentionally not exposed via the trace
        # node — but B's other identifiers ARE, confirming the endpoint
        # is reachable for B and returns B's data.
        assert node_b["request_id"] == request_b_id

        # ------------------------------------------------------------------
        # 4. Tenant A's listing of their own tasks must not surface B's task.
        # ------------------------------------------------------------------
        resp_a_list = await client.get(
            "/tasks/",
            headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
        )
        assert resp_a_list.json()["count"] == 0


@pytest.mark.asyncio
async def test_monitoring_endpoints_require_authentication(redis_queue, clean_db):
    """Every /monitoring/* endpoint that can return tenant data must reject
    unauthenticated callers with 401.

    Closes the audit's primary finding: ``/monitoring/*`` was auth-exempt
    in ``TenantMiddleware``.  An attacker who could reach the network port
    could enumerate tenant task payloads via ``/monitoring/traces``,
    ``/monitoring/tasks``, etc.
    """
    tm = TenantManager(_TenantStorage())
    a = await tm.create_tenant("Monitoring Auth Tenant A")
    b = await tm.create_tenant("Monitoring Auth Tenant B")
    key_a = await tm.generate_api_key(a.tenant_id)
    key_b = await tm.generate_api_key(b.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    async with _client(app) as client:
        # Seed tenant B with a real task so /monitoring/traces has data
        # the unauthenticated caller might otherwise retrieve.
        resp_b = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo",
                  "payload": {"x": "y", "secret": "B-payload"}},
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        assert resp_b.status_code == 200
        task_b_id = resp_b.json()["task_id"]
        request_b_id = resp_b.headers["X-Request-ID"]

        # Every tenant-data endpoint must require authentication.
        endpoints = [
            ("GET", "/monitoring/status"),
            ("GET", "/monitoring/agents"),
            ("GET", "/monitoring/tasks"),
            ("GET", "/monitoring/metrics"),
            ("GET", f"/monitoring/traces?trace_id={task_b_id}"),
            ("GET", f"/monitoring/traces?trace_id={request_b_id}"),
            ("GET", "/monitoring/logs"),
            ("GET", "/monitoring/health"),
        ]
        for method, path in endpoints:
            resp = await client.request(method, path)
            assert resp.status_code == 401, (
                f"unauthenticated {method} {path} must return 401, "
                f"got {resp.status_code}"
            )
            assert "B-payload" not in resp.text, (
                f"unauthenticated {method} {path} leaked B's payload"
            )

        # Sanity: an authenticated caller CAN still reach each endpoint
        # (status 200).  This protects against an over-aggressive fix
        # that would lock out legitimate same-tenant callers.
        for _method, path in endpoints:
            resp = await client.request(
                _method, path,
                headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
            )
            assert resp.status_code == 200, (
                f"authenticated {_method} {path} must return 200, "
                f"got {resp.status_code}"
            )


@pytest.mark.asyncio
async def test_monitoring_tasks_endpoint_is_tenant_scoped(redis_queue, clean_db):
    """``GET /monitoring/tasks`` must return only the caller's tenant's
    task statistics.  Tenant A must never see tenant B's counts.
    """
    tm = TenantManager(_TenantStorage())
    a = await tm.create_tenant("Monitoring Stats Tenant A")
    b = await tm.create_tenant("Monitoring Stats Tenant B")
    key_a = await tm.generate_api_key(a.tenant_id)
    key_b = await tm.generate_api_key(b.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    async with _client(app) as client:
        # Tenant B submits 3 tasks; tenant A submits 0.
        for _ in range(3):
            r = await client.post(
                "/tasks/",
                json={"agent_id": "agent", "task_type": "echo",
                      "payload": {"i": 1}},
                headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
            )
            assert r.status_code == 200

        stats_a = await client.get(
            "/monitoring/tasks",
            headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
        )
        assert stats_a.status_code == 200
        body_a = stats_a.json()
        assert body_a["total"] == 0, (
            f"tenant A must not see tenant B's 3 tasks; got {body_a}"
        )

        stats_b = await client.get(
            "/monitoring/tasks",
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        assert stats_b.status_code == 200
        body_b = stats_b.json()
        assert body_b["total"] == 3, (
            f"tenant B must see their own 3 tasks; got {body_b}"
        )


@pytest.mark.asyncio
async def test_monitoring_logs_endpoint_is_tenant_scoped(redis_queue, clean_db):
    """``GET /monitoring/logs`` must scope the response to the caller's
    tenant.  An attacker with tenant A's key must not be able to obtain
    tenant B's logs.
    """
    tm = TenantManager(_TenantStorage())
    a = await tm.create_tenant("Monitoring Logs Tenant A")
    b = await tm.create_tenant("Monitoring Logs Tenant B")
    key_a = await tm.generate_api_key(a.tenant_id)
    key_b = await tm.generate_api_key(b.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    async with _client(app) as client:
        # No-key: 401.
        no_auth = await client.get("/monitoring/logs")
        assert no_auth.status_code == 401

        # Tenant B: response must echo tenant B's id, not A's.
        resp_b = await client.get(
            "/monitoring/logs",
            headers={"X-API-Key": key_b, "X-Tenant-ID": b.tenant_id},
        )
        assert resp_b.status_code == 200
        assert resp_b.json().get("tenant_id") == b.tenant_id

        # Tenant A: response must echo tenant A's id, not B's.
        resp_a = await client.get(
            "/monitoring/logs",
            headers={"X-API-Key": key_a, "X-Tenant-ID": a.tenant_id},
        )
        assert resp_a.status_code == 200
        assert resp_a.json().get("tenant_id") == a.tenant_id
