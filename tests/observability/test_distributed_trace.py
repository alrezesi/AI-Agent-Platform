"""
Distributed trace acceptance test (real API flow, real Redis + PostgreSQL).

This test proves an operator can start from a Request ID and navigate the
EXISTING observable system to:

    Request ID -> Task ID -> Tenant ID -> Queue Message ID ->
    Worker ID   -> Execution ID -> Retry history -> Final Result

It deliberately forces a retry (worker crash / lease expiry -> reclaim) and
verifies the retry root-cause is recorded without reading source code.

No fake trace object is created: the trace is assembled from the real
PostgreSQL + Redis task store via build_task_trace() and is also exposed
through the existing /monitoring/traces interface.
"""

import asyncio

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.agent_platform.api.routes import monitoring, tasks, tenants
from src.agent_platform.api.routes.monitoring import get_dashboard_api
from src.agent_platform.core.task import TaskStatus
from src.agent_platform.monitoring.rate_limit import RateLimitMiddleware
from src.agent_platform.monitoring.request_id import RequestIdMiddleware
from src.agent_platform.monitoring.task_trace import build_task_trace, verify_trace_chain
from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.multi_tenant.middleware import TenantMiddleware
from src.agent_platform.scheduler.scheduler import TaskScheduler


def _make_client(app: FastAPI) -> AsyncClient:
    """Async client sharing the test event loop (avoids cross-loop engines)."""
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


class _TenantStorage:
    def __init__(self):
        self._tenants = {}


def _build_app(tenant_manager: TenantManager, scheduler: TaskScheduler, rate_per_second: int = 100000):
    app = FastAPI(title="AI Agent Platform (trace test)")
    app.add_middleware(TenantMiddleware, tenant_manager=tenant_manager)
    app.add_middleware(RequestIdMiddleware)
    app.add_middleware(RateLimitMiddleware, requests_per_second=rate_per_second, burst=rate_per_second)
    app.include_router(tasks.router)
    app.include_router(tenants.router)
    app.include_router(monitoring.router)

    app.dependency_overrides[tasks.get_scheduler] = lambda: scheduler
    app.dependency_overrides[tenants.get_tenant_manager] = lambda: tenant_manager

    def _dashboard():
        from src.agent_platform.monitoring.dashboard import DashboardAPI
        from src.agent_platform.monitoring.logging import LogManager
        from src.agent_platform.monitoring.metrics import MetricRegistry, MetricsCollector
        from src.agent_platform.monitoring.tracing import Tracer

        return DashboardAPI(MetricsCollector(MetricRegistry()), Tracer(), LogManager(), None, scheduler)

    app.dependency_overrides[get_dashboard_api] = _dashboard
    return app


@pytest.mark.asyncio
async def test_distributed_trace_request_to_final_result_with_retry(redis_queue, clean_db):
    """End-to-end trace: request -> task -> tenant -> message -> worker ->
    execution -> retry -> final result, using the real API + queue."""
    tm = TenantManager(_TenantStorage())
    tenant = await tm.create_tenant("Trace Tenant")
    api_key = await tm.generate_api_key(tenant.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    # 1. Send a real task request through the API.
    async with _make_client(app) as client:
        response = await client.post(
            "/tasks/",
            json={"agent_id": "trace-agent", "task_type": "echo", "payload": {"value": 42}},
            headers={"X-API-Key": api_key, "X-Tenant-ID": tenant.tenant_id},
        )
    assert response.status_code == 200, response.text
    body = response.json()
    task_id = body["task_id"]

    # 2. Capture the Request ID from the response header.
    request_id = response.headers.get("X-Request-ID")
    assert request_id, "Request ID must be echoed in the response"

    # 3. Tenant ID known from the authenticated tenant.
    tenant_id = tenant.tenant_id

    # 4/5. Worker A claims the task (short lease) -> identifies Queue Message ID
    #      and Worker ID and Execution ID.
    claim1 = await redis_queue.dequeue(worker_id="worker-A", lease_seconds=0.5)
    assert claim1 is not None
    assert claim1.task_id == task_id
    message_id = claim1.message_id
    execution_id_1 = claim1.execution_id
    assert message_id is not None
    assert execution_id_1 is not None
    assert claim1.lease_owner == "worker-A"

    # 6. Force a retry: let the lease expire, then reclaim (worker crash model).
    await asyncio.sleep(0.7)
    reclaimed = await redis_queue.reclaim_orphaned_tasks()
    assert task_id in reclaimed

    # 7. Worker B re-claims and completes the task.
    claim2 = await redis_queue.dequeue(worker_id="worker-B", lease_seconds=60)
    assert claim2 is not None
    assert claim2.task_id == task_id
    execution_id_2 = claim2.execution_id
    assert execution_id_2 != execution_id_1  # new execution identity

    claim2.status = TaskStatus.COMPLETED
    claim2.result = {"value": 42, "ok": True}
    await redis_queue.update_task(claim2)

    # 8. Build the trace from the real store, starting at the Request ID.
    #    ``build_task_trace`` requires a tenant scope; pass the
    #    authenticated tenant id so the trace is filtered to it.
    nodes = await build_task_trace(redis_queue, request_id, tenant_id=tenant_id)
    assert len(nodes) == 1
    node = nodes[0]

    # Verify every identifier belongs to the SAME logical task.
    verified = verify_trace_chain(nodes, request_id=request_id, task_id=task_id)
    assert verified["found"]
    assert verified["consistent"], verified
    assert verified["request_id"] == request_id
    assert verified["task_id"] == task_id
    assert verified["tenant_id"] == tenant_id
    assert verified["message_id"] == message_id
    assert verified["execution_id"] == execution_id_2  # current execution
    assert verified["status"] == "completed"
    assert verified["result"] == {"value": 42, "ok": True}

    # 9/10/11. The retry must be recorded with a reason, observable without
    # reading source code.
    assert node["retry_count"] == 1
    history = node["retry_history"]
    assert isinstance(history, list) and len(history) == 1
    retry = history[0]
    assert retry["retry_number"] == 1
    assert retry["worker_id"] == "worker-A"          # worker that held the lost lease
    assert retry["execution_id"] == execution_id_1  # the abandoned execution
    assert retry["previous_state"] == "running"
    assert retry["lease_expired"] is True
    assert retry["error_category"] == "lease_expired"
    assert "lease" in retry["reason"].lower()        # human-readable root cause
    assert retry["next_retry_decision"] == "requeue"

    # Cross-check: the trace is also exposed via the existing monitoring
    # interface (/monitoring/traces?trace_id=...).  The endpoint now
    # requires authentication (the audit closed the unauthenticated
    # cross-tenant IDOR), so pass the caller's API key.
    async with _make_client(app) as client:
        trace_resp = await client.get(
            f"/monitoring/traces?trace_id={request_id}",
            headers={"X-API-Key": api_key, "X-Tenant-ID": tenant_id},
        )
    assert trace_resp.status_code == 200
    trace_body = trace_resp.json()
    assert trace_body.get("source") == "task_store"
    assert trace_body["count"] == 1
    assert trace_body["traces"][0]["task_id"] == task_id


@pytest.mark.asyncio
async def test_trace_navigation_from_task_id_and_message_id(redis_queue, clean_db):
    """The trace must be navigable from task_id and message_id too."""
    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task("agent", "echo", {"x": 1}, task_id="trace-nav-001", tenant_id="tenant-nav")

    submitted = await redis_queue.get_task("trace-nav-001")
    assert submitted.message_id is not None

    by_task = await build_task_trace(redis_queue, "trace-nav-001", tenant_id="tenant-nav")
    assert len(by_task) == 1
    assert by_task[0]["task_id"] == "trace-nav-001"

    by_message = await build_task_trace(redis_queue, submitted.message_id, tenant_id="tenant-nav")
    assert len(by_message) == 1
    assert by_message[0]["task_id"] == "trace-nav-001"


@pytest.mark.asyncio
async def test_trace_records_explicit_failure_reason(redis_queue, clean_db):
    """An explicit worker failure records a retry root-cause entry."""
    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task("agent", "echo", {}, task_id="trace-fail-001", tenant_id="tenant-fail")

    claim = await redis_queue.dequeue(worker_id="worker-X", lease_seconds=30)
    assert claim is not None
    exec_id = claim.execution_id

    # Worker fails with a category + reason (operator-readable).
    claim.record_failure(
        worker_id=claim.lease_owner,
        execution_id=claim.execution_id,
        error_category="transient",
        reason="Downstream dependency returned 503; will retry.",
        lease_expired=False,
    )
    await redis_queue.update_task(claim)

    node = (await build_task_trace(redis_queue, "trace-fail-001", tenant_id="tenant-fail"))[0]
    assert node["error_category"] == "transient"
    assert len(node["retry_history"]) == 1
    assert node["retry_history"][0]["error_category"] == "transient"
    assert node["retry_history"][0]["execution_id"] == exec_id
    assert node["retry_history"][0]["reason"] == "Downstream dependency returned 503; will retry."
    assert node["retry_history"][0]["lease_expired"] is False


@pytest.mark.asyncio
async def test_monitoring_trace_endpoint_requires_no_auth_leak(redis_queue, clean_db):
    """The monitoring trace endpoint must not leak secrets (API keys)
    even though it now requires authentication.

    NOTE: the audit tightened ``/monitoring/*`` to require an API key
    (closes an unauthenticated cross-tenant IDOR).  The test therefore
    authenticates before reading the trace and asserts that the response
    body — even when populated with the caller's task — never contains
    secret material.
    """
    tm = TenantManager(_TenantStorage())
    tenant = await tm.create_tenant("Trace Leak Tenant")
    api_key = await tm.generate_api_key(tenant.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)

    async with _make_client(app) as client:
        resp = await client.post(
            "/tasks/",
            json={"agent_id": "a", "task_type": "echo", "payload": {"k": "v"}},
            headers={"X-API-Key": api_key, "X-Tenant-ID": tenant.tenant_id},
        )
        assert resp.status_code == 200
        rid = resp.headers["X-Request-ID"]

        # Unauthenticated callers must be rejected (401) — no trace data
        # leaked to anyone without a key.
        no_auth = await client.get(f"/monitoring/traces?trace_id={rid}")
        assert no_auth.status_code == 401, no_auth.status_code

        # Authenticated callers get the trace but the body must still
        # never contain secret material.
        trace = await client.get(
            f"/monitoring/traces?trace_id={rid}",
            headers={"X-API-Key": api_key, "X-Tenant-ID": tenant.tenant_id},
        )
    assert trace.status_code == 200
    dumped = trace.text
    assert api_key not in dumped
    assert "agent123" not in dumped
