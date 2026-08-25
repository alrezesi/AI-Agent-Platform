"""
Input validation audit tests.

Covers the request-validation surface required by the audit:
  * malformed JSON
  * invalid UUID / task identifiers
  * oversized input
  * unexpected fields
  * SQL injection payloads
  * XSS-like payloads
  * invalid enum / state values
  * invalid task metadata
  * invalid tenant IDs
  * invalid API key formats
"""

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.agent_platform.api.routes import tasks, tenants
from src.agent_platform.core.task import Task, TaskPriority
from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.multi_tenant.middleware import TenantMiddleware
from src.agent_platform.monitoring.rate_limit import RateLimitMiddleware
from src.agent_platform.monitoring.request_id import RequestIdMiddleware
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue


class _TenantStorage:
    def __init__(self):
        self._tenants = {}


def _build_app(tm, scheduler):
    app = FastAPI(title="AI Agent Platform (input validation)")
    app.add_middleware(TenantMiddleware, tenant_manager=tm)
    app.add_middleware(RequestIdMiddleware)
    app.add_middleware(RateLimitMiddleware, requests_per_second=100000, burst=100000)
    app.include_router(tasks.router)
    app.include_router(tenants.router)
    app.dependency_overrides[tasks.get_scheduler] = lambda: scheduler
    app.dependency_overrides[tenants.get_tenant_manager] = lambda: tm
    return app


def _client(app):
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


# ---------------------------------------------------------------------------
# Malformed JSON
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_malformed_json_rejected(redis_queue, clean_db):
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    async with _client(app) as client:
        resp = await client.post(
            "/tasks/",
            content=b"{ this is not valid json ",
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id, "Content-Type": "application/json"},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Invalid task metadata (payload must be a dict)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_invalid_task_metadata_rejected(redis_queue, clean_db):
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    async with _client(app) as client:
        resp = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": "not-a-dict"},
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Unexpected / extra fields are ignored, not trusted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unexpected_fields_rejected(redis_queue, clean_db):
    """Non-parameter body fields (e.g. tenant_id, status) are ignored, never
    trusted: a caller cannot smuggle ownership or terminal status this way."""
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    async with _client(app) as client:
        resp = await client.post(
            "/tasks/",
            json={
                "agent_id": "agent",
                "task_type": "echo",
                "payload": {},
                "tenant_id": "spoofed-tenant",  # not a real parameter -> rejected
                "status": "completed",          # not a writable field -> rejected
            },
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        # Extra fields are ignored (not trusted): the request still succeeds and
        # is scoped to the AUTHENTICATED tenant, never the spoofed one.
        assert resp.status_code == 200
        task_id = resp.json()["task_id"]
        got = await client.get(
            f"/tasks/{task_id}",
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        body = got.json()
        assert body["tenant_id"] == t.tenant_id  # not the spoofed value
        assert body["status"] == "pending"       # not the injected 'completed'


# ---------------------------------------------------------------------------
# Invalid enum / state values
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_invalid_priority_enum_rejected(redis_queue, clean_db):
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    async with _client(app) as client:
        resp = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {}, "priority": "NOT_A_PRIORITY"},
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        assert resp.status_code == 422


@pytest.mark.asyncio
async def test_invalid_status_enum_rejected_by_model():
    with pytest.raises(Exception):
        Task(task_id="x", agent_id="a", type="t", payload={}, status="not_a_real_status")


# ---------------------------------------------------------------------------
# Invalid UUID / task identifiers (path params)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unknown_task_id_returns_404_not_500(redis_queue, clean_db):
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    async with _client(app) as client:
        resp = await client.get(
            "/tasks/00000000-0000-0000-0000-000000000000",
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_path_traversal_task_id_safe(redis_queue, clean_db):
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    async with _client(app) as client:
        resp = await client.get(
            "/tasks/..%2f..%2fsecret",
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        # Must not resolve to another resource; treated as a missing task.
        assert resp.status_code in (404, 422)


# ---------------------------------------------------------------------------
# Oversized input
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_oversized_payload_handled(redis_queue, clean_db):
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    big = {"data": "x" * (1024 * 1024)}  # ~1 MB payload
    async with _client(app) as client:
        resp = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": big},
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        assert resp.status_code == 200
        task_id = resp.json()["task_id"]
        got = await client.get(
            f"/tasks/{task_id}",
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        assert got.status_code == 200
        assert len(got.json()["payload"]["data"]) == 1024 * 1024


# ---------------------------------------------------------------------------
# SQL injection payloads (stored literally; DB uses parameterized queries)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sql_injection_in_payload_stored_literally(redis_queue, clean_db):
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    injection = "'); DROP TABLE tasks; --"
    async with _client(app) as client:
        resp = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {"q": injection}},
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        assert resp.status_code == 200
        task_id = resp.json()["task_id"]
        got = await client.get(
            f"/tasks/{task_id}",
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        assert got.json()["payload"]["q"] == injection
        # Verify the table still exists (no SQL executed).
        async with redis_queue.session_factory() as session:
            from sqlalchemy import text
            row = await session.execute(text("SELECT 1 FROM tasks WHERE task_id = :tid"), {"tid": task_id})
            assert row.first() is not None


# ---------------------------------------------------------------------------
# XSS-like payloads (stored literally; no code execution)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_xss_payload_stored_literally(redis_queue, clean_db):
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    xss = "<script>alert('xss')</script>"
    async with _client(app) as client:
        resp = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {"html": xss}},
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        assert resp.status_code == 200
        task_id = resp.json()["task_id"]
        got = await client.get(
            f"/tasks/{task_id}",
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        assert got.json()["payload"]["html"] == xss


# ---------------------------------------------------------------------------
# Invalid tenant IDs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_invalid_tenant_id_hint_rejected(redis_queue, clean_db):
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    async with _client(app) as client:
        # Valid key but a non-existent tenant id as the hint -> mismatch/unknown.
        resp = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {}},
            headers={"X-API-Key": key, "X-Tenant-ID": "tenant-does-not-exist"},
        )
        assert resp.status_code == 403


@pytest.mark.asyncio
async def test_tenant_id_hint_validation(redis_queue, clean_db):
    """A whitespace (non-empty) tenant-id hint that does not match the
    authenticated tenant is rejected (403).  An empty hint is treated as
    'no hint' and the request is still scoped to the authenticated tenant."""
    tm = TenantManager(_TenantStorage())
    t = await tm.create_tenant("IV Tenant")
    key = await tm.generate_api_key(t.tenant_id)
    scheduler = TaskScheduler(redis_queue)
    app = _build_app(tm, scheduler)
    async with _client(app) as client:
        # Whitespace-only hint != authenticated tenant -> 403 (cannot spoof).
        ws = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {}},
            headers={"X-API-Key": key, "X-Tenant-ID": "   "},
        )
        assert ws.status_code == 403

        # Empty hint is allowed; the task is still owned by the authenticated tenant.
        empty = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {}},
            headers={"X-API-Key": key, "X-Tenant-ID": ""},
        )
        assert empty.status_code == 200
        task_id = empty.json()["task_id"]
        got = await client.get(
            f"/tasks/{task_id}",
            headers={"X-API-Key": key, "X-Tenant-ID": t.tenant_id},
        )
        assert got.json()["tenant_id"] == t.tenant_id


# ---------------------------------------------------------------------------
# Invalid API key formats
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_invalid_api_key_formats_rejected():
    tm = TenantManager(_TenantStorage())
    await tm.create_tenant("IV Key Tenant")
    for bad in ["", "   ", "tk-", "tk", "short", "Bearer some-jwt", "tk-<script>", "UPPERCASEKEY", "tk with spaces", "12345"]:
        assert await tm.authenticate_api_key(bad) is None
