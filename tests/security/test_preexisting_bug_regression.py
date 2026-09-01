"""
Regression tests for the four confirmed pre-existing defects.

Each test reproduces the original bug against a fixed code path and proves
the fix holds.  All tests run against the real RedisTaskQueue backed by live
Redis and PostgreSQL (no mocks), except the middleware test which exercises
the middleware directly.

Bug #1 — cross-tenant task hijack via task_id collision
Bug #2 — state-corrupting duplicate/retry submission in enqueue
Bug #3 — dead idempotency code (now Redis-backed + wired into enqueue)
Bug #4 — auth-bypass fallback in tenant middleware (fail closed)
"""

import asyncio

import pytest
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient

from src.agent_platform.core.task import TaskStatus
from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.multi_tenant.middleware import TenantMiddleware
from src.agent_platform.scheduler.exceptions import CrossTenantTaskConflictError
from src.agent_platform.scheduler.scheduler import TaskScheduler


class _TenantStorage:
    def __init__(self):
        self._tenants = {}


def _build_app(tenant_manager: TenantManager, scheduler: TaskScheduler) -> FastAPI:
    """Minimal FastAPI app mirroring the real middleware + task stack."""
    from src.agent_platform.api.routes import tasks as tasks_routes
    from src.agent_platform.monitoring.rate_limit import RateLimitMiddleware
    from src.agent_platform.monitoring.request_id import RequestIdMiddleware

    app = FastAPI()
    app.add_middleware(TenantMiddleware, tenant_manager=tenant_manager)
    app.add_middleware(RequestIdMiddleware)
    # Effectively disable rate limiting for tests.
    app.add_middleware(RateLimitMiddleware, requests_per_second=100000, burst=100000)
    app.include_router(tasks_routes.router)
    app.dependency_overrides[tasks_routes.get_scheduler] = lambda: scheduler

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


# ---------------------------------------------------------------------------
# Bug #1 — cross-tenant task hijack via task_id collision
# ---------------------------------------------------------------------------
#
# Before the fix, submit_task(task_id=X, tenant_id=B) would succeed even when
# task X already belonged to tenant A, because get_task(X, B) returned None
# and the code treated X as a new task — then enqueue() overwrote A's task
# (including its tenant_id) with no ownership check.
#
# After the fix the scheduler raises CrossTenantTaskConflictError and the API
# translates it to HTTP 409.  The original tenant's task is untouched.


@pytest.mark.asyncio
async def test_cross_tenant_hijack_rejected_at_scheduler(redis_queue, clean_db):
    """Scheduler must raise, not overwrite, when task_id belongs to another tenant."""
    scheduler = TaskScheduler(redis_queue)

    # Tenant A owns task "shared-id"
    await scheduler.submit_task(
        "agent", "echo", {"owner": "A"}, task_id="shared-id", tenant_id="tenant-A"
    )

    # Tenant B tries to submit the same task_id — must be rejected.
    with pytest.raises(CrossTenantTaskConflictError) as excinfo:
        await scheduler.submit_task(
            "agent", "echo", {"owner": "B"}, task_id="shared-id", tenant_id="tenant-B"
        )
    assert excinfo.value.actual_tenant == "tenant-A"
    assert excinfo.value.expected_tenant == "tenant-B"

    # Tenant A's task is untouched: still owned by A, still PENDING.
    task = await redis_queue.get_task("shared-id", tenant_id="tenant-A")
    assert task is not None
    assert task.tenant_id == "tenant-A"
    assert task.status == TaskStatus.PENDING
    assert task.payload == {"owner": "A"}

    # Tenant B never got the task.
    assert await redis_queue.get_task("shared-id", tenant_id="tenant-B") is None


@pytest.mark.asyncio
async def test_cross_tenant_hijack_returns_409_from_api(redis_queue, clean_db):
    """The API endpoint must surface the conflict as HTTP 409, not 200."""
    tm = TenantManager(_TenantStorage())
    tenant_a = await tm.create_tenant("Tenant A")
    tenant_b = await tm.create_tenant("Tenant B")
    key_a = await tm.generate_api_key(tenant_a.tenant_id)
    key_b = await tm.generate_api_key(tenant_b.tenant_id)

    app = _build_app(tm, TaskScheduler(redis_queue))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Tenant A creates the task.
        r_a = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {"owner": "A"}, "task_id": "hijack-api"},
            headers={"X-API-Key": key_a, "X-Tenant-ID": tenant_a.tenant_id},
        )
        assert r_a.status_code == 200, r_a.text

        # Tenant B attempts to hijack the same task_id -> must be 409.
        r_b = await client.post(
            "/tasks/",
            json={"agent_id": "agent", "task_type": "echo", "payload": {"owner": "B"}, "task_id": "hijack-api"},
            headers={"X-API-Key": key_b, "X-Tenant-ID": tenant_b.tenant_id},
        )
        assert r_b.status_code == 409, r_b.text


@pytest.mark.asyncio
async def test_same_tenant_idempotent_submit_still_works(redis_queue, clean_db):
    """Re-submitting the same task_id for the SAME tenant is idempotent (not a conflict)."""
    scheduler = TaskScheduler(redis_queue)
    id1 = await scheduler.submit_task("agent", "echo", {"n": 1}, task_id="same-tenant-idem", tenant_id="tenant-A")
    id2 = await scheduler.submit_task("agent", "echo", {"n": 2}, task_id="same-tenant-idem", tenant_id="tenant-A")
    assert id1 == id2 == "same-tenant-idem"
    # Exactly one queue entry.
    assert await redis_queue.redis.zcard(redis_queue.QUEUE_KEY) == 1


# ---------------------------------------------------------------------------
# Bug #2 — state-corrupting duplicate/retry submission
# ---------------------------------------------------------------------------
#
# Before the fix, enqueue() unconditionally reset status=PENDING and wiped
# results/lease/retry_history before checking existence.  A late retry for a
# task that was already RUNNING or COMPLETED corrupted its state.
#
# After the fix, re-submitting a RUNNING or COMPLETED task is a no-op that
# preserves existing state.


@pytest.mark.asyncio
async def test_resubmit_running_task_is_noop(redis_queue, clean_db):
    """Re-submitting a RUNNING task must not reset it to PENDING."""
    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task("agent", "echo", {"v": 1}, task_id="running-nop", tenant_id="t1")

    # Worker claims the task -> RUNNING.
    claimed = await redis_queue.dequeue(worker_id="w-1", lease_seconds=60)
    assert claimed is not None
    assert claimed.status == TaskStatus.RUNNING
    assert claimed.lease_owner == "w-1"

    # A late duplicate submission arrives while the task is RUNNING.
    await scheduler.submit_task("agent", "echo", {"v": 99}, task_id="running-nop", tenant_id="t1")

    # State must be unchanged: still RUNNING, still owned by w-1, payload intact.
    task = await redis_queue.get_task("running-nop")
    assert task.status == TaskStatus.RUNNING
    assert task.lease_owner == "w-1"
    assert task.payload == {"v": 1}


@pytest.mark.asyncio
async def test_resubmit_completed_task_preserves_result(redis_queue, clean_db):
    """Re-submitting a COMPLETED task must not wipe its result or reset status."""
    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task("agent", "echo", {"v": 1}, task_id="done-nop", tenant_id="t1")

    claimed = await redis_queue.dequeue(worker_id="w-1", lease_seconds=60)
    claimed.status = TaskStatus.COMPLETED
    claimed.result = {"answer": 42}
    await redis_queue.update_task(claimed)

    # Late retry submission.
    await scheduler.submit_task("agent", "echo", {"v": 99}, task_id="done-nop", tenant_id="t1")

    task = await redis_queue.get_task("done-nop")
    assert task.status == TaskStatus.COMPLETED
    assert task.result == {"answer": 42}
    assert task.payload == {"v": 1}


# ---------------------------------------------------------------------------
# Bug #3 — idempotency now actually runs (Redis-backed, wired into enqueue)
# ---------------------------------------------------------------------------
#
# Before the fix, IdempotencyManager was fully implemented but never wired
# into the submission path, so the advertised idempotency feature did not run.
#
# After the fix, a Redis-backed IdempotencyManager guards enqueue(): the first
# submission acquires the lock, concurrent duplicates see "processing" and are
# no-ops, and on completion the stored result is recorded so a later duplicate
# resolves to the completed outcome.  This is asserted below against the real
# Redis-backed manager the queue now constructs.


@pytest.mark.asyncio
async def test_idempotency_manager_is_redis_backed_and_wired(redis_queue, clean_db):
    """The queue's idempotency manager must be backed by its Redis client."""
    assert redis_queue.idempotency is not None
    assert redis_queue.idempotency._redis is redis_queue.redis


@pytest.mark.asyncio
async def test_concurrent_duplicate_submissions_exactly_one_enqueue(redis_queue, clean_db):
    """Concurrent duplicate submissions enqueue exactly one task."""
    scheduler = TaskScheduler(redis_queue)
    task_id = "idem-dedup-001"

    results = await asyncio.gather(*[
        scheduler.submit_task("agent", "echo", {"attempt": i}, task_id=task_id, tenant_id="t1")
        for i in range(10)
    ])
    assert all(r == task_id for r in results)
    assert await redis_queue.redis.zcard(redis_queue.QUEUE_KEY) == 1

    # After completion, the idempotency store records the outcome.
    claimed = await redis_queue.dequeue(worker_id="w", lease_seconds=60)
    claimed.status = TaskStatus.COMPLETED
    claimed.result = {"done": True}
    await redis_queue.update_task(claimed)

    stored = await redis_queue.idempotency.get_result(task_id)
    assert stored == {"done": True}
    assert await redis_queue.idempotency.is_completed(task_id) is True


@pytest.mark.asyncio
async def test_post_completion_duplicate_is_noop(redis_queue, clean_db):
    """Submitting a finished task again must not create a second queue entry."""
    scheduler = TaskScheduler(redis_queue)
    task_id = "idem-post-done-001"

    await scheduler.submit_task("agent", "echo", {}, task_id=task_id, tenant_id="t1")
    claimed = await redis_queue.dequeue(worker_id="w", lease_seconds=60)
    claimed.status = TaskStatus.COMPLETED
    claimed.result = {"k": "v"}
    await redis_queue.update_task(claimed)

    # A completed task is removed from the queue; nothing pending.
    assert await redis_queue.redis.zcard(redis_queue.QUEUE_KEY) == 0

    # Duplicate submission after completion.
    await scheduler.submit_task("agent", "echo", {"different": True}, task_id=task_id, tenant_id="t1")

    # Still not re-enqueued (no new pending entry) and the original result is
    # preserved.  This is the core idempotency guarantee: a finished task is
    # never silently re-executed.
    assert await redis_queue.redis.zcard(redis_queue.QUEUE_KEY) == 0
    task = await redis_queue.get_task(task_id)
    assert task.status == TaskStatus.COMPLETED
    assert task.result == {"k": "v"}


# ---------------------------------------------------------------------------
# Bug #4 — auth-bypass fallback in tenant middleware (fail closed)
# ---------------------------------------------------------------------------
#
# Before the fix, when tenant_manager was None the middleware fabricated a
# valid, active Tenant from the caller-supplied API key + X-Tenant-ID with no
# lookup — a full auth bypass under a plausible DI-failure mode.
#
# After the fix, a missing tenant_manager fails closed with HTTP 503.


@pytest.mark.asyncio
async def test_middleware_fails_closed_without_tenant_manager(monkeypatch):
    """With no tenant_manager, the middleware must reject (503), not fabricate a tenant.

    The middleware self-heals a None tenant_manager by calling the runtime's
    get_tenant_manager(); to exercise the true fail-closed branch we patch that
    import to return None, simulating a DI-wiring/startup failure where no
    tenant manager can be resolved at all.
    """
    from src.agent_platform import runtime as runtime_module

    # The middleware resolves a missing tenant_manager via
    # `from src.agent_platform.runtime import get_tenant_manager`, so we patch
    # it on the runtime module to simulate a DI/startup failure where no
    # tenant manager can be resolved at all.
    monkeypatch.setattr(runtime_module, "get_tenant_manager", lambda: None)

    app = FastAPI()
    app.add_middleware(TenantMiddleware, tenant_manager=None)

    @app.get("/protected")
    async def protected(request: Request):
        return {"tenant_id": getattr(request.state, "tenant_id", None)}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Even with a plausible-looking key + tenant header, auth must fail.
        resp = await client.get(
            "/protected",
            headers={"X-API-Key": "tk-deadbeef", "X-Tenant-ID": "any-tenant"},
        )
    assert resp.status_code == 503, resp.text
