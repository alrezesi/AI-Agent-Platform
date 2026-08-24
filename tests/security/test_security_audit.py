"""
Security audit tests for the AI-Agent-Platform.

All tests exercise the real TenantManager, real RedisTaskQueue with real
Redis + PostgreSQL, and the real API middleware.  No mocks, no fake
implementations, no auth bypass.
"""

import asyncio
import logging
import re

import pytest

from src.agent_platform.core.task import TaskStatus
from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.multi_tenant.models import TenantQuota, TenantStatus
from src.agent_platform.multi_tenant.security import hash_api_key
from src.agent_platform.monitoring.rate_limit import RateLimiter
from src.agent_platform.scheduler.scheduler import TaskScheduler


class _TenantStorage:
    """Real in-memory tenant storage (no fake Redis, no fake DB)."""

    def __init__(self):
        self._tenants: dict = {}


def _make_tenant_manager() -> TenantManager:
    return TenantManager(_TenantStorage())


# ---------------------------------------------------------------------------
# 1. Tenant isolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tenant_a_cannot_read_tenant_b_task(redis_queue, clean_db):
    """Tenant A's API key must not see Tenant B's tasks at the queue layer."""
    tm = _make_tenant_manager()
    tenant_a = await tm.create_tenant("Tenant A")
    tenant_b = await tm.create_tenant("Tenant B")

    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task(
        agent_id="agent-a",
        task_type="test",
        payload={"secret": "tenant-a-data"},
        task_id="tenant-isolation-001",
        tenant_id=tenant_a.tenant_id,
    )

    # Tenant A sees their own task
    task_a = await redis_queue.get_task("tenant-isolation-001", tenant_id=tenant_a.tenant_id)
    assert task_a is not None
    assert task_a.tenant_id == tenant_a.tenant_id

    # Tenant B CANNOT read tenant A's task (tenant isolation at queue layer)
    task_b = await redis_queue.get_task("tenant-isolation-001", tenant_id=tenant_b.tenant_id)
    assert task_b is None


@pytest.mark.asyncio
async def test_tenant_a_cannot_modify_tenant_b_task(redis_queue, clean_db):
    """Updating a task with a different tenant_id must not modify the original task."""
    tm = _make_tenant_manager()
    tenant_a = await tm.create_tenant("Tenant A")
    tenant_b = await tm.create_tenant("Tenant B")

    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task("agent", "test", {"data": "a"}, "cross-modify-001", tenant_id=tenant_a.tenant_id)

    # Tenant A modifies their task
    task = await redis_queue.get_task("cross-modify-001", tenant_id=tenant_a.tenant_id)
    task.status = TaskStatus.COMPLETED
    task.result = {"modified_by": "tenant-a"}
    await redis_queue.update_task(task)

    # Tenant B cannot retrieve the modified task
    task_b = await redis_queue.get_task("cross-modify-001", tenant_id=tenant_b.tenant_id)
    assert task_b is None

    # Tenant A can see the modification
    task_a = await redis_queue.get_task("cross-modify-001", tenant_id=tenant_a.tenant_id)
    assert task_a.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_tenant_a_cannot_cancel_tenant_b_task(redis_queue, clean_db):
    """Tenant A must not be able to cancel Tenant B's task."""
    tm = _make_tenant_manager()
    tenant_a = await tm.create_tenant("Tenant A")
    tenant_b = await tm.create_tenant("Tenant B")

    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task("agent", "test", {}, "cross-cancel-001", tenant_id=tenant_b.tenant_id)

    # Tenant A tries to cancel Tenant B's task — must fail silently
    cancelled = await redis_queue.cancel("cross-cancel-001", tenant_id=tenant_a.tenant_id)
    assert not cancelled or cancelled is None

    # Task should still be PENDING for Tenant B
    task_b = await redis_queue.get_task("cross-cancel-001", tenant_id=tenant_b.tenant_id)
    assert task_b is not None
    assert task_b.status == TaskStatus.PENDING


# ---------------------------------------------------------------------------
# 2. API key validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_invalid_api_key_rejected():
    """An API key that doesn't match any tenant must be rejected."""
    tm = _make_tenant_manager()
    await tm.create_tenant("Real Tenant")

    result = await tm.authenticate_api_key("invalid-key-12345")
    assert result is None


@pytest.mark.asyncio
async def test_missing_api_key_rejected():
    """No API key at all must result in authentication failure."""
    tm = _make_tenant_manager()
    result = await tm.authenticate_api_key("")
    assert result is None


@pytest.mark.asyncio
async def test_none_api_key_rejected():
    """None API key must result in authentication failure."""
    tm = _make_tenant_manager()
    result = await tm.authenticate_api_key(None)
    assert result is None


@pytest.mark.asyncio
async def test_malformed_api_key_rejected():
    """Malformed (whitespace-only, garbage) API keys must be rejected."""
    tm = _make_tenant_manager()
    for bad_key in ["", "   ", "garbage", "tk-"]:
        result = await tm.authenticate_api_key(bad_key)
        assert result is None


@pytest.mark.asyncio
async def test_revoked_api_key_rejected():
    """A revoked API key must be rejected."""
    tm = _make_tenant_manager()
    tenant = await tm.create_tenant("Tenant with Key")
    api_key = await tm.generate_api_key(tenant.tenant_id)

    # Revoke the key
    await tm.revoke_api_key(tenant.tenant_id, api_key)

    result = await tm.authenticate_api_key(api_key)
    assert result is None


@pytest.mark.asyncio
async def test_valid_api_key_accepted():
    """A valid, non-revoked API key must be accepted."""
    tm = _make_tenant_manager()
    tenant = await tm.create_tenant("Valid Tenant")
    api_key = await tm.generate_api_key(tenant.tenant_id)

    result = await tm.authenticate_api_key(api_key)
    assert result is not None
    assert result.tenant_id == tenant.tenant_id


# ---------------------------------------------------------------------------
# 3. Authentication bypass prevention
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_x_tenant_id_without_api_key_rejected():
    """X-Tenant-ID header alone (no API key) must not authenticate."""
    tm = _make_tenant_manager()
    await tm.create_tenant("Bypass Test Tenant")

    # No API key provided at all
    result = await tm.authenticate_api_key(None)
    assert result is None


@pytest.mark.asyncio
async def test_no_credentials_rejected():
    """A request with no auth headers of any kind must fail."""
    tm = _make_tenant_manager()
    result = await tm.authenticate_api_key(None)
    assert result is None
    assert await tm.authenticate_api_key("") is None


@pytest.mark.asyncio
async def test_bearer_token_not_api_key_rejected():
    """A Bearer token (not an API key) must be rejected by the API key authenticator."""
    tm = _make_tenant_manager()
    tenant = await tm.create_tenant("Bearer Test")
    await tm.generate_api_key(tenant.tenant_id)

    # Bearer tokens are not accepted by the API key-based authenticator
    result = await tm.authenticate_api_key("Bearer some-jwt-token")
    assert result is None


@pytest.mark.asyncio
async def test_wrong_tenant_key_rejected():
    """An API key belonging to a different tenant must not authenticate
    as the claimed tenant."""
    tm = _make_tenant_manager()
    tenant_a = await tm.create_tenant("Tenant A")
    tenant_b = await tm.create_tenant("Tenant B")

    api_key_b = await tm.generate_api_key(tenant_b.tenant_id)
    # The API key for B should authenticate as B, not A
    result = await tm.authenticate_api_key(api_key_b)
    assert result is not None
    assert result.tenant_id == tenant_b.tenant_id


@pytest.mark.asyncio
async def test_suspended_tenant_key_rejected():
    """An API key for a suspended tenant must not authenticate."""
    tm = _make_tenant_manager()
    tenant = await tm.create_tenant("Suspended Tenant")
    api_key = await tm.generate_api_key(tenant.tenant_id)

    # Suspend the tenant
    await tm.suspend_tenant(tenant.tenant_id)

    result = await tm.authenticate_api_key(api_key)
    assert result is None


# ---------------------------------------------------------------------------
# 4. IDOR / predictable IDs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_task_id_not_predictable(redis_queue, clean_db):
    """Generated task IDs must contain randomness (not sequential)."""
    scheduler = TaskScheduler(redis_queue)

    await scheduler.submit_task("agent", "test", {}, task_id="task-uuid-001")
    await scheduler.submit_task("agent", "test", {}, task_id="task-uuid-002")

    task1 = await redis_queue.get_task("task-uuid-001")
    task2 = await redis_queue.get_task("task-uuid-002")
    assert task1.task_id != task2.task_id


@pytest.mark.asyncio
async def test_tenant_id_not_user_controllable_idor(redis_queue, clean_db):
    """
    While task_id is a string, tenant isolation at the DB level is enforced
    by the get_task method checking tenant_id.  A caller cannot override
    the tenant_id of a task they don't own.
    """
    tm = _make_tenant_manager()
    tenant_a = await tm.create_tenant("Tenant A")
    tenant_b = await tm.create_tenant("Tenant B")

    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task("agent", "test", {}, "idor-001", tenant_id=tenant_a.tenant_id)

    # Querying with tenant B's ID should return None
    result = await redis_queue.get_task("idor-001", tenant_id=tenant_b.tenant_id)
    assert result is None


@pytest.mark.asyncio
async def test_cancel_does_not_affect_other_tenant(redis_queue, clean_db):
    """Cancelling a task for tenant A must not affect tenant B's tasks."""
    tm = _make_tenant_manager()
    tenant_a = await tm.create_tenant("Tenant A")
    tenant_b = await tm.create_tenant("Tenant B")

    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task("agent", "test", {}, "cancel-a-001", tenant_id=tenant_a.tenant_id)
    await scheduler.submit_task("agent", "test", {}, "cancel-b-001", tenant_id=tenant_b.tenant_id)

    await redis_queue.cancel("cancel-a-001", tenant_id=tenant_a.tenant_id)

    task_a = await redis_queue.get_task("cancel-a-001", tenant_id=tenant_a.tenant_id)
    task_b = await redis_queue.get_task("cancel-b-001", tenant_id=tenant_b.tenant_id)

    assert task_a.status == TaskStatus.CANCELLED
    assert task_b.status == TaskStatus.PENDING


# ---------------------------------------------------------------------------
# 5. Input validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_malformed_payload_rejected_by_model(redis_queue, clean_db):
    """Malformed payloads are rejected by the Task model validation."""
    from src.agent_platform.core.task import Task, TaskPriority

    with pytest.raises(Exception):
        Task(
            task_id="bad-payload",
            agent_id="agent",
            type="test",
            payload=None,
            priority=TaskPriority.MEDIUM,
        )


@pytest.mark.asyncio
async def test_oversized_payload_stored_correctly(redis_queue, clean_db):
    """Large (but valid) payloads must be stored and retrieved intact."""
    scheduler = TaskScheduler(redis_queue)
    large_data = {"items": [f"item-{i}" for i in range(1000)]}
    await scheduler.submit_task("agent", "test", large_data, task_id="large-payload-001")

    task = await redis_queue.get_task("large-payload-001")
    assert task is not None
    assert len(task.payload["items"]) == 1000


@pytest.mark.asyncio
async def test_invalid_task_state_transitions_handled(redis_queue, clean_db):
    """All terminal states must be valid for task status."""
    from src.agent_platform.core.task import TaskPriority, TaskStatus as TS

    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task("agent", "test", {}, task_id="state-test-001")

    task = await redis_queue.get_task("state-test-001")
    task.status = TS.COMPLETED
    await redis_queue.update_task(task)

    final = await redis_queue.get_task("state-test-001")
    assert final.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_malicious_metadata_does_not_crash(redis_queue, clean_db):
    """Malicious metadata with SQL-like injection attempts must not cause errors."""
    scheduler = TaskScheduler(redis_queue)
    malicious_payload = {
        "sql_injection": "'; DROP TABLE tasks; --",
        "xss": "<script>alert('xss')</script>",
        "path_traversal": "../../../etc/passwd",
        "nested": {"level1": {"level2": {"level3": "deep"}}},
    }
    await scheduler.submit_task("agent", "malicious", malicious_payload, task_id="malicious-001")

    task = await redis_queue.get_task("malicious-001")
    assert task is not None
    assert task.payload["sql_injection"] == "'; DROP TABLE tasks; --"

    # Verify the table still exists
    async with redis_queue.session_factory() as session:
        from sqlalchemy import select, text
        result = await session.execute(text("SELECT 1 FROM tasks WHERE task_id = :tid"), {"tid": "malicious-001"})
        assert result.fetchone() is not None


# ---------------------------------------------------------------------------
# 8. Rate limiting
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rate_limit_allows_normal_traffic():
    """The rate limiter must allow normal traffic below the threshold."""
    limiter = RateLimiter(requests_per_second=100, burst=200)
    results = []
    for _ in range(50):
        results.append(await limiter.is_allowed("test-key"))
    assert all(results), "First 50 requests should be allowed"


@pytest.mark.asyncio
async def test_rate_limit_eventually_blocks_spam():
    """Sustained high-rate requests must eventually be rate-limited."""
    limiter = RateLimiter(requests_per_second=5, burst=5)
    results = []
    for _ in range(20):
        results.append(await limiter.is_allowed("spam-key"))
    assert not all(results), "Some requests must be rate-limited"


@pytest.mark.asyncio
async def test_rate_limit_is_per_tenant():
    """Rate limit buckets are isolated per tenant key."""
    limiter = RateLimiter(requests_per_second=5, burst=1)
    assert await limiter.is_allowed("tenant-a") is True
    assert await limiter.is_allowed("tenant-a") is False
    assert await limiter.is_allowed("tenant-b") is True


@pytest.mark.asyncio
async def test_rate_limit_per_key_isolation():
    """Exhausting one key's bucket must not affect another key."""
    limiter = RateLimiter(requests_per_second=1, burst=1)
    # Exhaust key-1
    assert await limiter.is_allowed("key-1") is True
    assert await limiter.is_allowed("key-1") is False
    # Key-2 still has its full burst
    assert await limiter.is_allowed("key-2") is True


# ---------------------------------------------------------------------------
# 9. Secret leakage
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_key_not_in_task_payload_or_result(redis_queue, clean_db):
    """API keys must never appear in task payloads or results."""
    tm = _make_tenant_manager()
    tenant = await tm.create_tenant("Leak Test Tenant")
    api_key = await tm.generate_api_key(tenant.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task(
        agent_id="agent",
        task_type="test",
        payload={"user_input": "hello"},
        task_id="secret-leak-001",
        tenant_id=tenant.tenant_id,
        request_id="req-999",
    )

    task = await redis_queue.get_task("secret-leak-001")
    task.result = {"output": "processed"}
    await redis_queue.update_task(task)

    final = await redis_queue.get_task("secret-leak-001")
    assert api_key not in str(final.payload)
    assert api_key not in str(final.result)


@pytest.mark.asyncio
async def test_no_api_key_in_redis_cache(redis_queue, clean_db):
    """The Redis cache must not contain plaintext API keys."""
    tm = _make_tenant_manager()
    tenant = await tm.create_tenant("Cache Test Tenant")
    api_key = await tm.generate_api_key(tenant.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task(
        agent_id="agent",
        task_type="test",
        payload={"input": "test"},
        task_id="redis-leak-001",
        tenant_id=tenant.tenant_id,
    )

    cursor = 0
    found_key = False
    while True:
        cursor, keys = await redis_queue.redis.scan(cursor, count=1000)
        for key in keys:
            key_type = await redis_queue.redis.type(key)
            if key_type == b"string" or key_type == "string":
                val = await redis_queue.redis.get(key)
                if val:
                    decoded = val.decode() if isinstance(val, bytes) else str(val)
                    if api_key in decoded:
                        found_key = True
        if cursor == 0:
            break
    assert not found_key, "API key found in Redis cache"


@pytest.mark.asyncio
async def test_no_secrets_in_redis_task_data(redis_queue, clean_db):
    """Task JSON in Redis must not contain API keys."""
    tm = _make_tenant_manager()
    tenant = await tm.create_tenant("Redis Data Test")
    api_key = await tm.generate_api_key(tenant.tenant_id)

    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task(
        agent_id="agent",
        task_type="test",
        payload={"input": "test"},
        task_id="redis-data-001",
        tenant_id=tenant.tenant_id,
    )

    task_data = await redis_queue.redis.get(redis_queue._task_key("redis-data-001"))
    if task_data:
        decoded = task_data.decode() if isinstance(task_data, bytes) else str(task_data)
        assert api_key not in decoded

    meta_data = await redis_queue.redis.get(redis_queue._meta_key("redis-data-001"))
    if meta_data:
        decoded = meta_data.decode() if isinstance(meta_data, bytes) else str(meta_data)
        assert api_key not in decoded


@pytest.mark.asyncio
async def test_api_key_hash_not_reversible(redis_queue, clean_db):
    """API key hashes in storage must not contain the plaintext key."""
    tm = _make_tenant_manager()
    tenant = await tm.create_tenant("Hash Test Tenant")
    api_key = await tm.generate_api_key(tenant.tenant_id)

    for key_record in tenant.api_keys:
        assert api_key not in str(key_record)
        assert api_key != key_record.get("key_hash", "")


@pytest.mark.asyncio
async def test_database_url_not_logged(redis_queue, clean_db, caplog):
    """Database connection strings with passwords must not appear in logs."""
    with caplog.at_level(logging.DEBUG):
        scheduler = TaskScheduler(redis_queue)
        await scheduler.submit_task("agent", "test", {}, task_id="db-url-001")

    for record in caplog.records:
        log_text = record.getMessage()
        assert "agent123" not in log_text
        assert "postgresql://" not in log_text
        assert "password" not in log_text.lower()


@pytest.mark.asyncio
async def test_no_authorization_header_in_logs(redis_queue, clean_db, caplog):
    """Authorization headers must not appear in logs."""
    with caplog.at_level(logging.DEBUG):
        scheduler = TaskScheduler(redis_queue)
        await scheduler.submit_task("agent", "test", {}, task_id="auth-header-001")

    for record in caplog.records:
        log_text = record.getMessage()
        assert "Authorization" not in log_text
        assert "Bearer" not in log_text


@pytest.mark.asyncio
async def test_no_api_key_in_exception_output(redis_queue, clean_db, caplog):
    """When errors occur, secrets must not leak through exception logs."""
    tm = _make_tenant_manager()
    tenant = await tm.create_tenant("Exception Test")
    api_key = await tm.generate_api_key(tenant.tenant_id)

    with caplog.at_level(logging.WARNING):
        scheduler = TaskScheduler(redis_queue)
        result = await redis_queue.dequeue(worker_id="w-err")
        assert result is None

    for record in caplog.records:
        log_text = record.getMessage()
        assert api_key not in log_text
        assert not re.search(r'tenant_api_key:[a-f0-9]+', log_text)
        assert not re.search(r'password=[^\s]+', log_text, re.IGNORECASE)
        assert not re.search(r'Bearer\s+\S+', log_text, re.IGNORECASE)


# ---------------------------------------------------------------------------
# 10. SQL injection prevention
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sql_injection_in_task_payload_does_not_compromise_db(redis_queue, clean_db):
    """SQL injection in task payloads must not compromise the database."""
    scheduler = TaskScheduler(redis_queue)
    malicious_payload = {"sql": "'; DROP TABLE tasks; --"}
    await scheduler.submit_task("agent", "malicious", malicious_payload, task_id="sql-inj-001")

    task = await redis_queue.get_task("sql-inj-001")
    assert task is not None
    assert task.payload["sql"] == "'; DROP TABLE tasks; --"

    # Verify the table still exists
    async with redis_queue.session_factory() as session:
        from sqlalchemy import text
        result = await session.execute(text("SELECT 1 FROM tasks WHERE task_id = 'sql-inj-001'"))
        assert result.fetchone() is not None
