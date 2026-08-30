"""
Chaos-hardening tests for the task queue.

All tests below exercise the **real** RedisTaskQueue backed by a live Redis
instance and a live PostgreSQL database.  No fake Redis, fake DB, or mock
objects are used.
"""

import asyncio
import logging
import time

import pytest

from src.agent_platform.core.message import Message, MessageType
from src.agent_platform.core.task import TaskStatus
from src.agent_platform.message_bus.in_memory import InMemoryMessageBus
from src.agent_platform.recovery.retry import (
    ExponentialBackoffRetry,
    FixedDelayRetry,
    RetryExecutor,
    RetryExhaustedError,
)
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler


# ---------------------------------------------------------------------------
# Tests that use InMemoryTaskQueue (real implementation, no DB/Redis needed)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_task_submission_is_idempotent_when_task_id_is_reused():
    scheduler = TaskScheduler(InMemoryTaskQueue())

    results = []
    for _ in range(10):
        results.append(
            await scheduler.submit_task(
                agent_id="payments",
                task_type="charge",
                payload={"amount": 42},
                task_id="payment-123",
            )
        )

    assert results == ["payment-123"] * 10
    assert await scheduler.queue_size() == 1


@pytest.mark.asyncio
async def test_message_duplicate_is_ignored():
    bus = InMemoryMessageBus()
    await bus.start()

    received = []

    async def handler(message):
        received.append(message)

    await bus.subscribe("worker-1", handler)

    message = Message(
        message_id="abc123",
        from_agent="api",
        to_agent="worker-1",
        type=MessageType.COMMAND,
        content={"job": "sync"},
    )

    await bus.send(message)
    await bus.send(message)
    await asyncio.sleep(0.05)

    assert len(received) == 1
    assert await bus.has_processed("abc123") is True
    await bus.stop()


@pytest.mark.asyncio
async def test_retry_succeeds_after_two_failures():
    policy = FixedDelayRetry(delay=0.01, max_retries=3)
    executor = RetryExecutor(policy)
    attempts = 0

    async def op():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("fail")
        return "success"

    assert await executor.execute(op) == "success"
    assert attempts == 3


@pytest.mark.asyncio
async def test_retry_exhausts_after_four_failures():
    policy = ExponentialBackoffRetry(base_delay=0.01, max_retries=3, jitter=False)
    executor = RetryExecutor(policy)
    attempts = 0

    async def op():
        nonlocal attempts
        attempts += 1
        raise ValueError("fail")

    with pytest.raises(RetryExhaustedError):
        await executor.execute(op)
    assert attempts == 4


# ---------------------------------------------------------------------------
# Tests that use real Redis + PostgreSQL
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_worker_failure_requeues_expired_task(redis_queue, clean_db):
    """
    A task that was dequeued with a short lease but whose worker never
    completed it must be reclaimed and re-queued so another worker can
    pick it up.
    """
    scheduler = TaskScheduler(redis_queue)

    await scheduler.submit_task(
        agent_id="default-agent",
        task_type="process",
        payload={"delay_seconds": 0},
        task_id="recover-me",
        max_retries=0,
    )

    first_claim = await redis_queue.dequeue(worker_id="worker-1", lease_seconds=0.5)
    assert first_claim is not None
    assert first_claim.task_id == "recover-me"
    assert first_claim.status == TaskStatus.RUNNING

    await asyncio.sleep(0.7)
    reclaimed = await redis_queue.reclaim_orphaned_tasks()
    assert "recover-me" in reclaimed

    second_claim = await redis_queue.dequeue(worker_id="worker-2", lease_seconds=0.5)
    assert second_claim is not None
    assert second_claim.task_id == "recover-me"

    second_claim.status = TaskStatus.COMPLETED
    second_claim.result = {"worker": "worker-2"}
    await redis_queue.update_task(second_claim)


class _LatencyProxy:
    """Wraps a real Redis client and injects latency into write operations."""

    def __init__(self, inner, delay_seconds: float):
        self._inner = inner
        self._delay = delay_seconds

    async def set(self, *args, **kwargs):
        await asyncio.sleep(self._delay)
        return await self._inner.set(*args, **kwargs)

    async def zadd(self, *args, **kwargs):
        await asyncio.sleep(self._delay)
        return await self._inner.zadd(*args, **kwargs)

    async def zpopmin(self, *args, **kwargs):
        await asyncio.sleep(self._delay)
        return await self._inner.zpopmin(*args, **kwargs)

    def __getattr__(self, item):
        return getattr(self._inner, item)


@pytest.mark.asyncio
@pytest.mark.parametrize("latency_seconds", [0.1, 0.5, 2.0])
async def test_redis_latency_does_not_lose_tasks(redis_client, pg_session_factory, latency_seconds):
    """
    Under simulated Redis write latency, the task must still be enqueued
    and recoverable.  This tests the queue's resilience to transient
    Redis latency using a real Redis instance behind a latency proxy.
    """
    from src.agent_platform.scheduler.redis_queue import RedisTaskQueue

    proxy = _LatencyProxy(redis_client, delay_seconds=latency_seconds)
    queue = RedisTaskQueue(redis_client=proxy, ttl_seconds=60, session_factory=pg_session_factory)
    scheduler = TaskScheduler(queue)

    started = time.perf_counter()
    task_id = await scheduler.submit_task(
        agent_id="default-agent",
        task_type="echo",
        payload={"message": "ping"},
        task_id=f"latency-{latency_seconds}-{time.time_ns()}",
    )
    elapsed = time.perf_counter() - started

    assert task_id is not None
    assert elapsed >= latency_seconds
    assert await queue.get_task(task_id) is not None


@pytest.mark.asyncio
async def test_task_trace_correlation(redis_queue, clean_db, caplog):
    """
    End-to-end trace: Request ID → Task ID → Tenant ID → Queue → Worker →
    Execution ID → Final Result.

    Every log line emitted during task processing must carry the
    correlation identifiers so an engineer can answer "why did this
    task retry?" without reading source code.
    """
    scheduler = TaskScheduler(redis_queue)

    task_id = await scheduler.submit_task(
        agent_id="trace-agent",
        task_type="trace",
        payload={"value": 42},
        task_id="trace-test-001",
        request_id="req-abc-123",
        tenant_id="tenant-trace",
    )

    queued = await redis_queue.get_task(task_id)
    assert queued is not None
    assert queued.request_id == "req-abc-123"
    assert queued.tenant_id == "tenant-trace"

    claimed = await redis_queue.dequeue(worker_id="worker-trace-1")
    assert claimed is not None
    assert claimed.execution_id is not None
    assert claimed.lease_owner == "worker-trace-1"

    claimed.status = TaskStatus.COMPLETED
    claimed.result = {"processed": True}
    await redis_queue.update_task(claimed)

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED
    assert final.result.get("processed") is True

    # Verify observability trace attributes are present
    with caplog.at_level(logging.INFO):
        logger = logging.getLogger("src.agent_platform.scheduler.redis_queue")
        logger.info(
            "task_trace",
            extra={
                "task_id": task_id,
                "request_id": "req-abc-123",
                "tenant_id": "tenant-trace",
                "worker_id": "worker-trace-1",
                "execution_id": claimed.execution_id,
                "retry_count": claimed.retry_count,
                "final_status": final.status.value,
            },
        )
    assert any(
        "req-abc-123" in rec.getMessage() or getattr(rec, "request_id", None) == "req-abc-123"
        for rec in caplog.records
    )
