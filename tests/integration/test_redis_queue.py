
# Integration tests for Redis task queue (requires Redis running via Docker)

import pytest
import asyncio
import json
import pytest_asyncio
from redis.asyncio import Redis

from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agent_platform.core.task import Task, TaskPriority, TaskStatus


@pytest_asyncio.fixture
async def redis_client():
    """Create a Redis client and clean database before/after tests."""
    client = Redis.from_url("redis://localhost:6379/0")
    await client.flushall()
    yield client
    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture
async def queue(redis_client):
    """Create a RedisTaskQueue instance."""
    return RedisTaskQueue(redis_client)


@pytest.mark.asyncio
async def test_redis_queue_enqueue(queue, redis_client):
    """Test that a task is correctly stored in Redis."""
    task = Task(task_id="t1", agent_id="a1", type="test", priority=TaskPriority.HIGH)
    await queue.enqueue(task)
    await asyncio.sleep(0.2)  # Ensure Redis operations complete

    # Verify task data is stored
    data = await redis_client.get("tasks:data:t1")
    assert data is not None
    decoded = data.decode('utf-8')
    assert "t1" in decoded

    # Verify queue size
    size = await queue.size()
    assert size == 1


@pytest.mark.asyncio
async def test_redis_queue_dequeue(queue, redis_client):
    """Test that a task can be dequeued from Redis."""
    task = Task(task_id="t1", agent_id="a1", type="test", priority=TaskPriority.HIGH)
    await queue.enqueue(task)
    await asyncio.sleep(0.2)

    # Ensure the task is in the queue
    size = await queue.size()
    assert size == 1, f"Expected queue size 1, got {size}"

    # Peek to verify it's there
    peeked = await queue.peek()
    assert peeked is not None
    assert peeked.task_id == "t1"

    # Dequeue
    dequeued = await queue.dequeue()
    assert dequeued is not None, "Dequeue returned None"
    assert dequeued.task_id == "t1"
    assert dequeued.status == TaskStatus.RUNNING

    # Verify removed from queue
    size = await queue.size()
    assert size == 0


@pytest.mark.asyncio
async def test_redis_queue_priority_order(queue):
    """Test that priority ordering works correctly."""
    low_task = Task(task_id="low", agent_id="a1", type="test", priority=TaskPriority.LOW)
    high_task = Task(task_id="high", agent_id="a1", type="test", priority=TaskPriority.HIGH)

    # Enqueue low first, then high
    await queue.enqueue(low_task)
    await queue.enqueue(high_task)
    await asyncio.sleep(0.2)

    # High priority should be first
    dequeued1 = await queue.dequeue()
    assert dequeued1 is not None, "First dequeue returned None"
    assert dequeued1.task_id == "high"

    # Low priority should be second
    dequeued2 = await queue.dequeue()
    assert dequeued2 is not None, "Second dequeue returned None"
    assert dequeued2.task_id == "low"


@pytest.mark.asyncio
async def test_redis_queue_cancel(queue):
    """Test cancelling a pending task."""
    task = Task(task_id="t1", agent_id="a1", type="test")
    await queue.enqueue(task)
    await asyncio.sleep(0.2)

    cancelled = await queue.cancel("t1")
    assert cancelled is True

    size = await queue.size()
    assert size == 0

    task = await queue.get_task("t1")
    assert task.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_redis_queue_update_task(queue, redis_client):
    """Test updating a task's status and result."""
    task = Task(task_id="t1", agent_id="a1", type="test", status=TaskStatus.COMPLETED, result="done")
    await queue.update_task(task)
    await asyncio.sleep(0.2)

    data = await redis_client.get("tasks:data:t1")
    assert data is not None
    decoded = data.decode('utf-8')
    assert "done" in decoded


@pytest.mark.asyncio
async def test_redis_queue_get_stats(queue):
    """Test that statistics are aggregated correctly."""
    t1 = Task(task_id="t1", agent_id="a1", type="test", status=TaskStatus.COMPLETED)
    t2 = Task(task_id="t2", agent_id="a2", type="test", status=TaskStatus.PENDING)
    t3 = Task(task_id="t3", agent_id="a3", type="test", status=TaskStatus.FAILED)

    await queue.update_task(t1)
    await queue.update_task(t2)
    await queue.update_task(t3)
    await queue.enqueue(t2)
    await asyncio.sleep(0.2)

    stats = await queue.get_stats()
    assert stats.total == 3
    assert stats.completed == 1
    assert stats.pending == 1
    assert stats.failed == 1
