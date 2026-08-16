import asyncio
import time
from collections import defaultdict

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
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.zsets = defaultdict(dict)
        self.lists = defaultdict(list)

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.kv:
            return False
        self.kv[key] = value.encode("utf-8") if isinstance(value, str) else value
        return True

    async def setex(self, key, ttl, value):
        return await self.set(key, value, ex=ttl)

    async def get(self, key):
        return self.kv.get(key)

    async def delete(self, key):
        return 1 if self.kv.pop(key, None) is not None else 0

    async def zadd(self, key, mapping):
        self.zsets[key].update(mapping)
        return len(mapping)

    async def zpopmin(self, key, count=1):
        bucket = self.zsets.get(key, {})
        items = sorted(bucket.items(), key=lambda item: item[1])[:count]
        for member, _ in items:
            bucket.pop(member, None)
        return items

    async def zrange(self, key, start, end, withscores=False):
        bucket = self.zsets.get(key, {})
        items = sorted(bucket.items(), key=lambda item: item[1])
        sliced = items[start : None if end == -1 else end + 1]
        if withscores:
            return sliced
        return [member for member, _ in sliced]

    async def zrem(self, key, *members):
        bucket = self.zsets.get(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    async def zcard(self, key):
        return len(self.zsets.get(key, {}))

    async def scan(self, cursor=0, match=None, count=100):
        keys = list(self.kv.keys())
        if match:
            import fnmatch

            keys = [key for key in keys if fnmatch.fnmatch(key, match)]
        return 0, keys

    async def lpush(self, key, *values):
        for value in values:
            self.lists[key].insert(0, value)
        return len(self.lists[key])

    async def rpop(self, key):
        bucket = self.lists.get(key, [])
        if not bucket:
            return None
        value = bucket.pop()
        return value.encode("utf-8") if isinstance(value, str) else value


class LatencyRedisProxy:
    def __init__(self, inner: FakeRedis, delay_seconds: float):
        self._inner = inner
        self._delay = delay_seconds

    async def set(self, *args, **kwargs):
        await asyncio.sleep(self._delay)
        return await self._inner.set(*args, **kwargs)

    async def setex(self, *args, **kwargs):
        await asyncio.sleep(self._delay)
        return await self._inner.setex(*args, **kwargs)

    def __getattr__(self, item):
        return getattr(self._inner, item)


class _ScalarResult:
    def __init__(self, items):
        self._items = items

    def scalars(self):
        return self

    def all(self):
        return list(self._items)


class FakeAsyncSession:
    def __init__(self, store):
        self._store = store

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, model, key):
        return self._store.get(key)

    async def execute(self, stmt):
        # Only the queue's recovery query path uses this.
        return _ScalarResult(
            orm
            for orm in self._store.values()
            if orm.status in ("pending", "running")
        )

    def add(self, orm):
        self._store[orm.task_id] = orm

    async def commit(self):
        return None


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


@pytest.mark.asyncio
async def test_worker_failure_requeues_expired_task():
    redis = FakeRedis()
    db_store = {}

    def session_factory():
        return FakeAsyncSession(db_store)

    queue = RedisTaskQueue(redis, ttl_seconds=60, session_factory=session_factory)
    scheduler = TaskScheduler(queue)

    await scheduler.submit_task(
        agent_id="default-agent",
        task_type="process",
        payload={"delay_seconds": 0},
        task_id="recover-me",
        max_retries=0,
    )

    first_claim = await queue.dequeue(worker_id="worker-1", lease_seconds=0.01)
    assert first_claim is not None
    assert first_claim.task_id == "recover-me"
    assert first_claim.status == TaskStatus.RUNNING

    await asyncio.sleep(0.02)
    reclaimed = await queue.reclaim_expired_tasks()
    assert reclaimed == ["recover-me"]

    second_claim = await queue.dequeue(worker_id="worker-2", lease_seconds=0.01)
    assert second_claim is not None
    assert second_claim.task_id == "recover-me"

    second_claim.status = TaskStatus.COMPLETED
    second_claim.result = {"worker": "worker-2"}
    await queue.update_task(second_claim)
    assert await queue.get_task("recover-me") is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("latency_seconds", [0.1, 0.5, 2.0, 5.0])
async def test_api_to_redis_latency_injection(latency_seconds):
    redis = LatencyRedisProxy(FakeRedis(), delay_seconds=latency_seconds)
    queue = RedisTaskQueue(redis, ttl_seconds=60)
    scheduler = TaskScheduler(queue)

    started = time.perf_counter()
    task_id = await scheduler.submit_task(
        agent_id="default-agent",
        task_type="echo",
        payload={"message": "ping"},
        task_id=f"latency-{latency_seconds}",
    )
    elapsed = time.perf_counter() - started

    assert task_id == f"latency-{latency_seconds}"
    assert elapsed >= latency_seconds
    assert await scheduler.get_task(task_id) is not None
