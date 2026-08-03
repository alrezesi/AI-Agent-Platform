
# Unit tests for distributed execution components

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from redis.asyncio import Redis

from src.agent_platform.distributed.node import Node, NodeInfo, NodeStatus
from src.agent_platform.distributed.registry import DistributedRegistry
from src.agent_platform.distributed.queue import DistributedTaskQueue
from src.agent_platform.distributed.lock import DistributedLock
from src.agent_platform.core.task import Task, TaskPriority
from src.agent_platform.core.agent import AgentRecord


@pytest.fixture
async def redis_client():
    """Create a mock Redis client."""
    client = AsyncMock(spec=Redis)
    client.set = AsyncMock()
    client.get = AsyncMock()
    client.setex = AsyncMock()
    client.delete = AsyncMock()
    client.zadd = AsyncMock()
    client.zpopmin = AsyncMock()
    client.zrange = AsyncMock()
    client.zrem = AsyncMock()
    client.zcard = AsyncMock()
    client.scan = AsyncMock()
    client.sadd = AsyncMock()
    client.smembers = AsyncMock()
    client.eval = AsyncMock()
    return client


@pytest.mark.asyncio
async def test_node_lifecycle():
    info = NodeInfo.create(port=8080)
    node = Node(info)

    assert node.info.status == NodeStatus.INITIALIZING
    await node.start()
    assert node.info.status == NodeStatus.ACTIVE
    assert node.is_active is True

    await node.heartbeat()
    old_time = node.info.last_heartbeat
    await asyncio.sleep(0.1)
    await node.heartbeat()
    assert node.info.last_heartbeat > old_time

    health = await node.health_check()
    assert health is True

    await node.stop()
    assert node.info.status == NodeStatus.OFFLINE
    assert node.is_active is False


@pytest.mark.asyncio
async def test_distributed_registry(redis_client):
    registry = DistributedRegistry(redis_client)

    agent = AgentRecord(
        agent_id="test-agent",
        name="Test",
        capabilities=[],
    )

    await registry.register(agent)
    redis_client.setex.assert_called_once()

    redis_client.get.return_value = agent.model_dump_json()
    retrieved = await registry.get_agent("test-agent")
    assert retrieved is not None
    assert retrieved.agent_id == "test-agent"


@pytest.mark.asyncio
async def test_distributed_queue(redis_client):
    queue = DistributedTaskQueue(redis_client)

    task = Task(
        task_id="t1",
        agent_id="a1",
        type="test",
        payload={"x": 1},
        priority=TaskPriority.HIGH,
    )

    await queue.enqueue(task)
    redis_client.setex.assert_called()
    redis_client.zadd.assert_called()

    # Test dequeue
    redis_client.zpopmin.return_value = [("t1", 1.0)]
    redis_client.get.return_value = task.model_dump_json()

    dequeued = await queue.dequeue()
    assert dequeued is not None
    assert dequeued.task_id == "t1"
    assert dequeued.status == "running"


@pytest.mark.asyncio
async def test_distributed_lock(redis_client):
    lock = DistributedLock(redis_client, "test-lock", ttl_seconds=5)

    # Test acquire
    redis_client.set.return_value = True
    acquired = await lock.acquire()
    assert acquired is True

    # Test release
    redis_client.eval.return_value = 1
    released = await lock.release()
    assert released is True

    # Test context manager
    redis_client.set.return_value = True
    redis_client.eval.return_value = 1
    async with lock:
        assert lock.is_locked is True
    assert lock.is_locked is False