
# Unit tests for distributed components

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timedelta, timezone

from src.agent_platform.core.agent import AgentRecord
from src.agent_platform.core.task import Task, TaskPriority, TaskStatus
from src.agent_platform.distributed.lock import DistributedLock
from src.agent_platform.distributed.node import Node, NodeInfo, NodeStatus
from src.agent_platform.distributed.queue import DistributedTaskQueue
from src.agent_platform.distributed.registry import DistributedRegistry


# === Fixture mock_redis ===
@pytest.fixture
def mock_redis():
    """Create a mock Redis client for testing."""
    redis = AsyncMock()
    redis.set = AsyncMock()
    redis.get = AsyncMock()
    redis.setex = AsyncMock()
    redis.delete = AsyncMock()
    redis.zadd = AsyncMock()
    redis.zpopmin = AsyncMock()
    redis.zrange = AsyncMock()
    redis.zrem = AsyncMock()
    redis.zcard = AsyncMock()
    redis.scan = AsyncMock(return_value=(0, []))
    redis.sadd = AsyncMock()
    redis.smembers = AsyncMock(return_value=set())
    redis.eval = AsyncMock(return_value=1)
    return redis


# === Tests ===

@pytest.mark.asyncio
async def test_node_lifecycle():
    """Test node lifecycle: start, heartbeat, stop."""
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
async def test_distributed_registry(mock_redis):
    """Test distributed registry operations."""
    registry = DistributedRegistry(mock_redis)
    agent = AgentRecord(agent_id="test-agent", name="Test Agent")
    await registry.register(agent)
    mock_redis.setex.assert_called_once()

    mock_redis.get.return_value = agent.model_dump_json()
    retrieved = await registry.get_agent("test-agent")
    assert retrieved is not None
    assert retrieved.agent_id == "test-agent"


@pytest.mark.asyncio
async def test_distributed_queue(mock_redis):
    """Test distributed queue operations."""
    queue = DistributedTaskQueue(mock_redis)
    task = Task(task_id="t1", agent_id="a1", type="test", priority=TaskPriority.HIGH)
    await queue.enqueue(task)
    mock_redis.setex.assert_called()
    mock_redis.zadd.assert_called()

    mock_redis.zpopmin.return_value = [("t1", 1.0)]
    mock_redis.get.return_value = task.model_dump_json()
    dequeued = await queue.dequeue()
    assert dequeued is not None
    assert dequeued.task_id == "t1"
    assert dequeued.status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_distributed_lock(mock_redis):
    """Test distributed lock operations."""
    lock = DistributedLock(mock_redis, "test-lock", ttl_seconds=5)

    mock_redis.set.return_value = True
    acquired = await lock.acquire()
    assert acquired is True

    mock_redis.eval.return_value = 1
    released = await lock.release()
    assert released is True

    # Context manager
    mock_redis.set.return_value = True
    async with lock:
        assert lock.is_locked is True
    assert lock.is_locked is False


# === Advanced tests (added in later phases) ===

@pytest.mark.asyncio
async def test_distributed_queue_enqueue_dequeue(mock_redis):
    """Test enqueue and dequeue operations of distributed queue."""
    queue = DistributedTaskQueue(mock_redis)
    task = Task(task_id="t1", agent_id="a1", type="test")
    await queue.enqueue(task)
    assert mock_redis.zadd.called

    mock_redis.zpopmin.return_value = [("t1", 1.0)]
    mock_redis.get.return_value = task.model_dump_json()
    dequeued = await queue.dequeue()
    assert dequeued is not None
    assert dequeued.task_id == "t1"


@pytest.mark.asyncio
async def test_distributed_queue_update_task(mock_redis):
    """Test updating a task in distributed queue."""
    queue = DistributedTaskQueue(mock_redis)
    task = Task(task_id="t1", agent_id="a1", type="test", status=TaskStatus.COMPLETED)
    await queue.update_task(task)
    # setex or set with ex is called
    assert mock_redis.setex.called or mock_redis.set.called


@pytest.mark.asyncio
async def test_distributed_registry_register_node(mock_redis):
    """Test node registration in distributed registry."""
    registry = DistributedRegistry(mock_redis)
    node_info = NodeInfo.create(port=8080)
    await registry.register_node(node_info)
    assert mock_redis.setex.called or mock_redis.set.called
    assert mock_redis.sadd.called


@pytest.mark.asyncio
async def test_distributed_registry_list_nodes(mock_redis):
    """Test listing nodes from distributed registry."""
    registry = DistributedRegistry(mock_redis)
    mock_redis.smembers.return_value = {"node1"}
    mock_redis.get.return_value = json.dumps({"node_id": "node1", "hostname": "localhost"})
    nodes = await registry.list_nodes()
    assert len(nodes) == 1
    assert nodes[0]["node_id"] == "node1"



@pytest.mark.asyncio
async def test_distributed_queue_peek(mock_redis):
    queue = DistributedTaskQueue(mock_redis)
    task = Task(task_id="t1", agent_id="a1", type="test")
    await queue.enqueue(task)

    mock_redis.zrange.return_value = [("t1", 1.0)]
    mock_redis.get.return_value = task.model_dump_json()

    peeked = await queue.peek()
    assert peeked is not None
    assert peeked.task_id == "t1"


@pytest.mark.asyncio
async def test_distributed_queue_cancel(mock_redis):
    queue = DistributedTaskQueue(mock_redis)
    task = Task(task_id="t1", agent_id="a1", type="test")
    await queue.enqueue(task)

    mock_redis.get.return_value = task.model_dump_json()
    cancelled = await queue.cancel("t1")
    assert cancelled is True
    mock_redis.zrem.assert_called()


@pytest.mark.asyncio
async def test_distributed_queue_get_stats(mock_redis):
    queue = DistributedTaskQueue(mock_redis)
    mock_redis.scan.return_value = (0, ["tasks:data:t1", "tasks:data:t2"])
    mock_redis.get.side_effect = [
        Task(
            task_id="t1", agent_id="a1", type="test", status=TaskStatus.COMPLETED
        ).model_dump_json(),
        Task(task_id="t2", agent_id="a2", type="test", status=TaskStatus.PENDING).model_dump_json(),
    ]
    stats = await queue.get_stats()
    assert stats.total == 2
    assert stats.completed == 1
    assert stats.pending == 1


@pytest.mark.asyncio
async def test_distributed_registry_get_active_nodes(mock_redis):
    registry = DistributedRegistry(mock_redis)
    mock_redis.smembers.return_value = {"node1"}
    mock_redis.get.return_value = json.dumps(
        {"node_id": "node1", "last_heartbeat": datetime.now(timezone.utc).isoformat(), "status": "active"}
    )
    active = await registry.get_active_nodes()
    assert len(active) == 1


@pytest.mark.asyncio
async def test_distributed_registry_update_node_status(mock_redis):
    registry = DistributedRegistry(mock_redis)
    node_info = NodeInfo.create(port=8080)
    node_info.status = NodeStatus.ACTIVE
    mock_redis.get.return_value = json.dumps(
        {
            "node_id": "node1",
            "hostname": "localhost",
            "status": "initializing",
            "last_heartbeat": "2026-01-01T00:00:00",
        }
    )
    await registry.update_node_status(node_info)
    mock_redis.setex.assert_called()



@pytest.mark.asyncio
async def test_distributed_queue_list_tasks(mock_redis):
    """Test listing tasks with filters."""
    queue = DistributedTaskQueue(mock_redis)

    # Mock scan to return some task keys
    mock_redis.scan.return_value = (0, ["tasks:data:t1", "tasks:data:t2", "tasks:data:t3"])

    mock_redis.get.side_effect = [
        Task(
            task_id="t1", agent_id="a1", type="test", status=TaskStatus.COMPLETED
        ).model_dump_json(),
        Task(task_id="t2", agent_id="a2", type="test", status=TaskStatus.PENDING).model_dump_json(),
        Task(task_id="t3", agent_id="a1", type="test", status=TaskStatus.FAILED).model_dump_json(),
    ]

    from src.agent_platform.scheduler.models import TaskFilterOptions

    filters = TaskFilterOptions(agent_id="a1")
    tasks = await queue.list_tasks(filters, limit=10)
    assert len(tasks) == 2  # t1 and t3 have agent_id="a1"
    assert all(t.agent_id == "a1" for t in tasks)


@pytest.mark.asyncio
async def test_distributed_registry_heartbeat(mock_redis):
    """Test heartbeat in distributed registry."""
    registry = DistributedRegistry(mock_redis)
    agent = AgentRecord(agent_id="a1", name="Test")
    await registry.register(agent)

    # Simulate existing agent data
    mock_redis.get.return_value = agent.model_dump_json()

    result = await registry.heartbeat("a1")
    assert result is True
    mock_redis.setex.assert_called()


@pytest.mark.asyncio
async def test_distributed_registry_discover_with_tenant(mock_redis):
    """Test discovery with tenant isolation."""
    registry = DistributedRegistry(mock_redis)

    # Mock multiple agents
    mock_redis.scan.return_value = (0, ["dist:agent:a1", "dist:agent:a2"])
    mock_redis.get.side_effect = [
        AgentRecord(agent_id="a1", name="A1", tenant_id="tenant1").model_dump_json(),
        AgentRecord(agent_id="a2", name="A2", tenant_id="tenant2").model_dump_json(),
    ]

    # Discover only tenant1 agents
    results = await registry.discover(tenant_id="tenant1")
    assert len(results) == 1
    assert results[0].tenant_id == "tenant1"


@pytest.mark.asyncio
async def test_distributed_registry_get_node(mock_redis):
    """Test getting node information."""
    registry = DistributedRegistry(mock_redis)
    node_id = "node1"
    mock_redis.get.return_value = json.dumps({"node_id": node_id, "hostname": "localhost"})

    node = await registry.get_node(node_id)
    assert node is not None
    assert node["node_id"] == node_id
