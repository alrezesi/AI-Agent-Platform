
# Advanced tests for distributed components

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.agent_platform.distributed.orchestrator import DistributedOrchestrator
from src.agent_platform.distributed.worker import WorkerNode, WorkerConfig
from src.agent_platform.distributed.queue import DistributedTaskQueue
from src.agent_platform.distributed.registry import DistributedRegistry
from src.agent_platform.distributed.node import NodeInfo, NodeStatus
from src.agent_platform.core.task import Task, TaskPriority


@pytest.fixture
def mock_redis():
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


@pytest.mark.asyncio
async def test_distributed_orchestrator_add_node(mock_redis):
    registry = DistributedRegistry(mock_redis)
    queue = DistributedTaskQueue(mock_redis)
    orchestrator = DistributedOrchestrator(registry, queue, mock_redis)

    node_info = NodeInfo.create(port=8080)
    await orchestrator.add_node(node_info)

    assert node_info.node_id in orchestrator._nodes
    assert orchestrator._nodes[node_info.node_id].is_active is True


@pytest.mark.asyncio
async def test_distributed_orchestrator_remove_node(mock_redis):
    registry = DistributedRegistry(mock_redis)
    queue = DistributedTaskQueue(mock_redis)
    orchestrator = DistributedOrchestrator(registry, queue, mock_redis)

    node_info = NodeInfo.create(port=8080)
    await orchestrator.add_node(node_info)

    removed = await orchestrator.remove_node(node_info.node_id)
    assert removed is True
    assert node_info.node_id not in orchestrator._nodes


@pytest.mark.asyncio
async def test_distributed_orchestrator_start_stop(mock_redis):
    registry = DistributedRegistry(mock_redis)
    queue = DistributedTaskQueue(mock_redis)
    orchestrator = DistributedOrchestrator(registry, queue, mock_redis)

    await orchestrator.start()
    assert orchestrator._running is True

    await orchestrator.stop()
    assert orchestrator._running is False


@pytest.mark.asyncio
async def test_worker_node_task_execution(mock_redis):
    # Mock agent registry
    agent_registry = AsyncMock()
    from src.agent_platform.core.agent import AgentRecord, AgentStatus

    agent_registry.get_agent = AsyncMock(
        return_value=AgentRecord(
            agent_id="test-agent", name="Test", status=AgentStatus.ACTIVE, capabilities=[]
        )
    )

    queue = DistributedTaskQueue(mock_redis)
    node_info = NodeInfo.create(port=8080)

    # Create a real agent for testing
    from src.agent_platform.core.agent import BaseAgent, AgentRuntimeState

    class EchoAgent(BaseAgent):
        async def initialize(self):
            self.state = AgentRuntimeState.RUNNING
            self._initialized = True

        async def run(self, task: Task):
            return f"Echo: {task.payload.get('message', '')}"

        async def shutdown(self):
            pass

    agent = EchoAgent("test-agent", "Test")
    agent_registry.get_agent = AsyncMock(return_value=agent)

    worker = WorkerNode(node_info, queue, agent_registry, WorkerConfig())
    await worker.start()
    assert worker.is_active is True

    await worker.stop()
    assert worker.is_active is False


@pytest.mark.asyncio
async def test_distributed_lock(mock_redis):
    from src.agent_platform.distributed.lock import DistributedLock

    lock = DistributedLock(mock_redis, "test-lock", ttl_seconds=5)

    # Mock successful acquire
    mock_redis.set.return_value = True
    acquired = await lock.acquire()
    assert acquired is True

    # Mock release
    mock_redis.eval.return_value = 1
    released = await lock.release()
    assert released is True
