
# Unit tests for Agent Registry implementations

import pytest
import asyncio
from datetime import datetime, timedelta

from src.agent_platform.core.agent import AgentRecord, AgentStatus, AgentCapability
from src.agent_platform.registry.in_memory import InMemoryAgentRegistry


@pytest.fixture
def registry():
    return InMemoryAgentRegistry()


@pytest.mark.asyncio
async def test_register_and_get(registry):
    agent = AgentRecord(
        agent_id="test-1",
        name="Tester",
        capabilities=[AgentCapability(name="ping")],
    )
    await registry.register(agent)

    retrieved = await registry.get_agent("test-1")
    assert retrieved is not None
    assert retrieved.agent_id == "test-1"
    assert retrieved.status == AgentStatus.ACTIVE


@pytest.mark.asyncio
async def test_heartbeat(registry):
    agent = AgentRecord(agent_id="test-2", name="Heartbeat")
    await registry.register(agent)

    old_time = agent.last_heartbeat
    await asyncio.sleep(0.1)  # small delay
    await registry.heartbeat("test-2")

    updated = await registry.get_agent("test-2")
    assert updated.last_heartbeat > old_time


@pytest.mark.asyncio
async def test_discover_by_capability(registry):
    agent1 = AgentRecord(agent_id="a1", name="A1", capabilities=[AgentCapability(name="echo")])
    agent2 = AgentRecord(agent_id="a2", name="A2", capabilities=[AgentCapability(name="math")])
    await registry.register(agent1)
    await registry.register(agent2)

    results = await registry.discover(capability="echo")
    assert len(results) == 1
    assert results[0].agent_id == "a1"


@pytest.mark.asyncio
async def test_cleanup_stale(registry):
    agent = AgentRecord(agent_id="stale", name="Stale")
    # Register agent normally
    await registry.register(agent)

    # Manually set the heartbeat to a stale value in the internal dictionary
    # This bypasses the register method which would reset the timestamp
    async with registry._lock:
        registry._agents["stale"].last_heartbeat = datetime.utcnow() - timedelta(seconds=100)

    removed = await registry.cleanup_stale(ttl_seconds=30)
    assert removed == 1
    assert await registry.get_agent("stale") is None
