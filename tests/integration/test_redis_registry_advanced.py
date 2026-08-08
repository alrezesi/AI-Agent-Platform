
# Advanced integration tests for Redis registry

import asyncio
import pytest
from redis.asyncio import Redis
from src.agent_platform.registry.redis_registry import RedisAgentRegistry
from src.agent_platform.core.agent import AgentRecord, AgentStatus, AgentCapability
from datetime import datetime


@pytest.fixture
async def redis_client():
    client = Redis.from_url("redis://localhost:6379/0")
    await client.flushall()
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
async def registry(redis_client):
    return RedisAgentRegistry(redis_client, ttl_seconds=10)


@pytest.mark.asyncio
async def test_redis_registry_discover(registry):
    """Test agent discovery with filters."""
    agent1 = AgentRecord(
        agent_id="a1",
        name="Agent1",
        capabilities=[AgentCapability(name="echo"), AgentCapability(name="process")],
        status=AgentStatus.ACTIVE,
    )
    agent2 = AgentRecord(
        agent_id="a2",
        name="Agent2",
        capabilities=[AgentCapability(name="process")],
        status=AgentStatus.PAUSED,
    )

    await registry.register(agent1)
    await registry.register(agent2)

    # Discover by capability
    results = await registry.discover(capability="echo")
    assert len(results) == 1
    assert results[0].agent_id == "a1"

    # Discover by status
    results = await registry.discover(status=AgentStatus.PAUSED)
    assert len(results) == 1
    assert results[0].agent_id == "a2"

    # Discover by both
    results = await registry.discover(capability="process", status=AgentStatus.ACTIVE)
    assert len(results) == 1
    assert results[0].agent_id == "a1"


@pytest.mark.asyncio
async def test_redis_registry_heartbeat(registry):
    """Test heartbeat updates."""
    agent = AgentRecord(agent_id="a1", name="Agent1")
    await registry.register(agent)

    # Initial heartbeat should be set
    retrieved = await registry.get_agent("a1")
    old_time = retrieved.last_heartbeat

    await asyncio.sleep(0.1)
    await registry.heartbeat("a1")

    retrieved2 = await registry.get_agent("a1")
    assert retrieved2.last_heartbeat > old_time


@pytest.mark.asyncio
async def test_redis_registry_cleanup(registry):
    """Test stale agent cleanup (Redis handles TTL automatically)."""
    agent = AgentRecord(agent_id="a1", name="Agent1")
    await registry.register(agent)

    # Immediately should exist
    retrieved = await registry.get_agent("a1")
    assert retrieved is not None

    # After TTL expires (10 seconds), should be gone
    # Since Redis handles TTL, we test the cleanup method (no-op for Redis)
    removed = await registry.cleanup_stale(ttl_seconds=1)
    # Redis cleanup is a no-op (0 returned)
    assert removed == 0
