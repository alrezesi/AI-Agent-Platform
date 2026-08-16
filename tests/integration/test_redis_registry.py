
# Integration tests for Redis registry

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from src.agent_platform.core.agent import AgentRecord
from src.agent_platform.registry.redis_registry import RedisAgentRegistry


@pytest_asyncio.fixture
async def redis_client():
    client = Redis.from_url("redis://localhost:6379/0")
    await client.flushall()
    yield client
    await client.flushall()
    await client.aclose()


@pytest.mark.asyncio
async def test_redis_registry(redis_client):
    registry = RedisAgentRegistry(redis_client, ttl_seconds=10)
    agent = AgentRecord(agent_id="test", name="Test")
    await registry.register(agent)

    retrieved = await registry.get_agent("test")
    assert retrieved is not None
    assert retrieved.agent_id == "test"

    await registry.heartbeat("test")
    retrieved2 = await registry.get_agent("test")
    assert retrieved2 is not None

    await registry.unregister("test")
    retrieved3 = await registry.get_agent("test")
    assert retrieved3 is None
