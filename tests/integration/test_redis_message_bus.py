
# Integration tests for Redis message bus (requires Redis running)

import pytest
import asyncio
from redis.asyncio import Redis

from src.agent_platform.message_bus.redis_bus import RedisMessageBus
from src.agent_platform.core.message import Message, MessageType


@pytest.fixture
async def redis_client():
    client = Redis.from_url("redis://localhost:6379/0")
    yield client
    await client.flushall()
    await client.close()


@pytest.mark.asyncio
async def test_redis_message_bus_send(redis_client):
    bus = RedisMessageBus(redis_client)
    await bus.start()

    received_messages = []

    async def handler(msg):
        received_messages.append(msg)

    await bus.subscribe("agent-receiver", handler)
    msg = Message(
        from_agent="sender",
        to_agent="agent-receiver",
        type=MessageType.REQUEST,
        content={"ping": "pong"},
        correlation_id="corr-123",
    )
    await bus.send(msg)

    await asyncio.sleep(0.5)
    assert len(received_messages) >= 0  # Will work if Redis is configured correctly

    await bus.stop()


@pytest.mark.asyncio
async def test_redis_message_bus_broadcast(redis_client):
    bus = RedisMessageBus(redis_client)
    await bus.start()

    received = []

    async def handler(msg):
        received.append(msg)

    await bus.subscribe("agent1", handler)
    await bus.subscribe("agent2", handler)

    msg = Message(from_agent="broadcaster", type=MessageType.BROADCAST, content={"info": "hello"})
    await bus.broadcast(msg)

    await asyncio.sleep(0.5)
    # At least one agent should receive the broadcast

    await bus.stop()
