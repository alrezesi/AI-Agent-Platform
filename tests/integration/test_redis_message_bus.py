# tests/integration/test_redis_message_bus.py
# Integration tests for Redis message bus (requires Docker with Redis running)

import asyncio

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from src.agent_platform.core.message import Message, MessageType
from src.agent_platform.message_bus.redis_bus import RedisMessageBus


@pytest_asyncio.fixture
async def redis_client():
    client = Redis.from_url("redis://localhost:6379/0")
    await client.flushall()
    yield client
    await client.flushall()
    await client.aclose()


@pytest.mark.asyncio
async def test_redis_message_bus_send(redis_client):
    bus = RedisMessageBus(redis_client)
    await bus.start()

    received_messages = []

    async def handler(msg):
        received_messages.append(msg)

    await bus.subscribe("agent-receiver", handler)

    # Wait for worker to start
    await asyncio.sleep(1.0)

    msg = Message(
        from_agent="sender",
        to_agent="agent-receiver",
        type=MessageType.REQUEST,
        content={"ping": "pong"},
        correlation_id="corr-123",
    )
    await bus.send(msg)

    # Poll for delivery
    timeout = 5.0
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < timeout:
        if len(received_messages) >= 1:
            break
        await asyncio.sleep(0.1)

    assert len(received_messages) >= 1, f"Expected at least 1 message, got {len(received_messages)}"
    assert received_messages[0].content == {"ping": "pong"}

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

    await asyncio.sleep(1.0)

    msg = Message(
        from_agent="broadcaster",
        type=MessageType.BROADCAST,
        content={"info": "hello everyone"}
    )
    await bus.broadcast(msg)

    timeout = 5.0
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < timeout:
        if len(received) >= 2:
            break
        await asyncio.sleep(0.1)

    assert len(received) >= 2
    for r in received:
        assert r.content == {"info": "hello everyone"}

    await bus.stop()
