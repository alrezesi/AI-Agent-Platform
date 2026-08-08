
# Unit tests for Message Bus implementations

import pytest
import asyncio

from src.agent_platform.core.message import Message, MessageType, MessagePriority
from src.agent_platform.message_bus.in_memory import InMemoryMessageBus
from src.agent_platform.message_bus.validator import MessageValidator
from src.agent_platform.message_bus.exceptions import MessageValidationError


@pytest.fixture
async def bus():
    bus = InMemoryMessageBus()
    await bus.start()
    yield bus
    await bus.stop()


@pytest.mark.asyncio
async def test_send_and_receive_point_to_point(bus):
    # Setup subscriber
    received_messages = []
    async def handler(msg):
        received_messages.append(msg)

    await bus.subscribe("agent-receiver", handler)

    # Send message
    msg = Message(
        from_agent="agent-sender",
        to_agent="agent-receiver",
        type=MessageType.REQUEST,
        content={"command": "ping"},
        correlation_id="corr-123"
    )
    msg_id = await bus.send(msg)

    # Wait for delivery
    await asyncio.sleep(0.1)

    assert len(received_messages) == 1
    assert received_messages[0].message_id == msg_id
    assert received_messages[0].content == {"command": "ping"}


@pytest.mark.asyncio
async def test_broadcast(bus):
    # Setup multiple subscribers
    received1 = []
    received2 = []

    async def handler1(msg):
        received1.append(msg)
    async def handler2(msg):
        received2.append(msg)

    await bus.subscribe("agent-1", handler1)
    await bus.subscribe("agent-2", handler2)

    # Broadcast
    msg = Message(
        from_agent="broadcaster",
        to_agent=None,
        type=MessageType.BROADCAST,
        content={"info": "hello everyone"}
    )
    await bus.broadcast(msg)

    await asyncio.sleep(0.1)

    assert len(received1) == 1
    assert len(received2) == 1
    assert received1[0].content == {"info": "hello everyone"}


@pytest.mark.asyncio
async def test_publish_topic(bus):
    received = []

    async def handler(msg):
        received.append(msg)

    # Subscribe to topic
    await bus.subscribe("agent-topic", handler, topics=["weather"])

    # Publish to topic
    msg = Message(
        from_agent="weather-service",
        to_agent=None,
        type=MessageType.EVENT,
        content={"temperature": 25},
        topic="weather"
    )
    await bus.publish("weather", msg)

    await asyncio.sleep(0.1)

    assert len(received) == 1
    assert received[0].topic == "weather"


@pytest.mark.asyncio
async def test_message_validation():
    # Valid message
    valid_msg = Message(
        from_agent="a1",
        to_agent="a2",
        type=MessageType.REQUEST,
        content={"x": 1},
        correlation_id="c1"
    )
    assert MessageValidator.is_valid(valid_msg) is True

    # Invalid: missing to_agent for request
    invalid_msg = Message(
        from_agent="a1",
        to_agent=None,
        type=MessageType.REQUEST,
        content={"x": 1}
    )
    assert MessageValidator.is_valid(invalid_msg) is False

    # Invalid: missing correlation_id for request
    invalid_msg2 = Message(
        from_agent="a1",
        to_agent="a2",
        type=MessageType.REQUEST,
        content={"x": 1}
    )
    assert MessageValidator.is_valid(invalid_msg2) is False


@pytest.mark.asyncio
async def test_acknowledge(bus):
    # Setup
    ack_received = False
    async def handler(msg):
        nonlocal ack_received
        ack_received = True
        await bus.acknowledge(msg.message_id, "agent-receiver")

    await bus.subscribe("agent-receiver", handler)

    msg = Message(
        from_agent="a1",
        to_agent="agent-receiver",
        type=MessageType.COMMAND,
        content={"cmd": "do"}
    )
    await bus.send(msg)

    await asyncio.sleep(0.1)

    assert ack_received is True
    # Check message status if we implement it



def test_message_validator_response_validation():
    from src.agent_platform.message_bus.validator import MessageValidator

    # Valid response
    msg = Message(
        from_agent="a1",
        to_agent="a2",
        type=MessageType.RESPONSE,
        content={"result": "ok"},
        correlation_id="corr123",
    )
    assert MessageValidator.is_valid(msg) is True

    # Missing correlation_id
    msg2 = Message(
        from_agent="a1", to_agent="a2", type=MessageType.RESPONSE, content={"result": "ok"}
    )
    assert MessageValidator.is_valid(msg2) is False


def test_message_validator_broadcast_validation():
    from src.agent_platform.message_bus.validator import MessageValidator

    # Valid broadcast
    msg = Message(
        from_agent="a1", to_agent=None, type=MessageType.BROADCAST, content={"info": "hello"}
    )
    assert MessageValidator.is_valid(msg) is True

    # Invalid: broadcast with specific target
    msg2 = Message(
        from_agent="a1", to_agent="a2", type=MessageType.BROADCAST, content={"info": "hello"}
    )
    assert MessageValidator.is_valid(msg2) is False


def test_message_validator_command_validation():
    from src.agent_platform.message_bus.validator import MessageValidator

    # Valid command
    msg = Message(from_agent="a1", to_agent="a2", type=MessageType.COMMAND, content={"cmd": "do"})
    assert MessageValidator.is_valid(msg) is True

    # Invalid: missing target
    msg2 = Message(from_agent="a1", to_agent=None, type=MessageType.COMMAND, content={"cmd": "do"})
    assert MessageValidator.is_valid(msg2) is False