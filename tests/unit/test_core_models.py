
# Unit tests for core models

import pytest
from datetime import datetime
from src.agent_platform.core.agent import AgentRecord, AgentStatus, AgentCapability
from src.agent_platform.core.task import Task, TaskPriority, TaskStatus
from src.agent_platform.core.message import Message, MessageType


def test_agent_record_creation():
    agent = AgentRecord(
        agent_id="test-001",
        name="Test Agent",
        capabilities=[AgentCapability(name="echo", description="Echoes input")],
    )
    assert agent.agent_id == "test-001"
    assert agent.status == AgentStatus.ACTIVE
    assert isinstance(agent.registered_at, datetime)


def test_task_defaults():
    task = Task(
        task_id="t1",
        agent_id="a1",
        type="echo",
        payload={"text": "hello"}
    )
    assert task.status == TaskStatus.PENDING
    assert task.priority == TaskPriority.MEDIUM
    assert task.retry_count == 0


def test_message_validation():
    msg = Message(
        message_id="m1",
        from_agent="a1",
        to_agent="a2",
        type=MessageType.REQUEST,
        content={"query": "ping"}
    )
    assert msg.type == MessageType.REQUEST