
# Unit tests for Agent Engine

import asyncio

import pytest

from src.agent_platform.core.agent import AgentRuntimeState, BaseAgent
from src.agent_platform.core.task import Task, TaskPriority, TaskStatus
from src.agent_platform.engine.engine import AgentEngine
from src.agent_platform.registry.in_memory import InMemoryAgentRegistry
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler


class EchoAgent(BaseAgent):
    """Simple test agent that echoes back the payload."""
    async def initialize(self) -> None:
        self._initialized = True
        self.state = AgentRuntimeState.RUNNING

    async def run(self, task: Task) -> str:
        return f"Echo: {task.payload.get('message', '')}"

    async def shutdown(self) -> None:
        self._initialized = False


@pytest.fixture
def engine():
    registry = InMemoryAgentRegistry()
    scheduler = TaskScheduler(InMemoryTaskQueue())
    return AgentEngine(registry, scheduler, poll_interval=0.1)


@pytest.mark.asyncio
async def test_register_agent(engine):
    agent = EchoAgent("echo-1", "EchoBot")
    await engine.register_agent(agent)

    assert "echo-1" in engine._agents
    assert engine._agents["echo-1"].is_ready()


@pytest.mark.asyncio
async def test_unregister_agent(engine):
    agent = EchoAgent("echo-2", "EchoBot")
    await engine.register_agent(agent)

    removed = await engine.unregister_agent("echo-2")
    assert removed is True
    assert "echo-2" not in engine._agents


@pytest.mark.asyncio
async def test_engine_start_stop(engine):
    agent = EchoAgent("echo-3", "EchoBot")
    await engine.register_agent(agent)

    await engine.start()
    assert engine.is_running is True

    await engine.stop()
    assert engine.is_running is False


@pytest.mark.asyncio
async def test_task_execution(engine):
    # Setup
    agent = EchoAgent("echo-4", "EchoBot")
    await engine.register_agent(agent)

    # Submit task via scheduler
    task_id = await engine.scheduler.submit_task(
        agent_id="echo-4",
        task_type="echo",
        payload={"message": "Hello World"},
        priority=TaskPriority.HIGH,
    )

    # Start engine to process
    await engine.start()

    # Wait for processing
    await asyncio.sleep(0.5)

    # Check task result
    task = await engine.scheduler.get_task(task_id)
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert task.result == "Echo: Hello World"

    await engine.stop()


@pytest.mark.asyncio
async def test_pause_agent(engine):
    agent = EchoAgent("echo-5", "EchoBot")
    await engine.register_agent(agent)

    paused = await engine.pause_agent("echo-5")
    assert paused is True

    state = await engine.get_agent_state("echo-5")
    assert state == AgentRuntimeState.PAUSED
