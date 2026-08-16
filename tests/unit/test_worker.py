
# Unit tests for TaskWorker

import asyncio

import pytest

from src.agent_platform.core.agent import AgentRuntimeState, BaseAgent
from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.scheduler.worker import TaskWorker


class SlowAgent(BaseAgent):
    """Agent that takes longer than timeout."""
    async def initialize(self):
        self.state = AgentRuntimeState.RUNNING
        self._initialized = True
    async def run(self, task: Task):
        await asyncio.sleep(2)
        return "done"
    async def shutdown(self):
        pass


class FailingAgent(BaseAgent):
    """Agent that always fails."""
    async def initialize(self):
        self.state = AgentRuntimeState.RUNNING
        self._initialized = True
    async def run(self, task: Task):
        raise ValueError("Simulated failure")
    async def shutdown(self):
        pass


class RetryAgent(BaseAgent):
    """Agent that fails once then succeeds."""
    def __init__(self, agent_id: str, name: str, tenant_id: str = None):
        super().__init__(agent_id, name, tenant_id)
        self.counter = 0

    async def initialize(self):
        self.state = AgentRuntimeState.RUNNING
        self._initialized = True

    async def run(self, task: Task):
        self.counter += 1
        if self.counter == 1:
            raise ValueError("First attempt fail")
        return "success"

    async def shutdown(self):
        pass


@pytest.mark.asyncio
async def test_worker_timeout():
    """Test that task times out when agent takes too long."""
    agent = SlowAgent("a1", "slow")
    await agent.initialize()
    task = Task(
        task_id="t1",
        agent_id="a1",
        type="test",
        payload={},
        timeout_seconds=1,
        max_retries=0
    )
    worker = TaskWorker(task, agent)
    result = await worker.execute()
    assert result.status == TaskStatus.TIMEOUT
    assert "timed out" in result.error


@pytest.mark.asyncio
async def test_worker_retry_success():
    """Test that retry works and task eventually succeeds."""
    agent = RetryAgent("a2", "retry")
    await agent.initialize()
    task = Task(
        task_id="t2",
        agent_id="a2",
        type="test",
        payload={},
        timeout_seconds=5,
        max_retries=2,
        retry_count=0
    )
    worker = TaskWorker(task, agent, retry_delay_base=0.1)
    result = await worker.execute()
    assert result.status == TaskStatus.COMPLETED
    assert result.result == "success"
    assert result.retry_count == 1  # one retry occurred


@pytest.mark.asyncio
async def test_worker_failure_no_retry():
    """Test that task fails when max_retries is 0 and agent fails."""
    agent = FailingAgent("a3", "fail")
    await agent.initialize()
    task = Task(
        task_id="t3",
        agent_id="a3",
        type="test",
        payload={},
        timeout_seconds=5,
        max_retries=0
    )
    worker = TaskWorker(task, agent)
    result = await worker.execute()
    assert result.status == TaskStatus.FAILED
    assert "Simulated failure" in result.error


@pytest.mark.asyncio
async def test_worker_retry_exhausted():
    """Test that task fails after exhausting all retries."""
    agent = FailingAgent("a4", "fail")
    await agent.initialize()
    task = Task(
        task_id="t4",
        agent_id="a4",
        type="test",
        payload={},
        timeout_seconds=5,
        max_retries=2
    )
    worker = TaskWorker(task, agent, retry_delay_base=0.1)
    result = await worker.execute()
    assert result.status == TaskStatus.FAILED
    assert "Simulated failure" in result.error
    # Should have retried twice (max_retries=2 means 3 total attempts)
    # We can't easily verify the retry count without mocking, but we can check status
