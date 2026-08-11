
# Unit tests for Task Scheduler components

import pytest
import asyncio

from src.agent_platform.core.task import TaskPriority, TaskStatus
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.scheduler.models import TaskFilterOptions


@pytest.fixture
def scheduler():
    queue = InMemoryTaskQueue()
    return TaskScheduler(queue)


@pytest.mark.asyncio
async def test_submit_and_get_task(scheduler):
    task_id = await scheduler.submit_task(
        agent_id="agent-1",
        task_type="echo",
        payload={"msg": "hello"},
    )
    task = await scheduler.get_task(task_id)
    assert task is not None
    assert task.agent_id == "agent-1"
    assert task.status == TaskStatus.PENDING


@pytest.mark.asyncio
async def test_dequeue_priority_order(scheduler):
    # Submit low priority first, then high priority
    id1 = await scheduler.submit_task("a1", "low", {}, priority=TaskPriority.LOW)
    id2 = await scheduler.submit_task("a1", "high", {}, priority=TaskPriority.HIGH)

    next_task = await scheduler.dequeue_next()
    assert next_task.task_id == id2  # High priority should come first

    next_task = await scheduler.dequeue_next()
    assert next_task.task_id == id1


@pytest.mark.asyncio
async def test_cancel_task(scheduler):
    task_id = await scheduler.submit_task("a1", "test", {})
    cancelled = await scheduler.cancel_task(task_id)
    assert cancelled is True

    task = await scheduler.get_task(task_id)
    assert task.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_list_tasks_with_filters(scheduler):
    await scheduler.submit_task("a1", "type1", {})
    await scheduler.submit_task("a2", "type2", {})

    filters = TaskFilterOptions(agent_id="a1")
    tasks = await scheduler.list_tasks(filters)
    assert len(tasks) == 1
    assert tasks[0].agent_id == "a1"


@pytest.mark.asyncio
async def test_get_stats(scheduler):
    await scheduler.submit_task("a1", "t1", {})
    await scheduler.submit_task("a1", "t2", {})
    # Dequeue one to make it running
    await scheduler.dequeue_next()

    stats = await scheduler.get_stats()
    assert stats.total == 2
    assert stats.pending == 1
    assert stats.running == 1
