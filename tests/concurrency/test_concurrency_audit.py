
# Concurrency audit for the existing task scheduling system.

# IMPORTANT:
# These tests intentionally do not modify production code.
# They are designed to discover concurrency defects in the current implementation.

import asyncio
from collections import Counter

import pytest

from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_scheduler() -> TaskScheduler:
    """Create an isolated scheduler instance for one concurrency test."""
    return TaskScheduler(InMemoryTaskQueue())


async def submit_task(
    scheduler: TaskScheduler,
    task_id: str | None = None,
) -> str:
    """Submit a deterministic test task."""
    return await scheduler.submit_task(
        agent_id="audit-agent",
        task_type="concurrency-audit",
        payload={"source": "concurrency-audit"},
        task_id=task_id,
    )


async def submit_many(
    scheduler: TaskScheduler,
    count: int,
    task_id_factory=None,
) -> list[str]:
    """
    Submit many tasks concurrently.

    The task_id_factory allows tests to generate either unique
    or intentionally duplicated task IDs.
    """
    if task_id_factory is None:
        task_id_factory = lambda index: f"audit-task-{index}"

    return await asyncio.gather(
        *[
            submit_task(
                scheduler,
                task_id=task_id_factory(index),
            )
            for index in range(count)
        ]
    )


async def dequeue_many(
    scheduler: TaskScheduler,
    worker_count: int,
) -> list[Task]:
    """
    Let multiple workers compete for tasks concurrently.

    Each worker performs one dequeue operation.
    """
    results = await asyncio.gather(
        *[
            scheduler.dequeue_next()
            for _ in range(worker_count)
        ]
    )

    return [task for task in results if task is not None]


# ===========================================================================
# 1. 100 CONCURRENT TASK SUBMISSIONS
# ===========================================================================


@pytest.mark.asyncio
async def test_100_concurrent_submissions_preserve_all_tasks():
    """100 unique concurrent submissions must create exactly 100 tasks."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 100)

    assert len(task_ids) == 100
    assert len(set(task_ids)) == 100

    stats = await scheduler.get_stats()

    assert stats.total == 100
    assert stats.pending == 100


@pytest.mark.asyncio
async def test_100_concurrent_submissions_have_no_lost_tasks():
    """No task should disappear during 100 concurrent submissions."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 100)

    stored_tasks = await scheduler.list_tasks(limit=200)

    stored_ids = {task.task_id for task in stored_tasks}

    assert stored_ids == set(task_ids)


@pytest.mark.asyncio
async def test_100_concurrent_submissions_return_unique_ids():
    """Every unique submission must receive a unique task ID."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 100)

    counts = Counter(task_ids)

    assert all(count == 1 for count in counts.values())


@pytest.mark.asyncio
async def test_100_concurrent_submissions_remain_pending():
    """Concurrent submission alone must not transition tasks to RUNNING."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 100)

    for task_id in task_ids:
        task = await scheduler.get_task(task_id)

        assert task is not None
        assert task.status == TaskStatus.PENDING


@pytest.mark.asyncio
async def test_100_concurrent_submissions_preserve_payload():
    """Every concurrently submitted task must retain its payload."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 100)

    for task_id in task_ids:
        task = await scheduler.get_task(task_id)

        assert task is not None
        assert task.payload == {"source": "concurrency-audit"}


@pytest.mark.asyncio
async def test_100_concurrent_submissions_preserve_agent_id():
    """Every task must retain its target agent."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 100)

    for task_id in task_ids:
        task = await scheduler.get_task(task_id)

        assert task is not None
        assert task.agent_id == "audit-agent"


@pytest.mark.asyncio
async def test_100_concurrent_submissions_queue_size_is_100():
    """The queue must contain exactly 100 pending tasks."""
    scheduler = build_scheduler()

    await submit_many(scheduler, 100)

    assert await scheduler.queue_size() == 100


@pytest.mark.asyncio
async def test_100_concurrent_submissions_can_all_be_dequeued():
    """All 100 submitted tasks must be available for execution."""
    scheduler = build_scheduler()

    await submit_many(scheduler, 100)

    dequeued = []

    for _ in range(100):
        task = await scheduler.dequeue_next()

        assert task is not None
        dequeued.append(task)

    assert len(dequeued) == 100
    assert len({task.task_id for task in dequeued}) == 100


@pytest.mark.asyncio
async def test_100_concurrent_submissions_do_not_duplicate_queue_entries():
    """A task must appear only once in the queue."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 100)

    dequeued = await dequeue_many(scheduler, 100)

    dequeued_ids = [task.task_id for task in dequeued]

    assert set(dequeued_ids) == set(task_ids)
    assert len(dequeued_ids) == len(set(dequeued_ids))


@pytest.mark.asyncio
async def test_100_concurrent_submissions_exact_task_count():
    """
    Audit evidence for the basic exactly-once creation property.

    This does not prove exactly-once execution.
    It only proves that 100 unique submissions create 100 logical tasks
    in the in-memory backend.
    """
    scheduler = build_scheduler()

    await submit_many(scheduler, 100)

    stats = await scheduler.get_stats()

    assert stats.total == 100


# ===========================================================================
# 2. 1000 CONCURRENT TASK SUBMISSIONS
# ===========================================================================


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_preserve_all_tasks():
    """1000 unique concurrent submissions must create exactly 1000 tasks."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 1000)

    assert len(task_ids) == 1000
    assert len(set(task_ids)) == 1000

    stats = await scheduler.get_stats()

    assert stats.total == 1000


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_have_no_lost_tasks():
    """No task may be lost under a 1000-request concurrency burst."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 1000)

    stored_tasks = await scheduler.list_tasks(limit=1100)

    assert len(stored_tasks) == 1000
    assert {task.task_id for task in stored_tasks} == set(task_ids)


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_have_unique_ids():
    """All 1000 unique submissions must have distinct IDs."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 1000)

    assert len(set(task_ids)) == 1000


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_preserve_pending_state():
    """All tasks must remain PENDING after submission."""
    scheduler = build_scheduler()

    await submit_many(scheduler, 1000)

    stats = await scheduler.get_stats()

    assert stats.pending == 1000
    assert stats.running == 0


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_queue_size():
    """The pending queue must contain exactly 1000 tasks."""
    scheduler = build_scheduler()

    await submit_many(scheduler, 1000)

    assert await scheduler.queue_size() == 1000


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_all_have_payload():
    """Every task must retain its original payload."""
    scheduler = build_scheduler()

    task_ids = await submit_many(scheduler, 1000)

    tasks = [
        await scheduler.get_task(task_id)
        for task_id in task_ids
    ]

    assert all(task is not None for task in tasks)
    assert all(
        task.payload == {"source": "concurrency-audit"}
        for task in tasks
    )


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_can_be_consumed():
    """All 1000 tasks must eventually be consumable."""
    scheduler = build_scheduler()

    await submit_many(scheduler, 1000)

    dequeued = []

    for _ in range(1000):
        task = await scheduler.dequeue_next()

        assert task is not None
        dequeued.append(task)

    assert len(dequeued) == 1000
    assert len({task.task_id for task in dequeued}) == 1000


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_do_not_duplicate_execution_slots():
    """
    Each dequeue operation must return a distinct task.

    This checks the queue's basic concurrency property.
    """
    scheduler = build_scheduler()

    await submit_many(scheduler, 1000)

    results = await asyncio.gather(
        *[
            scheduler.dequeue_next()
            for _ in range(1000)
        ]
    )

    tasks = [task for task in results if task is not None]

    assert len(tasks) == 1000
    assert len({task.task_id for task in tasks}) == 1000


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_leave_empty_queue_after_consumption():
    """The queue must become empty after consuming all submitted tasks."""
    scheduler = build_scheduler()

    await submit_many(scheduler, 1000)

    await asyncio.gather(
        *[
            scheduler.dequeue_next()
            for _ in range(1000)
        ]
    )

    assert await scheduler.queue_size() == 0


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_final_statistics():
    """Final statistics must account for all 1000 submitted tasks."""
    scheduler = build_scheduler()

    await submit_many(scheduler, 1000)

    results = await asyncio.gather(
        *[
            scheduler.dequeue_next()
            for _ in range(1000)
        ]
    )

    assert all(task is not None for task in results)

    stats = await scheduler.get_stats()

    assert stats.total == 1000
    assert stats.running == 1000


# ===========================================================================
# 3. 10 SIMULTANEOUS DUPLICATE SUBMISSIONS
# ===========================================================================


@pytest.mark.asyncio
async def test_10_duplicate_submissions_create_one_logical_task():
    """
    Ten simultaneous submissions using the same task ID must produce
    exactly one logical task.
    """
    scheduler = build_scheduler()

    results = await asyncio.gather(
        *[
            submit_task(scheduler, task_id="duplicate-task")
            for _ in range(10)
        ]
    )

    assert results == ["duplicate-task"] * 10

    stats = await scheduler.get_stats()

    assert stats.total == 1


@pytest.mark.asyncio
async def test_10_duplicate_submissions_return_same_task_id():
    """All duplicate submissions must resolve to the same task ID."""
    scheduler = build_scheduler()

    results = await asyncio.gather(
        *[
            submit_task(scheduler, task_id="same-task")
            for _ in range(10)
        ]
    )

    assert len(set(results)) == 1
    assert results[0] == "same-task"


@pytest.mark.asyncio
async def test_10_duplicate_submissions_create_one_queue_entry():
    """Duplicate submissions must not create multiple queue entries."""
    scheduler = build_scheduler()

    await asyncio.gather(
        *[
            submit_task(scheduler, task_id="queue-duplicate")
            for _ in range(10)
        ]
    )

    assert await scheduler.queue_size() == 1


@pytest.mark.asyncio
async def test_10_duplicate_submissions_dequeue_once():
    """A duplicated logical task must be dequeued only once."""
    scheduler = build_scheduler()

    await asyncio.gather(
        *[
            submit_task(scheduler, task_id="execution-duplicate")
            for _ in range(10)
        ]
    )

    results = await asyncio.gather(
        *[
            scheduler.dequeue_next()
            for _ in range(10)
        ]
    )

    tasks = [task for task in results if task is not None]

    assert len(tasks) == 1
    assert tasks[0].task_id == "execution-duplicate"


@pytest.mark.asyncio
async def test_duplicate_submission_is_idempotent_at_creation_layer():
    """
    This verifies at-most-once logical task creation for the
    in-memory backend.

    It does NOT prove exactly-once business execution.
    """
    scheduler = build_scheduler()

    results = await asyncio.gather(
        *[
            submit_task(scheduler, task_id="idempotent-task")
            for _ in range(10)
        ]
    )

    assert len(set(results)) == 1

    stats = await scheduler.get_stats()

    assert stats.total == 1
    assert stats.pending == 1


# ===========================================================================
# 4. 10 WORKERS COMPETING FOR TASKS
# ===========================================================================


@pytest.mark.asyncio
async def test_10_workers_competing_for_one_task_only_one_wins():
    """
    Ten concurrent workers competing for one queued task must result
    in exactly one successful dequeue.
    """
    scheduler = build_scheduler()

    await submit_task(scheduler, task_id="single-task")

    results = await asyncio.gather(
        *[
            scheduler.dequeue_next()
            for _ in range(10)
        ]
    )

    winners = [task for task in results if task is not None]

    assert len(winners) == 1
    assert winners[0].task_id == "single-task"


@pytest.mark.asyncio
async def test_10_workers_cannot_receive_the_same_task_instance_twice():
    """A task must never be returned to two concurrent workers."""
    scheduler = build_scheduler()

    await submit_task(scheduler, task_id="worker-race")

    results = await asyncio.gather(
        *[
            scheduler.dequeue_next()
            for _ in range(10)
        ]
    )

    task_ids = [
        task.task_id
        for task in results
        if task is not None
    ]

    assert task_ids == ["worker-race"]


@pytest.mark.asyncio
async def test_10_workers_competing_for_ten_tasks_get_distinct_tasks():
    """Ten workers competing for ten tasks must receive ten distinct tasks."""
    scheduler = build_scheduler()

    await submit_many(scheduler, 10)

    results = await asyncio.gather(
        *[
            scheduler.dequeue_next()
            for _ in range(10)
        ]
    )

    tasks = [task for task in results if task is not None]

    assert len(tasks) == 10
    assert len({task.task_id for task in tasks}) == 10


@pytest.mark.asyncio
async def test_10_workers_competing_for_fewer_tasks_do_not_duplicate():
    """Workers must not duplicate work when workers outnumber tasks."""
    scheduler = build_scheduler()

    await submit_many(scheduler, 5)

    results = await asyncio.gather(
        *[
            scheduler.dequeue_next()
            for _ in range(10)
        ]
    )

    tasks = [task for task in results if task is not None]

    assert len(tasks) == 5
    assert len({task.task_id for task in tasks}) == 5


@pytest.mark.asyncio
async def test_10_workers_competing_leave_no_pending_tasks():
    """After ten workers consume ten tasks, no task should remain pending."""
    scheduler = build_scheduler()

    await submit_many(scheduler, 10)

    results = await asyncio.gather(
        *[
            scheduler.dequeue_next()
            for _ in range(10)
        ]
    )

    assert all(task is not None for task in results)

    stats = await scheduler.get_stats()

    assert stats.total == 10
    assert stats.pending == 0
    assert stats.running == 10
