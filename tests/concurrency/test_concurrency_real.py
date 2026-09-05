"""
Concurrency tests using the REAL RedisTaskQueue backed by live Redis
and live PostgreSQL.

These tests complement the InMemoryTaskQueue-based tests in
test_concurrency_audit.py by exercising the full persistence and
queueing stack under concurrent load.
"""

import asyncio
import uuid

import pytest

from src.agent_platform.core.task import TaskStatus
from src.agent_platform.scheduler.scheduler import TaskScheduler

# ---------------------------------------------------------------------------
# 100 concurrent task submissions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_100_concurrent_submissions_real_queue(redis_queue, clean_db):
    """100 concurrent task submissions to the real Redis+PostgreSQL queue."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-100-{i:03d}" for i in range(100)]

    results = await asyncio.gather(*[
        scheduler.submit_task("agent", "echo", {"i": i}, task_id=tid)
        for i, tid in enumerate(task_ids)
    ])

    assert all(r in task_ids for r in results)
    assert len(set(results)) == 100  # all unique

    # All tasks must be in Redis
    for tid in task_ids:
        task = await redis_queue.get_task(tid)
        assert task is not None
        assert task.task_id == tid


@pytest.mark.asyncio
async def test_100_concurrent_submissions_no_lost_tasks(redis_queue, clean_db):
    """No task is lost when 100 are submitted concurrently."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-noloss-{i:03d}" for i in range(100)]

    await asyncio.gather(*[
        scheduler.submit_task("agent", "echo", {}, task_id=tid)
        for tid in task_ids
    ])

    # Queue size must be exactly 100
    zcard = await redis_queue.redis.zcard(redis_queue.QUEUE_KEY)
    assert zcard == 100


@pytest.mark.asyncio
async def test_100_concurrent_submissions_preserve_payload(redis_queue, clean_db):
    """All 100 payloads must be stored and retrieved intact."""
    scheduler = TaskScheduler(redis_queue)

    await asyncio.gather(*[
        scheduler.submit_task("agent", "echo", {"payload_id": i}, task_id=f"real-payload-{i:03d}")
        for i in range(100)
    ])

    for i in range(100):
        task = await redis_queue.get_task(f"real-payload-{i:03d}")
        assert task.payload["payload_id"] == i


@pytest.mark.asyncio
async def test_100_concurrent_submissions_remain_pending(redis_queue, clean_db):
    """All 100 tasks must remain in PENDING state until dequeued."""
    scheduler = TaskScheduler(redis_queue)

    await asyncio.gather(*[
        scheduler.submit_task("agent", "echo", {}, task_id=f"real-pending-{i:03d}")
        for i in range(100)
    ])

    for i in range(100):
        task = await redis_queue.get_task(f"real-pending-{i:03d}")
        assert task.status == TaskStatus.PENDING


@pytest.mark.asyncio
async def test_100_concurrent_submissions_queue_size_correct(redis_queue, clean_db):
    """The zset queue size must be exactly 100 after 100 concurrent submissions."""
    scheduler = TaskScheduler(redis_queue)

    await asyncio.gather(*[
        scheduler.submit_task("agent", "echo", {}, task_id=f"real-qsize-{i:03d}")
        for i in range(100)
    ])

    zcard = await redis_queue.redis.zcard(redis_queue.QUEUE_KEY)
    assert zcard == 100


# ---------------------------------------------------------------------------
# 1000 concurrent task submissions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_1000_concurrent_submissions_all_present(redis_queue, clean_db):
    """1000 concurrent submissions: all tasks must be present in Redis."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-1000-{i:04d}" for i in range(1000)]

    semaphore = asyncio.Semaphore(50)

    async def submit_limited(tid: str) -> str:
        async with semaphore:
            return await scheduler.submit_task("agent", "echo", {}, task_id=tid)

    await asyncio.gather(*[submit_limited(tid) for tid in task_ids])

    zcard = await redis_queue.redis.zcard(redis_queue.QUEUE_KEY)
    assert zcard == 1000

    # Spot-check some tasks
    for i in range(0, 1000, 100):
        task = await redis_queue.get_task(task_ids[i])
        assert task is not None
        assert task.task_id == task_ids[i]


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_unique_ids(redis_queue, clean_db):
    """All 1000 task IDs must be unique (no lost IDs)."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-1000-unique-{i:04d}" for i in range(1000)]

    semaphore = asyncio.Semaphore(50)

    async def submit_limited(tid: str) -> str:
        async with semaphore:
            return await scheduler.submit_task("agent", "echo", {}, task_id=tid)

    results = await asyncio.gather(*[submit_limited(tid) for tid in task_ids])

    assert len(set(results)) == 1000


@pytest.mark.asyncio
async def test_1000_concurrent_submissions_all_consumable(redis_queue, clean_db):
    """After 1000 concurrent submissions, all tasks must be consumable."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-1000-consume-{i:04d}" for i in range(1000)]

    semaphore = asyncio.Semaphore(50)

    async def submit_limited(tid: str) -> str:
        async with semaphore:
            return await scheduler.submit_task("agent", "echo", {}, task_id=tid)

    await asyncio.gather(*[submit_limited(tid) for tid in task_ids])

    consumed = 0
    while True:
        task = await redis_queue.dequeue(worker_id="batch-worker")
        if task is None:
            break
        consumed += 1

    assert consumed == 1000


# ---------------------------------------------------------------------------
# 10 simultaneous duplicate submissions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_10_duplicate_submissions_create_one_task(redis_queue, clean_db):
    """10 concurrent submissions of the same task_id must produce exactly one task."""
    scheduler = TaskScheduler(redis_queue)
    task_id = f"real-dup-{uuid.uuid4().hex[:8]}"

    results = await asyncio.gather(*[
        scheduler.submit_task("agent", "echo", {"attempt": i}, task_id=task_id)
        for i in range(10)
    ])

    assert all(r == task_id for r in results)
    zcard = await redis_queue.redis.zcard(redis_queue.QUEUE_KEY)
    assert zcard == 1


@pytest.mark.asyncio
async def test_10_duplicate_submissions_dequeue_once(redis_queue, clean_db):
    """After 10 duplicate submissions, the task must be dequeued exactly once."""
    scheduler = TaskScheduler(redis_queue)
    task_id = f"real-dup-deq-{uuid.uuid4().hex[:8]}"

    await asyncio.gather(*[
        scheduler.submit_task("agent", "echo", {}, task_id=task_id)
        for _ in range(10)
    ])

    claims = await asyncio.gather(*[
        redis_queue.dequeue(worker_id=f"dup-w-{i}")
        for i in range(10)
    ])

    non_none = [c for c in claims if c is not None]
    assert len(non_none) == 1
    assert non_none[0].task_id == task_id


# ---------------------------------------------------------------------------
# 10 workers competing for the same task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_10_workers_compete_for_one_task(redis_queue, clean_db):
    """10 concurrent workers dequeue: only one must win the task."""
    scheduler = TaskScheduler(redis_queue)
    task_id = f"real-compete-{uuid.uuid4().hex[:8]}"

    await scheduler.submit_task("agent", "echo", {}, task_id=task_id)

    results = await asyncio.gather(*[
        redis_queue.dequeue(worker_id=f"compete-w-{i}")
        for i in range(10)
    ])

    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert winners[0].task_id == task_id


@pytest.mark.asyncio
async def test_10_workers_compete_for_ten_tasks(redis_queue, clean_db):
    """10 workers competing for 10 tasks: each task claimed by exactly one worker."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-10w-10t-{i:02d}" for i in range(10)]

    for tid in task_ids:
        await scheduler.submit_task("agent", "echo", {}, task_id=tid)

    results = await asyncio.gather(*[
        redis_queue.dequeue(worker_id=f"w10-{i}")
        for i in range(10)
    ])

    winners = [r for r in results if r is not None]
    assert len(winners) == 10
    claimed_ids = {w.task_id for w in winners}
    assert claimed_ids == set(task_ids)


@pytest.mark.asyncio
async def test_10_workers_compete_for_fewer_tasks_no_duplicates(redis_queue, clean_db):
    """10 workers competing for 3 tasks: exactly 3 wins, no duplicates."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = ["real-3task-001", "real-3task-002", "real-3task-003"]

    for tid in task_ids:
        await scheduler.submit_task("agent", "echo", {}, task_id=tid)

    results = await asyncio.gather(*[
        redis_queue.dequeue(worker_id=f"w3-{i}")
        for i in range(10)
    ])

    winners = [r for r in results if r is not None]
    assert len(winners) == 3
    claimed_ids = {w.task_id for w in winners}
    assert claimed_ids == set(task_ids)


# ---------------------------------------------------------------------------
# Additional concurrency: lost tasks, duplicates, consistency
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_duplicated_queue_entries(redis_queue, clean_db):
    """Concurrent submissions must not create duplicate zset entries."""
    scheduler = TaskScheduler(redis_queue)

    await asyncio.gather(*[
        scheduler.submit_task("agent", "echo", {}, task_id=f"real-nodup-{i:03d}")
        for i in range(50)
    ])

    # Each task appears exactly once in the zset
    for i in range(50):
        score = await redis_queue.redis.zscore(redis_queue.QUEUE_KEY, f"real-nodup-{i:03d}")
        assert score is not None
        # Verify task exists in DB
        task = await redis_queue.get_task(f"real-nodup-{i:03d}")
        assert task is not None


@pytest.mark.asyncio
async def test_concurrent_dequeue_no_duplicate_execution(redis_queue, clean_db):
    """Concurrent dequeue of multiple tasks: each task claimed by exactly one worker."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-cons-deq-{i:03d}" for i in range(20)]

    for tid in task_ids:
        await scheduler.submit_task("agent", "echo", {}, task_id=tid)

    results = await asyncio.gather(*[
        redis_queue.dequeue(worker_id=f"cons-w-{i}")
        for i in range(20)
    ])

    winners = [r for r in results if r is not None]
    assert len(winners) == 20
    claimed_ids = {w.task_id for w in winners}
    assert claimed_ids == set(task_ids)


@pytest.mark.asyncio
async def test_concurrent_completion_state_consistency(redis_queue, clean_db):
    """Tasks completed concurrently must all end in COMPLETED state."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-cons-comp-{i:03d}" for i in range(20)]

    for tid in task_ids:
        await scheduler.submit_task("agent", "echo", {}, task_id=tid)

    # All workers dequeue and complete concurrently
    async def claim_and_complete(worker_id):
        task = await redis_queue.dequeue(worker_id=worker_id, lease_seconds=60)
        if task:
            task.status = TaskStatus.COMPLETED
            task.result = {"worker": worker_id}
            await redis_queue.update_task(task)
        return task

    results = await asyncio.gather(*[
        claim_and_complete(f"comp-w-{i}")
        for i in range(20)
    ])

    winners = [r for r in results if r is not None]
    assert len(winners) == 20

    # All tasks must be COMPLETED
    for tid in task_ids:
        task = await redis_queue.get_task(tid)
        assert task.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_concurrent_completion_visibility(redis_queue, clean_db):
    """After concurrent completion, tasks must be visible as COMPLETED in Redis."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-vis-{i:03d}" for i in range(10)]

    for tid in task_ids:
        await scheduler.submit_task("agent", "echo", {}, task_id=tid)

    async def claim_and_complete(worker_id):
        task = await redis_queue.dequeue(worker_id=worker_id, lease_seconds=60)
        if task:
            task.status = TaskStatus.COMPLETED
            await redis_queue.update_task(task)

    await asyncio.gather(*[
        claim_and_complete(f"vis-w-{i}")
        for i in range(10)
    ])

    for tid in task_ids:
        task = await redis_queue.get_task(tid)
        assert task.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_execution_count_under_concurrency(redis_queue, clean_db):
    """Concurrent dequeue + complete: execution count must be exactly 1 per task."""
    scheduler = TaskScheduler(redis_queue)
    task_id = "real-exec-count-001"

    await scheduler.submit_task("agent", "echo", {}, task_id=task_id)

    # Multiple workers try to dequeue concurrently
    results = await asyncio.gather(*[
        redis_queue.dequeue(worker_id=f"exec-w-{i}")
        for i in range(10)
    ])

    winners = [r for r in results if r is not None]
    assert len(winners) == 1

    # The winner completes the task
    winners[0].status = TaskStatus.COMPLETED
    winners[0].result = {"once": True}
    await redis_queue.update_task(winners[0])

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_idempotency_concurrent_dequeue(redis_queue, clean_db):
    """Concurrent dequeue of the same task must be idempotent — only one worker claims."""
    scheduler = TaskScheduler(redis_queue)
    task_id = "real-idempotent-001"

    await scheduler.submit_task("agent", "echo", {}, task_id=task_id)

    # 20 concurrent dequeues
    results = await asyncio.gather(*[
        redis_queue.dequeue(worker_id=f"idem-w-{i}")
        for i in range(20)
    ])

    winners = [r for r in results if r is not None]
    assert len(winners) == 1

    # Complete the task
    winners[0].status = TaskStatus.COMPLETED
    await redis_queue.update_task(winners[0])

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_queue_recovery_after_concurrent_drain(redis_queue, clean_db):
    """After all tasks are consumed concurrently, the queue must be empty."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-drain-{i:03d}" for i in range(30)]

    for tid in task_ids:
        await scheduler.submit_task("agent", "echo", {}, task_id=tid)

    async def drain(worker_id):
        count = 0
        while True:
            task = await redis_queue.dequeue(worker_id=worker_id, lease_seconds=60)
            if task is None:
                break
            count += 1
        return count

    results = await asyncio.gather(*[
        drain(f"drain-w-{i}")
        for i in range(5)
    ])

    total = sum(results)
    assert total == 30
    assert await redis_queue.redis.zcard(redis_queue.QUEUE_KEY) == 0


@pytest.mark.asyncio
async def test_concurrent_requeue_after_lease_expiry(redis_queue, clean_db):
    """Expired tasks must be reclaimed and requeued for concurrent workers."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"real-req-{i:03d}" for i in range(5)]

    for tid in task_ids:
        await scheduler.submit_task("agent", "echo", {}, task_id=tid)

    # Dequeue with short lease
    claims = await asyncio.gather(*[
        redis_queue.dequeue(worker_id=f"req-w-{i}", lease_seconds=0.5)
        for i in range(5)
    ])
    claimed = [c for c in claims if c is not None]
    assert len(claimed) == 5

    # Every claimed task is RUNNING with an active (short) lease.
    for tid in task_ids:
        t = await redis_queue.get_task(tid)
        assert t.status == TaskStatus.RUNNING
        assert t.lease_owner is not None

    # Wait for the leases to expire.
    await asyncio.sleep(0.7)

    # Reclaim must recover all five expired tasks exactly once and record a retry.
    reclaimed = await redis_queue.reclaim_orphaned_tasks()
    assert len(reclaimed) == 5
    for tid in task_ids:
        t = await redis_queue.get_task(tid)
        assert t.status == TaskStatus.PENDING
        assert t.lease_owner is None
        assert t.retry_count == 1

    # Tasks should be available for dequeue again (exactly one valid claim each).
    results = await asyncio.gather(*[
        redis_queue.dequeue(worker_id=f"req-w2-{i}", lease_seconds=60)
        for i in range(5)
    ])
    winners = [r for r in results if r is not None]
    assert len(winners) == 5
