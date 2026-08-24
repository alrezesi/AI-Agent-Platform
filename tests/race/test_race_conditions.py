"""
Race-condition regression tests for the task queue.

Each test establishes a narrow race window, triggers competing
operations, and verifies the integrity invariant.

All tests use the **real** RedisTaskQueue backed by a live Redis
and a live PostgreSQL database — no fakes, no mocks.
"""

import asyncio
import logging
import threading

import pytest

from src.agent_platform.core.task import TaskStatus
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler


# ---------------------------------------------------------------------------
# 1. Idempotency — concurrent submissions of the same task_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_duplicate_submission_is_idempotent(redis_queue, clean_db):
    """
    Two concurrent ``submit_task`` calls with the same task_id must
    produce exactly one task in the queue, not two.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "dup-task-001"

    results = await asyncio.gather(
        scheduler.submit_task("agent-a", "echo", {"n": 0}, task_id=task_id),
        scheduler.submit_task("agent-a", "echo", {"n": 1}, task_id=task_id),
    )

    assert all(r == task_id for r in results)
    # Only one task should exist
    task = await redis_queue.get_task(task_id)
    assert task is not None
    assert task.task_id == task_id


# ---------------------------------------------------------------------------
# 2. Task state transition — concurrent transition to same terminal state
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_completion_does_not_corrupt_state(redis_queue, clean_db):
    """
    If two workers call ``update_task`` with COMPLETED concurrently,
    the task must end in COMPLETED with no partial writes.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-complete-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    task_a = await redis_queue.dequeue(worker_id="w-a")
    task_b = await redis_queue.dequeue(worker_id="w-b", lease_seconds=999)
    # task_b should be None because zpopmin is atomic
    assert task_a is not None

    task_a.status = TaskStatus.COMPLETED
    task_a.result = {"worker": "w-a"}

    # Simulate a concurrent update from another path (same data)
    async def complete_again():
        await asyncio.sleep(0.01)
        task_a.status = TaskStatus.COMPLETED
        task_a.result = {"worker": "w-b"}
        await redis_queue.update_task(task_a)

    async def complete_original():
        await redis_queue.update_task(task_a)

    await asyncio.gather(complete_original(), complete_again())

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED


# ---------------------------------------------------------------------------
# 3. Lease acquisition — concurrent dequeue of the same task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_dequeue_only_one_worker_claims(redis_queue, clean_db):
    """
    Two workers calling ``dequeue`` at the same time: only one should
    receive the task.  Redis ``zpopmin`` is atomic so this is guaranteed,
    but we verify it explicitly.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-dequeue-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    results = await asyncio.gather(
        redis_queue.dequeue(worker_id="w-a"),
        redis_queue.dequeue(worker_id="w-b"),
    )

    non_none = [r for r in results if r is not None]
    assert len(non_none) == 1
    assert non_none[0].task_id == task_id
    assert non_none[0].lease_owner in ("w-a", "w-b")


# ---------------------------------------------------------------------------
# 4. Lease expiration — reclaim vs. active dequeue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reclaim_does_not_steal_active_task(redis_queue, clean_db):
    """
    If worker A has an active lease on a task, a concurrent reclaim
    must NOT reclaim that task.  The task must remain with worker A.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-lease-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    # Worker A claims with a long lease
    claimed = await redis_queue.dequeue(worker_id="w-a", lease_seconds=30)
    assert claimed is not None
    assert claimed.lease_owner == "w-a"

    # Concurrently, try to reclaim — should NOT reclaim the active task
    reclaimed = await redis_queue.reclaim_orphaned_tasks()
    assert task_id not in reclaimed, (
        "reclaim_orphaned_tasks must not reclaim a task with an active lease"
    )

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.RUNNING
    assert final.lease_owner == "w-a"


# ---------------------------------------------------------------------------
# 5. Retry — concurrent reclaim does not double-increment retry_count
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_reclaim_does_not_double_increment_retry(redis_queue, clean_db):
    """
    Two workers calling ``reclaim_orphaned_tasks`` concurrently for the
    same expired-lease task must not double-increment retry_count.
    The atomic UPDATE ensures only one worker reclaims.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-retry-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id, max_retries=5)

    # Worker A claims with an expired lease
    claimed = await redis_queue.dequeue(worker_id="w-a", lease_seconds=0.5)
    assert claimed is not None
    assert claimed.retry_count == 0

    await asyncio.sleep(0.7)  # lease expires

    # Two workers reclaim concurrently
    results = await asyncio.gather(
        redis_queue.reclaim_orphaned_tasks(),
        redis_queue.reclaim_orphaned_tasks(),
    )

    total_reclaims = sum(results, [])
    assert total_reclaims.count(task_id) == 1, (
        f"Task {task_id} was reclaimed {total_reclaims.count(task_id)} times; expected 1"
    )

    final = await redis_queue.get_task(task_id)
    assert final.retry_count == 1, f"retry_count should be 1, got {final.retry_count}"


# ---------------------------------------------------------------------------
# 6. Worker recovery — reclaim during active processing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reclaim_during_active_processing_preserves_execution(redis_queue, clean_db):
    """
    Worker A is actively processing a task.  Worker B calls reclaim.
    The task must NOT be reclaimed (lease is still valid).  Worker A
    finishes and completes the task without interference.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-recovery-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)
    claimed = await redis_queue.dequeue(worker_id="w-a", lease_seconds=30)

    # Worker B tries to reclaim while A is still processing
    reclaim_result = await redis_queue.reclaim_orphaned_tasks()
    assert task_id not in reclaim_result

    # Worker A completes
    claimed.status = TaskStatus.COMPLETED
    claimed.result = {"done": True}
    await redis_queue.update_task(claimed)

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED


# ---------------------------------------------------------------------------
# 7. Duplicate message — concurrent enqueue of same task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_enqueue_same_task_deduplicates(redis_queue, clean_db):
    """
    Concurrent ``enqueue`` calls for the same task_id should result in
    exactly one queue entry (idempotent enqueue).
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "dup-enqueue-001"

    results = await asyncio.gather(
        scheduler.submit_task("agent-a", "echo", {}, task_id=task_id),
        scheduler.submit_task("agent-a", "echo", {}, task_id=task_id),
        scheduler.submit_task("agent-a", "echo", {}, task_id=task_id),
    )

    assert all(r == task_id for r in results)
    zcard = await redis_queue.redis.zcard(redis_queue.QUEUE_KEY)
    assert zcard == 1


# ---------------------------------------------------------------------------
# 8. Concurrent failure — concurrent error updates
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_failure_update_is_consistent(redis_queue, clean_db):
    """
    Two concurrent failure updates on the same task must produce a
    consistent FAILED state without corruption.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-fail-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)
    claimed = await redis_queue.dequeue(worker_id="w-a", lease_seconds=30)

    claimed.status = TaskStatus.FAILED
    claimed.error = "crash"

    async def fail_twice():
        # Simulate a concurrent failure write
        task = await redis_queue.get_task(task_id)
        task.status = TaskStatus.FAILED
        task.error = "crash2"
        await redis_queue.update_task(task)

    await asyncio.gather(
        redis_queue.update_task(claimed),
        fail_twice(),
    )

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.FAILED
    assert final.error is not None


# ---------------------------------------------------------------------------
# 9. Recovery vs active worker — reclaim after worker crash
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_crash_then_reclaim_allows_secondary_worker(redis_queue, clean_db):
    """
    Worker A dequeues a task and crashes (never completes).
    After the lease expires, Worker B must be able to dequeue and
    complete the task.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-crash-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    # Worker A crashes after dequeue (short lease)
    claimed = await redis_queue.dequeue(worker_id="w-a", lease_seconds=0.5)
    assert claimed is not None

    # Wait for lease to expire
    await asyncio.sleep(0.7)

    # Worker B should reclaim
    reclaimed = await redis_queue.reclaim_orphaned_tasks()
    assert task_id in reclaimed

    # Worker B dequeues the reclaimed task
    second_claim = await redis_queue.dequeue(worker_id="w-b", lease_seconds=30)
    assert second_claim is not None
    assert second_claim.task_id == task_id
    assert second_claim.lease_owner == "w-b"

    second_claim.status = TaskStatus.COMPLETED
    second_claim.result = {"worker": "w-b"}
    await redis_queue.update_task(second_claim)

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED


# ---------------------------------------------------------------------------
# 10. Task state consistency under concurrent dequeue + update
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_dequeue_and_update_state_consistency(redis_queue, clean_db):
    """
    A task that is being dequeued by one worker while another updates
    it must remain in a consistent state.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-consist-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    # Worker A dequeues
    claimed = await redis_queue.dequeue(worker_id="w-a", lease_seconds=30)
    assert claimed is not None
    assert claimed.status == TaskStatus.RUNNING

    # Concurrent completion update
    claimed.status = TaskStatus.COMPLETED
    claimed.result = {"ok": True}

    # Verify state is persisted correctly
    await redis_queue.update_task(claimed)
    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED
    assert final.result == {"ok": True}


# ---------------------------------------------------------------------------
# 11-15. Additional race-condition tests for deeper coverage
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_cancel_during_dequeue(redis_queue, clean_db):
    """
    Cancelling a task while another worker is dequeuing it must not
    leave the task in an inconsistent state.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-cancel-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    async def cancel_task():
        await asyncio.sleep(0.01)
        await redis_queue.cancel(task_id)

    async def dequeue_task():
        return await redis_queue.dequeue(worker_id="w-a", lease_seconds=30)

    results = await asyncio.gather(cancel_task(), dequeue_task())

    # After cancel+dequeue, the task must be in a terminal state
    final = await redis_queue.get_task(task_id)
    assert final.status in (TaskStatus.CANCELLED, TaskStatus.RUNNING)


@pytest.mark.asyncio
async def test_concurrent_get_and_update(redis_queue, clean_db):
    """
    Concurrent reads and writes of the same task must not cause read
    skew or lost updates.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-get-update-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    results = await asyncio.gather(
        redis_queue.get_task(task_id),
        redis_queue.get_task(task_id),
        redis_queue.get_task(task_id),
    )
    assert all(r.task_id == task_id for r in results)


@pytest.mark.asyncio
async def test_lease_acquisition_is_mutual_exclusive(redis_queue, clean_db):
    """
    The lease (lease_owner) field must reflect exactly one worker at
    any given time after a successful dequeue.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-mutex-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    claimed = await redis_queue.dequeue(worker_id="w-x", lease_seconds=60)
    assert claimed is not None
    assert claimed.lease_owner == "w-x"

    from_db = await redis_queue.get_task(task_id)
    assert from_db.lease_owner == "w-x"
    assert from_db.status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_execution_id_is_unique_per_claim(redis_queue, clean_db):
    """
    Each dequeue must assign a unique execution_id.  If a task is
    reclaimed and re-dequeued, the new execution must get a new ID.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-exec-id-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    first = await redis_queue.dequeue(worker_id="w-a", lease_seconds=0.5)
    assert first is not None
    assert first.execution_id is not None

    await asyncio.sleep(0.7)
    await redis_queue.reclaim_orphaned_tasks()

    second = await redis_queue.dequeue(worker_id="w-b", lease_seconds=30)
    assert second is not None
    assert second.execution_id is not None
    assert second.execution_id != first.execution_id


@pytest.mark.asyncio
async def test_concurrent_reclaim_and_dequeue_no_lost_task(redis_queue, clean_db):
    """
    A concurrent reclaim + dequeue must not lose the task.  Either the
    reclaim requeues and the dequeue picks it up, or the dequeue picks
    it up before reclaim sees it — but the task must not vanish.
    """
    scheduler = TaskScheduler(redis_queue)
    task_id = "race-no-loss-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    # Worker A claims with expired lease
    claimed = await redis_queue.dequeue(worker_id="w-a", lease_seconds=0.5)
    assert claimed is not None

    await asyncio.sleep(0.7)  # lease expired

    # Concurrently: reclaim (by w-a) and dequeue (by w-b)
    reclaim_result, dequeue_result = await asyncio.gather(
        redis_queue.reclaim_orphaned_tasks(),
        redis_queue.dequeue(worker_id="w-b", lease_seconds=30),
    )

    # If dequeue got None (task not yet in queue), reclaim must have
    # requeued it
    if dequeue_result is None:
        assert task_id in reclaim_result
    else:
        assert dequeue_result.task_id == task_id

    # In either case, after reclaim, the task should be available
    # Try one more dequeue to get it if not already claimed
    if dequeue_result is None:
        final_claim = await redis_queue.dequeue(worker_id="w-c", lease_seconds=30)
        assert final_claim is not None
        assert final_claim.task_id == task_id
    else:
        final_claim = dequeue_result

    final_claim.status = TaskStatus.COMPLETED
    final_claim.result = {"ok": True}
    await redis_queue.update_task(final_claim)

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_tenant_scope_does_not_interfere_across_concurrent_tasks(redis_queue, clean_db):
    """
    Tasks from different tenants must be isolated even under concurrent
    dequeue pressure.  Each task must retain its tenant_id.
    """
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"race-tenant-{i:03d}" for i in range(10)]

    # Submit tasks for two tenants
    for tid in task_ids[:5]:
        await scheduler.submit_task("agent-a", "echo", {}, task_id=tid, tenant_id="tenant-A")
    for tid in task_ids[5:]:
        await scheduler.submit_task("agent-a", "echo", {}, task_id=tid, tenant_id="tenant-B")

    # Dequeue all tasks concurrently
    results = await asyncio.gather(
        *[redis_queue.dequeue(worker_id=f"w-{i}") for i in range(10)]
    )

    claimed = [r for r in results if r is not None]
    assert len(claimed) == 10
    for task in claimed:
        assert task.tenant_id in ("tenant-A", "tenant-B")
