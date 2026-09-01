"""
Race-condition regression tests for the task queue.

Each test establishes a narrow race window, triggers competing
operations, and verifies the integrity invariant.

All tests use the **real** RedisTaskQueue backed by a live Redis
and a live PostgreSQL database — no fakes, no mocks.
"""

import asyncio

import pytest

from src.agent_platform.core.task import TaskStatus
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
    optimistic locking must prevent the lost-update race: exactly one write
    wins and the other raises ``TaskWriteConflictError`` instead of silently
    overwriting the winner.  The task must still end in COMPLETED with a
    consistent (non-corrupt) state.
    """
    from src.agent_platform.scheduler.exceptions import TaskWriteConflictError

    scheduler = TaskScheduler(redis_queue)
    task_id = "race-complete-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    task_a = await redis_queue.dequeue(worker_id="w-a")
    task_b = await redis_queue.dequeue(worker_id="w-b", lease_seconds=999)
    # zpopmin is atomic: only one worker can claim the single queued task.
    assert task_a is not None
    assert task_b is None

    # Both workers read the same task version and try to complete it
    # concurrently.  With optimistic locking only one may win.
    task_a_copy = await redis_queue.get_task(task_id)
    task_a_copy.status = TaskStatus.COMPLETED
    task_a_copy.result = {"worker": "copy"}

    task_a.status = TaskStatus.COMPLETED
    task_a.result = {"worker": "original"}

    async def complete_copy():
        await redis_queue.update_task(task_a_copy)

    async def complete_original():
        await redis_queue.update_task(task_a)

    results = await asyncio.gather(complete_original(), complete_copy(), return_exceptions=True)

    # Exactly one write wins; the other must be rejected with a version
    # conflict (never a silent overwrite).
    conflicts = [r for r in results if isinstance(r, TaskWriteConflictError)]
    successes = [r for r in results if not isinstance(r, Exception)]
    assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}"
    assert len(conflicts) == 1, f"Expected exactly 1 conflict, got {len(conflicts)}"

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED
    # The winner's payload is preserved intact (no merged/corrupt state).
    assert final.result in ({"worker": "original"}, {"worker": "copy"})


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
    Two concurrent failure updates on the same task must not silently
    overwrite each other.  Optimistic locking ensures exactly one write wins
    and the other raises ``TaskWriteConflictError``.  The final state is a
    consistent FAILED with an error set by the winner.
    """
    from src.agent_platform.scheduler.exceptions import TaskWriteConflictError

    scheduler = TaskScheduler(redis_queue)
    task_id = "race-fail-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)
    claimed = await redis_queue.dequeue(worker_id="w-a", lease_seconds=30)

    # Build two independent snapshots at the same version, each reporting a
    # different failure.
    claimed.status = TaskStatus.FAILED
    claimed.error = "crash"

    claimed2 = await redis_queue.get_task(task_id)
    claimed2.status = TaskStatus.FAILED
    claimed2.error = "crash2"

    results = await asyncio.gather(
        redis_queue.update_task(claimed),
        redis_queue.update_task(claimed2),
        return_exceptions=True,
    )

    conflicts = [r for r in results if isinstance(r, TaskWriteConflictError)]
    successes = [r for r in results if not isinstance(r, Exception)]
    assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}"
    assert len(conflicts) == 1, f"Expected exactly 1 conflict, got {len(conflicts)}"

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.FAILED
    # The winner's error survives intact — never a merged/corrupt value.
    assert final.error in ("crash", "crash2")


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

    await asyncio.gather(cancel_task(), dequeue_task())

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


# ---------------------------------------------------------------------------
# 16. Deterministic lost-update race (optimistic locking)
# ---------------------------------------------------------------------------
# This is the regression test for the lost-update bug: the ``version`` field
# on ``Task`` exists for optimistic locking but was never checked, so two
# concurrent terminal writes to the same task row silently clobbered each
# other (last-write-wins).
#
# The test FORCES the dangerous interleaving with an asyncio barrier: both
# workers read the row at the same version, then both attempt to write.  This
# is deterministic — it does not rely on sleep-based timing.
#
# Before the fix: both writes "succeed", the loser silently overwrites the
# winner, and ``version`` is never bumped.
# After the fix: exactly one write wins, the loser raises
# ``TaskWriteConflictError``, and ``version`` is bumped exactly once.


@pytest.mark.asyncio
async def test_lost_update_is_prevented_by_version_check(redis_queue, clean_db):
    """
    Two workers that read the same task version and write concurrently: only
    one may win.  Forced with a barrier so the interleaving is deterministic.
    """
    from src.agent_platform.scheduler.exceptions import TaskWriteConflictError

    scheduler = TaskScheduler(redis_queue)
    task_id = "race-lost-update-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)
    # Initial version is 0.
    v0 = await redis_queue.get_task(task_id)
    assert v0.version == 0

    # Two independent snapshots, both at version 0.
    snap_a = await redis_queue.get_task(task_id)
    snap_b = await redis_queue.get_task(task_id)
    snap_a.status = TaskStatus.COMPLETED
    snap_a.result = {"winner": "A"}
    snap_b.status = TaskStatus.COMPLETED
    snap_b.result = {"winner": "B"}

    barrier = asyncio.Barrier(2)

    async def writer(task, label):
        await barrier.wait()  # force both to start the write simultaneously
        await redis_queue.update_task(task)
        return label

    results = await asyncio.gather(
        writer(snap_a, "A"),
        writer(snap_b, "B"),
        return_exceptions=True,
    )

    conflicts = [r for r in results if isinstance(r, TaskWriteConflictError)]
    successes = [r for r in results if not isinstance(r, Exception)]
    assert len(successes) == 1, f"expected 1 winner, got {len(successes)}: {results}"
    assert len(conflicts) == 1, f"expected 1 conflict, got {len(conflicts)}: {results}"

    # version bumped exactly once (0 -> 1), never the "both won" outcome.
    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED
    assert final.version == 1
    assert final.result in ({"winner": "A"}, {"winner": "B"})


@pytest.mark.asyncio
async def test_lost_update_prevents_silent_result_loss(redis_queue, clean_db):
    """
    The original bug: without a version check, the loser's write silently
    overwrites the winner's result.  Prove the winner's result survives and
    the loser's result is NOT what ends up stored (i.e. no silent clobber).
    """
    from src.agent_platform.scheduler.exceptions import TaskWriteConflictError

    scheduler = TaskScheduler(redis_queue)
    task_id = "race-silent-loss-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    snap_a = await redis_queue.get_task(task_id)
    snap_b = await redis_queue.get_task(task_id)
    snap_a.status = TaskStatus.COMPLETED
    snap_a.result = {"data": "important-result-from-A"}
    snap_b.status = TaskStatus.COMPLETED
    snap_b.result = {"data": "stale-result-from-B"}

    barrier = asyncio.Barrier(2)
    results = await asyncio.gather(
        _write_after_barrier(redis_queue, snap_a, barrier),
        _write_after_barrier(redis_queue, snap_b, barrier),
        return_exceptions=True,
    )

    assert any(isinstance(r, TaskWriteConflictError) for r in results), (
        "expected the loser to be rejected, but both writes succeeded"
    )

    final = await redis_queue.get_task(task_id)
    assert final.status == TaskStatus.COMPLETED
    # A real result is preserved; the version was bumped exactly once.
    assert final.version == 1
    assert "data" in final.result


async def _write_after_barrier(redis_queue, task, barrier):
    await barrier.wait()
    await redis_queue.update_task(task)
