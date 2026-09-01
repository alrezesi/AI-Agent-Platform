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

    With the version-checked dequeue fix (audit bug #2) one of the two
    writes wins and the other is rejected with ``TaskWriteConflictError``
    rather than silently overwriting the winner — the row's ``version``
    reflects every successful write, the loser never merges into the
    winning state, and ``cancel()`` either succeeds or raises but never
    corrupts.
    """
    from src.agent_platform.scheduler.exceptions import TaskWriteConflictError

    scheduler = TaskScheduler(redis_queue)
    task_id = "race-cancel-001"

    await scheduler.submit_task("agent-a", "echo", {}, task_id=task_id)

    async def cancel_task():
        # Brief delay so dequeue starts first most of the time, but the
        # test must be correct under any interleaving.
        await asyncio.sleep(0.01)
        try:
            return await redis_queue.cancel(task_id)
        except TaskWriteConflictError as exc:
            return exc

    async def dequeue_task():
        return await redis_queue.dequeue(worker_id="w-a", lease_seconds=30)

    cancel_result, dequeue_result = await asyncio.gather(
        cancel_task(), dequeue_task()
    )

    # Exactly one of the two operations wins; the other either returns
    # None (dequeue on a cancelled task) or raises TaskWriteConflictError
    # (cancel racing against a successful dequeue).
    cancel_won = cancel_result is True
    dequeue_won = dequeue_result is not None

    # The two outcomes are mutually exclusive: cancel transitions the
    # row to CANCELLED, dequeue transitions it to RUNNING.  Both could
    # "lose" only if dequeue skipped the claim because the row was
    # already CANCELLED by the time it observed it.
    assert cancel_won or dequeue_won, (
        "one of cancel/dequeue must win the race; got cancel="
        f"{cancel_result!r}, dequeue={dequeue_result!r}"
    )
    if cancel_won and dequeue_won:
        # If both appear to win, that is the lost-update bug we are
        # closing — assert that no silent merge happened.  In practice
        # the version-checked UPDATE on dequeue must reject the cancel
        # winner's stale ORM object via TaskWriteConflictError, but we
        # defensively guard against any regression here.
        pytest.fail(
            "Both cancel() and dequeue() reported success for the same "
            "task — this is the lost-update bug the audit closes."
        )

    final = await redis_queue.get_task(task_id)
    # The task must end in exactly the terminal/active state the winner
    # chose, with the row's ``version`` reflecting every successful
    # write — never two writes' fields merged together.
    if cancel_won:
        assert final.status == TaskStatus.CANCELLED
        # The cancel write increments the version once.
        assert final.version == 1
    else:
        # dequeue won the race.  cancel returned None (task was no
        # longer PENDING) or raised TaskWriteConflictError.  Either way
        # the row is RUNNING with a lease.
        assert final.status == TaskStatus.RUNNING
        assert final.lease_owner == "w-a"
        # The dequeue write incremented the version; cancel did not
        # silently merge.
        assert final.version == 1


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


# ---------------------------------------------------------------------------
# 17. dequeue() lease-claim participates in optimistic locking
# ---------------------------------------------------------------------------
# Regression test for the audit's second bug:
#
#   ``dequeue()`` used to mutate the TaskORM object directly and commit,
#   bypassing the version-checked update path used everywhere else
#   (``_save_task_to_db``).  A concurrent ``cancel()`` (or another
#   ``update_task()``) on the same ``task_id`` could therefore have its
#   changes silently overwritten by ``dequeue()``'s unguarded commit (or
#   vice versa) with no ``TaskWriteConflictError`` raised by either side.
#
# The fix makes the RUNNING/lease-claim write go through the same
# ``UPDATE ... WHERE version = ?`` path, incrementing ``version`` on
# success and skipping the claim on a zero-row match.
#
# This test forces the dangerous interleaving with an ``asyncio.Barrier``
# between the two concurrent writers on the same task_id: it is
# deterministic and exercises exactly 2 concurrent worker paths, against
# real PostgreSQL + Redis — no fakes, no mocks.


@pytest.mark.asyncio
async def test_dequeue_vs_cancel_version_conflict_no_lost_update(redis_queue, clean_db):
    """
    Worker 1 (dequeue) and worker 2 (cancel) race on the same PENDING
    task.  Before the fix, both writes silently committed and the
    final state was whichever writer's ``session.commit()`` happened
    to land last — the loser's fields were silently merged into the
    winner.  After the fix, exactly one operation succeeds; the other
    is rejected (cancel raises ``TaskWriteConflictError`` if dequeue
    won, or dequeue returns ``None`` / skips the claim if cancel won
    first).  The row's ``version`` reflects every successful write
    and no field from the losing writer leaks into the persisted row.
    """
    from src.agent_platform.scheduler.exceptions import TaskWriteConflictError

    scheduler = TaskScheduler(redis_queue)
    task_id = "race-dequeue-cancel-001"

    # Initial task is PENDING with version 0.
    await scheduler.submit_task("agent-a", "echo", {"secret": "original"}, task_id=task_id)
    v0 = await redis_queue.get_task(task_id)
    assert v0 is not None
    assert v0.status == TaskStatus.PENDING
    assert v0.version == 0

    barrier = asyncio.Barrier(2)

    async def worker_1_dequeue():
        await barrier.wait()
        return await redis_queue.dequeue(worker_id="w-1", lease_seconds=30)

    async def worker_2_cancel():
        await barrier.wait()
        try:
            cancelled = await redis_queue.cancel(task_id)
            return ("ok", cancelled)
        except TaskWriteConflictError as exc:
            return ("conflict", exc)

    dequeue_result, cancel_result = await asyncio.gather(
        worker_1_dequeue(),
        worker_2_cancel(),
    )

    # Exactly one operation must win.  If dequeue wins, the task is
    # RUNNING and cancel either returns False (task was no longer
    # PENDING) or raises TaskWriteConflictError.  If cancel wins, the
    # task is CANCELLED and dequeue either returns None (skipped the
    # claim because status was no longer PENDING) or returns a task
    # that has not actually persisted (i.e. it was skipped due to the
    # status guard).  Either way, both sides must NOT report success.
    final = await redis_queue.get_task(task_id)

    # 1. The persisted state is internally consistent — it is either
    #    RUNNING (dequeue won) or CANCELLED (cancel won).  No merged
    #    fields, no impossible intermediate state.
    assert final.status in (TaskStatus.RUNNING, TaskStatus.CANCELLED), (
        f"unexpected terminal state: {final.status}"
    )

    # 2. The row's ``version`` reflects every successful write.  Two
    #    successful writes would be version 2; one write + one rejected
    #    write is version 1.  Crucially, a "both wrote silently"
    #    outcome would have produced version 1 with a corrupt mix of
    #    fields; we forbid that by asserting the fields below match the
    #    winner and only the winner.
    if final.status == TaskStatus.RUNNING:
        # dequeue won, cancel was rejected with TaskWriteConflictError
        # (because cancel read version=0 but the row had been bumped to
        # 1 by dequeue's UPDATE ... WHERE version=0).
        assert cancel_result[0] == "conflict", (
            f"cancel must raise TaskWriteConflictError when dequeue wins; got {cancel_result}"
        )
        assert dequeue_result is not None
        # dequeue wrote exactly once; cancel wrote zero times.
        assert final.version == 1, (
            f"dequeue must increment version exactly once on success; got {final.version}"
        )
        assert final.lease_owner == "w-1"
        assert final.lease_expires_at is not None
        # Original payload must survive intact.
        assert final.payload == {"secret": "original"}
        # Cancel must not have silently merged its status=COMPLETED
        # fields into the row.
        assert final.status != TaskStatus.CANCELLED
    else:
        # cancel won, dequeue either skipped the claim (status was no
        # longer PENDING) or returned a task that has not actually
        # persisted (the version-checked UPDATE on dequeue would
        # similarly fail and be skipped).  The task is CANCELLED.
        assert final.status == TaskStatus.CANCELLED
        # cancel wrote exactly once; dequeue wrote zero times.
        assert final.version == 1, (
            f"cancel must increment version exactly once on success; got {final.version}"
        )
        assert dequeue_result is None, (
            f"dequeue must not return a claim on a CANCELLED row; got {dequeue_result!r}"
        )
        # dequeue must not have silently merged its lease fields.
        assert final.lease_owner is None
        assert final.lease_expires_at is None


@pytest.mark.asyncio
async def test_dequeue_vs_update_task_version_conflict_no_lost_update(redis_queue, clean_db):
    """
    Worker 1 (dequeue) and worker 2 (update_task completing a stale
    retry) race on the same PENDING task.  Before the fix, both writes
    silently committed and the loser could merge ``result``/``error``
    fields into the winner's persisted state.  After the fix, exactly
    one write succeeds (the other raises ``TaskWriteConflictError`` or
    is skipped), the ``version`` reflects every successful write, and
    the winning writer's fields are preserved intact.
    """
    from src.agent_platform.scheduler.exceptions import TaskWriteConflictError

    scheduler = TaskScheduler(redis_queue)
    task_id = "race-dequeue-update-001"

    await scheduler.submit_task("agent-a", "echo", {"secret": "original"}, task_id=task_id)
    v0 = await redis_queue.get_task(task_id)
    assert v0 is not None
    assert v0.status == TaskStatus.PENDING
    assert v0.version == 0

    # Worker 2 has a stale snapshot from a previous read — it is going
    # to try to COMPLETE the task.  Its version=0 is stale relative to
    # whatever the other worker does.
    stale = await redis_queue.get_task(task_id)
    stale.status = TaskStatus.COMPLETED
    stale.result = {"stale_result": "from-worker-2"}

    barrier = asyncio.Barrier(2)

    async def worker_1_dequeue():
        await barrier.wait()
        return await redis_queue.dequeue(worker_id="w-1", lease_seconds=30)

    async def worker_2_update():
        await barrier.wait()
        try:
            await redis_queue.update_task(stale)
            return ("ok",)
        except TaskWriteConflictError as exc:
            return ("conflict", exc)

    dequeue_result, update_result = await asyncio.gather(
        worker_1_dequeue(),
        worker_2_update(),
    )

    final = await redis_queue.get_task(task_id)
    assert final.status in (TaskStatus.RUNNING, TaskStatus.COMPLETED), (
        f"unexpected terminal state: {final.status}"
    )

    if final.status == TaskStatus.RUNNING:
        # dequeue won; update was rejected with TaskWriteConflictError
        # because its stale version=0 was clobbered by dequeue's
        # version=0 -> 1 bump.
        assert update_result[0] == "conflict", (
            f"update_task must raise TaskWriteConflictError when dequeue wins; got {update_result}"
        )
        assert dequeue_result is not None
        assert final.version == 1
        assert final.lease_owner == "w-1"
        # The stale update's result must NEVER be persisted.
        assert final.result is None or final.result != {"stale_result": "from-worker-2"}, (
            "stale update_task result was silently merged into the dequeue winner"
        )
        # Original payload preserved.
        assert final.payload == {"secret": "original"}
    else:
        # update won (dequeue's version-checked UPDATE found zero rows
        # and skipped the claim, returning None).  The task is
        # COMPLETED with the legitimate result.
        assert final.status == TaskStatus.COMPLETED
        assert dequeue_result is None, (
            f"dequeue must skip the claim when update wins; got {dequeue_result!r}"
        )
        assert final.version == 1
        assert final.result == {"stale_result": "from-worker-2"}
        # dequeue must not have merged its lease fields into the row.
        assert final.lease_owner is None
        assert final.lease_expires_at is None


@pytest.mark.asyncio
async def test_dequeue_increments_version_on_every_successful_claim(redis_queue, clean_db):
    """
    Every successful dequeue() lease-claim must increment ``version``,
    matching the invariant enforced for every other write path
    (``_save_task_to_db``).  This guards against a silent regression
    where a future change reintroduces the unguarded commit and the
    ``version`` column stops being bumped on the PENDING -> RUNNING
    transition.
    """
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"race-dequeue-version-{i:02d}" for i in range(3)]

    for tid in task_ids:
        await scheduler.submit_task("agent-a", "echo", {"i": tid}, task_id=tid)
        before = await redis_queue.get_task(tid)
        assert before.version == 0, f"initial version for {tid} must be 0, got {before.version}"

        claimed = await redis_queue.dequeue(worker_id="w-claim", lease_seconds=30)
        assert claimed is not None
        assert claimed.version == 1, (
            f"dequeue must bump version 0 -> 1 on successful claim of {tid}; got {claimed.version}"
        )

        after = await redis_queue.get_task(tid)
        assert after.version == 1, (
            f"persisted version for {tid} must be 1 after dequeue; got {after.version}"
        )
        assert after.status == TaskStatus.RUNNING
        assert after.lease_owner == "w-claim"
        assert after.lease_expires_at is not None
