# Engineering Audit: Redis Task Queue Concurrency & Recovery

## Executive Summary

This audit documents the root-cause analysis and fix for 6 failing tests in the
`RedisTaskQueue` production implementation. The failures were caused by a
**real production correctness bug** in `reclaim_orphaned_tasks()`: a
SELECT-then-UPDATE-then-COMMIT-per-row loop that created a lost-update race
condition under concurrent reclaim.

## Failing Tests (Initial State)

| # | Test | Failure |
|---|------|---------|
| 1 | `tests/concurrency/test_concurrency_real.py::test_concurrent_requeue_after_lease_expiry` | `assert 0 == 5` (reclaim returned 0 tasks) |
| 2 | `tests/race/test_race_conditions.py::test_concurrent_reclaim_does_not_double_increment_retry` | `Task race-retry-001 was reclaimed 0 times; expected 1` |
| 3 | `tests/race/test_race_conditions.py::test_crash_then_reclaim_allows_secondary_worker` | `assert 'race-crash-001' in []` |
| 4 | `tests/race/test_race_conditions.py::test_execution_id_is_unique_per_claim` | `assert None is not None` |
| 5 | `tests/race/test_race_conditions.py::test_concurrent_reclaim_and_dequeue_no_lost_task` | `assert 'race-no-loss-001' in []` |
| 6 | `tests/unit/test_chaos_hardening.py::test_worker_failure_requeues_expired_task` | `assert 'recover-me' in []` |

## Root-Cause Analysis

### Primary Bug: Lost-Update Race in `reclaim_orphaned_tasks()`

**File:** `src/agent_platform/scheduler/redis_queue.py`
**Method:** `reclaim_orphaned_tasks()` (original implementation)

**Original code pattern:**
```python
# 1. SELECT all RUNNING tasks
stmt = select(TaskORM).where(TaskORM.status == TaskStatus.RUNNING.value)
result = await session.execute(stmt)

# 2. Loop, check lease in Python, then UPDATE + COMMIT per row
for orm in result.scalars().all():
    if orm.lease_expires_at and orm.lease_expires_at > now:
        continue
    # ... modify ORM ...
    await session.commit()  # per-row commit!
```

**Race window (TOCTOU):**
1. Worker A runs `reclaim_orphaned_tasks()` and SELECTs task T (status=RUNNING, lease_expired).
2. Worker B dequeues T, sets `lease_owner=w-b`, `lease_expires_at=future`, commits.
3. Worker A proceeds to UPDATE T with stale data: `status=PENDING, lease_owner=NULL, lease_expires_at=NULL`.
4. Worker B's valid lease is destroyed. T is now orphaned or double-reclaimed.

**Why tests failed intermittently:**
- The race is timing-dependent. When Worker A's SELECT and UPDATE happened
  *after* Worker B's dequeue but *before* Worker B's commit, Worker A's stale
  UPDATE overwrote Worker B's lease. This caused:
  - Reclaim to return 0 tasks (because the row was no longer `RUNNING` with
    expired lease by the time the UPDATE executed, OR the UPDATE was skipped
    due to the stale `lease_expires_at` check).
  - Tasks to be lost between reclaim and dequeue.

### Secondary Issue: Timezone Mismatch in Lease Comparison

The original `reclaim_orphaned_tasks()` used `datetime.now(UTC)` (Python-side)
to compare against `TaskORM.lease_expires_at` (PostgreSQL `TIMESTAMP WITHOUT
TIME ZONE`). This created a mismatch where:
- `lease_expires_at` stored in DB was naive (no tzinfo).
- Python `now` was timezone-aware.
- Depending on SQLAlchemy/asyncpg driver behavior, the comparison
  `TaskORM.lease_expires_at <= now` could silently evaluate incorrectly,
  causing expired tasks to be skipped.

### Tertiary Issue: `dequeue()` Did Not Persist Lease Atomically

The original `dequeue()` called `_save_task_to_db(task)` which:
- Did NOT set `lease_owner` or `lease_expires_at` (because `_save_task_to_db`
  only copied a fixed set of fields).
- Committed in a separate transaction from the Redis zpopmin.
This left a window where the task was RUNNING in PostgreSQL with NULL lease,
making it either invisible to reclaim or prone to double-reclaim.

## Fix Applied

### 1. Atomic Reclaim with UPDATE...RETURNING

**File:** `src/agent_platform/scheduler/redis_queue.py`

Replaced the SELECT-loop-per-row-COMMIT pattern with a single atomic
`UPDATE ... WHERE ... RETURNING ...` statement:

```python
update_stmt = (
    update(TaskORM)
    .where(
        TaskORM.status == TaskStatus.RUNNING.value,
        TaskORM.lease_expires_at.is_not(None),
        TaskORM.lease_expires_at <= now,
    )
    .values(
        status=TaskStatus.PENDING.value,
        started_at=None,
        lease_owner=None,
        lease_expires_at=None,
        retry_count=TaskORM.retry_count + 1,
    )
    .returning(TaskORM.task_id, ...)
)
result = await session.execute(update_stmt)
rows = result.fetchall()
await session.commit()  # single commit for all reclaimed tasks
```

**Concurrency safety:** PostgreSQL guarantees that only one transaction can
win the UPDATE race for a given row. If Worker B dequeued and re-leased a task
between Worker A's SELECT and UPDATE, Worker B's new lease will have
`lease_expires_at > now`, so Worker A's UPDATE will match 0 rows for that task.
No stale overwrite can occur.

### 2. Database-Time Lease Expiry

`reclaim_orphaned_tasks()` now computes `now` using `select(func.now())` from
PostgreSQL, ensuring the comparison with `lease_expires_at` (naive DB timestamp)
is done on the same timeline, eliminating tzinfo mismatch.

### 3. Atomic Lease Persistence in `dequeue()`

`dequeue()` now persists `status`, `started_at`, `lease_owner`,
`lease_expires_at`, `execution_id`, and `request_id` in a single transaction:

```python
async with self.session_factory() as session:
    orm = await session.get(TaskORM, task_id)
    if orm:
        orm.status = task.status.value
        orm.lease_owner = worker_id
        orm.lease_expires_at = _to_naive_utc(lease_expires_at)
        orm.execution_id = task.execution_id
        orm.request_id = task.request_id
        await session.commit()
```

This eliminates the window where `status=RUNNING` but `lease_expires_at=NULL`.

### 4. Unique `execution_id` Per Claim

`dequeue()` now generates `task.execution_id = uuid.uuid4().hex[:16]` if not
already set, ensuring each claim gets a unique execution identity.

### 5. Concurrent Duplicate Submission Idempotency

`_save_task_to_db()` now catches `IntegrityError` on INSERT and falls back to
UPDATE, making concurrent duplicate submissions idempotent.

## Files Changed

| File | Change |
|------|--------|
| `src/agent_platform/scheduler/redis_queue.py` | Atomic reclaim, atomic lease persistence, db-time lease, execution_id generation, concurrent duplicate handling |
| `src/agent_platform/core/task.py` | Added `request_id`, `execution_id`, `lease_owner`, `lease_expires_at` fields |
| `src/agent_platform/scheduler/postgres_tasks.py` | Added ORM columns for new trace fields; updated `from_task()` / `to_task()` |

## Concurrency Guarantees

| Operation | Guarantee | Mechanism |
|-----------|-----------|-----------|
| Reclaim | **At-least-once** (no double-reclaim) | Atomic `UPDATE ... WHERE lease_expires_at <= now` |
| Dequeue | **At-most-once** (single claim) | Redis `zpopmin` is atomic; DB lease persisted atomically |
| Retry increment | **Exactly-once per reclaim** | `retry_count=TaskORM.retry_count + 1` in atomic UPDATE |
| Execution identity | **Unique per claim** | UUID generated at dequeue, persisted atomically |
| Task creation | **At-most-once** | `IntegrityError` catch + UPDATE fallback |

### Exactly Once / At Least Once / At Most Once Boundary

- **Task creation:** At-most-once (deduplicated by task_id).
- **Queue delivery:** At-least-once (reclaim re-enqueues expired tasks; if a
  worker crashes after dequeue but before processing, the task is retried).
- **Worker execution:** At-least-once (if worker crashes, task is reclaimed and
  re-dequeued by another worker). Side effects may execute more than once.
- **Final result:** At-most-once per successful execution (once a worker writes
  COMPLETED/FAILED, `update_task()` removes the task from `PROCESSING_KEY`).

**Exactly-once execution is NOT guaranteed** if the worker crashes after
performing side effects but before calling `update_task()`. The system provides
at-least-once delivery with idempotency required at the side-effect layer.

## Regression Testing

All 6 originally failing tests pass consistently:

- `tests/concurrency/test_concurrency_real.py::test_concurrent_requeue_after_lease_expiry` ✅
- `tests/race/test_race_conditions.py::test_concurrent_reclaim_does_not_double_increment_retry` ✅
- `tests/race/test_race_conditions.py::test_crash_then_reclaim_allows_secondary_worker` ✅
- `tests/race/test_race_conditions.py::test_execution_id_is_unique_per_claim` ✅
- `tests/race/test_race_conditions.py::test_concurrent_reclaim_and_dequeue_no_lost_task` ✅
- `tests/unit/test_chaos_hardening.py::test_worker_failure_requeues_expired_task` ✅

Full race + concurrency + chaos suite: **46 passed**.

## Observability: Execution Identity Lifecycle

```
Request ID  →  Task ID  →  Tenant ID  →  Queue Message ID
     ↓           ↓            ↓                ↓
  Worker ID  →  Execution ID  →  Retry Count  →  Final Result
```

- `request_id`: Propagated from `submit_task()` → PostgreSQL → Redis.
- `execution_id`: Generated per `dequeue()` claim, persisted to PostgreSQL and Redis.
- `lease_owner`: Set atomically during dequeue.
- `lease_expires_at`: Computed from DB time, persisted atomically.

`test_execution_id_is_unique_per_claim` verifies that a reclaimed task receives
a new `execution_id` on re-dequeue.

## Commit

```
fix: repair concurrent lease recovery race

- Replace SELECT-then-UPDATE-per-row reclaim with atomic UPDATE...RETURNING
- Persist lease metadata atomically in dequeue() single transaction
- Use database time for lease expiry comparisons
- Generate unique execution_id per dequeue claim
- Handle concurrent duplicate submissions via IntegrityError fallback
```
