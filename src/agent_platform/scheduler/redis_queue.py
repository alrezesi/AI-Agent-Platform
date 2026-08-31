# Redis-backed task queue with PostgreSQL as the source of truth.

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select, update

try:
    from redis.asyncio import Redis
except ImportError:  # pragma: no cover - optional dependency
    Redis = Any

if TYPE_CHECKING:
    from redis.asyncio import Redis as RedisClient
else:
    RedisClient = Any

from src.agent_platform.core.task import Task, TaskPriority, TaskStatus
from src.agent_platform.scheduler.base import BaseTaskQueue
from src.agent_platform.scheduler.models import TaskFilterOptions, TaskStats
from src.agent_platform.scheduler.postgres_tasks import TaskORM, _normalize_json, _to_naive_utc

logger = logging.getLogger(__name__)


class RedisTaskQueue(BaseTaskQueue):
    """
    Redis-backed queue with PostgreSQL persistence.
    - PostgreSQL stores authoritative task state.
    - Redis stores the fast queue and processing lease markers.
    """

    QUEUE_KEY = "tasks:queue"
    PROCESSING_KEY = "tasks:processing"
    TASK_PREFIX = "tasks:data:"
    META_PREFIX = "tasks:meta:"

    def __init__(self, redis_client: RedisClient, ttl_seconds: int = 86400, session_factory: Any | None = None):
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds
        self.session_factory = session_factory

    def _priority_score(self, task: Task) -> float:
        return task.priority.value + (task.created_at.timestamp() % 1)

    def _task_key(self, task_id: str) -> str:
        return f"{self.TASK_PREFIX}{task_id}"

    def _meta_key(self, task_id: str) -> str:
        return f"{self.META_PREFIX}{task_id}"

    async def _task_exists(self, task_id: str) -> bool:
        try:
            return bool(await self.redis.exists(self._task_key(task_id)))
        except Exception:
            return False

    async def _save_task_to_db(self, task: Task) -> None:
        if not self.session_factory:
            return
        from sqlalchemy.exc import IntegrityError

        async with self.session_factory() as session:
            existing = await session.get(TaskORM, task.task_id)
            if existing:
                existing.agent_id = task.agent_id
                existing.task_type = task.type
                existing.payload = task.payload
                existing.priority = int(task.priority.value)
                existing.status = task.status.value
                existing.created_at = _to_naive_utc(task.created_at)
                existing.started_at = _to_naive_utc(task.started_at)
                existing.completed_at = _to_naive_utc(task.completed_at)
                existing.result = _normalize_json(task.result)
                existing.error = task.error
                existing.retry_count = task.retry_count
                existing.max_retries = task.max_retries
                existing.timeout_seconds = task.timeout_seconds
                existing.tenant_id = task.tenant_id
                existing.lease_owner = getattr(task, "lease_owner", None)
                existing.lease_expires_at = _to_naive_utc(getattr(task, "lease_expires_at", None))
                existing.request_id = getattr(task, "request_id", None)
                existing.message_id = getattr(task, "message_id", None)
                existing.execution_id = getattr(task, "execution_id", None)
                existing.error_category = getattr(task, "error_category", None)
                existing.retry_history = _normalize_json(getattr(task, "retry_history", None))
                await session.commit()
            else:
                try:
                    session.add(TaskORM.from_task(task))
                    await session.commit()
                except IntegrityError:
                    # Another concurrent submission inserted the same task_id.
                    # Fall back to UPDATE so concurrent duplicates are
                    # idempotent rather than raising a 500.
                    await session.rollback()
                    existing = await session.get(TaskORM, task.task_id)
                    if existing:
                        existing.agent_id = task.agent_id
                        existing.task_type = task.type
                        existing.payload = task.payload
                        existing.priority = int(task.priority.value)
                        existing.status = task.status.value
                        existing.created_at = _to_naive_utc(task.created_at)
                        existing.started_at = _to_naive_utc(task.started_at)
                        existing.completed_at = _to_naive_utc(task.completed_at)
                        existing.result = _normalize_json(task.result)
                        existing.error = task.error
                        existing.retry_count = task.retry_count
                        existing.max_retries = task.max_retries
                        existing.timeout_seconds = task.timeout_seconds
                        existing.tenant_id = task.tenant_id
                        existing.lease_owner = getattr(task, "lease_owner", None)
                        existing.lease_expires_at = _to_naive_utc(getattr(task, "lease_expires_at", None))
                        existing.request_id = getattr(task, "request_id", None)
                        existing.message_id = getattr(task, "message_id", None)
                        existing.execution_id = getattr(task, "execution_id", None)
                        existing.error_category = getattr(task, "error_category", None)
                        existing.retry_history = _normalize_json(getattr(task, "retry_history", None))
                        await session.commit()

    async def _load_task_from_db(self, task_id: str, tenant_id: str | None = None) -> Task | None:
        if not self.session_factory:
            return None
        async with self.session_factory() as session:
            orm = await session.get(TaskORM, task_id)
            if not orm:
                return None
            task = orm.to_task()
            if tenant_id and task.tenant_id != tenant_id:
                return None
            return task

    async def _list_tasks_from_db(
        self,
        filters: TaskFilterOptions | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task]:
        if not self.session_factory:
            return []
        async with self.session_factory() as session:
            stmt = select(TaskORM)
            if filters:
                if filters.agent_id:
                    stmt = stmt.where(TaskORM.agent_id == filters.agent_id)
                if filters.status:
                    stmt = stmt.where(TaskORM.status == filters.status.value)
                if filters.priority is not None:
                    stmt = stmt.where(TaskORM.priority == int(filters.priority.value))
                if filters.tenant_id:
                    stmt = stmt.where(TaskORM.tenant_id == filters.tenant_id)
                if filters.request_id:
                    stmt = stmt.where(TaskORM.request_id == filters.request_id)
            stmt = stmt.order_by(TaskORM.created_at.desc()).offset(offset).limit(limit)
            result = await session.execute(stmt)
            return [orm.to_task() for orm in result.scalars().all()]

    async def _get_stats_from_db(self, tenant_id: str | None = None) -> TaskStats:
        tasks = await self._list_tasks_from_db(None, limit=100000, offset=0)
        stats = TaskStats(total=0, pending=0, running=0, completed=0, failed=0, cancelled=0, timeout=0)
        for task in tasks:
            if tenant_id and task.tenant_id != tenant_id:
                continue
            stats.total += 1
            if task.status == TaskStatus.PENDING:
                stats.pending += 1
            elif task.status == TaskStatus.RUNNING:
                stats.running += 1
            elif task.status == TaskStatus.COMPLETED:
                stats.completed += 1
            elif task.status == TaskStatus.FAILED:
                stats.failed += 1
            elif task.status == TaskStatus.CANCELLED:
                stats.cancelled += 1
            elif task.status == TaskStatus.TIMEOUT:
                stats.timeout += 1
        return stats

    async def enqueue(self, task: Task) -> None:
        """Add a task to the queue."""
        task.status = TaskStatus.PENDING
        task.started_at = None
        task.completed_at = None
        # Assign a durable queue message id if one was not already provided.
        # This is the "Queue Message ID" in the distributed trace and is the
        # Redis zset member that represents this enqueued message.
        if not task.message_id:
            task.message_id = f"msg-{uuid.uuid4().hex[:16]}"
        await self._save_task_to_db(task)

        try:
            existing = await self._task_exists(task.task_id)
            if existing:
                return
            await self.redis.set(self._task_key(task.task_id), task.model_dump_json(), ex=self.ttl_seconds)
            await self.redis.set(
                self._meta_key(task.task_id),
                json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                ex=self.ttl_seconds,
            )
            await self.redis.zadd(self.QUEUE_KEY, {task.task_id: self._priority_score(task)})
        except Exception:
            logger.exception("Redis enqueue failed for %s; task remains durable in PostgreSQL", task.task_id)

    async def dequeue(self, worker_id: str | None = None, lease_seconds: float | None = None) -> Task | None:
        """
        Pop the highest priority task from the queue and persist the lease.

        All database state changes (status, started_at, lease_owner, and
        lease_expires_at) are committed in a single transaction so that
        there is no window where the task is RUNNING with a NULL lease,
        which would make it invisible to reclaim_orphaned_tasks().
        """
        await self.reclaim_orphaned_tasks()

        try:
            result = await self.redis.zpopmin(self.QUEUE_KEY, count=1)
        except Exception:
            logger.exception("Redis dequeue failed")
            result = []

        if not result:
            return None

        task_id_value = result[0][0]
        task_id = task_id_value.decode("utf-8") if isinstance(task_id_value, bytes) else task_id_value
        task = await self.get_task(task_id)
        if not task:
            return None

        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(UTC)
        lease_seconds = lease_seconds or float(self.ttl_seconds)

        # Use database time for lease expiry to avoid clock-skew with reclaim.
        if self.session_factory:
            async with self.session_factory() as session:
                now_result = await session.execute(select(func.now()))
                db_now = now_result.scalar()
                if db_now is None:
                    db_now = datetime.now(UTC)
                elif db_now.tzinfo is None:
                    db_now = db_now.replace(tzinfo=UTC)
                else:
                    db_now = db_now.astimezone(UTC)
                lease_expires_at = db_now + timedelta(seconds=lease_seconds)
        else:
            lease_expires_at = datetime.now(UTC) + timedelta(seconds=lease_seconds)
        task.lease_owner = worker_id
        task.lease_expires_at = lease_expires_at
        if not task.execution_id:
            task.execution_id = uuid.uuid4().hex[:16]

        # Persist all state—including lease metadata—in ONE transaction to
        # eliminate the window where lease_expires_at is NULL while status
        # is RUNNING, which would make it invisible to reclaim_orphaned_tasks().
        if self.session_factory:
            async with self.session_factory() as session:
                orm = await session.get(TaskORM, task_id)
                if orm:
                    orm.status = task.status.value
                    orm.started_at = _to_naive_utc(task.started_at)
                    orm.retry_count = task.retry_count
                    orm.lease_owner = worker_id
                    orm.lease_expires_at = _to_naive_utc(lease_expires_at)
                    orm.execution_id = task.execution_id
                    orm.request_id = task.request_id
                    orm.message_id = task.message_id
                    orm.error_category = task.error_category
                    orm.retry_history = _normalize_json(task.retry_history)
                    await session.commit()
                else:
                    # Task not in DB yet; insert it.
                    session.add(TaskORM.from_task(task))
                    await session.commit()
        else:
            await self._save_task_to_db(task)

        try:
            await self.redis.zadd(self.PROCESSING_KEY, {task_id: lease_expires_at.timestamp()})
            await self.redis.set(self._task_key(task_id), task.model_dump_json(), ex=self.ttl_seconds)
            await self.redis.set(
                self._meta_key(task_id),
                json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                ex=self.ttl_seconds,
            )
        except Exception:
            logger.exception("Redis dequeue bookkeeping failed for %s", task_id)
        return task

    async def reclaim_orphaned_tasks(self) -> list[str]:
        """
        Requeue tasks that were left RUNNING in PostgreSQL after a worker crash
        or Redis restart.

        Uses an atomic UPDATE with a WHERE clause that checks lease expiry
        so that concurrent workers cannot double-reclaim the same task.
        The previous implementation loaded ORM objects via SELECT and committed
        stale updates inside a loop, creating a lost-update race: if worker B
        dequeued and re-leased a task between worker A's SELECT and COMMIT,
        worker A's stale ORM object would overwrite worker B's lease.
        """
        if not self.session_factory:
            return []

        reclaimed: list[str] = []

        async with self.session_factory() as session:
            # Use database time to avoid clock-skew between Python and PostgreSQL.
            now_result = await session.execute(select(func.now()))
            now = now_result.scalar()
            if now and now.tzinfo is not None:
                now = now.astimezone(UTC).replace(tzinfo=None)
            elif now is None:
                now = datetime.now(UTC).replace(tzinfo=None)

            # Capture the *pre-update* state of candidate rows (old worker,
            # execution id and retry_count) so we can record an accurate,
            # operator-readable retry reason. This SELECT is a read-only
            # snapshot; the authoritative reclaim is the atomic UPDATE below.
            candidates_stmt = select(
                TaskORM.task_id,
                TaskORM.agent_id,
                TaskORM.priority,
                TaskORM.tenant_id,
                TaskORM.lease_owner,
                TaskORM.execution_id,
                TaskORM.retry_count,
                TaskORM.payload,
                TaskORM.request_id,
                TaskORM.message_id,
            ).where(
                TaskORM.status == TaskStatus.RUNNING.value,
                TaskORM.lease_expires_at.is_not(None),
                TaskORM.lease_expires_at <= now,
            )
            cand_result = await session.execute(candidates_stmt)
            candidates: dict[str, tuple] = {r[0]: r for r in cand_result.fetchall()}

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
                .returning(TaskORM.task_id)
            )

            result = await session.execute(update_stmt)
            reclaimed_ids = [r[0] for r in result.fetchall()]

            # Record a structured retry entry per reclaimed task using the
            # pre-update owner/execution. Only tasks actually reclaimed by
            # THIS transaction (in reclaimed_ids) get an entry; the atomic
            # UPDATE guarantees a concurrent reclaimer cannot double-count.
            for task_id in reclaimed_ids:
                cand = candidates.get(task_id)
                if cand is None:
                    continue
                _, agent_id, priority, tenant_id, old_owner, old_exec, old_retry, payload, req_id, msg_id = cand
                new_retry = (old_retry or 0) + 1
                task = Task(
                    task_id=task_id,
                    agent_id=agent_id,
                    type="reclaimed",
                    payload=payload or {},
                    priority=TaskPriority(priority) if priority is not None else TaskPriority.MEDIUM,
                    status=TaskStatus.PENDING,
                    retry_count=new_retry,
                    tenant_id=tenant_id,
                    request_id=req_id,
                    message_id=msg_id,
                )
                task.started_at = None
                task.completed_at = None
                task.add_retry_entry(
                    retry_number=new_retry,
                    worker_id=old_owner,
                    execution_id=old_exec,
                    previous_state=TaskStatus.RUNNING.value,
                    error_category="lease_expired",
                    reason=(
                        "Lease expired; the owning worker was assumed crashed or "
                        "stalled and the task was reclaimed for re-execution."
                    ),
                    lease_expired=True,
                    next_retry_decision="requeue",
                )
                await session.execute(
                    update(TaskORM)
                    .where(TaskORM.task_id == task_id)
                    .values(retry_history=_normalize_json(task.retry_history))
                )
                reclaimed.append(task_id)

                try:
                    await self.redis.set(self._task_key(task_id), task.model_dump_json(), ex=self.ttl_seconds)
                    await self.redis.set(
                        self._meta_key(task_id),
                        json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                        ex=self.ttl_seconds,
                    )
                    await self.redis.zadd(self.QUEUE_KEY, {task_id: self._priority_score(task)})
                    await self.redis.zrem(self.PROCESSING_KEY, task_id)
                except Exception:
                    logger.exception("Redis recovery enqueue failed for %s", task_id)

            # Single commit for the whole reclaim transaction (state reset +
            # retry_history entries). This keeps reclaim atomic with respect to
            # the DB and avoids interleaving with concurrent reclaimers.
            await session.commit()

        return reclaimed

    async def reclaim_expired_tasks(self) -> list[str]:
        """Compatibility alias for worker-failure recovery."""
        return await self.reclaim_orphaned_tasks()

    async def recover_orphaned_tasks(self) -> list[str]:
        """
        Recover tasks that are PENDING or RUNNING in PostgreSQL but missing from Redis.
        This is intended for startup after Redis restarts.

        Commits all DB changes in a single transaction instead of per-row
        commits inside a loop, which previously could cause inconsistency
        if the session was used for concurrent operations.
        """
        if not self.session_factory:
            return []

        recovered: list[str] = []
        now = datetime.now(UTC).replace(tzinfo=None)
        async with self.session_factory() as session:
            stmt = select(TaskORM).where(TaskORM.status.in_([TaskStatus.PENDING.value, TaskStatus.RUNNING.value]))
            result = await session.execute(stmt)
            orms = result.scalars().all()

            for orm in orms:
                if orm.status == TaskStatus.RUNNING.value and orm.lease_expires_at and orm.lease_expires_at > now:
                    pass  # still active lease, leave as-is
                else:
                    orm.status = TaskStatus.PENDING.value
                    orm.started_at = None
                    orm.lease_owner = None
                    orm.lease_expires_at = None

            await session.commit()

        for orm in orms:
            task = orm.to_task()
            try:
                current = await self.redis.get(self._task_key(task.task_id))
                if current:
                    continue
            except Exception:
                logger.debug("Redis unavailable during startup recovery for %s", task.task_id, exc_info=True)

            try:
                await self.redis.set(self._task_key(task.task_id), task.model_dump_json(), ex=self.ttl_seconds)
                await self.redis.set(
                    self._meta_key(task.task_id),
                    json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                    ex=self.ttl_seconds,
                )
                await self.redis.zadd(self.QUEUE_KEY, {task.task_id: self._priority_score(task)})
                recovered.append(task.task_id)
            except Exception:
                logger.exception("Redis startup recovery failed for %s", task.task_id)

        return recovered

    async def peek(self) -> Task | None:
        """Peek at the next task without removing it."""
        try:
            result = await self.redis.zrange(self.QUEUE_KEY, 0, 0, withscores=True)
            if result:
                task_id_value = result[0][0]
                task_id = task_id_value.decode("utf-8") if isinstance(task_id_value, bytes) else task_id_value
                task = await self.get_task(task_id)
                if task:
                    return task
        except Exception:
            logger.exception("Redis peek failed")
        return None

    async def cancel(self, task_id: str, tenant_id: str | None = None) -> bool:
        """Cancel a pending task."""
        task = await self.get_task(task_id, tenant_id)
        if not task:
            return False
        if task.status not in (TaskStatus.PENDING, TaskStatus.SCHEDULED):
            return False

        task.status = TaskStatus.CANCELLED
        task.completed_at = datetime.now(UTC)
        await self._save_task_to_db(task)
        try:
            await self.redis.zrem(self.QUEUE_KEY, task_id)
            await self.redis.zrem(self.PROCESSING_KEY, task_id)
            await self.redis.set(self._task_key(task_id), task.model_dump_json(), ex=self.ttl_seconds)
        except Exception:
            logger.exception("Redis cancel bookkeeping failed for %s", task_id)
        return True

    async def get_task(self, task_id: str, tenant_id: str | None = None) -> Task | None:
        """Retrieve a task by ID."""
        try:
            data = await self.redis.get(self._task_key(task_id))
            if data:
                task = Task.model_validate_json(data)
                if tenant_id and task.tenant_id != tenant_id:
                    return None
                return task
        except Exception:
            logger.debug("Redis get_task failed for %s", task_id, exc_info=True)
        return await self._load_task_from_db(task_id, tenant_id)

    async def list_tasks(
        self,
        filters: TaskFilterOptions | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task]:
        """List tasks with filtering and pagination."""
        if self.session_factory:
            return await self._list_tasks_from_db(filters, limit, offset)

        cursor = 0
        keys = []
        while True:
            cursor, batch = await self.redis.scan(cursor, match=f"{self.TASK_PREFIX}*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        tasks = []
        for key in keys:
            data = await self.redis.get(key)
            if not data:
                continue
            task = Task.model_validate_json(data)
            if filters:
                if filters.agent_id and task.agent_id != filters.agent_id:
                    continue
                if filters.status and task.status != filters.status:
                    continue
                if filters.priority and task.priority != filters.priority:
                    continue
                if filters.tenant_id and task.tenant_id != filters.tenant_id:
                    continue
            tasks.append(task)

        tasks.sort(key=lambda t: t.created_at, reverse=True)
        return tasks[offset:offset + limit]

    async def get_stats(self, tenant_id: str | None = None) -> TaskStats:
        """Get task statistics."""
        if self.session_factory:
            return await self._get_stats_from_db(tenant_id)

        cursor = 0
        keys = []
        while True:
            cursor, batch = await self.redis.scan(cursor, match=f"{self.TASK_PREFIX}*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        stats = TaskStats(total=0, pending=0, running=0, completed=0, failed=0, cancelled=0, timeout=0)
        for key in keys:
            data = await self.redis.get(key)
            if not data:
                continue
            task = Task.model_validate_json(data)
            if tenant_id and task.tenant_id != tenant_id:
                continue
            stats.total += 1
            if task.status == TaskStatus.PENDING:
                stats.pending += 1
            elif task.status == TaskStatus.RUNNING:
                stats.running += 1
            elif task.status == TaskStatus.COMPLETED:
                stats.completed += 1
            elif task.status == TaskStatus.FAILED:
                stats.failed += 1
            elif task.status == TaskStatus.CANCELLED:
                stats.cancelled += 1
            elif task.status == TaskStatus.TIMEOUT:
                stats.timeout += 1
        return stats

    async def size(self) -> int:
        """Get the number of pending tasks in the queue."""
        if self.session_factory:
            tasks = await self._list_tasks_from_db(None, limit=100000, offset=0)
            return sum(1 for task in tasks if task.status == TaskStatus.PENDING)
        try:
            return await self.redis.zcard(self.QUEUE_KEY)
        except Exception:
            raise

    async def update_task(self, task: Task) -> None:
        """Update an existing task in the store."""
        await self._save_task_to_db(task)
        try:
            await self.redis.set(self._task_key(task.task_id), task.model_dump_json(), ex=self.ttl_seconds)
            await self.redis.set(
                self._meta_key(task.task_id),
                json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                ex=self.ttl_seconds,
            )
            if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT):
                await self.redis.zrem(self.PROCESSING_KEY, task.task_id)
        except Exception:
            logger.exception("Redis task update failed for %s", task.task_id)
