# Redis-backed task queue with PostgreSQL as the source of truth.

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

try:
    from redis.asyncio import Redis
except ImportError:  # pragma: no cover - optional dependency
    Redis = Any

if TYPE_CHECKING:
    from redis.asyncio import Redis as RedisClient
else:
    RedisClient = Any

from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.scheduler.base import BaseTaskQueue
from src.agent_platform.scheduler.models import TaskFilterOptions, TaskStats
from src.agent_platform.scheduler.postgres_tasks import TaskORM

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
        async with self.session_factory() as session:
            existing = await session.get(TaskORM, task.task_id)
            if existing:
                existing.agent_id = task.agent_id
                existing.task_type = task.type
                existing.payload = task.payload
                existing.priority = int(task.priority.value)
                existing.status = task.status.value
                existing.created_at = task.created_at
                existing.started_at = task.started_at
                existing.completed_at = task.completed_at
                existing.result = task.result
                existing.error = task.error
                existing.retry_count = task.retry_count
                existing.max_retries = task.max_retries
                existing.timeout_seconds = task.timeout_seconds
                existing.tenant_id = task.tenant_id
                existing.lease_owner = getattr(task, "lease_owner", None)
                existing.lease_expires_at = getattr(task, "lease_expires_at", None)
            else:
                session.add(TaskORM.from_task(task))
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
        await self._save_task_to_db(task)

        try:
            existing = await self._task_exists(task.task_id)
            if existing:
                return
            if hasattr(self.redis, "setex"):
                await self.redis.setex(self._task_key(task.task_id), self.ttl_seconds, task.model_dump_json())
                await self.redis.setex(
                    self._meta_key(task.task_id),
                    self.ttl_seconds,
                    json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                )
            else:
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
        lease_expires_at = datetime.fromtimestamp(datetime.now(UTC).timestamp() + lease_seconds, tz=UTC)
        await self._save_task_to_db(task)

        if self.session_factory:
            async with self.session_factory() as session:
                orm = await session.get(TaskORM, task_id)
                if orm:
                    orm.status = task.status.value
                    orm.started_at = task.started_at
                    orm.retry_count = task.retry_count
                    orm.lease_owner = worker_id
                    orm.lease_expires_at = lease_expires_at
                    await session.commit()

        try:
            await self.redis.zadd(self.PROCESSING_KEY, {task_id: lease_expires_at.timestamp()})
            if hasattr(self.redis, "setex"):
                await self.redis.setex(self._task_key(task_id), self.ttl_seconds, task.model_dump_json())
                await self.redis.setex(
                    self._meta_key(task_id),
                    self.ttl_seconds,
                    json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                )
            else:
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
        """
        if not self.session_factory:
            return []

        now = datetime.now(UTC)
        reclaimed: list[str] = []
        async with self.session_factory() as session:
            stmt = select(TaskORM).where(TaskORM.status == TaskStatus.RUNNING.value)
            result = await session.execute(stmt)
            for orm in result.scalars().all():
                lease_expires_at = orm.lease_expires_at
                if lease_expires_at and lease_expires_at > now:
                    continue

                task = orm.to_task()
                task.status = TaskStatus.PENDING
                task.started_at = None
                task.retry_count += 1

                orm.status = TaskStatus.PENDING.value
                orm.started_at = None
                orm.lease_owner = None
                orm.lease_expires_at = None
                orm.retry_count = task.retry_count
                await session.commit()
                reclaimed.append(task.task_id)

                try:
                    if hasattr(self.redis, "setex"):
                        await self.redis.setex(self._task_key(task.task_id), self.ttl_seconds, task.model_dump_json())
                        await self.redis.setex(
                            self._meta_key(task.task_id),
                            self.ttl_seconds,
                            json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                        )
                    else:
                        await self.redis.set(self._task_key(task.task_id), task.model_dump_json(), ex=self.ttl_seconds)
                        await self.redis.set(
                            self._meta_key(task.task_id),
                            json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                            ex=self.ttl_seconds,
                        )
                    await self.redis.zadd(self.QUEUE_KEY, {task.task_id: self._priority_score(task)})
                    await self.redis.zrem(self.PROCESSING_KEY, task.task_id)
                except Exception:
                    logger.exception("Redis recovery enqueue failed for %s", task.task_id)

        return reclaimed

    async def reclaim_expired_tasks(self) -> list[str]:
        """Compatibility alias for worker-failure recovery."""
        return await self.reclaim_orphaned_tasks()

    async def recover_orphaned_tasks(self) -> list[str]:
        """
        Recover tasks that are PENDING or RUNNING in PostgreSQL but missing from Redis.
        This is intended for startup after Redis restarts.
        """
        if not self.session_factory:
            return []

        recovered: list[str] = []
        now = datetime.now(UTC)
        async with self.session_factory() as session:
            stmt = select(TaskORM).where(TaskORM.status.in_([TaskStatus.PENDING.value, TaskStatus.RUNNING.value]))
            result = await session.execute(stmt)
            for orm in result.scalars().all():
                task = orm.to_task()
                if orm.status == TaskStatus.RUNNING.value and orm.lease_expires_at and orm.lease_expires_at > now:
                    task.status = TaskStatus.RUNNING
                else:
                    task.status = TaskStatus.PENDING
                    task.started_at = None
                    orm.status = TaskStatus.PENDING.value
                    orm.started_at = None
                    orm.lease_owner = None
                    orm.lease_expires_at = None
                    await session.commit()

                try:
                    current = await self.redis.get(self._task_key(task.task_id))
                    if current:
                        continue
                except Exception:
                    logger.debug("Redis unavailable during startup recovery for %s", task.task_id, exc_info=True)

                try:
                    if hasattr(self.redis, "setex"):
                        await self.redis.setex(self._task_key(task.task_id), self.ttl_seconds, task.model_dump_json())
                        await self.redis.setex(
                            self._meta_key(task.task_id),
                            self.ttl_seconds,
                            json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                        )
                    else:
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
            if hasattr(self.redis, "setex"):
                await self.redis.setex(self._task_key(task_id), self.ttl_seconds, task.model_dump_json())
            else:
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
            if hasattr(self.redis, "setex"):
                await self.redis.setex(self._task_key(task.task_id), self.ttl_seconds, task.model_dump_json())
                await self.redis.setex(
                    self._meta_key(task.task_id),
                    self.ttl_seconds,
                    json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
                )
            else:
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
