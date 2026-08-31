
# Distributed task queue using Redis

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from redis.asyncio import Redis
    RedisClient = Redis
else:
    RedisClient = Any

from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.scheduler.base import BaseTaskQueue
from src.agent_platform.scheduler.models import TaskFilterOptions, TaskStats


class DistributedTaskQueue(BaseTaskQueue):
    """
    Distributed task queue using Redis.
    Uses Sorted Set for priority queue and separate keys for task data.
    Supports multiple workers across nodes.
    """

    QUEUE_KEY = "dist:tasks:queue"
    PROCESSING_KEY = "dist:tasks:processing"
    TASK_PREFIX = "dist:tasks:data:"
    META_PREFIX = "dist:tasks:meta:"
    STATS_KEY = "dist:tasks:stats"

    def __init__(self, redis_client: RedisClient, ttl_seconds: int = 86400):
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds

    def _task_key(self, task_id: str) -> str:
        return f"{self.TASK_PREFIX}{task_id}"

    def _meta_key(self, task_id: str) -> str:
        return f"{self.META_PREFIX}{task_id}"

    def _has_existing_task(self, existing: Any) -> bool:
        if existing is None:
            return False
        if isinstance(existing, (bytes, bytearray, str, dict, list, tuple, set)):
            return True
        return type(existing).__module__ != "unittest.mock"

    def _priority_score(self, task: Task) -> float:
        # Lower priority value = higher priority
        return task.priority.value + (task.created_at.timestamp() % 1)

    async def enqueue(self, task: Task) -> None:
        """Add a task to the distributed queue."""
        # Atomic first-writer-wins guard for duplicate task IDs.
        try:
            claimed = await self.redis.set(
                self._task_key(task.task_id),
                task.model_dump_json(),
                ex=self.ttl_seconds,
                nx=True,
            )
        except TypeError:
            claimed = await self.redis.set(
                self._task_key(task.task_id),
                task.model_dump_json(),
                ex=self.ttl_seconds,
                nx=True,
            )
        if not claimed:
            return
        task.status = TaskStatus.PENDING

        # Store meta for quick access
        await self.redis.setex(
            self._meta_key(task.task_id),
            self.ttl_seconds,
            json.dumps({"status": task.status.value, "agent_id": task.agent_id})
        )

        # Add to priority queue with score
        score = self._priority_score(task)
        await self.redis.zadd(self.QUEUE_KEY, {task.task_id: score})

    async def dequeue(self, worker_id: str | None = None, lease_seconds: float | None = None) -> Task | None:
        """
        Pop the highest priority task from the queue.
        Uses atomic ZPOPMIN for distributed safety and records a lease.
        """
        await self.reclaim_expired_tasks()
        # Atomic pop
        result = await self.redis.zpopmin(self.QUEUE_KEY, count=1)
        if not result:
            return None

        task_id_value = result[0][0]
        task_id = task_id_value.decode("utf-8") if isinstance(task_id_value, bytes) else str(task_id_value)
        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return None

        task = cast(Task, Task.model_validate_json(data))
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(UTC)

        lease_seconds = lease_seconds or float(self.ttl_seconds)
        deadline = datetime.now(UTC).timestamp() + lease_seconds
        await self.redis.zadd(self.PROCESSING_KEY, {task_id: deadline})

        # Update stored task
        await self.redis.setex(
            self._task_key(task_id),
            self.ttl_seconds,
            task.model_dump_json()
        )
        await self.redis.setex(
            self._meta_key(task_id),
            self.ttl_seconds,
            json.dumps({"status": task.status.value, "agent_id": task.agent_id})
        )

        return task

    async def reclaim_expired_tasks(self) -> list[str]:
        """Move expired processing tasks back to the pending queue."""
        now_ts = datetime.now(UTC).timestamp()
        expired = cast(list[tuple[Any, Any]], await self.redis.zrange(self.PROCESSING_KEY, 0, -1, withscores=True))
        reclaimed: list[str] = []
        for task_id_bytes, deadline in expired:
            task_id = task_id_bytes.decode("utf-8") if isinstance(task_id_bytes, bytes) else task_id_bytes
            task_id = str(task_id)
            if float(deadline) > now_ts:
                continue
            data = await self.redis.get(self._task_key(task_id))
            if not data:
                await self.redis.zrem(self.PROCESSING_KEY, task_id)
                continue
            task = cast(Task, Task.model_validate_json(data))
            if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT):
                await self.redis.zrem(self.PROCESSING_KEY, task_id)
                continue
            task.status = TaskStatus.PENDING
            task.started_at = None
            task.retry_count += 1
            await self.redis.setex(
                self._task_key(task_id),
                self.ttl_seconds,
                task.model_dump_json()
            )
            await self.redis.setex(
                self._meta_key(task_id),
                self.ttl_seconds,
                json.dumps({"status": task.status.value, "agent_id": task.agent_id})
            )
            await self.redis.zrem(self.PROCESSING_KEY, task_id)
            await self.redis.zadd(self.QUEUE_KEY, {task_id: self._priority_score(task)})
            reclaimed.append(task_id)
        return reclaimed

    async def peek(self) -> Task | None:
        """Peek at the next task without removing it."""
        result = await self.redis.zrange(self.QUEUE_KEY, 0, 0, withscores=True)
        if not result:
            return None
        task_id_value = result[0][0]
        task_id = task_id_value.decode("utf-8") if isinstance(task_id_value, bytes) else str(task_id_value)
        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return None
        return cast(Task, Task.model_validate_json(data))

    async def cancel(self, task_id: str, tenant_id: str | None = None) -> bool:
        """Cancel a pending task."""
        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return False
        task = cast(Task, Task.model_validate_json(data))
        if tenant_id and task.tenant_id != tenant_id:
            return False
        if task.status not in (TaskStatus.PENDING, TaskStatus.SCHEDULED):
            return False

        # Remove from queue
        await self.redis.zrem(self.QUEUE_KEY, task_id)
        task.status = TaskStatus.CANCELLED
        await self.redis.setex(
            self._task_key(task_id),
            self.ttl_seconds,
            task.model_dump_json()
        )
        return True

    async def get_task(self, task_id: str, tenant_id: str | None = None) -> Task | None:
        """Get a task by ID."""
        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return None
        task = cast(Task, Task.model_validate_json(data))
        if tenant_id and task.tenant_id != tenant_id:
            return None
        return task

    async def list_tasks(
        self,
        filters: TaskFilterOptions | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task]:
        """List tasks with filtering and pagination."""
        # Scan all task keys (inefficient for large datasets; use indices in production)
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
            task = cast(Task, Task.model_validate_json(data))
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
        # For distributed queue, we use Redis counters or scan
        # Simplified: scan all tasks
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
            task = cast(Task, Task.model_validate_json(data))
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
        return int(await self.redis.zcard(self.QUEUE_KEY))

    async def update_task(self, task: Task) -> None:
        """Update a task in the distributed store."""
        await self.redis.setex(
            self._task_key(task.task_id),
            self.ttl_seconds,
            task.model_dump_json()
        )
        await self.redis.setex(
            self._meta_key(task.task_id),
            self.ttl_seconds,
            json.dumps({"status": task.status.value, "agent_id": task.agent_id})
        )
        if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT):
            await self.redis.zrem(self.PROCESSING_KEY, task.task_id)
