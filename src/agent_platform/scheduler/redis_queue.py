
# Redis-backed task queue using Sorted Set for priority and task storage

import json
from typing import Optional, List
from datetime import datetime

from redis.asyncio import Redis

from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.scheduler.base import BaseTaskQueue
from src.agent_platform.scheduler.models import TaskFilterOptions, TaskStats


class RedisTaskQueue(BaseTaskQueue):
    """
    Redis-backed queue.
    - Sorted Set 'tasks:queue' with score = priority (lower is higher)
    - String key 'tasks:data:{task_id}' for full task JSON
    - String key 'tasks:meta:{task_id}' for status tracking
    """

    QUEUE_KEY = "tasks:queue"
    TASK_PREFIX = "tasks:data:"
    META_PREFIX = "tasks:meta:"

    def __init__(self, redis_client: Redis, ttl_seconds: int = 86400):
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds

    def _priority_score(self, task: Task) -> float:
        # Lower priority value = higher priority
        return task.priority.value + (task.created_at.timestamp() % 1)

    def _task_key(self, task_id: str) -> str:
        return f"{self.TASK_PREFIX}{task_id}"

    def _meta_key(self, task_id: str) -> str:
        return f"{self.META_PREFIX}{task_id}"

    async def enqueue(self, task: Task) -> None:
        """Add a task to the queue."""
        task.status = TaskStatus.PENDING

        await self.redis.set(
            self._task_key(task.task_id),
            task.model_dump_json(),
            ex=self.ttl_seconds
        )
        await self.redis.set(
            self._meta_key(task.task_id),
            json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
            ex=self.ttl_seconds,
        )
        score = self._priority_score(task)
        await self.redis.zadd(self.QUEUE_KEY, {task.task_id: score})

    async def dequeue(self) -> Optional[Task]:
        """
        Pop the highest priority task from the queue.
        Uses atomic ZPOPMIN for distributed safety.
        """
        result = await self.redis.zpopmin(self.QUEUE_KEY, count=1)
        if not result:
            return None

        # result is list of tuples: [(member, score)] where member is bytes
        task_id_bytes = result[0][0]
        task_id = task_id_bytes.decode('utf-8') if isinstance(task_id_bytes, bytes) else task_id_bytes

        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return None

        task = Task.model_validate_json(data)
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.utcnow()

        # Update stored task
        await self.redis.set(
            self._task_key(task_id),
            task.model_dump_json(),
            ex=self.ttl_seconds
        )
        await self.redis.set(
            self._meta_key(task_id),
            json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
            ex=self.ttl_seconds,
        )
        return task

    async def peek(self) -> Optional[Task]:
        """
        Peek at the next task without removing it.
        """
        result = await self.redis.zrange(self.QUEUE_KEY, 0, 0, withscores=True)
        if not result:
            return None
        # result is list of tuples: [(member, score)] where member is bytes
        task_id_bytes = result[0][0]
        task_id = task_id_bytes.decode('utf-8') if isinstance(task_id_bytes, bytes) else task_id_bytes

        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return None
        return Task.model_validate_json(data)

    async def cancel(self, task_id: str, tenant_id: Optional[str] = None) -> bool:
        """Cancel a pending task."""
        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return False
        task = Task.model_validate_json(data)
        if tenant_id and task.tenant_id != tenant_id:
            return False
        if task.status not in (TaskStatus.PENDING, TaskStatus.SCHEDULED):
            return False

        await self.redis.zrem(self.QUEUE_KEY, task_id)
        task.status = TaskStatus.CANCELLED
        await self.redis.set(
            self._task_key(task_id),
            task.model_dump_json(),
            ex=self.ttl_seconds
        )
        return True

    async def get_task(self, task_id: str, tenant_id: Optional[str] = None) -> Optional[Task]:
        """Retrieve a task by ID."""
        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return None
        task = Task.model_validate_json(data)
        if tenant_id and task.tenant_id != tenant_id:
            return None
        return task

    async def list_tasks(
        self,
        filters: Optional[TaskFilterOptions] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Task]:
        """List tasks with filtering and pagination."""
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

    async def get_stats(self, tenant_id: Optional[str] = None) -> TaskStats:
        """Get aggregated statistics for tasks."""
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
        return await self.redis.zcard(self.QUEUE_KEY)

    async def update_task(self, task: Task) -> None:
        """Update an existing task in the store."""
        await self.redis.set(
            self._task_key(task.task_id),
            task.model_dump_json(),
            ex=self.ttl_seconds
        )
        await self.redis.set(
            self._meta_key(task.task_id),
            json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
            ex=self.ttl_seconds,
        )