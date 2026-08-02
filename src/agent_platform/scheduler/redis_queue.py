
# Redis-backed task queue using Sorted Set for priority and Hash for task storage

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
    - Hash 'tasks:data:{task_id}' for full task JSON
    - Key 'tasks:meta:{task_id}' for status tracking
    """

    QUEUE_KEY = "tasks:queue"

    def __init__(self, redis_client: Redis):
        self.redis = redis_client

    def _priority_score(self, task: Task) -> float:
        # Lower number = higher priority.
        # Use priority value (0=CRITICAL, 3=LOW) plus fractional part for FIFO.
        # We'll use: priority_value + (timestamp microsecond / 1e6) for stable ordering.
        return task.priority.value + (task.created_at.timestamp() % 1)

    def _task_key(self, task_id: str) -> str:
        return f"tasks:data:{task_id}"

    def _meta_key(self, task_id: str) -> str:
        return f"tasks:meta:{task_id}"

    async def enqueue(self, task: Task) -> None:
        task.status = TaskStatus.PENDING
        # Store full task in hash
        await self.redis.set(
            self._task_key(task.task_id),
            task.model_dump_json(),
            ex=86400,  # 1 day TTL for cleanup
        )
        # Store status separately for quick access
        await self.redis.set(
            self._meta_key(task.task_id),
            json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
            ex=86400,
        )
        # Add to priority queue (sorted set)
        score = self._priority_score(task)
        await self.redis.zadd(self.QUEUE_KEY, {task.task_id: score})

    async def dequeue(self) -> Optional[Task]:
        # Get the smallest score (highest priority) task
        result = await self.redis.zpopmin(self.QUEUE_KEY, count=1)
        if not result:
            return None
        task_id = result[0][0]  # (member, score)
        # Fetch task data
        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return None
        task = Task.model_validate_json(data)
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.utcnow()
        # Update stored task
        await self.redis.set(self._task_key(task_id), task.model_dump_json(), ex=86400)
        await self.redis.set(
            self._meta_key(task_id),
            json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
            ex=86400,
        )
        return task

    async def peek(self) -> Optional[Task]:
        # Get the first element without popping
        result = await self.redis.zrange(self.QUEUE_KEY, 0, 0, withscores=True)
        if not result:
            return None
        task_id = result[0][0]
        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return None
        return Task.model_validate_json(data)

    async def cancel(self, task_id: str, tenant_id: Optional[str] = None) -> bool:
        # Check if task exists and belongs to tenant
        data = await self.redis.get(self._task_key(task_id))
        if not data:
            return False
        task = Task.model_validate_json(data)
        if tenant_id and task.tenant_id != tenant_id:
            return False
        if task.status not in (TaskStatus.PENDING, TaskStatus.SCHEDULED):
            return False
        # Remove from queue and mark as cancelled
        await self.redis.zrem(self.QUEUE_KEY, task_id)
        task.status = TaskStatus.CANCELLED
        await self.redis.set(self._task_key(task_id), task.model_dump_json(), ex=86400)
        await self.redis.set(
            self._meta_key(task_id),
            json.dumps({"status": task.status.value, "agent_id": task.agent_id}),
            ex=86400,
        )
        return True

    async def get_task(self, task_id: str, tenant_id: Optional[str] = None) -> Optional[Task]:
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
        # Scan all task keys (not efficient for huge scale, but works for now)
        cursor = 0
        keys = []
        while True:
            cursor, batch = await self.redis.scan(cursor, match="tasks:data:*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        tasks = []
        for key in keys:
            data = await self.redis.get(key)
            if not data:
                continue
            task = Task.model_validate_json(data)

            # Apply filters
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
        # This is approximate; for production, use dedicated counters.
        # We'll count using scan.
        cursor = 0
        keys = []
        while True:
            cursor, batch = await self.redis.scan(cursor, match="tasks:data:*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        stats = TaskStats(
            total=0, pending=0, running=0, completed=0, failed=0, cancelled=0, timeout=0
        )
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
        return await self.redis.zcard(self.QUEUE_KEY)