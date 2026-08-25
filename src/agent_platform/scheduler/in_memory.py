
# In-memory task queue using heapq for priority (FIFO on tie)

import asyncio
import heapq
import uuid
from datetime import UTC, datetime

from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.scheduler.base import BaseTaskQueue
from src.agent_platform.scheduler.models import TaskFilterOptions, TaskQueueItem, TaskStats


class InMemoryTaskQueue(BaseTaskQueue):
    """
    Thread-safe in-memory priority queue.
    Items are stored as (priority, created_at, task_id) for heapq ordering.
    """

    def __init__(self):
        self._heap: list[tuple[int, float, str]] = []  # (priority, created_at, task_id)
        self._tasks: dict[str, Task] = {}  # task_id -> Task
        self._lock = asyncio.Lock()

    def _priority_value(self, task: Task) -> int:
        # Lower number = higher priority
        return task.priority.value

    async def enqueue(self, task: Task) -> None:
        async with self._lock:
            if task.task_id in self._tasks:
                return
            task.status = TaskStatus.PENDING
            if not task.message_id:
                task.message_id = f"msg-{uuid.uuid4().hex[:16]}"
            self._tasks[task.task_id] = task
            item = TaskQueueItem(
                task_id=task.task_id,
                priority=self._priority_value(task),
                created_at=task.created_at.timestamp(),
            )
            heapq.heappush(self._heap, (item.priority, item.created_at, item.task_id))

    async def dequeue(self, worker_id: str | None = None, lease_seconds: float | None = None) -> Task | None:
        async with self._lock:
            if not self._heap:
                return None
            _, _, task_id = heapq.heappop(self._heap)
            task = self._tasks.get(task_id)
            if task:
                task.status = TaskStatus.RUNNING
                task.started_at = datetime.now(UTC)
            return task

    async def peek(self) -> Task | None:
        async with self._lock:
            if not self._heap:
                return None
            _, _, task_id = self._heap[0]
            return self._tasks.get(task_id)

    async def cancel(self, task_id: str, tenant_id: str | None = None) -> bool:
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            if tenant_id and task.tenant_id != tenant_id:
                return False
            if task.status not in (TaskStatus.PENDING, TaskStatus.SCHEDULED):
                return False
            task.status = TaskStatus.CANCELLED
            return True

    async def get_task(self, task_id: str, tenant_id: str | None = None) -> Task | None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if task and tenant_id and task.tenant_id != tenant_id:
                return None
            return task

    async def list_tasks(
        self,
        filters: TaskFilterOptions | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task]:
        async with self._lock:
            results = list(self._tasks.values())
            if filters:
                if filters.agent_id:
                    results = [t for t in results if t.agent_id == filters.agent_id]
                if filters.status:
                    results = [t for t in results if t.status == filters.status]
                if filters.priority:
                    results = [t for t in results if t.priority == filters.priority]
                if filters.tenant_id:
                    results = [t for t in results if t.tenant_id == filters.tenant_id]
                if filters.request_id:
                    results = [t for t in results if t.request_id == filters.request_id]
            results.sort(key=lambda t: t.created_at, reverse=True)
            return results[offset:offset + limit]

    async def get_stats(self, tenant_id: str | None = None) -> TaskStats:
        async with self._lock:
            tasks = list(self._tasks.values())
            if tenant_id:
                tasks = [t for t in tasks if t.tenant_id == tenant_id]
            stats = TaskStats(
                total=len(tasks),
                pending=sum(1 for t in tasks if t.status == TaskStatus.PENDING),
                running=sum(1 for t in tasks if t.status == TaskStatus.RUNNING),
                completed=sum(1 for t in tasks if t.status == TaskStatus.COMPLETED),
                failed=sum(1 for t in tasks if t.status == TaskStatus.FAILED),
                cancelled=sum(1 for t in tasks if t.status == TaskStatus.CANCELLED),
                timeout=sum(1 for t in tasks if t.status == TaskStatus.TIMEOUT),
            )
            return stats

    async def size(self) -> int:
        async with self._lock:
            return len(self._heap)

    async def update_task(self, task: Task) -> None:
        """Update an existing task in the store."""
        async with self._lock:
            if task.task_id in self._tasks:
                self._tasks[task.task_id] = task

    async def reclaim_expired_tasks(self) -> list[str]:
        """In-memory queue has no leases; nothing to reclaim."""
        return []
