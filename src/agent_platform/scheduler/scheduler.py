
# Main Task Scheduler service orchestrating queue operations

from typing import Optional, List
from uuid import uuid4

from src.agent_platform.core.task import Task, TaskStatus, TaskPriority
from src.agent_platform.scheduler.base import BaseTaskQueue
from src.agent_platform.scheduler.models import TaskFilterOptions, TaskStats


class TaskScheduler:
    """
    High-level task scheduler service.
    Handles submission, cancellation, status retrieval, and listing.
    """

    def __init__(self, queue: BaseTaskQueue):
        self.queue = queue

    async def submit_task(
        self,
        agent_id: str,
        task_type: str,
        payload: dict,
        priority: TaskPriority = TaskPriority.MEDIUM,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        tenant_id: Optional[str] = None,
    ) -> str:
        """
        Submit a new task to the queue.
        Returns the generated task_id.
        """
        task = Task(
            task_id=f"task-{uuid4().hex[:8]}",
            agent_id=agent_id,
            type=task_type,
            payload=payload,
            priority=priority,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            tenant_id=tenant_id,
        )
        await self.queue.enqueue(task)
        return task.task_id

    async def cancel_task(self, task_id: str, tenant_id: Optional[str] = None) -> bool:
        """Cancel a pending task."""
        return await self.queue.cancel(task_id, tenant_id)

    async def get_task_status(self, task_id: str, tenant_id: Optional[str] = None) -> Optional[TaskStatus]:
        """Get the status of a task."""
        task = await self.queue.get_task(task_id, tenant_id)
        if task:
            return task.status
        return None

    async def get_task(self, task_id: str, tenant_id: Optional[str] = None) -> Optional[Task]:
        """Get the full task details."""
        return await self.queue.get_task(task_id, tenant_id)

    async def list_tasks(
        self,
        filters: Optional[TaskFilterOptions] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Task]:
        """List tasks with filtering and pagination."""
        return await self.queue.list_tasks(filters, limit, offset)

    async def get_stats(self, tenant_id: Optional[str] = None) -> TaskStats:
        """Get task statistics."""
        return await self.queue.get_stats(tenant_id)

    async def dequeue_next(self) -> Optional[Task]:
        """Pop the next task for execution (used by workers)."""
        return await self.queue.dequeue()

    async def peek_next(self) -> Optional[Task]:
        """Look at the next task without removing it."""
        return await self.queue.peek()

    async def queue_size(self) -> int:
        """Get the number of pending tasks."""
        return await self.queue.size()

    async def on_task_completed(self, task: Task) -> None:
        """
        Called when a task is completed (success, failure, timeout, etc.)
        Updates the task in the queue's storage.
        """
        await self.queue.update_task(task)