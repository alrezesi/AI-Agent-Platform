
# Abstract base class for task queues

from abc import ABC, abstractmethod

from src.agent_platform.core.task import Task
from src.agent_platform.scheduler.models import TaskFilterOptions, TaskStats


class BaseTaskQueue(ABC):
    """Abstract interface for a priority-based task queue."""

    @abstractmethod
    async def enqueue(self, task: Task) -> None:
        """Add a task to the queue with its priority."""
        pass

    @abstractmethod
    async def dequeue(self) -> Task | None:
        """
        Pop the highest-priority task (FIFO within same priority).
        Returns None if queue is empty.
        """
        pass

    @abstractmethod
    async def peek(self) -> Task | None:
        """Return the next task without removing it."""
        pass

    @abstractmethod
    async def cancel(self, task_id: str, tenant_id: str | None = None) -> bool:
        """Cancel a task by ID (remove from queue if pending)."""
        pass

    @abstractmethod
    async def get_task(self, task_id: str, tenant_id: str | None = None) -> Task | None:
        """Retrieve a task by ID from the queue store."""
        pass

    @abstractmethod
    async def list_tasks(
        self,
        filters: TaskFilterOptions | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task]:
        """List tasks with filtering and pagination."""
        pass

    @abstractmethod
    async def get_stats(self, tenant_id: str | None = None) -> TaskStats:
        """Get aggregated statistics for tasks."""
        pass

    @abstractmethod
    async def size(self) -> int:
        """Return the number of pending tasks in the queue."""
        pass

    @abstractmethod
    async def update_task(self, task: Task) -> None:
        """Update an existing task in the store (status, result, error)."""
        pass
