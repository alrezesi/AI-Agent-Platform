
# Scheduling-specific models: filter options, statistics, and queue items

from pydantic import BaseModel

from src.agent_platform.core.task import TaskPriority, TaskStatus


class TaskFilterOptions(BaseModel):
    """Filtering options for listing tasks."""
    agent_id: str | None = None
    status: TaskStatus | None = None
    priority: TaskPriority | None = None
    tenant_id: str | None = None
    request_id: str | None = None
    from_date: str | None = None
    to_date: str | None = None


class TaskStats(BaseModel):
    """Aggregated statistics about tasks."""
    total: int
    pending: int
    running: int
    completed: int
    failed: int
    cancelled: int
    timeout: int


class TaskQueueItem(BaseModel):
    """Internal representation of a task in the queue with priority."""
    task_id: str
    priority: int  # Lower number = higher priority (0=CRITICAL, 3=LOW)
    created_at: float  # Unix timestamp for FIFO ordering within same priority
