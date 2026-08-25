
# Scheduling-specific models: filter options, statistics, and queue items

from pydantic import BaseModel, Field

from src.agent_platform.core.task import TaskPriority, TaskStatus


class TaskFilterOptions(BaseModel):
    """Filtering options for listing tasks."""
    agent_id: str | None = Field(None, description="Filter by target agent ID")
    status: TaskStatus | None = Field(None, description="Filter by task status")
    priority: TaskPriority | None = Field(None, description="Filter by priority")
    tenant_id: str | None = Field(None, description="Multi-tenant isolation")
    request_id: str | None = Field(None, description="Trace request ID correlation")
    from_date: str | None = Field(None, description="ISO datetime filter (created_at >=)")
    to_date: str | None = Field(None, description="ISO datetime filter (created_at <=)")


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
