
# Scheduling-specific models: filter options, statistics, and queue items

from typing import Optional, List
from pydantic import BaseModel, Field
from src.agent_platform.core.task import TaskStatus, TaskPriority


class TaskFilterOptions(BaseModel):
    """Filtering options for listing tasks."""
    agent_id: Optional[str] = Field(None, description="Filter by target agent ID")
    status: Optional[TaskStatus] = Field(None, description="Filter by task status")
    priority: Optional[TaskPriority] = Field(None, description="Filter by priority")
    tenant_id: Optional[str] = Field(None, description="Multi-tenant isolation")
    from_date: Optional[str] = Field(None, description="ISO datetime filter (created_at >=)")
    to_date: Optional[str] = Field(None, description="ISO datetime filter (created_at <=)")


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