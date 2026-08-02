
# Task models for scheduling and execution

from __future__ import annotations

from enum import Enum
from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskPriority(int, Enum):
    LOW = 0
    MEDIUM = 1
    HIGH = 2
    CRITICAL = 3


class Task(BaseModel):
    task_id: str = Field(..., description="Unique task ID")
    agent_id: str = Field(..., description="Target agent ID")
    type: str = Field(..., description="Task type (e.g., 'inference', 'tool_call')")
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: TaskPriority = TaskPriority.MEDIUM
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: Any | None = None
    error: str | None = None
    timeout_seconds: int = 30
    retry_count: int = 0
    max_retries: int = 3
    tenant_id: str | None = None

