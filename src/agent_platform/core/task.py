
# Task models for scheduling and execution

from datetime import UTC, datetime
from enum import Enum, StrEnum
from typing import Any

from pydantic import BaseModel, Field


class TaskStatus(StrEnum):
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskPriority(int, Enum):
    # Lower number = higher priority
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3


class Task(BaseModel):
    task_id: str = Field(..., description="Unique task ID")
    agent_id: str = Field(..., description="Target agent ID")
    type: str = Field(..., description="Task type (e.g., 'inference', 'tool_call')")
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: TaskPriority = TaskPriority.MEDIUM
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: Any | None = None
    error: str | None = None
    timeout_seconds: int = 30
    retry_count: int = 0
    max_retries: int = 3
    tenant_id: str | None = None
    # --- Observability / trace-correlation fields ---
    request_id: str | None = None        # HTTP request ID (from X-Request-ID)
    execution_id: str | None = None      # unique per execution attempt
    lease_owner: str | None = None       # worker that currently owns the lease
    lease_expires_at: datetime | None = None  # when the lease expires
