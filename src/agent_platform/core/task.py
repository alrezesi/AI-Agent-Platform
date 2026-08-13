
# Task models for scheduling and execution

from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timezone


class TaskStatus(str, Enum):
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
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: TaskPriority = TaskPriority.MEDIUM
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    timeout_seconds: int = 30
    retry_count: int = 0
    max_retries: int = 3
    tenant_id: Optional[str] = None
