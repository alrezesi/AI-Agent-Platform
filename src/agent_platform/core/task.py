
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
    message_id: str | None = None        # queue message id (Redis zset member / durable id)
    execution_id: str | None = None      # unique per execution attempt
    lease_owner: str | None = None       # worker that currently owns the lease
    lease_expires_at: datetime | None = None  # when the lease expires
    error_category: str | None = None    # category of last failure (e.g. transient, lease_expired)
    retry_history: list[dict[str, object]] | None = None  # structured audit of every retry
    version: int = 0  # optimistic locking version

    def add_retry_entry(
        self,
        *,
        retry_number: int,
        worker_id: str | None,
        execution_id: str | None,
        previous_state: str,
        error_category: str | None,
        reason: str,
        lease_expired: bool = False,
        next_retry_decision: str = "requeue",
        final_outcome: str | None = None,
        timestamp: datetime | None = None,
    ) -> dict[str, object]:
        """
        Append a structured, operator-readable audit entry describing one retry.

        This is the single source of truth that lets an operator answer
        "why did task T retry N times?" without reading source code.
        """
        entry: dict[str, object] = {
            "retry_number": retry_number,
            "timestamp": (timestamp or datetime.now(UTC)).isoformat(),
            "worker_id": worker_id,
            "execution_id": execution_id,
            "previous_state": previous_state,
            "error_category": error_category,
            "reason": reason,
            "lease_expired": lease_expired,
            "next_retry_decision": next_retry_decision,
            "final_outcome": final_outcome,
        }
        if self.retry_history is None:
            self.retry_history = []
        self.retry_history.append(entry)
        return entry

    def record_failure(
        self,
        *,
        worker_id: str | None,
        execution_id: str | None,
        error_category: str,
        reason: str,
        lease_expired: bool = False,
    ) -> dict[str, object]:
        """
        Record an explicit failure as a retry attempt (used by workers before
        re-enqueue). Returns the appended entry.
        """
        final_outcome = None
        next_decision = "requeue"
        if self.retry_count >= self.max_retries:
            next_decision = "max_retries_exceeded"
            final_outcome = "failed"
        self.error = reason if self.error is None else self.error
        self.error_category = error_category
        return self.add_retry_entry(
            retry_number=self.retry_count + 1,
            worker_id=worker_id,
            execution_id=execution_id,
            previous_state=self.status.value if hasattr(self.status, "value") else str(self.status),
            error_category=error_category,
            reason=reason,
            lease_expired=lease_expired,
            next_retry_decision=next_decision,
            final_outcome=final_outcome,
        )
