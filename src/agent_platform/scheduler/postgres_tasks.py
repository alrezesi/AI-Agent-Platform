# PostgreSQL task state persistence for the scheduler.

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import DateTime, Index, Integer, JSON, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.agent_platform.core.task import Task, TaskPriority, TaskStatus


class Base(DeclarativeBase):
    pass


class TaskORM(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        Index("idx_tasks_status_created", "status", "created_at"),
        Index("idx_tasks_agent_status", "agent_id", "status"),
    )

    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    agent_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    task_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    result: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    tenant_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    lease_owner: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    @classmethod
    def from_task(cls, task: Task) -> "TaskORM":
        return cls(
            task_id=task.task_id,
            agent_id=task.agent_id,
            task_type=task.type,
            payload=task.payload,
            priority=int(task.priority.value),
            status=task.status.value,
            created_at=task.created_at,
            started_at=task.started_at,
            completed_at=task.completed_at,
            result=_normalize_json(task.result),
            error=task.error,
            retry_count=task.retry_count,
            max_retries=task.max_retries,
            timeout_seconds=task.timeout_seconds,
            tenant_id=task.tenant_id,
        )

    def to_task(self) -> Task:
        return Task(
            task_id=self.task_id,
            agent_id=self.agent_id,
            type=self.task_type,
            payload=self.payload or {},
            priority=TaskPriority(self.priority),
            status=TaskStatus(self.status),
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            result=self.result,
            error=self.error,
            retry_count=self.retry_count,
            max_retries=self.max_retries,
            timeout_seconds=self.timeout_seconds,
            tenant_id=self.tenant_id,
        )


def _normalize_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, str, int, float, bool)):
        return value
    try:
        return json.loads(json.dumps(value, default=str))
    except Exception:
        return str(value)
