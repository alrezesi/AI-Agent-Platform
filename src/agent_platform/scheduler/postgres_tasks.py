
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import JSON, DateTime, Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.agent_platform.core.task import Task, TaskPriority, TaskStatus

# postgres_tasks.py
from src.agent_platform.registry.postgres_registry import utcnow_naive


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
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow_naive)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(String, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    tenant_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    lease_owner: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    request_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    execution_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)

    @classmethod
    def from_task(cls, task: Task) -> TaskORM:
        return cls(
            task_id=task.task_id,
            agent_id=task.agent_id,
            task_type=task.type,
            payload=task.payload,
            priority=int(task.priority.value),
            status=task.status.value,
            created_at=_to_naive_utc(task.created_at) or datetime.now(UTC).replace(tzinfo=None),
            started_at=_to_naive_utc(task.started_at),
            completed_at=_to_naive_utc(task.completed_at),
            result=_normalize_json(task.result),
            error=task.error,
            retry_count=task.retry_count,
            max_retries=task.max_retries,
            timeout_seconds=task.timeout_seconds,
            tenant_id=task.tenant_id,
            lease_owner=getattr(task, "lease_owner", None),
            lease_expires_at=_to_naive_utc(getattr(task, "lease_expires_at", None)),
            request_id=getattr(task, "request_id", None),
            execution_id=getattr(task, "execution_id", None),
        )

    def to_task(self) -> Task:
        created_at = self.created_at or datetime.now(UTC).replace(tzinfo=UTC)
        return Task(
            task_id=self.task_id,
            agent_id=self.agent_id,
            type=self.task_type,
            payload=self.payload or {},
            priority=TaskPriority(self.priority),
            status=TaskStatus(self.status),
            created_at=cast(datetime, _to_aware_utc(created_at)),
            started_at=_to_aware_utc(self.started_at),
            completed_at=_to_aware_utc(self.completed_at),
            result=self.result,
            error=self.error,
            retry_count=self.retry_count,
            max_retries=self.max_retries,
            timeout_seconds=self.timeout_seconds,
            tenant_id=self.tenant_id,
            lease_owner=self.lease_owner,
            lease_expires_at=_to_aware_utc(self.lease_expires_at),
            request_id=self.request_id,
            execution_id=self.execution_id,
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


def _to_naive_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _to_aware_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
