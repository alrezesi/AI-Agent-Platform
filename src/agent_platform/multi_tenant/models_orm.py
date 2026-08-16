# src/agent_platform/multi_tenant/models_orm.py
# SQLAlchemy ORM models for multi-tenant tables.

from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import String, DateTime, Boolean, JSON, Index
from sqlalchemy.orm import Mapped, mapped_column
from src.agent_platform.scheduler.postgres_tasks import Base


class TenantORM(Base):
    __tablename__ = "tenants"
    __table_args__ = (
        Index("idx_tenants_status", "status"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(500))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    config: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    quota: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    extra_metadata: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


class ApiKeyORM(Base):
    __tablename__ = "api_keys"
    __table_args__ = (
        Index("idx_apikeys_tenant", "tenant_id"),
        Index("idx_apikeys_status", "status"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)