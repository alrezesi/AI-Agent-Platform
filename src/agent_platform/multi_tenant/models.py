# src/agent_platform/multi_tenant/models.py
# Multi-tenant models: Tenant, TenantStatus, TenantQuota

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from src.agent_platform.security import api_key_record_matches


class TenantStatus(StrEnum):
    """Status of a tenant."""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    INACTIVE = "inactive"
    DELETED = "deleted"


class TenantQuota(BaseModel):
    """Resource quotas for a tenant."""
    max_agents: int = 10
    max_concurrent_tasks: int = 100
    max_messages_per_second: int = 1000
    max_storage_mb: int = 1024
    max_workflows: int = 50


class Tenant(BaseModel):
    """
    Tenant model representing an organization or user in the multi-tenant system.
    """
    tenant_id: str = Field(..., description="Unique tenant identifier")
    name: str = Field(..., description="Tenant name")
    description: str | None = None
    status: TenantStatus = TenantStatus.ACTIVE
    quota: TenantQuota = Field(default_factory=lambda: TenantQuota())
    config: dict[str, Any] = Field(default_factory=dict)
    api_keys: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    def is_active(self) -> bool:
        """Check if tenant is active."""
        return self.status == TenantStatus.ACTIVE

    def has_api_key(self, api_key: str) -> bool:
        """Check if the given API key is valid for this tenant."""
        return any(api_key_record_matches(key, api_key) for key in self.api_keys)

class TenantResourceUsage(BaseModel):
    tenant_id: str
    active_agents: int = 0
    pending_tasks: int = 0
    running_tasks: int = 0
    messages_per_second: float = 0.0
    storage_used_mb: float = 0.0
    active_workflows: int = 0
    last_updated: datetime = Field(default_factory=lambda: datetime.now(UTC))
