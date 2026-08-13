# src/agent_platform/multi_tenant/models.py
# Multi-tenant models: Tenant, TenantStatus, TenantQuota

from enum import Enum
from typing import Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime, timezone

from src.agent_platform.security import api_key_record_matches


class TenantStatus(str, Enum):
    """Status of a tenant."""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    INACTIVE = "inactive"
    DELETED = "deleted"


class TenantQuota(BaseModel):
    """Resource quotas for a tenant."""
    max_agents: int = Field(10, description="Maximum number of agents")
    max_concurrent_tasks: int = Field(100, description="Maximum concurrent tasks")
    max_messages_per_second: int = Field(1000, description="Maximum messages per second")
    max_storage_mb: int = Field(1024, description="Maximum storage in MB")
    max_workflows: int = Field(50, description="Maximum number of workflows")


class Tenant(BaseModel):
    """
    Tenant model representing an organization or user in the multi-tenant system.
    """
    tenant_id: str = Field(..., description="Unique tenant identifier")
    name: str = Field(..., description="Tenant name")
    description: Optional[str] = Field(None, description="Tenant description")
    status: TenantStatus = Field(TenantStatus.ACTIVE, description="Tenant status")
    quota: TenantQuota = Field(default_factory=TenantQuota, description="Resource quotas")
    config: dict[str, Any] = Field(default_factory=dict, description="Tenant-specific configuration")
    api_keys: list[dict[str, Any]] = Field(default_factory=list, description="API keys for authentication")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
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
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
