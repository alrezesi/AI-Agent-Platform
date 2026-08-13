# src/agent_platform/multi_tenant/quota.py
# Quota management for tenants

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from .models import TenantResourceUsage
from .exceptions import TenantQuotaExceededError
from .manager import TenantManager

logger = logging.getLogger(__name__)


class QuotaChecker:
    """
    Checks and enforces tenant quotas.
    """

    def __init__(self, manager: TenantManager):
        self.manager = manager
        self._usage: dict[str, TenantResourceUsage] = {}
        self._message_counts: dict[str, list[datetime]] = {}
        self._lock = asyncio.Lock()

    async def check_agent_quota(self, tenant_id: str, current_count: int) -> bool:
        """Check if the tenant can create more agents."""
        tenant = await self.manager.get_tenant(tenant_id)
        if not tenant:
            return False
        if current_count >= tenant.quota.max_agents:
            raise TenantQuotaExceededError(
                f"Tenant {tenant_id} has reached max agents limit "
                f"({tenant.quota.max_agents})"
            )
        return True

    async def check_task_quota(self, tenant_id: str, current_running: int) -> bool:
        """Check if the tenant can submit more tasks."""
        tenant = await self.manager.get_tenant(tenant_id)
        if not tenant:
            return False
        if current_running >= tenant.quota.max_concurrent_tasks:
            raise TenantQuotaExceededError(
                f"Tenant {tenant_id} has reached max concurrent tasks limit "
                f"({tenant.quota.max_concurrent_tasks})"
            )
        return True

    async def check_message_quota(self, tenant_id: str) -> bool:
        """Check message rate limit for a tenant."""
        tenant = await self.manager.get_tenant(tenant_id)
        if not tenant:
            return False

        async with self._lock:
            now = datetime.now(timezone.utc)
            if tenant_id not in self._message_counts:
                self._message_counts[tenant_id] = []
            # Clean old entries (older than 1 second)
            self._message_counts[tenant_id] = [
                ts for ts in self._message_counts[tenant_id]
                if (now - ts).total_seconds() < 1.0
            ]

            if len(self._message_counts[tenant_id]) >= tenant.quota.max_messages_per_second:
                raise TenantQuotaExceededError(
                    f"Tenant {tenant_id} has exceeded message rate limit "
                    f"({tenant.quota.max_messages_per_second}/s)"
                )

            self._message_counts[tenant_id].append(now)
            return True

    async def check_workflow_quota(self, tenant_id: str, current_count: int) -> bool:
        """Check if the tenant can create more workflows."""
        tenant = await self.manager.get_tenant(tenant_id)
        if not tenant:
            return False
        if current_count >= tenant.quota.max_workflows:
            raise TenantQuotaExceededError(
                f"Tenant {tenant_id} has reached max workflows limit "
                f"({tenant.quota.max_workflows})"
            )
        return True

    async def record_usage(self, tenant_id: str, usage: TenantResourceUsage) -> None:
        """Record current resource usage for a tenant."""
        async with self._lock:
            self._usage[tenant_id] = usage

    async def get_usage(self, tenant_id: str) -> Optional[TenantResourceUsage]:
        """Get current resource usage for a tenant."""
        return self._usage.get(tenant_id)


class QuotaManager:
    """
    Manages quota enforcement across the system.
    """

    def __init__(self, tenant_manager: TenantManager, quota_checker: QuotaChecker):
        self.tenant_manager = tenant_manager
        self.quota_checker = quota_checker
        self._usage_counts: dict[str, dict[str, int]] = {}
        self._lock = asyncio.Lock()

    async def increment_agent_count(self, tenant_id: str) -> None:
        """Increment agent count for a tenant."""
        async with self._lock:
            if tenant_id not in self._usage_counts:
                self._usage_counts[tenant_id] = {}
            self._usage_counts[tenant_id]['agents'] = (
                self._usage_counts[tenant_id].get('agents', 0) + 1
            )
            usage = await self.quota_checker.get_usage(tenant_id)
            if usage is None:
                usage = TenantResourceUsage(tenant_id=tenant_id)
            usage.active_agents = self._usage_counts[tenant_id]['agents']
            await self.quota_checker.record_usage(tenant_id, usage)

    async def decrement_agent_count(self, tenant_id: str) -> None:
        """Decrement agent count for a tenant."""
        async with self._lock:
            if tenant_id in self._usage_counts:
                self._usage_counts[tenant_id]['agents'] = max(
                    0, self._usage_counts[tenant_id].get('agents', 0) - 1
                )
                usage = await self.quota_checker.get_usage(tenant_id)
                if usage:
                    usage.active_agents = self._usage_counts[tenant_id]['agents']
                    await self.quota_checker.record_usage(tenant_id, usage)

    async def get_resource_usage(self, tenant_id: str) -> dict[str, int]:
        """Get current resource usage counts."""
        return self._usage_counts.get(tenant_id, {}).copy()
