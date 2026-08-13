# Tenant manager for creating, updating, and deleting tenants

import asyncio
import logging
import secrets
import uuid
from datetime import datetime, timezone
from typing import Optional, Any

from .models import Tenant, TenantStatus, TenantQuota
from .exceptions import TenantNotFoundError
from src.agent_platform.security import (
    api_key_record_matches,
    hash_api_key,
    stored_api_key_hash,
)

logger = logging.getLogger(__name__)


class TenantManager:
    """
    Manages tenant lifecycle: creation, updates, deletion, and status changes.
    """

    def __init__(self, storage):
        """
        Initialize the tenant manager.
        storage: A storage backend (in-memory, Redis, PostgreSQL) for tenant data.
        """
        self.storage = storage
        self._lock = asyncio.Lock()
        self._api_key_index: dict[str, str] = {}
        self._rebuild_api_key_index()

    async def create_tenant(
        self,
        name: str,
        description: Optional[str] = None,
        quota: Optional[TenantQuota] = None,
        config: Optional[dict[str, Any]] = None,
    ) -> Tenant:
        """Create a new tenant."""
        tenant_id = f"tenant-{uuid.uuid4().hex[:8]}"
        tenant = Tenant(
            tenant_id=tenant_id,
            name=name,
            description=description,
            quota=quota or TenantQuota(),
            config=config or {},
        )
        async with self._lock:
            await self._save_tenant(tenant)
        logger.info(f"Tenant {tenant_id} ({name}) created")
        return tenant

    async def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Get a tenant by ID."""
        return await self._load_tenant(tenant_id)

    async def get_tenant_or_raise(self, tenant_id: str) -> Tenant:
        """Get a tenant or raise TenantNotFoundError."""
        tenant = await self.get_tenant(tenant_id)
        if not tenant:
            raise TenantNotFoundError(f"Tenant {tenant_id} not found")
        return tenant

    async def update_tenant(self, tenant_id: str, updates: dict[str, Any]) -> Tenant:
        """Update a tenant."""
        tenant = await self.get_tenant_or_raise(tenant_id)
        for key, value in updates.items():
            if hasattr(tenant, key):
                setattr(tenant, key, value)
        tenant.updated_at = datetime.now(timezone.utc)
        await self._save_tenant(tenant)
        logger.info(f"Tenant {tenant_id} updated")
        return tenant

    async def delete_tenant(self, tenant_id: str) -> bool:
        """Soft-delete a tenant."""
        tenant = await self.get_tenant_or_raise(tenant_id)
        await self._remove_tenant_keys(tenant)
        tenant.status = TenantStatus.DELETED
        tenant.updated_at = datetime.now(timezone.utc)
        await self._save_tenant(tenant)
        logger.info(f"Tenant {tenant_id} deleted (soft)")
        return True

    async def suspend_tenant(self, tenant_id: str) -> bool:
        """Suspend a tenant."""
        tenant = await self.get_tenant_or_raise(tenant_id)
        tenant.status = TenantStatus.SUSPENDED
        tenant.updated_at = datetime.now(timezone.utc)
        await self._save_tenant(tenant)
        logger.info(f"Tenant {tenant_id} suspended")
        return True

    async def activate_tenant(self, tenant_id: str) -> bool:
        """Activate a tenant."""
        tenant = await self.get_tenant_or_raise(tenant_id)
        tenant.status = TenantStatus.ACTIVE
        tenant.updated_at = datetime.now(timezone.utc)
        await self._save_tenant(tenant)
        logger.info(f"Tenant {tenant_id} activated")
        return True

    async def list_tenants(
        self,
        status: Optional[TenantStatus] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Tenant]:
        """List tenants with optional filtering."""
        return await self._list_tenants(status, limit, offset)

    async def check_tenant_active(self, tenant_id: str) -> bool:
        """Check if a tenant is active."""
        tenant = await self.get_tenant(tenant_id)
        if not tenant:
            return False
        return tenant.is_active()

    async def _save_tenant(self, tenant: Tenant) -> None:
        """Save tenant to storage and refresh the API key index."""
        if not hasattr(self.storage, "_tenants"):
            self.storage._tenants = {}
        previous = self.storage._tenants.get(tenant.tenant_id)
        if previous is not None:
            self._unindex_tenant_keys(previous)
        self.storage._tenants[tenant.tenant_id] = tenant
        if tenant.is_active():
            self._index_tenant_keys(tenant)

    async def _load_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Load tenant from storage."""
        if not hasattr(self.storage, "_tenants"):
            self.storage._tenants = {}
        return self.storage._tenants.get(tenant_id)

    async def _list_tenants(
        self,
        status: Optional[TenantStatus] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Tenant]:
        """List tenants from storage."""
        if not hasattr(self.storage, "_tenants"):
            self.storage._tenants = {}
        tenants = list(self.storage._tenants.values())
        if status:
            tenants = [t for t in tenants if t.status == status]
        return tenants[offset : offset + limit]

    async def generate_api_key(self, tenant_id: str) -> str:
        """Generate a new API key for a tenant. Only the hash is stored."""
        tenant = await self.get_tenant_or_raise(tenant_id)
        api_key = f"tk-{secrets.token_hex(24)}"
        tenant.api_keys.append(
            {
                "key_hash": hash_api_key(api_key),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "is_active": True,
            }
        )
        await self._save_tenant(tenant)
        return api_key

    async def revoke_api_key(self, tenant_id: str, api_key: str) -> bool:
        """Revoke an API key for a tenant."""
        tenant = await self.get_tenant_or_raise(tenant_id)
        for key in tenant.api_keys:
            if not api_key_record_matches(key, api_key):
                continue
            key["is_active"] = False
            await self._save_tenant(tenant)
            return True
        return False

    async def authenticate_api_key(self, api_key: str) -> Optional[Tenant]:
        """Resolve a tenant from an API key via the hash index."""
        tenant_id = self._api_key_index.get(hash_api_key(api_key))
        if not tenant_id:
            return None
        tenant = await self.get_tenant(tenant_id)
        if tenant and tenant.is_active() and tenant.has_api_key(api_key):
            return tenant
        return None

    async def get_tenant_by_api_key(self, api_key: str) -> Optional[Tenant]:
        """Public helper used by authentication and middleware."""
        return await self.authenticate_api_key(api_key)

    def _index_tenant_keys(self, tenant: Tenant) -> None:
        for key in tenant.api_keys:
            if not key.get("is_active", True):
                continue
            stored_hash = stored_api_key_hash(key)
            if stored_hash:
                self._api_key_index[stored_hash] = tenant.tenant_id

    def _unindex_tenant_keys(self, tenant: Tenant) -> None:
        for key in tenant.api_keys:
            stored_hash = stored_api_key_hash(key)
            if stored_hash and self._api_key_index.get(stored_hash) == tenant.tenant_id:
                self._api_key_index.pop(stored_hash, None)

    def _rebuild_api_key_index(self) -> None:
        self._api_key_index.clear()
        tenants = getattr(self.storage, "_tenants", None) or {}
        for tenant in tenants.values():
            self._index_tenant_keys(tenant)

    async def _remove_tenant_keys(self, tenant: Tenant) -> None:
        self._unindex_tenant_keys(tenant)
