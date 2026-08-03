
# Unit tests for multi-tenant system

import pytest

from src.agent_platform.multi_tenant.models import Tenant, TenantStatus, TenantQuota
from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.multi_tenant.quota import QuotaChecker, QuotaManager
from src.agent_platform.multi_tenant.exceptions import (
    TenantNotFoundError,
    TenantQuotaExceededError,
)


@pytest.fixture
async def tenant_manager():
    class Storage:
        _tenants = {}
    return TenantManager(Storage())


@pytest.mark.asyncio
async def test_create_tenant(tenant_manager):
    tenant = await tenant_manager.create_tenant("Test Tenant", "Description")
    assert tenant.tenant_id is not None
    assert tenant.name == "Test Tenant"
    assert tenant.status == TenantStatus.ACTIVE


@pytest.mark.asyncio
async def test_get_tenant(tenant_manager):
    created = await tenant_manager.create_tenant("Test")
    retrieved = await tenant_manager.get_tenant(created.tenant_id)
    assert retrieved is not None
    assert retrieved.tenant_id == created.tenant_id


@pytest.mark.asyncio
async def test_get_tenant_not_found(tenant_manager):
    with pytest.raises(TenantNotFoundError):
        await tenant_manager.get_tenant_or_raise("nonexistent")


@pytest.mark.asyncio
async def test_suspend_activate_tenant(tenant_manager):
    tenant = await tenant_manager.create_tenant("Test")
    await tenant_manager.suspend_tenant(tenant.tenant_id)
    suspended = await tenant_manager.get_tenant(tenant.tenant_id)
    assert suspended.status == TenantStatus.SUSPENDED

    await tenant_manager.activate_tenant(tenant.tenant_id)
    activated = await tenant_manager.get_tenant(tenant.tenant_id)
    assert activated.status == TenantStatus.ACTIVE


@pytest.mark.asyncio
async def test_api_key_generation(tenant_manager):
    tenant = await tenant_manager.create_tenant("Test")
    api_key = await tenant_manager.generate_api_key(tenant.tenant_id)
    assert api_key.startswith("tk-")

    # Verify tenant has the key
    updated = await tenant_manager.get_tenant(tenant.tenant_id)
    assert any(k.get('key') == api_key for k in updated.api_keys)


@pytest.mark.asyncio
async def test_quota_checker(tenant_manager):
    quota_checker = QuotaChecker(tenant_manager)

    # Create tenant with small quota
    quota = TenantQuota(max_agents=2)
    tenant = await tenant_manager.create_tenant("Test", quota=quota)

    # Check agent quota
    assert await quota_checker.check_agent_quota(tenant.tenant_id, 1) is True
    with pytest.raises(TenantQuotaExceededError):
        await quota_checker.check_agent_quota(tenant.tenant_id, 2)

    # Check message quota
    for _ in range(5):
        assert await quota_checker.check_message_quota(tenant.tenant_id) is True


@pytest.mark.asyncio
async def test_quota_manager(tenant_manager):
    quota_checker = QuotaChecker(tenant_manager)
    quota_manager = QuotaManager(tenant_manager, quota_checker)

    tenant = await tenant_manager.create_tenant("Test")
    await quota_manager.increment_agent_count(tenant.tenant_id)
    usage = await quota_manager.get_resource_usage(tenant.tenant_id)
    assert usage.get('agents') == 1