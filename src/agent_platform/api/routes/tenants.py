
# REST API endpoints for tenant management

from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Body, Query

from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.multi_tenant.models import Tenant, TenantQuota
from src.agent_platform.multi_tenant.exceptions import TenantNotFoundError

router = APIRouter(prefix="/tenants", tags=["tenants"])


# Dependency: get tenant manager
def get_tenant_manager() -> TenantManager:
    from src.agent_platform.multi_tenant.manager import TenantManager
    class Storage:
        _tenants = {}
    return TenantManager(Storage())


@router.post("/", response_model=Tenant)
async def create_tenant(
    name: str = Body(...),
    description: Optional[str] = Body(None),
    quota: Optional[TenantQuota] = Body(None),
    config: Optional[dict] = Body(None),
    manager: TenantManager = Depends(get_tenant_manager),
):
    """Create a new tenant."""
    tenant = await manager.create_tenant(name, description, quota, config)
    return tenant


@router.get("/{tenant_id}", response_model=Tenant)
async def get_tenant(
    tenant_id: str,
    manager: TenantManager = Depends(get_tenant_manager),
):
    """Get tenant by ID."""
    tenant = await manager.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return tenant


@router.put("/{tenant_id}", response_model=Tenant)
async def update_tenant(
    tenant_id: str,
    updates: dict = Body(...),
    manager: TenantManager = Depends(get_tenant_manager),
):
    """Update a tenant."""
    try:
        tenant = await manager.update_tenant(tenant_id, updates)
        return tenant
    except TenantNotFoundError:
        raise HTTPException(status_code=404, detail="Tenant not found")


@router.delete("/{tenant_id}")
async def delete_tenant(
    tenant_id: str,
    manager: TenantManager = Depends(get_tenant_manager),
):
    """Delete (soft-delete) a tenant."""
    result = await manager.delete_tenant(tenant_id)
    if not result:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return {"status": "deleted"}


@router.post("/{tenant_id}/api-keys")
async def generate_api_key(
    tenant_id: str,
    manager: TenantManager = Depends(get_tenant_manager),
):
    """Generate a new API key for a tenant."""
    try:
        api_key = await manager.generate_api_key(tenant_id)
        return {"api_key": api_key}
    except TenantNotFoundError:
        raise HTTPException(status_code=404, detail="Tenant not found")


@router.delete("/{tenant_id}/api-keys")
async def revoke_api_key(
    tenant_id: str,
    api_key: str = Query(..., description="API key to revoke"),  # <-- Changed from Body to Query
    manager: TenantManager = Depends(get_tenant_manager),
):
    """Revoke an API key for a tenant."""
    result = await manager.revoke_api_key(tenant_id, api_key)
    if not result:
        raise HTTPException(status_code=404, detail="API key not found")
    return {"status": "revoked"}


@router.get("/")
async def list_tenants(
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    manager: TenantManager = Depends(get_tenant_manager),
):
    """List all tenants."""
    tenants = await manager.list_tenants(status, limit, offset)
    return {"tenants": tenants, "count": len(tenants), "limit": limit, "offset": offset}