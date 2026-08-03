
# Tenant authentication utilities

import hashlib
import hmac
import secrets
from typing import Optional, Dict, Any

from .manager import TenantManager
from .exceptions import TenantAuthenticationError


class TenantAuthenticator:
    """
    Authenticates requests using API keys and tenant IDs.
    """

    def __init__(self, tenant_manager: TenantManager):
        self.tenant_manager = tenant_manager

    async def authenticate_api_key(self, api_key: str) -> Optional[str]:
        """
        Authenticate using API key.
        Returns tenant_id if valid, None otherwise.
        """
        tenants = await self.tenant_manager.list_tenants(limit=1000)
        for tenant in tenants:
            if tenant.has_api_key(api_key) and tenant.is_active():
                return tenant.tenant_id
        return None

    async def authenticate_tenant(self, tenant_id: str, api_key: Optional[str] = None) -> bool:
        """
        Authenticate a tenant with optional API key.
        """
        tenant = await self.tenant_manager.get_tenant(tenant_id)
        if not tenant or not tenant.is_active():
            return False
        if api_key and not tenant.has_api_key(api_key):
            return False
        return True

    def generate_api_key(self) -> str:
        """Generate a secure API key."""
        return f"tk-{secrets.token_hex(24)}"

    def verify_signature(self, payload: Dict[str, Any], signature: str, secret: str) -> bool:
        """
        Verify HMAC signature for webhook/notification authentication.
        """
        # Sort keys and create a canonical string
        sorted_payload = "&".join(f"{k}={v}" for k, v in sorted(payload.items()))
        expected = hmac.new(
            secret.encode(),
            sorted_payload.encode(),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)