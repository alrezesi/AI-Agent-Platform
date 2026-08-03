
# Custom exceptions for multi-tenant system

from src.agent_platform.core.exceptions import AgentPlatformError


class TenantError(AgentPlatformError):
    """Base exception for tenant-related errors."""
    pass


class TenantNotFoundError(TenantError):
    """Raised when a tenant is not found."""
    pass


class TenantInactiveError(TenantError):
    """Raised when a tenant is inactive/suspended."""
    pass


class TenantQuotaExceededError(TenantError):
    """Raised when a tenant exceeds its resource quota."""
    pass


class TenantAuthenticationError(TenantError):
    """Raised when tenant authentication fails."""
    pass