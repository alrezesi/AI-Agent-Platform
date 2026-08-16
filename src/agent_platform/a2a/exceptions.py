
# Custom exceptions for A2A communication

from src.agent_platform.core.exceptions import AgentPlatformError


class A2AError(AgentPlatformError):
    """Base exception for A2A communication errors."""
    pass


class HandoverError(A2AError):
    """Raised when handover fails."""
    pass


class DelegationError(A2AError):
    """Raised when delegation fails."""
    pass


class RoutingError(A2AError):
    """Raised when routing fails."""
    pass


class CollaborationError(A2AError):
    """Raised when collaboration pattern execution fails."""
    pass
