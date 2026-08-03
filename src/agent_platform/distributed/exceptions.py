
# Custom exceptions for distributed execution

from src.agent_platform.core.exceptions import AgentPlatformError


class DistributedError(AgentPlatformError):
    """Base exception for distributed execution errors."""
    pass


class NodeError(DistributedError):
    """Raised when node operations fail."""
    pass


class WorkerError(DistributedError):
    """Raised when worker node operations fail."""
    pass


class LockError(DistributedError):
    """Raised when distributed lock operations fail."""
    pass


class RegistryError(DistributedError):
    """Raised when distributed registry operations fail."""
    pass