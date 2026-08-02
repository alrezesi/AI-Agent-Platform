# Custom exceptions for the platform


class AgentPlatformError(Exception):
    """Base exception for the platform."""
    pass


class AgentNotFoundError(AgentPlatformError):
    """Raised when an agent is not found in registry."""
    pass


class AgentUnavailableError(AgentPlatformError):
    """Raised when agent is not available for task execution."""
    pass


class TaskSubmissionError(AgentPlatformError):
    """Raised when task submission fails."""
    pass


class MessageDeliveryError(AgentPlatformError):
    """Raised when message cannot be delivered."""
    pass


class WorkflowExecutionError(AgentPlatformError):
    """Raised when workflow execution fails."""
    pass