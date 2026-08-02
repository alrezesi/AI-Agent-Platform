
# Engine-specific exceptions

from src.agent_platform.core.exceptions import AgentPlatformError


class AgentEngineError(AgentPlatformError):
    """Base exception for engine errors."""
    pass


class AgentInitializationError(AgentEngineError):
    """Raised when an agent fails to initialize."""
    pass


class AgentNotReadyError(AgentEngineError):
    """Raised when an agent is not ready to process tasks."""
    pass


class TaskExecutionError(AgentEngineError):
    """Raised when a task execution fails."""
    pass