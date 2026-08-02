
# Custom exceptions for tool calling

from src.agent_platform.core.exceptions import AgentPlatformError


class ToolError(AgentPlatformError):
    """Base exception for tool-related errors."""
    pass


class ToolNotFoundError(ToolError):
    """Raised when a requested tool is not found."""
    pass


class ToolValidationError(ToolError):
    """Raised when tool parameters fail validation."""
    pass


class ToolExecutionError(ToolError):
    """Raised when tool execution fails."""
    pass