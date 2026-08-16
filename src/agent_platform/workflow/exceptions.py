
# Custom exceptions for workflow engine

from src.agent_platform.core.exceptions import AgentPlatformError


class WorkflowError(AgentPlatformError):
    """Base exception for workflow-related errors."""
    pass


class WorkflowNotFoundError(WorkflowError):
    """Raised when a workflow is not found."""
    pass


class WorkflowExecutionError(WorkflowError):
    """Raised when workflow execution fails."""
    pass


class WorkflowStepError(WorkflowError):
    """Raised when a specific step fails."""
    pass


class WorkflowDefinitionError(WorkflowError):
    """Raised when workflow definition is invalid."""
    pass
