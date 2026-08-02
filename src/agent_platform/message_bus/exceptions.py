
# Message bus specific exceptions

from src.agent_platform.core.exceptions import AgentPlatformError


class MessageBusError(AgentPlatformError):
    """Base exception for message bus errors."""
    pass


class MessageDeliveryError(MessageBusError):
    """Raised when a message cannot be delivered."""
    pass


class MessageValidationError(MessageBusError):
    """Raised when a message fails validation."""
    pass


class SubscriptionError(MessageBusError):
    """Raised when subscription operations fail."""
    pass