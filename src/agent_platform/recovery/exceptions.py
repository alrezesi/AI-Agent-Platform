# src/agent_platform/recovery/exceptions.py
# Custom exceptions for recovery components

from src.agent_platform.core.exceptions import AgentPlatformError


class RecoveryError(AgentPlatformError):
    """Base exception for recovery-related errors."""
    pass


class RetryExhaustedError(RecoveryError):
    """Raised when all retry attempts have been exhausted."""
    pass


class CircuitOpenError(RecoveryError):
    """Raised when a circuit breaker is open and rejects a request."""
    pass


class DeadLetterError(RecoveryError):
    """Raised when dead letter queue operations fail."""
    pass


class CheckpointError(RecoveryError):
    """Raised when checkpoint operations fail."""
    pass


class IdempotencyError(RecoveryError):
    """Raised when idempotency enforcement fails."""
    pass
