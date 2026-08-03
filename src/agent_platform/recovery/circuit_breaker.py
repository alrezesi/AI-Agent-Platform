import time
import asyncio
import logging
from enum import Enum
from typing import Optional, Callable, Awaitable, Any
from dataclasses import dataclass

from .exceptions import CircuitOpenError

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """States of a circuit breaker."""
    CLOSED = "closed"        # Normal operation, requests allowed
    OPEN = "open"            # Circuit open, requests blocked
    HALF_OPEN = "half_open"  # Testing if service is recovered


@dataclass
class CircuitBreakerConfig:
    """
    Configuration for a circuit breaker.
    """
    failure_threshold: int = 5          # Number of failures to open circuit
    success_threshold: int = 3          # Number of successes to close circuit in half-open
    timeout_seconds: float = 30.0       # Time to wait before half-open
    monitored_exceptions: tuple = (Exception,)  # Exceptions that count as failures


class CircuitBreaker:
    """
    Circuit breaker that protects a service/function from repeated failures.
    """

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    async def call(self, func: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        """
        Execute the given function with circuit breaker protection.
        """
        if not await self._allow_request():
            raise CircuitOpenError(f"Circuit breaker '{self.name}' is open")

        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            # Only count monitored exceptions
            if isinstance(e, self.config.monitored_exceptions):
                await self._record_failure()
            # Re-raise the original exception (unless circuit open, but we already check)
            raise

    async def _allow_request(self) -> bool:
        """Check if a request should be allowed."""
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True
            elif self._state == CircuitState.OPEN:
                # Check if timeout has elapsed
                if time.time() - self._last_failure_time >= self.config.timeout_seconds:
                    # Transition to half-open
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    logger.info(f"Circuit '{self.name}' transitioning to HALF_OPEN")
                    return True
                else:
                    return False
            else:  # HALF_OPEN
                return True

    async def _record_failure(self) -> None:
        """Record a failure and potentially open the circuit."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitState.CLOSED and self._failure_count >= self.config.failure_threshold:
                self._state = CircuitState.OPEN
                logger.warning(f"Circuit '{self.name}' opened after {self._failure_count} failures")
            elif self._state == CircuitState.HALF_OPEN:
                # A failure in half-open immediately re-opens
                self._state = CircuitState.OPEN
                self._failure_count = 0  # reset to start fresh
                logger.warning(f"Circuit '{self.name}' re-opened due to failure in half-open state")

    async def _record_success(self) -> None:
        """Record a success and potentially close the circuit."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    logger.info(f"Circuit '{self.name}' closed after {self._success_count} successes")
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = max(0, self._failure_count - 1)

    async def reset(self) -> None:  # <-- این خط اصلاح شد
        """Manually reset the circuit to closed state."""
        async with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            logger.info(f"Circuit '{self.name}' manually reset")