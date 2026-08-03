
# Retry policies with exponential backoff and jitter

import random
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Type, List, Optional, Callable, Awaitable, Any
from enum import Enum

from .exceptions import RetryExhaustedError

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Supported retry strategies."""
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_WITH_JITTER = "exponential_with_jitter"


class RetryPolicy(ABC):
    """
    Abstract base class for retry policies.
    """

    @abstractmethod
    def get_delay(self, attempt: int) -> float:
        """
        Get the delay in seconds for the given attempt number (0-indexed).
        """
        pass

    @abstractmethod
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """
        Determine whether to retry based on the exception and attempt count.
        """
        pass

    @property
    @abstractmethod
    def max_retries(self) -> int:
        pass


class FixedDelayRetry(RetryPolicy):
    """
    Fixed delay between retries.
    """

    def __init__(self, delay: float, max_retries: int, retryable_exceptions: Optional[List[Type[Exception]]] = None):
        self._delay = delay
        self._max_retries = max_retries
        self._retryable_exceptions = retryable_exceptions or [Exception]

    def get_delay(self, attempt: int) -> float:
        return self._delay

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        if attempt >= self._max_retries:
            return False
        return any(isinstance(exception, exc) for exc in self._retryable_exceptions)

    @property
    def max_retries(self) -> int:
        return self._max_retries


class ExponentialBackoffRetry(RetryPolicy):
    """
    Exponential backoff with optional jitter.
    delay = base * (multiplier ** attempt) + jitter
    """

    def __init__(
        self,
        base_delay: float = 1.0,
        multiplier: float = 2.0,
        max_delay: float = 60.0,
        max_retries: int = 5,
        retryable_exceptions: Optional[List[Type[Exception]]] = None,
        jitter: bool = True,
        jitter_factor: float = 0.1,
    ):
        self.base_delay = base_delay
        self.multiplier = multiplier
        self.max_delay = max_delay
        self._max_retries = max_retries
        self._retryable_exceptions = retryable_exceptions or [Exception]
        self.jitter = jitter
        self.jitter_factor = jitter_factor

    def get_delay(self, attempt: int) -> float:
        delay = self.base_delay * (self.multiplier ** attempt)
        delay = min(delay, self.max_delay)
        if self.jitter:
            # Add random jitter to spread retries
            jitter_amount = random.uniform(-self.jitter_factor * delay, self.jitter_factor * delay)
            delay = max(0, delay + jitter_amount)
        return delay

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        if attempt >= self._max_retries:
            return False
        return any(isinstance(exception, exc) for exc in self._retryable_exceptions)

    @property
    def max_retries(self) -> int:
        return self._max_retries


class RetryExecutor:
    """
    Executes an async function with retry logic using a retry policy.
    """

    def __init__(self, policy: RetryPolicy, on_retry: Optional[Callable[[Exception, int], Awaitable[None]]] = None):
        self.policy = policy
        self.on_retry = on_retry

    async def execute(self, func: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        """
        Execute the given async function with retries.
        """
        attempt = 0
        last_exception = None

        while True:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if not self.policy.should_retry(e, attempt):
                    logger.error(f"Retry policy exhausted or non-retryable error: {e}")
                    raise RetryExhaustedError(f"All retries exhausted: {e}") from e

                delay = self.policy.get_delay(attempt)
                logger.warning(f"Retry attempt {attempt+1}/{self.policy.max_retries} after {delay:.2f}s due to: {e}")
                if self.on_retry:
                    await self.on_retry(e, attempt + 1)
                await asyncio.sleep(delay)
                attempt += 1