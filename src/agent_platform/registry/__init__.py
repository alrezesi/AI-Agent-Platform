
# Registry package exports

from .base import BaseAgentRegistry
from .in_memory import InMemoryAgentRegistry

try:
    from .redis_registry import RedisAgentRegistry
except ImportError:  # pragma: no cover - optional dependency
    RedisAgentRegistry = None  # type: ignore[misc,assignment]

try:
    from .postgres_registry import PostgresAgentRegistry
except ImportError:  # pragma: no cover - optional dependency
    PostgresAgentRegistry = None  # type: ignore[misc,assignment]

__all__ = [
    "BaseAgentRegistry",
    "InMemoryAgentRegistry",
    "RedisAgentRegistry",
    "PostgresAgentRegistry",
]
