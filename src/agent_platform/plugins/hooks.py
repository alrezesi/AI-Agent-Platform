
# Hook system for plugins

from enum import Enum, auto
from typing import Dict, List, Callable, Any, Awaitable
import asyncio
import logging

from .exceptions import PluginHookError

logger = logging.getLogger(__name__)

# Type alias for hook handler function
HookHandler = Callable[[Any], Awaitable[Any]]


class HookPoint(Enum):
    """
    Enumeration of all available hook points in the platform.
    """
    PRE_AGENT_RUN = auto()      # Before an agent runs a task
    POST_AGENT_RUN = auto()     # After an agent runs a task
    ON_MESSAGE = auto()         # When a message is sent/received
    ON_TASK_COMPLETE = auto()   # When a task completes (success/failure)
    ON_AGENT_REGISTER = auto()  # When an agent registers
    ON_AGENT_UNREGISTER = auto() # When an agent unregisters
    ON_SCHEDULER_EVENT = auto() # Scheduler events (submit, cancel, etc.)


class HookRegistry:
    """
    Registry that manages hook handlers for different hook points.
    Plugins can register handlers for specific hook points.
    """

    def __init__(self):
        # hook_point -> list of (plugin_id, handler) tuples
        self._handlers: Dict[HookPoint, List[tuple]] = {}
        self._lock = asyncio.Lock()

    def register(
        self,
        hook_point: HookPoint,
        handler: HookHandler,
        plugin_id: str,
    ) -> None:
        """
        Register a handler for a hook point.
        """
        if hook_point not in self._handlers:
            self._handlers[hook_point] = []
        self._handlers[hook_point].append((plugin_id, handler))
        logger.debug(f"Handler registered for {hook_point} by plugin {plugin_id}")

    def unregister_all(self, plugin_id: str) -> None:
        """
        Remove all handlers belonging to a plugin.
        """
        for hook_point in list(self._handlers.keys()):
            self._handlers[hook_point] = [
                (pid, h) for pid, h in self._handlers[hook_point]
                if pid != plugin_id
            ]
        logger.debug(f"All handlers unregistered for plugin {plugin_id}")

    async def execute(
        self,
        hook_point: HookPoint,
        *args,
        **kwargs,
    ) -> List[Any]:
        """
        Execute all handlers registered for a hook point.
        Returns a list of results from each handler.
        """
        handlers = self._handlers.get(hook_point, [])
        if not handlers:
            return []

        results = []
        for plugin_id, handler in handlers:
            try:
                result = await handler(*args, **kwargs)
                results.append(result)
            except Exception as e:
                logger.error(f"Error in hook handler for {hook_point} from {plugin_id}: {e}")
                # Optionally raise or continue
                raise PluginHookError(f"Handler from {plugin_id} failed: {e}") from e
        return results

    def clear(self) -> None:
        """Clear all registered handlers."""
        self._handlers.clear()