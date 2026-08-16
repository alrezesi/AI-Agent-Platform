
# Example plugin: logs messages and task events

import logging
from typing import Any

from src.agent_platform.plugins.base import Plugin, PluginContext

logger = logging.getLogger(__name__)


class LoggerPlugin(Plugin):
    """
    A simple plugin that logs events using Python's logging.
    Demonstrates hook registration and event handling.
    """

    def __init__(self, plugin_id: str, name: str, version: str = "1.0.0"):
        super().__init__(plugin_id, name, version)
        self._enabled = True

    async def on_load(self, context: PluginContext) -> None:
        """Register hooks and set up logging."""
        self.context = context
        # Register a hook for agent run events
        # We need access to the plugin manager; we'll get it from context
        # For simplicity, we'll just log that we loaded
        logger.info(f"LoggerPlugin {self.plugin_id} loaded with config: {context.config}")
        # We could store a reference to the plugin manager to register hooks,
        # but we'll do that via the manager's register_hook method.
        # The manager will call this plugin's methods directly.

    async def on_unload(self) -> None:
        """Clean up."""
        logger.info(f"LoggerPlugin {self.plugin_id} unloaded")

    async def on_event(self, event_type: str, data: dict[str, Any]) -> None:
        """Handle arbitrary events."""
        logger.info(f"LoggerPlugin received event {event_type}: {data}")

    # Hook handlers - these will be registered by the plugin manager
    async def on_pre_agent_run(self, task_data: dict[str, Any]) -> None:
        """Hook: before agent runs a task."""
        logger.info(f"[Plugin] Pre-agent run: task_id={task_data.get('task_id')}")

    async def on_post_agent_run(self, result: Any) -> None:
        """Hook: after agent runs a task."""
        logger.info(f"[Plugin] Post-agent run: result={result}")

    async def on_message(self, message: dict[str, Any]) -> None:
        """Hook: on message."""
        logger.info(f"[Plugin] Message: {message.get('type')} from {message.get('from_agent')}")
