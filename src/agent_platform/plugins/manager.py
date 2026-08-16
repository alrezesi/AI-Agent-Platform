# src/agent_platform/plugins/manager.py
# Plugin manager for loading, unloading, and managing plugins

import logging
from pathlib import Path
from typing import Any

from .base import Plugin, PluginContext
from .discovery import discover_plugins
from .exceptions import PluginLoadError, PluginNotFoundError, PluginUnloadError
from .hooks import HookPoint, HookRegistry

logger = logging.getLogger(__name__)


class PluginManager:
    """
    Manages the lifecycle of plugins.
    Handles loading, unloading, enabling, disabling, and hook execution.
    """

    def __init__(self, plugin_dir: Path | None = None):
        self.plugin_dir = plugin_dir
        self._plugins: dict[str, Plugin] = {}  # plugin_id -> Plugin instance
        self._hook_registry = HookRegistry()
        self._loaded = False

    async def load_all(self, context: PluginContext | None = None) -> None:
        """
        Discover and load all plugins from the plugin directory.
        """
        if self._loaded:
            logger.warning("Plugins already loaded. Skipping.")
            return

        plugin_classes = discover_plugins(self.plugin_dir)
        if not plugin_classes:
            logger.info("No plugins found to load.")
            self._loaded = True
            return

        for plugin_class in plugin_classes:
            # Instantiate plugin (requires plugin_id, name, version)
            # We'll generate a plugin_id from the class name
            plugin_id = plugin_class.__name__.lower()
            try:
                plugin = plugin_class(plugin_id, plugin_class.__name__)
                await self.load_plugin(plugin, context)
            except Exception as e:
                logger.error(f"Failed to load plugin {plugin_id}: {e}")

        self._loaded = True
        logger.info(f"Loaded {len(self._plugins)} plugins")

    async def load_plugin(
        self,
        plugin: Plugin,
        context: PluginContext | None = None,
    ) -> None:
        """
        Load a specific plugin instance.
        """
        plugin_id = plugin.plugin_id
        if plugin_id in self._plugins:
            logger.warning(f"Plugin {plugin_id} already loaded. Skipping.")
            return

        if context is None:
            context = PluginContext(plugin_id=plugin_id, config={})

        try:
            await plugin.on_load(context)
            plugin.context = context
            plugin._loaded = True
            self._plugins[plugin_id] = plugin
            logger.info(f"Plugin {plugin_id} loaded successfully")
        except Exception as e:
            raise PluginLoadError(f"Failed to load plugin {plugin_id}: {e}") from e

    async def unload_plugin(self, plugin_id: str) -> None:
        """
        Unload a plugin and clean up its resources.
        """
        plugin = self._plugins.get(plugin_id)
        if not plugin:
            raise PluginNotFoundError(f"Plugin {plugin_id} not found")

        try:
            await plugin.on_unload()
            # Unregister all hook handlers
            self._hook_registry.unregister_all(plugin_id)
            plugin._loaded = False
            del self._plugins[plugin_id]
            logger.info(f"Plugin {plugin_id} unloaded successfully")
        except Exception as e:
            raise PluginUnloadError(f"Failed to unload plugin {plugin_id}: {e}") from e

    async def unload_all(self) -> None:
        """
        Unload all loaded plugins.
        """
        for plugin_id in list(self._plugins.keys()):
            await self.unload_plugin(plugin_id)
        self._loaded = False
        logger.info("All plugins unloaded")

    def get_plugin(self, plugin_id: str) -> Plugin | None:
        """Get a loaded plugin instance by ID."""
        return self._plugins.get(plugin_id)

    def list_plugins(self) -> list[str]:
        """List IDs of all loaded plugins."""
        return list(self._plugins.keys())

    def register_hook(
        self,
        plugin_id: str,
        hook_point: HookPoint,
        handler: Any,
    ) -> None:
        """
        Register a hook handler for a plugin.
        """
        if plugin_id not in self._plugins:
            raise PluginNotFoundError(f"Plugin {plugin_id} not loaded")
        self._hook_registry.register(hook_point, handler, plugin_id)

    async def execute_hook(
        self,
        hook_point: HookPoint,
        *args,
        **kwargs,
    ) -> list[Any]:
        """
        Execute all handlers for a hook point.
        """
        return await self._hook_registry.execute(hook_point, *args, **kwargs)

    async def trigger_event(self, event_type: str, data: dict[str, Any]) -> None:
        """
        Trigger an event on all loaded plugins that implement on_event.
        """
        for plugin in self._plugins.values():
            try:
                await plugin.on_event(event_type, data)
            except Exception as e:
                logger.error(f"Error in plugin {plugin.plugin_id} on_event: {e}")

    @property
    def hook_registry(self) -> HookRegistry:
        return self._hook_registry
