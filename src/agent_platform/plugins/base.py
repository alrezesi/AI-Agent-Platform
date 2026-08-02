
# Abstract base class for plugins and plugin context

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class PluginContext:
    """
    Context passed to plugins during lifecycle.
    Contains configuration, shared state, and references to core services.
    """
    plugin_id: str
    config: Dict[str, Any] = field(default_factory=dict)
    shared_state: Dict[str, Any] = field(default_factory=dict)
    # Reference to core services can be added later (e.g., registry, scheduler)
    services: Optional[Dict[str, Any]] = None


class Plugin(ABC):
    """
    Abstract base class for all plugins.
    Plugins must implement on_load, on_unload, and optionally on_event.
    """

    def __init__(self, plugin_id: str, name: str, version: str = "1.0.0"):
        self.plugin_id = plugin_id
        self.name = name
        self.version = version
        self.context: Optional[PluginContext] = None
        self._loaded = False

    @abstractmethod
    async def on_load(self, context: PluginContext) -> None:
        """
        Called when the plugin is loaded.
        Initialize resources, register hooks, etc.
        """
        pass

    @abstractmethod
    async def on_unload(self) -> None:
        """
        Called when the plugin is unloaded.
        Clean up resources, unregister hooks, etc.
        """
        pass

    async def on_event(self, event_type: str, data: Dict[str, Any]) -> Any:
        """
        Handle arbitrary events. Can be overridden by plugins.
        Default implementation does nothing.
        """
        pass

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    def __repr__(self) -> str:
        return f"<Plugin {self.plugin_id} ({self.name} v{self.version})>"