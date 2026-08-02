
# Custom exceptions for the plugin system

from src.agent_platform.core.exceptions import AgentPlatformError


class PluginError(AgentPlatformError):
    """Base exception for plugin-related errors."""
    pass


class PluginLoadError(PluginError):
    """Raised when a plugin fails to load."""
    pass


class PluginUnloadError(PluginError):
    """Raised when a plugin fails to unload."""
    pass


class PluginNotFoundError(PluginError):
    """Raised when a requested plugin is not found."""
    pass


class PluginHookError(PluginError):
    """Raised when a hook execution fails."""
    pass