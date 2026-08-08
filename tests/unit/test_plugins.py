
# Unit tests for plugin system

import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from src.agent_platform.plugins.base import Plugin, PluginContext
from src.agent_platform.plugins.exceptions import PluginLoadError, PluginNotFoundError
from src.agent_platform.plugins.hooks import HookRegistry, HookPoint
from src.agent_platform.plugins.manager import PluginManager
from src.agent_platform.plugins.discovery import discover_plugins, load_plugin_from_path


class DummyPlugin(Plugin):
    """A simple plugin for testing."""
    async def on_load(self, context: PluginContext) -> None:
        self._loaded = True

    async def on_unload(self) -> None:
        self._loaded = False


@pytest.mark.asyncio
async def test_plugin_lifecycle():
    plugin = DummyPlugin("dummy", "Dummy")
    context = PluginContext(plugin_id="dummy", config={})
    await plugin.on_load(context)
    assert plugin.is_loaded is True
    await plugin.on_unload()
    assert plugin.is_loaded is False


@pytest.mark.asyncio
async def test_hook_registry():
    registry = HookRegistry()
    called = False

    async def handler():
        nonlocal called
        called = True

    registry.register(HookPoint.PRE_AGENT_RUN, handler, "test_plugin")
    assert len(registry._handlers[HookPoint.PRE_AGENT_RUN]) == 1

    await registry.execute(HookPoint.PRE_AGENT_RUN)
    assert called is True

    registry.unregister_all("test_plugin")
    assert len(registry._handlers[HookPoint.PRE_AGENT_RUN]) == 0


@pytest.mark.asyncio
async def test_plugin_manager_load_all(tmp_path):
    # Create a temporary plugin file
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    plugin_file = plugin_dir / "test_plugin.py"
    plugin_file.write_text("""
from src.agent_platform.plugins.base import Plugin
class TestPlugin(Plugin):
    async def on_load(self, context): self._loaded = True
    async def on_unload(self): self._loaded = False
""")

    manager = PluginManager(plugin_dir)
    await manager.load_all()
    assert "testplugin" in manager.list_plugins()  # class name lowercased


@pytest.mark.asyncio
async def test_plugin_manager_load_specific():
    manager = PluginManager()
    plugin = DummyPlugin("dummy", "Dummy")
    await manager.load_plugin(plugin)
    assert "dummy" in manager._plugins

    # Try loading again (should skip)
    await manager.load_plugin(plugin)
    assert len(manager._plugins) == 1

    # Unload
    await manager.unload_plugin("dummy")
    assert "dummy" not in manager._plugins


@pytest.mark.asyncio
async def test_plugin_manager_unload_not_found():
    manager = PluginManager()
    with pytest.raises(PluginNotFoundError):
        await manager.unload_plugin("nonexistent")


@pytest.mark.asyncio
async def test_plugin_manager_hook_integration():
    manager = PluginManager()
    plugin = DummyPlugin("dummy", "Dummy")
    await manager.load_plugin(plugin)

    # Register a hook
    async def handler():
        return "handled"
    manager.register_hook("dummy", HookPoint.PRE_AGENT_RUN, handler)

    results = await manager.execute_hook(HookPoint.PRE_AGENT_RUN)
    assert results == ["handled"]



def test_discover_plugins_from_directory(tmp_path):
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    plugin_file = plugin_dir / "test_plugin.py"
    plugin_file.write_text("""
from src.agent_platform.plugins.base import Plugin
class MyPlugin(Plugin):
    async def on_load(self, context): pass
    async def on_unload(self): pass
""")
    from src.agent_platform.plugins.discovery import discover_plugins
    classes = discover_plugins(plugin_dir)
    assert len(classes) == 1
    assert classes[0].__name__ == "MyPlugin"


def test_discover_plugins_empty_dir(tmp_path):
    plugin_dir = tmp_path / "empty_plugins"
    plugin_dir.mkdir()
    from src.agent_platform.plugins.discovery import discover_plugins
    classes = discover_plugins(plugin_dir)
    assert len(classes) == 0


def test_load_plugin_from_path(tmp_path):
    plugin_file = tmp_path / "plugin.py"
    plugin_file.write_text("""
from src.agent_platform.plugins.base import Plugin
class MyPlugin(Plugin):
    async def on_load(self, context): pass
    async def on_unload(self): pass
""")
    from src.agent_platform.plugins.discovery import load_plugin_from_path
    cls = load_plugin_from_path(plugin_file)
    assert cls is not None
    assert cls.__name__ == "MyPlugin"