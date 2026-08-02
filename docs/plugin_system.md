# Plugin System

The platform supports a plugin system that allows extending functionality without modifying the core code.

## Architecture

- **Plugin**: Abstract base class with lifecycle methods (`on_load`, `on_unload`, `on_event`).
- **PluginManager**: Loads, unloads, and manages plugin instances.
- **HookRegistry**: Manages hook handlers for various hook points.
- **Discovery**: Automatically discovers plugins from the `plugins/` directory.

## Hook Points

| Hook Point | Description |
|------------|-------------|
| `PRE_AGENT_RUN` | Called before an agent executes a task. |
| `POST_AGENT_RUN` | Called after an agent executes a task. |
| `ON_MESSAGE` | Called when a message is sent/received. |
| `ON_TASK_COMPLETE` | Called when a task completes. |
| `ON_AGENT_REGISTER` | Called when an agent registers. |
| `ON_AGENT_UNREGISTER` | Called when an agent unregisters. |
| `ON_SCHEDULER_EVENT` | Called on scheduler events (submit, cancel). |

## Creating a Plugin

1. Subclass `Plugin` and implement `on_load` and `on_unload`.
2. Optionally override `on_event` for arbitrary events.
3. Register hook handlers using `PluginManager.register_hook` in `on_load`.

Example:

```python
from agent_platform.plugins import Plugin, HookPoint

class MyPlugin(Plugin):
    async def on_load(self, context):
        # Register a hook
        manager = context.services.get("plugin_manager")
        manager.register_hook(self.plugin_id, HookPoint.PRE_AGENT_RUN, self.pre_run)

    async def on_unload(self):
        pass

    async def pre_run(self, task_data):
        print(f"About to run task {task_data['task_id']}")