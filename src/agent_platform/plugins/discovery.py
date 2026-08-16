
# Plugin discovery utilities

import importlib
import importlib.util
import inspect
import logging
from pathlib import Path

from .base import Plugin
from .exceptions import PluginLoadError

logger = logging.getLogger(__name__)


def discover_plugins(directory: Path | None = None) -> list[type[Plugin]]:
    """
    Discover plugin classes from a directory.
    If directory is None, uses the default 'plugins/' folder in the project root.
    """
    if directory is None:
        # Default to project_root/plugins/
        project_root = Path(__file__).parent.parent.parent.parent
        directory = project_root / "plugins"

    if not directory.exists():
        logger.warning(f"Plugin directory {directory} does not exist.")
        return []

    plugin_classes = []

    # Walk through all Python files in the directory
    for file_path in directory.glob("*.py"):
        if file_path.name.startswith("_"):
            continue
        module_name = file_path.stem
        try:
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Find all classes that are subclasses of Plugin (but not Plugin itself)
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    inspect.isclass(attr)
                    and issubclass(attr, Plugin)
                    and attr is not Plugin
                ):
                    plugin_classes.append(attr)
        except Exception as e:
            logger.error(f"Error loading plugin module {file_path}: {e}")

    return plugin_classes


def load_plugin_from_path(file_path: Path) -> type[Plugin] | None:
    """
    Load a single plugin class from a specific Python file.
    """
    if not file_path.exists():
        raise PluginLoadError(f"Plugin file {file_path} not found")

    try:
        spec = importlib.util.spec_from_file_location(file_path.stem, file_path)
        if spec is None or spec.loader is None:
            raise PluginLoadError(f"Could not load spec for {file_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                inspect.isclass(attr)
                and issubclass(attr, Plugin)
                and attr is not Plugin
            ):
                return attr
    except Exception as e:
        raise PluginLoadError(f"Error loading plugin from {file_path}: {e}") from e

    return None
