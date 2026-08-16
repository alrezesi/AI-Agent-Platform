
# ToolRegistry for registering and discovering tools

import logging

from .base import Tool
from .exceptions import ToolNotFoundError

logger = logging.getLogger(__name__)


class ToolRegistry:
    """
    Registry for managing available tools.
    Tools are registered by name and can be retrieved by name.
    """

    def __init__(self):
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        """
        Register a tool.
        If a tool with the same name already exists, it will be overwritten.
        """
        self._tools[tool.name] = tool
        logger.info(f"Tool {tool.name} registered")

    def unregister(self, tool_name: str) -> bool:
        """
        Unregister a tool by name.
        Returns True if removed, False if not found.
        """
        if tool_name in self._tools:
            del self._tools[tool_name]
            logger.info(f"Tool {tool_name} unregistered")
            return True
        return False

    def get_tool(self, tool_name: str) -> Tool | None:
        """Retrieve a tool by name."""
        return self._tools.get(tool_name)

    def get_tool_or_raise(self, tool_name: str) -> Tool:
        """
        Retrieve a tool by name or raise ToolNotFoundError.
        """
        tool = self.get_tool(tool_name)
        if tool is None:
            raise ToolNotFoundError(f"Tool '{tool_name}' not found")
        return tool

    def list_tools(self) -> list[str]:
        """List all registered tool names."""
        return list(self._tools.keys())

    def get_all_tools(self) -> list[Tool]:
        """Get all registered tools."""
        return list(self._tools.values())

    def clear(self) -> None:
        """Remove all tools."""
        self._tools.clear()
