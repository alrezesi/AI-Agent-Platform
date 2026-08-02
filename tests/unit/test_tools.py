
# Unit tests for tool calling system

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.agent_platform.tools.base import Tool, ToolParameter, ToolSchema
from src.agent_platform.tools.registry import ToolRegistry
from src.agent_platform.tools.executor import ToolExecutor
from src.agent_platform.tools.exceptions import (
    ToolNotFoundError,
    ToolValidationError,
    ToolExecutionError,
)
from src.agent_platform.tools.examples.echo_tool import EchoTool


class DummyTool(Tool):
    """A simple tool for testing."""
    async def execute(self, **kwargs):
        return kwargs.get("value", "default")


@pytest.mark.asyncio
async def test_tool_validation():
    tool = DummyTool(
        name="dummy",
        description="Dummy tool",
        parameters=[
            ToolParameter(name="value", type="string", required=True),
            ToolParameter(name="count", type="integer", required=False, default=1),
        ],
    )

    # Valid params
    validated = tool.validate_params({"value": "hello", "count": 2})
    assert validated == {"value": "hello", "count": 2}

    # Missing required
    with pytest.raises(ToolValidationError, match="Missing required parameter: value"):
        tool.validate_params({})

    # Invalid type
    with pytest.raises(ToolValidationError, match="must be an integer"):
        tool.validate_params({"value": "hello", "count": "two"})


@pytest.mark.asyncio
async def test_tool_registry():
    registry = ToolRegistry()
    tool = DummyTool("dummy", "Dummy")
    registry.register(tool)

    assert registry.get_tool("dummy") is tool
    assert "dummy" in registry.list_tools()

    # Get or raise
    assert registry.get_tool_or_raise("dummy") is tool
    with pytest.raises(ToolNotFoundError):
        registry.get_tool_or_raise("nonexistent")

    # Unregister
    assert registry.unregister("dummy") is True
    assert registry.get_tool("dummy") is None
    assert registry.unregister("dummy") is False  # already gone


@pytest.mark.asyncio
async def test_tool_executor():  # <-- removed 'registry' parameter
    registry = ToolRegistry()
    tool = DummyTool(
        "dummy",
        "Dummy",
        parameters=[ToolParameter(name="value", type="string", required=True)],
    )
    registry.register(tool)
    executor = ToolExecutor(registry)

    # Successful execution
    result = await executor.execute("dummy", {"value": "test"})
    assert result == "test"

    # Validation error
    with pytest.raises(ToolValidationError):
        await executor.execute("dummy", {})

    # Tool not found
    with pytest.raises(ToolNotFoundError):
        await executor.execute("unknown", {})


@pytest.mark.asyncio
async def test_echo_tool():
    tool = EchoTool()
    # Execute with required param
    result = await tool.execute(message="Hello")
    assert result == "Hello"

    # With uppercase
    result = await tool.execute(message="Hello", uppercase=True)
    assert result == "HELLO"

    # Missing required - should raise ToolValidationError
    with pytest.raises(ToolValidationError):
        await tool.execute(uppercase=True)  # missing message