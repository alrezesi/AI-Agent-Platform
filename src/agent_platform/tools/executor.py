
# ToolExecutor: executes tools with validation and error handling

import logging
from typing import Dict, Any, Optional

from .base import Tool
from .registry import ToolRegistry
from .exceptions import ToolValidationError, ToolExecutionError

logger = logging.getLogger(__name__)


class ToolExecutor:
    """
    Executes tools by name, validating parameters before execution.
    """

    def __init__(self, registry: ToolRegistry):
        self.registry = registry

    async def execute(
        self,
        tool_name: str,
        params: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Execute a tool by name with the given parameters.
        Parameters are validated against the tool's schema.
        """
        tool = self.registry.get_tool_or_raise(tool_name)

        try:
            # Validate parameters
            validated_params = tool.validate_params(params)
            logger.debug(f"Executing tool {tool_name} with params: {validated_params}")
            # Execute
            result = await tool.execute(**validated_params)
            logger.debug(f"Tool {tool_name} executed successfully")
            return result
        except ToolValidationError as e:
            logger.error(f"Validation error for tool {tool_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Execution error for tool {tool_name}: {e}")
            raise ToolExecutionError(f"Tool {tool_name} failed: {e}") from e