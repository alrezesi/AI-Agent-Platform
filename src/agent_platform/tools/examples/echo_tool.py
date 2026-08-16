
# Example tool: echoes the input message

from typing import Any

from src.agent_platform.tools.base import Tool, ToolParameter
from src.agent_platform.tools.exceptions import ToolValidationError


class EchoTool(Tool):
    """
    A simple tool that echoes back the input message.
    Useful for testing and demonstrating the tool system.
    """

    def __init__(self):
        super().__init__(
            name="echo",
            description="Echoes back the input message",
            parameters=[
                ToolParameter(
                    name="message",
                    type="string",
                    description="The message to echo",
                    required=True,
                ),
                ToolParameter(
                    name="uppercase",
                    type="boolean",
                    description="Whether to return the message in uppercase",
                    required=False,
                    default=False,
                ),
            ],
        )

    async def execute(self, **kwargs) -> Any:
        """Echo the message, optionally in uppercase."""
        # Manually check required parameter because the base class validation
        # is normally done by ToolExecutor, but when called directly we need to ensure.
        if "message" not in kwargs or kwargs["message"] is None:
            raise ToolValidationError("Missing required parameter: message")
        message = kwargs.get("message", "")
        uppercase = kwargs.get("uppercase", False)
        if uppercase:
            return message.upper()
        return message
