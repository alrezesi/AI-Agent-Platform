# Tool Calling

Agents can call external tools to perform actions like fetching data, performing computations, or interacting with external APIs.

## Architecture

- **Tool**: Abstract base class with a name, description, parameters schema, and `execute` method.
- **ToolRegistry**: Manages registered tools.
- **ToolExecutor**: Executes tools with parameter validation and error handling.

## Defining a Tool

Subclass `Tool` and implement `execute`. Define parameters using `ToolParameter`.

Example:

```python
from agent_platform.tools import Tool, ToolParameter

class MyTool(Tool):
    def __init__(self):
        super().__init__(
            name="my_tool",
            description="Does something",
            parameters=[
                ToolParameter(name="input", type="string", required=True),
            ],
        )

    async def execute(self, **kwargs):
        input_val = kwargs["input"]
        # process...
        return result