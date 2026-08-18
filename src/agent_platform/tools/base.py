# src/agent_platform/tools/base.py
# Base Tool class with schema definition

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field


class ToolParameter(BaseModel):
    """Definition of a single parameter for a tool."""
    name: str = Field(..., description="Parameter name")
    type: str = Field(..., description="JSON Schema type (string, number, integer, boolean, array, object)")
    description: str | None = Field(None, description="Parameter description")
    required: bool = Field(False, description="Whether this parameter is required")
    default: Any | None = Field(None, description="Default value if not provided")
    enum: list[Any] | None = Field(None, description="Allowed values (enum)")


class ToolSchema(BaseModel):
    """
    JSON Schema for a tool's input parameters.
    Compatible with OpenAI function calling schema.
    """
    type: str = Field("object", description="Schema type (always object)")
    properties: dict[str, ToolParameter] = Field(default_factory=dict)
    required: list[str] = Field(default_factory=list)


class Tool(ABC):
    """
    Abstract base class for all tools.
    Tools are callable functions that agents can invoke.
    """

    def __init__(
        self,
        name: str,
        description: str,
        parameters: list[ToolParameter] | None = None,
    ):
        self.name = name
        self.description = description
        self.parameters = parameters or []

    @abstractmethod
    async def execute(self, **kwargs) -> Any:
        """
        Execute the tool with validated parameters.
        Must be implemented by subclasses.
        """
        pass

    def get_schema(self) -> ToolSchema:
        """
        Generate a JSON Schema for the tool's parameters.
        """
        props = {}
        required = []
        for param in self.parameters:
            props[param.name] = param
            if param.required:
                required.append(param.name)
        return ToolSchema(type="object", properties=props, required=required)

    def validate_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Validate and clean parameters based on the schema.
        Raises ToolValidationError if validation fails.
        """
        from .exceptions import ToolValidationError

        # Build a dict of valid parameters
        validated = {}
        schema = self.get_schema()

        # Check required
        for param_name in schema.required:
            if param_name not in params:
                raise ToolValidationError(f"Missing required parameter: {param_name}")

        # Validate types and defaults
        for param in self.parameters:
            value = params.get(param.name)
            if value is None and param.default is not None:
                validated[param.name] = param.default
            elif value is not None:
                # Type validation (basic)
                if param.type == "string" and not isinstance(value, str):
                    raise ToolValidationError(f"Parameter {param.name} must be a string")
                elif param.type == "number" and not isinstance(value, (int, float)):
                    raise ToolValidationError(f"Parameter {param.name} must be a number")
                elif param.type == "integer" and not isinstance(value, int):
                    raise ToolValidationError(f"Parameter {param.name} must be an integer")
                elif param.type == "boolean" and not isinstance(value, bool):
                    raise ToolValidationError(f"Parameter {param.name} must be a boolean")
                elif param.type == "array" and not isinstance(value, list):
                    raise ToolValidationError(f"Parameter {param.name} must be an array")
                elif param.type == "object" and not isinstance(value, dict):
                    raise ToolValidationError(f"Parameter {param.name} must be an object")
                # Enum check
                if param.enum is not None and value not in param.enum:
                    raise ToolValidationError(
                        f"Parameter {param.name} must be one of {param.enum}"
                    )
                validated[param.name] = value

        return validated

    def to_dict(self) -> dict[str, Any]:
        """Convert tool to a dictionary for API responses."""
        return {
            "name": self.name,
            "description": self.description,
            "schema": self.get_schema().model_dump(),
        }
