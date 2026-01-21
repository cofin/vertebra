"""Model Context Protocol (MCP) integration for Vertebra.
Provides decorators and utilities for exposing Python functions as MCP tools.
"""

import inspect
from collections.abc import Callable
from typing import Any, TypeVar

T = TypeVar("T")


class ToolRegistry:
    """Registry for MCP tools."""

    def __init__(self) -> None:
        self._tools: dict[str, dict[str, Any]] = {}

    def register(self, func: Callable[..., Any], name: str | None = None, description: str | None = None) -> None:
        tool_name = name or func.__name__
        tool_doc = description or func.__doc__ or ""

        # Analyze signature to generate schema
        # For now, we'll placeholder the schema generation logic
        # TODO: Implement full JSON schema generation via msgspec
        input_schema = self._generate_schema(func)

        self._tools[tool_name] = {
            "name": tool_name,
            "description": tool_doc.strip(),
            "inputSchema": input_schema,
            "handler": func,
        }

    def _generate_schema(self, func: Callable[..., Any]) -> dict[str, Any]:
        """Generate JSON schema for function arguments using msgspec."""
        # Using msgspec's Struct to define arguments would be ideal,
        # but we need to inspect the function signature dynamically.
        sig = inspect.signature(func)

        # Simple schema generation for now
        properties = {}
        required = []

        for param_name, param in sig.parameters.items():
            if param_name == "self":
                continue

            param_type = param.annotation
            if param_type is inspect.Parameter.empty:
                param_type = Any

            # Map simple types (expand this later)
            json_type = "string"
            if param_type is int:
                json_type = "integer"
            elif param_type is bool:
                json_type = "boolean"

            properties[param_name] = {"type": json_type, "description": f"Parameter {param_name}"}

            if param.default is inspect.Parameter.empty:
                required.append(param_name)

        return {"type": "object", "properties": properties, "required": required}

    def get_tools(self) -> list[dict[str, Any]]:
        """Return list of registered tools metadata."""
        return [{k: v for k, v in tool.items() if k != "handler"} for tool in self._tools.values()]

    def get_handler(self, name: str) -> Callable[..., Any] | None:
        tool = self._tools.get(name)
        return tool["handler"] if tool else None


# Global registry
_registry = ToolRegistry()


def mcp_tool(name: str | None = None, description: str | None = None) -> Callable[[T], T]:
    """Decorator to register a function as an MCP tool.

    Args:
        name: Optional override for the tool name.
        description: Optional override for the tool description.
    """

    def decorator(func: T) -> T:
        _registry.register(func, name, description)  # type: ignore
        return func

    return decorator


def get_registry() -> ToolRegistry:
    return _registry
