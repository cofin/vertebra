from dataclasses import dataclass
from typing import Any, Literal

from vertebra.mcp.registry import Tool
from vertebra.mcp.schema import MCPSchemaGenerator


@dataclass
class User:
    id: int
    name: str
    email: str | None = None


def simple_func(x: int, y: str) -> bool:
    """Simple function."""


def complex_func(user: User, tags: list[str]) -> dict[str, Any]:
    """Complex function."""


def default_func(limit: int = 100, mode: Literal["fast", "slow"] = "fast") -> None:
    pass


def test_generate_simple_schema() -> None:
    tool = Tool(name="simple", handler=simple_func, description="Simple function.")
    schema = MCPSchemaGenerator.generate_tool_schema(tool)

    assert schema["name"] == "simple"
    assert schema["description"] == "Simple function."
    assert schema["inputSchema"]["type"] == "object"
    props = schema["inputSchema"]["properties"]
    assert props["x"]["type"] == "number"  # Extractor maps int -> number
    assert props["y"]["type"] == "string"
    assert "x" in schema["inputSchema"]["required"]
    assert "y" in schema["inputSchema"]["required"]


def test_generate_complex_schema() -> None:
    tool = Tool(name="complex", handler=complex_func)
    schema = MCPSchemaGenerator.generate_tool_schema(tool)

    props = schema["inputSchema"]["properties"]
    assert props["tags"]["type"] == "array"
    assert props["tags"]["items"]["type"] == "string"

    user_schema = props["user"]
    assert user_schema["type"] == "object"
    assert user_schema["properties"]["id"]["type"] == "number"
    assert user_schema["properties"]["name"]["type"] == "string"
    # email is optional, so it shouldn't be in required
    assert "email" not in user_schema["required"]


def test_generate_default_values() -> None:
    tool = Tool(name="defaults", handler=default_func)
    schema = MCPSchemaGenerator.generate_tool_schema(tool)

    props = schema["inputSchema"]["properties"]
    assert props["limit"]["default"] == 100

    mode_schema = props["mode"]
    # We optimized Literal unions to "enum"
    if "enum" in mode_schema:
        assert sorted(mode_schema["enum"]) == ["fast", "slow"]
    else:
        assert "anyOf" in mode_schema

    # defaults are optional in JSON schema validation sense (not required)
    assert "limit" not in schema["inputSchema"]["required"]
    assert "mode" not in schema["inputSchema"]["required"]
