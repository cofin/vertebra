from vertebra.mcp import get_registry, mcp_tool


def test_mcp_tool_registration() -> None:
    registry = get_registry()

    # Clear registry for test isolation (hacky but works for unit test)
    registry._tools.clear()

    @mcp_tool(description="A test tool")
    def my_tool(x: int, y: str) -> str:
        """Docstring is ignored if description provided."""
        return f"{y}-{x}"

    tools = registry.get_tools()
    assert len(tools) == 1
    tool = tools[0]

    assert tool["name"] == "my_tool"
    assert tool["description"] == "A test tool"
    assert tool["inputSchema"]["type"] == "object"
    assert tool["inputSchema"]["required"] == ["x", "y"]
    assert tool["inputSchema"]["properties"]["x"]["type"] == "integer"
    assert tool["inputSchema"]["properties"]["y"]["type"] == "string"


def test_mcp_tool_execution() -> None:
    registry = get_registry()
    handler = registry.get_handler("my_tool")
    assert handler is not None
    assert handler(10, "hello") == "hello-10"
