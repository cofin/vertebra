from vertebra.mcp import mcp_resource, mcp_tool


@mcp_tool(description="Echo back the input message")
def echo(message: str) -> str:
    return message


@mcp_tool(description="Add two numbers")
def add(a: int, b: int) -> int:
    return a + b


@mcp_resource("config://settings", name="Settings")
def get_settings() -> dict:
    return {"theme": "dark", "version": "1.0.0"}
