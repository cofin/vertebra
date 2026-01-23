from vertebra.mcp import mcp_tool, mcp_resource

@mcp_tool(description="Echo back the input message")
def echo(message: str) -> str:
    return message

@mcp_tool(description="Add two numbers")
def add(a: int, b: int) -> int:
    return a + b

@mcp_resource("config://settings", name="Settings")
def get_settings() -> dict:
    return {"theme": "dark", "version": "1.0.0"}

if __name__ == "__main__":
    # In a real app, this would be run via `vertebra serve`
    print("MCP Tools defined. Run with `vertebra serve` to expose.")
