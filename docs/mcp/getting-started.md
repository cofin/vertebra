# Getting Started with MCP in Vertebra

Vertebra provides a high-performance implementation of the **Model Context Protocol (MCP)**, allowing you to expose Python functions and resources to AI assistants like Claude Desktop.

## Prerequisites

- Vertebra installed (`pip install vertebra`)
- Claude Desktop (optional, for testing)

## Creating a Tool

Use the `@mcp_tool` decorator to register a Python function as an MCP tool.

```python
from vertebra.mcp import mcp_tool

@mcp_tool(description="Calculate the sum of two numbers")
def add(a: int, b: int) -> int:
    return a + b
```

## Creating a Resource

Use the `@mcp_resource` decorator to expose data as an MCP resource.

```python
from vertebra.mcp import mcp_resource

@mcp_resource("users://{user_id}/profile", name="User Profile")
def get_user_profile(user_id: int) -> dict:
    return {"id": user_id, "name": "Alice"}
```

## Running the Server

Start the Vertebra server with MCP support enabled.

```bash
vertebra serve --mcp
```

## connecting with Claude Desktop

Add the following configuration to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "vertebra": {
      "command": "vertebra",
      "args": ["serve", "--mcp", "--stdio"]
    }
  }
}
```
