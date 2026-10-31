# Agent Protocols (MCP, A2A & ADK)

`vertebra.agent` provides a unified protocol layer for exposing application tools, resources, prompts, skills, and multi-turn agent workflows over the **Model Context Protocol (MCP)** and **Agent-to-Agent (A2A)** protocol.

## Model Context Protocol (`vertebra.agent.mcp`)

Register typed Python functions as MCP tools, resources, and prompts on `MCPRegistry`. Input JSON Schemas are compiled and cached from function type annotations and `msgspec.Struct` models:

```python
from vertebra.agent.mcp import MCPConfig, MCPRegistry

mcp = MCPRegistry(MCPConfig(name="inventory-mcp", version="1.0.0"))


@mcp.tool(description="Look up current stock count for a SKU.")
def check_stock(sku: str) -> dict[str, int | str]:
    return {"sku": sku, "available": 42}


@mcp.resource("config://warehouse/regions", description="Supported warehouse regions.")
def warehouse_regions() -> list[str]:
    return ["us-east", "eu-west", "ap-south"]
```

### MCP Skills & Long-Running Tasks

- **Skills (`MCPSkillsConfig`, `SkillController`)**: Serve structured `SKILL.md` capability bundles over MCP resources and HTTP endpoints so external coding agents can discover domain workflows on demand.
- **Tasks (`MCPTaskConfig`, `TaskRecord`)**: Track asynchronous tool executions with cancellation, progress reporting, and bounded retention.
- **Stdio-to-HTTP Bridge (`vtb mcp bridge`)**: Connect stdio-only MCP desktop clients to remote Vertebra HTTP/SSE MCP servers with automatic W3C trace context propagation.

## Agent-to-Agent Protocol (`vertebra.agent.a2a`)

Expose an A2A-compliant agent discovery card (`/.well-known/agent.json`) and JSON-RPC/SSE task orchestration endpoints using `A2AConfig`, `A2AService`, and `AgentCard`:

```python
from vertebra.agent.a2a import A2AConfig, A2AService, AgentCapabilities, AgentSkill

a2a = A2AService(
    A2AConfig(
        name="OrderSupportAgent",
        description="Resolves order status and shipment inquiries.",
        url="https://agents.example.com/a2a",
        capabilities=AgentCapabilities(streaming=True),
        skills=[AgentSkill(id="order-lookup", name="Order Lookup", description="Query order status.")],
    )
)
```

## Agent Harness & Google ADK (`vertebra.agent.adk`)

Combine `AgentPlugin`, `TurnBudget`, human-in-the-loop confirmation gates (`ConfirmationDecisionRequest`), and `vertebra.agent.adk` to host Google ADK agents inside Vertebra with persistent session history and live SSE streaming.
