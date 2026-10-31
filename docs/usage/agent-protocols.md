# Agent Protocols (MCP, A2A & ADK)

`vertebra.agent` provides a unified protocol layer for exposing application tools, resources, prompts, skills, and multi-turn agent workflows over the **Model Context Protocol (MCP)** and **Agent-to-Agent (A2A)** protocol.

## Model Context Protocol (`vertebra.agent.mcp`)

Register typed Python functions as MCP tools on `MCPRegistry`. Input JSON Schemas are compiled and cached from function type annotations and `msgspec.Struct` models:

```python
from vertebra.agent.core import tool
from vertebra.agent.mcp.registry import MCPRegistry

registry = MCPRegistry()


@tool(description="Look up current stock count for a SKU.")
def check_stock(sku: str) -> dict[str, int | str]:
    return {"sku": sku, "available": 42}


registry.register_tool(check_stock)
```

### MCP Skills & Long-Running Tasks

- **Skills (`MCPSkillsConfig`, `SkillController`)**: Serve structured `SKILL.md` capability bundles over MCP resources and HTTP endpoints so external coding agents can discover domain workflows on demand.
- **Tasks (`MCPTaskConfig`, `TaskRecord`)**: Track asynchronous tool executions with cancellation, progress reporting, and bounded retention.
- **Stdio-to-HTTP Bridge (`vtb mcp bridge`)**: Connect stdio-only MCP desktop clients to remote Vertebra HTTP/SSE MCP servers with automatic W3C trace context propagation.

## Agent-to-Agent Protocol (`vertebra.agent.a2a`)

Expose an A2A-compliant agent discovery card (`/.well-known/agent.json`) and JSON-RPC/SSE task orchestration endpoints using `A2AConfig`, `A2AService`, and `AgentCard`:

```python
from vertebra.agent.a2a import (
    A2AConfig,
    A2AService,
    AgentCapabilities,
    AgentSkill,
)

skill = AgentSkill(
    id="order-lookup",
    name="Order Lookup",
    description="Query order status.",
)
a2a = A2AService(
    A2AConfig(
        name="OrderSupportAgent",
        description="Resolves order status and shipment inquiries.",
        url="https://agents.example.com/a2a",
        capabilities=AgentCapabilities(streaming=True),
        skills=[skill],
    )
)
```

## Agent Harness & Workflow Engine (`vertebra.agent`)

Combine `AgentPlugin`, `AgentRuntime`, `WorkflowEngine`, and human-in-the-loop confirmation gates (`ConfirmationDecisionRequest`) to host multi-turn agents inside Vertebra with persistent session history (`MemorySessionStore`) and live SSE streaming.
