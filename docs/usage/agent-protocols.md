# Model Context Protocol (MCP, Skills & Stdio)

`vertebra.agent.mcp` exposes typed Python functions, prompt templates, resources, and `SKILL.md` bundles over the **Model Context Protocol (MCP 2026-07-28)**. See [A2A, WebMCP & Workflows](a2a-and-workflows.md), [Vector Embeddings & Slabs](embeddings-and-vectors.md), and the [`vertebra.agent` API Reference](../reference/agent.md) for companion AI subsystems.

## Protocol & Runtime Selection Matrix

| Subsystem | Primary Entrypoints | When to Use |
| --- | --- | --- |
| **MCP Server (`vertebra.agent.mcp`)** | `MCPRegistry`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig` | Expose typed Python `@tool`, `@prompt`, `@resource` functions and `SKILL.md` bundles to IDE/desktop MCP clients |
| **[A2A & WebMCP](a2a-and-workflows.md)** | `A2AService`, `A2AConfig`, `AgentCard`, `@vertebra/client/webmcp` | Publish `/.well-known/agent-card.json` discovery cards, handle streaming A2A tasks, and expose browser `navigator.modelContext` tools |
| **[Protocol Bridges & Workflows](a2a-and-workflows.md)** | `McpToolBridge`, `A2aAgentBridge`, `AgentRuntime`, `SpecTree`, `DynamicWorkflow` | Bridge MCP and A2A services or run multi-turn tool-calling loops with human-in-the-loop approval gates and DAG workflows |
| **[Vector Embeddings](embeddings-and-vectors.md)** | `Embedder`, `VertexEmbedder`, `GemmaEmbedder`, `VectorView`, `VectorBatchView` | Generate zero-copy shared-memory embeddings via Google gRPC, in-DB SQL pushdown, or `1x` RAM local models |

## Registering Tools, Prompts & Resources (`vertebra.agent.mcp`)

Register typed Python functions as MCP tools (`@tool`), prompt templates (`@prompt`), and resources (`@resource`) on `MCPRegistry`. Input JSON Schemas are compiled automatically from type annotations and `msgspec.Struct` models:

```python
from vertebra.agent.core import tool
from vertebra.agent.mcp import MCPRegistry, prompt, resource

registry = MCPRegistry()


@tool(description="Look up current stock count for a SKU.")
def check_stock(sku: str) -> dict[str, int | str]:
    return {"sku": sku, "available": 42}


@prompt(description="Draft a restock request prompt.")
def restock_prompt(sku: str, quantity: int = 50) -> str:
    return f"Please restock {quantity} units of {sku}."


@resource(uri="inventory://warehouses", description="Active warehouse list.")
def list_warehouses() -> str:
    return "us-east, us-west, eu-central"


registry.register_tool(check_stock)
registry.register_prompt(restock_prompt)
registry.register_resource(list_warehouses)
```

## MCP Skills, Long-Running Tasks & `vtb ai mcp` CLI

- **Skills (`MCPSkillsConfig`, `SkillController`, `FilesystemSkillSource`)**: Serve structured `SKILL.md` bundles over MCP resources and HTTP endpoints so coding agents can load domain workflows on demand.
- **Async Tasks (`MCPTaskConfig`, `MCPTaskStore`, `TaskRecord`)**: Track long-running tool calls with progress updates, cancellation, and TTL retention.
- **Stdio Server & Stdio-to-HTTP Bridge (`vtb ai mcp serve|bridge`)**: Run a local MCP server over stdio (`vtb ai mcp serve app:app`) or bridge stdio-only MCP desktop clients to remote Vertebra HTTP/SSE MCP endpoints with W3C trace header propagation:

```bash
vtb ai mcp serve app:app
vtb ai mcp bridge --url http://127.0.0.1:8000/mcp
```
