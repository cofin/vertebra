# Model Context Protocol (MCP, Skills & Stdio)

`vertebra.agent.mcp` exposes typed Python functions, prompt templates, resources, and `SKILL.md` bundles over the **Model Context Protocol (MCP 2026-07-28)**. See [A2A, WebMCP & Workflows](a2a-and-workflows.md), [Vector Embeddings & Slabs](embeddings-and-vectors.md), and the [`vertebra.agent` API Reference](../reference/agent.md) for companion AI subsystems.

## Protocol & Runtime Selection Matrix

| Subsystem | Primary Entrypoints | When to Use |
| --- | --- | --- |
| **MCP Server (`vertebra.agent.mcp`)** | `MCPCore`, `ToolRegistry`, `ToolSpec`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig` | Expose typed Python `@tool`, `ToolProvider`, `@prompt`, `@resource` functions and `SKILL.md` bundles over MCP `2026-07-28` Streamable HTTP and stdio |
| **[A2A & WebMCP](a2a-and-workflows.md)** | `A2AService`, `A2AConfig`, `AgentCard`, `@vertebra/client/webmcp` | Publish `/.well-known/agent-card.json` discovery cards, handle streaming A2A tasks, and expose browser `document.modelContext` tools |
| **[Protocol Bridges & Workflows](a2a-and-workflows.md)** | `McpToolBridge`, `A2aAgentBridge`, `AgentRuntime`, `SpecTree`, `DynamicWorkflow` | Bridge MCP and A2A services or run multi-turn tool-calling loops with human-in-the-loop approval gates and DAG workflows |
| **[Vector Embeddings](embeddings-and-vectors.md)** | `Embedder`, `VertexEmbedder`, `GemmaEmbedder`, `VectorView`, `VectorBatchView` | Generate zero-copy shared-memory embeddings via Google gRPC, in-DB SQL pushdown, or `1x` RAM local models |

## Registering Tools, Prompts & Resources (`vertebra.agent.mcp`)

Register typed Python functions as MCP tools (`@tool`), prompt templates (`@prompt`), and resources (`@resource`) on `ToolRegistry` (or `MCPRegistry`). Input and output JSON Schemas are compiled once into immutable `ToolSpec` descriptors via `msgspec.defstruct` and `msgspec.json.schema_components`:

```python
from vertebra.agent.core import tool
from vertebra.agent.mcp import ToolRegistry, prompt, resource

registry = ToolRegistry()


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

## MCP 2026-07-28 Transport, Framework Mounts & `vtb ai mcp` CLI

- **Transport-Agnostic `MCPCore` (`McpHttpRequest` -> `McpHttpResponse`)**: Enforces MCP `2026-07-28` HTTP semantics (`server/discover`, `_meta` protocol version negotiation, `Mcp-Method`/`Mcp-Name` header validation, default same-origin `Origin` checks, `202 Accepted` notifications, `400` batch rejection, `405` on `GET /mcp`, and `ETag` `304 Not Modified` caching).
- **Shared ASGI/WSGI Adapters & `litestar-mcp` Delegation**: Starlette and FastAPI share `AsgiMcpApp`, Flask and Django share `WsgiMcpApp` over a persistent `Portal`, Litestar delegates `/mcp` to `litestar-mcp` without duplicating `opt["mcp_tool"]` handlers, and all frameworks serve one canonical `/.well-known/mcp.json` discovery schema.
- **Skills (`MCPSkillsConfig`, `SkillController`, `FilesystemSkillSource`)**: Serve structured `SKILL.md` bundles over MCP resources and HTTP endpoints so coding agents can load domain workflows on demand.
- **Async Tasks (`MCPTaskConfig`, `MCPTaskStore`, `TaskRecord`)**: Track promoted tool calls with owner-scoped records and cancellation that stops execution. Terminal records remain terminal.
- **Stdio Server & Stdio-to-HTTP Bridge (`vtb ai mcp serve|bridge`)**: Run a local MCP server over stdio (`vtb ai mcp serve --name my-mcp`) or bridge stdio-only MCP desktop clients to remote Vertebra HTTP/SSE MCP endpoints with W3C trace header propagation:

```bash
vtb ai mcp serve --name my-mcp
vtb ai mcp bridge --url http://127.0.0.1:8000/mcp
```


## Caller Identity and Cancellation

MCP tools receive an `AgentContext`, also available through `AgentContext.current()` during dispatch. ASGI authentication state and WSGI `REMOTE_USER` supply application identity. Proxy identity headers are ignored by default. To trust a certificate subject header from your proxy, configure both the opt-in and its policy:

```python
from vertebra.agent.core.identity import MtlsIdentitySource
from vertebra.agent.mcp import MCPConfig

config = MCPConfig(
    trust_proxy_identity=True,
    identity_source=MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",)),
)
```

The mTLS policy accepts `x-vertebra-mtls-subject` only when the adapter supplies an immediate peer inside that network. Configure the network for proxies that authenticate clients and replace incoming identity headers. Header-only native routes do not supply a peer, so this policy refuses identity there. `CallableIdentitySource` supports application-defined policies. Plain IAP email headers never establish identity; verified IAP JWT support remains pending.

Promoted tools inherit immutable caller identity and receive independent cancellation state. An owner calling `tasks/cancel` signals that tool's token and cancels its coroutine. Anonymous callers cannot access authenticated callers' task records. Stdio accepts an explicit `AgentContext` or creates one session context shared by its input frames.
