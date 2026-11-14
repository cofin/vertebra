# Model Context Protocol (MCP, Skills & Stdio)

`vertebra.agent.mcp` exposes typed Python functions, prompt templates, resources, and `SKILL.md` bundles over **Model Context Protocol (`2026-07-28`)** and legacy **Streamable HTTP (`2025-11-25`, `2025-06-18`, `2025-03-26`)**. See [A2A, WebMCP & Workflows](a2a-and-workflows.md), [Vector Embeddings & Slabs](embeddings-and-vectors.md), and the [`vertebra.agent` API Reference](../reference/agent.md) for companion AI subsystems.

## Protocol & Runtime Selection Matrix

| Subsystem | Primary Entrypoints | When to Use |
| --- | --- | --- |
| **MCP Server (`vertebra.agent.mcp`)** | `MCPCore`, `ToolRegistry`, `MCPSessionStore`, `ToolSpec`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig` | Expose typed Python `@tool`, `ToolProvider`, `@prompt`, and `@resource` handlers with dual-mode version negotiation, SSE streams, subscriptions, `SKILL.md` bundles, and SEP-1865 UI links |
| **[A2A & WebMCP](a2a-and-workflows.md)** | `A2AService`, `A2AConfig`, `AgentCard`, `@vertebra/client/webmcp` | Publish `/.well-known/agent-card.json` discovery cards, handle streaming A2A tasks, and expose browser `document.modelContext` tools |
| **[Protocol Bridges & Workflows](a2a-and-workflows.md)** | `McpToolBridge`, `A2aAgentBridge`, `AgentRuntime`, `SpecTree`, `DynamicWorkflow` | Bridge MCP and A2A services or run multi-turn tool-calling loops with single-use approval gates and DAG workflows |
| **[Vector Embeddings](embeddings-and-vectors.md)** | `Embedder`, `VertexEmbedder`, `GemmaEmbedder`, `VectorView`, `VectorBatchView` | Generate zero-copy shared-memory embeddings via Google gRPC, in-DB SQL pushdown, or `1x` RAM local models |

## Registering Tools, Prompts & Resources (`vertebra.agent.mcp`)

Register typed Python functions as MCP tools (`@tool` / `@mcp_tool`), prompt templates (`@prompt` / `@mcp_prompt`), and resources (`@resource` / `@mcp_resource`) on `ToolRegistry` (or `MCPRegistry`). Input and output JSON Schemas compile once into immutable `ToolSpec` descriptors via `msgspec.defstruct` and `msgspec.json.schema_components`:

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

## Dual-Mode MCP Transport, Extensions & `vtb ai mcp` CLI

- **Transport-Agnostic `MCPCore` (`McpHttpRequest` -> `McpHttpResponse`)**: Negotiates `SUPPORTED_PROTOCOL_VERSIONS = ("2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26")` (returning `-32022` with `data.supported` on mismatch). Stateless `2026-07-28` serves `server/discover`, `subscriptions/listen` SSE, `Mcp-Method`/`Mcp-Name`/`Mcp-Param-*` (`-32020`) header checks, default `Origin` checks, and `ETag` `304 Not Modified`; legacy sessions issue owner-scoped `MCPSessionStore` IDs on `initialize`, stream notifications via `GET /mcp` SSE (`Last-Event-ID`, heartbeats, `Mcp-Session-Id`), terminate via `DELETE /mcp`, support `resources/subscribe` (`notify_resource_updated`), and accept `2025-03-26` JSON-RPC batches.
- **Shared ASGI/WSGI Adapters & `litestar-mcp` Delegation**: Starlette and FastAPI share `AsgiMcpApp`, Flask and Django share `WsgiMcpApp` over `PortalProvider.default()`, Litestar delegates `/mcp` to `litestar-mcp>=0.20.0`, and all frameworks serve one canonical `/.well-known/mcp.json` schema and validate `ui://` resource links on startup via `MCPCore.validate()`.
- **Skills (`SKILLS_EXTENSION`) & MCP Apps UI (`UI_EXTENSION`)**: Advertises `SkillCatalog` metadata under `capabilities.extensions['io.modelcontextprotocol/skills']` alongside `capabilities.extensions['io.modelcontextprotocol/ui']` (`McpIcon`, tool/resource `_meta["ui"]`, `visibility`, and app-only tool filtering for non-UI clients).
- **Async Tasks & Guards**: Promotes slow tool calls into owner-scoped `MCPTaskStore` records with isolated cancellation tokens. `Tool.authorize` evaluates guards once (`-32004` permission denied, `-32005` rate/budget limit) before `Tool.call`.
- **Incremental Stdio & OIDC Bridge (`vtb ai mcp serve|bridge`)**: Discover and serve an application's MCP surface over incremental/cancellable stdio, or bridge desktop stdio hosts to remote HTTPS/IAP/Cloud Run MCP servers using `OidcTokenSource` (`AsyncGcpIdTokenCache`, automatic `401` refresh retry) and dual-mode `GET` SSE relay:

```bash
vtb ai mcp serve --app app:app
vtb ai mcp bridge \
  --url https://agent.example.com/mcp \
  --audience https://agent.example.com
```

## Caller Identity, IAP Verification & Cancellation

MCP tools receive an `AgentContext` via `AgentContext.current()`. ASGI auth state and WSGI `REMOTE_USER` establish application caller identity; proxy identity headers require both `MCPConfig(trust_proxy_identity=True)` and an explicit `IdentitySource`:

```python
from vertebra.agent.core.identity import IapIdentitySource, MtlsIdentitySource
from vertebra.agent.mcp import MCPConfig

iap_config = MCPConfig(
    trust_proxy_identity=True,
    identity_source=IapIdentitySource(
        "/projects/123456789/global/backendServices/987"
    ),
)
mtls_config = MCPConfig(
    trust_proxy_identity=True,
    identity_source=MtlsIdentitySource(trusted_proxies=("10.0.0.0/8",)),
)
```

- **`IapIdentitySource` (`vertebra-core[iap]`)**: Verifies ES256 `x-goog-iap-jwt-assertion` headers against Google IAP JWKs (`aud`, `iss == 'https://cloud.google.com/iap'`, `exp`), failing closed (`None`) on invalid/expired assertions and never trusting plain `x-goog-authenticated-user-email` headers.
- **`MtlsIdentitySource` & `CallableIdentitySource`**: Accept `x-vertebra-mtls-subject` only from configured proxy CIDRs (returning `None` on header-only routes without peer addresses) or delegate to custom application policies.
- **Cancellation**: `notifications/cancelled` aborts in-flight stdio dispatches, while `tasks/cancel` signals promoted task tokens for matching owners.
