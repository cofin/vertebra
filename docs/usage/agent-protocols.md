# Model Context Protocol (MCP, Skills & Stdio)

Expose typed Python functions, prompt templates, resources, and `SKILL.md` bundles over the Model Context Protocol, covering both stateless HTTP and legacy Streamable HTTP sessions. See [A2A, WebMCP & Workflows](a2a-and-workflows.md), [Vector Embeddings & Slabs](embeddings-and-vectors.md), and the [Agent API Reference](../reference/agent.md) for companion AI subsystems.

## Subsystem Overview

| Subsystem | When to Use |
| --- | --- |
| **MCP Server** | Expose Python tools, prompts, resources, `SKILL.md` bundles, and UI links over HTTP or stdio |
| **[A2A & WebMCP](a2a-and-workflows.md)** | Publish `/.well-known/agent-card.json` discovery cards, handle streaming A2A tasks, and expose browser tools |
| **[Bridges & Workflows](a2a-and-workflows.md)** | Bridge MCP and A2A services or run multi-turn agent loops with approval gates and DAG workflows |
| **[Vector Embeddings](embeddings-and-vectors.md)** | Generate embeddings via Google Cloud APIs, in-database SQL pushdown, or local models |

## Registering Tools, Prompts & Resources

Decorate typed Python functions with `@tool`, `@prompt`, and `@resource` and register them on a `ToolRegistry`. Input and output JSON Schemas compile once at registration time:

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

## Transport, Extensions & CLI

- **Dual-mode HTTP transport**: Negotiates stateless MCP and legacy session-based Streamable HTTP automatically. Supports server discovery, SSE streams and resource subscriptions, `Origin` validation, `ETag` caching (`304 Not Modified`), and session lifecycle endpoints (`GET /mcp`, `DELETE /mcp`).
- **Framework adapters**: Works across Vertebra, Starlette, FastAPI, Flask, Django, and Litestar (which delegates `/mcp` to `litestar-mcp`). All frameworks serve a single `/.well-known/mcp.json` descriptor and validate `ui://` resource links on startup.
- **Skills and UI extensions**: Advertises `SKILL.md` catalogs and MCP Apps UI metadata, filtering app-only tools for non-UI clients.
- **Async tasks and guards**: Promotes slow tool calls into background task records with per-owner cancellation tokens. Guards check permissions and rate or budget limits before running the handler.
- **Stdio and OIDC bridge**: Serve an app's MCP surface over stdio, or bridge a local stdio host to a remote HTTPS, IAP, or Cloud Run MCP server with automatic token refresh on `401`:

```bash
vtb ai mcp serve --app app:app
vtb ai mcp bridge \
  --url https://agent.example.com/mcp \
  --audience https://agent.example.com
```

## Caller Identity, IAP Verification & Cancellation

Read caller context inside tools via `AgentContext.current()`. ASGI auth state and WSGI `REMOTE_USER` establish caller identity automatically. Accepting proxy identity headers requires `MCPConfig(trust_proxy_identity=True)` paired with an explicit identity source:

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

- **Google IAP (`vertebra-core[iap]`)**: `IapIdentitySource` verifies `x-goog-iap-jwt-assertion` JWTs against Google IAP JWKs and fails closed on invalid or expired tokens. Plain `x-goog-authenticated-user-email` headers are never trusted.
- **mTLS and custom policies**: `MtlsIdentitySource` accepts `x-vertebra-mtls-subject` only from configured proxy CIDRs. Use `CallableIdentitySource` for custom policies.
- **Cancellation**: Stdio cancellation notifications abort in-flight dispatches, and `tasks/cancel` cancels promoted background tasks for matching owners.
