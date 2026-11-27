# A2A, WebMCP, Bridges & Agent Workflows

Vertebra supports peer-to-peer Agent-to-Agent (A2A 1.0) discovery and streaming tasks, browser-side WebMCP tools, bidirectional MCP/A2A bridges, and DAG agent workflows.

## Agent-to-Agent Protocol

Serve an A2A 1.0 discovery card at `/.well-known/agent-card.json` (with optional JWS signing) alongside JSON-RPC and SSE task endpoints.

When `supported_interfaces` is omitted, Vertebra adds a default JSON-RPC interface and resolves its public URL from incoming Forwarded, X-Forwarded-*, or Host headers and the configured path, or from an explicit `base_url`:

```python
from vertebra.agent.a2a import (
    A2AConfig,
    A2AService,
    AgentCapabilities,
    AgentCard,
    AgentSkill,
)

skill = AgentSkill(
    id="order-lookup",
    name="Order Lookup",
    description="Query order status.",
)
card = AgentCard(
    name="OrderSupportAgent",
    description="Resolves order status and shipment inquiries.",
    capabilities=AgentCapabilities(streaming=True),
    skills=(skill,),
)
a2a = A2AService(A2AConfig(path="/a2a", agent_card=card))
```

## Browser WebMCP & Protocol Bridges

- **Browser WebMCP**: Use `@vertebra/client/webmcp` to register client-side tools on `document.modelContext` (falling back to `navigator.modelContext`) via `registerWebMCPTools`. Mark server tools with `@mcp_tool(webmcp=True)` to publish them in `/.well-known/webmcp.json`, including confirmation hints when `requires_confirmation=True`.
- **Protocol bridges**: Convert between MCP tools and A2A agents in either direction using `vertebra.agent.bridges`.

## SpecTree Planning & DAG Workflows

Break goals into dependency trees with `SpecTree`, or run parallel topological waves with `DynamicWorkflow` and `WorkflowEngine`:

```python
from vertebra.agent.spec import SpecTree
from vertebra.agent.workflow import (
    DynamicWorkflow,
    WorkflowEngine,
    WorkflowNode,
)

tree = SpecTree(root_goal="Fulfill enterprise order")
validate_node = tree.add_node(
    "validate-inventory",
    description="Verify warehouse stock",
)
ship_node = tree.add_node(
    "create-shipment",
    description="Book carrier label",
    depends_on=(validate_node.id,),
)

workflow = DynamicWorkflow(
    name="order-fulfillment",
    nodes=[
        WorkflowNode(name="validate", fn=lambda ctx: {"ok": True}),
        WorkflowNode(
            name="ship",
            depends_on=("validate",),
            fn=lambda ctx: {"shipped": ctx.get_output("validate")["ok"]},
        ),
    ],
)
engine = WorkflowEngine()
```

## Agent Runtime, Confirmation Gates & Sessions

Combine `AgentRuntime`, guard chains, turn budgets, and human-in-the-loop confirmation gates with memory, cache, SQL, or object-store session backends.

When an MCP tool sets `requires_confirmation=True`, unconfirmed calls fail and return a confirmation token. Approve the token once (for example through `/agent/confirmations/{token}` with `AgentPlugin`) and repeat the exact call with `_meta["vertebra/confirmationToken"]`. Confirmations are single-use, expire after `ttl_seconds`, and reject mismatched callers or modified arguments.
