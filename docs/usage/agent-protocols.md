# Agent Protocols (MCP, A2A & Workflows)

`vertebra.agent` provides a unified protocol layer for exposing application tools, prompts, resources, skills, and multi-step agent workflows over the **Model Context Protocol (MCP)** and **Agent-to-Agent (A2A)** protocol. See [`vertebra.agent` API Reference](../reference/agent.md) for complete class and method signatures.

## Protocol & Runtime Selection Matrix

| Subsystem | Primary Entrypoints | When to Use |
| --- | --- | --- |
| **MCP Server (`vertebra.agent.mcp`)** | `MCPRegistry`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig` | Expose typed Python `@tool`, `@prompt`, `@resource` functions and `SKILL.md` bundles to IDE/desktop MCP clients |
| **A2A Server (`vertebra.agent.a2a`)** | `A2AService`, `A2AConfig`, `AgentCard` | Publish `/.well-known/agent-card.json` discovery cards and handle streaming JSON-RPC/SSE multi-agent tasks |
| **Protocol Bridges (`vertebra.agent.bridges`)** | `McpToolBridge`, `A2aAgentBridge` | Expose remote A2A agents as callable MCP tools or wrap an `MCPRegistry` as an A2A task handler |
| **Harness & Planning (`vertebra.agent.runtime`, `spec`, `workflow`)** | `AgentRuntime`, `ConfirmationGate`, `SpecTree`, `DynamicWorkflow`, `WorkflowEngine` | Run multi-turn tool-calling loops with human-in-the-loop approval gates, hierarchical task decomposition, and parallel DAG execution |

## Model Context Protocol (`vertebra.agent.mcp`)

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

## MCP Skills, Long-Running Tasks & Stdio Bridge

- **Skills (`MCPSkillsConfig`, `SkillController`, `FilesystemSkillSource`)**: Serve structured `SKILL.md` bundles over MCP resources and HTTP endpoints so coding agents can load domain workflows on demand.
- **Async Tasks (`MCPTaskConfig`, `MCPTaskStore`, `TaskRecord`)**: Track long-running tool calls with progress updates, cancellation, and TTL retention.
- **Stdio-to-HTTP Bridge (`vtb mcp bridge` / `run_stdio_http_bridge`)**: Connect stdio-only MCP desktop clients to remote Vertebra HTTP/SSE MCP endpoints with W3C trace header propagation.

```bash
vtb mcp bridge --url http://127.0.0.1:8000/mcp
```

## Agent-to-Agent Protocol (`vertebra.agent.a2a`)

Expose an A2A 1.0 discovery card (`/.well-known/agent-card.json`) with optional JWS card signing (`card_secret` / `AgentCardSigner`) and JSON-RPC/SSE task endpoints using `AgentCard`, `AgentInterface`, `A2AConfig`, and `A2AService`:

```python
from vertebra.agent.a2a import (
    A2AConfig,
    A2AService,
    AgentCapabilities,
    AgentCard,
    AgentInterface,
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
    supported_interfaces=(
        AgentInterface(
            url="https://agents.example.com/a2a",
            protocol_binding="JSONRPC",
        ),
    ),
    capabilities=AgentCapabilities(streaming=True),
    skills=(skill,),
)
a2a = A2AService(A2AConfig(path="/a2a", agent_card=card))
```

## Bidirectional MCP / A2A Bridges (`vertebra.agent.bridges`)

Bridge MCP tools and A2A agents in either direction using `McpToolBridge`, `A2aAgentBridge`, `a2a_card_to_mcp_tools`, and `mcp_service_to_a2a` so an MCP client can delegate tasks to remote A2A agents or an A2A client can invoke local MCP tools.

## `SpecTree` Planning & DAG Workflows (`vertebra.agent.spec`, `vertebra.agent.workflow`)

Decompose complex goals into hierarchical `SpecTree` task nodes (`SpecNode`) or execute parallel topological waves with `DynamicWorkflow`, `WorkflowNode`, `WorkflowEngine`, and real-time `WorkflowLiveState` delta broadcasting:

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

Combine `AgentPlugin`, `AgentRuntime`, `GoogleGenAIProvider`, `GuardChain`, `TurnBudget`, and human-in-the-loop `ConfirmationGate` (`ConfirmationTicket`, `ConfirmationDecisionRequest`) with pluggable session stores (`MemorySessionStore`, `CacheSessionStore`, `SQLSpecSessionStore`, `AlchemySessionStore`, `ObjectStoreSessionStore`).
