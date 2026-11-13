# `vertebra.agent` — Model Context Protocol (MCP), A2A & Workflow Engine

Agent runtime (`AgentRuntime`), Model Context Protocol (`MCPCore`, `MCPSessionStore`, `ToolRegistry`, `MCPRegistry`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig`, `SkillCatalog`), Identity (`IapIdentitySource`, `MtlsIdentitySource`), Agent-to-Agent (`A2AService`, `A2AConfig`, `AgentCard`), bidirectional protocol bridges (`McpToolBridge`, `A2aAgentBridge`), hierarchical task planning (`SpecTree`), and multi-step DAG execution (`DynamicWorkflow`, `WorkflowEngine`). See the [Agent Protocols Usage Guide](../usage/agent-protocols.md) for end-to-end walkthroughs.

| Section | Core Classes | Responsibility |
| --- | --- | --- |
| **Agent Plugin & Harness Runtime** | `AgentPlugin`, `AgentPluginConfig`, `AgentRuntime`, `ConfirmationGate`, `Tool`, `ToolSpec`, `ToolAnnotations` | Mount MCP/A2A/chat endpoints on `App`, execute bounded LLM tool-calling loops, and gate destructive tools on single-use human approval |
| **Model Context Protocol** | `MCPCore`, `MCPSessionStore`, `ToolRegistry`, `MCPRegistry`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig`, `SkillCatalog`, `McpIcon`, `IapIdentitySource` | Negotiate `2026-07-28` and legacy MCP versions, stream `GET` SSE notifications, serve `SKILL.md` & SEP-1865 UI resources, verify IAP JWTs, and track async tool tasks |
| **Agent-to-Agent Protocol & Bridges** | `AgentCard`, `A2AConfig`, `A2AService`, `McpToolBridge`, `A2aAgentBridge` | Serve JWS-signed `/.well-known/agent-card.json` descriptors, stream A2A task updates, and bridge MCP tools with A2A agents |
| **SpecTree, Workflows & Session Stores** | `SpecTree`, `DynamicWorkflow`, `WorkflowEngine`, `MemorySessionStore`, `SQLSpecSessionStore`, `AlchemySessionStore`, `ObjectStoreSessionStore` | Decompose goals into dependency trees, execute parallel DAG waves with checkpointing, and persist multi-turn conversation state |

## Agent Plugin & Harness Runtime

`AgentPlugin` mounts MCP, A2A, and SSE chat routes onto `App`, while `AgentRuntime` coordinates multi-turn LLM generation, `ConfirmationGate` approvals, and `Tool` execution.

::: vertebra.agent.plugin.AgentPluginConfig
    options:
      heading_level: 4
      members: false

::: vertebra.agent.plugin.AgentPlugin
    options:
      heading_level: 3

::: vertebra.agent.runtime.AgentRuntime
    options:
      heading_level: 3

::: vertebra.agent.runtime.ConfirmationGate
    options:
      heading_level: 4
      members:
        - request_confirmation
        - approve
        - reject

::: vertebra.agent.core.schema.Tool
    options:
      heading_level: 4
      members:
        - invoke

## Provider Tools (`vertebra.agent.tools`)

`build_tools` adapts a `vertebra.typing.ToolProvider` into validated `Tool` objects.
Native adapters own their operations and policies; construction resolves no connection.
Operations use named parameters; positional-only and variadic signatures are rejected.

```python
from vertebra.agent import ConfirmationGate, build_tools
from vertebra.db.adapters.bigquery import BigQueryConfig, BigQueryToolProvider

gate = ConfirmationGate()
tools = build_tools(
    BigQueryToolProvider(
        config=BigQueryConfig(project_id="my-project"),
        maximum_bytes_billed=1_000_000,
    ),
    confirmation_gate=gate,
)
rows = await tools["bigquery_query"].invoke(
    {"sql": "SELECT @id AS id", "params": {"id": 1}}
)
```

Spanner's adapter exports `SpannerToolProvider` with optional read-only snapshot staleness.
Each provider accepts a native driver, config, or registry alias resolved at invocation.
Mutations and ambiguous SQL, including `WITH`, require approval; approval does not expand
native SQL support. Retry with the approved `confirmation_token` input. Tickets bind to
the operation, validated arguments including defaults, and injected caller identity.
BigQuery sends the minimum provider, per-call, and configured billing cap to native
execution, including when its optional dry-run guard is disabled.

::: vertebra.agent.tools.build_tools
    options:
      heading_level: 4

## Model Context Protocol (`vertebra.agent.mcp`)

::: vertebra.agent.mcp.service.MCPCore
    options:
      heading_level: 4
      members: false

::: vertebra.agent.mcp.models.MCPConfig
    options:
      heading_level: 4
      members: false

::: vertebra.agent.core.identity.IapIdentitySource
    options:
      heading_level: 4
      members: false

::: vertebra.agent.mcp.registry.ToolRegistry
    options:
      heading_level: 3
      members:
        - register_tool
        - register_provider
        - list_tools
        - list_specs
        - register_controller

::: vertebra.agent.mcp.skills.MCPSkillsConfig
    options:
      heading_level: 4
      members: false

::: vertebra.agent.mcp.tasks.MCPTaskConfig
    options:
      heading_level: 4
      members: false

## Agent-to-Agent Protocol & Bridges (`vertebra.agent.a2a`, `vertebra.agent.bridges`)

::: vertebra.agent.a2a.models.AgentCard
    options:
      heading_level: 4
      members: false

::: vertebra.agent.a2a.service.A2AConfig
    options:
      heading_level: 4
      members: false

::: vertebra.agent.a2a.service.A2AService
    options:
      heading_level: 3
      members:
        - handle_payload
        - execute_task_flow
        - handle_message_send
        - handle_message_stream
        - stream_message

::: vertebra.agent.bridges.McpToolBridge
    options:
      heading_level: 4
      members:
        - from_a2a_service
        - from_agent_card

::: vertebra.agent.bridges.A2aAgentBridge
    options:
      heading_level: 4
      members:
        - from_mcp_service

## SpecTree, Workflows & Session Stores

::: vertebra.agent.spec.SpecTree
    options:
      heading_level: 4
      members:
        - add_node
        - ready_nodes
        - to_workflow

::: vertebra.agent.workflow.DynamicWorkflow
    options:
      heading_level: 4
      members:
        - add_node
        - add_step
        - topological_waves

::: vertebra.agent.workflow.WorkflowEngine
    options:
      heading_level: 3

::: vertebra.agent.sessions.MemorySessionStore
    options:
      heading_level: 3

::: vertebra.agent.sessions.SQLSpecSessionStore
    options:
      heading_level: 4
      members:
        - load
        - save
        - delete

::: vertebra.agent.sessions.AlchemySessionStore
    options:
      heading_level: 4
      members:
        - load
        - save
        - delete

::: vertebra.agent.sessions.ObjectStoreSessionStore
    options:
      heading_level: 4
      members:
        - load
        - save
        - delete
