# Agent Runtime, MCP, A2A & Workflows

`vertebra.agent` provides the agent execution runtime, Model Context Protocol (MCP) and Agent-to-Agent (A2A) services, identity verification, protocol bridges, task planning trees, and DAG workflows. See [OKF v0.2 & Knowledge Catalog](okf-and-catalog.md) for bundle serialization and Dataplex sync, and [Agent Protocols Usage Guide](../usage/agent-protocols.md) for walkthroughs.

| Section | Responsibility |
| --- | --- |
| **Agent Plugin & Harness Runtime** | Mount MCP, A2A, and chat routes on `App`, run bounded LLM tool-calling loops, and gate destructive tools behind single-use human approval. |
| **Model Context Protocol** | Negotiate current and legacy MCP protocol versions, stream SSE notifications, serve `SKILL.md` and UI resources, verify IAP JWTs, and track async tool tasks. |
| **Agent-to-Agent Protocol & Bridges** | Serve JWS-signed `/.well-known/agent-card.json` descriptors, stream A2A task updates, and bridge MCP tools with A2A agents. |
| **PlanTree, Workflows & Session Stores** | Break goals into dependency trees, run parallel DAG waves with checkpointing, and store multi-turn conversation state. |

## Agent Plugin & Harness Runtime

`AgentPlugin` mounts MCP, A2A, and SSE chat routes onto `App`. `AgentRuntime` coordinates multi-turn LLM generation, approval checks, and tool invocation.

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

## Provider Tools

`build_tools` adapts a `vertebra.typing.ToolProvider` into validated `Tool` objects. Adapters own their operations and policies, and constructing tools opens no database connection. Operations use named parameters; positional-only and variadic signatures are rejected.

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

Spanner's adapter exports `SpannerToolProvider` with optional read-only snapshot staleness. Each provider accepts a driver, config, or registry alias resolved at invocation. Mutations and ambiguous SQL, including `WITH`, require approval; retry with the approved `confirmation_token` input. Approval tickets bind to the operation, validated arguments (including defaults), and caller identity. BigQuery enforces the lowest of the provider, per-call, and configured billing caps even when the dry-run guard is disabled.

::: vertebra.agent.tools.build_tools
    options:
      heading_level: 4

## Model Context Protocol

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

## Agent-to-Agent Protocol & Bridges

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

::: vertebra.agent.bridges.MCPToolBridge
    options:
      heading_level: 4
      members:
        - from_a2a_service
        - from_agent_card

::: vertebra.agent.bridges.A2AAgentBridge
    options:
      heading_level: 4
      members:
        - from_mcp_service

## PlanTree, Workflows & Session Stores

::: vertebra.agent.planning.PlanTree
    options:
      heading_level: 4
      members:
        - add_node
        - add_step
        - ready_nodes
        - to_workflow

::: vertebra.agent.workflow.Workflow
    options:
      heading_level: 4
      members:
        - add
        - add_step
        - topological_waves

::: vertebra.agent.workflow.WorkflowEngine
    options:
      heading_level: 3
      members: false
      show_bases: false
      show_source: false

::: vertebra.agent.sessions.MemorySessionStore
    options:
      heading_level: 3
      members:
        - load
        - save
        - delete

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
