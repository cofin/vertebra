# `vertebra.agent` — Model Context Protocol (MCP), A2A & Workflow Engine

Agent runtime (`AgentRuntime`), Model Context Protocol (`MCPRegistry`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig`), Agent-to-Agent (`A2AService`, `A2AConfig`, `AgentCard`), bidirectional protocol bridges (`McpToolBridge`, `A2aAgentBridge`), hierarchical task planning (`SpecTree`), and multi-step DAG execution (`DynamicWorkflow`, `WorkflowEngine`). See the [Agent Protocols Usage Guide](../usage/agent-protocols.md) for end-to-end walkthroughs.

| Section | Core Classes | Responsibility |
| --- | --- | --- |
| **Agent Plugin & Harness Runtime** | `AgentPlugin`, `AgentPluginConfig`, `AgentRuntime`, `ConfirmationGate`, `Tool` | Mount MCP/A2A/chat endpoints on `App`, execute bounded LLM tool-calling loops, and gate destructive tools on human approval |
| **Model Context Protocol** | `MCPConfig`, `MCPRegistry`, `MCPSkillsConfig`, `MCPTaskConfig` | Register `@tool`, `@prompt`, and `@resource` handlers, expose `SKILL.md` progressive disclosure, and track long-running async tool tasks |
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

## Model Context Protocol (`vertebra.agent.mcp`)

::: vertebra.agent.mcp.models.MCPConfig
    options:
      heading_level: 4
      members: false

::: vertebra.agent.mcp.registry.MCPRegistry
    options:
      heading_level: 3
      members:
        - register_tool
        - list_tools
        - register_resource
        - list_resources
        - register_prompt
        - list_prompts
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
