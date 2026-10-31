# `vertebra.agent` — Model Context Protocol (MCP), A2A & Workflow Engine

Agent runtime (`AgentRuntime`), Model Context Protocol (`MCPRegistry`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig`), Agent-to-Agent (`A2AService`, `A2AConfig`), and multi-step `WorkflowEngine`.

## Agent Plugin & Harness Runtime

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
      members: false

::: vertebra.agent.core.schema.Tool
    options:
      heading_level: 4
      members: false

## Model Context Protocol (`vertebra.agent.mcp`)

::: vertebra.agent.mcp.models.MCPConfig
    options:
      heading_level: 4
      members: false

::: vertebra.agent.mcp.registry.MCPRegistry
    options:
      heading_level: 3

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

::: vertebra.agent.bridges.McpToolBridge
    options:
      heading_level: 4
      members: false

::: vertebra.agent.bridges.A2aAgentBridge
    options:
      heading_level: 4
      members: false

## SpecTree, Workflows & Session Stores

::: vertebra.agent.spec.SpecTree
    options:
      heading_level: 4
      members: false

::: vertebra.agent.workflow.DynamicWorkflow
    options:
      heading_level: 4
      members: false

::: vertebra.agent.workflow.WorkflowEngine
    options:
      heading_level: 3

::: vertebra.agent.sessions.MemorySessionStore
    options:
      heading_level: 3

::: vertebra.agent.sessions.SQLSpecSessionStore
    options:
      heading_level: 4
      members: false

::: vertebra.agent.sessions.AlchemySessionStore
    options:
      heading_level: 4
      members: false

::: vertebra.agent.sessions.ObjectStoreSessionStore
    options:
      heading_level: 4
      members: false
