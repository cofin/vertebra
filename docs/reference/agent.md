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

## Agent-to-Agent Protocol (`vertebra.agent.a2a`)

::: vertebra.agent.a2a.service.A2AConfig
    options:
      heading_level: 4
      members: false

::: vertebra.agent.a2a.service.A2AService
    options:
      heading_level: 3

## Agent Workflows & Session Stores

::: vertebra.agent.workflow.WorkflowEngine
    options:
      heading_level: 3

::: vertebra.agent.sessions.MemorySessionStore
    options:
      heading_level: 3
