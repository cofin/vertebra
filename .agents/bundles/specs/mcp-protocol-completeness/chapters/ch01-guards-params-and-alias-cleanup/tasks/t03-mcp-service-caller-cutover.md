---
type: Task
id: "mcp-protocol-completeness:ch01:t03"
flow_id: mcp-protocol-completeness
chapter_id: ch01-guards-params-and-alias-cleanup
title: "Migrate Agent Bridges, Plugin, and Stdio Callers from MCPService to MCPCore"
description: "Switch A2aAgentBridge, AgentPlugin, and run_stdio_server from MCPService (and MCPRegistry in AgentPlugin) to canonical MCPCore and ToolRegistry."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch01:t02"
files:
  - src/py/vertebra/agent/bridges/a2a.py
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/agent/mcp/stdio.py
tests:
  - src/py/tests/unit/agent/test_a2a_protocol.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/bridges/a2a.py
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/agent/mcp/stdio.py
  - src/py/tests/unit/agent/test_a2a_protocol.py
estimated_loc: 65
tags:
  - mcp
  - a2a
  - agent
  - cleanup
sources:
  - id: a2a-bridge
    resource: "src/py/vertebra/agent/bridges/a2a.py::A2aAgentBridge"
    title: A2A bridge typed against MCPService
  - id: agent-plugin
    resource: "src/py/vertebra/agent/plugin.py::AgentPlugin"
    title: AgentPlugin instantiating MCPService and MCPRegistry
  - id: stdio-py
    resource: "src/py/vertebra/agent/mcp/stdio.py::run_stdio_server"
    title: Stdio server typed against MCPService
---

# Task `t03`: Migrate Agent Bridges, Plugin, and Stdio Callers from MCPService to MCPCore

## Objective & Scope Boundary

Replace internal `MCPService` (and `AgentPlugin` `MCPRegistry`) usages with `MCPCore` and `ToolRegistry` per [`../spec.md` §2](../spec.md) [^a2a-bridge] [^agent-plugin] [^stdio-py].

## Bounded Read Manifest

- `src/py/vertebra/agent/bridges/a2a.py`, `src/py/vertebra/agent/plugin.py`, & `src/py/vertebra/agent/mcp/stdio.py`.

## Contracts & Edge Cases

- Annotate `A2aAgentBridge`, `project_mcp_to_a2a`, and `run_stdio_server` with `MCPCore`.
- Construct `ToolRegistry()` and `MCPCore(...)` in `AgentPlugin.__init__`.

## Test Matrix

- Update `test_a2a_protocol.py` to instantiate `MCPCore` and verify `AgentPlugin` exposes `isinstance(plugin.mcp_service, MCPCore)` and `isinstance(plugin.mcp_registry, ToolRegistry)`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/bridges/a2a.py src/py/vertebra/agent/plugin.py src/py/vertebra/agent/mcp/stdio.py && uv run --no-sync mypy src/py/vertebra/agent && uv run --no-sync pytest src/py/tests/unit/agent/test_a2a_protocol.py src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [ ] Zero `MCPService` imports remain in `a2a.py`, `plugin.py`, or `stdio.py`.
- [ ] A2A and MCP unit suites pass.

[^a2a-bridge]: `src/py/vertebra/agent/bridges/a2a.py::A2aAgentBridge` — A2A bridge typed against MCPService.
[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin` — AgentPlugin instantiating MCPService and MCPRegistry.
[^stdio-py]: `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` — Stdio server typed against MCPService.
