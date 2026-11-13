---
type: Task
id: "mcp-protocol-completeness:ch07:t01"
flow_id: mcp-protocol-completeness
chapter_id: ch07-docs-and-okf-sync
title: Update MCP Usage Guides and Agent API Reference
description: "Document dual-mode MCP sessions, GET SSE, subscriptions, skills, SEP-1865 UI metadata, IapIdentitySource, single-use confirmations, and MCPCore in user docs."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: []
files:
  - docs/usage/agent-protocols.md
  - docs/usage/a2a-and-workflows.md
  - docs/reference/agent.md
tests: []
read_set:
  - docs/usage/agent-protocols.md
  - docs/usage/a2a-and-workflows.md
  - docs/reference/agent.md
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/core/identity.py
estimated_loc: 210
tags:
  - docs
  - mcp
  - a2a
  - agent
sources:
  - id: usage-guide
    resource: docs/usage/agent-protocols.md
    title: MCP and agent protocols usage guide
  - id: agent-ref
    resource: docs/reference/agent.md
    title: Agent API reference directives
---

# Task `t01`: Update MCP Usage Guides and Agent API Reference

## Objective & Scope Boundary

Rewrite `docs/usage/agent-protocols.md`, `docs/usage/a2a-and-workflows.md`, and `docs/reference/agent.md` in present tense per [`../spec.md` §2](../spec.md) [^usage-guide] [^agent-ref].

## Bounded Read Manifest

- `docs/usage/{agent-protocols,a2a-and-workflows}.md` & `docs/reference/agent.md`.
- `src/py/vertebra/agent/{mcp/models,mcp/service,core/identity}.py`.

## Contracts & Edge Cases

- Replace `MCPService` references with `MCPCore`; document `SUPPORTED_PROTOCOL_VERSIONS`, `MCPSessionStore`, `GET /mcp` SSE, `resources/subscribe` + `subscriptions/listen`, `Mcp-Param-*` validation, `SKILLS_EXTENSION`, `UI_EXTENSION` (`McpIcon`, `ui://`), `IapIdentitySource`, and single-use expiring `ConfirmationGate` tickets.
- Keep code examples concise (`class Model(msgspec.Struct):` without internal `frozen=True, gc=False` boilerplate) and conceptual Mermaid diagrams `<= 6` nodes.

## Test Matrix

- Verified via `make docs` (`scan_public_docstrings` + Zensical build).

## Verification

```bash
make docs
```

## Acceptance Criteria

- [ ] Zero stale `MCPService` or stateless-only `405` claims remain in agent usage/reference docs.
- [ ] `make docs` exits `0`.

[^usage-guide]: `docs/usage/agent-protocols.md` — MCP and agent protocols usage guide.
[^agent-ref]: `docs/reference/agent.md` — Agent API reference directives.
