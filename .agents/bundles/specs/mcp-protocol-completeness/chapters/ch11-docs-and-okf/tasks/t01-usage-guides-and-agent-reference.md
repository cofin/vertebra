---
type: Task
id: "mcp-protocol-completeness:ch11:t01"
chapter_id: ch11-docs-and-okf
title: Update MCP usage guides and the agent reference
description: Rewrite the MCP usage guide and the confirmation section of the A2A/workflows guide in present tense for dual-mode transport, sessions, GET SSE, subscriptions, skills, MCP Apps, trusted identity headers, guards, error codes, and confirmation lifecycle, and add reference directives for new public symbols.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "mcp-protocol-completeness:ch10:t02"
read_set:
  - docs/usage/agent-protocols.md
  - docs/usage/a2a-and-workflows.md
  - docs/reference/agent.md
  - .agents/bundles/specs/mcp-protocol-completeness/spec.md
files:
  - docs/usage/agent-protocols.md
  - docs/usage/a2a-and-workflows.md
  - docs/reference/agent.md
tests: []
estimated_loc: 220
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: usage-guide
    resource: docs/usage/agent-protocols.md
    title: MCP usage guide
  - id: workflows-guide
    resource: docs/usage/a2a-and-workflows.md
    title: A2A, WebMCP, bridges, and confirmation guide
  - id: agent-reference
    resource: docs/reference/agent.md
    title: vertebra.agent API reference
tags:
  - docs
---

# Task t01: Update MCP Usage Guides and the Agent Reference

## Objective & Scope Boundary

User-facing guides describe the shipped MCP surface [^usage-guide] [^workflows-guide] [^agent-reference]. Out of scope: CLI pages (`t02`) and OKF chapters (`t03`, `t04`).

## Bounded Read Manifest

- `docs/usage/agent-protocols.md`.
- `docs/usage/a2a-and-workflows.md` (confirmation paragraph that cites `-32003`).
- `docs/reference/agent.md` (`## Model Context Protocol (vertebra.agent.mcp)` directives).
- `.agents/bundles/specs/mcp-protocol-completeness/spec.md` Section 2.2 (symbol list).

## Symbol & Signature Contracts

- `agent-protocols.md` gains present-tense sections: **Protocol versions** (`SUPPORTED_PROTOCOL_VERSIONS`, mode rule, `-32022` data), **Legacy sessions** (`initialize`, `Mcp-Session-Id`, `DELETE`, `session_ttl_seconds`, `max_sessions`), **Server push** (`GET` SSE, `sse_heartbeat_seconds`, `max_streams_per_owner`, list changes, `MCPCore.log`, progress), **Resource subscriptions** (`resources/subscribe`, `subscription_authorizer`, `await core.notify_resource_updated(uri)`, `subscriptions/listen`), **Skills** (`AgentPluginConfig.skills`, `mount_mcp_routes(skills=...)`, extension capability, `expose_as_prompts`), **MCP Apps** (`ui://` resources, `@mcp_tool(ui_resource_uri=..., ui_visibility=...)`, `UI_MIME_TYPE`, `MCPCore.validate`), **Caller identity and guards** (`MCPConfig.trust_proxy_identity` and `identity_source` (proxy headers ignored by default; `IapIdentitySource` verifying `x-goog-iap-jwt-assertion`, `MtlsIdentitySource` with trusted proxy CIDRs, `CallableIdentitySource`; construction-time errors), `AgentContext.current()`, `-32004`, `-32005`). Install snippets use linked `pymdownx.tabbed` blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`) for `vertebra-core[mcp]` where shown. Mermaid diagrams stay at 4–6 conceptual nodes.
- `a2a-and-workflows.md` confirmation paragraph states: tickets expire after `ttl_seconds` (default `600`), approvals are single-use, owner-bound tickets require the same caller, and a reused token returns `-32003` with a new token.
- `reference/agent.md` adds `:::` directives for `vertebra.agent.mcp.sessions.MCPSessionStore`, `vertebra.agent.mcp.sessions.MCPSession`, `vertebra.agent.mcp.models.McpIcon`, `vertebra.agent.mcp.models.NegotiatedProtocol`, `vertebra.agent.core.context.AgentContext` (with `from_headers`, `from_scope`, `from_wsgi_environ`, `fork`), and `vertebra.agent.runtime.ConfirmationGate` (updated members), without duplicating existing directives.

## Edge Cases & Error Semantics

- Example code blocks that import symbols must use names that exist at the flow's final commit.
- No statement may claim `GET /mcp` always returns `405` or that `initialize` is unsupported.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation` through `make docs` (docstring scan, Zensical build, artifact verification).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Build | `make docs` | Exit `0` |
| Stale text | `git grep -n -E "405 on .GET /mcp.|removed .*initialize" -- docs/usage docs/reference` | No matches |
| Symbols resolve | `git grep -n "MCPSessionStore" -- docs/reference/agent.md` | One directive line |
| Error codes | `git grep -n -E "\-32004|\-32005" -- docs/usage/agent-protocols.md` | At least one match each |
| Install tabs | `git grep -n '=== "uv"' -- docs/usage/agent-protocols.md` | Present wherever an install command appears |

## Verification

- **Baseline**: `make docs` exits `0` before edits.
- **Final**: `make docs && git grep -n -E "405 on .GET /mcp.|removed .*initialize" -- docs/usage docs/reference; test $? -eq 1`.

## Acceptance Criteria

- [ ] R17.1: usage guides and reference describe shipped behaviour; `[3/12 files documented]` after this task.

## Notes & Discoveries

- [2026-11-12 00:30] `docs/usage/a2a-and-workflows.md` already documents the `-32003` confirmation flow and the `_meta["vertebra/confirmationToken"]` key; only lifecycle details change.

[^usage-guide]: `docs/usage/agent-protocols.md` — MCP usage guide.
[^workflows-guide]: `docs/usage/a2a-and-workflows.md` — A2A, WebMCP, bridges, and confirmation guide.
[^agent-reference]: `docs/reference/agent.md` — `vertebra.agent` API reference.
