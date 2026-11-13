---
type: Task
id: "mcp-protocol-completeness:ch07:t03"
flow_id: mcp-protocol-completeness
chapter_id: ch07-docs-and-okf-sync
title: Compact mcp-and-a2a-protocol-harness.md (<= 200L) and Sync mcp-limitations.md
description: "Rewrite mcp-and-a2a-protocol-harness.md within <= 200 lines, update mcp-limitations.md in place with residual caveats, and sync domains/index.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: []
files:
  - .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
  - .agents/bundles/knowledge/domains/agent/mcp-limitations.md
  - .agents/bundles/knowledge/domains/index.md
tests: []
read_set:
  - .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
  - .agents/bundles/knowledge/domains/agent/mcp-limitations.md
  - .agents/bundles/knowledge/domains/index.md
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/vertebra/agent/core/identity.py
estimated_loc: 200
tags:
  - okf
  - mcp
  - limitations
  - agent
sources:
  - id: harness-okf
    resource: .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
    title: Agentic harness OKF chapter at 227 lines
  - id: mcp-lims
    resource: .agents/bundles/knowledge/domains/agent/mcp-limitations.md
    title: MCP limitations OKF chapter
  - id: domains-idx
    resource: .agents/bundles/knowledge/domains/index.md
    title: Domain vocabulary index
---

# Task `t03`: Compact mcp-and-a2a-protocol-harness.md (<= 200L) and Sync mcp-limitations.md

## Objective & Scope Boundary

Compact `mcp-and-a2a-protocol-harness.md` (`227L -> <= 200L`) and update `mcp-limitations.md` + `domains/index.md` in place per [`../spec.md` §2](../spec.md) [^harness-okf] [^mcp-lims] [^domains-idx].

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/agent/{mcp-and-a2a-protocol-harness,mcp-limitations}.md` & `domains/index.md`.
- `src/py/vertebra/agent/{mcp/service,mcp/sessions,core/identity}.py`.

## Contracts & Edge Cases

- Compact `mcp-and-a2a-protocol-harness.md` to `<= 200` lines (`<= 40`-line truth block), covering dual-mode versions, `MCPSessionStore`, `GET` SSE, `resources/subscribe`, `subscriptions/listen`, `SKILLS_EXTENSION`, `UI_EXTENSION`, `IapIdentitySource`, `OidcTokenSource`, and `MCPCore`.
- Update `mcp-limitations.md` in place (`<= 200` lines) removing resolved gaps and recording remaining boundaries: per-process in-memory `MCPSessionStore`, stateless `2026-07-28`-only `@vertebra/client` `McpClient`, header-only native route peer limits for `MtlsIdentitySource`, and host-extension `MCPRegistry(ToolRegistry)` shims; update line 16 of `domains/index.md` to match.

## Test Matrix

- Verified via `make review-check` and `<= 200`-line assertion on both OKF chapters.

## Verification

```bash
python3 -c 'import pathlib; [assert_ok := (len(pathlib.Path(p).read_text().splitlines()) <= 200) or (_ for _ in ()).throw(AssertionError(p)) for p in [".agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md", ".agents/bundles/knowledge/domains/agent/mcp-limitations.md"]]' && make review-check
```

## Acceptance Criteria

- [ ] `mcp-and-a2a-protocol-harness.md` and `mcp-limitations.md` are `<= 200` lines with valid `verified.commit` and footnotes.
- [ ] `make review-check` exits `0`.

[^harness-okf]: `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` — Agentic harness OKF chapter at 227 lines.
[^mcp-lims]: `.agents/bundles/knowledge/domains/agent/mcp-limitations.md` — MCP limitations OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — Domain vocabulary index.
