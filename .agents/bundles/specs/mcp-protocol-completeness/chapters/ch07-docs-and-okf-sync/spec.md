---
type: Spec
flow_id: mcp-protocol-completeness
chapter_id: ch07-docs-and-okf-sync
title: "Chapter 07: MCP Documentation and OKF Knowledge Synchronization"
description: "Update MCP/A2A guides, CLI docs, changelog, compact mcp-and-a2a-protocol-harness.md to <= 200 lines, and synchronize mcp-limitations.md and related OKF chapters."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch05-framework-mounts-and-litestar-parity
  - ch06-stdio-serve-bridge-and-gcp-auth
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: null
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - docs
  - okf
  - mcp
  - agent
sources:
  - id: usage-guide
    resource: docs/usage/agent-protocols.md
    title: MCP and agent protocols usage guide
  - id: cli-guide
    resource: docs/getting-started/cli.md
    title: CLI guide including vtb ai mcp commands
  - id: harness-okf
    resource: .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
    title: Agentic harness OKF chapter currently at 227 lines
  - id: mcp-lims
    resource: .agents/bundles/knowledge/domains/agent/mcp-limitations.md
    title: MCP implementation limitations OKF chapter
  - id: litestar-okf
    resource: .agents/bundles/knowledge/domains/extensions/litestar.md
    title: Litestar and framework extensions OKF chapter
---

# Chapter 07: MCP Documentation and OKF Knowledge Synchronization

<!-- truth: start -->
- `docs/usage/{agent-protocols,a2a-and-workflows}.md`, `docs/reference/agent.md`, `docs/getting-started/cli.md`, regenerated `docs/reference/cli.md`, and `docs/changelog/index.md` reflect dual-mode MCP, `GET` SSE, subscriptions, skills, MCP Apps UI, `IapIdentitySource`, `MCPCore`, and `vtb ai mcp serve`/`bridge --audience` [^usage-guide] [^cli-guide].
- `mcp-and-a2a-protocol-harness.md` (`227L -> <= 200L`), `mcp-limitations.md`, and `domains/index.md` update in place in present tense, removing closed limitations and recording remaining boundaries (per-process `MCPSessionStore`, stateless TS `McpClient`, header-only native peer limits, and host-extension `MCPRegistry` shims) [^harness-okf] [^mcp-lims].
- `domains/framework/cli.md`, `domains/extensions/litestar.md`, and `testing/polyglot-test-matrix.md` stay `<= 200` lines and pass `make review-check` [^litestar-okf].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Current docs and OKF chapters still state that `GET /mcp` unconditionally returns `405`, `initialize`/`resources/subscribe` are unsupported, `MCPService` is exported, and `vtb ai mcp serve` accepts `--name`, while `mcp-and-a2a-protocol-harness.md` is `227` lines (exceeding the `200`-line OKF cap) [^usage-guide] [^harness-okf] [^mcp-lims].

## 2. Requirements, Concrete Contracts & Architecture

- **`R7a` (`t01`)**: Update `docs/usage/agent-protocols.md`, `docs/usage/a2a-and-workflows.md`, and `docs/reference/agent.md` (`MCPCore`, `MCPSessionStore`, `IapIdentitySource`, `McpIcon`, `-32003`/`-32004`/`-32005`/`-32020`/`-32022`, single-use confirmations, skills, and SEP-1865 MCP Apps).
- **`R7b` (`t02`)**: Update `docs/getting-started/cli.md`, regenerate `docs/reference/cli.md` via `tools.docs.generate_cli_docs`, and add `Unreleased` entries linking [#109](https://github.com/cofin/vertebra/issues/109)–[#114](https://github.com/cofin/vertebra/issues/114) in `docs/changelog/index.md`.
- **`R7c` (`t03`)**: Compact and rewrite `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` (`227L -> <= 200L`), update `.agents/bundles/knowledge/domains/agent/mcp-limitations.md` in place with residual architectural boundaries, and update the `mcp-limitations.md` summary line in `.agents/bundles/knowledge/domains/index.md`.
- **`R7d` (`t04`)**: Sync `.agents/bundles/knowledge/domains/framework/cli.md`, `.agents/bundles/knowledge/domains/extensions/litestar.md` (`litestar-mcp>=0.20.0`), and `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`<= 200` lines each).

## 3. Documentation & OKF Knowledge Plan

All `12` documentation and OKF files for `mcp-protocol-completeness` are owned by `t01`–`t04`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R7a` | `t01` | `docs/usage/{agent-protocols,a2a-and-workflows}.md`, `docs/reference/agent.md` | `make docs` |
| `R7b` | `t02` | `docs/getting-started/cli.md`, `docs/reference/cli.md`, `docs/changelog/index.md` | `make docs` |
| `R7c` | `t03` | `mcp-and-a2a-protocol-harness.md`, `mcp-limitations.md`, `domains/index.md` | `make review-check` + line count `<= 200` |
| `R7d` | `t04` | `domains/framework/cli.md`, `domains/extensions/litestar.md`, `testing/polyglot-test-matrix.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-usage-guides-and-agent-reference.md](tasks/t01-usage-guides-and-agent-reference.md) — Update `agent-protocols.md`, `a2a-and-workflows.md`, and `docs/reference/agent.md`
- [ ] [t02-cli-docs-and-changelog.md](tasks/t02-cli-docs-and-changelog.md) — Update `getting-started/cli.md`, regenerate `docs/reference/cli.md`, and update `docs/changelog/index.md`
- [ ] [t03-harness-and-limitations-okf.md](tasks/t03-harness-and-limitations-okf.md) — Compact `mcp-and-a2a-protocol-harness.md` (`<= 200L`) and sync `mcp-limitations.md` and `domains/index.md`
- [ ] [t04-cli-litestar-and-test-matrix-okf.md](tasks/t04-cli-litestar-and-test-matrix-okf.md) — Sync `framework/cli.md`, `extensions/litestar.md`, and `testing/polyglot-test-matrix.md`

## 6. Current State & Remaining Plan Snapshot

- **Fast Chapter Gate (`< 40s`)**: `make docs && make review-check && uv run --no-sync pytest src/py/tests/unit/agent src/py/tests/unit/cli/test_mcp_cli.py src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q`.

[^usage-guide]: `docs/usage/agent-protocols.md` — MCP and agent protocols usage guide.
[^cli-guide]: `docs/getting-started/cli.md` — CLI guide including vtb ai mcp commands.
[^harness-okf]: `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` — Agentic harness OKF chapter currently at 227 lines.
[^mcp-lims]: `.agents/bundles/knowledge/domains/agent/mcp-limitations.md` — MCP implementation limitations OKF chapter.
[^litestar-okf]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar and framework extensions OKF chapter.
