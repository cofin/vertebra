---
type: Task
id: "mcp-protocol-completeness:ch11:t04"
chapter_id: ch11-docs-and-okf
title: Sync CLI, Litestar, and test-matrix OKF chapters
description: Update the vtb CLI OKF chapter for serve and bridge options, the Litestar extensions chapter for litestar-mcp>=0.20.0 delegation and well-known skills, and the polyglot test matrix for the new MCP conformance and CLI evidence.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "mcp-protocol-completeness:ch11:t03"
read_set:
  - .agents/bundles/knowledge/domains/framework/cli.md
  - .agents/bundles/knowledge/domains/extensions/litestar.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - .agents/skills/review-docs-okf/references/okf-frontmatter.md
files:
  - .agents/bundles/knowledge/domains/framework/cli.md
  - .agents/bundles/knowledge/domains/extensions/litestar.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
tests: []
estimated_loc: 120
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
  - id: cli-okf
    resource: .agents/bundles/knowledge/domains/framework/cli.md
    title: vtb CLI OKF chapter
  - id: litestar-okf
    resource: .agents/bundles/knowledge/domains/extensions/litestar.md
    title: Litestar extensions OKF chapter
  - id: test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: Polyglot test matrix
tags:
  - okf
---

# Task t04: Sync CLI, Litestar, and Test-Matrix OKF Chapters

## Objective & Scope Boundary

Remaining OKF chapters that mention MCP match the shipped behaviour [^cli-okf] [^litestar-okf] [^test-matrix]. Out of scope: user docs (`t01`, `t02`).

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/framework/cli.md` (`vtb ai mcp <subcmd>` table row).
- `.agents/bundles/knowledge/domains/extensions/litestar.md` (MCP delegation bullets and `sources:`).
- `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`## SQLSpec, MCP Conformance, and Native Client Boundary Evidence`).
- `.agents/skills/review-docs-okf/references/okf-frontmatter.md`.

## Symbol & Signature Contracts

- `cli.md`: the `vtb ai mcp` row lists `serve` (`--app`, `resolve_app_mcp_core`, Litestar delegation, fail-loud message) and `bridge` (`--url`, `--token`, `--audience`, `--timeout`, dual-mode forwarding, legacy notification relay, `-32001` on repeated `401`); `sources:` adds `src/py/vertebra/agent/mcp/bridge.py` and `src/py/vertebra/agent/mcp/stdio.py`.
- `litestar.md`: states that Litestar apps delegate `/mcp` to `litestar-mcp>=0.20.0` (dual-mode transport, sessions, `GET` SSE, subscriptions, skills, MCP Apps), that `create_mcp_plugin` fails loud on older installs, and that the Vertebra well-known document lists `litestar-mcp` skill names; Starlette, FastAPI, Flask, and Django bullets mention `skills=` and `MCPCore.validate()`.
- `polyglot-test-matrix.md`: the MCP evidence paragraph lists `test_mcp_conformance.py` coverage (negotiation, sessions, streams, subscriptions, skills, UI metadata, guards, confirmations), `test_core_and_observability.py` (context trust, broker streams, guard arity), `test_runtime_and_workflows.py` (confirmation replay), `test_mcp_cli.py` (stdio protocol, serve discovery, bridge dual mode and OIDC), and `test_litestar_ecosystem_mcp.py` (version guard, well-known skills), with `sources:` entries and footnotes for each test module.

## Edge Cases & Error Semantics

- Existing unrelated sections stay unchanged.
- Every new footnote id has a matching `sources:` entry.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation` through `make review-check`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Review gate | `make review-check` | Exit `0` |
| Old serve option | `git grep -n -E "serve. \(.--name" -- .agents/bundles/knowledge/domains/framework/cli.md` | No match |
| Litestar pin | `git grep -n "litestar-mcp>=0.20.0" -- .agents/bundles/knowledge/domains/extensions/litestar.md` | At least one match |
| Test modules cited | `git grep -n "test_mcp_cli.py" -- .agents/bundles/knowledge/testing/polyglot-test-matrix.md` | At least one match |

## Verification

- **Baseline**: `make review-check` exits `0` before edits.
- **Final**: `make review-check && make docs`.

## Acceptance Criteria

- [ ] R17.4: CLI, Litestar, and test-matrix chapters match shipped behaviour; `[12/12 files documented]` after this task.

## Notes & Discoveries

- [2026-11-12 00:30] At `c8722c76` the CLI chapter documents `serve (--name)`.

[^cli-okf]: `.agents/bundles/knowledge/domains/framework/cli.md` — `vtb` CLI OKF chapter.
[^litestar-okf]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar extensions OKF chapter.
[^test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot test matrix.
