---
type: Task
id: "mcp-protocol-completeness:ch07:t04"
flow_id: mcp-protocol-completeness
chapter_id: ch07-docs-and-okf-sync
title: "Sync CLI, Litestar Extension, and Polyglot Test Matrix OKF Chapters"
description: "Update domains/framework/cli.md, domains/extensions/litestar.md, and testing/polyglot-test-matrix.md to reflect shipped MCP CLI, Litestar 0.20+, and conformance coverage."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch07:t03"
files:
  - .agents/bundles/knowledge/domains/framework/cli.md
  - .agents/bundles/knowledge/domains/extensions/litestar.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
tests: []
read_set:
  - .agents/bundles/knowledge/domains/framework/cli.md
  - .agents/bundles/knowledge/domains/extensions/litestar.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - src/py/vertebra/cli/ai.py
  - src/py/vertebra/extensions/litestar/mcp/plugin.py
estimated_loc: 115
tags:
  - okf
  - cli
  - litestar
  - testing
sources:
  - id: cli-okf
    resource: .agents/bundles/knowledge/domains/framework/cli.md
    title: vtb CLI domain OKF chapter
  - id: litestar-okf
    resource: .agents/bundles/knowledge/domains/extensions/litestar.md
    title: Litestar and framework extensions OKF chapter
  - id: test-okf
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: Polyglot test matrix OKF chapter
---

# Task `t04`: Sync CLI, Litestar Extension, and Polyglot Test Matrix OKF Chapters

## Objective & Scope Boundary

Synchronize `framework/cli.md`, `extensions/litestar.md`, and `testing/polyglot-test-matrix.md` (`<= 200` lines each) per [`../spec.md` §2](../spec.md) [^cli-okf] [^litestar-okf] [^test-okf].

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/{framework/cli,extensions/litestar}.md` & `testing/polyglot-test-matrix.md`.

## Contracts & Edge Cases

- Update `cli.md` with `vtb ai mcp serve --app` (`resolve_app_mcp_core`) and `vtb ai mcp bridge --audience` (`OidcTokenSource`).
- Update `litestar.md` with `litestar-mcp>=0.20.0` dual-mode delegation and well-known skill merging, and update `polyglot-test-matrix.md` with dual-mode MCP conformance + CLI/bridge test coverage (`<= 200` lines each).

## Test Matrix

- Verified via `make review-check` and `<= 200`-line check.

## Verification

```bash
python3 -c 'import pathlib; [assert_ok := (len(pathlib.Path(p).read_text().splitlines()) <= 200) or (_ for _ in ()).throw(AssertionError(p)) for p in [".agents/bundles/knowledge/domains/framework/cli.md", ".agents/bundles/knowledge/domains/extensions/litestar.md", ".agents/bundles/knowledge/testing/polyglot-test-matrix.md"]]' && make review-check
```

## Acceptance Criteria

- [ ] All three OKF chapters stay `<= 200` lines with valid `sources:` footnotes and `verified.commit`.
- [ ] `make review-check` exits `0`.

[^cli-okf]: `.agents/bundles/knowledge/domains/framework/cli.md` — vtb CLI domain OKF chapter.
[^litestar-okf]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar and framework extensions OKF chapter.
[^test-okf]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot test matrix OKF chapter.
