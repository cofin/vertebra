---
type: Task
id: "mcp-protocol-completeness:ch07:t02"
flow_id: mcp-protocol-completeness
chapter_id: ch07-docs-and-okf-sync
title: "Sync CLI Guide, Regenerate CLI Reference, and Update Changelog"
description: "Document vtb ai mcp serve --app and bridge --audience, regenerate docs/reference/cli.md, and add Unreleased entries in docs/changelog/index.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch07:t01"
files:
  - docs/getting-started/cli.md
  - docs/reference/cli.md
  - docs/changelog/index.md
tests: []
read_set:
  - docs/getting-started/cli.md
  - docs/reference/cli.md
  - docs/changelog/index.md
  - src/py/vertebra/cli/ai.py
  - tools/docs/generate_cli_docs.py
estimated_loc: 120
tags:
  - docs
  - cli
  - changelog
  - mcp
sources:
  - id: cli-guide
    resource: docs/getting-started/cli.md
    title: Getting-started CLI guide
  - id: cli-gen
    resource: tools/docs/generate_cli_docs.py
    title: CLI reference doc generator
  - id: changelog
    resource: docs/changelog/index.md
    title: Release changelog
---

# Task `t02`: Sync CLI Guide, Regenerate CLI Reference, and Update Changelog

## Objective & Scope Boundary

Update `docs/getting-started/cli.md`, regenerate `docs/reference/cli.md`, and add `Unreleased` MCP entries to `docs/changelog/index.md` per [`../spec.md` §2](../spec.md) [^cli-guide] [^cli-gen] [^changelog].

## Bounded Read Manifest

- `docs/getting-started/cli.md`, `docs/reference/cli.md`, `docs/changelog/index.md`, & `src/py/vertebra/cli/ai.py`.

## Contracts & Edge Cases

- Document `vtb ai mcp serve --app` and `vtb ai mcp bridge --audience` in `getting-started/cli.md` and regenerate `docs/reference/cli.md` via `tools.docs.generate_cli_docs`.
- Add `Unreleased` user-visible entries in `docs/changelog/index.md` naming exact classes/functions (`MCPCore`, `MCPSessionStore`, `IapIdentitySource`, `OidcTokenSource`, `SkillCatalog`, `ConfirmationGate.consume_call_approval`) with issue links [#109](https://github.com/cofin/vertebra/issues/109)–[#114](https://github.com/cofin/vertebra/issues/114) and zero internal audit ceremony.

## Test Matrix

- Verified via `tools.docs.generate_cli_docs --check` and `make docs`.

## Verification

```bash
uv run --no-sync python -m tools.docs.generate_cli_docs --check && make docs
```

## Acceptance Criteria

- [ ] `docs/reference/cli.md` passes `--check` and `docs/changelog/index.md` follows repository conventions.
- [ ] `make docs` exits `0`.

[^cli-guide]: `docs/getting-started/cli.md` — Getting-started CLI guide.
[^cli-gen]: `tools/docs/generate_cli_docs.py` — CLI reference doc generator.
[^changelog]: `docs/changelog/index.md` — Release changelog.
