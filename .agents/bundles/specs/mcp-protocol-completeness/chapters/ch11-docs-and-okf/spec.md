---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch11-docs-and-okf
title: "Chapter 11: Docs and OKF"
description: Update the MCP user guides, agent reference, CLI guide and regenerated reference, changelog, agent harness OKF chapter, new MCP limitations chapter, domain index, CLI and Litestar OKF chapters, and the polyglot test matrix to present-tense shipped behaviour.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch10-bridge-dual-mode-and-oidc
  - ch12-iap-identity-source
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: usage-guide
    resource: docs/usage/agent-protocols.md
    title: MCP usage guide
  - id: cli-guide
    resource: docs/getting-started/cli.md
    title: CLI guide with the vtb ai mcp bridge section
  - id: cli-docs-tool
    resource: tools/docs/generate_cli_docs.py
    title: CLI reference generator checked by make docs
  - id: harness-okf
    resource: .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
    title: Agent harness OKF chapter
  - id: limitations-precedent
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Existing limitations chapter format
tags:
  - docs
  - okf
---

# Chapter 11: Docs and OKF

<!-- truth: start -->
- **User docs**: `docs/usage/agent-protocols.md`, `docs/usage/a2a-and-workflows.md`, and `docs/reference/agent.md` describe dual-mode transport, sessions, `GET` SSE, subscriptions, skills, MCP Apps metadata, trusted identity headers, guards, error codes `-32003`/`-32004`/`-32005`/`-32022`, and single-use expiring confirmations [^usage-guide].
- **CLI docs**: `docs/getting-started/cli.md` documents `vtb ai mcp serve --app` and `vtb ai mcp bridge --audience`; `docs/reference/cli.md` is regenerated with `tools.docs.generate_cli_docs`; `docs/changelog/index.md` gains an `Unreleased` section linking [#109](https://github.com/cofin/vertebra/issues/109)–[#114](https://github.com/cofin/vertebra/issues/114) [^cli-guide] [^cli-docs-tool].
- **OKF**: the harness chapter, a new `domains/agent/mcp-limitations.md`, `domains/index.md`, `domains/framework/cli.md`, `domains/extensions/litestar.md`, and `testing/polyglot-test-matrix.md` are rewritten in present tense with `sources:` and matching footnotes [^harness-okf] [^limitations-precedent].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Current docs and OKF chapters state that `GET /mcp` returns `405`, that `initialize`, `ping`, `logging/setLevel`, and `resources/subscribe` are removed methods, and that `vtb ai mcp serve` takes `--name`. After `ch01`–`ch10` those statements are false. This chapter brings every affected page in line and records the caveats in a limitations chapter.

### 1.2 Upstream Execution Baseline

- Depends on `ch01`–`ch10` being completed (documents shipped behaviour only).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Notes |
| --- | --- | --- |
| Usage | `docs/usage/agent-protocols.md`, `docs/usage/a2a-and-workflows.md` [^usage-guide] | Stateless-only transport text and confirmation section. |
| Reference | `docs/reference/agent.md` | `:::` directives for MCP symbols. |
| CLI | `docs/getting-started/cli.md`, `docs/reference/cli.md`, `tools/docs/generate_cli_docs.py` [^cli-guide] [^cli-docs-tool] | `make docs` runs `generate_cli_docs --check`. |
| Changelog | `docs/changelog/index.md` | Released sections only (`0.3.0` first). |
| OKF | `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` [^harness-okf], `domains/index.md`, `domains/framework/cli.md`, `domains/extensions/litestar.md`, `testing/polyglot-test-matrix.md`, `domains/storage/object-storage-limitations.md` [^limitations-precedent] | Present-tense OKF v0.2 chapters. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [`review-docs-okf`](../../../../../skills/review-docs-okf/SKILL.md), [Bundle index](../../../../index.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R17.1**: Usage guides and agent reference.
- **R17.2**: CLI guide, regenerated CLI reference, changelog.
- **R17.3**: Harness chapter, limitations chapter, domain index.
- **R17.4**: CLI and Litestar OKF chapters, test matrix.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- Documentation only; documented symbols are those in the root spec Section 2.2.
- Limitations chapter frontmatter follows `object-storage-limitations.md` (`type`, `title`, `description`, `status: stable`, `tags`, `generated`, `verified`, `sources`).

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `make docs` -> `tools.docs.scan_public_docstrings`, `tools.docs.generate_cli_docs --check`, `zensical build --clean` -> rendered pages.
- `make review-check` -> `tools/scripts/review_setup.py` -> OKF frontmatter, links, privacy constraints.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Stale statement | Text claiming `GET /mcp` returns `405` unconditionally | Removed or qualified to `2026-07-28` | Present-tense truth |
| CLI drift | Hand-edited `docs/reference/cli.md` | Regenerated output only | `generate_cli_docs --check` passes |
| Broken link | Relative link to a moved file | `make review-check` fails until fixed | Link integrity |
| Missing provenance | New chapter without `sources:` | `make review-check` fails | OKF provenance |

### 2.5 Acceptance Scenarios

- Given the finished flow, when `make docs && make review-check` run, then both exit `0` and no page states that `initialize` is unsupported.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| Limitations location | New `domains/agent/mcp-limitations.md` | Scattering caveats across the harness chapter hides them from reviewers. |
| Changelog | `Unreleased` section | Editing the released `0.3.0` section rewrites published history. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/12 files documented]`
- `[ ]` `docs/usage/agent-protocols.md` (`t01`)
- `[ ]` `docs/usage/a2a-and-workflows.md` (`t01`)
- `[ ]` `docs/reference/agent.md` (`t01`)
- `[ ]` `docs/getting-started/cli.md` (`t02`)
- `[ ]` `docs/reference/cli.md` (`t02`)
- `[ ]` `docs/changelog/index.md` (`t02`)
- `[ ]` `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` (`t03`)
- `[ ]` `.agents/bundles/knowledge/domains/agent/mcp-limitations.md` (`t03`)
- `[ ]` `.agents/bundles/knowledge/domains/index.md` (`t03`)
- `[ ]` `.agents/bundles/knowledge/domains/framework/cli.md` (`t04`)
- `[ ]` `.agents/bundles/knowledge/domains/extensions/litestar.md` (`t04`)
- `[ ]` `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`t04`)

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Doc Gate |
| --- | --- | --- | --- |
| R17.1 | [t01](tasks/t01-usage-guides-and-agent-reference.md) | `documentation_validation` | `make docs` |
| R17.2 | [t02](tasks/t02-cli-docs-and-changelog.md) | `documentation_validation` | `make docs` |
| R17.3 | [t03](tasks/t03-harness-and-limitations-okf.md) | `documentation_validation` | `make review-check` |
| R17.4 | [t04](tasks/t04-cli-litestar-and-test-matrix-okf.md) | `documentation_validation` | `make review-check` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch11:t01 — Update MCP usage guides and the agent reference (tasks/t01-usage-guides-and-agent-reference.md)
- [ ] mcp-protocol-completeness:ch11:t02 — Update the CLI guide, regenerate the CLI reference, and add changelog entries (tasks/t02-cli-docs-and-changelog.md)
- [ ] mcp-protocol-completeness:ch11:t03 — Rewrite the harness OKF chapter and add the MCP limitations chapter (tasks/t03-harness-and-limitations-okf.md)
- [ ] mcp-protocol-completeness:ch11:t04 — Sync CLI, Litestar, and test-matrix OKF chapters (tasks/t04-cli-litestar-and-test-matrix-okf.md)

**Chapter Recovery Checkpoint:** `make docs && make review-check && uv run --no-sync pytest src/py/tests/unit/agent src/py/tests/unit/cli/test_mcp_cli.py src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/4` tasks completed; `[0/12 files documented]`.
- **Downstream Dependents**: Flow finish and archive.

[^usage-guide]: `docs/usage/agent-protocols.md` — MCP usage guide.
[^cli-guide]: `docs/getting-started/cli.md` — CLI guide with the `vtb ai mcp bridge` section.
[^cli-docs-tool]: `tools/docs/generate_cli_docs.py` — CLI reference generator checked by `make docs`.
[^harness-okf]: `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` — Agent harness OKF chapter.
[^limitations-precedent]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Existing limitations chapter format.
