---
type: Task
id: "mcp-protocol-completeness:ch11:t02"
chapter_id: ch11-docs-and-okf
title: Update the CLI guide, regenerate the CLI reference, and add changelog entries
description: Document vtb ai mcp serve --app and vtb ai mcp bridge --audience in the CLI guide, regenerate docs/reference/cli.md with tools.docs.generate_cli_docs, and add an Unreleased changelog section linking issues 109 through 114.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "mcp-protocol-completeness:ch11:t01"
read_set:
  - docs/getting-started/cli.md
  - docs/reference/cli.md
  - docs/changelog/index.md
  - tools/docs/generate_cli_docs.py
  - src/py/vertebra/cli/ai.py
files:
  - docs/getting-started/cli.md
  - docs/reference/cli.md
  - docs/changelog/index.md
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
  - id: cli-guide
    resource: docs/getting-started/cli.md
    title: CLI guide
  - id: cli-generator
    resource: tools/docs/generate_cli_docs.py
    title: CLI reference generator
  - id: changelog
    resource: docs/changelog/index.md
    title: Release changelog
tags:
  - docs
  - cli
---

# Task t02: Update the CLI Guide, Regenerate the CLI Reference, and Add Changelog Entries

## Objective & Scope Boundary

CLI documentation matches the shipped `vtb ai mcp` commands and the changelog records user-visible changes [^cli-guide] [^cli-generator] [^changelog]. Out of scope: OKF (`t03`, `t04`).

## Bounded Read Manifest

- `docs/getting-started/cli.md` (`### MCP Stdio-to-HTTP Bridge (vtb ai mcp)` section).
- `docs/reference/cli.md` (generated).
- `docs/changelog/index.md` (`## 0.3.0` structure: `### Added`, `### Changed`, `### Fixed`).
- `tools/docs/generate_cli_docs.py`.
- `src/py/vertebra/cli/ai.py` (`serve`, `bridge` options).

## Symbol & Signature Contracts

- `cli.md` MCP section documents: `vtb ai mcp serve [--app module:attr]` (resolution order, Litestar delegation, failure message), `vtb ai mcp bridge --url URL [--token TOKEN | --audience AUDIENCE] [--timeout S]` (dual-mode forwarding, legacy notification relay, OIDC refresh, `VERTEBRA_MCP_TOKEN`).
- `reference/cli.md` is produced only by `PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.generate_cli_docs` (no hand edits).
- `changelog/index.md` gains `## Unreleased` above `## 0.3.0` with `### Added` (dual-mode MCP transport and sessions [#109](https://github.com/cofin/vertebra/issues/109), MCP Apps metadata [#110](https://github.com/cofin/vertebra/issues/110), skills extension [#111](https://github.com/cofin/vertebra/issues/111), resource subscriptions [#112](https://github.com/cofin/vertebra/issues/112), `vtb ai mcp serve` / `bridge --audience` [#114](https://github.com/cofin/vertebra/issues/114)), `### Changed` (`vtb ai mcp serve` drops `--name`; `run_stdio_http_bridge` takes `token_provider`; unknown MCP methods return HTTP `404`; `notify_resource_updated` is async; `ConfirmationGate.is_call_approved` replaced by `consume_call_approval`; `litestar-mcp>=0.20.0`), and `### Fixed` (context propagation, single-pass guards, single-use expiring confirmations, task cancellation) with exact class and function names; no version-bump entries.

## Edge Cases & Error Semantics

- The generator output must match `--check` exactly; formatting differences are fixed by regenerating, never by editing.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation` through `make docs`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Generator check | `PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.generate_cli_docs --check` | Exit `0` |
| `--name` gone | `git grep -n -- "--name" -- docs/reference/cli.md docs/getting-started/cli.md` | No match in the `vtb ai mcp serve` section |
| `--audience` present | `git grep -n -- "--audience" -- docs/reference/cli.md docs/getting-started/cli.md` | Matches in both files |
| Changelog links | `git grep -n -E "issues/(109|110|111|112|114)" -- docs/changelog/index.md` | Five matches |
| Build | `make docs` | Exit `0` |

## Verification

- **Baseline**: `PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.generate_cli_docs --check` reports drift after `ch09:t02` and `ch10:t02`.
- **Final**: `PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.generate_cli_docs && make docs`.

## Acceptance Criteria

- [ ] R17.2: CLI guide, regenerated reference, and changelog match shipped behaviour; `[6/12 files documented]` after this task.

## Notes & Discoveries

- [2026-11-12 00:30] `make docs` runs `generate_cli_docs --check`, so any CLI option change without regeneration fails the docs gate.

[^cli-guide]: `docs/getting-started/cli.md` — CLI guide.
[^cli-generator]: `tools/docs/generate_cli_docs.py` — CLI reference generator.
[^changelog]: `docs/changelog/index.md` — Release changelog.
