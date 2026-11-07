---
type: Task
id: "native-postgres-driver:ch30:t02"
chapter_id: ch30-user-and-domain-docs
title: Synchronize README, workflow knowledge chapter, and AGENTS instructions for vertebra-postgres
description: Synchronize 9 Rust crates (vertebra-postgres), 6 PyO3 extensions (_postgres), the [postgres] extra, and postgres-test / postgres-wheel-check / postgres-bench / postgres-bench-check Make targets across README.md, workflow.md (<= 200 lines), and AGENTS.md (<= 100 lines).
status: stable
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch30:t01"
read_set:
  - README.md
  - .agents/bundles/knowledge/workflow.md
  - AGENTS.md
  - Makefile
  - pyproject.toml
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - README.md
  - .agents/bundles/knowledge/workflow.md
  - AGENTS.md
tests: []
estimated_loc: 160
plan_revision: 3
plan_commit: dd432f18
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T16:45:00Z"
commit: null
sources:
  - id: repo-readme
    resource: README.md
    title: Repository overview, crate layout, and canonical Make targets
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: OKF workflow chapter for Makefile targets, rebuild cycle, and verification
  - id: repo-agents
    resource: AGENTS.md
    title: Root repository agent instructions and quick verification commands
  - id: root-makefile
    resource: Makefile
    title: Canonical workspace build, test, wheel, and benchmark targets
  - id: root-pyproject
    resource: pyproject.toml
    title: Optional [postgres] extra and [dependency-groups].bench configuration
tags:
  - postgres
  - documentation
  - okf
  - workflow
  - agents
---

# Task t02: Synchronize `README.md`, `workflow.md`, and `AGENTS.md` for `vertebra-postgres`

## Objective & Scope Boundary

Bring repository entry-point and workflow documentation in `README.md`, `.agents/bundles/knowledge/workflow.md`, and `AGENTS.md` into exact parity with the `9`-crate / `6`-PyO3-extension workspace while enforcing strict `DOC-08` line budgets [^repo-readme] [^okf-workflow] [^repo-agents] [^root-makefile] [^root-pyproject]:
1. `README.md`:
   - Include `vertebra-postgres` (`_postgres`, `vertebra-core[postgres]`) in the `Storage, Databases & TypeScript Client` architecture bullet and `src/rs/` repository layout tree.
   - Add `make postgres-test`, `make postgres-wheel-check`, `make postgres-bench`, and `make postgres-bench-check` under `Common Make Targets`.
2. `.agents/bundles/knowledge/workflow.md` (**strictly `<= 200` lines** per `DOC-08`):
   - Update in-place from `five` to `six` PyO3 extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`, `_postgres` from `src/rs/vertebra-postgres`), document `[postgres]` optional extra vs `--group bench` (`psycopg`/`asyncpg`) isolation, and register `postgres-test`, `postgres-wheel-check`, `postgres-bench`, and `postgres-bench-check` without exceeding `200` total lines, keeping the truth block `<= 40` lines and `sources:`-to-footnote parity intact.
3. `AGENTS.md` (**strictly `<= 100` lines** per `DOC-08`):
   - Add `vertebra-postgres` to the opening Rust runtime crate list, update `make dev` / `make py-test` / `make build` inline command summaries from `5` to `6` extensions (`_postgres`), and add concise entries for `make postgres-test`, `make postgres-wheel-check`, `make postgres-bench`, and `make postgres-bench-check` under `## Quick Verification Commands`, preserving `<!-- project-customization: start --> ... <!-- project-customization: end -->` verbatim and keeping total lines `<= 100`.

Out of scope: Modifying `Makefile` (`ch29:t03`) or data/extensions/testing/product OKF chapters (`ch30:t03`–`t04`).

## Bounded Read Manifest

- [README.md](../../../../../../../README.md) — Top-level crate summary, `Common Make Targets`, and `Repository Layout` [^repo-readme].
- [.agents/bundles/knowledge/workflow.md](../../../../../knowledge/workflow.md) — `192`-line workflow chapter requiring concise in-place edits under `200` lines [^okf-workflow].
- [AGENTS.md](../../../../../../../AGENTS.md) — `63`-line root agent instructions requiring `<= 100` lines [^repo-agents].
- [Makefile](../../../../../../../Makefile) — Exact `dev`, `py-test`, `build`, and `postgres-*` target behavior [^root-makefile].
- [pyproject.toml](../../../../../../../pyproject.toml) — `[project.optional-dependencies].postgres` and `[dependency-groups].bench` [^root-pyproject].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Standalone wheel and namespace contracts.

## Symbol & Signature Contracts

- `AGENTS.md`: total line count `<= 100`; `<!-- project-customization: start --> ... <!-- project-customization: end -->` block preserved untouched.
- `.agents/bundles/knowledge/workflow.md`: total line count `<= 200`; `<!-- truth: start --> ... <!-- truth: end -->` `<= 40` lines immediately after `# H1`; multi-line YAML frontmatter with fresh prepended `verified:` entry and 1:1 `sources:` <-> inline citation <-> bottom footnote parity; zero standalone `---` body lines.

## Edge Cases & Error Semantics

- `workflow.md` is already near the `200`-line `DOC-08` ceiling (`192` lines at baseline); tighten phrasing where `5` extensions are listed in-place rather than appending verbose multi-line blocks so `wc -l` stays `<= 200`.
- `README.md` already contains a decorative `---` after its HTML header outside `.agents/bundles/`; keep `README.md` edits surgical to the crate summary, Make targets block, and `src/rs/` layout tree.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check`, `make docs`, and explicit `wc -l` line-budget assertions (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `DOC-08` line-budget enforcement | Check line counts on `AGENTS.md` and `.agents/bundles/knowledge/workflow.md` | `AGENTS.md <= 100` lines and `workflow.md <= 200` lines. |
| OKF v0.2 schema & provenance gate | `make review-check` | Exits `0`; `workflow.md` passes YAML block syntax, truth-block bounds, `sources:` path existence, provenance footnote parity, and privacy/cliché checks. |
| Crate, extension, & Make target parity | Audit `README.md`, `workflow.md`, and `AGENTS.md` + run `make docs` | All three list `9` Rust crates (`vertebra-postgres`), `6` PyO3 extensions (`_postgres`), and `postgres-test` / `postgres-wheel-check` / `postgres-bench` / `postgres-bench-check`; `make docs` exits `0`. |

## Verification

- Line-budget & review validator gate: `test "$(wc -l < AGENTS.md)" -le 100 && test "$(wc -l < .agents/bundles/knowledge/workflow.md)" -le 200 && make review-check`
- Documentation build gate: `make docs`

## Acceptance Criteria

- [ ] `README.md`, `.agents/bundles/knowledge/workflow.md`, and `AGENTS.md` synchronize `vertebra-postgres` (`_postgres`), `[postgres]` extra, and `postgres-*` Make targets in present tense (R27).
- [ ] `AGENTS.md` stays `<= 100` lines, `workflow.md` stays `<= 200` lines, and `make review-check` + `make docs` exit `0` (R27).

## Notes & Discoveries

- [2026-11-07 16:45] Keep `AGENTS.md` `<!-- project-customization: start --> ... <!-- project-customization: end -->` lines untouched so `/flow:setup` upgrades preserve project customizations cleanly.

[^repo-readme]: `README.md` — Repository overview, crate layout, and canonical Make targets.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — OKF workflow chapter for Makefile targets, rebuild cycle, and verification.
[^repo-agents]: `AGENTS.md` — Root repository agent instructions and quick verification commands.
[^root-makefile]: `Makefile` — Canonical workspace build, test, wheel, and benchmark targets.
[^root-pyproject]: `pyproject.toml` — Optional `[postgres]` extra and `[dependency-groups].bench` configuration.
