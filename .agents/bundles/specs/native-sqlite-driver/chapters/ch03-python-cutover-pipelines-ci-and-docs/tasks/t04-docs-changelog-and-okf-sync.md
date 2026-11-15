---
type: Task
id: native-sqlite-driver:ch03-python-cutover-pipelines-ci-and-docs:t04
flow_id: native-sqlite-driver
chapter_id: ch03-python-cutover-pipelines-ci-and-docs
title: Synchronize Public Documentation, Changelog, OKF SQLite Chapters, and Review Skills
description: Update databases/native-stubs docs, changelog, AGENTS.md, OKF sqlite-limitations.md and database-drivers/workflow chapters, and review-pyo3-bridge skill.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 473e1f2e
depends_on:
  - native-sqlite-driver:ch03-python-cutover-pipelines-ci-and-docs:t03
files:
  - docs/usage/databases.md
  - docs/reference/native-stubs.md
  - docs/changelog/index.md
  - .agents/bundles/knowledge/domains/data/sqlite-limitations.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/skills/review-pyo3-bridge/SKILL.md
  - AGENTS.md
  - .agents/bundles/knowledge/workflow.md
tests: []
read_set:
  - docs/usage/databases.md
  - docs/reference/native-stubs.md
  - docs/changelog/index.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - AGENTS.md
  - .agents/bundles/knowledge/workflow.md
estimated_loc: 220
tags:
  - docs
  - sqlite
  - changelog
  - okf
  - limitations
  - skills
  - workflow
sources:
  - id: docs-db
    resource: docs/usage/databases.md
    title: Database drivers usage guide
  - id: docs-stubs
    resource: docs/reference/native-stubs.md
    title: Native extension stub API reference
  - id: docs-changelog
    resource: docs/changelog/index.md
    title: Release changelog
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Database drivers and dialers OKF chapter
  - id: domains-idx
    resource: .agents/bundles/knowledge/domains/index.md
    title: OKF domains catalog index
  - id: review-pyo3-skill
    resource: .agents/skills/review-pyo3-bridge/SKILL.md
    title: PyO3 bridge review skill
  - id: agents-md
    resource: AGENTS.md
    title: Repository agent instructions and quick verification commands
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: Project Makefile and verification workflow chapter
verification_strategy: documentation_validation
---

# Task `t04`: Synchronize Public Documentation, Changelog, OKF SQLite Chapters, and Review Skills

## Objective & Scope Boundary

Document `vertebra-core[sqlite]`, `SyncSQLitePipeline`/`AsyncSQLitePipeline`, `backup_to`/`restore_from`, and `vertebra._sqlite` stubs in `docs/`, author `sqlite-limitations.md` (`<= 200` lines), update `database-drivers-and-dialers.md` and `domains/index.md`, and synchronize `vertebra-sqlite`, `make sqlite-test`, and `make sqlite-wheel-check` across `review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `.agents/bundles/knowledge/workflow.md` per [`../spec.md` §2](../spec.md) [^docs-db] [^docs-stubs] [^docs-changelog] [^okf-drivers] [^domains-idx] [^review-pyo3-skill] [^agents-md] [^okf-workflow].

## Bounded Read Manifest

- `docs/usage/databases.md` — SQLite configuration, WAL, pipeline, backup, and Arrow examples.
- `docs/reference/native-stubs.md` — TOC structure and `heading_level: 4` mkdocstrings blocks.
- `docs/changelog/index.md` — `Unreleased` changelog section.
- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Driver wheel architecture chapter.
- `AGENTS.md` — Rust crate list and Quick Verification Commands block.
- `.agents/bundles/knowledge/workflow.md` — Canonical Makefile target catalog (`<= 200` lines).

## Contracts & Edge Cases

- Format install blocks with `=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"` tabs and declare public `msgspec.Struct` examples without internal `frozen=True, gc=False` boilerplate.
- Keep `docs/reference/native-stubs.md` within `<= 14` secondary TOC items and `< 120_000` HTML bytes using `heading_level: 4`.
- Record WAL single-writer serialization, `:memory:` single-worker affinity vs shared URI, dynamic SQLite expression affinity in `VTBARROW`, and disabled `load_extension` in `sqlite-limitations.md`.
- Keep `sqlite-limitations.md`, `database-drivers-and-dialers.md`, `review-pyo3-bridge/SKILL.md`, and `workflow.md` `<= 200` lines each, preserve `review-pyo3-bridge` `Suppressions Index` and `FP-FFI-*` entries verbatim, and keep every command in `AGENTS.md` identical to `Makefile` (`DOC-08`).
- **Edge Case**: Keep `domains/index.md` free of YAML frontmatter and standalone `---` rules.

## Test Matrix

- `uv run --no-sync python -m tools.docs.scan_public_docstrings` passes and Zensical output passes `tools/docs/verify_docs_artifacts.py`.
- `make review-check` exits `0` with zero OKF, link, truth-budget, line-cap, or `DOC-08` errors.

## Verification

```bash
uv run --no-sync python -m tools.docs.scan_public_docstrings && make review-check
```

## Acceptance Criteria

- [ ] Public docs and `Unreleased` changelog accurately cover `vertebra._sqlite`, `vertebra-core[sqlite]`, pipelines, and backups.
- [ ] `sqlite-limitations.md`, `database-drivers-and-dialers.md`, `AGENTS.md`, `workflow.md`, and `review-pyo3-bridge/SKILL.md` pass `make review-check`.
- [ ] Zero regressions in the fast verification gate.

[^docs-db]: `docs/usage/databases.md` — Database drivers usage guide.
[^docs-stubs]: `docs/reference/native-stubs.md` — Native extension stub API reference.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
[^review-pyo3-skill]: `.agents/skills/review-pyo3-bridge/SKILL.md` — PyO3 bridge review skill.
[^agents-md]: `AGENTS.md` — Repository agent instructions and quick verification commands.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project Makefile and verification workflow chapter.
