---
type: Spec
flow_id: native-sqlite-driver
chapter_id: ch05-docs-and-knowledge-sync
title: "Chapter 05: Public Documentation, Stub Reference, Review Skill, and OKF Synchronization"
description: "Document vertebra-core[sqlite] usage, vertebra._sqlite stubs, sqlite-limitations.md, database-drivers-and-dialers.md, AGENTS.md, workflow.md, and review-pyo3-bridge/SKILL.md."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
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
  - sqlite
  - limitations
sources:
  - id: docs-db
    resource: docs/usage/databases.md
    title: "Database drivers usage guide"
  - id: docs-stubs
    resource: docs/reference/native-stubs.md
    title: "Native extension stub API reference"
  - id: docs-changelog
    resource: docs/changelog/index.md
    title: "Release changelog"
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Database drivers and dialers OKF chapter"
  - id: domains-idx
    resource: .agents/bundles/knowledge/domains/index.md
    title: "OKF domains catalog index"
  - id: review-pyo3-skill
    resource: .agents/skills/review-pyo3-bridge/SKILL.md
    title: "PyO3 bridge review skill"
  - id: agents-md
    resource: AGENTS.md
    title: "Repository agent instructions and quick verification commands"
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: "Project Makefile and verification workflow chapter"
---

# Chapter 05: Public Documentation, Stub Reference, Review Skill, and OKF Synchronization

<!-- truth: start -->
- `docs/usage/databases.md`, `docs/reference/native-stubs.md` (`heading_level: 4`, `<= 14` TOC items, `< 120_000` bytes), and `docs/changelog/index.md` document `vertebra-core[sqlite]`, `SyncSQLitePipeline`/`AsyncSQLitePipeline`, `backup_to`/`restore_from`, and `vertebra._sqlite` [^docs-db] [^docs-stubs] [^docs-changelog].
- `sqlite-limitations.md` (`<= 200` lines) records single-writer WAL serialization, `:memory:` single-worker affinity vs shared URI, dynamic SQLite expression affinity in `VTBARROW`, and disabled `load_extension` [^okf-drivers] [^domains-idx].
- `review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `.agents/bundles/knowledge/workflow.md` list `vertebra-sqlite`, `make sqlite-test`, and `make sqlite-wheel-check` [^review-pyo3-skill] [^agents-md] [^okf-workflow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Shipping a new standalone native wheel requires synchronized installation tabs (`uv`, `pip`, `pdm`, `poetry`), `native-stubs.md` API documentation within Zensical TOC/byte limits, an OKF limitations record (`<= 200` lines), and updated `AGENTS.md` / `workflow.md` / `review-pyo3-bridge` inventories [^docs-stubs] [^agents-md].

**Non-Goals:** Redis/Valkey documentation (owned by `native-redis-driver`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Public docs | `docs/usage/databases.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md` [^docs-db] [^docs-stubs] [^docs-changelog] | User docs, stub API reference, and `Unreleased` changelog |
| OKF data chapters | `knowledge/domains/data/{sqlite-limitations,database-drivers-and-dialers}.md`, `domains/index.md` [^okf-drivers] [^domains-idx] | Architecture and limitations chapters (`<= 200` lines) |
| Workflow & skills | `review-pyo3-bridge/SKILL.md`, `AGENTS.md`, `knowledge/workflow.md` [^review-pyo3-skill] [^agents-md] [^okf-workflow] | `DOC-08` `Makefile` target parity and review skill sync |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R14a**: Update `docs/usage/databases.md`, `docs/reference/native-stubs.md` (`heading_level: 4`), and `docs/changelog/index.md` (`t01`).
- **R14b**: Create `.agents/bundles/knowledge/domains/data/sqlite-limitations.md` (`<= 200` lines), update `database-drivers-and-dialers.md`, and index in `domains/index.md` (`t02`).
- **R14c**: Update `.agents/skills/review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `.agents/bundles/knowledge/workflow.md` (`t03`).

```text
sqlite-limitations.md required caveats (<= 200 lines):
1. Single-writer WAL serialization (cross-process writers exceeding busy_timeout_ms surface transient OperationalError SQLITE_BUSY).
2. Plain :memory: pins to 1 worker thread; multi-reader in-memory concurrency requires file:...?mode=memory&cache=shared.
3. Dynamic SQLite expression columns without declared affinity infer VTBARROW schema from the first non-null row.
4. Runtime load_extension is disabled by default for FFI safety.
```

**Integration Flow:**
- `make docs` runs `scan_public_docstrings`, Zensical build, and `verify_docs_artifacts.py` (`<= 14` TOC items, `< 120_000` bytes).
- `make review-check` verifies `DOC-01`..`DOC-09` across `AGENTS.md`, skills, and OKF chapters.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Reference TOC overflow | Adding top-level `##` per class in `native-stubs.md` | Group under embedded engines with `heading_level: 4` (`<= 14` TOC items) | Zensical artifact budget |
| `DOC-08` command drift | `Makefile` adds `sqlite-test` without `AGENTS.md`/`workflow.md` | Synchronized in `t03` | Workflow command parity |

## 3. Documentation & OKF Knowledge Plan

This chapter completes `[9/9]` documentation, skill, and OKF deliverables for `native-sqlite-driver`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R14a` | `t01` | `docs/usage/databases.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md` | `make docs` |
| `R14b` | `t02` | `knowledge/domains/data/{sqlite-limitations,database-drivers-and-dialers}.md`, `domains/index.md` | `make review-check` |
| `R14c` | `t03` | `review-pyo3-bridge/SKILL.md`, `AGENTS.md`, `knowledge/workflow.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-public-docs-and-native-stubs.md](tasks/t01-public-docs-and-native-stubs.md) — Update docs/usage/databases.md, docs/reference/native-stubs.md (heading_level: 4), and docs/changelog/index.md.
- [ ] [t02-okf-sqlite-limitations-and-data-docs.md](tasks/t02-okf-sqlite-limitations-and-data-docs.md) — Create sqlite-limitations.md (<= 200 lines), update database-drivers-and-dialers.md, and link in domains/index.md.
- [ ] [t03-review-skill-agents-and-okf-sync.md](tasks/t03-review-skill-agents-and-okf-sync.md) — Add vertebra-sqlite, make sqlite-test, and make sqlite-wheel-check to review-pyo3-bridge/SKILL.md, AGENTS.md, and workflow.md.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/3` tasks completed.
- **Active Invariants**: All OKF and skill files stay `<= 200` lines; `make docs` and `make review-check` exit `0`.
- **Fast Chapter Gate (`< 60s`)**: `make review-check && make docs`

[^docs-db]: `docs/usage/databases.md` — Database drivers usage guide.
[^docs-stubs]: `docs/reference/native-stubs.md` — Native extension stub API reference.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
[^review-pyo3-skill]: `.agents/skills/review-pyo3-bridge/SKILL.md` — PyO3 bridge review skill.
[^agents-md]: `AGENTS.md` — Repository agent instructions and quick verification commands.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project Makefile and verification workflow chapter.
