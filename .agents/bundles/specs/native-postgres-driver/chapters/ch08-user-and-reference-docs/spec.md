---
type: Spec
flow_id: native-postgres-driver
chapter_id: ch08-user-and-reference-docs
title: "Chapter 08: User Guides, Reference Stubs, README, and AGENTS.md Sync"
description: "Synchronize Zensical usage/reference pages, tabbed [postgres] install blocks, native-stubs.md, README.md, and AGENTS.md."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-13T23:12:00Z"
generated:
  by: flow-plan
  at: "2026-11-13T00:25:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-13T00:25:00Z"
    commit: 18baadcd
tags:
  - docs
  - postgres
  - zensical
sources:
  - id: doc-db
    resource: docs/usage/databases.md
    title: "Database usage guide"
  - id: doc-stubs
    resource: docs/reference/native-stubs.md
    title: "Native PyO3 stubs reference"
  - id: doc-ext
    resource: docs/usage/extensions.md
    title: "SQLAlchemy and framework extensions guide"
  - id: readme
    resource: README.md
    title: "Repository overview and crate table"
---

# Chapter 08: User Guides, Reference Stubs, README, and AGENTS.md Sync

<!-- truth: start -->
- Public docs use `pymdownx.tabbed` (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`) for `vertebra-core[postgres]`, keep Mermaid diagrams at `4`–`6` conceptual nodes, and never compare against external libraries [^doc-db] [^doc-ext].
- `native-stubs.md` includes `_mysql`, `_mssql`, and `_postgres` while keeping `H2+H3 <= 14` and rendered HTML `<= 120,000` bytes; `extensions.md:101` and `README.md` remove stale pending-wheel claims [^doc-stubs] [^readme].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Synchronizes all public Zensical documentation, `README.md`, and `AGENTS.md` (`<= 100L`) with the completed native PostgreSQL driver and companion wheels.

## 2. Requirements, Concrete Contracts & Architecture

- Obey Zensical artifact budgets verified by `make docs` (`verify_docs_artifacts.py`), concise `class Model(msgspec.Struct):` snippets without internal `frozen=True, gc=False` boilerplate, and `AGENTS.md <= 100` lines.

## 3. Documentation & OKF Knowledge Plan

Covers all 10 affected `docs/` pages plus `README.md` and `AGENTS.md`; OKF data/product chapters follow in `ch09`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R5` Databases, CDC & capability matrix docs | `t01` | `databases.md`, `analytics-and-cdc.md`, `database-capabilities.md` | `make docs` |
| `R5` Native stubs, reference index & installation | `t02` | `native-stubs.md`, `reference/index.md`, `installation.md` | `make docs` |
| `R5` Cloud dialers, packaging & queues docs | `t03` | `cloud-dialers.md`, `packaging-and-deployment.md`, `queues-and-realtime.md` | `make docs` |
| `R5` `extensions.md`, `README.md` & `AGENTS.md` | `t04` | `extensions.md`, `README.md`, `AGENTS.md` | `make docs && make review-check` |

## 5. Implementation Plan

- [x] [d9d2fd01] [t01-databases-cdc-and-capabilities-docs.md](tasks/t01-databases-cdc-and-capabilities-docs.md) — Document vertebra-core[postgres], $N binds, single-Sync pipeline(), Pg* types, PostgresChangeSource, and PostgresEmbeddingPushdown.
- [x] [30c34daf] [t02-native-stubs-reference-and-installation-docs.md](tasks/t02-native-stubs-reference-and-installation-docs.md) — Add _mysql, _mssql, and _postgres to native-stubs.md within H2+H3/HTML budgets and sync reference/index.md and installation.md.
- [x] [d9d2fd01] [t03-dialers-packaging-and-queues-docs.md](tasks/t03-dialers-packaging-and-queues-docs.md) — Remove legacy psycopg/asyncpg dialer examples, drop false release-CI PGO prose, and document vertebra-core[postgres] queue guard.
- [x] [d9d2fd01] [t04-extensions-readme-and-agents-sync.md](tasks/t04-extensions-readme-and-agents-sync.md) — Update +vtb dialect wheel extras in extensions.md and usage/index.md, refresh README.md crate/wheel overview, and keep AGENTS.md <= 100 lines.

## 6. Current State & Remaining Plan Snapshot

Implemented at `d9d2fd01` (`4/4` tasks verified offline via `make docs`, `native-stubs/index.html` = `114,861 <= 120,000` bytes, `AGENTS.md` = `69L <= 100L`, `27 passed` in `test_review_setup.py`, and `review_setup validate` clean; awaiting zero-context chapter re-review).

```bash
make docs && make review-check
```

[^doc-db]: `docs/usage/databases.md` — Database usage guide.
[^doc-stubs]: `docs/reference/native-stubs.md` — Native PyO3 stubs reference.
[^doc-ext]: `docs/usage/extensions.md` — SQLAlchemy and framework extensions guide.
[^readme]: `README.md` — Repository overview and crate table.
