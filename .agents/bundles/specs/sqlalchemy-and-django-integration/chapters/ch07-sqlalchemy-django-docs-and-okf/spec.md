---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch07-sqlalchemy-django-docs-and-okf
title: "Chapter 07: Documentation, OKF Knowledge Sync & Dialect Limitations"
description: "Synchronize public extension/database docs, OKF knowledge chapters, and the dialect/backend limitations record."
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
  - documentation
  - okf
  - sqlalchemy
  - django
sources:
  - id: doc-ext
    resource: docs/usage/extensions.md
    title: "Extensions usage documentation"
  - id: okf-dialects
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: "SQLSpec and SQLAlchemy dialects OKF chapter"
  - id: okf-litestar
    resource: .agents/bundles/knowledge/domains/extensions/litestar.md
    title: "Litestar extension OKF chapter"
  - id: okf-limits
    resource: .agents/bundles/knowledge/domains/data/database-driver-limitations.md
    title: "Database and dialect limitations OKF chapter"
---

# Chapter 07: Documentation, OKF Knowledge Sync & Dialect Limitations

<!-- truth: start -->
- **Baseline (`18baadcd`)**: Public docs and OKF chapters still reference monolithic `extensions/sqlalchemy/{dialects,alembic,cache}.py`, `shm://` cache URLs, and unsupported `bigquery`/`duckdb`/`mssql` Django DB backends [^doc-ext] [^okf-dialects].
- **Target Architecture**: Present-tense `docs/` and `<= 200`-line OKF chapters document the 36 split `Sync<Engine>Dialect`/`Async<Engine>Dialect` entry points, `dogpile.cache` regions, 5 Django `6.0+` backends, and explicit limitations [^okf-litestar] [^okf-limits].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter synchronizes user-facing guides, API references, and OKF v0.2 knowledge chapters with the completed SQLAlchemy, Advanced Alchemy `1.12.0+`, Alembic, and Django `6.0+` architecture [^doc-ext] [^okf-dialects].

## 2. Requirements, Concrete Contracts & Architecture

- **R13 (Docs & OKF Synchronization)**: Update `docs/usage/{extensions,databases}.md`, `docs/reference/extensions.md`, `sqlspec-alchemy-and-dialects.md`, `litestar.md`, and `database-driver-limitations.md`.

```text
Entry-Point Matrix (36 entries across 9 engines):
  <engine>.vertebra       -> vertebra.extensions.sqlalchemy.dialects.<mod>:Sync<Engine>Dialect
  <engine>.vtb            -> vertebra.extensions.sqlalchemy.dialects.<mod>:Sync<Engine>Dialect
  <engine>.vertebra_async -> vertebra.extensions.sqlalchemy.dialects.<mod>:Async<Engine>Dialect
  <engine>.vtb_async      -> vertebra.extensions.sqlalchemy.dialects.<mod>:Async<Engine>Dialect

Django 6.0+ Supported ENGINE Paths (5 backends):
  vertebra.extensions.django.db.backends.{postgresql,mysql,oracle,sqlite3,spanner}
```

## 3. Documentation & OKF Knowledge Plan

Updates `docs/usage/extensions.md`, `docs/usage/databases.md`, `docs/reference/extensions.md`, [sqlspec-alchemy-and-dialects.md](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [litestar.md](../../../../knowledge/domains/extensions/litestar.md), and [database-driver-limitations.md](../../../../knowledge/domains/data/database-driver-limitations.md), keeping every OKF chapter `<= 200` lines.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R13` Public usage & reference docs | `t01` | `docs/usage/{extensions,databases}.md`, `docs/reference/extensions.md` | `pytest test_vite_and_docs_portal.py` |
| `R13` OKF sync & limitations ledger | `t02` | `sqlspec-alchemy-and-dialects.md`, `litestar.md`, `database-driver-limitations.md` | `review_setup.py validate` |

## 5. Implementation Plan

- [ ] [t01-public-docs-sqlalchemy-and-django.md](tasks/t01-public-docs-sqlalchemy-and-django.md) — Update `docs/usage/extensions.md`, `docs/usage/databases.md`, and `docs/reference/extensions.md`.
- [ ] [t02-okf-sync-and-limitations.md](tasks/t02-okf-sync-and-limitations.md) — Sync `sqlspec-alchemy-and-dialects.md`, `litestar.md`, and `database-driver-limitations.md`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch07-sqlalchemy-django-docs-and-okf` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing` and `native-sqlite-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

[^doc-ext]: `docs/usage/extensions.md` — Extensions usage documentation.
[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec and SQLAlchemy dialects OKF chapter.
[^okf-litestar]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar extension OKF chapter.
[^okf-limits]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — Database and dialect limitations OKF chapter.
