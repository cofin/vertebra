---
type: Task
id: "sqlalchemy-and-django-integration:ch07-sqlalchemy-django-docs-and-okf:t02"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch07-sqlalchemy-django-docs-and-okf
title: "Synchronize OKF Knowledge Chapters and Dialect/Backend Limitations Ledger"
description: "Update sqlspec-alchemy-and-dialects.md, litestar.md, and database-driver-limitations.md with verified truth blocks and dialect caveats."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - t01
files:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/domains/extensions/litestar.md
  - .agents/bundles/knowledge/domains/data/database-driver-limitations.md
tests:
  - src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
read_set:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/domains/extensions/litestar.md
  - .agents/bundles/knowledge/domains/data/database-driver-limitations.md
estimated_loc: 155
tags:
  - okf
  - knowledge
  - sqlalchemy
  - django
sources:
  - id: okf-dialects
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: "SQLSpec, Advanced Alchemy, and SQLAlchemy dialects OKF chapter"
  - id: okf-litestar
    resource: .agents/bundles/knowledge/domains/extensions/litestar.md
    title: "Litestar and Advanced Alchemy integration OKF chapter"
  - id: okf-limits
    resource: .agents/bundles/knowledge/domains/data/database-driver-limitations.md
    title: "Database driver and dialect limitations ledger"
---

# Task `t02`: Synchronize OKF Knowledge Chapters and Dialect/Backend Limitations Ledger

## Objective & Scope Boundary

Synchronize `.agents/bundles/knowledge/domains/extensions/{sqlspec-alchemy-and-dialects,litestar}.md` and `domains/data/database-driver-limitations.md` per [`../spec.md`](../spec.md) [^okf-dialects] [^okf-litestar] [^okf-limits].

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLAlchemy dialect and Alembic truth block and architecture.
- `.agents/bundles/knowledge/domains/extensions/litestar.md` — Advanced Alchemy `dogpile.cache` region and plugin wiring.
- `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — engine/dialect caveats.

## Contracts & Edge Cases

- Update present-tense bounded truth blocks (`<= 40` lines) and keep every OKF knowledge chapter `<= 200` lines.
- Record explicit caveats in `database-driver-limitations.md`: BigQuery, DuckDB, and MSSQL have no Vertebra Django `DATABASES` backend (use SQLAlchemy or SQLSpec); `duckdb-engine`, `sqlalchemy-bigquery`, `sqlalchemy-spanner`, and `django-google-spanner` are optional extras required for those respective dialect/Django bases; BigQuery and Spanner execute non-transactional DDL.

## Test Matrix

- Run `python -m tools.scripts.review_setup validate` verifying OKF frontmatter, truth budgets, relative links, and source paths.

## Verification

```bash
uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

## Acceptance Criteria

- [ ] All 3 OKF chapters stay `<= 200` lines and pass `review_setup.py validate` with zero issues.
- [ ] Zero regressions in the chapter fast verification gate.

[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec, Advanced Alchemy, and SQLAlchemy dialects OKF chapter.
[^okf-litestar]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar and Advanced Alchemy integration OKF chapter.
[^okf-limits]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — Database driver and dialect limitations ledger.
