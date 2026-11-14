---
type: Task
id: sqlalchemy-and-django-integration:ch03-django-backends-and-docs:t04
flow_id: sqlalchemy-and-django-integration
chapter_id: ch03-django-backends-and-docs
title: Synchronize SQLAlchemy, Alembic, Advanced Alchemy, and Django Docs and OKF Chapters
description: Update docs/usage/extensions.md and docs/reference/extensions.md and sync sqlspec-alchemy-and-dialects.md, django.md, and database-driver-limitations.md.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 1b191388
depends_on:
- sqlalchemy-and-django-integration:ch03-django-backends-and-docs:t03
files:
- docs/usage/extensions.md
- docs/usage/databases.md
- docs/reference/extensions.md
- .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
- .agents/bundles/knowledge/domains/extensions/litestar.md
- .agents/bundles/knowledge/domains/data/database-driver-limitations.md
tests:
- src/py/tests/unit/extensions/test_vite_and_docs_portal.py
- src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
read_set:
- docs/usage/extensions.md
- docs/usage/databases.md
- docs/reference/extensions.md
- src/py/vertebra/extensions/sqlalchemy/dialects/__init__.py
- src/py/vertebra/extensions/alchemy/cache.py
- .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
estimated_loc: 220
tags:
- documentation
- sqlalchemy
- advanced-alchemy
- django
- okf
- knowledge
sources:
- id: doc-ext
  resource: docs/usage/extensions.md
  title: Framework and ORM extension usage guide
- id: doc-db
  resource: docs/usage/databases.md
  title: Database driver and URL scheme usage guide
- id: ref-ext
  resource: docs/reference/extensions.md
  title: Extensions API reference
- id: okf-dialects
  resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  title: SQLSpec, Advanced Alchemy, and SQLAlchemy dialects OKF chapter
- id: okf-litestar
  resource: .agents/bundles/knowledge/domains/extensions/litestar.md
  title: Litestar and Advanced Alchemy integration OKF chapter
- id: okf-limits
  resource: .agents/bundles/knowledge/domains/data/database-driver-limitations.md
  title: Database driver and dialect limitations ledger
---

# Task `t04`: Synchronize SQLAlchemy, Alembic, Advanced Alchemy, and Django Docs and OKF Chapters

## Objective & Scope Boundary

Update public documentation in `docs/usage/{extensions,databases}.md` and `docs/reference/extensions.md` per [`../spec.md`](../spec.md) [^doc-ext] [^doc-db] [^ref-ext].

Synchronize `.agents/bundles/knowledge/domains/extensions/{sqlspec-alchemy-and-dialects,litestar}.md` and `domains/data/database-driver-limitations.md` per [`../spec.md`](../spec.md) [^okf-dialects] [^okf-litestar] [^okf-limits].

## Bounded Read Manifest

- `docs/usage/extensions.md` — SQLAlchemy, Advanced Alchemy `1.12.0+`, and Django `6.0+` sections.
- `docs/usage/databases.md` — URL scheme tables (`+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async`).
- `docs/reference/extensions.md` — API reference exports for `vertebra.extensions.{sqlalchemy,alchemy,django}`.

- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLAlchemy dialect and Alembic truth block and architecture.
- `.agents/bundles/knowledge/domains/extensions/litestar.md` — Advanced Alchemy `dogpile.cache` region and plugin wiring.
- `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — engine/dialect caveats.

## Contracts & Edge Cases

- Document all 36 SQLAlchemy dialect entry points (`Sync<Engine>Dialect` / `Async<Engine>Dialect`), Alembic `DefaultImpl` & autogenerate hooks, `dogpile.cache` (`vertebra.valkey`, `vertebra.redis`, `vertebra.memory`) + Advanced Alchemy `1.12.0+` (`SQLAlchemySyncConfig`, `SQLAlchemyAsyncConfig`, `ZeroCopyServiceMixin`), and the 5 Django `6.0+` `DATABASES` backends (`postgresql`, `mysql`, `oracle`, `sqlite3`, `spanner`).
- Format installation commands with linked `pymdownx.tabbed` blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`) and keep conceptual Mermaid diagrams to 4–6 high-level nodes.

- Update present-tense bounded truth blocks (`<= 40` lines) and keep every OKF knowledge chapter `<= 200` lines.
- Record explicit caveats in `database-driver-limitations.md`: BigQuery, DuckDB, and MSSQL have no Vertebra Django `DATABASES` backend (use SQLAlchemy or SQLSpec); `duckdb-engine`, `sqlalchemy-bigquery`, `sqlalchemy-spanner`, and `django-google-spanner` are optional extras required for those respective dialect/Django bases; BigQuery and Spanner execute non-transactional DDL.

## Test Matrix

- Run `test_vite_and_docs_portal.py` and verify zero stale `sqlalchemy.cache` or `shm://` cache references remain in `docs/`.

- Run `python -m tools.scripts.review_setup validate` verifying OKF frontmatter, truth budgets, relative links, and source paths.

## Verification

```bash
uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

## Acceptance Criteria

- [ ] Public docs accurately reflect `Sync<Engine>Dialect` / `Async<Engine>Dialect`, `dogpile.cache` regions, and the 5 supported Django DB backends.
- [ ] All 3 OKF chapters stay `<= 200` lines and pass `review_setup.py validate` with zero issues.
- [ ] Zero regressions in the fast verification gate.

[^doc-ext]: `docs/usage/extensions.md` — Framework and ORM extension usage guide.
[^doc-db]: `docs/usage/databases.md` — Database driver and URL scheme usage guide.
[^ref-ext]: `docs/reference/extensions.md` — Extensions API reference.
[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec, Advanced Alchemy, and SQLAlchemy dialects OKF chapter.
[^okf-litestar]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar and Advanced Alchemy integration OKF chapter.
[^okf-limits]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — Database driver and dialect limitations ledger.
