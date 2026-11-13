---
type: Task
id: "sqlalchemy-and-django-integration:ch07-sqlalchemy-django-docs-and-okf:t01"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch07-sqlalchemy-django-docs-and-okf
title: "Update Public SQLAlchemy, Advanced Alchemy, and Django Database Documentation"
description: "Update docs/usage/extensions.md, docs/usage/databases.md, and docs/reference/extensions.md for the 36 dialect entry points, dogpile.cache, and 5 Django backends."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "ch04-alembic-dogpile-and-advanced-alchemy:t03"
  - "ch06-django-spanner-and-unsupported-cleanup:t02"
files:
  - docs/usage/extensions.md
  - docs/usage/databases.md
  - docs/reference/extensions.md
tests:
  - src/py/tests/unit/extensions/test_vite_and_docs_portal.py
read_set:
  - docs/usage/extensions.md
  - docs/usage/databases.md
  - docs/reference/extensions.md
  - src/py/vertebra/extensions/sqlalchemy/dialects/__init__.py
  - src/py/vertebra/extensions/alchemy/cache.py
estimated_loc: 175
tags:
  - documentation
  - sqlalchemy
  - advanced-alchemy
  - django
sources:
  - id: doc-ext
    resource: docs/usage/extensions.md
    title: "Framework and ORM extension usage guide"
  - id: doc-db
    resource: docs/usage/databases.md
    title: "Database driver and URL scheme usage guide"
  - id: ref-ext
    resource: docs/reference/extensions.md
    title: "Extensions API reference"
---

# Task `t01`: Update Public SQLAlchemy, Advanced Alchemy, and Django Database Documentation

## Objective & Scope Boundary

Update public documentation in `docs/usage/{extensions,databases}.md` and `docs/reference/extensions.md` per [`../spec.md`](../spec.md) [^doc-ext] [^doc-db] [^ref-ext].

## Bounded Read Manifest

- `docs/usage/extensions.md` — SQLAlchemy, Advanced Alchemy `1.12.0+`, and Django `6.0+` sections.
- `docs/usage/databases.md` — URL scheme tables (`+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async`).
- `docs/reference/extensions.md` — API reference exports for `vertebra.extensions.{sqlalchemy,alchemy,django}`.

## Contracts & Edge Cases

- Document all 36 SQLAlchemy dialect entry points (`Sync<Engine>Dialect` / `Async<Engine>Dialect`), Alembic `DefaultImpl` & autogenerate hooks, `dogpile.cache` (`vertebra.valkey`, `vertebra.redis`, `vertebra.memory`) + Advanced Alchemy `1.12.0+` (`SQLAlchemySyncConfig`, `SQLAlchemyAsyncConfig`, `ZeroCopyServiceMixin`), and the 5 Django `6.0+` `DATABASES` backends (`postgresql`, `mysql`, `oracle`, `sqlite3`, `spanner`).
- Format installation commands with linked `pymdownx.tabbed` blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`) and keep conceptual Mermaid diagrams to 4–6 high-level nodes.

## Test Matrix

- Run `test_vite_and_docs_portal.py` and verify zero stale `sqlalchemy.cache` or `shm://` cache references remain in `docs/`.

## Verification

```bash
uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

## Acceptance Criteria

- [ ] Public docs accurately reflect `Sync<Engine>Dialect` / `Async<Engine>Dialect`, `dogpile.cache` regions, and the 5 supported Django DB backends.
- [ ] Zero regressions in the chapter fast verification gate.

[^doc-ext]: `docs/usage/extensions.md` — Framework and ORM extension usage guide.
[^doc-db]: `docs/usage/databases.md` — Database driver and URL scheme usage guide.
[^ref-ext]: `docs/reference/extensions.md` — Extensions API reference.
