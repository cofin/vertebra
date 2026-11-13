---
type: Task
id: "sqlalchemy-and-django-integration:ch03-sqlalchemy-analytical-cloud-dialects:t03"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch03-sqlalchemy-analytical-cloud-dialects
title: "Wire dialects/__init__.py, Delete Monolithic dialects.py, and Repoint All 36 Entry Points"
description: "Export all 18 Sync/Async dialect classes via PEP 562 lazy loading, delete dialects.py, and repoint all 36 sqlalchemy.dialects entry points in pyproject.toml."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on:
  - "ch02-sqlalchemy-core-relational-dialects:t02"
  - "ch02-sqlalchemy-core-relational-dialects:t03"
  - t01
  - t02
files:
  - src/py/vertebra/extensions/sqlalchemy/dialects/__init__.py
  - src/py/vertebra/extensions/sqlalchemy/__init__.py
  - pyproject.toml
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
read_set:
  - pyproject.toml
  - src/py/vertebra/extensions/sqlalchemy/__init__.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
estimated_loc: 145
tags:
  - sqlalchemy
  - dialects
  - entrypoints
sources:
  - id: entry-points
    resource: pyproject.toml
    title: "36 sqlalchemy.dialects entry points across 9 engines"
  - id: sa-init
    resource: src/py/vertebra/extensions/sqlalchemy/__init__.py
    title: "Public SQLAlchemy extension barrel"
---

# Task `t03`: Wire dialects/__init__.py, Delete Monolithic dialects.py, and Repoint All 36 Entry Points

## Objective & Scope Boundary

Wire `extensions/sqlalchemy/dialects/__init__.py` and `extensions/sqlalchemy/__init__.py`, delete monolithic `dialects.py`, and repoint all 36 `[project.entry-points."sqlalchemy.dialects"]` entries per [`../spec.md`](../spec.md) [^entry-points] [^sa-init].

## Bounded Read Manifest

- `pyproject.toml` — `[project.entry-points."sqlalchemy.dialects"]` section.
- `src/py/vertebra/extensions/sqlalchemy/__init__.py` — public package exports.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — legacy monolithic file to delete cleanly (`git rm`).

## Contracts & Edge Cases

- Preserve all 36 public entry-point names (`<engine>.vertebra`, `<engine>.vtb`, `<engine>.vertebra_async`, `<engine>.vtb_async` for `postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, `spangres`), repointing each to `vertebra.extensions.sqlalchemy.dialects.<module>:Sync<Engine>Dialect` or `:Async<Engine>Dialect`.
- Use PEP 562 lazy `__getattr__` in `dialects/__init__.py` so importing relational dialects never triggers optional `duckdb_engine` / `sqlalchemy_bigquery` / `sqlalchemy_spanner` imports.
- Delete `src/py/vertebra/extensions/sqlalchemy/dialects.py` completely with zero compatibility aliases.

## Test Matrix

- Parametrized test asserting all 36 `[project.entry-points."sqlalchemy.dialects"]` keys resolve to the exact split `Sync<Engine>Dialect` / `Async<Engine>Dialect` classes and zero `Vertebra*` / `*Vtb*` dialect names exist.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

## Acceptance Criteria

- [ ] Monolithic `src/py/vertebra/extensions/sqlalchemy/dialects.py` is deleted.
- [ ] All 36 SQLAlchemy dialect entry points in `pyproject.toml` resolve to `Sync<Engine>Dialect` / `Async<Engine>Dialect`.
- [ ] Zero regressions in the chapter fast verification gate.

[^entry-points]: `pyproject.toml` — 36 sqlalchemy.dialects entry points across 9 engines.
[^sa-init]: `src/py/vertebra/extensions/sqlalchemy/__init__.py` — Public SQLAlchemy extension barrel.
