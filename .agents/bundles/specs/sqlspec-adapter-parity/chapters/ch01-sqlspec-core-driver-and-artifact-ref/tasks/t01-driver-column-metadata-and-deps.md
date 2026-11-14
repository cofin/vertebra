---
type: Task
id: sqlspec-adapter-parity:ch01-sqlspec-core-driver-and-artifact-ref:t01
flow_id: sqlspec-adapter-parity
chapter_id: ch01-sqlspec-core-driver-and-artifact-ref
title: Bump sqlspec>=0.71.1 + adk Extra and Add Driver query_with_columns Metadata Hook
description: Raise sqlspec>=0.71.1, add adk = ['google-adk>=1.0.0'], and expose query_with_columns delegating to execute_with_description on base drivers.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on: []
files:
- pyproject.toml
- src/py/vertebra/db/driver/_sync.py
- src/py/vertebra/db/driver/_async.py
tests:
- src/py/tests/unit/db/test_native_db_drivers.py
read_set:
- pyproject.toml
- src/py/vertebra/db/driver/_sync.py
- src/py/vertebra/db/driver/_async.py
- src/py/vertebra/db/driver/_common.py
estimated_loc: 165
tags:
- sqlspec
- driver
- dependencies
sources:
- id: pyproject-sqlspec
  resource: pyproject.toml
  title: sqlspec and optional adk extra declarations
- id: sync-driver
  resource: src/py/vertebra/db/driver/_sync.py
  title: SyncDatabaseDriver base class
- id: async-driver
  resource: src/py/vertebra/db/driver/_async.py
  title: AsyncDatabaseDriver base class
---

# Task `t01`: Bump sqlspec>=0.71.1 + adk Extra and Add Driver query_with_columns Metadata Hook

## Objective & Scope Boundary

Raise `[project.optional-dependencies].sqlspec` to `sqlspec>=0.71.1`, declare `adk = ["google-adk>=1.0.0"]`, and add `query_with_columns` on `SyncDatabaseDriver` / `AsyncDatabaseDriver` per [`../spec.md`](../spec.md) [^pyproject-sqlspec] [^sync-driver] [^async-driver].

## Bounded Read Manifest

- `pyproject.toml` — `[project.optional-dependencies]` (`sqlspec`, `adk`).
- `src/py/vertebra/db/driver/_sync.py` & `_async.py` — row-returning query methods.

## Contracts & Edge Cases

- Raise `sqlspec>=0.71.1` in `pyproject.toml` (`uv.lock` already resolves `sqlspec==0.71.1`, consuming native `resolve_adapter_name` and `resolve_extension_store_class` with zero `_compat.py` monkeypatching) and add `adk = ["google-adk>=1.0.0"]`.
- Expose `query_with_columns(sql, parameters=None) -> tuple[list[dict[str, Any]], list[str]]` (and async equivalent) on `SyncDatabaseDriver` and `AsyncDatabaseDriver` by delegating to existing `execute_with_description` + `hydrate_rows` (`_common.py`) so zero-row `SELECT` and `INSERT ... RETURNING` statements preserve exact column names.

## Test Matrix

- Parametrized unit tests verifying `query_with_columns` preserves `column_names` on both populated and zero-row `SELECT` / `RETURNING` results.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/extensions/sqlspec src/py/vertebra/storage && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/extensions/sqlspec src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py src/py/tests/unit/storage/test_storage_store.py
```

## Acceptance Criteria

- [ ] `pyproject.toml` declares `sqlspec>=0.71.1` and `adk = ["google-adk>=1.0.0"]`.
- [ ] `query_with_columns` preserves exact `column_names` on empty result sets.
- [ ] Zero regressions in the fast verification gate.

[^pyproject-sqlspec]: `pyproject.toml` — sqlspec and optional adk extra declarations.
[^sync-driver]: `src/py/vertebra/db/driver/_sync.py` — SyncDatabaseDriver base class.
[^async-driver]: `src/py/vertebra/db/driver/_async.py` — AsyncDatabaseDriver base class.
