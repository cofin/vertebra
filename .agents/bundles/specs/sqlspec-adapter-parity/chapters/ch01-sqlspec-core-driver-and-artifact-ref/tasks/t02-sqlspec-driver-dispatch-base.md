---
type: Task
id: "sqlspec-adapter-parity:ch01-sqlspec-core-driver-and-artifact-ref:t02"
flow_id: sqlspec-adapter-parity
chapter_id: ch01-sqlspec-core-driver-and-artifact-ref
title: "Implement extensions/sqlspec/_driver.py Template-Method Driver & Config Bases"
description: "Implement BaseSyncDriver, BaseAsyncDriver, SyncAdapterConfig, AsyncAdapterConfig, StatementConfig preservation, and exception mapping."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t01
files:
  - src/py/vertebra/extensions/sqlspec/_driver.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/registry.py
estimated_loc: 245
tags:
  - sqlspec
  - driver
  - pipeline
  - arrow
sources:
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Current monolithic SQLSpec BaseSyncDriver and BaseAsyncDriver"
  - id: db-common
    resource: src/py/vertebra/db/driver/_common.py
    title: "Canonical DatabaseError hierarchy"
---

# Task `t02`: Implement extensions/sqlspec/_driver.py Template-Method Driver & Config Bases

## Objective & Scope Boundary

Create `src/py/vertebra/extensions/sqlspec/_driver.py` implementing the MyPyC-safe `SyncDriverAdapterBase` / `AsyncDriverAdapterBase` template-method contract per [`../spec.md`](../spec.md) [^sqlspec-adapters] [^db-common].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters.py` — legacy overrides of `execute`/`select` to eliminate.
- `src/py/vertebra/db/driver/_common.py` & `capabilities.py` — `DatabaseError`, `SyncPipelineCapable`/`AsyncPipelineCapable`, `SyncArrowCapable`/`AsyncArrowCapable`, `SyncStreamCapable`/`AsyncStreamCapable`.
- `src/py/vertebra/db/registry.py` — reference-counted shared pool lifecycle.

## Contracts & Edge Cases

- `BaseSyncDriver(SyncDriverAdapterBase)` and `BaseAsyncDriver(AsyncDriverAdapterBase)` inherit `execute`, `execute_many`, `select`, `select_one`, `select_one_or_none`, `select_value`, and `@final dispatch_statement_execution` untouched.
- Implement `with_cursor` (yielding `connection` so `cursor is self.connection` routes cache hits into `dispatch_execute`), `dispatch_execute`, `dispatch_execute_many`, `dispatch_execute_script`, `_connection_in_transaction`, `select_to_arrow`, `dispatch_select_stream`, and `execute_stack` (using `SyncPipelineCapable`/`AsyncPipelineCapable` when eligible, else `super().execute_stack`).
- Preserve explicit caller `StatementConfig` overrides via non-mutating `.replace(...)`, unwrap `TypedParameter` values cleanly, map `DatabaseError` & `GoogleCloudError` in `DeferredSyncExceptionHandler` / `DeferredAsyncExceptionHandler` (`pending_exception`), and release `DatabaseRegistry`-shared pools via reference counting on `close()`.

## Test Matrix

- Parametrized tests for `SELECT`, `RETURNING`, zero-row `SELECT`, `PRAGMA`, `DDL`, `execute_many`, `execute_stack` pipeline vs fallback, `StatementConfig` override preservation, `TypedParameter` unwrapping, `DatabaseError` -> `sqlspec.exceptions.*` mapping, and shared-pool refcount `close()`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/extensions/sqlspec src/py/vertebra/storage && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/extensions/sqlspec src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py src/py/tests/unit/storage/test_storage_store.py
```

## Acceptance Criteria

- [ ] `_driver.py` implements full `sqlspec>=0.71.1` MyPyC-compatible dispatch without overriding `execute` or `select*`.
- [ ] Zero `_compat.py` monkeypatching is introduced anywhere in `extensions/sqlspec/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Current monolithic SQLSpec BaseSyncDriver and BaseAsyncDriver.
[^db-common]: `src/py/vertebra/db/driver/_common.py` — Canonical DatabaseError hierarchy.
