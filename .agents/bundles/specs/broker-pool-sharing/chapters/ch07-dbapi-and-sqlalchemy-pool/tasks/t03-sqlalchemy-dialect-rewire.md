---
type: Task
id: "broker-pool-sharing:ch07:t03"
chapter_id: ch07-dbapi-and-sqlalchemy-pool
title: Rewire SQLAlchemy dialects and import sites to the new modules
description: Delete the embedded PEP 249 classes from extensions/sqlalchemy/dialects.py, import them from vertebra.db.dbapi and async_adapter, update extensions/sqlalchemy/__init__.py exports, and rewire test and alembic import sites without compatibility re-exports.
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "broker-pool-sharing:ch07:t02"
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/extensions/sqlalchemy/__init__.py
  - src/py/vertebra/extensions/sqlalchemy/alembic.py
  - src/py/vertebra/db/dbapi/__init__.py
  - src/py/vertebra/extensions/sqlalchemy/async_adapter.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
files:
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/extensions/sqlalchemy/__init__.py
  - src/py/vertebra/extensions/sqlalchemy/alembic.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/extensions/test_django_plugin.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
estimated_loc: 180
plan_revision: 3
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect
    title: Dialects whose import_dbapi returns the embedded modules
  - id: sa-init
    resource: src/py/vertebra/extensions/sqlalchemy/__init__.py
    title: SQLAlchemy extension public exports
  - id: alembic
    resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
    title: Alembic helpers importing SyncDBAPIConnection under TYPE_CHECKING
tags:
  - sqlalchemy
  - dbapi
  - clean-break
---

# Task t03: Rewire SQLAlchemy Dialects and Import Sites

## Objective & Scope Boundary

1. In `dialects.py`, delete the error classes, `Binary`, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPICursor`, `AsyncDBAPIConnection`, `AsyncDBAPIModule`, `_is_row_returning_sql`, `_ROW_RETURNING_LEADING_KEYWORDS`, and the `BigQueryConfig` / `SpannerConfig` / `SQLiteConfig` / `AsyncAdapt_dbapi_connection` imports when unused. Each dialect `import_dbapi` returns `vertebra.db.dbapi.SyncDBAPIModule(...)` or `vertebra.extensions.sqlalchemy.async_adapter.AsyncDBAPIModule(...)` [^dialects]. Remove the moved names from `dialects.__all__`.
2. `extensions/sqlalchemy/__init__.py` stops exporting DB-API classes and errors [^sa-init].
3. `alembic.py` imports `SyncDBAPIConnection` from `vertebra.db.dbapi` under `TYPE_CHECKING` [^alembic].
4. Rewire test import sites: `test_django_plugin.py` (`SyncDBAPIConnection`), `test_native_db_drivers.py` (`dialects.<DB-API name>` attribute access), `test_enterprise_olap_drivers.py`, `test_mysql_driver.py`, `test_gcp_adapters_and_sqlalchemy.py`, `test_sqlalchemy_and_sqlspec_vtb.py` — each import of a moved name switches to `vertebra.db.dbapi` or `async_adapter`. Test edits are mechanical import changes and are listed under `tests`.

## Bounded Read Manifest

- `dialects.py` — `__all__`, imports, every `import_dbapi` classmethod [^dialects].
- `extensions/sqlalchemy/__init__.py` [^sa-init]; `alembic.py` [^alembic].
- `db/dbapi/__init__.py`, `async_adapter.py` (from `t01`, `t02`).
- `test_sqlalchemy_and_sqlspec_vtb.py` — dialect regression coverage.

## Symbol & Signature Contracts

- `Sync<Engine>Dialect.import_dbapi(cls) -> SyncDBAPIModule` and `Async<Engine>Dialect.import_dbapi(cls) -> AsyncDBAPIModule` keep their return values' `paramstyle` per dialect exactly as at HEAD.
- `git grep -n "class SyncDBAPI\|class AsyncDBAPI\|_is_row_returning_sql" src/py/vertebra/extensions` prints only `async_adapter.py::AsyncDBAPIModule`.

## Edge Cases & Error Semantics

- Any remaining import of a moved name from `vertebra.extensions.sqlalchemy` fails with `ImportError` (no shim); the executor finds them with `git grep -n "SyncDBAPI\|AsyncDBAPI\|InterfaceError\|ProgrammingError" src/py docs`.
- `docs/` references are left for `ch09 / t01`; the executor lists any hits in Notes.

## Public-Seam Test Blueprint

| Test | Input | Expected |
| --- | --- | --- |
| `test_dialect_import_dbapi_module_identity[sync]` | `SyncPostgreSQLDialect.import_dbapi()` | `type(...) is vertebra.db.dbapi.SyncDBAPIModule` |
| `test_dialect_import_dbapi_module_identity[async]` | `AsyncPostgreSQLDialect.import_dbapi()` | `type(...) is async_adapter.AsyncDBAPIModule` |
| `test_dialects_no_dbapi_reexport` | `hasattr(dialects, "SyncDBAPIConnection")` | `False` |
| Existing SQLAlchemy round-trip tests | sqlite `+vtb` engine create/insert/select | unchanged pass |

Add the first three to `test_sqlalchemy_and_sqlspec_vtb.py`, parametrized over sync/async dialect pairs.

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/db/test_dbapi.py -q`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy && uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy`

## Acceptance Criteria

- `dialects.py` defines no PEP 249 classes; all listed suites pass.

## Notes & Discoveries

- None yet.
