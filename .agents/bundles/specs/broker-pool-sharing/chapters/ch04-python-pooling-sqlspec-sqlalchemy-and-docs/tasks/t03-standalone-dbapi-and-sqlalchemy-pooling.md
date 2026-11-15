---
type: Task
id: broker-pool-sharing:ch04-python-pooling-sqlspec-sqlalchemy-and-docs:t03
flow_id: broker-pool-sharing
chapter_id: ch04-python-pooling-sqlspec-sqlalchemy-and-docs
title: Extract Standalone vertebra.db.dbapi and Wire SQLAlchemy & Advanced Alchemy Broker Pooling
description: Extract standalone PEP 249 vertebra.db.dbapi, isolate async_adapter.py, clean dialects.py/alembic.py and stale alchemy exports, and wire BrokerPool/AsyncBrokerPool across SQLAlchemy and Advanced Alchemy.
status: stable
state: completed
commit: 1b3820b7
last_verified_commit: 1b3820b7
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- broker-pool-sharing:ch04-python-pooling-sqlspec-sqlalchemy-and-docs:t02
files:
- src/py/vertebra/db/dbapi/__init__.py
- src/py/vertebra/db/dbapi/_sync.py
- src/py/vertebra/db/dbapi/_async.py
- src/py/vertebra/extensions/sqlalchemy/async_adapter.py
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/vertebra/extensions/sqlalchemy/alembic.py
- src/py/vertebra/extensions/sqlalchemy/pool.py
- src/py/vertebra/extensions/sqlalchemy/__init__.py
- src/py/vertebra/extensions/alchemy/__init__.py
- src/py/vertebra/extensions/alchemy/plugin.py
tests:
- src/py/tests/unit/db/test_dbapi_standalone.py
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/vertebra/db/driver/_common.py
- src/py/vertebra/db/registry.py
- src/py/vertebra/extensions/sqlalchemy/alembic.py
- src/py/vertebra/extensions/alchemy/__init__.py
- src/py/vertebra/extensions/alchemy/plugin.py
estimated_loc: 245
tags:
- dbapi
- pep249
- db
- sqlalchemy
- dialects
- advanced-alchemy
- broker
- pool
sources:
- id: sa-dialects
  resource: src/py/vertebra/db/dbapi/__init__.py::SyncDBAPIModule
  title: Standalone PEP 249 DB-API module, connection, cursor, and exports
- id: db-config
  resource: src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig
  title: BaseDatabaseConfig with dialect ClassVar and pool_key
- id: py-db-registry
  resource: src/py/vertebra/db/registry.py::DatabaseRegistry
  title: DatabaseRegistry URL parsing and driver resolution
- id: alchemy-plugin
  resource: src/py/vertebra/extensions/alchemy/plugin.py::create_sync_alchemy_config
  title: Advanced Alchemy config factory and broker connect_args wiring
- id: broker-transport
  resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
  title: Python SHM broker transports and lease objects
---

# Task `t03`: Extract Standalone vertebra.db.dbapi and Wire SQLAlchemy & Advanced Alchemy Broker Pooling

## Objective & Scope Boundary

Create framework-neutral `vertebra.db.dbapi` (`__init__.py` with `lazy_exports`, `_sync.py`, `_async.py`), isolate `AsyncAdapt_dbapi_connection` wrapping in `extensions/sqlalchemy/async_adapter.py`, remove embedded DB-API classes and adapter `isinstance` checks from `dialects.py`, update `alembic.py`, remove remaining stale `InMemoryBrokerTransport`/`StatementCache` re-exports in `extensions/alchemy/__init__.py`, and implement `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` in `extensions/sqlalchemy/pool.py` (lazy-exported from `extensions/sqlalchemy/__init__.py` and wired into `extensions/alchemy/plugin.py`) per [`../spec.md` §2](../spec.md) [^sa-dialects] [^db-config] [^py-db-registry] [^alchemy-plugin] [^broker-transport].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Embedded PEP 249 errors, cursors, connections, modules, `_is_row_returning_sql`, and dialect `import_dbapi` hooks.
- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig.dialect` and driver contracts.
- `src/py/vertebra/db/registry.py` — `DatabaseRegistry.parse_url` and driver lookup.
- `src/py/vertebra/extensions/sqlalchemy/alembic.py` — `SyncDBAPIConnection` type-checking import site.
- `src/py/vertebra/extensions/alchemy/__init__.py` — Re-exports requiring cleanup of deleted in-memory transport and `StatementCache` symbols.
- `src/py/vertebra/extensions/alchemy/plugin.py` — `_prepare_engine_config`, `create_sync_alchemy_config`, and `create_async_alchemy_config`.

## Contracts & Edge Cases

- Export PEP 249 exceptions, `apilevel`, `threadsafety`, `paramstyle`, `is_row_returning_sql`, `connect`, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPICursor`, `AsyncDBAPIConnection`, and `AsyncDBAPIModule` from `vertebra.db.dbapi` using `lazy_exports` (`PY-06`).
- Restrict `vertebra.db.dbapi` imports to stdlib, `vertebra.db.registry`, `vertebra.db.driver`, `vertebra.typing`, and `vertebra.utils`; inspect `type(config).dialect` instead of adapter `isinstance` checks.
- Delete embedded DB-API exceptions, cursors, connections, and modules from `extensions/sqlalchemy/dialects.py` (`PY-07`) with zero compatibility shims while keeping all dialect entry points (`<engine>+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async`) functional.
- Remove stale `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, and `StatementCache` imports/exports from `extensions/alchemy/__init__.py` (`PY-07`).
- Implement `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` in `extensions/sqlalchemy/pool.py` to acquire SHM broker leases keyed by `config.pool_key()` on checkout (`_do_get`), execute rollback on return (`_do_return_conn`), and quarantine + raise `LeaseQuarantinedError` if rollback fails, integrating with SQLAlchemy `create_engine` / `create_async_engine` and Advanced Alchemy `create_sync_alchemy_config` / `create_async_alchemy_config`.
- **Edge Case**: Leading `--` / `/* */` SQL comments and string literals containing `'returning'` classify accurately in `is_row_returning_sql`.
- **Edge Case**: `RETURNING` capability checks and statement-autocommit `NotSupportedError` checks inspect dialect/capability metadata rather than concrete config `isinstance`.
- **Edge Case**: Constructing `BrokerPool` without a configured broker raises `ImproperConfigurationError` immediately.

## Test Matrix

- Subprocess import-boundary test in `test_dbapi_standalone.py` verifies `import vertebra.db.dbapi` loads zero `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters.*` modules.
- `@pytest.mark.parametrize` covers `is_row_returning_sql`, sync/async cursor `description`/`rowcount`, `autocommit=False` `NotSupportedError` on `bigquery`/`spanner`, and sync/async SQLAlchemy `import_dbapi()` across PostgreSQL, MySQL, SQLite, Oracle, MSSQL, DuckDB, BigQuery, and Spanner.
- Verify two SQLAlchemy / Advanced Alchemy engines sharing one `pool_key` acquire from one SHM partition, enforce `pool_size + max_overflow`, and quarantine + raise `LeaseQuarantinedError` on forced return rollback failure.

## Verification

```bash
uv run --no-sync pytest \
  src/py/tests/unit/db/test_dbapi_standalone.py \
  src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py \
  src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py \
  -q && \
  uv run --no-sync ruff check \
    src/py/vertebra/db/dbapi \
    src/py/vertebra/extensions/sqlalchemy \
    src/py/vertebra/extensions/alchemy && \
  uv run --no-sync mypy \
    src/py/vertebra/db/dbapi \
    src/py/vertebra/extensions/sqlalchemy \
    src/py/vertebra/extensions/alchemy
```

## Acceptance Criteria

- [x] `vertebra.db.dbapi` satisfies PEP 249 and async DB-API contracts with zero upward imports.
- [x] `dialects.py` contains only SQLAlchemy dialect definitions with zero embedded DB-API classes or re-export shims, and stale `alchemy/__init__.py` exports are removed.
- [x] `BrokerPool` and `AsyncBrokerPool` share broker pools cleanly across SQLAlchemy and Advanced Alchemy.
- [x] Zero regressions in the fast verification gate.

[^sa-dialects]: `src/py/vertebra/db/dbapi/__init__.py::SyncDBAPIModule` — Standalone PEP 249 DB-API module, connection, cursor, and exports.
[^db-config]: `src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig` — BaseDatabaseConfig with dialect ClassVar and pool_key.
[^py-db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — DatabaseRegistry URL parsing and driver resolution.
[^alchemy-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py::create_sync_alchemy_config` — Advanced Alchemy config factory and broker connect_args wiring.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
