---
type: Task
id: broker-pool-sharing:ch04-python-pooling-sqlspec-sqlalchemy-and-docs:t03
flow_id: broker-pool-sharing
chapter_id: ch04-python-pooling-sqlspec-sqlalchemy-and-docs
title: Extract Standalone vertebra.db.dbapi and Wire SQLAlchemy & Advanced Alchemy Broker Pooling
description: Extract PEP 249 dbapi.py into vertebra.db, rewire extensions/sqlalchemy/{adapter,dialects}.py, and support broker_client across SQLAlchemy and Advanced Alchemy.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
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
tests:
- src/py/tests/unit/db/test_dbapi_standalone.py
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/vertebra/db/driver/_common.py
- src/py/vertebra/db/registry.py
- src/py/vertebra/extensions/sqlalchemy/alembic.py
- src/py/vertebra/db/dbapi/__init__.py
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
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
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
  title: SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes
- id: db-config
  resource: src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig
  title: BaseDatabaseConfig with dialect ClassVar and pool_key
- id: py-db-registry
  resource: src/py/vertebra/db/registry.py::DatabaseRegistry
  title: DatabaseRegistry URL parsing and driver resolution
- id: broker-transport
  resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
  title: Python SHM broker transports and lease objects
---

# Task `t03`: Extract Standalone vertebra.db.dbapi and Wire SQLAlchemy & Advanced Alchemy Broker Pooling

## Objective & Scope Boundary

Create `vertebra.db.dbapi` (`__init__.py` with PEP 562 lazy exports, `_sync.py`, `_async.py`) per [`../spec.md` §2](../spec.md) [^sa-dialects] [^db-config] [^py-db-registry]. Out of scope: SQLAlchemy `AsyncAdapt` wrapper (`t02`).

Isolate `AsyncAdapt_dbapi_connection` wrapping in `extensions/sqlalchemy/async_adapter.py` and rewire `dialects.py` and `alembic.py` onto `vertebra.db.dbapi` with zero compatibility re-exports per [`../spec.md` §2](../spec.md) [^sa-dialects] [^db-config].

Implement `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` in `src/py/vertebra/extensions/sqlalchemy/pool.py` and lazy-export them in `extensions/sqlalchemy/__init__.py` per [`../spec.md` §2](../spec.md) [^broker-transport] [^sa-dialects].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Embedded PEP 249 errors, cursors, connections, modules, and `_is_row_returning_sql`.
- `src/py/vertebra/db/driver/_common.py` — `BaseDatabaseConfig.dialect` and driver contracts.
- `src/py/vertebra/db/registry.py` — `DatabaseRegistry.parse_url` and driver lookup.

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Dialect `import_dbapi` hooks and embedded DB-API classes to remove.
- `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Alembic import sites.
- `src/py/vertebra/db/dbapi/__init__.py` — Extracted DB-API exports from `t01`.
- `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` — Dialect and DB-API unit tests.

- `src/py/vertebra/extensions/broker/transport.py` — `SyncSHMBrokerTransport` and `AsyncSHMBrokerTransport`.
- `src/py/vertebra/extensions/sqlspec/provider.py` — `SyncBrokerLeaseSource` and `AsyncBrokerLeaseSource`.
- `src/py/vertebra/db/dbapi/_sync.py` — `SyncDBAPIConnection`.
- `src/py/vertebra/extensions/sqlalchemy/async_adapter.py` — SQLAlchemy async DB-API wrapper.
- `src/py/vertebra/extensions/sqlalchemy/__init__.py` — Lazy package exports (`PY-06`).

## Contracts & Edge Cases

- Export PEP 249 exceptions, `apilevel`, `threadsafety`, `paramstyle`, `is_row_returning_sql`, `connect`, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPICursor`, `AsyncDBAPIConnection`, and `AsyncDBAPIModule`.
- Restrict imports to stdlib, `vertebra.db.registry`, `vertebra.db.driver`, `vertebra.typing`, and `vertebra.utils`; inspect `type(config).dialect` instead of adapter `isinstance`.
- **Edge Case**: Leading `--` / `/* */` SQL comments and string literals containing `'returning'` classify accurately in `is_row_returning_sql`.
- **Edge Case**: `__init__.py` uses `lazy_exports` (`PY-06`).

- Delete embedded DB-API exceptions, cursors, connections, and modules from `dialects.py` (`PY-07`).
- Keep all four dialect entry points per engine (`<engine>+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async`) working identically.
- **Edge Case**: `RETURNING` capability checks consult dialect/capability attributes rather than concrete config `isinstance`.

- Acquire SHM broker leases keyed by `config.pool_key()` on checkout (`_do_get`), execute rollback on return (`_do_return_conn`), and quarantine + raise `LeaseQuarantinedError` if rollback fails.
- Support both SQLAlchemy `create_engine` / `create_async_engine` and Advanced Alchemy `SQLAlchemySyncConfig` / `SQLAlchemyAsyncConfig`.
- **Edge Case**: Constructing `BrokerPool` without a configured broker raises `ImproperConfigurationError` immediately.

## Test Matrix

- Subprocess import-boundary test verifies `import vertebra.db.dbapi` loads zero `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters.*` modules.
- `@pytest.mark.parametrize` covers `is_row_returning_sql`, sync/async cursor `description`/`rowcount`, and `autocommit` on `bigquery`/`spanner`.

- `@pytest.mark.parametrize` verifies sync and async SQLAlchemy `import_dbapi()` across PostgreSQL, MySQL, SQLite, Oracle, MSSQL, DuckDB, BigQuery, and Spanner.

- Verify two SQLAlchemy engines sharing one `pool_key` acquire from the same SHM partition and enforce `pool_size + max_overflow`.
- Forced rollback failure during connection return quarantines the lease and raises `LeaseQuarantinedError`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy
```

## Acceptance Criteria

- [ ] `vertebra.db.dbapi` satisfies PEP 249 and async DB-API contracts with zero upward imports.
- [ ] `dialects.py` contains only SQLAlchemy dialect definitions with zero embedded DB-API classes or re-export shims.
- [ ] `BrokerPool` and `AsyncBrokerPool` share broker pools cleanly across SQLAlchemy and Advanced Alchemy.
- [ ] Zero regressions in the fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes.
[^db-config]: `src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig` — BaseDatabaseConfig with dialect ClassVar and pool_key.
[^py-db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — DatabaseRegistry URL parsing and driver resolution.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
