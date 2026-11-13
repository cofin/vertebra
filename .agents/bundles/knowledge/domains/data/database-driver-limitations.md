---
type: Gotcha
title: Database Driver Limitations
description: Verified cross-driver runtime boundaries covering native PostgreSQL/Oracle/MSSQL wheel status and DuckDB GCS credential handling.
status: stable
tags:
- data
- database
- postgres
- duckdb
- limitations
generated:
  by: flow/0.28.0
  at: "2026-11-12T01:15:00Z"
verified:
- by: agent:flow-sync
  at: "2026-11-12T01:15:00Z"
  commit: c8722c76
sources:
- id: pg-driver
  resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
  title: PostgreSQL drivers re-exported over native _sync and _async engines
- id: pg-queue
  resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
  title: PostgreSQL queue backend vertebra._postgres guard
- id: pg-stub
  resource: src/py/vertebra/_postgres.pyi
  title: Native vertebra._postgres client, pool, COPY, Arrow, and listener stubs
- id: duckdb-core
  resource: src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore
  title: DuckDBCore write-statement detection
- id: duckdb-secret
  resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig
  title: Cloud-agnostic DuckDB CREATE SECRET declaration
---

# Database Driver Limitations

<!-- truth: start -->
- `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (`adapters/postgres/driver.py` re-exporting `_sync` / `_async`) execute natively through `vertebra._postgres` (`vertebra-core[postgres]`) with `$1..$N` SQLSpec (`ParameterStyle.NUMERIC`) and SQLAlchemy (`numeric_dollar`) compilation, COPY/Arrow streams, and `AsyncPostgresListener`; `psycopg`, `psycopg_pool`, and `asyncpg` runtime paths and legacy dialer helpers are removed [^pg-driver] [^pg-stub].
- `PostgreSQLQueueBackend` probes `vertebra._postgres` (`vertebra-core[postgres]`, extra `postgres`) when no `executor=` is passed and raises `MissingDependencyError` or `ImproperConfigurationError`; no live executor ships for `PostgreSQLQueueBackend` or `OracleQueueBackend` (`oracledb`) [^pg-queue].
- Native Oracle (`vertebra._oracle`) and SQL Server (`vertebra._mssql`) optional extension packages are not yet published; `OracleConfig` and `MSSQLConfig` construct while driver initialization raises `MissingDependencyError`.
- `DuckDBCore` provides only `is_write_statement`; there is no GCS `CREATE SECRET` builder and no bundled GCS bearer-token secret provider. GCS `httpfs` access requires a caller-supplied `DuckDBSecretConfig` (`secret_type="gcs"`) with a static `value` mapping or a sync/async `value_provider` [^duckdb-core] [^duckdb-secret].
<!-- truth: end -->

## Planned Fixes

- The `native-postgres-driver` verification and conformance chapters cover live PostgreSQL integration runs against `vertebra._postgres` and final legacy cleanup.
- The `driver-conformance-ci` flow owns GCS bearer secret rotation for DuckDB and the live conformance lanes that exercise it.

Current adapter behavior is described in [Database Drivers & Cloud Dialers](database-drivers-and-dialers.md).

[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Native PostgreSQL drivers over `vertebra._postgres`.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend `vertebra._postgres` probe.
[^pg-stub]: `src/py/vertebra/_postgres.pyi` — Native `vertebra._postgres` client, pool, COPY, Arrow, and listener stubs.
[^duckdb-core]: `src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore` — `DuckDBCore` write-statement detection.
[^duckdb-secret]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — Cloud-agnostic DuckDB `CREATE SECRET` declaration.
