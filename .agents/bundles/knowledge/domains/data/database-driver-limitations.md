---
type: Gotcha
title: Database Driver Limitations
description: Verified gaps in the database adapters, covering the unwired native PostgreSQL wheel and DuckDB GCS credential handling, that callers and maintainers must account for.
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
  title: PostgreSQL drivers over psycopg_pool and asyncio.to_thread
- id: pg-arrow
  resource: src/py/vertebra/db/adapters/postgres/arrow.py
  title: PostgreSQL Arrow helpers referencing vertebra._postgres under TYPE_CHECKING
- id: pg-notify
  resource: src/py/vertebra/db/adapters/postgres/notify.py
  title: PostgreSQL notification source referencing vertebra._postgres under TYPE_CHECKING
- id: pg-queue
  resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
  title: PostgreSQL queue backend psycopg probe
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
- The `vertebra-postgres` wheel (`vertebra._postgres`, `vertebra-core[postgres]`) ships `SyncPostgresClient`, `AsyncPostgresClient`, pools, portals, COPY streams, Arrow streams, and `AsyncPostgresListener`, but no production adapter calls it [^pg-stub].
- `SyncPostgreSQLDriver` executes through `psycopg_pool.ConnectionPool` (`ensure_module("psycopg_pool", install_package="psycopg[binary,pool]")`), and `AsyncPostgreSQLDriver` offloads the sync driver with `asyncio.to_thread` [^pg-driver].
- `adapters/postgres/arrow.py`, `events.py`, and `notify.py` import `vertebra._postgres` names only under `TYPE_CHECKING`; at runtime `pipeline.py` and `arrow.py` are exercised only by unit tests [^pg-arrow] [^pg-notify].
- `PostgreSQLQueueBackend` probes `psycopg` when no `executor=` is passed and raises `MissingDependencyError` or `ImproperConfigurationError` [^pg-queue].
- `DuckDBCore` provides only `is_write_statement`; there is no GCS `CREATE SECRET` builder and no bundled GCS bearer-token secret provider. GCS `httpfs` access requires a caller-supplied `DuckDBSecretConfig` (`secret_type="gcs"`) with a static `value` mapping or a sync/async `value_provider` [^duckdb-core] [^duckdb-secret].
<!-- truth: end -->

## Planned Fixes

- The `native-postgres-driver` flow owns the atomic cutover of `SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver`, the queue backend, SQLSpec, SQLAlchemy, and dialer helpers onto `vertebra._postgres`, and the removal of `psycopg`/`asyncpg` from runtime paths.
- The `driver-conformance-ci` flow owns GCS bearer secret rotation for DuckDB and the live conformance lanes that exercise it.

Current adapter behavior is described in [Database Drivers & Cloud Dialers](database-drivers-and-dialers.md).

[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — PostgreSQL drivers over `psycopg_pool` and `asyncio.to_thread`.
[^pg-arrow]: `src/py/vertebra/db/adapters/postgres/arrow.py` — PostgreSQL Arrow helpers referencing `vertebra._postgres` under `TYPE_CHECKING`.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification source referencing `vertebra._postgres` under `TYPE_CHECKING`.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend `psycopg` probe.
[^pg-stub]: `src/py/vertebra/_postgres.pyi` — Native `vertebra._postgres` client, pool, COPY, Arrow, and listener stubs.
[^duckdb-core]: `src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore` — `DuckDBCore` write-statement detection.
[^duckdb-secret]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — Cloud-agnostic DuckDB `CREATE SECRET` declaration.
