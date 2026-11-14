---
type: Gotcha
title: Database Driver Limitations
description: Cross-driver runtime caveats covering companion wheel loading, GIL-only DuckDB benchmark packaging, queue executors, and GCS secret configuration.
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
- by: agent:flow-executor
  at: "2026-11-14T01:15:00Z"
  commit: 8c615b73
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
- `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (`adapters/postgres/driver.py` re-exporting `_sync` / `_async`) execute natively through `vertebra._postgres` (`vertebra-core[postgres]`) with `$1..$N` dialect compilation; runtime pooler, logical replication slot, and Arrow interface boundaries are detailed in [PostgreSQL Native Driver Limitations](postgres-limitations.md) [^pg-driver] [^pg-stub].
- `PostgreSQLQueueBackend` probes `vertebra._postgres` (`vertebra-core[postgres]`, extra `postgres`) when no `executor=` is passed and raises `MissingDependencyError` when absent or `ImproperConfigurationError` when present without `executor=`; `OracleQueueBackend` (`oracledb`) likewise requires a caller-supplied executor [^pg-queue].
- Optional companion driver wheels (`vertebra-postgres`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-mssql`, `vertebra-duckdb`, `vertebra-bigquery`, `vertebra-spanner`) load dynamically via `ensure_module` and fail loud with `MissingDependencyError(install_package="vertebra-core[<extra>]")` when omitted from the active environment; upstream `duckdb` publishes no free-threaded wheel so `[duckdb]` / `dependency-groups.duckdb` benchmark history stays GIL-only while `vertebra._duckdb` builds across all supported CPython ABIs.
- `DuckDBCore` provides only `is_write_statement`; there is no GCS `CREATE SECRET` builder and no bundled GCS bearer-token secret provider. GCS `httpfs` access requires a caller-supplied `DuckDBSecretConfig` (`secret_type="gcs"`) with a static `value` mapping or a sync/async `value_provider` [^duckdb-core] [^duckdb-secret].
<!-- truth: end -->

## Related Context

- PostgreSQL operational caveats: [PostgreSQL Native Driver Limitations](postgres-limitations.md).
- Driver and cloud dialer behavior: [Database Drivers & Cloud Dialers](database-drivers-and-dialers.md).

[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Native PostgreSQL drivers over `vertebra._postgres`.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend `vertebra._postgres` probe.
[^pg-stub]: `src/py/vertebra/_postgres.pyi` — Native `vertebra._postgres` client, pool, COPY, Arrow, and listener stubs.
[^duckdb-core]: `src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore` — `DuckDBCore` write-statement detection.
[^duckdb-secret]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — Cloud-agnostic DuckDB `CREATE SECRET` declaration.
