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
  at: "2026-11-14T18:05:00Z"
  commit: 1b191388
- by: agent:flow-learn
  at: "2026-11-14T17:15:00Z"
  commit: 1b191388
sources:
- id: gcs-secrets
  resource: src/py/vertebra/storage/_secrets.py::SyncGcsBearerSecretProvider
  title: Shipped DuckDB GCS OAuth2 bearer secret providers in vertebra.storage
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
- id: duckdb-driver
  resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
  title: DuckDB driver HTTP 401/403 regex invalidation and startup cleanup
- id: ci-workflow
  resource: .github/workflows/ci.yml
  title: Non-blocking conformance-oracle job and test-db-conformance matrix
---

# Database Driver Limitations

<!-- truth: start -->
- `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (`adapters/postgres/driver.py` re-exporting `_sync` / `_async`) execute natively through `vertebra._postgres` (`vertebra-core[postgres]`) with `$1..$N` dialect compilation; runtime pooler, logical replication slot, and Arrow interface boundaries are detailed in [PostgreSQL Native Driver Limitations](postgres-limitations.md) [^pg-driver] [^pg-stub].
- `PostgreSQLQueueBackend` probes `vertebra._postgres` (`vertebra-core[postgres]`, extra `postgres`) when no `executor=` is passed and raises `MissingDependencyError` when absent or `ImproperConfigurationError` when present without `executor=`; `OracleQueueBackend` (`oracledb`) likewise requires a caller-supplied executor [^pg-queue].
- Optional companion driver wheels (`vertebra-postgres`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-mssql`, `vertebra-duckdb`, `vertebra-sqlite`, `vertebra-redis`, `vertebra-bigquery`, `vertebra-spanner`) load dynamically via `ensure_module` and fail loud with `MissingDependencyError(install_package="vertebra-core[<extra>]")` when omitted from the active environment; upstream PyPI `duckdb` publishes no free-threaded wheel and is isolated to `[dependency-groups].bench` (`python_version < '3.14'`) for `tools.benchmarks.store` result persistence while `vertebra._duckdb` builds across all supported CPython ABIs.
- **SQLAlchemy, Alembic & Django Dialect Boundaries**: BigQuery, DuckDB, and SQL Server have no Vertebra Django `DATABASES` backend (`VertebraPlugin` raises `ImproperConfigurationError` if configured; use SQLAlchemy or SQLSpec instead). `duckdb-engine`, `sqlalchemy-bigquery`, `sqlalchemy-spanner`, `django-google-spanner`, and `dogpile.cache` (`vertebra-core[dogpile]`) are optional upstream packages required only when instantiating those specific SQLAlchemy/Django dialect bases or `use_dogpile=True` cache regions, and BigQuery/Spanner execute non-transactional DDL (`autocommit=False` raises PEP 249 `NotSupportedError`).
- `DuckDBCore` provides only `is_write_statement`; `vertebra.storage.{SyncGcsBearerSecretProvider,AsyncGcsBearerSecretProvider}` supply GCS OAuth2 bearer-token rotation for `DuckDBSecretConfig(secret_type="gcs", value_provider=...)` (`persistent=True` with `value_provider` or non-positive/boolean `refresh_interval_seconds` fails validation) [^duckdb-core] [^duckdb-secret] [^gcs-secrets].
- DuckDB `httpfs` `(HTTP 401)` / `(HTTP 403)` reactive invalidation relies on error-message regex matching (`\(HTTP (?:401|403)\)`) because `vertebra._duckdb` surfaces HTTP status codes inside the exception message rather than a structured integer attribute, and invalidation marks the provider generation stale for the next statement without implicitly retrying the failed query [^duckdb-driver].
- Live DuckDB `httpfs` extension tests are gated behind `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` when `httpfs` is not pre-cached because `INSTALL httpfs` requires outbound network access, and the CI `conformance-oracle` job runs with `continue-on-error: true` in `.github/workflows/ci.yml` to isolate `gvenzl/oracle-free:23-slim-faststart` container startup variance on shared `ubuntu-latest` runners [^duckdb-driver] [^ci-workflow].
<!-- truth: end -->

## Related Context

- PostgreSQL operational caveats: [PostgreSQL Native Driver Limitations](postgres-limitations.md).
- Driver and cloud dialer behavior: [Database Drivers & Cloud Dialers](database-drivers-and-dialers.md).

[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Native PostgreSQL drivers over `vertebra._postgres`.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend `vertebra._postgres` probe.
[^pg-stub]: `src/py/vertebra/_postgres.pyi` — Native `vertebra._postgres` client, pool, COPY, Arrow, and listener stubs.
[^duckdb-core]: `src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore` — `DuckDBCore` write-statement detection.
[^duckdb-secret]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — Cloud-agnostic DuckDB `CREATE SECRET` declaration.
[^gcs-secrets]: `src/py/vertebra/storage/_secrets.py::SyncGcsBearerSecretProvider` — Shipped DuckDB GCS OAuth2 bearer secret providers in `vertebra.storage`.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB driver HTTP 401/403 regex invalidation and startup cleanup.
[^ci-workflow]: `.github/workflows/ci.yml` — Non-blocking `conformance-oracle` job and `test-db-conformance` matrix.
