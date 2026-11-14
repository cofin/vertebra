---
type: Guide
title: Database Drivers & Cloud Dialers
description: Per-adapter database and cache package contracts, standalone database driver wheels, shared Rust transport and Arrow SHM bridges, wire codecs, and Cloud SQL / AlloyDB mTLS dialers across vertebra-db and vertebra.db.
status: stable
tags:
  - domains
  - database
  - drivers
  - postgres
  - oracle
  - cloud-sql
  - alloydb
  - shm
generated:
  by: flow/0.30.1
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-14T18:43:00Z"
    commit: 104fd6fa
  - by: agent:flow-executor
    at: "2026-11-14T18:05:00Z"
    commit: 8290822e
  - by: agent:flow-learn
    at: "2026-11-14T17:15:00Z"
    commit: 334dc98d
sources:
  - id: src-vertebra-core-src-shm-rs
    resource: src/rs/vertebra-core/src/shm.rs
    title: ShmRegion and ShmCapsule shared-memory primitives
  - id: src-vertebra-core-src-proto-rs
    resource: src/rs/vertebra-core/src/proto.rs
    title: Zero-dependency Protobuf v3 wire helpers
  - id: src-vertebra-db-src-lib-rs
    resource: src/rs/vertebra-db/src/lib.rs
    title: Explicit vertebra-db Rust crate exports
  - id: src-vertebra-db-src-arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: Shared-memory Arrow IPC writers and unregistered regions
  - id: src-vertebra-db-src-connection-rs
    resource: src/rs/vertebra-db/src/connection.rs
    title: TransportStream, TlsMode, EndpointTarget, and Connection traits
  - id: src-vertebra-db-src-py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: Per-extension event-loop completion drainer and ShmCapsule bridge
  - id: src-vertebra-db-src-dialer-rs
    resource: src/rs/vertebra-db/src/dialer.rs
    title: Zero-sidecar Cloud SQL and AlloyDB mTLS dialers
  - id: src-dialer-base-py
    resource: src/py/vertebra/db/dialer/_base.py
    title: Shared _BaseGCPDialer, EphemeralCertBundle, and VALID_GCP_IP_TYPES primitives
  - id: src-vertebra-postgres-src-lib-rs
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: Standalone vertebra._postgres free-threaded extension exports
  - id: src-postgres-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py
    title: PostgreSQL SyncPostgreSQLDriver and AsyncPostgreSQLDriver re-exports
  - id: src-vertebra-mysql-src-lib-rs
    resource: src/rs/vertebra-mysql/src/lib.rs
    title: Native MySQL and MariaDB binary wire extension crate
  - id: src-vertebra-oracle-src-lib-rs
    resource: src/rs/vertebra-oracle/src/lib.rs
    title: Standalone vertebra._oracle gil_used=false PyO3 extension exports
  - id: src-oracle-driver-py
    resource: src/py/vertebra/db/adapters/oracle/driver.py
    title: Python SyncOracleDriver, AsyncOracleDriver, and delayed-commit pipelines
  - id: src-mssql-driver-py
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: Python SyncMSSQLDriver and AsyncMSSQLDriver over vertebra._mssql
  - id: src-vertebra-duckdb-src-lib-rs
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: Native DuckDB 2.0 PyO3 client, Arrow SHM writer, Appender, and savepoint journal
  - id: src-duckdb-driver-py
    resource: src/py/vertebra/db/adapters/duckdb/driver.py
    title: SyncDuckDBDriver and AsyncDuckDBDriver native DuckDB 2.0 adapters
  - id: src-vertebra-py-src-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs
    title: Canonical DatabaseError and multi-base cloud exception hierarchy
  - id: src-db-init-py
    resource: src/py/vertebra/db/__init__.py
    title: Lazy engine-agnostic vertebra.db public exports
  - id: src-db-registry-py
    resource: src/py/vertebra/db/registry.py
    title: Declarative DatabaseRegistry URL parsing and pool deduplication
  - id: src-db-driver-init-py
    resource: src/py/vertebra/db/driver/__init__.py
    title: SyncDatabaseDriver and AsyncDatabaseDriver base hierarchy
  - id: src-db-driver-capabilities-py
    resource: src/py/vertebra/db/driver/capabilities.py
    title: Runtime-checkable driver capability protocols
  - id: src-tests-integration-db-conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: Cross-driver acceptance and conformance suite
  - id: src-module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Dynamic optional dependency loader and MissingDependencyError
  - id: src-packaging-extras-gate
    resource: src/py/tests/unit/build/test_packaging_and_pgo.py
    title: Packaging extras gate verifying ensure_module extra literals
---

# Database Drivers & Cloud Dialers

<!-- truth: start -->
- **Structured Per-Adapter Layout (`adapters/<engine>/` & `vertebra.db`)**: Built-in adapters (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`, `datafusion`) live under `vertebra.db.adapters`, while `vertebra.db` lazily exports only engine-agnostic base classes, `DatabaseError` subclasses, dialers, `ArrowCursor`, `DatabaseRegistry`, CDC, and RLS primitives [^src-db-init-py].
- **Shared Connector & Async Drainers (`vertebra-db`, `vertebra-core`, `vertebra-py`)**: `connection.rs` defines `TransportStream`, `TlsMode`, `EndpointTarget`, `CancelHandle`, and `Connection`; `py_async.rs` provides `spawn_rust_db_task_with_attr` and cancel-safe `spawn_rust_db_task_with_attr_and_future` attaching per-extension event-loop drainers; `ShmCapsule` transfers `Arc<ShmRegion>` into `ArrowCursor._from_shm_capsule`; `vertebra_py::errors` defines `DatabaseError`, 12 typed subclasses, and `BigQueryError` / `SpannerError` [^src-vertebra-db-src-connection-rs] [^src-vertebra-db-src-py-async-rs] [^src-vertebra-py-src-errors-rs].
- **Fail-Loud Optional Companion Wheels**: `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` (`vertebra._postgres`, `vertebra-core[postgres]`), `MySQL` (`vertebra._mysql`), `Oracle` (`vertebra._oracle`), `MSSQL` (`vertebra._mssql`), `DuckDB` (`vertebra._duckdb`), `BigQuery` (`vertebra._bigquery`), and `Spanner` (`vertebra._spanner`) resolve through `ensure_module`, raising `MissingDependencyError(install_package="vertebra-core[<extra>]")` when absent; non-SQLite drivers never open `sqlite3`, and `postgres`/`mysql`/`redis`/`valkey` reject `memory://` with `ImproperConfigurationError` [^src-module-loader] [^src-packaging-extras-gate].
- **Wire Codecs & Zero-Sidecar Cloud Dialers**: `vertebra-db` holds stateless codecs (`PGCOPY`/`pgvector`, TTC/26ai `VECTOR`/`OSON`, TDS `@P1`, DuckDB `CREATE SECRET`, `RESP2`/`RESP3`) and zero-sidecar `CloudSQLDialer` (`:3307`) / `AlloyDBDialer` (`:5433` `MetadataExchangeRequest` via `vertebra_core::proto`) [^src-vertebra-db-src-dialer-rs] [^src-vertebra-core-src-proto-rs].
<!-- truth: end -->

[`src/rs/vertebra-db`](../../../../../src/rs/vertebra-db/src/lib.rs)[^src-vertebra-db-src-lib-rs] and [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py)[^src-db-init-py] define Vertebra's engine-neutral database contracts, companion PyO3 wheel seams, `VTBARROW` `ArrowCursor` transport, wire codecs, and zero-sidecar Cloud SQL / AlloyDB dialers. Standalone extensions are detailed in [PostgreSQL Native Extension](postgres-native-driver.md), [GCP BigQuery & Spanner Native Drivers](gcp-bigquery-and-spanner-drivers.md), and [ADR-004](../../decisions/adr-004-optional-driver-wheels-and-shm-ownership.md); query pipelines, CDC, and RLS live in [query-pipeline-cdc-and-tenancy](query-pipeline-cdc-and-tenancy.md); runtime caveats live in [PostgreSQL Limitations](postgres-limitations.md) and [Database Driver Limitations](database-driver-limitations.md).

## Polymorphic Registry & Driver Contracts

[`DatabaseRegistry`](../../../../../src/py/vertebra/db/registry.py)[^src-db-registry-py] builds `SCHEME_REGISTRY` declaratively from `REGISTERED_CONFIG_TYPES` (`BaseDatabaseConfig.url_schemes`):

- **URL Dispatch & Pool Deduplication**: `parse_url(url, **overrides)` resolves `postgresql`/`postgres`, `mysql`/`mariadb`, `sqlite`, `oracle`, `mssql`/`sqlserver`, `duckdb`, `bigquery`/`bq`, `spanner`/`spangres`/`spanner+googlesql`/`spanner+spangres`, `redis`/`rediss`, `valkey`/`valkeys`, `datafusion`, `cloudsql`, and `alloydb`. `get_shared_driver` and `get_shared_sync_driver` deduplicate by `config.pool_key()` (`None` isolates `:memory:` SQLite/DuckDB), while `provide_driver` rejects `CloudSQLConfig` / `AlloyDBConfig` with `TypeError` (use `get_dialer`).
- **Uniform Driver Surface & Capabilities**: [`vertebra.db.driver`](../../../../../src/py/vertebra/db/driver/__init__.py)[^src-db-driver-init-py] standardizes `execute`, `execute_many`, `execute_tuples`, `execute_with_description`, `select`, `select_one` (`NotFoundError`), `select_one_or_none`, `select_value`, `select_to_arrow -> ArrowCursor`, `stream_arrow_batches`, `load_from_arrow`, `transaction()`, `cancel()`, and `close()`; [`capabilities.py`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py] defines `SyncPipelineCapable`, `AsyncPipelineCapable`, `NotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, and `EmbeddingPushdown`.

## Relational, Analytical & Cache Adapters

| Adapter & Rust Module | Config & Core | Wire & Runtime Behavior |
|---|---|---|
| [`adapters/postgres/`](../../../../../src/py/vertebra/db/adapters/postgres/__init__.py)[^src-postgres-driver-py] / [`vertebra-postgres`](../../../../../src/rs/vertebra-postgres/src/lib.rs)[^src-vertebra-postgres-src-lib-rs] | `PostgreSQLConfig`, `PostgreSQLCore`, `binds.py`, `options.py`, `PostgresChangeSource` | Pure-Rust `vertebra._postgres` (`SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver`), Protocol `3.0` (`14`–`18`), SCRAM-SHA-256-PLUS, Cloud SQL (`:3307`) / AlloyDB (`:5433`) mTLS + `:5432` IAM, `PoolerMode` cache gating, single-pass `$N` lexer (`binds.py`), libpq/Unix-socket/multi-host `options.py`, single-`Sync` pipelines, `PGCOPY`, `VTBARROW`, `AsyncPostgresListener`, `AsyncPostgresReplication`, and shielded CAS `PostgresChangeSource`; rejects `memory://`. |
| [`adapters/mysql/`](../../../../../src/py/vertebra/db/adapters/mysql/__init__.py) / [`vertebra-mysql`](../../../../../src/rs/vertebra-mysql/src/lib.rs)[^src-vertebra-mysql-src-lib-rs] | `MySQLConfig` | Pure-Rust `vertebra._mysql`, Handshake v10 (`caching_sha2_password`), LRU `COM_STMT_PREPARE`/`EXECUTE`, MySQL 9 `VECTOR(N)`, `SyncMySQLPipeline` / `AsyncMySQLPipeline`, `VTBARROW`, `:3306` IAM + `:3307` Cloud SQL mTLS, and `KILL QUERY`. |
| [`adapters/sqlite/`](../../../../../src/py/vertebra/db/adapters/sqlite/__init__.py) / [`sqlite/mod.rs`](../../../../../src/rs/vertebra-db/src/sqlite/mod.rs) | `SQLiteConfig`, `SQLiteCore` | `SqliteConfig` WAL pragmas, `LeaseBroker`-serialized writes, `RETURNING` finalization before commit, and `ArrowCursor` streaming. |
| [`adapters/oracle/`](../../../../../src/py/vertebra/db/adapters/oracle/driver.py)[^src-oracle-driver-py] / [`vertebra-oracle`](../../../../../src/rs/vertebra-oracle/src/lib.rs)[^src-vertebra-oracle-src-lib-rs] | `OracleConfig`, `OracleCore`, `SyncOraclePool`, `AsyncOraclePool` | Standalone `vertebra._oracle` pure-Rust `oracledb` (`=26.0.0-beta.5`, zero Instant Client), per-connection worker threads + `OracleBreakHandle` cancellation, `.zip`/`ewallet.pem` mTLS + IAM, DRCP (`cclass`/`purity`), TTC `[0x11, 0x6e]` piggyback, 26ai `VECTOR`/`OSON`, `arrow 59` -> `VTBARROW` `ArrowCursor`, delayed-commit pipelines, and `OracleNotificationSource` `wait_timeout_seconds` `SELECT 1 FROM DUAL` idle liveness probing. |
| [`adapters/mssql/`](../../../../../src/py/vertebra/db/adapters/mssql/driver.py)[^src-mssql-driver-py] / [`vertebra-mssql`](../../../../../src/rs/vertebra-mssql/src/lib.rs) | `MSSQLConfig`, `MSSQLCore` | Standalone `vertebra._mssql` (`tiberius` + `rustls`), TDS 7.4/8.0, `sp_executesql` RPC, `BulkLoad`, `ATTENTION` cancel, `@P1` rewriter, `VTBARROW`, pipelines, and Cloud SQL `:3307` mTLS. |
| [`adapters/duckdb/`](../../../../../src/py/vertebra/db/adapters/duckdb/driver.py)[^src-duckdb-driver-py] / [`vertebra-duckdb`](../../../../../src/rs/vertebra-duckdb/src/lib.rs)[^src-vertebra-duckdb-src-lib-rs] | `DuckDBConfig`, `DuckDBSecretConfig`, `DuckDBCore` | Native `vertebra._duckdb` (bundled `duckdb`, `arrow 60`), single-writer / multi-reader pool, `TxJournal` savepoints, pipelines, `VTBARROW`, `Appender` bulk load, `InterruptHandle`, `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` `CREATE SECRET` rotation with `(HTTP 401)`/`(HTTP 403)` invalidation, and startup `_client` cleanup on secret failure. |
| [`adapters/redis/`](../../../../../src/py/vertebra/db/adapters/redis/__init__.py) & [`valkey/`](../../../../../src/py/vertebra/db/adapters/valkey/__init__.py) | `RedisConfig`, `ValkeyConfig` | `RESP2`/`RESP3` (`HELLO 3`), `CLIENT TRACKING` invalidation, Sentinel, Cluster CRC16 slot routing, and Valkey IAM; reject `memory://`. |
| [`adapters/datafusion/`](../../../../../src/py/vertebra/db/adapters/datafusion/__init__.py) | `DataFusionConfig`, `DataFusionEngine` | Embedded DataFusion SQL federation joining Parquet/CSV object stores with live relational and [GCP BigQuery & Spanner](gcp-bigquery-and-spanner-drivers.md) `ArrowCursor` tables. |

## Shared Connector, Drainers, Missing Dependencies & Cloud Dialers

- **Transport & Drainers**: [`connection.rs`](../../../../../src/rs/vertebra-db/src/connection.rs)[^src-vertebra-db-src-connection-rs] normalizes TCP, `rustls`, Unix socket, Cloud SQL `:3307`, and AlloyDB `:5433` streams with `is_transient()` / SQLSTATE mapping. [`py_async.rs`](../../../../../src/rs/vertebra-db/src/py_async.rs)[^src-vertebra-db-src-py-async-rs] attaches per-extension `AsyncCompletionQueue` drainers (`_vertebra_{core,bigquery,mysql,mssql,oracle,spanner,duckdb,postgres}_completion_drainer`), converting panics to `TaskPanicError`, binding Python future cancellation to Tokio `AbortHandle` (`spawn_rust_db_task_with_attr_and_future` / `DbTaskCanceller`) or cancel callbacks (`spawn_rust_db_task_with_attr_and_cancel` / `CustomDbTaskCanceller`), and wrapping [`ShmRegion`](../../../../../src/rs/vertebra-core/src/shm.rs)[^src-vertebra-core-src-shm-rs] ownership in `c"vertebra.ShmCapsule"` via `construct_py_arrow_cursor`. [`arrow_shm.rs`](../../../../../src/rs/vertebra-db/src/arrow_shm.rs)[^src-vertebra-db-src-arrow-shm-rs] handles `VTBARROW` IPC framing and `allocate_shm_region`, while `ArrowCursor.release()`, `close()`, `__exit__()`, and `Drop` (`src/rs/vertebra-py/src/db.rs`) trigger `ShmRegion::unlink_now()`.
- **Missing Dependencies**: [`ensure_module`](../../../../../src/py/vertebra/utils/module_loader.py)[^src-module-loader] raises `MissingDependencyError` defaulting `install_package="vertebra-core[<extra>]"`, enforced by AST scanning in [`test_packaging_and_pgo.py`](../../../../../src/py/tests/unit/build/test_packaging_and_pgo.py)[^src-packaging-extras-gate].
- **Cloud Dialers (`_base.py`, `cloudsql.py`, `alloydb.py`, `dialer.rs`)**: `_BaseGCPDialer`, `EphemeralCertBundle`, and `VALID_GCP_IP_TYPES` (`dialer/_base.py`) share async cert refresh (`generateEphemeralCert` `:3307` / `generateClientCertificate` `:5433`), RSA keygen, verified TLS 1.3 mTLS, and explicit IAM token propagation with `cloudsql.py`, `alloydb.py`, and pure-Rust dialers (`zero third-party driver packages`) [^src-dialer-base-py].

## Database Driver Conformance

Driver transaction scopes roll back on `BaseException` (shielding async rollback/eviction via `anyio.CancelScope(shield=True)`). Cross-engine parity is enforced by [`test_driver_conformance.py`](../../../../../src/py/tests/integration/db/test_driver_conformance.py)[^src-tests-integration-db-conformance-py] and `test_{duckdb,mysql,mssql,oracle}_native.py` under zero-skip `ConformanceGate` (`VERTEBRA_CONFORMANCE_ENGINES`) and `ServiceVersionMatrix` (`VERTEBRA_CONFORMANCE_{MYSQL,MARIADB,MSSQL,ORACLE}_VERSION`) covering `duckdb`, `mysql` (`8.0`, `8.4`, `9.6`, `9.7`), `mariadb` (`11.3`, `11.4`, `11.8`, `12.2`, `12.3`, `13.0`), `mssql` (`2022`, `2025`), and `oracle` (`23ai` / `26ai Free`), alongside `postgres` (`14`–`18` under `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`), `sqlite`, `bigquery`, and `spanner`. While all seven native SQL/cloud engines execute over pure-Rust PyO3 wheels, `sqlite` runs on stdlib `sqlite3` and `redis`/`valkey` pair `vertebra-db` RESP3 parsers with Python network streams.

[^src-vertebra-core-src-shm-rs]: `src/rs/vertebra-core/src/shm.rs` — ShmRegion and ShmCapsule shared-memory primitives.
[^src-vertebra-core-src-proto-rs]: `src/rs/vertebra-core/src/proto.rs` — Zero-dependency Protobuf v3 wire helpers.
[^src-vertebra-db-src-lib-rs]: `src/rs/vertebra-db/src/lib.rs` — Explicit vertebra-db Rust crate exports.
[^src-vertebra-db-src-arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs` — Shared-memory Arrow IPC writers and unregistered regions.
[^src-vertebra-db-src-connection-rs]: `src/rs/vertebra-db/src/connection.rs` — TransportStream, TlsMode, EndpointTarget, and Connection traits.
[^src-vertebra-db-src-py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Per-extension event-loop completion drainer and ShmCapsule bridge.
[^src-vertebra-db-src-dialer-rs]: `src/rs/vertebra-db/src/dialer.rs` — Zero-sidecar Cloud SQL and AlloyDB mTLS dialers.
[^src-dialer-base-py]: `src/py/vertebra/db/dialer/_base.py` — Shared `_BaseGCPDialer`, `EphemeralCertBundle`, and `VALID_GCP_IP_TYPES` primitives.
[^src-vertebra-postgres-src-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Standalone `vertebra._postgres` extension exports.
[^src-postgres-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py` — PostgreSQL `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` re-exports.
[^src-vertebra-mysql-src-lib-rs]: `src/rs/vertebra-mysql/src/lib.rs` — Native MySQL and MariaDB binary wire extension crate.
[^src-vertebra-oracle-src-lib-rs]: `src/rs/vertebra-oracle/src/lib.rs` — Standalone `vertebra._oracle` `gil_used=false` PyO3 extension exports.
[^src-oracle-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py` — Python `SyncOracleDriver`, `AsyncOracleDriver`, and delayed-commit pipelines.
[^src-mssql-driver-py]: `src/py/vertebra/db/adapters/mssql/driver.py` — Python `SyncMSSQLDriver` and `AsyncMSSQLDriver` over `vertebra._mssql`.
[^src-vertebra-duckdb-src-lib-rs]: `src/rs/vertebra-duckdb/src/lib.rs` — Native DuckDB 2.0 PyO3 client, Arrow SHM writer, Appender, and savepoint journal.
[^src-duckdb-driver-py]: `src/py/vertebra/db/adapters/duckdb/driver.py` — `SyncDuckDBDriver` and `AsyncDuckDBDriver` native DuckDB 2.0 adapters.
[^src-vertebra-py-src-errors-rs]: `src/rs/vertebra-py/src/errors.rs` — Canonical `DatabaseError` and multi-base cloud exception hierarchy.
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Lazy engine-agnostic `vertebra.db` public exports.
[^src-db-registry-py]: `src/py/vertebra/db/registry.py` — Declarative `DatabaseRegistry` URL parsing and pool deduplication.
[^src-db-driver-init-py]: `src/py/vertebra/db/driver/__init__.py` — `SyncDatabaseDriver` and `AsyncDatabaseDriver` base hierarchy.
[^src-db-driver-capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — Runtime-checkable driver capability protocols.
[^src-tests-integration-db-conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py` — Cross-driver acceptance and conformance suite.
[^src-module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Dynamic optional dependency loader.
[^src-packaging-extras-gate]: `src/py/tests/unit/build/test_packaging_and_pgo.py` — Packaging extras gate verifying `ensure_module` extra literals.
