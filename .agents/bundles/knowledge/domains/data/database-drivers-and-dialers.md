---
type: Guide
title: Database Drivers & Cloud Dialers
description: Per-adapter database and cache contracts, standalone driver wheels, shared Rust transport and Arrow SHM bridges, cloud IAM token providers, and Cloud SQL, AlloyDB, AWS, and Azure dialers.
status: stable
tags:
  - domains
  - database
  - drivers
  - postgres
  - oracle
  - cloud-sql
  - alloydb
  - aws-rds
  - azure-entra
  - shm
generated:
  by: flow/0.30.1
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-21T21:06:00Z"
    commit: 06e99fbf
  - by: agent:flow-executor
    at: "2026-11-21T01:18:00Z"
    commit: 06e99fbf
  - by: agent:flow-executor
    at: "2026-11-19T03:10:00Z"
    commit: 6f76f8a7
  - by: agent:flow-executor
    at: "2026-11-17T01:22:00Z"
    commit: 5a833832
  - by: agent:flow-completion
    at: "2026-11-14T21:38:00Z"
    commit: c526639f
  - by: agent:flow-executor
    at: "2026-11-14T20:49:30Z"
    commit: c526639f
  - by: agent:flow-setup
    at: "2026-11-14T18:43:00Z"
    commit: 1b191388
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
    title: Zero-sidecar Cloud SQL, AlloyDB, AWS RDS, Aurora DSQL, and Azure Entra dialers plus AWS RDS Root CAs
  - id: src-dialer-base-py
    resource: src/py/vertebra/db/dialer/_base.py
    title: Shared _BaseGCPDialer, EphemeralCertBundle, and VALID_GCP_IP_TYPES primitives
  - id: src-dialer-aws-py
    resource: src/py/vertebra/db/dialer/aws.py
    title: Python SyncAwsRdsDialer, AsyncAwsRdsDialer, SyncAwsAuroraDsqlDialer, and AsyncAwsAuroraDsqlDialer
  - id: src-dialer-azure-py
    resource: src/py/vertebra/db/dialer/azure.py
    title: Python SyncAzureEntraDialer and AsyncAzureEntraDialer TLS and token dialers
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
    title: Python SyncOracleDriver and AsyncOracleDriver barrel over _sync.py and _async.py
  - id: src-oracle-pipeline-py
    resource: src/py/vertebra/db/adapters/oracle/pipeline.py
    title: Delayed-commit SyncOraclePipeline and AsyncOraclePipeline
  - id: src-bigquery-object-tables-py
    resource: src/py/vertebra/db/adapters/bigquery/object_tables.py
    title: BigQuery ObjectRef and ObjectTableConfig domain helpers
  - id: src-spanner-mutations-py
    resource: src/py/vertebra/db/adapters/spanner/mutations.py
    title: Spanner StalenessBound, Mutation, and MutationGroup structs
  - id: src-mssql-driver-py
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: Python SyncMSSQLDriver and AsyncMSSQLDriver over vertebra._mssql
  - id: src-vertebra-duckdb-src-lib-rs
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: Native DuckDB 2.0 PyO3 client, Arrow SHM writer, Appender, and savepoint journal
  - id: src-duckdb-driver-py
    resource: src/py/vertebra/db/adapters/duckdb/driver.py
    title: SyncDuckDBDriver and AsyncDuckDBDriver native DuckDB 2.0 adapters
  - id: src-vertebra-sqlite-src-lib-rs
    resource: src/rs/vertebra-sqlite/src/lib.rs
    title: Native SQLite PyO3 client, WAL worker pool, and Arrow SHM bridge
  - id: src-sqlite-driver-py
    resource: src/py/vertebra/db/adapters/sqlite/driver.py
    title: SyncSQLiteDriver and AsyncSQLiteDriver barrel over _sync.py and _async.py
  - id: src-redis-driver-py
    resource: src/py/vertebra/db/adapters/redis/driver.py
    title: SyncRedisDriver and AsyncRedisDriver barrel over _sync.py and _async.py
  - id: src-valkey-driver-py
    resource: src/py/vertebra/db/adapters/valkey/driver.py
    title: SyncValkeyDriver and AsyncValkeyDriver barrel over _sync.py and _async.py
  - id: src-datafusion-core-py
    resource: src/py/vertebra/db/adapters/datafusion/core.py
    title: DataFusionCore session builder, source registration, and explain helpers
  - id: src-datafusion-driver-py
    resource: src/py/vertebra/db/adapters/datafusion/driver.py
    title: DataFusionEngine federated Arrow session context
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
  - id: src-db-driver-common-py
    resource: src/py/vertebra/db/driver/_common.py
    title: CommonDriverMixin shared checkout broker lease, trace annotation, and row hydration helpers
  - id: src-db-driver-resp-py
    resource: src/py/vertebra/db/driver/_resp.py
    title: Shared RESP3 ClusterRedirect, PubSubMessage, CRC16-XMODEM, and Resp3Error helpers
  - id: src-db-driver-cloud-auth-py
    resource: src/py/vertebra/db/driver/_cloud_auth.py
    title: AwsRdsIamTokenProvider, AwsAuroraDsqlIamTokenProvider, and AzureEntraTokenProvider
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
- **Uniform Per-Adapter Layout (`adapters/<engine>/` & `vertebra.db`)**: All ten client/embedded engine adapters (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`) split synchronous and asynchronous drivers into `_sync.py` and `_async.py` behind a thin `driver.py` barrel (`config.py`, `core.py`, relational `{postgres,mysql,oracle,mssql,duckdb,sqlite}/pipeline.py`, `bigquery/object_tables.py`, `spanner/{mutations,transactions}.py`, `{redis,valkey}/notify.py`, and lazy `__init__.py` `_EXPORT_MAP`). Embedded `datafusion` (`config.py`, `core.py`, `driver.py`, `__init__.py`, `mode="in_process"`) builds `SessionContext` with `target_partitions` and `memory_limit_mb`, registers Parquet/CSV/NDJSON/Arrow/store sources via `DataFusionCore`, and `vertebra.db` lazily exports only engine-agnostic bases, dialers, `ArrowCursor`, `DatabaseRegistry`, CDC, and RLS primitives (no `vertebra/drivers/` package exists) [^src-db-init-py] [^src-datafusion-core-py].
- **Uniform Cursor Surface, Single-Call Methods, Checkout Leases, RESP & Cloud-Auth Paths (`db/driver/`)**: Every SQL driver exposes `cursor()` returning the extension's PEP 249-shaped cursor. On SQLite, DuckDB, PostgreSQL, and Oracle, native-driver query methods call `fetch_rows`, `fetch_dicts`, `fetch_value`, or `execute_dml` directly, while `cursor()` serves DB-API, SQLSpec, and step-by-step fetches. `CommonDriverMixin` (`_common.py`) handles per-connection checkout broker leases for network pools (embedded SQLite and DuckDB take none). `_resp.py` centralizes RESP3 cluster and PubSub helpers (`ClusterRedirect`, `PubSubMessage`, `NON_KEY_COMMANDS`, `crc16_xmodem`, `hash_slot`, `parse_cluster_redirect`, `Resp3Error(DatabaseError)`), and `_cloud_auth.py` provides `AwsRdsIamTokenProvider`, `AwsAuroraDsqlIamTokenProvider`, and `AzureEntraTokenProvider` across PostgreSQL, MySQL, MSSQL, Redis, and Valkey [^src-db-driver-common-py] [^src-db-driver-resp-py] [^src-db-driver-cloud-auth-py].
- **Fail-Loud Companion Wheels & Shared Async Bridge**: Optional driver wheels (`vertebra._postgres`, `_mysql`, `_oracle`, `_mssql`, `_duckdb`, `_sqlite`, `_bigquery`, `_spanner`, `_redis`, `_valkey`) resolve via `ensure_module` (`MissingDependencyError(install_package="vertebra-core[<extra>]")`); `connection.rs` and `py_async.rs` provide unified stream transport, event-loop completion drainers, and `ShmCapsule` `ArrowCursor` transfer; `postgres`/`mysql`/`redis`/`valkey` reject `memory://` with `ImproperConfigurationError` [^src-module-loader] [^src-vertebra-db-src-connection-rs] [^src-vertebra-db-src-py-async-rs].
- **Wire Extensions & Zero-Sidecar Dialers**: `vertebra-postgres` supports CockroachDB (`cockroach_cluster` `--cluster=<id>` startup options, `crdb_version` `RESET ALL`, `40001` / `SAVEPOINT cockroach_restart` retry) and AlloyDB (`v1beta` with `v1alpha` fallback, `pscDnsName`/`pscAutoDnsName` precedence, `enable_alloydb_columnar_engine` GUCs, and structured `MetadataExchangeResponse` diagnostics) over `vertebra-db` stateless codecs. [`dialer.rs`](../../../../../src/rs/vertebra-db/src/dialer.rs)[^src-vertebra-db-src-dialer-rs] and `vertebra.db.dialer` bundle 114 AWS RDS Root CAs (`aws_rds_root_ca_pem()`) alongside Cloud SQL, AlloyDB, `AwsRdsDialer`, `AwsAuroraDsqlDialer`, and `AzureEntraDialer` dialers plus ambient AWS and Azure credential chains.
<!-- truth: end -->

[`src/rs/vertebra-db`](../../../../../src/rs/vertebra-db/src/lib.rs)[^src-vertebra-db-src-lib-rs] and [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py)[^src-db-init-py] define Vertebra's engine-neutral database contracts, companion PyO3 wheel seams, `VTBARROW` `ArrowCursor` transport, wire codecs, cloud IAM token providers, and zero-sidecar Cloud SQL, AlloyDB, AWS, and Azure dialers. Standalone extensions are detailed in [PostgreSQL Native Extension](postgres-native-driver.md), [Google Cloud BigQuery & Spanner Native Drivers](gcp-bigquery-and-spanner-drivers.md), and [ADR-004](../../decisions/adr-004-optional-driver-wheels-and-shm-ownership.md); query pipelines, CDC, and RLS live in [query-pipeline-cdc-and-tenancy](query-pipeline-cdc-and-tenancy.md); runtime caveats live in [PostgreSQL Limitations](postgres-limitations.md) and [Database Driver Limitations](database-driver-limitations.md).

## Polymorphic Registry, `CommonDriverMixin` & Cloud Auth

[`DatabaseRegistry`](../../../../../src/py/vertebra/db/registry.py)[^src-db-registry-py] builds `SCHEME_REGISTRY` declaratively from `REGISTERED_CONFIG_TYPES` (`BaseDatabaseConfig.url_schemes`):

- **URL Dispatch & Pool Deduplication**: `parse_url(url, **overrides)` resolves `postgresql`/`postgres`, `mysql`/`mariadb`, `sqlite`, `oracle`, `mssql`/`sqlserver`, `duckdb`, `bigquery`/`bq`, `spanner`/`spangres`/`spanner+googlesql`/`spanner+spangres`, `redis`/`rediss`, `valkey`/`valkeys`, `datafusion`, `cloudsql`, and `alloydb`. `get_shared_driver` and `get_shared_sync_driver` deduplicate by `config.pool_key()` (`None` isolates `:memory:` SQLite/DuckDB), while `provide_driver` rejects `CloudSQLConfig` / `AlloyDBConfig` with `TypeError` (use `get_dialer`).
- **Uniform Driver Surface & `CommonDriverMixin`**: [`vertebra.db.driver`](../../../../../src/py/vertebra/db/driver/__init__.py)[^src-db-driver-init-py] standardizes `cursor()`, `execute`, `execute_dml`, `execute_many`, `execute_tuples`, `execute_with_description`, `select`, `select_one` (`NotFoundError`), `select_one_or_none`, `select_value`, `select_to_arrow -> ArrowCursor`, `stream_arrow_batches`, `load_from_arrow`, `transaction()`, `cancel()`, and `close()`. Every SQL driver exposes `cursor()` returning the extension's PEP 249-shaped cursor. On SQLite, DuckDB, PostgreSQL, and Oracle, native-driver query methods call one of four single-call methods directly while MySQL, SQL Server, Spanner, and BigQuery native drivers run their query methods on the cursor. [`CommonDriverMixin`](../../../../../src/py/vertebra/db/driver/_common.py)[^src-db-driver-common-py] owns `_acquire_broker_lease`, `_attach_checkout_lease`, `_lease_plan`, `_BrokerLeasePlan`, `_acquire_pid_lease`, and `_release_pid_lease`; network adapters (`oracle`, `mssql`, `mysql`, `postgres`) attach one broker lease per checked-out connection through this path, while embedded engines (`sqlite`, `duckdb`) take no broker lease. [`capabilities.py`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py] defines `SyncPipelineCapable`, `AsyncPipelineCapable`, `SyncNotificationSource`, `AsyncNotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, and `EmbeddingPushdown`.
- **Cloud IAM Database Token Providers (`_cloud_auth.py`)**: [`_cloud_auth.py`](../../../../../src/py/vertebra/db/driver/_cloud_auth.py)[^src-db-driver-cloud-auth-py] implements `DatabaseTokenProvider` (`resolve_sync`, `resolve_async`, `invalidate`, `TokenSnapshot`) with pre-expiry refresh margins and temporary credential expiry clamping. `AwsRdsIamTokenProvider` (`refresh_margin_seconds = 60.0`, `ttl <= 900s`) and `AwsAuroraDsqlIamTokenProvider` (`refresh_margin_seconds = 60.0`, `ttl <= 604800s`) resolve `AWS_*` env vars, `~/.aws/{credentials,config}` profiles, `AWS_WEB_IDENTITY_TOKEN_FILE` STS XML, host-allowlisted `AWS_CONTAINER_CREDENTIALS_*`, and EC2 IMDSv2 off-loop via `anyio.to_thread.run_sync`. `AzureEntraTokenProvider` (`refresh_margin_seconds = 120.0`, `AZURE_{POSTGRES,MYSQL,MSSQL,REDIS}_SCOPE`) supports explicit client secrets, ambient `AZURE_*` env vars, `AZURE_FEDERATED_TOKEN_FILE`, `IDENTITY_ENDPOINT` + `IDENTITY_HEADER`, and VM IMDS, forwarding tokens into `tiberius`, PostgreSQL, MySQL, Redis, and Valkey.

| Single-call method | Returns | Driver methods that call it |
| --- | --- | --- |
| `fetch_rows(sql, params=())` | `(rows, names)`; `([], ())` with no result set | `execute_with_description`, `execute_tuples`, and `select` / `select_one` / `select_one_or_none` when `schema_type` is set |
| `fetch_dicts(sql, params=())` | One `dict` per row, keyed by column name | `select`, `select_one`, and `select_one_or_none` without `schema_type` (or with `schema_type=dict`) |
| `fetch_value(sql, params=())` | First column of the first row; `NotFoundError` with no row | `select_value` |
| `execute_dml(sql, params=())` | `(rowcount, lastrowid)` | `execute` and `execute_dml` |

These methods exist on each client class of the SQLite, DuckDB, PostgreSQL, and Oracle extensions and on the PostgreSQL and Oracle pool classes (`SyncPostgresPool`, `AsyncPostgresPool`, `SyncOraclePool`, `AsyncOraclePool`). A pool method checks a connection out, runs the statement, and returns the connection inside the extension. Each method runs the statement through the same statement runner the cursor uses and keeps no cursor state.

MySQL, SQL Server, Spanner, and BigQuery native drivers run their query methods on the cursor. On the Oracle driver, out-bind values and session-context tracking come back only from `execute_dml`, which `execute` calls; `fetch_rows`, `fetch_dicts`, and `fetch_value` return rows only, so a caller needing out-binds from a row-returning statement uses `cursor()`.

## Relational, Analytical & Cache Adapters

| Adapter & Rust Module | Config, Core & Layout | Wire & Runtime Behavior |
|---|---|---|
| [`adapters/postgres/`](../../../../../src/py/vertebra/db/adapters/postgres/__init__.py)[^src-postgres-driver-py] / [`vertebra-postgres`](../../../../../src/rs/vertebra-postgres/src/lib.rs)[^src-vertebra-postgres-src-lib-rs] | `PostgreSQLConfig`, `PostgreSQLCore`, `_sync.py`/`_async.py`/`driver.py`, `binds.py`, `options.py`, `change_stream.py` | Pure-Rust `vertebra._postgres` (`SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver`), Protocol `3.0` (`14`–`18`), SCRAM-SHA-256-PLUS, Cloud SQL (`:3307`) / AlloyDB (`:5433` `v1beta` with `v1alpha` fallback, `pscDnsName`/`pscAutoDnsName`, `enable_alloydb_columnar_engine` GUCs, `MetadataExchangeResponse`), CockroachDB (`cockroach_cluster` `--cluster=<id>`, `crdb_version` `RESET ALL`, `40001`/`SAVEPOINT cockroach_restart`), AWS RDS/Aurora DSQL & Azure Entra token auth, `PoolerMode` O(1) LRU `StatementCache`, `PGCOPY`, `VTBARROW`, `AsyncPostgresListener`, `AsyncPostgresReplication`, `PostgresChangeSource` + `SqlPostgresCheckpointStore`; rejects `memory://`. |
| [`adapters/mysql/`](../../../../../src/py/vertebra/db/adapters/mysql/__init__.py) / [`vertebra-mysql`](../../../../../src/rs/vertebra-mysql/src/lib.rs)[^src-vertebra-mysql-src-lib-rs] | `MySQLConfig`, `MySQLCore`, `_sync.py`/`_async.py`/`driver.py`, `pipeline.py` | Pure-Rust `vertebra._mysql`, `MySQLCore` literal-safe bind compilation, non-blocking async DSN parsing (`_parse_dsn_fields`), Handshake v10, LRU `COM_STMT_PREPARE`/`EXECUTE`, MySQL 9 `VECTOR(N)`, `SyncMySQLPipeline` / `AsyncMySQLPipeline`, `VTBARROW`, AWS RDS IAM + Azure Entra token auth, `:3307` Cloud SQL mTLS, `CommonDriverMixin` broker leasing, and `KILL QUERY`. |
| [`adapters/sqlite/`](../../../../../src/py/vertebra/db/adapters/sqlite/__init__.py)[^src-sqlite-driver-py] / [`vertebra-sqlite`](../../../../../src/rs/vertebra-sqlite/src/lib.rs)[^src-vertebra-sqlite-src-lib-rs] | `SQLiteConfig`, `SQLiteCore`, `_sync.py`/`_async.py`/`driver.py`, `pipeline.py` | Native `vertebra._sqlite` (bundled SQLite 3.45+, `rusqlite`), 1-writer/multi-reader WAL pool, autocommit `execute_tuples` outside caller transactions, `SyncSQLitePipeline` / `AsyncSQLitePipeline`, `VTBARROW`, online `backup_to` / `restore_from`, and `InterruptHandle`. |
| [`adapters/oracle/`](../../../../../src/py/vertebra/db/adapters/oracle/driver.py)[^src-oracle-driver-py] / [`vertebra-oracle`](../../../../../src/rs/vertebra-oracle/src/lib.rs)[^src-vertebra-oracle-src-lib-rs] | `OracleConfig`, `OracleCore`, `_sync.py`/`_async.py`/`driver.py`, `pipeline.py`, `SyncOraclePool`, `AsyncOraclePool` | Standalone `vertebra._oracle` (lazy `__getattr__` `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED`), `OracleBreakHandle` cancellation, `.zip`/`ewallet.pem` mTLS + IAM, `OracleConfig.enable_reset_relay=False` connecting directly with no relay thread or local socket (default `True`), DRCP (`cclass`/`purity`), TTC `[0x11, 0x6e]` piggyback, 26ai `VECTOR`/`OSON`, `arrow 59` -> `VTBARROW` `ArrowCursor`, delayed-commit [`pipeline.py`](../../../../../src/py/vertebra/db/adapters/oracle/pipeline.py)[^src-oracle-pipeline-py], and `SyncOracleNotificationSource` / `AsyncOracleNotificationSource`. |
| [`adapters/mssql/`](../../../../../src/py/vertebra/db/adapters/mssql/driver.py)[^src-mssql-driver-py] / [`vertebra-mssql`](../../../../../src/rs/vertebra-mssql/src/lib.rs) | `MSSQLConfig`, `MSSQLCore`, `_sync.py`/`_async.py`/`driver.py`, `pipeline.py` | Standalone `vertebra._mssql` (`tiberius` + `rustls`), TDS 7.4/8.0, Azure Entra `AuthMethod::aad_token` (`set_aad_token`), `sp_executesql` RPC, `BulkLoad`, `ATTENTION` cancel, `@P1` rewriter, `VTBARROW`, `CommonDriverMixin` broker leasing, pipelines, and Cloud SQL `:3307` mTLS. |
| [`adapters/duckdb/`](../../../../../src/py/vertebra/db/adapters/duckdb/driver.py)[^src-duckdb-driver-py] / [`vertebra-duckdb`](../../../../../src/rs/vertebra-duckdb/src/lib.rs)[^src-vertebra-duckdb-src-lib-rs] | `DuckDBConfig`, `DuckDBSecretConfig`, `DuckDBCore`, `_sync.py`/`_async.py`/`driver.py`, `pipeline.py` | Native `vertebra._duckdb` (bundled `duckdb`, `arrow 60`), single-writer / multi-reader pool, `TxJournal` savepoints, pipelines, `VTBARROW`, `Appender` bulk load, `InterruptHandle`, and `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` `CREATE SECRET` rotation with `(HTTP 401)`/`(HTTP 403)` invalidation. |
| [`adapters/redis/`](../../../../../src/py/vertebra/db/adapters/redis/driver.py)[^src-redis-driver-py] / [`vertebra-redis`](../../../../../src/rs/vertebra-redis/src/lib.rs) & [`valkey/`](../../../../../src/py/vertebra/db/adapters/valkey/driver.py)[^src-valkey-driver-py] / [`vertebra-valkey`](../../../../../src/rs/vertebra-valkey/src/lib.rs) | `RedisConfig`, `RedisCore`, `ValkeyConfig`, `ValkeyCore`, `_sync.py`/`_async.py`/`driver.py`, `notify.py`, shared [`_resp.py`](../../../../../src/py/vertebra/db/driver/_resp.py)[^src-db-driver-resp-py] | Standalone `vertebra._redis` and `vertebra._valkey` over shared `_resp.py` (`ClusterRedirect`, `PubSubMessage`, `NON_KEY_COMMANDS`, `crc16_xmodem`, `hash_slot`, `parse_cluster_redirect`, `Resp3Error(DatabaseError)`), `RESP2`/`RESP3` (`HELLO 3`), Azure Entra & Valkey IAM token auth, native `slot_map()` (`dict[int, tuple[str, int]]`), and `discover_sentinel_master` rebuilding clients via `_create_client` while preserving scheme, TLS, client tracking, and IAM token providers (chaining the last sentinel failure on exhaustion); reject `memory://`. |
| [`adapters/datafusion/`](../../../../../src/py/vertebra/db/adapters/datafusion/__init__.py) | `DataFusionConfig` (`mode="in_process"`), [`DataFusionCore`](../../../../../src/py/vertebra/db/adapters/datafusion/core.py)[^src-datafusion-core-py], [`DataFusionEngine`](../../../../../src/py/vertebra/db/adapters/datafusion/driver.py)[^src-datafusion-driver-py] | Embedded DataFusion `SessionContext` configured via `SessionConfig.with_target_partitions` and `RuntimeEnvBuilder.with_fair_spill_pool(memory_limit_mb)`, registering Parquet, CSV, NDJSON, `SyncObjectStore` Parquet (`register_parquet_from_store`), and `ArrowCursor` / PyArrow tables (`register_arrow_table`), with planner-backed `explain_plan`. |

## Shared Connector, Drainers, Missing Dependencies & Cloud Dialers

- **Transport & Drainers**: [`connection.rs`](../../../../../src/rs/vertebra-db/src/connection.rs)[^src-vertebra-db-src-connection-rs] normalizes TCP, `rustls`, Unix socket, Cloud SQL `:3307`, and AlloyDB `:5433` streams with `is_transient()` / SQLSTATE mapping. [`py_async.rs`](../../../../../src/rs/vertebra-db/src/py_async.rs)[^src-vertebra-db-src-py-async-rs] attaches per-extension `AsyncCompletionQueue` drainers (`_vertebra_{core,bigquery,mysql,mssql,oracle,spanner,duckdb,postgres,sqlite,redis}_completion_drainer`), converting panics to `TaskPanicError`, binding Python future cancellation to Tokio `AbortHandle` (`spawn_rust_db_task_with_attr_and_future` / `DbTaskCanceller`) or cancel callbacks (`spawn_rust_db_task_with_attr_and_cancel` / `CustomDbTaskCanceller`), and wrapping [`ShmRegion`](../../../../../src/rs/vertebra-core/src/shm.rs)[^src-vertebra-core-src-shm-rs] ownership in `c"vertebra.ShmCapsule"` via `construct_py_arrow_cursor`. [`arrow_shm.rs`](../../../../../src/rs/vertebra-db/src/arrow_shm.rs)[^src-vertebra-db-src-arrow-shm-rs] handles `VTBARROW` IPC framing and `allocate_shm_region`, while `ArrowCursor.release()`, `close()`, `__exit__()`, and `Drop` (`src/rs/vertebra-py/src/db.rs`) trigger `ShmRegion::unlink_now()`.
- **Missing Dependencies**: [`ensure_module`](../../../../../src/py/vertebra/utils/module_loader.py)[^src-module-loader] raises `MissingDependencyError` defaulting `install_package="vertebra-core[<extra>]"`, enforced by AST scanning in [`test_packaging_and_pgo.py`](../../../../../src/py/tests/unit/build/test_packaging_and_pgo.py)[^src-packaging-extras-gate].
- **Cloud Dialers (`_base.py`, `cloudsql.py`, `alloydb.py`, `aws.py`, `azure.py`, `dialer.rs`)**: `_BaseGCPDialer`, `EphemeralCertBundle`, and `VALID_GCP_IP_TYPES` (`dialer/_base.py`) share async cert refresh (`generateEphemeralCert` `:3307` / `generateClientCertificate` `:5433`), RSA keygen, verified TLS 1.3 mTLS, explicit IAM token propagation, and bounded jittered exponential retries (`connector_retry_count=3`, `connector_retry_delay=0.5`, `connector_retry_max_delay=5.0`, `is_transient_connector_error`, `calculate_connector_backoff_seconds`, `validate_connector_retry_config`) with `cloudsql.py`, `alloydb.py`, `PostgreSQLConfig`, `MySQLConfig`, `MSSQLConfig`, and pure-Rust dialers (`zero third-party driver packages`) [^src-dialer-base-py]. [`aws.py`](../../../../../src/py/vertebra/db/dialer/aws.py)[^src-dialer-aws-py] (`SyncAwsRdsDialer`, `AsyncAwsRdsDialer`, `SyncAwsAuroraDsqlDialer`, `AsyncAwsAuroraDsqlDialer`) and [`azure.py`](../../../../../src/py/vertebra/db/dialer/azure.py)[^src-dialer-azure-py] (`SyncAzureEntraDialer`, `AsyncAzureEntraDialer`) provide `verify-full` TLS contexts (defaulting AWS RDS to the bundled 114-cert `aws_rds_root_ca_pem()`), verified async TLS connections, and `connection_kwargs()`. All AWS and Azure dialers are lazily re-exported by `vertebra.db` and `vertebra.db.dialer`.

## Database Driver Conformance

Driver operational paths catch `Exception` (preserving `asyncio.CancelledError`, `KeyboardInterrupt`, and `SystemExit`), while transaction scopes roll back on `BaseException` (shielding async rollback/eviction via `anyio.CancelScope(shield=True)`). Cross-engine parity is enforced by [`test_driver_conformance.py`](../../../../../src/py/tests/integration/db/test_driver_conformance.py)[^src-tests-integration-db-conformance-py] and `test_{duckdb,mysql,mssql,oracle}_native.py` under zero-skip `ConformanceGate` (`VERTEBRA_CONFORMANCE_ENGINES`) and `ServiceVersionMatrix` (`VERTEBRA_CONFORMANCE_{MYSQL,MARIADB,MSSQL,ORACLE}_VERSION`) covering `duckdb`, `mysql` (`8.0`, `8.4`, `9.6`, `9.7`), `mariadb` (`11.3`, `11.4`, `11.8`, `12.2`, `12.3`, `13.0`), `mssql` (`2022`, `2025`), `oracle` (`23ai` / `26ai Free`), `sqlite`, `redis`, and `valkey`, alongside `postgres` (`14`–`18` under `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`), `bigquery`, and `spanner`. All ten native SQL, cloud, and cache engines execute over standalone pure-Rust PyO3 wheels.

[^src-vertebra-core-src-shm-rs]: `src/rs/vertebra-core/src/shm.rs` — ShmRegion and ShmCapsule shared-memory primitives.
[^src-vertebra-core-src-proto-rs]: `src/rs/vertebra-core/src/proto.rs` — Zero-dependency Protobuf v3 wire helpers.
[^src-vertebra-db-src-lib-rs]: `src/rs/vertebra-db/src/lib.rs` — Explicit vertebra-db Rust crate exports.
[^src-vertebra-db-src-arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs` — Shared-memory Arrow IPC writers and unregistered regions.
[^src-vertebra-db-src-connection-rs]: `src/rs/vertebra-db/src/connection.rs` — TransportStream, TlsMode, EndpointTarget, and Connection traits.
[^src-vertebra-db-src-py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Per-extension event-loop completion drainer and ShmCapsule bridge.
[^src-vertebra-db-src-dialer-rs]: `src/rs/vertebra-db/src/dialer.rs` — Zero-sidecar Cloud SQL, AlloyDB, AWS RDS, Aurora DSQL, and Azure Entra dialers plus AWS RDS Root CAs.
[^src-dialer-base-py]: `src/py/vertebra/db/dialer/_base.py` — Shared `_BaseGCPDialer`, `EphemeralCertBundle`, and `VALID_GCP_IP_TYPES` primitives.
[^src-dialer-aws-py]: `src/py/vertebra/db/dialer/aws.py` — Python `SyncAwsRdsDialer`, `AsyncAwsRdsDialer`, `SyncAwsAuroraDsqlDialer`, and `AsyncAwsAuroraDsqlDialer`.
[^src-dialer-azure-py]: `src/py/vertebra/db/dialer/azure.py` — Python `SyncAzureEntraDialer` and `AsyncAzureEntraDialer` TLS and token dialers.
[^src-vertebra-postgres-src-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Standalone `vertebra._postgres` extension exports.
[^src-postgres-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py` — PostgreSQL `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` re-exports.
[^src-vertebra-mysql-src-lib-rs]: `src/rs/vertebra-mysql/src/lib.rs` — Native MySQL and MariaDB binary wire extension crate.
[^src-vertebra-oracle-src-lib-rs]: `src/rs/vertebra-oracle/src/lib.rs` — Standalone `vertebra._oracle` `gil_used=false` PyO3 extension exports.
[^src-oracle-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py` — Python `SyncOracleDriver` and `AsyncOracleDriver` barrel over `_sync.py` and `_async.py`.
[^src-oracle-pipeline-py]: `src/py/vertebra/db/adapters/oracle/pipeline.py` — Delayed-commit `SyncOraclePipeline` and `AsyncOraclePipeline`.
[^src-bigquery-object-tables-py]: `src/py/vertebra/db/adapters/bigquery/object_tables.py` — BigQuery `ObjectRef` and `ObjectTableConfig` domain helpers.
[^src-spanner-mutations-py]: `src/py/vertebra/db/adapters/spanner/mutations.py` — Spanner `StalenessBound`, `Mutation`, and `MutationGroup` structs.
[^src-mssql-driver-py]: `src/py/vertebra/db/adapters/mssql/driver.py` — Python `SyncMSSQLDriver` and `AsyncMSSQLDriver` over `vertebra._mssql`.
[^src-vertebra-duckdb-src-lib-rs]: `src/rs/vertebra-duckdb/src/lib.rs` — Native DuckDB 2.0 PyO3 client, Arrow SHM writer, Appender, and savepoint journal.
[^src-duckdb-driver-py]: `src/py/vertebra/db/adapters/duckdb/driver.py` — `SyncDuckDBDriver` and `AsyncDuckDBDriver` native DuckDB 2.0 adapters.
[^src-vertebra-sqlite-src-lib-rs]: `src/rs/vertebra-sqlite/src/lib.rs` — Native SQLite PyO3 client, WAL worker pool, and Arrow SHM bridge.
[^src-sqlite-driver-py]: `src/py/vertebra/db/adapters/sqlite/driver.py` — `SyncSQLiteDriver` and `AsyncSQLiteDriver` barrel over `_sync.py` and `_async.py`.
[^src-redis-driver-py]: `src/py/vertebra/db/adapters/redis/driver.py` — `SyncRedisDriver` and `AsyncRedisDriver` barrel over `_sync.py` and `_async.py`.
[^src-valkey-driver-py]: `src/py/vertebra/db/adapters/valkey/driver.py` — `SyncValkeyDriver` and `AsyncValkeyDriver` barrel over `_sync.py` and `_async.py`.
[^src-datafusion-core-py]: `src/py/vertebra/db/adapters/datafusion/core.py` — `DataFusionCore` session builder, source registration, and explain helpers.
[^src-datafusion-driver-py]: `src/py/vertebra/db/adapters/datafusion/driver.py` — `DataFusionEngine` federated Arrow session context.
[^src-vertebra-py-src-errors-rs]: `src/rs/vertebra-py/src/errors.rs` — Canonical `DatabaseError` and multi-base cloud exception hierarchy.
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Lazy engine-agnostic `vertebra.db` public exports.
[^src-db-registry-py]: `src/py/vertebra/db/registry.py` — Declarative `DatabaseRegistry` URL parsing and pool deduplication.
[^src-db-driver-init-py]: `src/py/vertebra/db/driver/__init__.py` — `SyncDatabaseDriver` and `AsyncDatabaseDriver` base hierarchy.
[^src-db-driver-common-py]: `src/py/vertebra/db/driver/_common.py` — `CommonDriverMixin` shared checkout broker lease, trace annotation, and row hydration helpers.
[^src-db-driver-resp-py]: `src/py/vertebra/db/driver/_resp.py` — Shared RESP3 `ClusterRedirect`, `PubSubMessage`, CRC16-XMODEM, and `Resp3Error` helpers.
[^src-db-driver-cloud-auth-py]: `src/py/vertebra/db/driver/_cloud_auth.py` — `AwsRdsIamTokenProvider`, `AwsAuroraDsqlIamTokenProvider`, and `AzureEntraTokenProvider`.
[^src-db-driver-capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — Runtime-checkable driver capability protocols.
[^src-tests-integration-db-conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py` — Cross-driver acceptance and conformance suite.
[^src-module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Dynamic optional dependency loader.
[^src-packaging-extras-gate]: `src/py/tests/unit/build/test_packaging_and_pgo.py` — Packaging extras gate verifying `ensure_module` extra literals.
