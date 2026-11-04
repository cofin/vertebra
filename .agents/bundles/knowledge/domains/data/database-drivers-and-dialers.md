---
type: Guide
title: "Database Drivers & Cloud Dialers"
description: "The `vertebra-db` and `vertebra.db` Rust-native shared-memory drivers across PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB, BigQuery, Spanner, Redis, and Valkey, the `vertebra.db.driver` base hierarchy, `ArrowCursor`, `DatabaseRegistry`, and Cloud SQL / AlloyDB dialers."
status: stable
tags: [domains, database, drivers, bigquery, spanner, cloud-sql, alloydb, arrow, shm]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-core-src-shm-rs
    resource: src/rs/vertebra-core/src/shm.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/shm.rs)"
  - id: src-vertebra-db-src-lib-rs
    resource: src/rs/vertebra-db/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/lib.rs)"
  - id: src-vertebra-db-src-arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/arrow_shm.rs)"
  - id: src-vertebra-db-src-connection-rs
    resource: src/rs/vertebra-db/src/connection.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/connection.rs)"
  - id: src-vertebra-db-src-py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/py_async.rs)"
  - id: src-vertebra-db-src-postgres-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/postgres/mod.rs)"
  - id: src-vertebra-db-src-mysql-rs
    resource: src/rs/vertebra-db/src/mysql/mod.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/mysql/mod.rs)"
  - id: src-vertebra-db-src-sqlite-rs
    resource: src/rs/vertebra-db/src/sqlite/mod.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/sqlite/mod.rs)"
  - id: src-vertebra-db-src-oracle-rs
    resource: src/rs/vertebra-db/src/oracle/mod.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/oracle/mod.rs)"
  - id: src-vertebra-db-src-tns-rs
    resource: src/rs/vertebra-db/src/oracle/tns.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/oracle/tns.rs)"
  - id: src-vertebra-db-src-mssql-rs
    resource: src/rs/vertebra-db/src/mssql/mod.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/mssql/mod.rs)"
  - id: src-vertebra-db-src-duckdb-rs
    resource: src/rs/vertebra-db/src/duckdb/mod.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/duckdb/mod.rs)"
  - id: src-vertebra-db-src-gcp-rs
    resource: src/rs/vertebra-db/src/gcp.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/gcp.rs)"
  - id: src-vertebra-db-src-bigquery-rs
    resource: src/rs/vertebra-db/src/bigquery/mod.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/bigquery/mod.rs)"
  - id: src-vertebra-db-src-spanner-rs
    resource: src/rs/vertebra-db/src/spanner/mod.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/spanner/mod.rs)"
  - id: src-vertebra-db-src-resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/resp3/mod.rs)"
  - id: src-vertebra-db-src-dialer-rs
    resource: src/rs/vertebra-db/src/dialer.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/dialer.rs)"
  - id: src-vertebra-py-src-completion-rs
    resource: src/rs/vertebra-py/src/completion.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/completion.rs)"
  - id: src-vertebra-py-src-db-rs
    resource: src/rs/vertebra-py/src/db.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/db.rs)"
  - id: src-vertebra-py-src-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/errors.rs)"
  - id: src-vertebra-db-tests-gcp-credentials-tests-rs
    resource: src/rs/vertebra-db/tests/gcp_credentials_tests.rs
    title: "Rust implementation module (src/rs/vertebra-db/tests/gcp_credentials_tests.rs)"
  - id: src-vertebra-db-tests-spanner-wire-tests-rs
    resource: src/rs/vertebra-db/tests/spanner_wire_tests.rs
    title: "Rust implementation module (src/rs/vertebra-db/tests/spanner_wire_tests.rs)"
  - id: src-vertebra-db-tests-bigquery-wire-tests-rs
    resource: src/rs/vertebra-db/tests/bigquery_wire_tests.rs
    title: "Rust implementation module (src/rs/vertebra-db/tests/bigquery_wire_tests.rs)"
  - id: src-db-init-py
    resource: src/py/vertebra/db/__init__.py
    title: "Python implementation module (src/py/vertebra/db/__init__.py)"
  - id: src-db-arrow-py
    resource: src/py/vertebra/db/arrow.py
    title: "Python implementation module (src/py/vertebra/db/arrow.py)"
  - id: src-db-registry-py
    resource: src/py/vertebra/db/registry.py
    title: "Python implementation module (src/py/vertebra/db/registry.py)"
  - id: src-db-driver-init-py
    resource: src/py/vertebra/db/driver/__init__.py
    title: "Python implementation module (src/py/vertebra/db/driver/__init__.py)"
  - id: src-db-driver-common-py
    resource: src/py/vertebra/db/driver/_common.py
    title: "Python implementation module (src/py/vertebra/db/driver/_common.py)"
  - id: src-db-driver-sync-py
    resource: src/py/vertebra/db/driver/_sync.py
    title: "Python implementation module (src/py/vertebra/db/driver/_sync.py)"
  - id: src-db-driver-async-py
    resource: src/py/vertebra/db/driver/_async.py
    title: "Python implementation module (src/py/vertebra/db/driver/_async.py)"
  - id: src-db-postgres-init-py
    resource: src/py/vertebra/db/adapters/postgres/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/postgres/__init__.py)"
  - id: src-db-mysql-init-py
    resource: src/py/vertebra/db/adapters/mysql/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/mysql/__init__.py)"
  - id: src-db-sqlite-init-py
    resource: src/py/vertebra/db/adapters/sqlite/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/sqlite/__init__.py)"
  - id: src-db-oracle-init-py
    resource: src/py/vertebra/db/adapters/oracle/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/oracle/__init__.py)"
  - id: src-db-mssql-init-py
    resource: src/py/vertebra/db/adapters/mssql/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/mssql/__init__.py)"
  - id: src-db-duckdb-init-py
    resource: src/py/vertebra/db/adapters/duckdb/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/duckdb/__init__.py)"
  - id: src-db-bigquery-init-py
    resource: src/py/vertebra/db/adapters/bigquery/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/bigquery/__init__.py)"
  - id: src-db-spanner-init-py
    resource: src/py/vertebra/db/adapters/spanner/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/spanner/__init__.py)"
  - id: src-db-redis-init-py
    resource: src/py/vertebra/db/adapters/redis/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/redis/__init__.py)"
  - id: src-db-valkey-init-py
    resource: src/py/vertebra/db/adapters/valkey/__init__.py
    title: "Python implementation module (src/py/vertebra/db/adapters/valkey/__init__.py)"
  - id: src-db-dialer-cloudsql-py
    resource: src/py/vertebra/db/dialer/cloudsql.py
    title: "Python implementation module (src/py/vertebra/db/dialer/cloudsql.py)"
  - id: src-db-dialer-alloydb-py
    resource: src/py/vertebra/db/dialer/alloydb.py
    title: "Python implementation module (src/py/vertebra/db/dialer/alloydb.py)"
  - id: src-tests-unit-db-test-native-db-drivers-py
    resource: src/py/tests/unit/db/test_native_db_drivers.py
    title: "Python implementation module (src/py/tests/unit/db/test_native_db_drivers.py)"
  - id: src-tests-unit-db-test-enterprise-olap-drivers-py
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py
    title: "Python implementation module (src/py/tests/unit/db/test_enterprise_olap_drivers.py)"
  - id: src-tests-unit-db-test-redis-valkey-resp3-py
    resource: src/py/tests/unit/db/test_redis_valkey_resp3.py
    title: "Python implementation module (src/py/tests/unit/db/test_redis_valkey_resp3.py)"
  - id: src-tests-unit-db-test-bigquery-py
    resource: src/py/tests/unit/db/test_bigquery.py
    title: "Python implementation module (src/py/tests/unit/db/test_bigquery.py)"
  - id: src-tests-unit-db-test-spanner-py
    resource: src/py/tests/unit/db/test_spanner.py
    title: "Python implementation module (src/py/tests/unit/db/test_spanner.py)"
---

# Database Drivers & Cloud Dialers

<!-- truth: start -->
- **Structured Per-Adapter Package Layout (`src/py/vertebra/db/adapters/<engine>/{config,core,driver,__init__}.py` & `src/py/vertebra/db/driver/`)**: Every built-in database, cache, and federated query adapter under `vertebra.db.adapters` (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`, `datafusion`) is organized as a package under `src/py/vertebra/db/adapters/<engine>/`, while `vertebra.db` lazily exports only engine-agnostic base contracts, `DatabaseError` subclasses, dialers, `ArrowCursor`, `DatabaseRegistry`, CDC, and RLS primitives.
- **Shared Connector & Lifecycle Traits (`vertebra-db/src/connection.rs`, `gcp.rs`)**: `connection.rs` defines `install_rustls_ring_provider()` (re-exported by `gcp.rs`), `TlsMode` (`Disable`, `Prefer`, `Require`, `VerifyCa`, `VerifyFull`), `ConnectOptions`, `TransportError` (`is_transient`, `sqlstate`), `EndpointTarget` (`Tcp`, `Unix`, `CloudSql`, `AlloyDb`), `TransportStream` (`AsyncRead + AsyncWrite + Unpin` over TCP, `tokio-rustls` with `webpki-root-certs` + `rustls-pemfile`, Unix sockets, Cloud SQL `:3307` `project:instance` CN verification, and AlloyDB `:5433` mTLS + `exchange_alloydb_metadata_over_stream`), `CancelHandle`, and `Connection` (`ping`, `is_broken`, `cancel_handle`, `close`). `vertebra-db/src/lib.rs` exports explicit symbol lists without wildcard `pub use *` globs.
- **Shared PyO3 Async Drainer & Cross-`.so` `ShmCapsule` (`vertebra-db/src/py_async.rs`, `vertebra-core/src/shm.rs`, `vertebra-db/src/arrow_shm.rs`, `vertebra-py/src/{completion,db}.rs`)**: `spawn_rust_db_task_with_attr(..., attr_name)` keys each extension's event-loop completion drainer by `attr_name` (`_vertebra_core_completion_drainer` in `_vertebra`), dynamically resolves `TaskPanicError` from `vertebra._vertebra` (falling back to `PyRuntimeError`), and wires Python future cancellation through `AbortHandle`. `ShmCapsule` (`#[repr(C)] { ptr, len, ctx, drop_fn }`) and `write_shm_arrow_region_unregistered` / `ShmArrowStreamWriter::finish_unregistered` transfer `Arc<ShmRegion>` ownership across `.so` boundaries via `PyCapsule` (`c"vertebra.ShmCapsule"`) into `ArrowCursor._from_shm_capsule` (`construct_py_arrow_cursor`), while `ShmRegion::unlink_now` unlinks `/dev/shm` immediately on `ArrowCursor.release()` without invalidating live mappings.
- **Unified `DatabaseError` & Multi-Base Google Cloud Exception Hierarchy (`vertebra-py/src/errors.rs`, `vertebra.db`, `vertebra.db.driver`)**: `vertebra_py::errors` registers `DatabaseError(VertebraError)` and 12 subclasses (`InterfaceError`, `DataError`, `NotFoundError(DataError, ValueError)`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`, `QueryCancelledError`, `SerializationFailure`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`) with class-level `sqlstate`, `vendor_code`, and `is_transient` defaults, and constructs multi-base `SpannerError(GoogleCloudError, DatabaseError)`, `SpannerAbortedError(SpannerError, SerializationFailure)`, `BigQueryError(GoogleCloudError, DatabaseError)`, and `BigQueryRowError(BigQueryError, DataError)` via safe `PyType` calls without `unsafe`.
- **Binary Vector Wire Codecs (`postgres/mod.rs`, `oracle/mod.rs`, `vertebra-py/src/db.rs`)**: PostgreSQL (`postgres/mod.rs`) encodes and decodes binary `pgvector` (`vector` `f32`, `halfvec` `f16`, and `bit` bit-packed) frames without text formatting, while Oracle 26ai (`oracle/mod.rs`, `vertebra-py/src/db.rs`) encodes and decodes binary `0xDB` `VECTOR` frames directly over PEP 3118 `PyBuffer` views (`encode_oracle_vector_binary`, `decode_oracle_vector_binary`).
- **Per-Session DuckDB `httpfs` Secret Rotation (`adapters/duckdb/`, `duckdb/mod.rs`)**: Each pooled DuckDB connection tracks `applied_token_generation: u64` against the shared [`SyncGcpTokenCache` / `AsyncGcpTokenCache`](../../../../../src/rs/vertebra-core/src/gcp_auth.rs) and executes `CREATE OR REPLACE SECRET vtb_gcs_adc (TYPE GCS, BEARER_TOKEN '...')` whenever the cached ADC token generation advances.
- **Official Google Cloud Rust Clients & Auth Resilience (`gcp.rs`, `spanner/mod.rs`, `bigquery/mod.rs`, `vertebra-core/src/gcp_auth.rs`)**: `SpannerClient` and `BigQueryClient` delegate gRPC Data, Database Admin, Storage Read, and Storage Write APIs to `google-cloud-spanner` (`=0.34.8-preview`) and `google-cloud-bigquery` (`=0.19.0`) bridged from `SyncGcpTokenCache` / `AsyncGcpTokenCache` via `vertebra_db::gcp::{GcpTokenCredentials, official_credentials}`. Spanner DDL executes over REST (`DatabaseAdmin`) with `admin_endpoint` resolution and LRO polling; `SpannerConfig.ca_pem` configures Spanner Omni TLS for non-`*.googleapis.com` HTTPS endpoints, while `BigQueryConfig.ca_pem` applies to REST v2 calls. `ArrowAppendPlan::build` splits Arrow IPC streams into one record batch per `ArrowAppendRequest` and verifies response offsets. `GcpTokenCache::from_options(raw_opts, default_scopes)` requires explicit default OAuth scopes, honors `ProxyPolicy` on token/IAM calls while keeping metadata-server calls direct, and runs `GcpAuthSource::GcloudCli` with `kill_on_drop(true)` and `GCLOUD_CLI_TIMEOUT` (`30s`).
<!-- truth: end -->

[`src/rs/vertebra-db`](../../../../../src/rs/vertebra-db/src/lib.rs)[^src-vertebra-db-src-lib-rs] and [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py)[^src-db-init-py] implement Vertebra's Tier-1 shared-memory database and cache drivers, the [`vertebra.db.driver`](../../../../../src/py/vertebra/db/driver/__init__.py)[^src-db-driver-init-py] base hierarchy (`BaseDatabaseConfig`, `CommonDriverMixin`, `SyncDatabaseDriver`, `AsyncDatabaseDriver`, `ThreadOffloadAsyncDatabaseDriver`), zero-copy `ShmRegion` Arrow IPC cursors, and Cloud SQL / AlloyDB mTLS and IAM token dialers. Query composition, CDC, and tenancy in the same package are covered in [query-pipeline-cdc-and-tenancy](query-pipeline-cdc-and-tenancy.md).

## 1. Registry (`registry.py`)

[`DatabaseRegistry`](../../../../../src/py/vertebra/db/registry.py)[^src-db-registry-py] maps names to configs and lazily built drivers.

- `parse_url(url, **overrides)` dispatches on the scheme prefix before `+`: `postgres`/`postgresql` → `PostgreSQLConfig`, `mysql`/`mariadb` → `MySQLConfig`, `sqlite` → `SQLiteConfig` (`:memory:` for an empty path), `oracle` → `OracleConfig`, `mssql`/`sqlserver` → `MSSQLConfig`, `duckdb` → `DuckDBConfig`, `bigquery` → `BigQueryConfig`, `spanner`/`spangres` → `SpannerConfig` (via `SpannerConfig.from_dsn`; `spanner+postgresql` raises `ImproperConfigurationError`), `redis`/`rediss` → `RedisConfig`, `valkey`/`valkeys` → `ValkeyConfig`, `cloudsql` → `CloudSQLConfig`, `alloydb` → `AlloyDBConfig`.
- `register(name, config_or_url)` stores a config and drops any cached driver. `get_sync_driver(name)` builds the `Sync*Driver` or `Sync*Dialer` and `get_async_driver(name)` builds the `Async*Driver` or `Async*Dialer` that matches the config type.
- [`vertebra.extensions.sqlalchemy.dialects`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) integrates the relational and analytical drivers into SQLAlchemy 2.1 via the `+vtb` dialect suffix (`{postgresql,oracle,mysql,sqlite,mssql,duckdb,bigquery,spanner,spangres}+{vertebra,vtb,vertebra_async,vtb_async}`).

## 2. Driver Implementations (`vertebra-db` & `vertebra.db.adapters`)

| Package / Module | Config & Core | Driver behavior |
|---|---|---|
| [`adapters/postgres/`](../../../../../src/py/vertebra/db/adapters/postgres/__init__.py)[^src-db-postgres-init-py] / [`postgres/mod.rs`](../../../../../src/rs/vertebra-db/src/postgres/mod.rs)[^src-vertebra-db-src-postgres-rs] | `PostgreSQLConfig`, `PostgreSQLCore`, `PoolerMode = Literal["direct", "transaction", "session"]` | Native v3 wire framing (`StartupMessage`, `SASLInitialResponse` / `SASLResponse` SCRAM-SHA-256, `Parse`/`Bind`/`Describe`/`Execute`/`Sync`, binary `DataRow`, `CopyOutResponse`). `PostgreSQLCore.strip_jsonb_header` validates the `0x01` JSONB version byte and returns a zero-copy `memoryview` slice. Supports `execute_arrow_shm()` into `ArrowCursor` and direct `:5432` IAM OAuth2 token password authentication (`enable_iam_auth=True`). |
| [`adapters/mysql/`](../../../../../src/py/vertebra/db/adapters/mysql/__init__.py)[^src-db-mysql-init-py] / [`mysql/mod.rs`](../../../../../src/rs/vertebra-db/src/mysql/mod.rs)[^src-vertebra-db-src-mysql-rs] | `MySQLConfig`, `MySQLCore` | Handshake v10 packet parsing, `caching_sha2_password` / `mysql_native_password` / `mysql_clear_password` auth, `COM_QUERY` and `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` dispatch, length-encoded integer and binary result row decoding, `execute_arrow_shm()`, and direct `:3306` IAM OAuth2 token password authentication (`enable_iam_auth=True`). |
| [`adapters/sqlite/`](../../../../../src/py/vertebra/db/adapters/sqlite/__init__.py)[^src-db-sqlite-init-py] / [`sqlite/mod.rs`](../../../../../src/rs/vertebra-db/src/sqlite/mod.rs)[^src-vertebra-db-src-sqlite-rs] | `SQLiteConfig`, `SQLiteCore` | Opens WAL-mode connections (`journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout`, `mmap_size`, `temp_store=MEMORY`), serializes writes through `LeaseBroker` slots when `broker` is attached, and streams `execute_arrow_shm()` via `ArrowCursor`. |
| [`adapters/oracle/`](../../../../../src/py/vertebra/db/adapters/oracle/__init__.py)[^src-db-oracle-init-py] / [`oracle/mod.rs`](../../../../../src/rs/vertebra-db/src/oracle/mod.rs)[^src-vertebra-db-src-oracle-rs] | `OracleConfig`, `OracleCore` | Pure-Rust Thin TNS/TTC framing (zero ODPI-C / Instant Client dependency), Autonomous Database mTLS `ewallet.pem` + `tnsnames.ora` zip extraction (`oracle/tns.rs`), `O5LOGON` verifier parsing, and native Oracle 26ai `VECTOR` (`FLOAT32`/`FLOAT64`/`INT8`/`BINARY`) and `OSON` binary JSON codecs (`OracleCore.encode_vector_binary`, `OracleCore.decode_vector_binary`, `OracleCore.encode_oson_frame`, `OracleCore.decode_oson_frame`). |
| [`adapters/mssql/`](../../../../../src/py/vertebra/db/adapters/mssql/__init__.py)[^src-db-mssql-init-py] / [`mssql/mod.rs`](../../../../../src/rs/vertebra-db/src/mssql/mod.rs)[^src-vertebra-db-src-mssql-rs] | `MSSQLConfig`, `MSSQLCore` | Pure-Rust TDS 7.4/8.0 wire protocol (`PRELOGIN` `0x12`, `LOGIN7` `0x10`, `SQL_BATCH` `0x01`, `RPC_REQUEST` `0x03` `sp_executesql`), automatic positional (`?`/`$1`) and named (`:name`) parameter rewriting to `@P1..@Pn` via `MSSQLCore.rewrite_sql_to_tds_params`, and Cloud SQL for SQL Server `:3307` TLS routing. |
| [`adapters/duckdb/`](../../../../../src/py/vertebra/db/adapters/duckdb/__init__.py)[^src-db-duckdb-init-py] / [`duckdb/mod.rs`](../../../../../src/rs/vertebra-db/src/duckdb/mod.rs)[^src-vertebra-db-src-duckdb-rs] | `DuckDBConfig`, `DuckDBCore` | Single-writer / multi-reader SHM analytical architecture preventing `IO Error: Could not set lock on file` across multi-worker forks, DuckDB 2.0-safe startup pragmas (`threads`, `memory_limit`, `temp_directory`, `preserve_insertion_order`), and zero-copy Arrow `RecordBatch` streaming over `ShmRegion` (`execute_arrow_shm()`). |
| [`adapters/bigquery/`](../../../../../src/py/vertebra/db/adapters/bigquery/__init__.py)[^src-db-bigquery-init-py] / [`bigquery/mod.rs`](../../../../../src/rs/vertebra-db/src/bigquery/mod.rs)[^src-vertebra-db-src-bigquery-rs] | `BigQueryConfig`, `BigQueryCore` | BigQuery REST v2 (`jobs.query`, `jobs.getQueryResults`, `jobs.insert` dry-run; optional `ca_pem` custom CA for REST; `check_rest_error` fails only on `error` or `status.errorResult` so `errors[]` warnings succeed; `build_get_query_results_url` percent-encodes path segments and query pairs with `url::Url`) + official `google-cloud-bigquery` (`client::Read`, `client::Write`) Storage Read/Write gRPC APIs (`CreateReadSession`, `ReadRows`, `AppendRows`) streaming into `VTBARROW` shared memory (`ArrowCursor`) and sending one record batch per `AppendRowsRequest` (`ArrowAppendPlan::build`, rejecting dictionary batches and verifying response count and offsets), with automatic fallback to `jobs.query` via `BigQueryCore.is_storage_api_fallback_error` (`BigQueryError.code in {7, 9, 12, 14}`). |
| [`adapters/spanner/`](../../../../../src/py/vertebra/db/adapters/spanner/__init__.py)[^src-db-spanner-init-py] / [`spanner/mod.rs`](../../../../../src/rs/vertebra-db/src/spanner/mod.rs)[^src-vertebra-db-src-spanner-rs] | `SpannerConfig`, `SpannerCore` | Official `google-cloud-spanner` (`client::Spanner`, `client::DatabaseAdmin`) across GoogleSQL and Spangres dialects (`CreateSession` multiplexed session, `ExecuteStreamingSql`, `ExecuteBatchDml`, `Commit` with inline `BeginTransaction`, `MultiplexedSessionPrecommitToken` tracking, `BatchWrite`, `PartitionQuery` + `partitioned_query` Data Boost rows hydrated into `VTBARROW` `ArrowCursor` by `fetch_arrow` with `SpannerCore.is_partitioned_query_fallback_error` checking `SpannerError.code in {3, 7, 9, 12, 14}`, `run_in_transaction` retrying `SpannerAbortedError`, char-aware GoogleSQL placeholder rewriting in `rewrite_spangres_placeholders`, and REST `update_database_ddl` with `admin_endpoint` resolution and `ca_pem` Omni TLS for non-`*.googleapis.com` HTTPS endpoints). |
| [`adapters/redis/`](../../../../../src/py/vertebra/db/adapters/redis/__init__.py)[^src-db-redis-init-py] / [`resp3/mod.rs`](../../../../../src/rs/vertebra-db/src/resp3/mod.rs)[^src-vertebra-db-src-resp3-rs] | `RedisConfig`, `RedisCore` | Rust `RESP2`/`RESP3` parser (`+`, `-`, `:`, `$`, `*`, `_`, `,`, `#`, `!`, `=`, `%`, `~`, `>`, `\|`), `HELLO 3` negotiation, `CLIENT TRACKING ON ... BCAST` invalidation push handling with bounded LRU cache, Sentinel master discovery, Cluster CRC16 slot routing (`MOVED`/`ASK`), and multiplexed Pub/Sub fan-out. |
| [`adapters/valkey/`](../../../../../src/py/vertebra/db/adapters/valkey/__init__.py)[^src-db-valkey-init-py] | `ValkeyConfig`, `ValkeyCore` | Full Redis RESP3 compatibility plus Valkey 8.0+/9.0+ client tracking, Cluster slot routing, and GCP Memorystore IAM token authentication (`needs_iam_refresh` / `authenticate_iam`). |

`PostgreSQLConfig` rules:

- `__post_init__` rejects invalid `min_size`/`max_size`.
- `effective_statement_cache_size` is `0` in `transaction` pooler mode unless `protocol_level_pooler_cache` is set. Otherwise it is `max_prepared_statements` or 256.
- `effective_direct_dsn` falls back to `dsn`. Use it for `LISTEN`/`NOTIFY`, replication, and migrations, which cannot run through a transaction pooler.
- `compute_worker_pool_budget` (static) divides `max_connections` minus reserved connections across replicas × workers, with a minimum of 1.
- `build_session_gucs` sets `application_name`, the timeouts, and `alloydb.enable_columnar_engine=on` when an AlloyDB URI or the columnar flag is set.
- `to_cloud_dialer_config` returns an `AlloyDBConfig` or `CloudSQLConfig` when an instance is configured. `MySQLConfig.to_cloud_dialer_config` and `MSSQLConfig.to_cloud_dialer_config` do the same for Cloud SQL (`:3307`).

## 3. Shared-Memory Arrow Cursor & `ShmCapsule` (`arrow.py`, `shm.rs`, `arrow_shm.rs`, `db.rs`)

[`ArrowCursor`](../../../../../src/py/vertebra/db/arrow.py)[^src-db-arrow-py] wraps a [`ShmRegion`](../../../../../src/rs/vertebra-core/src/shm.rs)[^src-vertebra-core-src-shm-rs] containing an Apache Arrow IPC stream written by `vertebra-db` ([`arrow_shm.rs`](../../../../../src/rs/vertebra-db/src/arrow_shm.rs)[^src-vertebra-db-src-arrow-shm-rs]) or an optional native driver extension:

- Exposes `memoryview` over the shared-memory buffer (`buffer_view()`), zero-copy `pyarrow.Table` / `pyarrow.RecordBatchReader` / `polars.DataFrame` materialization (`to_arrow()`, `to_polars()`), and fallback row-dict/tuple iteration (`fetchall()`, `fetchmany()`).
- Supports cross-extension zero-copy handoff via `#[repr(C)] ShmCapsule { ptr, len, ctx, drop_fn }`, `write_shm_arrow_region_unregistered` / `ShmArrowStreamWriter::finish_unregistered` (which omit `ACTIVE_ARROW_SHM_REGIONS` registration so optional `.so` crates never retain leaked regions), and [`ArrowCursor._from_shm_capsule`](../../../../../src/rs/vertebra-py/src/db.rs)[^src-vertebra-py-src-db-rs].
- Reclaims and unlinks (`ShmRegion::unlink_now`) the backing `/dev/shm` entry immediately on `release()`, `close()`, or context-manager exit (`with driver.execute_arrow_shm(...) as cursor:`) while keeping the `mmap` valid until `ArrowCursor` drops.

## 4. Shared Connector, PyO3 Async Drainer & `DatabaseError` Hierarchy (`connection.rs`, `py_async.rs`, `errors.rs`)

- **Shared Transport & Connection Traits ([`connection.rs`](../../../../../src/rs/vertebra-db/src/connection.rs)[^src-vertebra-db-src-connection-rs])**: `TransportStream` unifies plaintext TCP (`TCP_NODELAY`), `tokio-rustls` TLS (`TlsMode::{Disable, Prefer, Require, VerifyCa, VerifyFull}` over `webpki-root-certs` + `rustls-pemfile`), Unix domain sockets (`EndpointTarget::Unix`), Cloud SQL `:3307` mTLS (`EndpointTarget::CloudSql` verifying `project:instance` server CN), and AlloyDB `:5433` mTLS + `MetadataExchangeRequest` (`EndpointTarget::AlloyDb` paired with `exchange_alloydb_metadata_over_stream`). `TransportError` exposes `is_transient()` and canonical SQLSTATEs (`08001`, `08006`, `57014`, `28000`), while `CancelHandle` and `Connection` (`ping`, `is_broken`, `cancel_handle`, `close`) standardize pooled driver lifecycles.
- **Per-Extension Event-Loop Drainer ([`py_async.rs`](../../../../../src/rs/vertebra-db/src/py_async.rs)[^src-vertebra-db-src-py-async-rs])**: `spawn_rust_db_task_with_attr(py, attr_name, work, map_result)` caches each extension's completion drainer on the active `asyncio` event loop under `attr_name` (delegated by [`vertebra-py/src/completion.rs`](../../../../../src/rs/vertebra-py/src/completion.rs)[^src-vertebra-py-src-completion-rs] with `_vertebra_core_completion_drainer`), dynamically resolves `TaskPanicError` from `vertebra._vertebra` with `PyRuntimeError` fallback, and wires Python `Future.cancel()` to Tokio `AbortHandle`. `construct_py_arrow_cursor` wraps `Arc<ShmRegion>` in a `c"vertebra.ShmCapsule"` `PyCapsule` and invokes `ArrowCursor._from_shm_capsule`.
- **Canonical `DatabaseError` Hierarchy ([`errors.rs`](../../../../../src/rs/vertebra-py/src/errors.rs)[^src-vertebra-py-src-errors-rs])**: Registers `DatabaseError(VertebraError)` and `InterfaceError`, `DataError`, `NotFoundError(DataError, ValueError)`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`, `QueryCancelledError`, `SerializationFailure`, `IntegrityError`, `InternalError`, `ProgrammingError`, and `NotSupportedError` (re-exported from `vertebra.db` and `vertebra.db.driver`) with class-level `sqlstate`, `vendor_code`, and `is_transient` defaults, plus safe multi-base `SpannerError(GoogleCloudError, DatabaseError)`, `SpannerAbortedError(SpannerError, SerializationFailure)`, `BigQueryError(GoogleCloudError, DatabaseError)`, and `BigQueryRowError(BigQueryError, DataError)`.

## 5. Cloud Dialers (`dialer/` & `vertebra-db/src/dialer.rs`)

| Symbol | Responsibility |
|---|---|
| [`CloudSQLInstanceURI`](../../../../../src/py/vertebra/db/dialer/cloudsql.py)[^src-db-dialer-cloudsql-py] | Parses `project:region:instance`, including domain-scoped `domain:project:region:instance`. `expected_server_cn` is `project:instance`. `generate_ephemeral_cert_url` builds the SQL Admin `v1beta4 ...:generateEphemeralCert` URL. |
| `CloudSQLConfig` / `SyncCloudSQLDialer` & `AsyncCloudSQLDialer` | Port 3307. `needs_refresh` / `cache_iam_token` track IAM token expiry against `refresh_lead_seconds` (default 240). `build_tls_context` returns a TLS 1.3 context with `check_hostname=False`, because the server CN is not a hostname. Rust `vertebra_db::dialer::CloudSQLDialer` provides native `rustls` custom `ServerCertVerifier` matching `project:instance`. |
| [`AlloyDBClusterURI`](../../../../../src/py/vertebra/db/dialer/alloydb.py)[^src-db-dialer-alloydb-py] | Parses `projects/<p>/locations/<l>/clusters/<c>/instances/<i>`. `generate_client_cert_url` builds the `v1beta ...:generateClientCertificate` URL. |
| `AlloyDBConfig` / `SyncAlloyDBDialer` & `AsyncAlloyDBDialer` / `AlloyDBAuthType` | Port 5433. `encode_metadata_exchange` (and Rust `vertebra_db::dialer::AlloyDBDialer::encode_metadata_exchange`) writes a 4-byte big-endian length followed by protobuf-style fields: user agent (tag `0x0A`), auth type (`0x10`, `BUILT_IN=1` / `IAM=2`), token (`0x1A`). `build_handshake_frame` picks IAM or built-in auth from `enable_iam_auth`. |

Provenance sources: [^src-vertebra-core-src-shm-rs], [^src-vertebra-db-src-arrow-shm-rs], [^src-vertebra-db-src-connection-rs], [^src-vertebra-db-src-py-async-rs], [^src-vertebra-db-src-tns-rs], [^src-vertebra-db-src-gcp-rs], [^src-vertebra-db-src-dialer-rs], [^src-vertebra-py-src-completion-rs], [^src-vertebra-py-src-db-rs], [^src-vertebra-py-src-errors-rs], [^src-vertebra-db-tests-gcp-credentials-tests-rs], [^src-vertebra-db-tests-spanner-wire-tests-rs], [^src-vertebra-db-tests-bigquery-wire-tests-rs], [^src-db-driver-common-py], [^src-db-driver-sync-py], [^src-db-driver-async-py], [^src-tests-unit-db-test-native-db-drivers-py], [^src-tests-unit-db-test-enterprise-olap-drivers-py], [^src-tests-unit-db-test-redis-valkey-resp3-py], [^src-tests-unit-db-test-bigquery-py], [^src-tests-unit-db-test-spanner-py].

[^src-vertebra-core-src-shm-rs]: `src/rs/vertebra-core/src/shm.rs` — Rust implementation module (src/rs/vertebra-core/src/shm.rs).
[^src-vertebra-db-src-lib-rs]: `src/rs/vertebra-db/src/lib.rs` — Rust implementation module (src/rs/vertebra-db/src/lib.rs).
[^src-vertebra-db-src-arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs` — Rust implementation module (src/rs/vertebra-db/src/arrow_shm.rs).
[^src-vertebra-db-src-connection-rs]: `src/rs/vertebra-db/src/connection.rs` — Rust implementation module (src/rs/vertebra-db/src/connection.rs).
[^src-vertebra-db-src-py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Rust implementation module (src/rs/vertebra-db/src/py_async.rs).
[^src-vertebra-db-src-postgres-rs]: `src/rs/vertebra-db/src/postgres/mod.rs` — Rust implementation module (src/rs/vertebra-db/src/postgres/mod.rs).
[^src-vertebra-db-src-mysql-rs]: `src/rs/vertebra-db/src/mysql/mod.rs` — Rust implementation module (src/rs/vertebra-db/src/mysql/mod.rs).
[^src-vertebra-db-src-sqlite-rs]: `src/rs/vertebra-db/src/sqlite/mod.rs` — Rust implementation module (src/rs/vertebra-db/src/sqlite/mod.rs).
[^src-vertebra-db-src-oracle-rs]: `src/rs/vertebra-db/src/oracle/mod.rs` — Rust implementation module (src/rs/vertebra-db/src/oracle/mod.rs).
[^src-vertebra-db-src-tns-rs]: `src/rs/vertebra-db/src/oracle/tns.rs` — Rust implementation module (src/rs/vertebra-db/src/oracle/tns.rs).
[^src-vertebra-db-src-mssql-rs]: `src/rs/vertebra-db/src/mssql/mod.rs` — Rust implementation module (src/rs/vertebra-db/src/mssql/mod.rs).
[^src-vertebra-db-src-duckdb-rs]: `src/rs/vertebra-db/src/duckdb/mod.rs` — Rust implementation module (src/rs/vertebra-db/src/duckdb/mod.rs).
[^src-vertebra-db-src-gcp-rs]: `src/rs/vertebra-db/src/gcp.rs` — Rust implementation module (src/rs/vertebra-db/src/gcp.rs).
[^src-vertebra-db-src-bigquery-rs]: `src/rs/vertebra-db/src/bigquery/mod.rs` — Rust implementation module (src/rs/vertebra-db/src/bigquery/mod.rs).
[^src-vertebra-db-src-spanner-rs]: `src/rs/vertebra-db/src/spanner/mod.rs` — Rust implementation module (src/rs/vertebra-db/src/spanner/mod.rs).
[^src-vertebra-db-src-resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs` — Rust implementation module (src/rs/vertebra-db/src/resp3/mod.rs).
[^src-vertebra-db-src-dialer-rs]: `src/rs/vertebra-db/src/dialer.rs` — Rust implementation module (src/rs/vertebra-db/src/dialer.rs).
[^src-vertebra-py-src-completion-rs]: `src/rs/vertebra-py/src/completion.rs` — Rust implementation module (src/rs/vertebra-py/src/completion.rs).
[^src-vertebra-py-src-db-rs]: `src/rs/vertebra-py/src/db.rs` — Rust implementation module (src/rs/vertebra-py/src/db.rs).
[^src-vertebra-py-src-errors-rs]: `src/rs/vertebra-py/src/errors.rs` — Rust implementation module (src/rs/vertebra-py/src/errors.rs).
[^src-vertebra-db-tests-gcp-credentials-tests-rs]: `src/rs/vertebra-db/tests/gcp_credentials_tests.rs` — Rust implementation module (src/rs/vertebra-db/tests/gcp_credentials_tests.rs).
[^src-vertebra-db-tests-spanner-wire-tests-rs]: `src/rs/vertebra-db/tests/spanner_wire_tests.rs` — Rust implementation module (src/rs/vertebra-db/tests/spanner_wire_tests.rs).
[^src-vertebra-db-tests-bigquery-wire-tests-rs]: `src/rs/vertebra-db/tests/bigquery_wire_tests.rs` — Rust implementation module (src/rs/vertebra-db/tests/bigquery_wire_tests.rs).
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Python implementation module (src/py/vertebra/db/__init__.py).
[^src-db-arrow-py]: `src/py/vertebra/db/arrow.py` — Python implementation module (src/py/vertebra/db/arrow.py).
[^src-db-registry-py]: `src/py/vertebra/db/registry.py` — Python implementation module (src/py/vertebra/db/registry.py).
[^src-db-driver-init-py]: `src/py/vertebra/db/driver/__init__.py` — Python implementation module (src/py/vertebra/db/driver/__init__.py).
[^src-db-driver-common-py]: `src/py/vertebra/db/driver/_common.py` — Python implementation module (src/py/vertebra/db/driver/_common.py).
[^src-db-driver-sync-py]: `src/py/vertebra/db/driver/_sync.py` — Python implementation module (src/py/vertebra/db/driver/_sync.py).
[^src-db-driver-async-py]: `src/py/vertebra/db/driver/_async.py` — Python implementation module (src/py/vertebra/db/driver/_async.py).
[^src-db-postgres-init-py]: `src/py/vertebra/db/adapters/postgres/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/postgres/__init__.py).
[^src-db-mysql-init-py]: `src/py/vertebra/db/adapters/mysql/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/mysql/__init__.py).
[^src-db-sqlite-init-py]: `src/py/vertebra/db/adapters/sqlite/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/sqlite/__init__.py).
[^src-db-oracle-init-py]: `src/py/vertebra/db/adapters/oracle/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/oracle/__init__.py).
[^src-db-mssql-init-py]: `src/py/vertebra/db/adapters/mssql/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/mssql/__init__.py).
[^src-db-duckdb-init-py]: `src/py/vertebra/db/adapters/duckdb/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/duckdb/__init__.py).
[^src-db-bigquery-init-py]: `src/py/vertebra/db/adapters/bigquery/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/bigquery/__init__.py).
[^src-db-spanner-init-py]: `src/py/vertebra/db/adapters/spanner/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/spanner/__init__.py).
[^src-db-redis-init-py]: `src/py/vertebra/db/adapters/redis/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/redis/__init__.py).
[^src-db-valkey-init-py]: `src/py/vertebra/db/adapters/valkey/__init__.py` — Python implementation module (src/py/vertebra/db/adapters/valkey/__init__.py).
[^src-db-dialer-cloudsql-py]: `src/py/vertebra/db/dialer/cloudsql.py` — Python implementation module (src/py/vertebra/db/dialer/cloudsql.py).
[^src-db-dialer-alloydb-py]: `src/py/vertebra/db/dialer/alloydb.py` — Python implementation module (src/py/vertebra/db/dialer/alloydb.py).
[^src-tests-unit-db-test-native-db-drivers-py]: `src/py/tests/unit/db/test_native_db_drivers.py` — Python implementation module (src/py/tests/unit/db/test_native_db_drivers.py).
[^src-tests-unit-db-test-enterprise-olap-drivers-py]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — Python implementation module (src/py/tests/unit/db/test_enterprise_olap_drivers.py).
[^src-tests-unit-db-test-redis-valkey-resp3-py]: `src/py/tests/unit/db/test_redis_valkey_resp3.py` — Python implementation module (src/py/tests/unit/db/test_redis_valkey_resp3.py).
[^src-tests-unit-db-test-bigquery-py]: `src/py/tests/unit/db/test_bigquery.py` — Python implementation module (src/py/tests/unit/db/test_bigquery.py).
[^src-tests-unit-db-test-spanner-py]: `src/py/tests/unit/db/test_spanner.py` — Python implementation module (src/py/tests/unit/db/test_spanner.py).
